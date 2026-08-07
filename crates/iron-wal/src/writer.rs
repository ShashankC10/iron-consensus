use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use fs2::FileExt;
use iron_core::{Lsn, SyncPolicy, WalConfig};

use crate::WalError;
use crate::format::{WAL_FILE_NAME, encode_frame};
use crate::reader::scan;
use crate::record::{AppendOutcome, Replay, ReplayReport, WalRecordInput};

pub(crate) trait WalIo: Read + Write + Seek + Send {
    fn file_len(&mut self) -> io::Result<u64>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    fn sync_data(&mut self) -> io::Result<()>;
    fn unlock(&mut self) -> io::Result<()>;
}

struct StdWalIo(File);

impl Read for StdWalIo {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.0.read(buffer)
    }
}

impl Write for StdWalIo {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for StdWalIo {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}

impl WalIo for StdWalIo {
    fn file_len(&mut self) -> io::Result<u64> {
        self.0.metadata().map(|metadata| metadata.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn unlock(&mut self) -> io::Result<()> {
        FileExt::unlock(&self.0)
    }
}

/// The exclusive writer and strict replay handle for one `wal-v1.log` file.
pub struct FileWal {
    file: Box<dyn WalIo>,
    config: WalConfig,
    last_lsn: Option<Lsn>,
    durable_through: Option<Lsn>,
    valid_length: u64,
    unsynced_records: u32,
    poisoned: bool,
    open_report: ReplayReport,
}

impl std::fmt::Debug for FileWal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileWal")
            .field("config", &self.config)
            .field("last_lsn", &self.last_lsn)
            .field("durable_through", &self.durable_through)
            .field("valid_length", &self.valid_length)
            .field("unsynced_records", &self.unsynced_records)
            .field("poisoned", &self.poisoned)
            .field("open_report", &self.open_report)
            .finish_non_exhaustive()
    }
}

impl FileWal {
    /// Creates or opens and exclusively locks the configured WAL.
    ///
    /// Opening scans from byte zero, applies only the configured incomplete-tail
    /// policy, and derives the next LSN from validated frames. A newly created
    /// file and its containing directory are synchronized before this method
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::AlreadyOpen`] if another writer owns the advisory
    /// lock. I/O, unsupported format, corruption, size-limit, and rejected torn
    /// tail errors are returned without searching ahead for another frame.
    pub fn open(config: WalConfig) -> Result<Self, WalError> {
        let directory = config.directory();
        fs::create_dir_all(directory)
            .map_err(|source| WalError::io("creating WAL directory", source))?;
        let path = directory.join(WAL_FILE_NAME);
        let created = !path.exists();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|source| WalError::io("opening WAL file", source))?;
        FileExt::try_lock_exclusive(&file).map_err(|source| {
            if source.kind() == io::ErrorKind::WouldBlock {
                WalError::AlreadyOpen
            } else {
                WalError::io("locking WAL file", source)
            }
        })?;

        if created {
            file.sync_all()
                .map_err(|source| WalError::io("synchronizing new WAL file", source))?;
            sync_directory(directory)?;
        }

        let mut file: Box<dyn WalIo> = Box::new(StdWalIo(file));
        let replay = scan(
            file.as_mut(),
            config.max_record_bytes().get(),
            config.tail_repair(),
        )?;
        let report = replay.report().clone();
        Ok(Self {
            file,
            config,
            last_lsn: report.last_lsn(),
            durable_through: report.last_lsn(),
            valid_length: report.durable_file_length(),
            unsynced_records: 0,
            poisoned: false,
            open_report: report,
        })
    }

    /// Returns the scan report produced by opening this writer.
    #[must_use]
    pub const fn open_report(&self) -> &ReplayReport {
        &self.open_report
    }

    /// Returns the last fully written LSN, which may be newer than the known
    /// durability frontier under batch or manual policy.
    #[must_use]
    pub const fn last_lsn(&self) -> Option<Lsn> {
        self.last_lsn
    }

    /// Returns the latest LSN confirmed through `sync_data`.
    #[must_use]
    pub const fn durable_through(&self) -> Option<Lsn> {
        self.durable_through
    }

    /// Returns whether an uncertain write or failed sync requires reopen.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Appends one exact frame and applies the configured deterministic sync
    /// policy.
    ///
    /// # Errors
    ///
    /// Rejects oversize records before writing. Any `write_all` or `sync_data`
    /// failure poisons the writer, which then refuses appends and flushes until
    /// reopened and scanned.
    pub fn append(&mut self, record: WalRecordInput<'_>) -> Result<AppendOutcome, WalError> {
        self.ensure_writable()?;
        let actual = u64::try_from(record.payload().len()).unwrap_or(u64::MAX);
        let maximum = self.config.max_record_bytes().get();
        if actual > u64::from(maximum) {
            return Err(WalError::RecordTooLarge { actual, maximum });
        }

        let lsn = match self.last_lsn {
            Some(previous) => previous.checked_successor().ok_or(WalError::LsnExhausted)?,
            None => Lsn::FIRST,
        };
        let frame = encode_frame(lsn, record)?;
        self.file
            .seek(SeekFrom::Start(self.valid_length))
            .map_err(|source| WalError::io("seeking to WAL append position", source))?;
        if let Err(source) = self.file.write_all(&frame) {
            self.poisoned = true;
            return Err(WalError::io("writing WAL frame", source));
        }

        self.valid_length = self
            .valid_length
            .checked_add(u64::try_from(frame.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                self.poisoned = true;
                WalError::LsnExhausted
            })?;
        self.last_lsn = Some(lsn);
        self.unsynced_records = self.unsynced_records.checked_add(1).ok_or_else(|| {
            self.poisoned = true;
            WalError::LsnExhausted
        })?;

        let must_sync = match self.config.sync_policy() {
            SyncPolicy::Always => true,
            SyncPolicy::Batch {
                max_unsynced_records,
            } => self.unsynced_records >= max_unsynced_records.get(),
            SyncPolicy::Manual => false,
        };
        if must_sync {
            self.sync_current()?;
        }
        Ok(AppendOutcome::new(lsn, self.durable_through))
    }

    /// Calls `sync_data` and advances durability to the latest fully written
    /// LSN. This calls the barrier even when no record is currently pending.
    ///
    /// # Errors
    ///
    /// A sync failure poisons the writer and leaves the last confirmed
    /// durability frontier unchanged.
    pub fn flush(&mut self) -> Result<Option<Lsn>, WalError> {
        self.ensure_writable()?;
        self.sync_current()?;
        Ok(self.durable_through)
    }

    /// Establishes a barrier, then scans and returns all valid records.
    ///
    /// The barrier ensures the report's `durable_file_length` does not
    /// overstate unsynchronized writes made through this handle.
    ///
    /// # Errors
    ///
    /// A poisoned writer must be dropped and reopened. Otherwise flush, format,
    /// recovery, corruption, and I/O errors are propagated.
    pub fn replay(&mut self) -> Result<Replay, WalError> {
        self.flush()?;
        let replay = scan(
            self.file.as_mut(),
            self.config.max_record_bytes().get(),
            self.config.tail_repair(),
        )?;
        self.last_lsn = replay.report().last_lsn();
        self.durable_through = replay.report().last_lsn();
        self.valid_length = replay.report().durable_file_length();
        self.unsynced_records = 0;
        Ok(replay)
    }

    fn ensure_writable(&self) -> Result<(), WalError> {
        if self.poisoned {
            Err(WalError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn sync_current(&mut self) -> Result<(), WalError> {
        if let Err(source) = self.file.sync_data() {
            self.poisoned = true;
            return Err(WalError::io("synchronizing WAL data", source));
        }
        self.durable_through = self.last_lsn;
        self.unsynced_records = 0;
        Ok(())
    }
}

impl Drop for FileWal {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn sync_directory(directory: &Path) -> Result<(), WalError> {
    let directory = File::open(directory)
        .map_err(|source| WalError::io("opening WAL directory for sync", source))?;
    directory
        .sync_all()
        .map_err(|source| WalError::io("synchronizing WAL directory", source))
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};

    use iron_core::{
        DEFAULT_MAX_RECORD_BYTES, Lsn, RawSyncPolicy, RawWalConfig, SchemaVersion, TailRepair,
        WalConfig,
    };
    use tempfile::TempDir;

    use super::{FileWal, WalIo};
    use crate::format::{HEADER_LEN, WAL_FILE_NAME, encode_frame};
    use crate::record::{ReplayReport, WalRecordInput};
    use crate::{CorruptionKind, WalError};

    fn config(
        directory: &TempDir,
        sync_policy: RawSyncPolicy,
        tail_repair: TailRepair,
    ) -> WalConfig {
        WalConfig::try_from(RawWalConfig {
            directory: directory.path().to_path_buf(),
            max_record_bytes: u64::from(DEFAULT_MAX_RECORD_BYTES),
            sync_policy,
            tail_repair,
        })
        .expect("test WAL configuration is valid")
    }

    fn schema() -> SchemaVersion {
        SchemaVersion::try_from(1).expect("one is a valid schema version")
    }

    fn input(payload: &[u8]) -> WalRecordInput<'_> {
        WalRecordInput::new(1_024, schema(), payload).expect("test record kind is nonzero")
    }

    #[test]
    fn empty_replay_and_reopen_continuation_are_gapless() {
        let directory = tempfile::tempdir().expect("temporary directory is created");
        let config = config(&directory, RawSyncPolicy::Always, TailRepair::Reject);
        let mut wal = FileWal::open(config.clone()).expect("new WAL opens");
        let empty = wal.replay().expect("empty WAL replays");
        assert_eq!(empty.report().record_count(), 0);
        assert_eq!(empty.report().last_lsn(), None);

        let first = wal.append(input(b"first")).expect("first append succeeds");
        assert_eq!(first.lsn(), Lsn::FIRST);
        assert_eq!(first.durable_through(), Some(Lsn::FIRST));
        assert!(matches!(
            FileWal::open(config.clone()),
            Err(WalError::AlreadyOpen)
        ));
        drop(wal);

        let mut reopened = FileWal::open(config).expect("lock is released on drop");
        let second = reopened
            .append(input(b"second"))
            .expect("append continues after scan");
        assert_eq!(second.lsn().get(), 2);
        let replay = reopened.replay().expect("records replay");
        assert_eq!(
            replay
                .records()
                .iter()
                .map(|record| record.payload().as_ref())
                .collect::<Vec<_>>(),
            vec![&b"first"[..], &b"second"[..]]
        );
    }

    #[test]
    fn sync_policies_report_only_known_durability() {
        let manual_dir = tempfile::tempdir().expect("temporary directory is created");
        let mut manual = FileWal::open(config(
            &manual_dir,
            RawSyncPolicy::Manual,
            TailRepair::Reject,
        ))
        .expect("manual WAL opens");
        let first = manual.append(input(b"one")).expect("append succeeds");
        assert_eq!(first.durable_through(), None);
        assert_eq!(manual.flush().expect("barrier succeeds"), Some(Lsn::FIRST));

        let batch_dir = tempfile::tempdir().expect("temporary directory is created");
        let mut batch = FileWal::open(config(
            &batch_dir,
            RawSyncPolicy::Batch {
                max_unsynced_records: 2,
            },
            TailRepair::Reject,
        ))
        .expect("batch WAL opens");
        assert_eq!(
            batch
                .append(input(b"one"))
                .expect("append succeeds")
                .durable_through(),
            None
        );
        assert_eq!(
            batch
                .append(input(b"two"))
                .expect("batch boundary syncs")
                .durable_through()
                .map(Lsn::get),
            Some(2)
        );
    }

    #[test]
    fn append_rejects_payload_above_configured_limit() {
        let directory = tempfile::tempdir().expect("temporary directory is created");
        let config = WalConfig::try_from(RawWalConfig {
            directory: directory.path().to_path_buf(),
            max_record_bytes: 1_024,
            sync_policy: RawSyncPolicy::Always,
            tail_repair: TailRepair::Reject,
        })
        .expect("minimum record limit is valid");
        let mut wal = FileWal::open(config).expect("WAL opens");
        let payload = vec![0_u8; 1_025];
        assert!(matches!(
            wal.append(input(&payload)),
            Err(WalError::RecordTooLarge {
                actual: 1_025,
                maximum: 1_024
            })
        ));
        assert_eq!(wal.last_lsn(), None);
    }

    #[test]
    fn every_incomplete_frame_cut_obeys_both_tail_policies() {
        let frame = encode_frame(Lsn::FIRST, input(b"payload"))
            .expect("test frame is below configured limit");
        for cut in 1..frame.len() {
            for tail_repair in [TailRepair::Reject, TailRepair::Truncate] {
                let directory = tempfile::tempdir().expect("temporary directory is created");
                fs::write(directory.path().join(WAL_FILE_NAME), &frame[..cut])
                    .expect("torn frame fixture is written");
                let result = FileWal::open(config(&directory, RawSyncPolicy::Always, tail_repair));
                match tail_repair {
                    TailRepair::Reject => {
                        assert!(
                            matches!(result, Err(WalError::TornTail { offset: 0, .. })),
                            "cut {cut} must be rejected"
                        );
                        assert_eq!(
                            fs::metadata(directory.path().join(WAL_FILE_NAME))
                                .expect("fixture remains")
                                .len(),
                            cut as u64,
                            "reject policy must not mutate cut {cut}"
                        );
                    }
                    TailRepair::Truncate => {
                        let wal = result.expect("truncate policy repairs incomplete tail");
                        let repair = wal.open_report().tail_repair().expect("repair is reported");
                        assert_eq!(repair.bytes_removed(), cut as u64);
                        assert_eq!(wal.open_report().durable_file_length(), 0);
                    }
                }
            }
        }
    }

    #[test]
    fn payload_corruption_is_never_repaired() {
        let directory = tempfile::tempdir().expect("temporary directory is created");
        let config = config(&directory, RawSyncPolicy::Always, TailRepair::Truncate);
        {
            let mut wal = FileWal::open(config.clone()).expect("WAL opens");
            wal.append(input(b"payload")).expect("append succeeds");
        }
        let path = directory.path().join(WAL_FILE_NAME);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("fixture opens");
        file.seek(SeekFrom::Start(HEADER_LEN as u64))
            .expect("payload offset exists");
        file.write_all(b"P").expect("payload byte is flipped");
        file.sync_data().expect("fixture sync succeeds");

        assert!(matches!(
            FileWal::open(config),
            Err(WalError::Corruption {
                kind: CorruptionKind::PayloadChecksum,
                ..
            })
        ));
    }

    #[test]
    fn bad_magic_and_lsn_gap_are_hard_corruption() {
        let bad_magic_dir = tempfile::tempdir().expect("temporary directory is created");
        let mut frame = encode_frame(Lsn::FIRST, input(b"x")).expect("frame encodes");
        frame[0] ^= 1;
        fs::write(bad_magic_dir.path().join(WAL_FILE_NAME), frame).expect("fixture is written");
        assert!(matches!(
            FileWal::open(config(
                &bad_magic_dir,
                RawSyncPolicy::Always,
                TailRepair::Truncate
            )),
            Err(WalError::Corruption {
                kind: CorruptionKind::BadMagic,
                ..
            })
        ));

        let gap_dir = tempfile::tempdir().expect("temporary directory is created");
        let mut bytes = encode_frame(Lsn::FIRST, input(b"one")).expect("frame encodes");
        let lsn_three = Lsn::try_from(3).expect("three is nonzero");
        bytes.extend(encode_frame(lsn_three, input(b"three")).expect("frame encodes"));
        fs::write(gap_dir.path().join(WAL_FILE_NAME), bytes).expect("fixture is written");
        assert!(matches!(
            FileWal::open(config(&gap_dir, RawSyncPolicy::Always, TailRepair::Reject)),
            Err(WalError::Corruption {
                kind: CorruptionKind::LsnSequence {
                    expected: 2,
                    actual: 3
                },
                ..
            })
        ));
    }

    #[test]
    fn sync_and_partial_write_failures_poison_writer() {
        let directory = tempfile::tempdir().expect("temporary directory is created");
        let config = config(&directory, RawSyncPolicy::Always, TailRepair::Reject);
        let report = ReplayReport::new(0, None, 0, None);
        let mut sync_failure = FileWal {
            file: Box::new(FaultIo::failing_sync()),
            config: config.clone(),
            last_lsn: None,
            durable_through: None,
            valid_length: 0,
            unsynced_records: 0,
            poisoned: false,
            open_report: report.clone(),
        };
        assert!(matches!(
            sync_failure.append(input(b"x")),
            Err(WalError::Io { .. })
        ));
        assert!(sync_failure.is_poisoned());
        assert!(matches!(
            sync_failure.append(input(b"again")),
            Err(WalError::Poisoned)
        ));

        let mut write_failure = FileWal {
            file: Box::new(FaultIo::failing_partial_write(7)),
            config,
            last_lsn: None,
            durable_through: None,
            valid_length: 0,
            unsynced_records: 0,
            poisoned: false,
            open_report: report,
        };
        assert!(matches!(
            write_failure.append(input(b"payload")),
            Err(WalError::Io { .. })
        ));
        assert!(write_failure.is_poisoned());
    }

    struct FaultIo {
        cursor: Cursor<Vec<u8>>,
        fail_sync: bool,
        partial_write: Option<usize>,
        partial_was_written: bool,
    }

    impl FaultIo {
        fn failing_sync() -> Self {
            Self {
                cursor: Cursor::new(Vec::new()),
                fail_sync: true,
                partial_write: None,
                partial_was_written: false,
            }
        }

        fn failing_partial_write(bytes: usize) -> Self {
            Self {
                cursor: Cursor::new(Vec::new()),
                fail_sync: false,
                partial_write: Some(bytes),
                partial_was_written: false,
            }
        }
    }

    impl Read for FaultIo {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.cursor.read(buffer)
        }
    }

    impl Write for FaultIo {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if let Some(limit) = self.partial_write {
                if self.partial_was_written {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "injected partial write failure",
                    ));
                }
                self.partial_was_written = true;
                return self.cursor.write(&buffer[..limit.min(buffer.len())]);
            }
            self.cursor.write(buffer)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Seek for FaultIo {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.cursor.seek(position)
        }
    }

    impl WalIo for FaultIo {
        fn file_len(&mut self) -> io::Result<u64> {
            Ok(self.cursor.get_ref().len() as u64)
        }

        fn set_len(&mut self, len: u64) -> io::Result<()> {
            let len = usize::try_from(len)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "length too large"))?;
            self.cursor.get_mut().resize(len, 0);
            if self.cursor.position() > len as u64 {
                self.cursor.set_position(len as u64);
            }
            Ok(())
        }

        fn sync_data(&mut self) -> io::Result<()> {
            if self.fail_sync {
                Err(io::Error::other("injected sync failure"))
            } else {
                Ok(())
            }
        }

        fn unlock(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
