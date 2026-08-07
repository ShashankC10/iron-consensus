use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use iron_core::{RawSyncPolicy, RawWalConfig, TailRepair, ValidationErrors, WalConfig};
use iron_wal::WAL_FILE_NAME;
use tempfile::TempDir;

/// An isolated WAL directory with explicit byte-level crash/corruption tools.
#[derive(Debug)]
pub struct WalFixture {
    directory: TempDir,
}

impl WalFixture {
    /// Creates a uniquely isolated fixture directory.
    pub fn new() -> io::Result<Self> {
        tempfile::tempdir().map(|directory| Self { directory })
    }

    /// Returns the containing directory retained for this fixture's lifetime.
    #[must_use]
    pub fn directory(&self) -> &Path {
        self.directory.path()
    }

    /// Returns the fixed v1 file path.
    #[must_use]
    pub fn wal_path(&self) -> PathBuf {
        self.directory.path().join(WAL_FILE_NAME)
    }

    /// Builds a standalone validated WAL configuration for this fixture.
    ///
    /// # Errors
    ///
    /// Returns sorted validation errors when `max_record_bytes` or batch
    /// policy bounds are invalid.
    pub fn config(
        &self,
        max_record_bytes: u64,
        sync_policy: RawSyncPolicy,
        tail_repair: TailRepair,
    ) -> Result<WalConfig, ValidationErrors> {
        WalConfig::try_from(RawWalConfig {
            directory: self.directory.path().to_path_buf(),
            max_record_bytes,
            sync_policy,
            tail_repair,
        })
    }

    /// Truncates the WAL to an exact byte length and synchronizes the mutation.
    pub fn truncate_to(&self, length: u64) -> io::Result<()> {
        let file = OpenOptions::new().write(true).open(self.wal_path())?;
        file.set_len(length)?;
        file.sync_data()
    }

    /// XORs one nonzero bit mask at an exact file offset and synchronizes it.
    pub fn flip_bits(&self, offset: u64, mask: u8) -> io::Result<()> {
        if mask == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bit-flip mask must be nonzero",
            ));
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.wal_path())?;
        file.seek(SeekFrom::Start(offset))?;
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte)?;
        byte[0] ^= mask;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&byte)?;
        file.sync_data()
    }

    /// Appends arbitrary bytes to model an incomplete final write and
    /// synchronizes the fixture mutation.
    pub fn append_raw(&self, bytes: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.wal_path())?;
        file.write_all(bytes)?;
        file.sync_data()
    }

    /// Returns the current physical WAL length, or zero before file creation.
    pub fn wal_len(&self) -> io::Result<u64> {
        match fs::metadata(self.wal_path()) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::WalFixture;

    #[test]
    fn byte_operations_are_explicit_and_repeatable() {
        let fixture = WalFixture::new().expect("fixture directory is created");
        fixture.append_raw(&[1, 2, 3]).expect("bytes append");
        fixture.flip_bits(1, 0b1).expect("bit flips");
        assert_eq!(
            fs::read(fixture.wal_path()).expect("fixture reads"),
            vec![1, 3, 3]
        );
        fixture.truncate_to(2).expect("fixture truncates");
        assert_eq!(fixture.wal_len().expect("length reads"), 2);
    }
}
