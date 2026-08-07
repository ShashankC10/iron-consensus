use bytes::Bytes;
use iron_core::{Lsn, SchemaVersion};

use crate::WalError;

/// An opaque record supplied for append.
#[derive(Clone, Copy, Debug)]
pub struct WalRecordInput<'a> {
    record_kind: u16,
    schema_version: SchemaVersion,
    payload: &'a [u8],
}

impl<'a> WalRecordInput<'a> {
    /// Creates an append input with nonzero metadata.
    ///
    /// # Errors
    ///
    /// Returns [`WalError::ZeroRecordKind`] when `record_kind` is zero. The
    /// validated [`SchemaVersion`] type already excludes zero.
    pub fn new(
        record_kind: u16,
        schema_version: SchemaVersion,
        payload: &'a [u8],
    ) -> Result<Self, WalError> {
        if record_kind == 0 {
            return Err(WalError::ZeroRecordKind);
        }
        Ok(Self {
            record_kind,
            schema_version,
            payload,
        })
    }

    /// Returns the protocol-neutral kind.
    #[must_use]
    pub const fn record_kind(self) -> u16 {
        self.record_kind
    }

    /// Returns the payload schema version.
    #[must_use]
    pub const fn schema_version(self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the opaque payload.
    #[must_use]
    pub const fn payload(self) -> &'a [u8] {
        self.payload
    }
}

/// One strictly validated replay record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    lsn: Lsn,
    record_kind: u16,
    schema_version: SchemaVersion,
    payload: Bytes,
}

impl WalRecord {
    pub(crate) fn from_validated(
        lsn: Lsn,
        record_kind: u16,
        schema_version: SchemaVersion,
        payload: Bytes,
    ) -> Self {
        Self {
            lsn,
            record_kind,
            schema_version,
            payload,
        }
    }

    /// Returns the assigned LSN.
    #[must_use]
    pub const fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Returns the protocol-neutral record kind.
    #[must_use]
    pub const fn record_kind(&self) -> u16 {
        self.record_kind
    }

    /// Returns the payload schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns reference-counted opaque payload bytes.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// Details of an explicitly policy-authorized torn-tail truncation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TailRepairReport {
    offset: u64,
    bytes_removed: u64,
    last_valid_lsn: Option<Lsn>,
}

impl TailRepairReport {
    pub(crate) const fn new(offset: u64, bytes_removed: u64, last_valid_lsn: Option<Lsn>) -> Self {
        Self {
            offset,
            bytes_removed,
            last_valid_lsn,
        }
    }

    /// Returns the frame-start offset retained as the new file end.
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Returns the number of bytes synchronously removed.
    #[must_use]
    pub const fn bytes_removed(self) -> u64 {
        self.bytes_removed
    }

    /// Returns the final valid LSN before the repaired tail.
    #[must_use]
    pub const fn last_valid_lsn(self) -> Option<Lsn> {
        self.last_valid_lsn
    }
}

/// Stable replay statistics and optional repair information.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayReport {
    record_count: u64,
    last_lsn: Option<Lsn>,
    durable_file_length: u64,
    tail_repair: Option<TailRepairReport>,
}

impl ReplayReport {
    pub(crate) const fn new(
        record_count: u64,
        last_lsn: Option<Lsn>,
        durable_file_length: u64,
        tail_repair: Option<TailRepairReport>,
    ) -> Self {
        Self {
            record_count,
            last_lsn,
            durable_file_length,
            tail_repair,
        }
    }

    /// Returns the complete valid record count.
    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Returns the final valid LSN.
    #[must_use]
    pub const fn last_lsn(&self) -> Option<Lsn> {
        self.last_lsn
    }

    /// Returns the complete, synchronized file length after optional repair.
    #[must_use]
    pub const fn durable_file_length(&self) -> u64 {
        self.durable_file_length
    }

    /// Returns torn-tail repair details, if any.
    #[must_use]
    pub const fn tail_repair(&self) -> Option<TailRepairReport> {
        self.tail_repair
    }
}

/// Replay records plus their externally observable report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Replay {
    records: Vec<WalRecord>,
    report: ReplayReport,
}

impl Replay {
    pub(crate) const fn new(records: Vec<WalRecord>, report: ReplayReport) -> Self {
        Self { records, report }
    }

    /// Returns records in ascending LSN order.
    #[must_use]
    pub fn records(&self) -> &[WalRecord] {
        &self.records
    }

    /// Returns replay statistics and repair information.
    #[must_use]
    pub const fn report(&self) -> &ReplayReport {
        &self.report
    }

    /// Consumes replay output.
    #[must_use]
    pub fn into_parts(self) -> (Vec<WalRecord>, ReplayReport) {
        (self.records, self.report)
    }
}

/// Result of one fully written frame and its known durability frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendOutcome {
    lsn: Lsn,
    durable_through: Option<Lsn>,
}

impl AppendOutcome {
    pub(crate) const fn new(lsn: Lsn, durable_through: Option<Lsn>) -> Self {
        Self {
            lsn,
            durable_through,
        }
    }

    /// Returns the assigned LSN.
    #[must_use]
    pub const fn lsn(self) -> Lsn {
        self.lsn
    }

    /// Returns the last LSN confirmed through `sync_data`.
    #[must_use]
    pub const fn durable_through(self) -> Option<Lsn> {
        self.durable_through
    }
}
