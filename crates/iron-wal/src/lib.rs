#![forbid(unsafe_code)]

//! A locked, synchronous, single-file write-ahead log with strict recovery.
//!
//! Layer 1 stores opaque record payloads in `wal-v1.log`. The writer assigns
//! gapless LSNs and explicitly reports how far durability has advanced. A
//! policy may repair only an incomplete trailing frame; checksum, framing, or
//! sequence corruption is always a hard error.

mod error;
mod format;
mod reader;
mod record;
mod writer;

pub use error::{CorruptionKind, UnsupportedField, WalError};
pub use format::{FORMAT_VERSION, HEADER_LEN, MAGIC, WAL_FILE_NAME};
pub use record::{
    AppendOutcome, Replay, ReplayReport, TailRepairReport, WalRecord, WalRecordInput,
};
pub use writer::FileWal;

/// Synchronous durable-log operations used by the future runtime's dedicated
/// blocking boundary.
pub trait WriteAheadLog {
    /// Appends one opaque record.
    ///
    /// The returned `durable_through` is authoritative. A successful write
    /// under manual or not-yet-full batch policy need not be durable.
    ///
    /// # Errors
    ///
    /// Returns validation, I/O, LSN exhaustion, or poisoned-writer errors.
    fn append(&mut self, record: WalRecordInput<'_>) -> Result<AppendOutcome, WalError>;

    /// Establishes an explicit durability barrier through the latest fully
    /// written record.
    ///
    /// # Errors
    ///
    /// A sync failure poisons the writer and retains the previous known
    /// `durable_through` value.
    fn flush(&mut self) -> Result<Option<iron_core::Lsn>, WalError>;

    /// Strictly replays complete valid records in ascending LSN order.
    ///
    /// # Errors
    ///
    /// Returns typed format, corruption, torn-tail, configured-limit, or I/O
    /// failures. Only an incomplete tail can be mutated, and only under the
    /// configured truncate policy.
    fn replay(&mut self) -> Result<Replay, WalError>;
}

impl WriteAheadLog for FileWal {
    fn append(&mut self, record: WalRecordInput<'_>) -> Result<AppendOutcome, WalError> {
        FileWal::append(self, record)
    }

    fn flush(&mut self) -> Result<Option<iron_core::Lsn>, WalError> {
        FileWal::flush(self)
    }

    fn replay(&mut self) -> Result<Replay, WalError> {
        FileWal::replay(self)
    }
}
