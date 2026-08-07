use std::io;

use iron_core::Lsn;
use thiserror::Error;

/// A hard corruption classification. These failures are never auto-repaired.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CorruptionKind {
    /// Frame magic did not equal `ICWL`.
    #[error("bad frame magic")]
    BadMagic,
    /// Header checksum did not match bytes 0 through 35.
    #[error("header CRC32C mismatch")]
    HeaderChecksum,
    /// Payload checksum did not match the declared payload.
    #[error("payload CRC32C mismatch")]
    PayloadChecksum,
    /// Total frame length and payload length were inconsistent.
    #[error("frame length relationship is invalid")]
    LengthMismatch,
    /// The record kind or schema version was zero.
    #[error("record kind and schema version must be nonzero")]
    ZeroRecordMetadata,
    /// The observed LSN was not the exact expected successor.
    #[error("LSN sequence mismatch: expected {expected}, found {actual}")]
    LsnSequence {
        /// Required next LSN.
        expected: u64,
        /// On-disk LSN.
        actual: u64,
    },
}

/// A v1 header field whose non-v1 value must fail closed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum UnsupportedField {
    /// WAL format version.
    #[error("format version")]
    FormatVersion,
    /// Header length.
    #[error("header length")]
    HeaderLength,
    /// Flags bitmap.
    #[error("flags")]
    Flags,
}

/// WAL validation, recovery, locking, and durability failures.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WalError {
    /// Another writer owns the advisory lock for `wal-v1.log`.
    #[error("write-ahead log is already open by another writer")]
    AlreadyOpen,

    /// A filesystem operation failed; the typed source is retained.
    #[error("WAL I/O failure while {operation}")]
    Io {
        /// Stable operation name.
        operation: &'static str,
        /// Typed operating-system error.
        #[source]
        source: io::Error,
    },

    /// A format field is understood but has no supported v1 value.
    #[error("unsupported WAL {field} value {actual} at offset {offset}")]
    UnsupportedFormat {
        /// Byte offset of the failing frame.
        offset: u64,
        /// Failing header field.
        field: UnsupportedField,
        /// On-disk value.
        actual: u64,
    },

    /// A complete frame is corrupt and cannot be auto-repaired.
    #[error("WAL corruption at offset {offset} after LSN {last_valid_lsn:?}: {kind}")]
    Corruption {
        /// Byte offset of the failing frame.
        offset: u64,
        /// Last completely validated LSN.
        last_valid_lsn: Option<Lsn>,
        /// Stable corruption class.
        kind: CorruptionKind,
    },

    /// The configured tail policy rejected an incomplete trailing frame.
    #[error(
        "torn WAL tail at offset {offset} (file length {file_length}) after LSN {last_valid_lsn:?}"
    )]
    TornTail {
        /// Start of the incomplete frame.
        offset: u64,
        /// Current physical length.
        file_length: u64,
        /// Last completely validated LSN.
        last_valid_lsn: Option<Lsn>,
    },

    /// A record exceeds the configured payload bound.
    #[error("WAL record payload is {actual} bytes; configured maximum is {maximum}")]
    RecordTooLarge {
        /// Payload size.
        actual: u64,
        /// Configured maximum.
        maximum: u32,
    },

    /// An on-disk record exceeds the configured bound before allocation.
    #[error(
        "on-disk WAL payload at offset {offset} is {actual} bytes; configured maximum is {maximum}"
    )]
    OnDiskRecordTooLarge {
        /// Frame offset.
        offset: u64,
        /// Declared payload size.
        actual: u32,
        /// Configured maximum.
        maximum: u32,
    },

    /// A caller supplied record kind zero.
    #[error("WAL record kind must be nonzero")]
    ZeroRecordKind,

    /// The next LSN cannot be represented.
    #[error("WAL LSN space is exhausted")]
    LsnExhausted,

    /// A prior uncertain write or failed sync prevents safe continued writes.
    #[error("WAL writer is poisoned; reopen and scan before writing again")]
    Poisoned,
}

impl WalError {
    pub(crate) fn io(operation: &'static str, source: io::Error) -> Self {
        Self::Io { operation, source }
    }
}
