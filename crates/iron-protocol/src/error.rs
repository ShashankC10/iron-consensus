use std::error::Error;

use thiserror::Error;

/// A type-erased codec error that retains its concrete source chain.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// A typed protocol-boundary failure.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Peer-controlled input was structurally valid as an envelope but invalid
    /// for the selected protocol.
    #[error("malformed protocol input ({code}): {message}")]
    MalformedInput {
        /// Stable machine-readable rejection code.
        code: &'static str,
        /// Bounded diagnostic that excludes complete payloads and secrets.
        message: String,
    },

    /// A valid request was rejected by the current protocol state.
    #[error("protocol request rejected ({code}): {message}")]
    Rejected {
        /// Stable machine-readable rejection code.
        code: &'static str,
        /// Bounded diagnostic.
        message: String,
    },

    /// Recovery encountered a record this implementation cannot interpret.
    #[error("unsupported recovery record kind {record_kind}, schema {schema_version}")]
    UnsupportedRecoveryRecord {
        /// Protocol-neutral WAL record kind.
        record_kind: u16,
        /// Payload schema version.
        schema_version: u16,
    },

    /// A protocol payload could not be decoded or encoded.
    #[error("protocol codec failure while {context}")]
    Codec {
        /// Stable operation context.
        context: &'static str,
        /// Typed codec source.
        #[source]
        source: BoxError,
    },

    /// A logical duration or record kind violated a construction invariant.
    #[error("invalid protocol value for `{field}`: {message}")]
    InvalidValue {
        /// Stable field name.
        field: &'static str,
        /// Bounded diagnostic.
        message: &'static str,
    },

    /// A state-machine invariant was violated. This represents an
    /// implementation defect, not malformed peer input.
    #[error("protocol invariant violated ({code})")]
    InvariantViolation {
        /// Stable diagnostic code.
        code: &'static str,
    },
}
