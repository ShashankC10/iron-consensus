#![forbid(unsafe_code)]
#![doc = "Protocol-neutral validated types for Iron Consensus."]

pub mod config;
pub mod dedup;
pub mod envelope;
pub mod error;
pub mod id;
pub mod limits;

pub use config::{
    DedupConfig, NodeConfig, PeerEndpoint, RawDedupConfig, RawNodeConfig, RawSyncPolicy,
    RawTransportConfig, RawWalConfig, SyncPolicy, TailRepair, TransportConfig, WalConfig,
};
pub use dedup::{AbortResult, BeginResult, CompleteResult, DedupKey, DedupTable, RestoreResult};
pub use envelope::{
    Envelope, EnvelopeFields, EnvelopeV1, FINGERPRINT_FORMAT_VERSION, SemanticFingerprint,
};
pub use error::{CoreError, ValidationError, ValidationErrors};
pub use id::{
    ClientRequestId, ClusterId, CorrelationId, EnvelopeVersion, IdGenerator, Lsn, MessageId,
    MessageType, NodeId, ProtocolName, SchemaVersion, TimerId,
};
pub use limits::{
    DEFAULT_MAX_DEDUP_ENTRIES, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_OUTCOME_BYTES,
    DEFAULT_MAX_RECORD_BYTES, DEFAULT_MAX_TOTAL_OUTCOME_BYTES, MAX_TIMEOUT_MILLIS, MaxDedupEntries,
    MaxMessageBytes, MaxOutcomeBytes, MaxRecordBytes, MaxTotalOutcomeBytes, MaxUnsyncedRecords,
    TimeoutMillis, WAL_FRAME_OVERHEAD_BYTES,
};
