use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::de::{Error as DeError, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

use crate::error::{CoreError, ValidationError, ValidationErrors};
use crate::id::{ClusterId, NodeId};
use crate::limits::{
    DEFAULT_MAX_DEDUP_ENTRIES, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_OUTCOME_BYTES,
    DEFAULT_MAX_RECORD_BYTES, DEFAULT_MAX_TOTAL_OUTCOME_BYTES, MaxDedupEntries, MaxMessageBytes,
    MaxOutcomeBytes, MaxRecordBytes, MaxTotalOutcomeBytes, MaxUnsyncedRecords, TimeoutMillis,
    WAL_FRAME_OVERHEAD_BYTES,
};

/// A validated advertised peer URL.
///
/// Only `http` and `https` URLs are accepted. Credentials, query strings, and
/// fragments are rejected. Unlike listen addresses, advertised URLs may use a
/// DNS hostname.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerEndpoint(Url);

impl PeerEndpoint {
    pub fn parse(input: &str) -> Result<Self, CoreError> {
        let url =
            Url::parse(input).map_err(|source| CoreError::InvalidPeerEndpointUrl { source })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(CoreError::InvalidPeerEndpointScheme);
        }
        if url.host().is_none() || url.cannot_be_a_base() {
            return Err(CoreError::MissingPeerEndpointHost);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(CoreError::PeerEndpointCredentialsNotAllowed);
        }
        if url.query().is_some() {
            return Err(CoreError::PeerEndpointQueryNotAllowed);
        }
        if url.fragment().is_some() {
            return Err(CoreError::PeerEndpointFragmentNotAllowed);
        }
        Ok(Self(url))
    }

    #[must_use]
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for PeerEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl FromStr for PeerEndpoint {
    type Err = CoreError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl Ord for PeerEndpoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for PeerEndpoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for PeerEndpoint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Serialize for PeerEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PeerEndpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        Self::parse(&input).map_err(D::Error::custom)
    }
}

/// Deterministic WAL synchronization policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SyncPolicy {
    #[default]
    Always,
    Batch {
        max_unsynced_records: MaxUnsyncedRecords,
    },
    Manual,
}

impl SyncPolicy {
    pub fn batch(max_unsynced_records: u64) -> Result<Self, CoreError> {
        Ok(Self::Batch {
            max_unsynced_records: MaxUnsyncedRecords::new(max_unsynced_records)?,
        })
    }
}

/// Policy for an incomplete final WAL frame.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TailRepair {
    Truncate,
    #[default]
    Reject,
}

/// Deserializable synchronization policy, validated during conversion.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum RawSyncPolicy {
    #[default]
    Always,
    Batch {
        max_unsynced_records: u64,
    },
    Manual,
}

/// Deserializable, unvalidated WAL configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawWalConfig {
    pub directory: PathBuf,
    #[serde(default = "default_max_record_bytes")]
    pub max_record_bytes: u64,
    #[serde(default)]
    pub sync_policy: RawSyncPolicy,
    #[serde(default)]
    pub tail_repair: TailRepair,
}

/// Deserializable, unvalidated transport configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawTransportConfig {
    pub listen_address: String,
    pub advertised_url: String,
    pub request_timeout_ms: u64,
    pub connect_timeout_ms: u64,
    #[serde(default = "default_max_message_bytes")]
    pub max_message_bytes: u64,
}

/// Deserializable, unvalidated deduplication configuration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawDedupConfig {
    #[serde(default = "default_max_dedup_entries")]
    pub max_entries: u64,
    #[serde(default = "default_max_outcome_bytes")]
    pub max_outcome_bytes: u64,
    #[serde(default = "default_max_total_outcome_bytes")]
    pub max_total_outcome_bytes: u64,
}

impl Default for RawDedupConfig {
    fn default() -> Self {
        Self {
            max_entries: u64::from(DEFAULT_MAX_DEDUP_ENTRIES),
            max_outcome_bytes: u64::from(DEFAULT_MAX_OUTCOME_BYTES),
            max_total_outcome_bytes: u64::from(DEFAULT_MAX_TOTAL_OUTCOME_BYTES),
        }
    }
}

/// Deserializable external node configuration. It is deliberately raw: no
/// production component should consume it before `NodeConfig::try_from`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RawNodeConfig {
    pub cluster_id: String,
    pub node_id: String,
    #[serde(deserialize_with = "deserialize_members")]
    pub members: BTreeMap<String, String>,
    pub wal: RawWalConfig,
    pub transport: RawTransportConfig,
    #[serde(default)]
    pub dedup: RawDedupConfig,
}

fn deserialize_members<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MembersVisitor;

    impl<'de> Visitor<'de> for MembersVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map from unique node IDs to peer endpoint URLs")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut members = BTreeMap::new();
            while let Some((node_id, endpoint)) = map.next_entry::<String, String>()? {
                if members.insert(node_id, endpoint).is_some() {
                    return Err(A::Error::custom(
                        "members must not contain a duplicate node ID",
                    ));
                }
            }
            Ok(members)
        }
    }

    deserializer.deserialize_map(MembersVisitor)
}

const fn default_max_record_bytes() -> u64 {
    DEFAULT_MAX_RECORD_BYTES as u64
}

const fn default_max_message_bytes() -> u64 {
    DEFAULT_MAX_MESSAGE_BYTES as u64
}

const fn default_max_dedup_entries() -> u64 {
    DEFAULT_MAX_DEDUP_ENTRIES as u64
}

const fn default_max_outcome_bytes() -> u64 {
    DEFAULT_MAX_OUTCOME_BYTES as u64
}

const fn default_max_total_outcome_bytes() -> u64 {
    DEFAULT_MAX_TOTAL_OUTCOME_BYTES as u64
}

/// Validated WAL configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalConfig {
    directory: PathBuf,
    max_record_bytes: MaxRecordBytes,
    sync_policy: SyncPolicy,
    tail_repair: TailRepair,
}

impl WalConfig {
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub const fn max_record_bytes(&self) -> MaxRecordBytes {
        self.max_record_bytes
    }

    #[must_use]
    pub const fn sync_policy(&self) -> SyncPolicy {
        self.sync_policy
    }

    #[must_use]
    pub const fn tail_repair(&self) -> TailRepair {
        self.tail_repair
    }
}

impl TryFrom<RawWalConfig> for WalConfig {
    type Error = ValidationErrors;

    fn try_from(raw: RawWalConfig) -> Result<Self, Self::Error> {
        let mut errors = Vec::new();
        if raw.directory.as_os_str().is_empty() {
            errors.push(ValidationError::new(
                "directory",
                "empty",
                "WAL directory must not be empty",
            ));
        }
        let max_record_bytes = match MaxRecordBytes::new(raw.max_record_bytes) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "max_record_bytes",
                    "out_of_range",
                    "WAL record limit must be between 1 KiB and 64 MiB",
                ));
                None
            }
        };
        let sync_policy = match raw.sync_policy {
            RawSyncPolicy::Always => Some(SyncPolicy::Always),
            RawSyncPolicy::Manual => Some(SyncPolicy::Manual),
            RawSyncPolicy::Batch {
                max_unsynced_records,
            } => match SyncPolicy::batch(max_unsynced_records) {
                Ok(value) => Some(value),
                Err(_) => {
                    errors.push(ValidationError::new(
                        "sync_policy.max_unsynced_records",
                        "zero",
                        "batch size must be nonzero",
                    ));
                    None
                }
            },
        };

        if let Some(errors) = ValidationErrors::from_vec(errors) {
            return Err(errors);
        }
        Ok(Self {
            directory: raw.directory,
            max_record_bytes: max_record_bytes.expect("validated value is present"),
            sync_policy: sync_policy.expect("validated value is present"),
            tail_repair: raw.tail_repair,
        })
    }
}

/// Validated transport configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransportConfig {
    listen_address: SocketAddr,
    advertised_url: PeerEndpoint,
    request_timeout: TimeoutMillis,
    connect_timeout: TimeoutMillis,
    max_message_bytes: MaxMessageBytes,
}

impl TransportConfig {
    #[must_use]
    pub const fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    #[must_use]
    pub fn advertised_url(&self) -> &PeerEndpoint {
        &self.advertised_url
    }

    #[must_use]
    pub const fn request_timeout(&self) -> TimeoutMillis {
        self.request_timeout
    }

    #[must_use]
    pub const fn connect_timeout(&self) -> TimeoutMillis {
        self.connect_timeout
    }

    #[must_use]
    pub const fn max_message_bytes(&self) -> MaxMessageBytes {
        self.max_message_bytes
    }
}

impl TryFrom<RawTransportConfig> for TransportConfig {
    type Error = ValidationErrors;

    fn try_from(raw: RawTransportConfig) -> Result<Self, Self::Error> {
        let mut errors = Vec::new();
        let listen_address = match raw.listen_address.parse::<SocketAddr>() {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "listen_address",
                    "invalid_socket_address",
                    "listen address must be an explicit IP socket address",
                ));
                None
            }
        };
        let advertised_url = match PeerEndpoint::parse(&raw.advertised_url) {
            Ok(value) => Some(value),
            Err(error) => {
                let (code, message) = endpoint_error_description(&error);
                errors.push(ValidationError::new("advertised_url", code, message));
                None
            }
        };
        let request_timeout =
            validate_timeout(raw.request_timeout_ms, "request_timeout_ms", &mut errors);
        let connect_timeout =
            validate_timeout(raw.connect_timeout_ms, "connect_timeout_ms", &mut errors);
        let max_message_bytes = match MaxMessageBytes::new(raw.max_message_bytes) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "max_message_bytes",
                    "out_of_range",
                    "message limit must be nonzero and at most 64 MiB",
                ));
                None
            }
        };

        if let Some(errors) = ValidationErrors::from_vec(errors) {
            return Err(errors);
        }
        Ok(Self {
            listen_address: listen_address.expect("validated value is present"),
            advertised_url: advertised_url.expect("validated value is present"),
            request_timeout: request_timeout.expect("validated value is present"),
            connect_timeout: connect_timeout.expect("validated value is present"),
            max_message_bytes: max_message_bytes.expect("validated value is present"),
        })
    }
}

fn validate_timeout(
    raw: u64,
    path: &'static str,
    errors: &mut Vec<ValidationError>,
) -> Option<TimeoutMillis> {
    match TimeoutMillis::new(raw) {
        Ok(value) => Some(value),
        Err(_) => {
            errors.push(ValidationError::new(
                path,
                "out_of_range",
                "timeout must be between 1 and 300000 milliseconds",
            ));
            None
        }
    }
}

/// Validated deterministic deduplication bounds.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DedupConfig {
    max_entries: MaxDedupEntries,
    max_outcome_bytes: MaxOutcomeBytes,
    max_total_outcome_bytes: MaxTotalOutcomeBytes,
}

impl DedupConfig {
    #[must_use]
    pub const fn max_entries(self) -> MaxDedupEntries {
        self.max_entries
    }

    #[must_use]
    pub const fn max_outcome_bytes(self) -> MaxOutcomeBytes {
        self.max_outcome_bytes
    }

    #[must_use]
    pub const fn max_total_outcome_bytes(self) -> MaxTotalOutcomeBytes {
        self.max_total_outcome_bytes
    }
}

impl TryFrom<RawDedupConfig> for DedupConfig {
    type Error = ValidationErrors;

    fn try_from(raw: RawDedupConfig) -> Result<Self, Self::Error> {
        let mut errors = Vec::new();
        let max_entries = match MaxDedupEntries::new(raw.max_entries) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "max_entries",
                    "out_of_range",
                    "dedup entry limit must be nonzero and fit in 32 bits",
                ));
                None
            }
        };
        let max_outcome_bytes = match MaxOutcomeBytes::new(raw.max_outcome_bytes) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "max_outcome_bytes",
                    "out_of_range",
                    "outcome limit must be nonzero and at most 1 GiB",
                ));
                None
            }
        };
        let max_total_outcome_bytes = match MaxTotalOutcomeBytes::new(raw.max_total_outcome_bytes) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "max_total_outcome_bytes",
                    "out_of_range",
                    "total outcome limit must be between 64 KiB and 1 GiB",
                ));
                None
            }
        };
        if let (Some(outcome), Some(total)) = (max_outcome_bytes, max_total_outcome_bytes) {
            if outcome.get() > total.get() {
                errors.push(ValidationError::new(
                    "max_total_outcome_bytes",
                    "less_than_max_outcome_bytes",
                    "total outcome limit must be at least the per-outcome limit",
                ));
            }
        }

        if let Some(errors) = ValidationErrors::from_vec(errors) {
            return Err(errors);
        }
        Ok(Self {
            max_entries: max_entries.expect("validated value is present"),
            max_outcome_bytes: max_outcome_bytes.expect("validated value is present"),
            max_total_outcome_bytes: max_total_outcome_bytes.expect("validated value is present"),
        })
    }
}

/// Fully validated configuration for one node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    cluster_id: ClusterId,
    node_id: NodeId,
    members: BTreeMap<NodeId, PeerEndpoint>,
    wal: WalConfig,
    transport: TransportConfig,
    dedup: DedupConfig,
}

impl NodeConfig {
    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    #[must_use]
    pub fn members(&self) -> &BTreeMap<NodeId, PeerEndpoint> {
        &self.members
    }

    #[must_use]
    pub const fn wal(&self) -> &WalConfig {
        &self.wal
    }

    #[must_use]
    pub const fn transport(&self) -> &TransportConfig {
        &self.transport
    }

    #[must_use]
    pub const fn dedup(&self) -> DedupConfig {
        self.dedup
    }
}

impl TryFrom<RawNodeConfig> for NodeConfig {
    type Error = ValidationErrors;

    fn try_from(raw: RawNodeConfig) -> Result<Self, Self::Error> {
        let RawNodeConfig {
            cluster_id: raw_cluster_id,
            node_id: raw_node_id,
            members: raw_members,
            wal: raw_wal,
            transport: raw_transport,
            dedup: raw_dedup,
        } = raw;

        let mut errors = Vec::new();
        let cluster_id = match ClusterId::parse(&raw_cluster_id) {
            Ok(value) => Some(value),
            Err(error) => {
                let (code, message) = identity_error_description(&error);
                errors.push(ValidationError::new("cluster_id", code, message));
                None
            }
        };
        let node_id = match NodeId::parse(&raw_node_id) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    "node_id",
                    "invalid_node_id",
                    "node ID must satisfy the lowercase 1-64 byte grammar",
                ));
                None
            }
        };

        let members = validate_members(raw_members, node_id.as_ref(), &mut errors);

        let wal = match WalConfig::try_from(raw_wal) {
            Ok(value) => Some(value),
            Err(nested) => {
                extend_prefixed(&mut errors, "wal", nested);
                None
            }
        };
        let transport = match TransportConfig::try_from(raw_transport) {
            Ok(value) => Some(value),
            Err(nested) => {
                extend_prefixed(&mut errors, "transport", nested);
                None
            }
        };
        let dedup = match DedupConfig::try_from(raw_dedup) {
            Ok(value) => Some(value),
            Err(nested) => {
                extend_prefixed(&mut errors, "dedup", nested);
                None
            }
        };

        if let (Some(wal), Some(transport)) = (&wal, &transport) {
            let framed_message_bytes = transport
                .max_message_bytes()
                .get()
                .checked_add(WAL_FRAME_OVERHEAD_BYTES);
            if framed_message_bytes.is_none()
                || framed_message_bytes.is_some_and(|bytes| bytes > wal.max_record_bytes().get())
            {
                errors.push(ValidationError::new(
                    "transport.max_message_bytes",
                    "exceeds_wal_record_limit",
                    "message plus WAL framing must fit within the WAL record limit",
                ));
            }
        }
        if let (Some(dedup), Some(transport)) = (&dedup, &transport) {
            if dedup.max_outcome_bytes().get() > transport.max_message_bytes().get() {
                errors.push(ValidationError::new(
                    "dedup.max_outcome_bytes",
                    "exceeds_message_limit",
                    "one dedup outcome must fit within the message limit",
                ));
            }
        }

        if let Some(errors) = ValidationErrors::from_vec(errors) {
            return Err(errors);
        }
        Ok(Self {
            cluster_id: cluster_id.expect("validated value is present"),
            node_id: node_id.expect("validated value is present"),
            members,
            wal: wal.expect("validated value is present"),
            transport: transport.expect("validated value is present"),
            dedup: dedup.expect("validated value is present"),
        })
    }
}

fn validate_members(
    raw_members: BTreeMap<String, String>,
    local_node: Option<&NodeId>,
    errors: &mut Vec<ValidationError>,
) -> BTreeMap<NodeId, PeerEndpoint> {
    let mut members = BTreeMap::new();
    let mut endpoints = BTreeMap::<String, usize>::new();

    for (index, (raw_node, raw_endpoint)) in raw_members.into_iter().enumerate() {
        let node_path = format!("members[{index}].node_id");
        let endpoint_path = format!("members[{index}].endpoint");
        let node = match NodeId::parse(&raw_node) {
            Ok(value) => Some(value),
            Err(_) => {
                errors.push(ValidationError::new(
                    node_path,
                    "invalid_node_id",
                    "member node ID must satisfy the lowercase 1-64 byte grammar",
                ));
                None
            }
        };
        let endpoint = match PeerEndpoint::parse(&raw_endpoint) {
            Ok(value) => {
                if endpoints.insert(value.as_str().to_owned(), index).is_some() {
                    errors.push(ValidationError::new(
                        endpoint_path,
                        "duplicate_endpoint",
                        "member endpoints must be unique",
                    ));
                }
                Some(value)
            }
            Err(error) => {
                let (code, message) = endpoint_error_description(&error);
                errors.push(ValidationError::new(endpoint_path, code, message));
                None
            }
        };
        if let (Some(node), Some(endpoint)) = (node, endpoint) {
            let previous = members.insert(node, endpoint);
            debug_assert!(previous.is_none(), "raw member IDs are unique");
        }
    }

    if let Some(local_node) = local_node {
        if !members.contains_key(local_node) {
            errors.push(ValidationError::new(
                "members",
                "missing_local_node",
                "members must contain the local node exactly once",
            ));
        }
    }
    members
}

fn extend_prefixed(target: &mut Vec<ValidationError>, prefix: &str, nested: ValidationErrors) {
    target.extend(nested.into_iter().map(|error| {
        ValidationError::new(
            format!("{prefix}.{}", error.field_path()),
            error.code(),
            error.message(),
        )
    }));
}

fn identity_error_description(error: &CoreError) -> (&'static str, &'static str) {
    match error {
        CoreError::InvalidUuid { .. } => ("invalid_uuid", "cluster ID must be a UUID"),
        CoreError::ZeroIdentifier { .. } => ("zero", "cluster ID must not be all zeroes"),
        _ => ("invalid", "cluster ID is invalid"),
    }
}

fn endpoint_error_description(error: &CoreError) -> (&'static str, &'static str) {
    match error {
        CoreError::InvalidPeerEndpointUrl { .. } => {
            ("invalid_url", "peer endpoint must be a valid URL")
        }
        CoreError::InvalidPeerEndpointScheme => (
            "invalid_scheme",
            "peer endpoint scheme must be http or https",
        ),
        CoreError::MissingPeerEndpointHost => ("missing_host", "peer endpoint must contain a host"),
        CoreError::PeerEndpointCredentialsNotAllowed => (
            "credentials_not_allowed",
            "peer endpoint must not contain credentials",
        ),
        CoreError::PeerEndpointQueryNotAllowed => (
            "query_not_allowed",
            "peer endpoint must not contain a query string",
        ),
        CoreError::PeerEndpointFragmentNotAllowed => (
            "fragment_not_allowed",
            "peer endpoint must not contain a fragment",
        ),
        _ => ("invalid_url", "peer endpoint is invalid"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::{
        DedupConfig, NodeConfig, PeerEndpoint, RawDedupConfig, RawNodeConfig, RawSyncPolicy,
        RawTransportConfig, RawWalConfig, SyncPolicy, TailRepair, TransportConfig, WalConfig,
    };
    use crate::limits::{
        DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_RECORD_BYTES, WAL_FRAME_OVERHEAD_BYTES,
    };

    const CLUSTER: &str = "00000000-0000-0000-0000-000000000001";

    fn valid_raw() -> RawNodeConfig {
        RawNodeConfig {
            cluster_id: CLUSTER.to_owned(),
            node_id: "node-1".to_owned(),
            members: BTreeMap::from([
                ("node-1".to_owned(), "http://127.0.0.1:5001".to_owned()),
                (
                    "node-2".to_owned(),
                    "https://node-2.example:5001".to_owned(),
                ),
            ]),
            wal: RawWalConfig {
                directory: PathBuf::from("data/wal"),
                max_record_bytes: u64::from(DEFAULT_MAX_RECORD_BYTES),
                sync_policy: RawSyncPolicy::Always,
                tail_repair: TailRepair::Reject,
            },
            transport: RawTransportConfig {
                listen_address: "127.0.0.1:5001".to_owned(),
                advertised_url: "http://127.0.0.1:5001".to_owned(),
                request_timeout_ms: 1_000,
                connect_timeout_ms: 500,
                max_message_bytes: u64::from(DEFAULT_MAX_MESSAGE_BYTES),
            },
            dedup: RawDedupConfig::default(),
        }
    }

    #[test]
    fn valid_node_config_exposes_only_validated_values() {
        let config = NodeConfig::try_from(valid_raw()).expect("valid config");
        assert_eq!(config.node_id().as_str(), "node-1");
        assert_eq!(config.members().len(), 2);
        assert_eq!(config.transport().listen_address().port(), 5001);
        assert_eq!(config.wal().sync_policy(), SyncPolicy::Always);
    }

    #[test]
    fn standalone_subconfig_conversions_are_available() {
        let raw = valid_raw();
        assert!(WalConfig::try_from(raw.wal).is_ok());
        assert!(TransportConfig::try_from(raw.transport).is_ok());
        assert!(DedupConfig::try_from(raw.dedup).is_ok());
    }

    #[test]
    fn peer_endpoint_rules_are_exact() {
        assert!(PeerEndpoint::parse("http://host.example/path").is_ok());
        assert!(PeerEndpoint::parse("https://127.0.0.1:443").is_ok());
        for rejected in [
            "ftp://host.example",
            "http://user@host.example",
            "http://host.example?q=1",
            "http://host.example#fragment",
            "not a url",
        ] {
            assert!(
                PeerEndpoint::parse(rejected).is_err(),
                "accepted {rejected:?}"
            );
        }
    }

    #[test]
    fn validation_aggregates_and_sorts_independent_failures() {
        let mut raw = valid_raw();
        raw.cluster_id = "not-a-uuid".to_owned();
        raw.node_id = "BAD".to_owned();
        raw.members.clear();
        raw.wal.directory = PathBuf::new();
        raw.wal.max_record_bytes = 0;
        raw.transport.listen_address = "localhost:5".to_owned();
        raw.transport.request_timeout_ms = 0;
        raw.transport.connect_timeout_ms = 300_001;
        raw.dedup.max_entries = 0;
        raw.dedup.max_total_outcome_bytes = 1;

        let errors = NodeConfig::try_from(raw).expect_err("invalid config");
        assert!(errors.len() >= 8);
        let sorted: Vec<_> = errors
            .iter()
            .map(|error| (error.field_path(), error.code()))
            .collect();
        let mut independently_sorted = sorted.clone();
        independently_sorted.sort_unstable();
        assert_eq!(sorted, independently_sorted);
    }

    #[test]
    fn duplicate_canonical_endpoints_and_missing_local_member_are_rejected() {
        let mut raw = valid_raw();
        raw.members = BTreeMap::from([
            ("node-2".to_owned(), "HTTP://EXAMPLE.COM".to_owned()),
            ("node-3".to_owned(), "http://example.com/".to_owned()),
        ]);
        let errors = NodeConfig::try_from(raw).expect_err("invalid membership");
        assert!(
            errors
                .iter()
                .any(|error| error.code() == "duplicate_endpoint")
        );
        assert!(
            errors
                .iter()
                .any(|error| error.code() == "missing_local_node")
        );
    }

    #[test]
    fn cross_config_byte_relationships_are_enforced() {
        let mut raw = valid_raw();
        raw.wal.max_record_bytes = 1_024;
        raw.transport.max_message_bytes = 1_024 - u64::from(WAL_FRAME_OVERHEAD_BYTES) + 1;
        raw.dedup.max_outcome_bytes = raw.transport.max_message_bytes + 1;
        raw.dedup.max_total_outcome_bytes = 65_536;
        let errors = NodeConfig::try_from(raw).expect_err("invalid byte relationships");
        assert!(
            errors
                .iter()
                .any(|error| error.code() == "exceeds_wal_record_limit")
        );
        assert!(
            errors
                .iter()
                .any(|error| error.code() == "exceeds_message_limit")
        );
    }

    #[test]
    fn batch_sync_count_must_be_nonzero() {
        let mut raw = valid_raw().wal;
        raw.sync_policy = RawSyncPolicy::Batch {
            max_unsynced_records: 0,
        };
        assert!(WalConfig::try_from(raw).is_err());
        assert!(SyncPolicy::batch(1).is_ok());
    }
}
