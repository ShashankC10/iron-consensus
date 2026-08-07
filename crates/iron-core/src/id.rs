use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

use serde::de::{Error as DeError, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::error::CoreError;

const OPAQUE_ID_BYTES: usize = 16;
const MAX_TEXT_ID_BYTES: usize = 64;

/// Supplies opaque identifier bytes without introducing ambient randomness or
/// time into core logic.
///
/// Implementations are expected to return a distinct value on each call. Core
/// still rejects the all-zero value. Production UUIDv7 generation belongs in
/// the runtime; deterministic tests can use a counter-based implementation.
pub trait IdGenerator {
    fn next_id(&mut self) -> [u8; OPAQUE_ID_BYTES];
}

struct FixedBytesVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for FixedBytesVisitor<N> {
    type Value = [u8; N];

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "exactly {N} bytes")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut bytes = [0_u8; N];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = sequence
                .next_element()?
                .ok_or_else(|| A::Error::invalid_length(index, &self))?;
        }
        Ok(bytes)
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        value
            .try_into()
            .map_err(|_| E::invalid_length(value.len(), &self))
    }

    fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        self.visit_bytes(value)
    }
}

fn serialize_fixed_bytes<S, const N: usize>(
    bytes: &[u8; N],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut tuple = serializer.serialize_tuple(N)?;
    for byte in bytes {
        tuple.serialize_element(byte)?;
    }
    tuple.end()
}

fn deserialize_fixed_bytes<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_tuple(N, FixedBytesVisitor::<N>)
}

macro_rules! opaque_id {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("A validated nonzero 16-byte ", $kind, ".")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; OPAQUE_ID_BYTES]);

        impl $name {
            pub fn parse(input: &str) -> Result<Self, CoreError> {
                let uuid = Uuid::parse_str(input).map_err(|source| CoreError::InvalidUuid {
                    kind: $kind,
                    source,
                })?;
                Self::from_bytes(*uuid.as_bytes())
            }

            pub fn from_bytes(bytes: [u8; OPAQUE_ID_BYTES]) -> Result<Self, CoreError> {
                if bytes == [0_u8; OPAQUE_ID_BYTES] {
                    Err(CoreError::ZeroIdentifier { kind: $kind })
                } else {
                    Ok(Self(bytes))
                }
            }

            pub fn generate<G>(generator: &mut G) -> Result<Self, CoreError>
            where
                G: IdGenerator + ?Sized,
            {
                let bytes = generator.next_id();
                if bytes == [0_u8; OPAQUE_ID_BYTES] {
                    Err(CoreError::GeneratedZeroIdentifier { kind: $kind })
                } else {
                    Ok(Self(bytes))
                }
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; OPAQUE_ID_BYTES] {
                &self.0
            }

            #[must_use]
            pub const fn to_bytes(self) -> [u8; OPAQUE_ID_BYTES] {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", Uuid::from_bytes(self.0).hyphenated())
            }
        }

        impl FromStr for $name {
            type Err = CoreError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }

        impl TryFrom<[u8; OPAQUE_ID_BYTES]> for $name {
            type Error = CoreError;

            fn try_from(bytes: [u8; OPAQUE_ID_BYTES]) -> Result<Self, Self::Error> {
                Self::from_bytes(bytes)
            }
        }

        impl From<$name> for [u8; OPAQUE_ID_BYTES] {
            fn from(value: $name) -> Self {
                value.to_bytes()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                if serializer.is_human_readable() {
                    serializer.serialize_str(&self.to_string())
                } else {
                    serialize_fixed_bytes(&self.0, serializer)
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                if deserializer.is_human_readable() {
                    let input = String::deserialize(deserializer)?;
                    Self::parse(&input).map_err(D::Error::custom)
                } else {
                    let bytes = deserialize_fixed_bytes(deserializer)?;
                    Self::from_bytes(bytes).map_err(D::Error::custom)
                }
            }
        }
    };
}

opaque_id!(ClusterId, "cluster ID");
opaque_id!(MessageId, "message ID");
opaque_id!(CorrelationId, "correlation ID");
opaque_id!(ClientRequestId, "client request ID");
opaque_id!(TimerId, "timer ID");

impl From<MessageId> for CorrelationId {
    fn from(message_id: MessageId) -> Self {
        Self(message_id.to_bytes())
    }
}

/// A validated node identifier.
///
/// Node IDs contain 1 through 64 bytes. They use lowercase ASCII letters,
/// digits, and `-`, and begin and end with an alphanumeric byte.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(Box<str>);

impl NodeId {
    pub fn parse(input: &str) -> Result<Self, CoreError> {
        validate_node_id(input)?;
        Ok(Self(input.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn is_lower_alphanumeric(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

fn validate_node_id(input: &str) -> Result<(), CoreError> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Err(CoreError::InvalidText {
            kind: "node ID",
            reason: "must contain at least one byte",
        });
    }
    if bytes.len() > MAX_TEXT_ID_BYTES {
        return Err(CoreError::InvalidText {
            kind: "node ID",
            reason: "must contain at most 64 bytes",
        });
    }
    if !bytes
        .iter()
        .copied()
        .all(|byte| is_lower_alphanumeric(byte) || byte == b'-')
    {
        return Err(CoreError::InvalidText {
            kind: "node ID",
            reason: "contains a disallowed byte",
        });
    }
    if !is_lower_alphanumeric(bytes[0]) || !is_lower_alphanumeric(bytes[bytes.len() - 1]) {
        return Err(CoreError::InvalidText {
            kind: "node ID",
            reason: "must begin and end with a lowercase letter or digit",
        });
    }
    Ok(())
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for NodeId {
    type Err = CoreError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl TryFrom<String> for NodeId {
    type Error = CoreError;

    fn try_from(input: String) -> Result<Self, Self::Error> {
        Self::parse(&input)
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        Self::parse(&input).map_err(D::Error::custom)
    }
}

macro_rules! validated_name {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("A validated ", $kind, ".")]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            pub fn parse(input: &str) -> Result<Self, CoreError> {
                let bytes = input.as_bytes();
                if bytes.is_empty() {
                    return Err(CoreError::InvalidText {
                        kind: $kind,
                        reason: "must contain at least one byte",
                    });
                }
                if bytes.len() > MAX_TEXT_ID_BYTES {
                    return Err(CoreError::InvalidText {
                        kind: $kind,
                        reason: "must contain at most 64 bytes",
                    });
                }
                if !bytes
                    .iter()
                    .copied()
                    .all(|byte| is_lower_alphanumeric(byte) || matches!(byte, b'-' | b'_' | b'.'))
                {
                    return Err(CoreError::InvalidText {
                        kind: $kind,
                        reason: "contains a disallowed byte",
                    });
                }
                Ok(Self(input.into()))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = CoreError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }

        impl TryFrom<String> for $name {
            type Error = CoreError;

            fn try_from(input: String) -> Result<Self, Self::Error> {
                Self::parse(&input)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let input = String::deserialize(deserializer)?;
                Self::parse(&input).map_err(D::Error::custom)
            }
        }
    };
}

validated_name!(ProtocolName, "protocol name");
validated_name!(MessageType, "message type");

macro_rules! nonzero_integer {
    ($name:ident, $integer:ty, $kind:literal) => {
        #[doc = concat!("A validated nonzero ", $kind, ".")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name($integer);

        impl $name {
            pub const fn new(value: $integer) -> Result<Self, CoreError> {
                if value == 0 {
                    Err(CoreError::ZeroValue { kind: $kind })
                } else {
                    Ok(Self(value))
                }
            }

            pub fn parse(input: &str) -> Result<Self, CoreError> {
                let value =
                    input
                        .parse::<$integer>()
                        .map_err(|source| CoreError::InvalidInteger {
                            kind: $kind,
                            source,
                        })?;
                Self::new(value)
            }

            #[must_use]
            pub const fn get(self) -> $integer {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = CoreError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }

        impl TryFrom<$integer> for $name {
            type Error = CoreError;

            fn try_from(value: $integer) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for $integer {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = <$integer>::deserialize(deserializer)?;
                Self::new(value).map_err(D::Error::custom)
            }
        }
    };
}

nonzero_integer!(Lsn, u64, "LSN");
nonzero_integer!(SchemaVersion, u16, "schema version");
nonzero_integer!(EnvelopeVersion, u16, "envelope version");

impl Lsn {
    pub const FIRST: Self = Self(1);

    /// Returns the next LSN, or `None` rather than wrapping at `u64::MAX`.
    #[must_use]
    pub const fn checked_successor(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl EnvelopeVersion {
    pub const V1: Self = Self(1);
}

#[cfg(test)]
mod tests {
    use super::{
        ClientRequestId, ClusterId, CorrelationId, EnvelopeVersion, IdGenerator, Lsn, MessageId,
        MessageType, NodeId, ProtocolName, SchemaVersion, TimerId,
    };

    const UUID_TEXT: &str = "00000000-0000-0000-0000-000000000001";

    struct Counter(u128);

    impl IdGenerator for Counter {
        fn next_id(&mut self) -> [u8; 16] {
            self.0 += 1;
            self.0.to_be_bytes()
        }
    }

    #[test]
    fn opaque_ids_reject_zero_and_format_canonically() {
        macro_rules! check_id {
            ($id:ty) => {{
                assert!(<$id>::from_bytes([0; 16]).is_err());
                let id = <$id>::parse(UUID_TEXT).expect("valid fixture");
                assert_eq!(id.to_string(), UUID_TEXT);
                assert_eq!(id.to_bytes()[15], 1);
            }};
        }

        check_id!(ClusterId);
        check_id!(MessageId);
        check_id!(CorrelationId);
        check_id!(ClientRequestId);
        check_id!(TimerId);
    }

    #[test]
    fn generator_is_explicit_and_zero_is_checked() {
        let mut counter = Counter(0);
        assert_eq!(
            MessageId::generate(&mut counter)
                .expect("counter ID")
                .to_string(),
            UUID_TEXT
        );

        struct Zero;
        impl IdGenerator for Zero {
            fn next_id(&mut self) -> [u8; 16] {
                [0; 16]
            }
        }
        assert!(TimerId::generate(&mut Zero).is_err());
    }

    #[test]
    fn node_id_boundaries_and_alphabet_are_enforced() {
        assert!(NodeId::parse("a").is_ok());
        assert!(NodeId::parse(&"a".repeat(64)).is_ok());
        for rejected in ["", "-a", "a-", "A", "a_b", "a.b", "é"] {
            assert!(NodeId::parse(rejected).is_err(), "accepted {rejected:?}");
        }
        assert!(NodeId::parse(&"a".repeat(65)).is_err());
    }

    #[test]
    fn protocol_and_message_names_have_distinct_validated_types() {
        assert!(ProtocolName::parse("raft.v1_leader-election").is_ok());
        assert!(MessageType::parse("append.entries_1").is_ok());
        for rejected in ["", "Upper", "has/slash", "has space", "é"] {
            assert!(ProtocolName::parse(rejected).is_err());
            assert!(MessageType::parse(rejected).is_err());
        }
        assert!(ProtocolName::parse(&"a".repeat(65)).is_err());
    }

    #[test]
    fn numeric_ids_reject_zero_and_lsn_successor_is_checked() {
        assert!(Lsn::new(0).is_err());
        assert_eq!(Lsn::FIRST.get(), 1);
        assert_eq!(Lsn::FIRST.checked_successor().map(Lsn::get), Some(2));
        assert_eq!(
            Lsn::new(u64::MAX).expect("nonzero").checked_successor(),
            None
        );
        assert!(SchemaVersion::new(0).is_err());
        assert_eq!(EnvelopeVersion::V1.get(), 1);
    }
}
