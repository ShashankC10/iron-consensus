use std::fmt;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::error::CoreError;
use crate::id::{
    ClusterId, CorrelationId, EnvelopeVersion, IdGenerator, MessageId, MessageType, NodeId,
    ProtocolName, SchemaVersion,
};
use crate::limits::MaxMessageBytes;

pub const FINGERPRINT_FORMAT_VERSION: u16 = 1;
const FINGERPRINT_MAGIC: &[u8; 4] = b"ICFP";

/// The complete input to the validated version-1 envelope constructor.
///
/// This is an input value, not a validated envelope. In particular, its
/// payload has not yet been checked against a configured message limit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvelopeFields {
    cluster_id: ClusterId,
    source: NodeId,
    destination: NodeId,
    message_id: MessageId,
    correlation_id: Option<CorrelationId>,
    protocol: ProtocolName,
    message_type: MessageType,
    schema_version: SchemaVersion,
    delivery_attempt: u32,
    payload: Vec<u8>,
}

impl EnvelopeFields {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        cluster_id: ClusterId,
        source: NodeId,
        destination: NodeId,
        message_id: MessageId,
        correlation_id: Option<CorrelationId>,
        protocol: ProtocolName,
        message_type: MessageType,
        schema_version: SchemaVersion,
        delivery_attempt: u32,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            cluster_id,
            source,
            destination,
            message_id,
            correlation_id,
            protocol,
            message_type,
            schema_version,
            delivery_attempt,
            payload: payload.into(),
        }
    }

    /// Creates fields for a first delivery attempt.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn initial(
        cluster_id: ClusterId,
        source: NodeId,
        destination: NodeId,
        message_id: MessageId,
        correlation_id: Option<CorrelationId>,
        protocol: ProtocolName,
        message_type: MessageType,
        schema_version: SchemaVersion,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self::new(
            cluster_id,
            source,
            destination,
            message_id,
            correlation_id,
            protocol,
            message_type,
            schema_version,
            0,
            payload,
        )
    }
}

/// A validated version-1 protocol-neutral envelope.
///
/// Fields are immutable. The only supported mutation-like operation is
/// `Envelope::retry`, which returns a new value after incrementing only the
/// delivery attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvelopeV1 {
    cluster_id: ClusterId,
    source: NodeId,
    destination: NodeId,
    message_id: MessageId,
    correlation_id: Option<CorrelationId>,
    protocol: ProtocolName,
    message_type: MessageType,
    schema_version: SchemaVersion,
    delivery_attempt: u32,
    payload: Box<[u8]>,
}

impl EnvelopeV1 {
    fn from_fields(
        fields: EnvelopeFields,
        max_message_bytes: MaxMessageBytes,
    ) -> Result<Self, CoreError> {
        let payload_length = u64::try_from(fields.payload.len()).unwrap_or(u64::MAX);
        if payload_length > u64::from(max_message_bytes.get()) {
            return Err(CoreError::PayloadTooLarge {
                actual: payload_length,
                maximum: max_message_bytes.get(),
            });
        }
        Ok(Self {
            cluster_id: fields.cluster_id,
            source: fields.source,
            destination: fields.destination,
            message_id: fields.message_id,
            correlation_id: fields.correlation_id,
            protocol: fields.protocol,
            message_type: fields.message_type,
            schema_version: fields.schema_version,
            delivery_attempt: fields.delivery_attempt,
            payload: fields.payload.into_boxed_slice(),
        })
    }

    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    #[must_use]
    pub fn source(&self) -> &NodeId {
        &self.source
    }

    #[must_use]
    pub fn destination(&self) -> &NodeId {
        &self.destination
    }

    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.message_id
    }

    #[must_use]
    pub const fn correlation_id(&self) -> Option<CorrelationId> {
        self.correlation_id
    }

    #[must_use]
    pub fn protocol(&self) -> &ProtocolName {
        &self.protocol
    }

    #[must_use]
    pub fn message_type(&self) -> &MessageType {
        &self.message_type
    }

    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    #[must_use]
    pub const fn delivery_attempt(&self) -> u32 {
        self.delivery_attempt
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn semantic_fingerprint(&self) -> SemanticFingerprint {
        let mut digest = Sha256::new();
        digest.update(FINGERPRINT_MAGIC);
        digest.update(FINGERPRINT_FORMAT_VERSION.to_be_bytes());
        update_field(&mut digest, self.cluster_id.as_bytes());
        update_field(&mut digest, self.source.as_str().as_bytes());
        update_field(&mut digest, self.destination.as_str().as_bytes());
        update_field(&mut digest, self.message_id.as_bytes());
        match self.correlation_id {
            Some(correlation_id) => update_field(&mut digest, correlation_id.as_bytes()),
            None => update_field(&mut digest, &[]),
        }
        update_field(&mut digest, self.protocol.as_str().as_bytes());
        update_field(&mut digest, self.message_type.as_str().as_bytes());
        update_field(&mut digest, &self.schema_version.get().to_be_bytes());
        update_field(&mut digest, &self.payload);
        SemanticFingerprint(digest.finalize().into())
    }
}

fn update_field(digest: &mut Sha256, field: &[u8]) {
    let length = u32::try_from(field.len())
        .expect("validated envelope fingerprint fields fit in a u32 length prefix");
    digest.update(length.to_be_bytes());
    digest.update(field);
}

/// A closed, versioned envelope. Unknown versions cannot inhabit this enum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Envelope {
    V1(EnvelopeV1),
}

impl Envelope {
    /// Constructs the only envelope version supported by Layer 1.
    pub fn new_v1(
        fields: EnvelopeFields,
        max_message_bytes: MaxMessageBytes,
    ) -> Result<Self, CoreError> {
        Ok(Self::V1(EnvelopeV1::from_fields(
            fields,
            max_message_bytes,
        )?))
    }

    /// Converts a raw wire version through the validated constructor.
    ///
    /// Version zero and every version other than one return
    /// `CoreError::UnsupportedEnvelopeVersion`; they are never guessed or
    /// downgraded.
    pub fn try_from_version(
        version: u16,
        fields: EnvelopeFields,
        max_message_bytes: MaxMessageBytes,
    ) -> Result<Self, CoreError> {
        if version != EnvelopeVersion::V1.get() {
            return Err(CoreError::UnsupportedEnvelopeVersion { version });
        }
        Self::new_v1(fields, max_message_bytes)
    }

    /// Builds a response with swapped routing, a generated message ID, a
    /// correlation ID equal to the request message ID, and attempt zero.
    pub fn response_v1<G>(
        request: &Self,
        generator: &mut G,
        message_type: MessageType,
        schema_version: SchemaVersion,
        payload: impl Into<Vec<u8>>,
        max_message_bytes: MaxMessageBytes,
    ) -> Result<Self, CoreError>
    where
        G: IdGenerator + ?Sized,
    {
        let message_id = MessageId::generate(generator)?;
        let fields = EnvelopeFields::initial(
            request.cluster_id(),
            request.destination().clone(),
            request.source().clone(),
            message_id,
            Some(CorrelationId::from(request.message_id())),
            request.protocol().clone(),
            message_type,
            schema_version,
            payload,
        );
        Self::new_v1(fields, max_message_bytes)
    }

    /// Returns a retry that preserves every other semantic field and payload.
    pub fn retry(&self) -> Result<Self, CoreError> {
        let Self::V1(envelope) = self;
        let delivery_attempt = envelope
            .delivery_attempt
            .checked_add(1)
            .ok_or(CoreError::DeliveryAttemptOverflow)?;
        let mut retried = envelope.clone();
        retried.delivery_attempt = delivery_attempt;
        Ok(Self::V1(retried))
    }

    #[must_use]
    pub const fn version(&self) -> EnvelopeVersion {
        match self {
            Self::V1(_) => EnvelopeVersion::V1,
        }
    }

    #[must_use]
    pub const fn as_v1(&self) -> &EnvelopeV1 {
        match self {
            Self::V1(envelope) => envelope,
        }
    }

    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.as_v1().cluster_id()
    }

    #[must_use]
    pub fn source(&self) -> &NodeId {
        self.as_v1().source()
    }

    #[must_use]
    pub fn destination(&self) -> &NodeId {
        self.as_v1().destination()
    }

    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.as_v1().message_id()
    }

    #[must_use]
    pub const fn correlation_id(&self) -> Option<CorrelationId> {
        self.as_v1().correlation_id()
    }

    #[must_use]
    pub fn protocol(&self) -> &ProtocolName {
        self.as_v1().protocol()
    }

    #[must_use]
    pub fn message_type(&self) -> &MessageType {
        self.as_v1().message_type()
    }

    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.as_v1().schema_version()
    }

    #[must_use]
    pub const fn delivery_attempt(&self) -> u32 {
        self.as_v1().delivery_attempt()
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.as_v1().payload()
    }

    /// Computes the stable semantic fingerprint.
    ///
    /// Fingerprint format 1 is SHA-256 over `ICFP`, the big-endian `u16`
    /// fingerprint version, then the following fields in order. Every field is
    /// encoded as a big-endian `u32` byte length followed by its bytes:
    /// cluster ID (raw 16 bytes), source UTF-8, destination UTF-8, message ID
    /// (raw 16 bytes), correlation ID (raw 16 bytes or zero-length when absent),
    /// protocol UTF-8, message type UTF-8, schema version (big-endian `u16`),
    /// and payload. Envelope version and delivery attempt are deliberately
    /// excluded. This encoding is independent of Serde and transport formats.
    #[must_use]
    pub fn semantic_fingerprint(&self) -> SemanticFingerprint {
        self.as_v1().semantic_fingerprint()
    }
}

/// A 32-byte SHA-256 semantic fingerprint.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SemanticFingerprint([u8; 32]);

impl SemanticFingerprint {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn parse(input: &str) -> Result<Self, CoreError> {
        if input.len() != 64 {
            return Err(CoreError::InvalidFingerprint {
                reason: "must contain exactly 64 lowercase hexadecimal characters",
            });
        }
        let mut bytes = [0_u8; 32];
        for (index, output) in bytes.iter_mut().enumerate() {
            let offset = index * 2;
            let high = decode_lower_hex(input.as_bytes()[offset])?;
            let low = decode_lower_hex(input.as_bytes()[offset + 1])?;
            *output = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

fn decode_lower_hex(byte: u8) -> Result<u8, CoreError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(CoreError::InvalidFingerprint {
            reason: "must contain exactly 64 lowercase hexadecimal characters",
        }),
    }
}

impl fmt::Display for SemanticFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for SemanticFingerprint {
    type Err = CoreError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl Serialize for SemanticFingerprint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for SemanticFingerprint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let input = String::deserialize(deserializer)?;
            Self::parse(&input).map_err(D::Error::custom)
        } else {
            Ok(Self(<[u8; 32]>::deserialize(deserializer)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Envelope, EnvelopeFields, SemanticFingerprint};
    use crate::error::CoreError;
    use crate::id::{
        ClusterId, CorrelationId, IdGenerator, MessageId, MessageType, NodeId, ProtocolName,
        SchemaVersion,
    };
    use crate::limits::MaxMessageBytes;

    fn fixture(attempt: u32) -> Envelope {
        Envelope::new_v1(
            EnvelopeFields::new(
                ClusterId::from_bytes({
                    let mut bytes = [0; 16];
                    bytes[15] = 1;
                    bytes
                })
                .expect("nonzero"),
                NodeId::parse("node-a").expect("valid node"),
                NodeId::parse("node-b").expect("valid node"),
                MessageId::from_bytes({
                    let mut bytes = [0; 16];
                    bytes[15] = 2;
                    bytes
                })
                .expect("nonzero"),
                None,
                ProtocolName::parse("raft").expect("valid protocol"),
                MessageType::parse("append.entries").expect("valid type"),
                SchemaVersion::new(7).expect("nonzero"),
                attempt,
                vec![0, 1, 2, 255],
            ),
            MaxMessageBytes::new(4).expect("valid limit"),
        )
        .expect("valid envelope")
    }

    #[test]
    fn unknown_versions_fail_closed() {
        let envelope = fixture(0);
        let v1 = envelope.as_v1();
        let fields = EnvelopeFields::new(
            v1.cluster_id(),
            v1.source().clone(),
            v1.destination().clone(),
            v1.message_id(),
            v1.correlation_id(),
            v1.protocol().clone(),
            v1.message_type().clone(),
            v1.schema_version(),
            v1.delivery_attempt(),
            v1.payload().to_vec(),
        );
        assert!(matches!(
            Envelope::try_from_version(2, fields, MaxMessageBytes::new(4).expect("valid limit")),
            Err(CoreError::UnsupportedEnvelopeVersion { version: 2 })
        ));
    }

    #[test]
    fn payload_limit_is_checked_at_the_constructor() {
        let envelope = fixture(0);
        let v1 = envelope.as_v1();
        let fields = EnvelopeFields::new(
            v1.cluster_id(),
            v1.source().clone(),
            v1.destination().clone(),
            v1.message_id(),
            None,
            v1.protocol().clone(),
            v1.message_type().clone(),
            v1.schema_version(),
            0,
            vec![0; 5],
        );
        assert!(matches!(
            Envelope::new_v1(fields, MaxMessageBytes::new(4).expect("valid limit")),
            Err(CoreError::PayloadTooLarge {
                actual: 5,
                maximum: 4
            })
        ));
    }

    #[test]
    fn retry_changes_only_attempt_and_preserves_fingerprint() {
        let original = fixture(0);
        let retried = original.retry().expect("attempt can increment");
        assert_eq!(retried.delivery_attempt(), 1);
        assert_eq!(original.message_id(), retried.message_id());
        assert_eq!(original.payload(), retried.payload());
        assert_eq!(
            original.semantic_fingerprint(),
            retried.semantic_fingerprint()
        );
    }

    #[test]
    fn retry_overflow_is_reported_without_wrapping() {
        assert!(matches!(
            fixture(u32::MAX).retry(),
            Err(CoreError::DeliveryAttemptOverflow)
        ));
    }

    #[test]
    fn semantic_fingerprint_has_a_golden_vector() {
        let fingerprint = fixture(0).semantic_fingerprint();
        assert_eq!(
            fingerprint.to_string(),
            "adaa94b68769313d69af1e3ae1beaf7d210d054534a6c7dfdecd1a7f39e232d0"
        );
        assert_eq!(
            SemanticFingerprint::parse(&fingerprint.to_string()).expect("round trip"),
            fingerprint
        );

        let base = fixture(99);
        let mut correlation_bytes = [0; 16];
        correlation_bytes[15] = 3;
        let correlated = Envelope::new_v1(
            EnvelopeFields::new(
                base.cluster_id(),
                base.source().clone(),
                base.destination().clone(),
                base.message_id(),
                Some(CorrelationId::from_bytes(correlation_bytes).expect("nonzero correlation ID")),
                base.protocol().clone(),
                base.message_type().clone(),
                base.schema_version(),
                99,
                base.payload().to_vec(),
            ),
            MaxMessageBytes::new(4).expect("valid limit"),
        )
        .expect("valid correlated envelope");
        assert_eq!(
            correlated.semantic_fingerprint().to_string(),
            "5651449a11b2ac060037e4a496021f7d57bfa01a1e5c1c6a8055a602514aae47"
        );
    }

    #[test]
    fn response_uses_fresh_generated_id_and_request_correlation() {
        struct Fixed([u8; 16]);
        impl IdGenerator for Fixed {
            fn next_id(&mut self) -> [u8; 16] {
                self.0
            }
        }

        let request = fixture(3);
        let mut generated = [0; 16];
        generated[15] = 9;
        let response = Envelope::response_v1(
            &request,
            &mut Fixed(generated),
            MessageType::parse("append.response").expect("valid type"),
            SchemaVersion::new(1).expect("nonzero"),
            vec![1],
            MaxMessageBytes::new(4).expect("valid limit"),
        )
        .expect("valid response");

        assert_eq!(response.message_id().to_bytes(), generated);
        assert_eq!(
            response.correlation_id().map(|id| id.to_bytes()),
            Some(request.message_id().to_bytes())
        );
        assert_eq!(response.source(), request.destination());
        assert_eq!(response.destination(), request.source());
        assert_eq!(response.delivery_attempt(), 0);
    }
}
