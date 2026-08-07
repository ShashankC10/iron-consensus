#![forbid(unsafe_code)]

//! Protobuf v1 wire adapter. Generated transport services are deferred to the
//! process layer; protocol code only sees validated [`iron_core::Envelope`]s.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use iron_core::{
    ClusterId, CorrelationId, Envelope, EnvelopeFields, MaxMessageBytes, MessageId, MessageType,
    NodeId, ProtocolName, SchemaVersion,
};
use prost::Message;
use thiserror::Error;
use tonic::{Request, Response, Status};

/// Generated gRPC service/client surface. Implementations must convert the
/// generated envelope through [`WireEnvelopeV1::into_core`] before dispatch.
pub mod proto {
    tonic::include_proto!("iron.v1");
}

/// Stable protobuf representation of the protocol-neutral envelope.
#[derive(Clone, PartialEq, Message)]
pub struct WireEnvelopeV1 {
    #[prost(uint32, tag = "1")]
    pub envelope_version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub cluster_id: Vec<u8>,
    #[prost(string, tag = "3")]
    pub source_node_id: String,
    #[prost(string, tag = "4")]
    pub destination_node_id: String,
    #[prost(bytes = "vec", tag = "5")]
    pub message_id: Vec<u8>,
    #[prost(bytes = "vec", optional, tag = "6")]
    pub correlation_id: Option<Vec<u8>>,
    #[prost(string, tag = "7")]
    pub protocol: String,
    #[prost(string, tag = "8")]
    pub message_type: String,
    #[prost(uint32, tag = "9")]
    pub schema_version: u32,
    #[prost(uint32, tag = "10")]
    pub delivery_attempt: u32,
    #[prost(bytes = "vec", tag = "11")]
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum GrpcError {
    #[error("invalid protobuf envelope field `{field}`")]
    InvalidField { field: &'static str },
    #[error(transparent)]
    Core(#[from] iron_core::CoreError),
}

/// Application callback used by the generated gRPC service.
#[async_trait]
pub trait EnvelopeHandler: Send + Sync + 'static {
    async fn deliver(&self, envelope: Envelope) -> Result<(), GrpcError>;
}

/// Generated-service implementation that validates the wire envelope before
/// invoking application code.
#[derive(Clone)]
pub struct NodeTransportService<H> {
    handler: Arc<H>,
    max_message_bytes: MaxMessageBytes,
}

impl<H> NodeTransportService<H> {
    #[must_use]
    pub fn new(handler: H, max_message_bytes: MaxMessageBytes) -> Self {
        Self {
            handler: Arc::new(handler),
            max_message_bytes,
        }
    }
}

#[tonic::async_trait]
impl<H> proto::node_transport_server::NodeTransport for NodeTransportService<H>
where
    H: EnvelopeHandler,
{
    async fn deliver(
        &self,
        request: Request<proto::Envelope>,
    ) -> Result<Response<proto::DeliveryAck>, Status> {
        let wire = request.into_inner();
        let envelope = WireEnvelopeV1 {
            envelope_version: wire.envelope_version,
            cluster_id: wire.cluster_id,
            source_node_id: wire.source_node_id,
            destination_node_id: wire.destination_node_id,
            message_id: wire.message_id,
            correlation_id: wire.correlation_id,
            protocol: wire.protocol,
            message_type: wire.message_type,
            schema_version: wire.schema_version,
            delivery_attempt: wire.delivery_attempt,
            payload: wire.payload,
        }
        .into_core(self.max_message_bytes)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.handler
            .deliver(envelope)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(proto::DeliveryAck { accepted: true }))
    }
}

/// Serves the generated NodeTransport service over HTTP/2.
pub async fn serve<H>(
    address: SocketAddr,
    handler: H,
    max_message_bytes: MaxMessageBytes,
) -> Result<(), tonic::transport::Error>
where
    H: EnvelopeHandler,
{
    tonic::transport::Server::builder()
        .add_service(proto::node_transport_server::NodeTransportServer::new(
            NodeTransportService::new(handler, max_message_bytes),
        ))
        .serve(address)
        .await
}

impl WireEnvelopeV1 {
    pub fn from_core(envelope: &Envelope) -> Self {
        Self {
            envelope_version: u32::from(envelope.version().get()),
            cluster_id: envelope.cluster_id().to_bytes().to_vec(),
            source_node_id: envelope.source().to_string(),
            destination_node_id: envelope.destination().to_string(),
            message_id: envelope.message_id().to_bytes().to_vec(),
            correlation_id: envelope.correlation_id().map(|id| id.to_bytes().to_vec()),
            protocol: envelope.protocol().to_string(),
            message_type: envelope.message_type().to_string(),
            schema_version: u32::from(envelope.schema_version().get()),
            delivery_attempt: envelope.delivery_attempt(),
            payload: envelope.payload().to_vec(),
        }
    }

    pub fn into_core(self, max_message_bytes: MaxMessageBytes) -> Result<Envelope, GrpcError> {
        let bytes16 = |value: &[u8], field: &'static str| -> Result<[u8; 16], GrpcError> {
            value
                .try_into()
                .map_err(|_| GrpcError::InvalidField { field })
        };
        let cluster_id = ClusterId::from_bytes(bytes16(&self.cluster_id, "cluster_id")?)?;
        let message_id = MessageId::from_bytes(bytes16(&self.message_id, "message_id")?)?;
        let correlation_id = match self.correlation_id.as_deref() {
            Some(bytes) => Some(CorrelationId::from_bytes(bytes16(
                bytes,
                "correlation_id",
            )?)?),
            None => None,
        };
        let source = NodeId::parse(&self.source_node_id)?;
        let destination = NodeId::parse(&self.destination_node_id)?;
        let protocol = ProtocolName::parse(&self.protocol)?;
        let message_type = MessageType::parse(&self.message_type)?;
        let schema_version =
            SchemaVersion::try_from(u16::try_from(self.schema_version).map_err(|_| {
                GrpcError::InvalidField {
                    field: "schema_version",
                }
            })?)?;
        let version =
            u16::try_from(self.envelope_version).map_err(|_| GrpcError::InvalidField {
                field: "envelope_version",
            })?;
        Ok(Envelope::try_from_version(
            version,
            EnvelopeFields::new(
                cluster_id,
                source,
                destination,
                message_id,
                correlation_id,
                protocol,
                message_type,
                schema_version,
                self.delivery_attempt,
                self.payload,
            ),
            max_message_bytes,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn wire_round_trip_preserves_semantics() {
        let cluster = ClusterId::from_bytes([1; 16]).expect("nonzero");
        let message = MessageId::from_bytes([2; 16]).expect("nonzero");
        let envelope = Envelope::new_v1(
            EnvelopeFields::initial(
                cluster,
                NodeId::parse("a").expect("valid"),
                NodeId::parse("b").expect("valid"),
                message,
                None,
                ProtocolName::parse("raft").expect("valid"),
                MessageType::parse("append").expect("valid"),
                SchemaVersion::new(1).expect("valid"),
                b"hello".to_vec(),
            ),
            MaxMessageBytes::new(1024).expect("valid"),
        )
        .expect("valid envelope");
        let wire = WireEnvelopeV1::from_core(&envelope);
        let decoded = WireEnvelopeV1::decode(wire.encode_to_vec().as_slice()).expect("protobuf");
        let restored = decoded
            .into_core(MaxMessageBytes::new(1024).expect("valid"))
            .expect("core");
        assert_eq!(restored, envelope);
    }
}
