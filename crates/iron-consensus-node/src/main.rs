use async_trait::async_trait;
use iron_core::{DEFAULT_MAX_MESSAGE_BYTES, Envelope, MaxMessageBytes, NodeId};
use iron_grpc::{EnvelopeHandler, GrpcError};
use iron_raft::RaftNode;
use std::sync::Mutex;

struct ConsensusHandler {
    state: Mutex<RaftNode>,
}

#[async_trait]
impl EnvelopeHandler for ConsensusHandler {
    async fn deliver(&self, envelope: Envelope) -> Result<(), GrpcError> {
        let mut state = self.state.lock().map_err(|_| GrpcError::InvalidField {
            field: "raft_state",
        })?;
        match envelope.message_type().as_str() {
            "tick" => state
                .tick()
                .map(|_| ())
                .map_err(|_| GrpcError::InvalidField {
                    field: "message_type",
                }),
            "propose" => state
                .propose(envelope.payload().to_vec())
                .map(|_| ())
                .map_err(|_| GrpcError::InvalidField {
                    field: "message_type",
                }),
            _ => Err(GrpcError::InvalidField {
                field: "message_type",
            }),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("IRON_LISTEN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7003".to_owned())
        .parse()?;
    let id = NodeId::parse(&std::env::var("IRON_NODE_ID").unwrap_or_else(|_| "node-1".to_owned()))?;
    let members = std::env::var("IRON_MEMBERS").unwrap_or_else(|_| id.as_str().to_owned());
    let members = members
        .split(',')
        .map(|name| NodeId::parse(name.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    let state = RaftNode::new(id, members, 5)?;
    iron_grpc::serve(
        address,
        ConsensusHandler {
            state: Mutex::new(state),
        },
        MaxMessageBytes::new(u64::from(DEFAULT_MAX_MESSAGE_BYTES))?,
    )
    .await?;
    Ok(())
}
