use async_trait::async_trait;
use iron_2pc::Coordinator;
use iron_core::{DEFAULT_MAX_MESSAGE_BYTES, Envelope, MaxMessageBytes, NodeId};
use iron_grpc::{EnvelopeHandler, GrpcError};
use std::sync::Mutex;

struct CoordinatorHandler {
    state: Mutex<Coordinator>,
}

impl CoordinatorHandler {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let names = std::env::var("IRON_PARTICIPANTS").unwrap_or_else(|_| "participant".to_owned());
        let participants = names
            .split(',')
            .map(|name| NodeId::parse(name.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            state: Mutex::new(Coordinator::new(participants)?),
        })
    }
}

#[async_trait]
impl EnvelopeHandler for CoordinatorHandler {
    async fn deliver(&self, envelope: Envelope) -> Result<(), GrpcError> {
        let mut state = self.state.lock().map_err(|_| GrpcError::InvalidField {
            field: "coordinator_state",
        })?;
        match envelope.message_type().as_str() {
            "begin" => state
                .begin()
                .map(|_| ())
                .map_err(|_| GrpcError::InvalidField {
                    field: "message_type",
                }),
            "prepared" => {
                let name = std::str::from_utf8(envelope.payload())
                    .map_err(|_| GrpcError::InvalidField { field: "payload" })?;
                let participant = NodeId::parse(name.trim())
                    .map_err(|_| GrpcError::InvalidField { field: "payload" })?;
                state
                    .vote_prepared(&participant)
                    .map(|_| ())
                    .map_err(|_| GrpcError::InvalidField { field: "payload" })
            }
            "abort" => {
                let name = std::str::from_utf8(envelope.payload())
                    .map_err(|_| GrpcError::InvalidField { field: "payload" })?;
                let participant = NodeId::parse(name.trim())
                    .map_err(|_| GrpcError::InvalidField { field: "payload" })?;
                state
                    .vote_abort(&participant)
                    .map(|_| ())
                    .map_err(|_| GrpcError::InvalidField { field: "payload" })
            }
            _ => Err(GrpcError::InvalidField {
                field: "message_type",
            }),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("IRON_LISTEN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7001".to_owned())
        .parse()?;
    iron_grpc::serve(
        address,
        CoordinatorHandler::new()?,
        MaxMessageBytes::new(u64::from(DEFAULT_MAX_MESSAGE_BYTES))?,
    )
    .await?;
    Ok(())
}
