use async_trait::async_trait;
use iron_2pc::Participant;
use iron_core::{DEFAULT_MAX_MESSAGE_BYTES, Envelope, MaxMessageBytes};
use iron_grpc::{EnvelopeHandler, GrpcError};
use std::sync::Mutex;

struct ParticipantHandler {
    state: Mutex<Participant>,
}

#[async_trait]
impl EnvelopeHandler for ParticipantHandler {
    async fn deliver(&self, envelope: Envelope) -> Result<(), GrpcError> {
        let mut state = self.state.lock().map_err(|_| GrpcError::InvalidField {
            field: "participant_state",
        })?;
        match envelope.message_type().as_str() {
            "prepare" => {
                let can_commit = envelope.payload() == b"1";
                state.prepare(can_commit);
                Ok(())
            }
            "commit" => state
                .commit()
                .map(|_| ())
                .map_err(|_| GrpcError::InvalidField {
                    field: "message_type",
                }),
            "abort" => state
                .abort()
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
        .unwrap_or_else(|_| "127.0.0.1:7002".to_owned())
        .parse()?;
    iron_grpc::serve(
        address,
        ParticipantHandler {
            state: Mutex::new(Participant::new()),
        },
        MaxMessageBytes::new(u64::from(DEFAULT_MAX_MESSAGE_BYTES))?,
    )
    .await?;
    Ok(())
}
