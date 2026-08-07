#![forbid(unsafe_code)]

//! WAL-before-effects runtime composition for protocol engines.

use async_trait::async_trait;
use iron_core::Envelope;
use iron_protocol::{Action, Event, Protocol, ProtocolError, RecoveredRecord, Transition};
use iron_transport::{Transport, TransportError};
use iron_wal::{WalError, WalRecordInput, WriteAheadLog};
use thiserror::Error;

/// Runtime-side effects. Implementations connect these operations to gRPC,
/// client channels, and a logical timer service.
#[async_trait]
pub trait Effects: Send + Sync {
    async fn send(&self, envelope: Envelope) -> Result<(), TransportError>;
    async fn schedule(&self, timer: iron_protocol::ScheduleTimer) -> Result<(), RuntimeError>;
    async fn cancel(&self, timer: iron_core::TimerId) -> Result<(), RuntimeError>;
    async fn reply(&self, reply: iron_protocol::ReplyToClient) -> Result<(), RuntimeError>;
}

/// Runtime failures are typed and preserve the durability boundary.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Wal(#[from] WalError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("effect execution failed: {0}")]
    Effect(String),
    #[error("transition requested external actions without a durable record")]
    UndurableEffects,
}

/// A serialized protocol instance with an explicit WAL-before-effects rule.
pub struct NodeRuntime<P, W, E> {
    protocol: P,
    wal: W,
    effects: E,
}

impl<P, W, E> NodeRuntime<P, W, E>
where
    P: Protocol,
    W: WriteAheadLog,
    E: Effects,
{
    #[must_use]
    pub fn new(protocol: P, wal: W, effects: E) -> Self {
        Self {
            protocol,
            wal,
            effects,
        }
    }

    /// Replays the WAL into the pure protocol before accepting new events.
    /// Recovery emits no effects; retransmission is an explicit later event.
    pub async fn recover(&mut self) -> Result<(), RuntimeError> {
        let replay = self.wal.replay()?;
        let records = replay
            .records()
            .iter()
            .map(|record| {
                RecoveredRecord::new(
                    record.lsn(),
                    record.record_kind(),
                    record.schema_version(),
                    record.payload().clone(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.protocol.recover(&records).await?;
        Ok(())
    }

    /// Handles one event. Durable state is appended and flushed before any
    /// returned action is executed.
    pub async fn handle(&mut self, event: Event) -> Result<(), RuntimeError> {
        let transition = self.protocol.handle(event).await?;
        self.commit_transition(transition).await
    }

    async fn commit_transition(&mut self, transition: Transition) -> Result<(), RuntimeError> {
        let (record, actions) = transition.into_parts();
        if record.is_none() && !actions.is_empty() {
            return Err(RuntimeError::UndurableEffects);
        }
        if let Some(record) = record {
            let input = WalRecordInput::new(
                record.record_kind(),
                record.schema_version(),
                record.payload(),
            )?;
            self.wal.append(input)?;
            self.wal.flush()?;
        }
        for action in actions {
            match action {
                Action::Send(envelope) => self.effects.send(envelope).await?,
                Action::ScheduleTimer(timer) => self.effects.schedule(timer).await?,
                Action::CancelTimer(timer) => self.effects.cancel(timer).await?,
                Action::ReplyToClient(reply) => self.effects.reply(reply).await?,
                _ => {
                    return Err(RuntimeError::Effect(
                        "unsupported protocol action".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn protocol(&self) -> &P {
        &self.protocol
    }

    #[must_use]
    pub fn protocol_mut(&mut self) -> &mut P {
        &mut self.protocol
    }

    #[must_use]
    pub fn wal(&self) -> &W {
        &self.wal
    }
}

/// Adapter that delegates outbound sends to an existing transport.
pub struct TransportEffects<T> {
    transport: T,
}

impl<T> TransportEffects<T> {
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl<T> Effects for TransportEffects<T>
where
    T: Transport + Send + Sync,
{
    async fn send(&self, envelope: Envelope) -> Result<(), TransportError> {
        self.transport.send(envelope).await
    }
    async fn schedule(&self, _timer: iron_protocol::ScheduleTimer) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn cancel(&self, _timer: iron_core::TimerId) -> Result<(), RuntimeError> {
        Ok(())
    }
    async fn reply(&self, _reply: iron_protocol::ReplyToClient) -> Result<(), RuntimeError> {
        Ok(())
    }
}
