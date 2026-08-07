use async_trait::async_trait;
use iron_core::ProtocolName;

use crate::{Event, ProtocolError, RecoveredRecord, Transition};

/// An object-safe deterministic protocol state machine.
///
/// Implementations must not perform I/O, spawn tasks, read system time or the
/// environment, or access ambient randomness. The asynchronous shape allows
/// the future runtime to use a uniform dispatch boundary; implementations are
/// expected to complete from in-memory computation alone.
#[async_trait]
pub trait Protocol: Send {
    /// Returns the validated protocol identifier routed to this engine.
    fn name(&self) -> &ProtocolName;

    /// Reconstructs state from records in strictly ascending LSN order.
    ///
    /// Recovery never returns actions. Retransmission decisions must be made
    /// in response to a later explicit event.
    async fn recover(&mut self, records: &[RecoveredRecord]) -> Result<(), ProtocolError>;

    /// Applies one event and returns a declarative transition.
    async fn handle(&mut self, event: Event) -> Result<Transition, ProtocolError>;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use iron_core::ProtocolName;

    use super::Protocol;
    use crate::{Event, ProtocolError, RecoveredRecord, Transition};

    struct NoopProtocol {
        name: ProtocolName,
    }

    #[async_trait]
    impl Protocol for NoopProtocol {
        fn name(&self) -> &ProtocolName {
            &self.name
        }

        async fn recover(&mut self, _records: &[RecoveredRecord]) -> Result<(), ProtocolError> {
            Ok(())
        }

        async fn handle(&mut self, _event: Event) -> Result<Transition, ProtocolError> {
            Ok(Transition::no_op())
        }
    }

    #[test]
    fn protocol_is_object_safe() {
        fn accept_trait_object(_protocol: &mut dyn Protocol) {}

        let name = "test"
            .parse()
            .expect("test is a valid lowercase protocol name");
        let mut protocol = NoopProtocol { name };
        accept_trait_object(&mut protocol);
    }
}
