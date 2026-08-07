use async_trait::async_trait;
use iron_core::Envelope;

use crate::TransportError;

/// An at-least-once outbound envelope transport.
///
/// Implementations perform one delivery attempt. They must not implement
/// hidden retry loops, deduplication, protocol correlation, or backoff. A
/// successful result means only that the receiving transport boundary
/// accepted this attempt.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Attempts to deliver one already-validated envelope.
    async fn send(&self, envelope: Envelope) -> Result<(), TransportError>;
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use iron_core::Envelope;

    use super::Transport;
    use crate::TransportError;

    struct Sink;

    #[async_trait]
    impl Transport for Sink {
        async fn send(&self, _envelope: Envelope) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[test]
    fn transport_is_object_safe() {
        fn accept_trait_object(_transport: &dyn Transport) {}
        accept_trait_object(&Sink);
    }
}
