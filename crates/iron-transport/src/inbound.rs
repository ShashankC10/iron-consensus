use async_trait::async_trait;
use iron_core::Envelope;

use crate::TransportError;

/// A validated inbound delivery boundary implemented by the future runtime.
///
/// Wire adapters must validate and convert generated wire types into an
/// [`Envelope`] before invoking this interface. Returning success means the
/// runtime accepted responsibility for processing; protocol-level responses
/// are separate envelopes.
#[async_trait]
pub trait InboundHandler: Send + Sync {
    /// Delivers one validated envelope to the node runtime.
    async fn deliver(&self, envelope: Envelope) -> Result<(), TransportError>;
}
