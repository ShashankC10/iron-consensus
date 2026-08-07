#![forbid(unsafe_code)]

//! Framework-neutral asynchronous message delivery boundaries.
//!
//! A successful send means that a remote delivery boundary accepted an
//! envelope. It is not an acknowledgement from a protocol, a durability
//! guarantee, or an exactly-once promise. Implementations may lose, delay,
//! duplicate, and reorder accepted messages.

mod error;
mod inbound;
mod transport;

pub use error::{BoxError, TransportError};
pub use inbound::InboundHandler;
pub use transport::Transport;
