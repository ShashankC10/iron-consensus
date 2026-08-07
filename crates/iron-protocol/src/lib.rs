#![forbid(unsafe_code)]

//! Pure state-machine boundaries shared by future transaction and consensus
//! protocol implementations.
//!
//! A protocol receives validated events and returns declarative transitions.
//! It cannot perform I/O or run an external effect itself. The future runtime
//! is responsible for serializing calls, appending a transition's durable
//! record, crossing the configured durability barrier, and only then executing
//! the returned actions in order.

mod action;
mod error;
mod event;
mod protocol;
mod transition;

pub use action::{Action, LogicalDuration, ReplyToClient, ScheduleTimer};
pub use error::{BoxError, ProtocolError};
pub use event::{ClientRequest, Event, RecoveredRecord, TimerFired};
pub use protocol::Protocol;
pub use transition::{DurableRecord, Transition};
