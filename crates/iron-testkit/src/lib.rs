#![forbid(unsafe_code)]

//! Deterministic test support for foundation and future protocol crates.
//!
//! This crate is never a production dependency. Scheduling uses logical ticks
//! and checked insertion sequences; scripted faults and seeded random choices
//! never consult wall-clock time, ambient randomness, or thread scheduling.

mod clock;
mod driver;
mod fault;
mod network;
mod wal_fixture;

pub use clock::{ClockError, DeterministicClock, LogicalTick};
pub use driver::{Driver, DriverError, Scheduled, SeededScenario};
pub use fault::{FaultAction, FaultPredicate, FaultRule};
pub use network::{
    AcceptedAttempt, NetworkError, SimulatedNetwork, SimulatedTransport,
    exercise_at_least_once_contract,
};
pub use wal_fixture::WalFixture;
