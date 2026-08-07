use std::num::NonZeroU64;

use bytes::Bytes;
use iron_core::{ClientRequestId, Envelope, TimerId};

use crate::ProtocolError;

/// A positive logical duration measured in simulator/runtime ticks.
///
/// Protocols never receive a wall-clock timestamp. The runtime chooses the
/// mapping between a logical tick and elapsed time.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalDuration(NonZeroU64);

impl LogicalDuration {
    /// Creates a positive logical duration.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidValue`] when `ticks` is zero.
    pub fn from_ticks(ticks: u64) -> Result<Self, ProtocolError> {
        NonZeroU64::new(ticks)
            .map(Self)
            .ok_or(ProtocolError::InvalidValue {
                field: "logical_duration",
                message: "must be at least one tick",
            })
    }

    /// Returns the number of logical ticks.
    #[must_use]
    pub const fn ticks(self) -> u64 {
        self.0.get()
    }
}

/// A request to create or replace a logical timer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleTimer {
    timer_id: TimerId,
    delay: LogicalDuration,
}

impl ScheduleTimer {
    /// Creates a timer action.
    #[must_use]
    pub const fn new(timer_id: TimerId, delay: LogicalDuration) -> Self {
        Self { timer_id, delay }
    }

    /// Returns the stable timer identity.
    #[must_use]
    pub const fn timer_id(&self) -> TimerId {
        self.timer_id
    }

    /// Returns the logical delay.
    #[must_use]
    pub const fn delay(&self) -> LogicalDuration {
        self.delay
    }
}

/// An opaque response to a previously accepted client request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplyToClient {
    request_id: ClientRequestId,
    outcome: Bytes,
}

impl ReplyToClient {
    /// Creates a response action. Outcome size enforcement belongs to the
    /// validated runtime/dedup configuration.
    #[must_use]
    pub fn new(request_id: ClientRequestId, outcome: impl Into<Bytes>) -> Self {
        Self {
            request_id,
            outcome: outcome.into(),
        }
    }

    /// Returns the client request identity.
    #[must_use]
    pub const fn request_id(&self) -> ClientRequestId {
        self.request_id
    }

    /// Returns the opaque response bytes.
    #[must_use]
    pub fn outcome(&self) -> &Bytes {
        &self.outcome
    }
}

/// An ordered effect executed by the runtime only after durability.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Action {
    /// Attempt one at-least-once envelope delivery.
    Send(Envelope),
    /// Create or replace a logical timer.
    ScheduleTimer(ScheduleTimer),
    /// Cancel a logical timer if it exists.
    CancelTimer(TimerId),
    /// Return an opaque outcome to a client boundary.
    ReplyToClient(ReplyToClient),
}

#[cfg(test)]
mod tests {
    use super::LogicalDuration;

    #[test]
    fn logical_duration_rejects_zero() {
        assert!(LogicalDuration::from_ticks(0).is_err());
        assert_eq!(
            LogicalDuration::from_ticks(1)
                .expect("one tick is valid")
                .ticks(),
            1
        );
        assert_eq!(
            LogicalDuration::from_ticks(u64::MAX)
                .expect("maximum tick count is valid")
                .ticks(),
            u64::MAX
        );
    }
}
