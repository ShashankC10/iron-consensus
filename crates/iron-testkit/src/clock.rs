use thiserror::Error;

/// An integer logical instant used only by deterministic tests.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalTick(u64);

impl LogicalTick {
    /// The initial simulator tick.
    pub const ZERO: Self = Self(0);

    /// Constructs an explicit logical tick.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the integer tick value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Adds a logical delay with overflow checking.
    ///
    /// # Errors
    ///
    /// Returns [`ClockError::Overflow`] if the target is not representable.
    pub fn checked_add(self, ticks: u64) -> Result<Self, ClockError> {
        self.0
            .checked_add(ticks)
            .map(Self)
            .ok_or(ClockError::Overflow)
    }
}

/// A manually advanced logical clock.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeterministicClock {
    now: LogicalTick,
}

impl DeterministicClock {
    /// Creates a clock at tick zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            now: LogicalTick::ZERO,
        }
    }

    /// Returns the current logical tick.
    #[must_use]
    pub const fn now(self) -> LogicalTick {
        self.now
    }

    /// Advances by an explicit number of ticks.
    ///
    /// # Errors
    ///
    /// Returns [`ClockError::Overflow`] when addition overflows.
    pub fn advance_by(&mut self, ticks: u64) -> Result<LogicalTick, ClockError> {
        self.now = self.now.checked_add(ticks)?;
        Ok(self.now)
    }

    pub(crate) fn advance_to(&mut self, target: LogicalTick) -> Result<(), ClockError> {
        if target < self.now {
            return Err(ClockError::WentBackwards {
                current: self.now,
                target,
            });
        }
        self.now = target;
        Ok(())
    }
}

/// Logical clock failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ClockError {
    /// Checked tick arithmetic overflowed.
    #[error("logical tick overflow")]
    Overflow,
    /// An operation attempted to move time backwards.
    #[error("logical clock cannot move backwards from {current:?} to {target:?}")]
    WentBackwards {
        /// Current tick.
        current: LogicalTick,
        /// Invalid target tick.
        target: LogicalTick,
    },
}

#[cfg(test)]
mod tests {
    use super::{ClockError, DeterministicClock, LogicalTick};

    #[test]
    fn clock_is_checked_and_manual() {
        let mut clock = DeterministicClock::new();
        assert_eq!(clock.advance_by(7).expect("seven ticks fit").get(), 7);
        assert_eq!(clock.now(), LogicalTick::new(7));
        assert_eq!(
            LogicalTick::new(u64::MAX).checked_add(1),
            Err(ClockError::Overflow)
        );
    }
}
