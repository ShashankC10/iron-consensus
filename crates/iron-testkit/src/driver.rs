use std::collections::BTreeMap;

use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use thiserror::Error;

use crate::{ClockError, DeterministicClock, LogicalTick};

/// An event paired with its deterministic scheduling metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scheduled<T> {
    tick: LogicalTick,
    insertion_sequence: u64,
    event: T,
}

impl<T> Scheduled<T> {
    /// Returns the delivery tick.
    #[must_use]
    pub const fn tick(&self) -> LogicalTick {
        self.tick
    }

    /// Returns the scheduler tie-break sequence.
    #[must_use]
    pub const fn insertion_sequence(&self) -> u64 {
        self.insertion_sequence
    }

    /// Returns the scheduled event.
    #[must_use]
    pub const fn event(&self) -> &T {
        &self.event
    }

    /// Consumes the wrapper and returns the event.
    #[must_use]
    pub fn into_event(self) -> T {
        self.event
    }
}

/// A single-owner event queue ordered by `(tick, insertion_sequence)`.
#[derive(Clone, Debug)]
pub struct Driver<T> {
    clock: DeterministicClock,
    next_sequence: u64,
    queue: BTreeMap<(LogicalTick, u64), T>,
}

impl<T> Default for Driver<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Driver<T> {
    /// Creates an empty queue at logical tick zero.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            clock: DeterministicClock::new(),
            next_sequence: 0,
            queue: BTreeMap::new(),
        }
    }

    /// Returns the current logical tick.
    #[must_use]
    pub const fn now(&self) -> LogicalTick {
        self.clock.now()
    }

    /// Returns the queued event count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns whether no event is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Schedules at an absolute logical tick.
    ///
    /// # Errors
    ///
    /// Returns [`DriverError::Past`] for an earlier tick or
    /// [`DriverError::SequenceOverflow`] when the stable tie-break counter is
    /// exhausted.
    pub fn schedule_at(&mut self, tick: LogicalTick, event: T) -> Result<u64, DriverError> {
        if tick < self.clock.now() {
            return Err(DriverError::Past {
                now: self.clock.now(),
                requested: tick,
            });
        }

        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(DriverError::SequenceOverflow)?;
        let replaced = self.queue.insert((tick, sequence), event);
        debug_assert!(replaced.is_none(), "insertion sequence must be unique");
        Ok(sequence)
    }

    /// Schedules relative to the current tick.
    ///
    /// # Errors
    ///
    /// Returns a checked clock or sequence failure.
    pub fn schedule_after(&mut self, delay: u64, event: T) -> Result<u64, DriverError> {
        let tick = self.clock.now().checked_add(delay)?;
        self.schedule_at(tick, event)
    }

    /// Removes the next event and advances the clock to its tick.
    ///
    /// Events at the same tick are returned in insertion order.
    pub fn pop_next(&mut self) -> Result<Option<Scheduled<T>>, DriverError> {
        let Some(((tick, insertion_sequence), event)) = self.queue.pop_first() else {
            return Ok(None);
        };
        self.clock.advance_to(tick)?;
        Ok(Some(Scheduled {
            tick,
            insertion_sequence,
            event,
        }))
    }

    /// Retains only events accepted by a deterministic predicate.
    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        self.queue.retain(|_, event| keep(event));
    }
}

/// Deterministic scheduler failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DriverError {
    /// A checked insertion counter overflowed.
    #[error("event insertion sequence overflow")]
    SequenceOverflow,
    /// An event was scheduled before the current tick.
    #[error("cannot schedule at {requested:?}; current tick is {now:?}")]
    Past {
        /// Current logical time.
        now: LogicalTick,
        /// Invalid requested time.
        requested: LogicalTick,
    },
    /// Logical tick arithmetic failed.
    #[error(transparent)]
    Clock(#[from] ClockError),
}

/// An explicitly seeded random scenario source with an attached action trace.
///
/// A failed randomized test should print [`SeededScenario::failure_context`]
/// so the seed and action sequence can be replayed and minimized.
#[derive(Clone, Debug)]
pub struct SeededScenario {
    seed: u64,
    rng: ChaCha8Rng,
    trace: Vec<String>,
}

impl SeededScenario {
    /// Creates a deterministic generator from an explicit seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: ChaCha8Rng::seed_from_u64(seed),
            trace: Vec::new(),
        }
    }

    /// Returns the replay seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the next deterministic random word.
    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Records one stable action description.
    pub fn record(&mut self, action: impl Into<String>) {
        self.trace.push(action.into());
    }

    /// Returns the action trace.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        &self.trace
    }

    /// Formats context suitable for a failing assertion.
    #[must_use]
    pub fn failure_context(&self) -> String {
        format!("seed={} trace={:?}", self.seed, self.trace)
    }
}

#[cfg(test)]
mod tests {
    use super::{Driver, DriverError, SeededScenario};
    use crate::LogicalTick;

    #[test]
    fn queue_orders_by_tick_then_insertion() {
        let mut driver = Driver::new();
        driver
            .schedule_at(LogicalTick::new(2), "later")
            .expect("schedule succeeds");
        driver
            .schedule_at(LogicalTick::new(1), "first")
            .expect("schedule succeeds");
        driver
            .schedule_at(LogicalTick::new(1), "second")
            .expect("schedule succeeds");

        let observed = (0..3)
            .map(|_| {
                driver
                    .pop_next()
                    .expect("clock stays monotonic")
                    .expect("event exists")
                    .into_event()
            })
            .collect::<Vec<_>>();
        assert_eq!(observed, vec!["first", "second", "later"]);
    }

    #[test]
    fn queue_rejects_the_past() {
        let mut driver = Driver::new();
        driver
            .schedule_at(LogicalTick::new(4), ())
            .expect("schedule succeeds");
        let _ = driver.pop_next().expect("clock stays monotonic");
        assert!(matches!(
            driver.schedule_at(LogicalTick::new(3), ()),
            Err(DriverError::Past { .. })
        ));
    }

    #[test]
    fn seeded_scenarios_are_reproducible() {
        let mut left = SeededScenario::new(41);
        let mut right = SeededScenario::new(41);
        let left_values = (0..8).map(|_| left.next_u64()).collect::<Vec<_>>();
        let right_values = (0..8).map(|_| right.next_u64()).collect::<Vec<_>>();
        assert_eq!(left_values, right_values);

        let mut different = SeededScenario::new(42);
        let different_values = (0..8).map(|_| different.next_u64()).collect::<Vec<_>>();
        assert_ne!(left_values, different_values);
    }

    #[test]
    fn failure_context_contains_seed_and_trace() {
        let mut scenario = SeededScenario::new(7);
        scenario.record("drop message 3");
        assert_eq!(
            scenario.failure_context(),
            "seed=7 trace=[\"drop message 3\"]"
        );
    }
}
