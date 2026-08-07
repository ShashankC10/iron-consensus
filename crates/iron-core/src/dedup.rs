use std::collections::{BTreeMap, VecDeque};

use crate::config::DedupConfig;
use crate::envelope::{Envelope, SemanticFingerprint};
use crate::error::CoreError;
use crate::id::{ClusterId, MessageId, NodeId};

/// The deterministic deduplication key: cluster, source node, and message ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DedupKey {
    cluster_id: ClusterId,
    source_node_id: NodeId,
    message_id: MessageId,
}

impl DedupKey {
    #[must_use]
    pub fn new(cluster_id: ClusterId, source_node_id: NodeId, message_id: MessageId) -> Self {
        Self {
            cluster_id,
            source_node_id,
            message_id,
        }
    }

    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    #[must_use]
    pub fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    #[must_use]
    pub const fn message_id(&self) -> MessageId {
        self.message_id
    }

    /// Derives the specified `(cluster, source, message)` key from an envelope.
    #[must_use]
    pub fn from_envelope(envelope: &Envelope) -> Self {
        Self::new(
            envelope.cluster_id(),
            envelope.source().clone(),
            envelope.message_id(),
        )
    }
}

impl From<&Envelope> for DedupKey {
    fn from(envelope: &Envelope) -> Self {
        Self::from_envelope(envelope)
    }
}

/// Result of attempting to reserve a deduplication key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BeginResult<'a> {
    /// The key was absent and is now reserved in flight.
    New,
    /// The same semantic message is already reserved.
    InFlight,
    /// The same semantic message completed; replay these exact bytes.
    Replay(&'a [u8]),
    /// The retained key has a different semantic fingerprint.
    Conflict,
    /// Admission is blocked because every evictable slot is in flight.
    Full,
}

/// Result of completing an existing reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompleteResult {
    Completed,
    AlreadyCompleted,
    NotReserved,
    Conflict,
}

/// Result of aborting an existing reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortResult {
    Aborted,
    AlreadyCompleted,
    NotReserved,
    Conflict,
}

/// Result of replaying a durable completed entry during recovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreResult {
    Restored,
    AlreadyPresent,
    InFlight,
    Conflict,
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EntryState {
    InFlight,
    Completed(Box<[u8]>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Entry {
    fingerprint: SemanticFingerprint,
    insertion_sequence: u64,
    state: EntryState,
}

/// Single-owner, deterministic, bounded idempotency state.
///
/// The table performs no locking and observes no time. A future serialized
/// node runtime owns it. Entries are ordered by a checked local insertion
/// sequence; duplicate access and completion never refresh that order.
#[derive(Clone, Debug)]
pub struct DedupTable {
    config: DedupConfig,
    entries: BTreeMap<DedupKey, Entry>,
    insertion_order: VecDeque<(u64, DedupKey)>,
    retained_outcome_bytes: u64,
    last_insertion_sequence: u64,
}

impl DedupTable {
    #[must_use]
    pub fn new(config: DedupConfig) -> Self {
        Self {
            config,
            entries: BTreeMap::new(),
            insertion_order: VecDeque::new(),
            retained_outcome_bytes: 0,
            last_insertion_sequence: 0,
        }
    }

    /// Reserves a missing key before state-machine evaluation.
    ///
    /// Oldest completed entries are evicted until the entry bound admits the
    /// new key. In-flight entries are never evicted. Sequence exhaustion is
    /// returned as a typed error before any eviction or insertion occurs.
    pub fn begin(
        &mut self,
        key: &DedupKey,
        fingerprint: SemanticFingerprint,
    ) -> Result<BeginResult<'_>, CoreError> {
        if self.entries.contains_key(key) {
            let entry = self
                .entries
                .get(key)
                .expect("a key reported present remains present during single-owner access");
            if entry.fingerprint != fingerprint {
                return Ok(BeginResult::Conflict);
            }
            return Ok(match &entry.state {
                EntryState::InFlight => BeginResult::InFlight,
                EntryState::Completed(outcome) => BeginResult::Replay(outcome),
            });
        }

        let insertion_sequence =
            self.last_insertion_sequence
                .checked_add(1)
                .ok_or(CoreError::SequenceExhausted {
                    counter: "dedup insertion",
                })?;

        while self.entries.len() >= self.config.max_entries().get() as usize {
            if !self.evict_oldest_completed(None) {
                return Ok(BeginResult::Full);
            }
        }

        let previous = self.entries.insert(
            key.clone(),
            Entry {
                fingerprint,
                insertion_sequence,
                state: EntryState::InFlight,
            },
        );
        debug_assert!(previous.is_none(), "missing key cannot replace an entry");
        self.insertion_order
            .push_back((insertion_sequence, key.clone()));
        self.last_insertion_sequence = insertion_sequence;
        Ok(BeginResult::New)
    }

    /// Completes only a matching in-flight reservation.
    ///
    /// An oversized outcome returns `CoreError::OutcomeTooLarge` without
    /// changing the reservation. Before storing an individually valid outcome,
    /// oldest other completed entries are evicted until the total-byte bound
    /// fits. Completion never evicts an in-flight entry.
    pub fn complete(
        &mut self,
        key: &DedupKey,
        fingerprint: SemanticFingerprint,
        outcome: impl Into<Vec<u8>>,
    ) -> Result<CompleteResult, CoreError> {
        match self.entries.get(key) {
            None => return Ok(CompleteResult::NotReserved),
            Some(entry) if entry.fingerprint != fingerprint => {
                return Ok(CompleteResult::Conflict);
            }
            Some(Entry {
                state: EntryState::Completed(_),
                ..
            }) => return Ok(CompleteResult::AlreadyCompleted),
            Some(Entry {
                state: EntryState::InFlight,
                ..
            }) => {}
        }

        let outcome = outcome.into();
        let outcome_length = u64::try_from(outcome.len()).unwrap_or(u64::MAX);
        if outcome_length > u64::from(self.config.max_outcome_bytes().get()) {
            return Err(CoreError::OutcomeTooLarge {
                actual: outcome_length,
                maximum: self.config.max_outcome_bytes().get(),
            });
        }

        let total_limit = u64::from(self.config.max_total_outcome_bytes().get());
        while self
            .retained_outcome_bytes
            .checked_add(outcome_length)
            .is_none_or(|total| total > total_limit)
        {
            let evicted = self.evict_oldest_completed(Some(key));
            debug_assert!(
                evicted,
                "an individually valid outcome must fit after completed eviction"
            );
            if !evicted {
                return Err(CoreError::OutcomeTooLarge {
                    actual: outcome_length,
                    maximum: self.config.max_total_outcome_bytes().get(),
                });
            }
        }

        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(CompleteResult::NotReserved);
        };
        entry.state = EntryState::Completed(outcome.into_boxed_slice());
        self.retained_outcome_bytes += outcome_length;
        Ok(CompleteResult::Completed)
    }

    /// Removes only a matching in-flight reservation.
    pub fn abort(&mut self, key: &DedupKey, fingerprint: SemanticFingerprint) -> AbortResult {
        match self.entries.get(key) {
            None => return AbortResult::NotReserved,
            Some(entry) if entry.fingerprint != fingerprint => return AbortResult::Conflict,
            Some(Entry {
                state: EntryState::Completed(_),
                ..
            }) => return AbortResult::AlreadyCompleted,
            Some(Entry {
                state: EntryState::InFlight,
                ..
            }) => {}
        }

        let Some(removed) = self.entries.remove(key) else {
            return AbortResult::NotReserved;
        };
        let insertion_sequence = removed.insertion_sequence;
        if let Some(position) = self
            .insertion_order
            .iter()
            .position(|(sequence, queued_key)| *sequence == insertion_sequence && queued_key == key)
        {
            let removed_from_order = self.insertion_order.remove(position);
            debug_assert!(removed_from_order.is_some());
        }
        AbortResult::Aborted
    }

    /// Replays one durable completion in caller-provided LSN order.
    ///
    /// This uses the same reservation, eviction, and completion algorithm as
    /// live processing. Recovery callers should start with an empty table and
    /// invoke this method in ascending LSN order.
    pub fn restore_completed(
        &mut self,
        key: &DedupKey,
        fingerprint: SemanticFingerprint,
        outcome: impl Into<Vec<u8>>,
    ) -> Result<RestoreResult, CoreError> {
        let outcome = outcome.into();
        match self.begin(key, fingerprint)? {
            BeginResult::New => {}
            BeginResult::InFlight => return Ok(RestoreResult::InFlight),
            BeginResult::Replay(existing) => {
                return if existing == outcome {
                    Ok(RestoreResult::AlreadyPresent)
                } else {
                    Ok(RestoreResult::Conflict)
                };
            }
            BeginResult::Conflict => return Ok(RestoreResult::Conflict),
            BeginResult::Full => return Ok(RestoreResult::Full),
        }
        match self.complete(key, fingerprint, outcome)? {
            CompleteResult::Completed => Ok(RestoreResult::Restored),
            CompleteResult::AlreadyCompleted => Ok(RestoreResult::AlreadyPresent),
            CompleteResult::NotReserved | CompleteResult::Conflict => Ok(RestoreResult::Conflict),
        }
    }

    #[must_use]
    pub const fn config(&self) -> DedupConfig {
        self.config
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub const fn retained_outcome_bytes(&self) -> u64 {
        self.retained_outcome_bytes
    }

    fn evict_oldest_completed(&mut self, excluded: Option<&DedupKey>) -> bool {
        let position = self.insertion_order.iter().position(|(_, key)| {
            if excluded.is_some_and(|excluded_key| excluded_key == key) {
                return false;
            }
            matches!(
                self.entries.get(key).map(|entry| &entry.state),
                Some(EntryState::Completed(_))
            )
        });
        let Some(position) = position else {
            return false;
        };
        let (_, key) = self
            .insertion_order
            .remove(position)
            .expect("located queue entry remains present");
        let entry = self
            .entries
            .remove(&key)
            .expect("queue and entry map remain consistent");
        let EntryState::Completed(outcome) = entry.state else {
            unreachable!("only completed entries are selected for eviction");
        };
        self.retained_outcome_bytes -=
            u64::try_from(outcome.len()).expect("validated outcome length fits in u64");
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{AbortResult, BeginResult, CompleteResult, DedupKey, DedupTable, RestoreResult};
    use crate::config::{DedupConfig, RawDedupConfig};
    use crate::envelope::SemanticFingerprint;
    use crate::error::CoreError;
    use crate::id::{ClusterId, MessageId, NodeId};

    fn config(max_entries: u64, max_outcome: u64, max_total: u64) -> DedupConfig {
        DedupConfig::try_from(RawDedupConfig {
            max_entries,
            max_outcome_bytes: max_outcome,
            max_total_outcome_bytes: max_total,
        })
        .expect("valid test config")
    }

    fn key(ordinal: u8) -> DedupKey {
        let mut cluster = [0; 16];
        cluster[15] = 1;
        let mut message = [0; 16];
        message[15] = ordinal;
        DedupKey::new(
            ClusterId::from_bytes(cluster).expect("nonzero"),
            NodeId::parse("source").expect("valid node"),
            MessageId::from_bytes(message).expect("nonzero"),
        )
    }

    fn fingerprint(ordinal: u8) -> SemanticFingerprint {
        let mut bytes = [0; 32];
        bytes[31] = ordinal;
        SemanticFingerprint::from_bytes(bytes)
    }

    #[test]
    fn reservation_replay_abort_and_conflict_are_explicit() {
        let mut table = DedupTable::new(config(4, 16, 65_536));
        let key = key(1);
        assert_eq!(
            table
                .begin(&key, fingerprint(1))
                .expect("sequence available"),
            BeginResult::New
        );
        assert_eq!(
            table.begin(&key, fingerprint(1)).expect("lookup"),
            BeginResult::InFlight
        );
        assert_eq!(
            table.begin(&key, fingerprint(2)).expect("lookup"),
            BeginResult::Conflict
        );
        assert_eq!(table.abort(&key, fingerprint(2)), AbortResult::Conflict);
        assert_eq!(table.abort(&key, fingerprint(1)), AbortResult::Aborted);

        assert_eq!(
            table
                .begin(&key, fingerprint(1))
                .expect("sequence available"),
            BeginResult::New
        );
        assert_eq!(
            table
                .complete(&key, fingerprint(1), b"outcome".to_vec())
                .expect("valid outcome"),
            CompleteResult::Completed
        );
        assert_eq!(
            table.begin(&key, fingerprint(1)).expect("lookup"),
            BeginResult::Replay(b"outcome")
        );
        assert_eq!(
            table.abort(&key, fingerprint(1)),
            AbortResult::AlreadyCompleted
        );
    }

    #[test]
    fn entry_eviction_uses_insertion_fifo_without_access_refresh() {
        let mut table = DedupTable::new(config(2, 16, 65_536));
        let first = key(1);
        let second = key(2);
        let third = key(3);
        for (key, fingerprint) in [(&first, fingerprint(1)), (&second, fingerprint(2))] {
            assert_eq!(
                table.begin(key, fingerprint).expect("admitted"),
                BeginResult::New
            );
            assert_eq!(
                table
                    .complete(key, fingerprint, vec![key.message_id().to_bytes()[15]])
                    .expect("completed"),
                CompleteResult::Completed
            );
        }
        assert!(matches!(
            table.begin(&first, fingerprint(1)).expect("lookup"),
            BeginResult::Replay(_)
        ));
        assert_eq!(
            table.begin(&third, fingerprint(3)).expect("admitted"),
            BeginResult::New
        );
        assert_eq!(
            table.begin(&first, fingerprint(1)).expect("readmission"),
            BeginResult::New
        );
    }

    #[test]
    fn insertion_order_survives_out_of_order_completion() {
        let mut table = DedupTable::new(config(2, 16, 65_536));
        let first = key(1);
        let second = key(2);
        let third = key(3);
        assert_eq!(
            table.begin(&first, fingerprint(1)).expect("admit"),
            BeginResult::New
        );
        assert_eq!(
            table.begin(&second, fingerprint(2)).expect("admit"),
            BeginResult::New
        );
        table
            .complete(&second, fingerprint(2), vec![2])
            .expect("complete");
        table
            .complete(&first, fingerprint(1), vec![1])
            .expect("complete");
        assert_eq!(
            table.begin(&third, fingerprint(3)).expect("admit"),
            BeginResult::New
        );
        assert_eq!(
            table
                .begin(&first, fingerprint(1))
                .expect("oldest was evicted"),
            BeginResult::New
        );
    }

    #[test]
    fn in_flight_entries_are_never_evicted() {
        let mut table = DedupTable::new(config(2, 16, 65_536));
        let first = key(1);
        let second = key(2);
        let third = key(3);
        table.begin(&first, fingerprint(1)).expect("admit");
        table.begin(&second, fingerprint(2)).expect("admit");
        assert_eq!(
            table.begin(&third, fingerprint(3)).expect("bounded"),
            BeginResult::Full
        );
        table
            .complete(&first, fingerprint(1), vec![1])
            .expect("complete");
        assert_eq!(
            table
                .begin(&third, fingerprint(3))
                .expect("admit after eviction"),
            BeginResult::New
        );
        assert_eq!(
            table
                .begin(&second, fingerprint(2))
                .expect("still retained"),
            BeginResult::InFlight
        );
    }

    #[test]
    fn total_outcome_bytes_evict_oldest_completed_entry() {
        let mut table = DedupTable::new(config(4, 40_000, 65_536));
        let first = key(1);
        let second = key(2);
        table.begin(&first, fingerprint(1)).expect("admit");
        table
            .complete(&first, fingerprint(1), vec![1; 40_000])
            .expect("complete");
        table.begin(&second, fingerprint(2)).expect("admit");
        table
            .complete(&second, fingerprint(2), vec![2; 40_000])
            .expect("complete");
        assert_eq!(table.retained_outcome_bytes(), 40_000);
        assert_eq!(
            table
                .begin(&first, fingerprint(1))
                .expect("evicted key is new"),
            BeginResult::New
        );
    }

    #[test]
    fn oversized_completion_keeps_the_reservation_in_flight() {
        let mut table = DedupTable::new(config(2, 4, 65_536));
        let key = key(1);
        table.begin(&key, fingerprint(1)).expect("admit");
        assert!(matches!(
            table.complete(&key, fingerprint(1), vec![0; 5]),
            Err(CoreError::OutcomeTooLarge {
                actual: 5,
                maximum: 4
            })
        ));
        assert_eq!(
            table.begin(&key, fingerprint(1)).expect("lookup"),
            BeginResult::InFlight
        );
    }

    #[test]
    fn recovery_uses_the_same_bounded_algorithm() {
        let mut table = DedupTable::new(config(1, 16, 65_536));
        let first = key(1);
        let second = key(2);
        assert_eq!(
            table
                .restore_completed(&first, fingerprint(1), vec![1])
                .expect("restore"),
            RestoreResult::Restored
        );
        assert_eq!(
            table
                .restore_completed(&second, fingerprint(2), vec![2])
                .expect("restore"),
            RestoreResult::Restored
        );
        assert_eq!(
            table.begin(&first, fingerprint(1)).expect("evicted key"),
            BeginResult::New
        );
    }

    #[test]
    fn sequence_overflow_is_nonmutating() {
        let mut table = DedupTable::new(config(2, 16, 65_536));
        table.last_insertion_sequence = u64::MAX;
        assert!(matches!(
            table.begin(&key(1), fingerprint(1)),
            Err(CoreError::SequenceExhausted {
                counter: "dedup insertion"
            })
        ));
        assert!(table.is_empty());
    }
}
