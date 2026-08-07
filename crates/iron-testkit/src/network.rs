use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use iron_core::{Envelope, NodeId};
use iron_transport::{Transport, TransportError};
use thiserror::Error;

use crate::{Driver, DriverError, FaultAction, FaultRule, LogicalTick, Scheduled};

#[derive(Clone, Debug)]
struct HeldDelivery {
    tick: LogicalTick,
    envelope: Envelope,
}

/// Observable result of accepting a simulated send attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptedAttempt {
    ordinal: u64,
    enqueued_copies: u32,
}

impl AcceptedAttempt {
    /// Returns the one-based attempt ordinal.
    #[must_use]
    pub const fn ordinal(self) -> u64 {
        self.ordinal
    }

    /// Returns the number of copies placed into the delivery queue.
    #[must_use]
    pub const fn enqueued_copies(self) -> u32 {
        self.enqueued_copies
    }
}

/// A deterministic, single-owner, at-least-once simulated network.
///
/// Rules are examined in declaration order and only the first matching rule is
/// applied. Directed partitions and crashed destinations accept but discard a
/// send. Crash/restart controls connectivity only; a higher-level harness owns
/// the node's volatile protocol and dedup state and must discard it on crash.
#[derive(Clone, Debug, Default)]
pub struct SimulatedNetwork {
    deliveries: Driver<Envelope>,
    held: Vec<HeldDelivery>,
    rules: Vec<FaultRule>,
    partitions: BTreeSet<(NodeId, NodeId)>,
    crashed: BTreeSet<NodeId>,
    last_ordinal: u64,
}

impl SimulatedNetwork {
    /// Creates an empty network at tick zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current logical delivery tick.
    #[must_use]
    pub fn now(&self) -> LogicalTick {
        self.deliveries.now()
    }

    /// Returns the number of scheduled (not held) delivery copies.
    #[must_use]
    pub fn queued_len(&self) -> usize {
        self.deliveries.len()
    }

    /// Adds a scripted fault after all currently declared rules.
    pub fn add_fault(&mut self, rule: FaultRule) {
        self.rules.push(rule);
    }

    /// Creates a directed partition from `source` to `destination`.
    pub fn partition(&mut self, source: NodeId, destination: NodeId) {
        self.partitions.insert((source, destination));
    }

    /// Heals one directed partition, returning whether it existed.
    pub fn heal(&mut self, source: &NodeId, destination: &NodeId) -> bool {
        self.partitions
            .remove(&(source.clone(), destination.clone()))
    }

    /// Marks a node crashed. The owning runtime harness must separately drop
    /// that node's volatile state.
    pub fn crash(&mut self, node: NodeId) {
        self.crashed.insert(node);
    }

    /// Marks a node restarted, returning whether it had been crashed.
    pub fn restart(&mut self, node: &NodeId) -> bool {
        self.crashed.remove(node)
    }

    /// Accepts one envelope and applies one deterministic network fault.
    ///
    /// # Errors
    ///
    /// Returns a checked ordinal, clock, or scheduling failure. Acceptance
    /// never implies delivery: drops and partitions return `Ok` with zero
    /// enqueued copies.
    pub fn send(&mut self, envelope: Envelope) -> Result<AcceptedAttempt, NetworkError> {
        let ordinal = self
            .last_ordinal
            .checked_add(1)
            .ok_or(NetworkError::AttemptOverflow)?;
        self.last_ordinal = ordinal;

        if self.is_blocked(&envelope) {
            return Ok(AcceptedAttempt {
                ordinal,
                enqueued_copies: 0,
            });
        }

        let action = self
            .rules
            .iter_mut()
            .find_map(|rule| rule.try_match(ordinal, &envelope));

        let enqueued_copies = match action {
            None => {
                self.deliveries.schedule_after(0, envelope)?;
                1
            }
            Some(FaultAction::Drop) => 0,
            Some(FaultAction::Duplicate { additional_copies }) => {
                let copies = additional_copies
                    .checked_add(1)
                    .ok_or(NetworkError::DuplicateCountOverflow)?;
                for _ in 0..copies {
                    self.deliveries.schedule_after(0, envelope.clone())?;
                }
                copies
            }
            Some(FaultAction::Delay { ticks }) => {
                self.deliveries.schedule_after(ticks, envelope)?;
                1
            }
            Some(FaultAction::ReorderAtTick { tick }) => {
                let tick = LogicalTick::new(tick);
                if tick < self.deliveries.now() {
                    return Err(NetworkError::ReorderInPast {
                        now: self.deliveries.now(),
                        requested: tick,
                    });
                }
                self.held.push(HeldDelivery { tick, envelope });
                0
            }
        };

        Ok(AcceptedAttempt {
            ordinal,
            enqueued_copies,
        })
    }

    /// Releases held reorder messages for `tick` in reverse hold order.
    ///
    /// # Errors
    ///
    /// Returns a scheduling error if the release tick is already in the past
    /// or the insertion sequence is exhausted.
    pub fn release_reordered_at(&mut self, tick: LogicalTick) -> Result<usize, NetworkError> {
        let mut retained = Vec::with_capacity(self.held.len());
        let mut releasing = Vec::new();
        for delivery in std::mem::take(&mut self.held) {
            if delivery.tick == tick {
                releasing.push(delivery);
            } else {
                retained.push(delivery);
            }
        }
        self.held = retained;
        let count = releasing.len();
        for delivery in releasing.into_iter().rev() {
            self.deliveries.schedule_at(tick, delivery.envelope)?;
        }
        Ok(count)
    }

    /// Returns the next currently deliverable copy, skipping copies blocked at
    /// their delivery instant.
    ///
    /// # Errors
    ///
    /// Returns a deterministic driver failure if queue invariants fail.
    pub fn pop_delivery(&mut self) -> Result<Option<Scheduled<Envelope>>, NetworkError> {
        while let Some(delivery) = self.deliveries.pop_next()? {
            if !self.is_blocked(delivery.event()) {
                return Ok(Some(delivery));
            }
        }
        Ok(None)
    }

    fn is_blocked(&self, envelope: &Envelope) -> bool {
        self.crashed.contains(envelope.destination())
            || self
                .partitions
                .contains(&(envelope.source().clone(), envelope.destination().clone()))
    }
}

/// Simulated network failures caused only by checked deterministic limits.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NetworkError {
    /// The one-based send ordinal was exhausted.
    #[error("simulated network attempt ordinal overflow")]
    AttemptOverflow,
    /// `additional_copies + 1` overflowed.
    #[error("simulated duplicate count overflow")]
    DuplicateCountOverflow,
    /// A reorder rule requested a release before current logical time.
    #[error("cannot reorder at {requested:?}; current tick is {now:?}")]
    ReorderInPast {
        /// Current logical time.
        now: LogicalTick,
        /// Invalid requested time.
        requested: LogicalTick,
    },
    /// Deterministic scheduling failed.
    #[error(transparent)]
    Driver(#[from] DriverError),
}

#[derive(Debug, Error)]
enum AdapterError {
    #[error("simulated network mutex was poisoned")]
    LockPoisoned,
    #[error(transparent)]
    Network(#[from] NetworkError),
}

/// A clonable asynchronous adapter over a shared simulated network.
#[derive(Clone, Debug)]
pub struct SimulatedTransport {
    network: Arc<Mutex<SimulatedNetwork>>,
}

impl SimulatedTransport {
    /// Creates a transport and returns access to its deterministic network.
    #[must_use]
    pub fn new(network: SimulatedNetwork) -> Self {
        Self {
            network: Arc::new(Mutex::new(network)),
        }
    }

    /// Returns the shared network so a test driver can pop deliveries and
    /// script state changes between attempts.
    #[must_use]
    pub fn network(&self) -> Arc<Mutex<SimulatedNetwork>> {
        Arc::clone(&self.network)
    }
}

#[async_trait]
impl Transport for SimulatedTransport {
    async fn send(&self, envelope: Envelope) -> Result<(), TransportError> {
        let mut network = self.network.lock().map_err(|_| TransportError::Internal {
            source: Box::new(AdapterError::LockPoisoned),
        })?;
        network
            .send(envelope)
            .map(|_| ())
            .map_err(|error| TransportError::Internal {
                source: Box::new(AdapterError::Network(error)),
            })
    }
}

/// Shared adapter contract: a transport must accept two attempts carrying the
/// same immutable envelope and message ID rather than deduplicating internally.
///
/// This helper is reusable by this simulated adapter and the future gRPC
/// adapter. Observable delivery multiplicity remains adapter-specific.
pub async fn exercise_at_least_once_contract(
    transport: &dyn Transport,
    envelope: Envelope,
) -> Result<(), TransportError> {
    transport.send(envelope.clone()).await?;
    transport.send(envelope).await
}
