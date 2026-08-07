use iron_core::{Envelope, MessageType, NodeId, ProtocolName};

/// Predicates applied to the one-based ordinal of a send attempt and immutable
/// envelope routing fields.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPredicate {
    /// Optional one-based send ordinal.
    pub ordinal: Option<u64>,
    /// Optional exact source node.
    pub source: Option<NodeId>,
    /// Optional exact destination node.
    pub destination: Option<NodeId>,
    /// Optional exact protocol name.
    pub protocol: Option<ProtocolName>,
    /// Optional exact message type.
    pub message_type: Option<MessageType>,
}

impl FaultPredicate {
    pub(crate) fn matches(&self, ordinal: u64, envelope: &Envelope) -> bool {
        self.ordinal.is_none_or(|expected| expected == ordinal)
            && self
                .source
                .as_ref()
                .is_none_or(|expected| expected == envelope.source())
            && self
                .destination
                .as_ref()
                .is_none_or(|expected| expected == envelope.destination())
            && self
                .protocol
                .as_ref()
                .is_none_or(|expected| expected == envelope.protocol())
            && self
                .message_type
                .as_ref()
                .is_none_or(|expected| expected == envelope.message_type())
    }
}

/// A deterministic action applied to the first matching scripted rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FaultAction {
    /// Accept but discard the attempt.
    Drop,
    /// Enqueue the original plus this many additional identical copies.
    Duplicate {
        /// Number of additional copies.
        additional_copies: u32,
    },
    /// Add a relative logical delay.
    Delay {
        /// Delay in ticks.
        ticks: u64,
    },
    /// Hold the delivery until the script releases this tick's held messages;
    /// release occurs in reverse hold order.
    ReorderAtTick {
        /// Absolute release tick.
        tick: u64,
    },
}

/// One declared scripted fault.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultRule {
    predicate: FaultPredicate,
    action: FaultAction,
    consuming: bool,
    consumed: bool,
}

impl FaultRule {
    /// Creates a fault rule. Consuming rules match at most once; reusable rules
    /// match every attempt until removed.
    #[must_use]
    pub const fn new(predicate: FaultPredicate, action: FaultAction, consuming: bool) -> Self {
        Self {
            predicate,
            action,
            consuming,
            consumed: false,
        }
    }

    pub(crate) fn try_match(&mut self, ordinal: u64, envelope: &Envelope) -> Option<FaultAction> {
        if self.consumed || !self.predicate.matches(ordinal, envelope) {
            return None;
        }
        if self.consuming {
            self.consumed = true;
        }
        Some(self.action)
    }
}
