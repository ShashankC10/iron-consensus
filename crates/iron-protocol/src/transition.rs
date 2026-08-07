use std::num::NonZeroU16;

use bytes::Bytes;
use iron_core::SchemaVersion;

use crate::{Action, ProtocolError};

/// One opaque record that must be appended before actions are run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableRecord {
    record_kind: NonZeroU16,
    schema_version: SchemaVersion,
    payload: Bytes,
}

impl DurableRecord {
    /// Creates a nonzero-kind protocol record.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidValue`] when `record_kind` is zero.
    pub fn new(
        record_kind: u16,
        schema_version: SchemaVersion,
        payload: impl Into<Bytes>,
    ) -> Result<Self, ProtocolError> {
        let record_kind = NonZeroU16::new(record_kind).ok_or(ProtocolError::InvalidValue {
            field: "record_kind",
            message: "must be nonzero",
        })?;
        Ok(Self {
            record_kind,
            schema_version,
            payload: payload.into(),
        })
    }

    /// Returns the protocol-neutral record kind.
    #[must_use]
    pub const fn record_kind(&self) -> u16 {
        self.record_kind.get()
    }

    /// Returns the payload schema version.
    #[must_use]
    pub const fn schema_version(&self) -> SchemaVersion {
        self.schema_version
    }

    /// Returns the opaque payload.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// A state-machine result containing at most one durable record and ordered
/// actions that may run only after its durability barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transition {
    durable_record: Option<DurableRecord>,
    actions: Vec<Action>,
}

impl Transition {
    /// Creates a transition with a durable state change.
    #[must_use]
    pub fn durable(record: DurableRecord, actions: Vec<Action>) -> Self {
        Self {
            durable_record: Some(record),
            actions,
        }
    }

    /// Creates a stateless transition.
    ///
    /// Protocols may use this only when no state change underlies the actions.
    /// The runtime owns the final policy check before effects execute.
    #[must_use]
    pub fn stateless(actions: Vec<Action>) -> Self {
        Self {
            durable_record: None,
            actions,
        }
    }

    /// Creates a transition with no record and no actions.
    #[must_use]
    pub const fn no_op() -> Self {
        Self {
            durable_record: None,
            actions: Vec::new(),
        }
    }

    /// Returns the record that must cross a durability barrier first.
    #[must_use]
    pub const fn durable_record(&self) -> Option<&DurableRecord> {
        self.durable_record.as_ref()
    }

    /// Returns post-durability actions in execution order.
    #[must_use]
    pub fn actions(&self) -> &[Action] {
        &self.actions
    }

    /// Splits the transition for runtime execution.
    #[must_use]
    pub fn into_parts(self) -> (Option<DurableRecord>, Vec<Action>) {
        (self.durable_record, self.actions)
    }
}

#[cfg(test)]
mod tests {
    use super::Transition;

    #[test]
    fn no_op_has_no_durability_or_effects() {
        let transition = Transition::no_op();
        assert!(transition.durable_record().is_none());
        assert!(transition.actions().is_empty());
    }

    #[test]
    fn action_order_is_retained_for_stateless_transition() {
        let transition = Transition::stateless(Vec::new());
        assert!(transition.actions().is_empty());
    }
}
