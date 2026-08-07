#![forbid(unsafe_code)]

//! Deterministic, durable-state-friendly Two-Phase Commit state machines.

use iron_core::NodeId;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoordinatorPhase {
    Idle,
    Preparing,
    Decided(Decision),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParticipantPhase {
    Working,
    Prepared,
    Committed,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoordinatorAction {
    Prepare,
    Commit,
    Abort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParticipantAction {
    Prepared,
    VoteAbort,
    Committed,
    Aborted,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TwoPcError {
    #[error("transaction is already decided")]
    AlreadyDecided,
    #[error("unknown participant")]
    UnknownParticipant,
    #[error("participant voted twice with a conflicting result")]
    ConflictingVote,
    #[error("invalid participant transition")]
    InvalidTransition,
}

/// Coordinator state. The caller persists every returned decision before
/// sending the corresponding action to participants.
#[derive(Clone, Debug)]
pub struct Coordinator {
    participants: BTreeSet<NodeId>,
    prepared: BTreeSet<NodeId>,
    aborted: bool,
    phase: CoordinatorPhase,
}

impl Coordinator {
    pub fn new(participants: impl IntoIterator<Item = NodeId>) -> Result<Self, TwoPcError> {
        let participants: BTreeSet<_> = participants.into_iter().collect();
        if participants.is_empty() {
            return Err(TwoPcError::InvalidTransition);
        }
        Ok(Self {
            participants,
            prepared: BTreeSet::new(),
            aborted: false,
            phase: CoordinatorPhase::Idle,
        })
    }

    pub fn begin(&mut self) -> Result<CoordinatorAction, TwoPcError> {
        if self.phase != CoordinatorPhase::Idle {
            return Err(TwoPcError::AlreadyDecided);
        }
        self.phase = CoordinatorPhase::Preparing;
        Ok(CoordinatorAction::Prepare)
    }

    pub fn vote_prepared(
        &mut self,
        participant: &NodeId,
    ) -> Result<Option<CoordinatorAction>, TwoPcError> {
        if !self.participants.contains(participant) {
            return Err(TwoPcError::UnknownParticipant);
        }
        if self.phase != CoordinatorPhase::Preparing {
            return Err(TwoPcError::AlreadyDecided);
        }
        self.prepared.insert(participant.clone());
        if self.prepared == self.participants {
            self.phase = CoordinatorPhase::Decided(Decision::Commit);
            return Ok(Some(CoordinatorAction::Commit));
        }
        Ok(None)
    }

    pub fn vote_abort(&mut self, participant: &NodeId) -> Result<CoordinatorAction, TwoPcError> {
        if !self.participants.contains(participant) {
            return Err(TwoPcError::UnknownParticipant);
        }
        if self.phase != CoordinatorPhase::Preparing {
            return Err(TwoPcError::AlreadyDecided);
        }
        self.aborted = true;
        self.phase = CoordinatorPhase::Decided(Decision::Abort);
        Ok(CoordinatorAction::Abort)
    }

    #[must_use]
    pub const fn phase(&self) -> CoordinatorPhase {
        self.phase
    }
    #[must_use]
    pub fn prepared(&self) -> &BTreeSet<NodeId> {
        &self.prepared
    }
    #[must_use]
    pub const fn decision(&self) -> Option<Decision> {
        match self.phase {
            CoordinatorPhase::Decided(decision) => Some(decision),
            _ => None,
        }
    }
    #[must_use]
    pub const fn aborted(&self) -> bool {
        self.aborted
    }
}

#[derive(Clone, Debug)]
pub struct Participant {
    phase: ParticipantPhase,
}

impl Participant {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            phase: ParticipantPhase::Working,
        }
    }
    pub fn prepare(&mut self, can_commit: bool) -> ParticipantAction {
        if self.phase != ParticipantPhase::Working {
            return match self.phase {
                ParticipantPhase::Prepared => ParticipantAction::Prepared,
                ParticipantPhase::Committed => ParticipantAction::Committed,
                ParticipantPhase::Aborted => ParticipantAction::Aborted,
                ParticipantPhase::Working => unreachable!(),
            };
        }
        if can_commit {
            self.phase = ParticipantPhase::Prepared;
            ParticipantAction::Prepared
        } else {
            self.phase = ParticipantPhase::Aborted;
            ParticipantAction::VoteAbort
        }
    }
    pub fn commit(&mut self) -> Result<ParticipantAction, TwoPcError> {
        if self.phase != ParticipantPhase::Prepared {
            return Err(TwoPcError::InvalidTransition);
        }
        self.phase = ParticipantPhase::Committed;
        Ok(ParticipantAction::Committed)
    }
    pub fn abort(&mut self) -> Result<ParticipantAction, TwoPcError> {
        if matches!(self.phase, ParticipantPhase::Committed) {
            return Err(TwoPcError::InvalidTransition);
        }
        self.phase = ParticipantPhase::Aborted;
        Ok(ParticipantAction::Aborted)
    }
    #[must_use]
    pub const fn phase(&self) -> ParticipantPhase {
        self.phase
    }
}

impl Default for Participant {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(name: &str) -> NodeId {
        NodeId::parse(name).expect("valid node")
    }

    #[test]
    fn coordinator_commits_only_after_all_prepared() {
        let mut c = Coordinator::new([node("a"), node("b")]).expect("participants");
        assert_eq!(c.begin().expect("begin"), CoordinatorAction::Prepare);
        assert_eq!(c.vote_prepared(&node("a")).expect("vote"), None);
        assert_eq!(
            c.vote_prepared(&node("b")).expect("vote"),
            Some(CoordinatorAction::Commit)
        );
        assert_eq!(c.decision(), Some(Decision::Commit));
    }

    #[test]
    fn participant_recovery_actions_are_idempotent_at_the_boundary() {
        let mut p = Participant::new();
        assert_eq!(p.prepare(true), ParticipantAction::Prepared);
        assert_eq!(p.commit().expect("commit"), ParticipantAction::Committed);
        assert!(p.commit().is_err());
    }
}
