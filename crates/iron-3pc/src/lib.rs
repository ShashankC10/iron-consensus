#![forbid(unsafe_code)]

//! Three-Phase Commit state transitions with explicit timeout semantics.

use iron_core::NodeId;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Idle,
    Preparing,
    PreCommit,
    Committed,
    Aborted,
}

impl Default for Phase {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Prepare,
    PreCommit,
    Commit,
    Abort,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ThreePcError {
    #[error("transaction is not in the required phase")]
    InvalidPhase,
    #[error("unknown participant")]
    UnknownParticipant,
    #[error("three-phase commit cannot guarantee non-blocking progress under arbitrary partitions")]
    PartitionMayBlock,
}

#[derive(Clone, Debug)]
pub struct Coordinator {
    participants: BTreeSet<NodeId>,
    prepared: BTreeSet<NodeId>,
    precommitted: BTreeSet<NodeId>,
    phase: Phase,
}

impl Coordinator {
    pub fn new(participants: impl IntoIterator<Item = NodeId>) -> Result<Self, ThreePcError> {
        let participants: BTreeSet<_> = participants.into_iter().collect();
        if participants.is_empty() {
            return Err(ThreePcError::InvalidPhase);
        }
        Ok(Self {
            participants,
            prepared: BTreeSet::new(),
            precommitted: BTreeSet::new(),
            phase: Phase::Idle,
        })
    }
    pub fn begin(&mut self) -> Result<Action, ThreePcError> {
        if self.phase != Phase::Idle {
            return Err(ThreePcError::InvalidPhase);
        }
        self.phase = Phase::Preparing;
        Ok(Action::Prepare)
    }
    pub fn prepared(&mut self, participant: &NodeId) -> Result<Option<Action>, ThreePcError> {
        if !self.participants.contains(participant) {
            return Err(ThreePcError::UnknownParticipant);
        }
        if self.phase != Phase::Preparing {
            return Err(ThreePcError::InvalidPhase);
        }
        self.prepared.insert(participant.clone());
        if self.prepared == self.participants {
            self.phase = Phase::PreCommit;
            Ok(Some(Action::PreCommit))
        } else {
            Ok(None)
        }
    }
    pub fn precommitted(&mut self, participant: &NodeId) -> Result<Option<Action>, ThreePcError> {
        if !self.participants.contains(participant) {
            return Err(ThreePcError::UnknownParticipant);
        }
        if self.phase != Phase::PreCommit {
            return Err(ThreePcError::InvalidPhase);
        }
        self.precommitted.insert(participant.clone());
        if self.precommitted == self.participants {
            self.phase = Phase::Committed;
            Ok(Some(Action::Commit))
        } else {
            Ok(None)
        }
    }
    pub fn timeout(&mut self) -> Result<Action, ThreePcError> {
        match self.phase {
            Phase::Preparing => {
                self.phase = Phase::Aborted;
                Ok(Action::Abort)
            }
            Phase::PreCommit => Err(ThreePcError::PartitionMayBlock),
            _ => Err(ThreePcError::InvalidPhase),
        }
    }
    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }
}

#[derive(Clone, Debug, Default)]
pub struct Participant {
    phase: Phase,
}

impl Participant {
    #[must_use]
    pub const fn new() -> Self {
        Self { phase: Phase::Idle }
    }
    pub fn prepare(&mut self, can_commit: bool) -> Result<Action, ThreePcError> {
        if self.phase != Phase::Idle {
            return Err(ThreePcError::InvalidPhase);
        }
        if can_commit {
            self.phase = Phase::Preparing;
            Ok(Action::Prepare)
        } else {
            self.phase = Phase::Aborted;
            Ok(Action::Abort)
        }
    }
    pub fn precommit(&mut self) -> Result<Action, ThreePcError> {
        if self.phase != Phase::Preparing {
            return Err(ThreePcError::InvalidPhase);
        }
        self.phase = Phase::PreCommit;
        Ok(Action::PreCommit)
    }
    pub fn commit(&mut self) -> Result<Action, ThreePcError> {
        if self.phase != Phase::PreCommit {
            return Err(ThreePcError::InvalidPhase);
        }
        self.phase = Phase::Committed;
        Ok(Action::Commit)
    }
    pub fn timeout(&mut self) -> Result<Action, ThreePcError> {
        match self.phase {
            Phase::Preparing => {
                self.phase = Phase::Aborted;
                Ok(Action::Abort)
            }
            Phase::PreCommit => Err(ThreePcError::PartitionMayBlock),
            _ => Err(ThreePcError::InvalidPhase),
        }
    }
    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(name: &str) -> NodeId {
        NodeId::parse(name).expect("valid")
    }
    #[test]
    fn precommit_requires_all_votes_and_partition_is_explicit() {
        let mut c = Coordinator::new([node("a"), node("b")]).expect("participants");
        assert_eq!(c.begin().expect("begin"), Action::Prepare);
        assert_eq!(c.prepared(&node("a")).expect("vote"), None);
        assert_eq!(
            c.prepared(&node("b")).expect("vote"),
            Some(Action::PreCommit)
        );
        assert_eq!(c.timeout(), Err(ThreePcError::PartitionMayBlock));
    }
}
