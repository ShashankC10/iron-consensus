#![forbid(unsafe_code)]

//! Deterministic Multi-Paxos proposer/acceptor/learner core.

use iron_core::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Ballot {
    pub round: u64,
    pub proposer: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value {
    pub slot: u64,
    pub command: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    Prepare {
        ballot: Ballot,
    },
    Promise {
        ballot: Ballot,
        accepted: Vec<(Ballot, Value)>,
    },
    Accept {
        ballot: Ballot,
        value: Value,
    },
    Accepted {
        ballot: Ballot,
        acceptor: NodeId,
        value: Value,
    },
    Decision {
        ballot: Ballot,
        value: Value,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Outbound {
    pub to: NodeId,
    pub message: Message,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PaxosError {
    #[error("membership is empty or does not contain this node")]
    InvalidMembership,
    #[error("ballot round overflow")]
    BallotOverflow,
    #[error("only a proposer may propose")]
    NotProposer,
    #[error("slot must be nonzero")]
    InvalidSlot,
}

#[derive(Clone, Debug)]
pub struct Node {
    id: NodeId,
    members: BTreeSet<NodeId>,
    ballot: Ballot,
    promised: Option<Ballot>,
    accepted: BTreeMap<u64, (Ballot, Value)>,
    promises: BTreeSet<NodeId>,
    accepted_votes: BTreeMap<u64, BTreeSet<NodeId>>,
}

impl Node {
    pub fn new(id: NodeId, members: impl IntoIterator<Item = NodeId>) -> Result<Self, PaxosError> {
        let members: BTreeSet<_> = members.into_iter().collect();
        if members.is_empty() || !members.contains(&id) {
            return Err(PaxosError::InvalidMembership);
        }
        Ok(Self {
            ballot: Ballot {
                round: 0,
                proposer: id.clone(),
            },
            id,
            members,
            promised: None,
            accepted: BTreeMap::new(),
            promises: BTreeSet::new(),
            accepted_votes: BTreeMap::new(),
        })
    }
    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    #[must_use]
    pub fn ballot(&self) -> &Ballot {
        &self.ballot
    }
    #[must_use]
    pub fn accepted(&self) -> &BTreeMap<u64, (Ballot, Value)> {
        &self.accepted
    }
    pub fn start_round(&mut self) -> Result<Vec<Outbound>, PaxosError> {
        self.ballot.round = self
            .ballot
            .round
            .checked_add(1)
            .ok_or(PaxosError::BallotOverflow)?;
        self.promises.clear();
        self.promises.insert(self.id.clone());
        Ok(self
            .members
            .iter()
            .filter(|peer| *peer != &self.id)
            .map(|peer| Outbound {
                to: peer.clone(),
                message: Message::Prepare {
                    ballot: self.ballot.clone(),
                },
            })
            .collect())
    }
    pub fn propose(&mut self, value: Value) -> Result<Vec<Outbound>, PaxosError> {
        if value.slot == 0 {
            return Err(PaxosError::InvalidSlot);
        }
        if self.promises.len() < self.majority() {
            return Err(PaxosError::NotProposer);
        }
        Ok(self
            .members
            .iter()
            .map(|peer| Outbound {
                to: peer.clone(),
                message: Message::Accept {
                    ballot: self.ballot.clone(),
                    value: value.clone(),
                },
            })
            .collect())
    }

    /// Restores one accepted value from durable storage. Older ballots never
    /// replace a newer accepted value for the same slot.
    pub fn restore_accepted(&mut self, ballot: Ballot, value: Value) -> Result<(), PaxosError> {
        if value.slot == 0 {
            return Err(PaxosError::InvalidSlot);
        }
        let replace = self
            .accepted
            .get(&value.slot)
            .is_none_or(|(current, _)| ballot > *current);
        if replace {
            self.accepted.insert(value.slot, (ballot, value));
        }
        Ok(())
    }
    pub fn receive(&mut self, from: NodeId, message: Message) -> Result<Vec<Outbound>, PaxosError> {
        match message {
            Message::Prepare { ballot } => self.prepare(from, ballot),
            Message::Promise { ballot, accepted } => self.promise(from, ballot, accepted),
            Message::Accept { ballot, value } => self.accept(from, ballot, value),
            Message::Accepted {
                ballot,
                acceptor,
                value,
            } => self.accepted_vote(from, ballot, acceptor, value),
            Message::Decision { ballot, value } => {
                self.accepted.insert(value.slot, (ballot, value));
                Ok(Vec::new())
            }
        }
    }
    fn prepare(&mut self, proposer: NodeId, ballot: Ballot) -> Result<Vec<Outbound>, PaxosError> {
        if self
            .promised
            .as_ref()
            .is_some_and(|current| current > &ballot)
        {
            return Ok(Vec::new());
        }
        self.promised = Some(ballot.clone());
        Ok(vec![Outbound {
            to: proposer,
            message: Message::Promise {
                ballot,
                accepted: self.accepted.values().cloned().collect(),
            },
        }])
    }
    fn promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        accepted: Vec<(Ballot, Value)>,
    ) -> Result<Vec<Outbound>, PaxosError> {
        if ballot != self.ballot {
            return Ok(Vec::new());
        }
        self.promises.insert(from);
        for (old_ballot, value) in accepted {
            let replace = self
                .accepted
                .get(&value.slot)
                .is_none_or(|(current, _)| old_ballot > *current);
            if replace {
                self.accepted.insert(value.slot, (old_ballot, value));
            }
        }
        Ok(Vec::new())
    }
    fn accept(
        &mut self,
        _from: NodeId,
        ballot: Ballot,
        value: Value,
    ) -> Result<Vec<Outbound>, PaxosError> {
        if self
            .promised
            .as_ref()
            .is_some_and(|current| current > &ballot)
        {
            return Ok(Vec::new());
        }
        self.promised = Some(ballot.clone());
        self.accepted
            .insert(value.slot, (ballot.clone(), value.clone()));
        Ok(vec![Outbound {
            to: ballot.proposer.clone(),
            message: Message::Accepted {
                ballot,
                acceptor: self.id.clone(),
                value,
            },
        }])
    }
    fn accepted_vote(
        &mut self,
        _from: NodeId,
        ballot: Ballot,
        acceptor: NodeId,
        value: Value,
    ) -> Result<Vec<Outbound>, PaxosError> {
        if ballot != self.ballot {
            return Ok(Vec::new());
        }
        let votes = self.accepted_votes.entry(value.slot).or_default();
        votes.insert(acceptor);
        if votes.len() >= self.majority() {
            return Ok(self
                .members
                .iter()
                .map(|peer| Outbound {
                    to: peer.clone(),
                    message: Message::Decision {
                        ballot: ballot.clone(),
                        value: value.clone(),
                    },
                })
                .collect());
        }
        Ok(Vec::new())
    }
    fn majority(&self) -> usize {
        self.members.len() / 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn n(name: &str) -> NodeId {
        NodeId::parse(name).expect("valid")
    }
    #[test]
    fn proposer_forms_a_quorum_and_acceptors_learn() {
        let mut a = Node::new(n("a"), [n("a"), n("b"), n("c")]).expect("node");
        let prepares = a.start_round().expect("round");
        assert_eq!(prepares.len(), 2);
        a.receive(
            n("b"),
            Message::Promise {
                ballot: a.ballot().clone(),
                accepted: Vec::new(),
            },
        )
        .expect("promise");
        let accepts = a
            .propose(Value {
                slot: 1,
                command: b"x".to_vec(),
            })
            .expect("quorum");
        assert_eq!(accepts.len(), 3);
    }
}
