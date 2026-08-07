#![forbid(unsafe_code)]

//! Deterministic Raft election and replicated-log core.
//!
//! Networking, persistence, and application execution stay outside this crate;
//! callers persist `LogEntry` values through `iron-wal` before applying them.

use iron_core::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEntry {
    pub term: u64,
    pub command: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Message {
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    Vote {
        term: u64,
        voter: NodeId,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendResponse {
        term: u64,
        follower: NodeId,
        success: bool,
        match_index: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Outbound {
    pub to: NodeId,
    pub message: Message,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum RaftError {
    #[error("node membership must contain at least one node")]
    EmptyMembership,
    #[error("only the leader may accept client commands")]
    NotLeader,
    #[error("log index overflow")]
    IndexOverflow,
}

#[derive(Clone, Debug)]
pub struct RaftNode {
    id: NodeId,
    members: BTreeSet<NodeId>,
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    votes: BTreeSet<NodeId>,
    log: Vec<LogEntry>,
    commit_index: u64,
    election_elapsed: u64,
    election_timeout: u64,
    next_index: BTreeMap<NodeId, u64>,
    replication_acks: BTreeSet<NodeId>,
}

impl RaftNode {
    pub fn new(
        id: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        election_timeout: u64,
    ) -> Result<Self, RaftError> {
        let members: BTreeSet<_> = members.into_iter().collect();
        if members.is_empty() || !members.contains(&id) || election_timeout == 0 {
            return Err(RaftError::EmptyMembership);
        }
        Ok(Self {
            id,
            members,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            votes: BTreeSet::new(),
            log: Vec::new(),
            commit_index: 0,
            election_elapsed: 0,
            election_timeout,
            next_index: BTreeMap::new(),
            replication_acks: BTreeSet::new(),
        })
    }

    #[must_use]
    pub fn id(&self) -> &NodeId {
        &self.id
    }
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.current_term
    }
    #[must_use]
    pub const fn commit_index(&self) -> u64 {
        self.commit_index
    }
    #[must_use]
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    /// Advances logical time. Election is deterministic and requires no clock.
    pub fn tick(&mut self) -> Result<Vec<Outbound>, RaftError> {
        self.election_elapsed = self
            .election_elapsed
            .checked_add(1)
            .ok_or(RaftError::IndexOverflow)?;
        if self.role == Role::Leader {
            return Ok(self.heartbeats());
        }
        if self.election_elapsed < self.election_timeout {
            return Ok(Vec::new());
        }
        self.current_term = self
            .current_term
            .checked_add(1)
            .ok_or(RaftError::IndexOverflow)?;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.election_elapsed = 0;
        let (index, term) = self.last_log();
        Ok(self
            .members
            .iter()
            .filter(|peer| *peer != &self.id)
            .map(|peer| Outbound {
                to: peer.clone(),
                message: Message::RequestVote {
                    term: self.current_term,
                    candidate: self.id.clone(),
                    last_log_index: index,
                    last_log_term: term,
                },
            })
            .collect())
    }

    pub fn propose(&mut self, command: Vec<u8>) -> Result<Vec<Outbound>, RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        self.log.push(LogEntry {
            term: self.current_term,
            command,
        });
        self.replication_acks.clear();
        self.replication_acks.insert(self.id.clone());
        Ok(self.heartbeats())
    }

    pub fn receive(&mut self, from: NodeId, message: Message) -> Result<Vec<Outbound>, RaftError> {
        match message {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
            } => self.request_vote(from, term, candidate, last_log_index, last_log_term),
            Message::Vote {
                term,
                voter,
                granted,
            } => self.vote(from, term, voter, granted),
            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.append(
                from,
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            Message::AppendResponse {
                term,
                follower,
                success,
                match_index,
            } => self.append_response(from, term, follower, success, match_index),
        }
    }

    fn request_vote(
        &mut self,
        _from: NodeId,
        term: u64,
        candidate: NodeId,
        index: u64,
        log_term: u64,
    ) -> Result<Vec<Outbound>, RaftError> {
        if term > self.current_term {
            self.become_follower(term);
        }
        let granted = term == self.current_term
            && (self.voted_for.is_none() || self.voted_for.as_ref() == Some(&candidate))
            && self.is_up_to_date(index, log_term);
        if granted {
            self.voted_for = Some(candidate.clone());
            self.election_elapsed = 0;
        }
        Ok(vec![Outbound {
            to: candidate.clone(),
            message: Message::Vote {
                term: self.current_term,
                voter: self.id.clone(),
                granted,
            },
        }])
    }

    fn vote(
        &mut self,
        _from: NodeId,
        term: u64,
        voter: NodeId,
        granted: bool,
    ) -> Result<Vec<Outbound>, RaftError> {
        if term > self.current_term {
            self.become_follower(term);
            return Ok(Vec::new());
        }
        if self.role != Role::Candidate || term != self.current_term || !granted {
            return Ok(Vec::new());
        }
        self.votes.insert(voter);
        if self.votes.len() >= self.majority() {
            self.become_leader();
            return Ok(self.heartbeats());
        }
        Ok(Vec::new())
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        _from: NodeId,
        term: u64,
        leader: NodeId,
        prev: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Result<Vec<Outbound>, RaftError> {
        if term > self.current_term {
            self.become_follower(term);
        }
        let mut success = term == self.current_term;
        if success
            && (prev > self.log.len() as u64
                || (prev > 0 && self.log[prev as usize - 1].term != prev_term))
        {
            success = false;
        }
        if success {
            for (offset, entry) in entries.into_iter().enumerate() {
                let index = prev as usize + offset;
                if index < self.log.len() && self.log[index].term != entry.term {
                    self.log.truncate(index);
                }
                if index >= self.log.len() {
                    self.log.push(entry);
                }
            }
            self.commit_index = leader_commit.min(self.log.len() as u64);
            self.election_elapsed = 0;
        }
        Ok(vec![Outbound {
            to: leader,
            message: Message::AppendResponse {
                term: self.current_term,
                follower: self.id.clone(),
                success,
                match_index: if success { self.log.len() as u64 } else { 0 },
            },
        }])
    }

    fn append_response(
        &mut self,
        _from: NodeId,
        term: u64,
        follower: NodeId,
        success: bool,
        match_index: u64,
    ) -> Result<Vec<Outbound>, RaftError> {
        if term > self.current_term {
            self.become_follower(term);
            return Ok(Vec::new());
        }
        if self.role == Role::Leader && success {
            self.next_index.insert(follower.clone(), match_index);
            self.replication_acks.insert(follower);
            if self.replication_acks.len() >= self.majority() {
                self.commit_index = match_index.min(self.log.len() as u64);
            }
        }
        Ok(Vec::new())
    }

    fn heartbeats(&self) -> Vec<Outbound> {
        self.members
            .iter()
            .filter(|peer| *peer != &self.id)
            .map(|peer| Outbound {
                to: peer.clone(),
                message: Message::AppendEntries {
                    term: self.current_term,
                    leader: self.id.clone(),
                    // Sending the complete suffix keeps this foundation
                    // deterministic and exercises conflict replacement;
                    // a production adapter can use `next_index` batching.
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: self.log.clone(),
                    leader_commit: self.commit_index,
                },
            })
            .collect()
    }
    fn majority(&self) -> usize {
        self.members.len() / 2 + 1
    }
    fn last_log(&self) -> (u64, u64) {
        let index = self.log.len() as u64;
        (index, self.log.last().map_or(0, |entry| entry.term))
    }
    fn is_up_to_date(&self, index: u64, term: u64) -> bool {
        let (mine, mine_term) = self.last_log();
        term > mine_term || (term == mine_term && index >= mine)
    }
    fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes.clear();
        self.election_elapsed = 0;
    }
    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.next_index = self
            .members
            .iter()
            .map(|peer| (peer.clone(), self.log.len() as u64))
            .collect();
        self.election_elapsed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn n(value: &str) -> NodeId {
        NodeId::parse(value).expect("valid")
    }
    #[test]
    fn election_requires_a_majority_and_leader_accepts_commands() {
        let mut a = RaftNode::new(n("a"), [n("a"), n("b"), n("c")], 1).expect("node");
        let requests = a.tick().expect("tick");
        assert_eq!(a.role(), Role::Candidate);
        let vote = a
            .receive(
                n("b"),
                Message::Vote {
                    term: 1,
                    voter: n("b"),
                    granted: true,
                },
            )
            .expect("vote");
        assert_eq!(a.role(), Role::Leader);
        assert_eq!(vote.len(), 2);
        assert_eq!(a.propose(b"set x".to_vec()).expect("proposal").len(), 2);
        assert_eq!(requests.len(), 2);
    }
    #[test]
    fn follower_rejects_conflicting_previous_log() {
        let mut a = RaftNode::new(n("a"), [n("a"), n("b")], 3).expect("node");
        let out = a
            .receive(
                n("b"),
                Message::AppendEntries {
                    term: 1,
                    leader: n("b"),
                    prev_log_index: 1,
                    prev_log_term: 1,
                    entries: Vec::new(),
                    leader_commit: 0,
                },
            )
            .expect("append");
        assert!(!matches!(
            out[0].message,
            Message::AppendResponse { success: true, .. }
        ));
    }
}
