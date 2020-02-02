use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc, Mutex};

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod raft_log;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use crate::raft::raft_log::RaftLog;
use std::collections::HashSet;
use std::ops::Deref;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, PartialEq, Message)]
pub struct ConfigEntry {
    #[prost(uint64, tag = "100")]
    pub x: u64,
}

#[derive(Clone)]
pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

#[derive(Debug, PartialEq)]
pub enum Role {
    Follower,
    Leader,
    Candidate,
    PreCandidate,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

pub fn resp_msg_type(ty: MessageType) -> MessageType {
    match ty {
        MessageType::MsgAppend => MessageType::MsgAppendResponse,
        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
        _ => unimplemented!(),
    }
}

pub const INVALID_ID: u64 = 0;
pub const INVALID_INDEX: u64 = 0;

const ELECTION_TIMEOUT: usize = 6;
const HEARTBEAT_TIMEOUT: usize = 3;

pub enum CampaignType {
    Election,
    PreElection,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2_A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    apply_ch: UnboundedSender<ApplyMsg>,
    //
    pub id: u64,
    pub term: u64,
    pub leader_id: u64,
    pub vote: u64,
    pub log: RaftLog,
    pub role: Role,
    pub election_elapsed: usize,
    pub random_election_timeout: usize,
    pub heartbeat_elapsed: usize,
    pub heartbeat_timeout: usize,
    pub voters: HashSet<u64>,
    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>,
    pub msg_recv: Receiver<Message>,
    pub msg_send: Arc<Mutex<Sender<Message>>>,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2_A, 2B, 2C).
        let n = peers.len();
        let (tx, rx) = mpsc::channel();
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::new(Default::default()),
            apply_ch,
            id: me as u64 + 1,
            term: 0,
            leader_id: INVALID_ID,
            vote: INVALID_ID,
            log: Default::default(),
            role: Default::default(),
            election_elapsed: 0,
            random_election_timeout: ELECTION_TIMEOUT + rand::random::<usize>() % 7,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
            voters: HashSet::new(),
            next_index: vec![1; n],
            match_index: vec![0; n],
            msg_recv: rx,
            msg_send: Arc::new(Mutex::new(tx)),
            commit_index: 0,
            last_applied: 0,
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut buf = vec![];
        let snap = Snapshot {
            term: self.term,
            vote: self.vote,
            log: self.log.get_entries_from(1),
        };
        labcodec::encode(&snap, &mut buf).unwrap();
        self.persister.save_raft_state(buf);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode::<Snapshot>(data) {
            Ok(o) => {
                // info!("Raft id={} is restoring data: {:?}", self.id, &o);
                self.term = o.term;
                self.vote = o.vote;
                self.log.restore(o.log);
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    fn get_peer_by_id(&self, id: u64) -> &RaftClient {
        debug_assert!(
            1 <= id && id <= self.peers.len() as u64,
            "get_peer_by_id error, id: {}, but peers[].len() = {}",
            id,
            self.peers.len()
        );
        &self.peers[id as usize - 1]
    }

    fn get_id(i: usize) -> u64 {
        i as u64 + 1
    }

    /// Handle response message
    fn step(&mut self, m: Message) -> Result<()> {
        // println!("Raft id={} [step] {:?}", self.id, &m);
        if m.term > self.term {
            if m.msg_type() == MessageType::MsgRequestPreVote {
                // do not update term
            } else if m.msg_type() == MessageType::MsgAppend {
                self.become_follower(m.term, m.from);
            } else {
                self.become_follower(m.term, INVALID_ID);
            }
        } else if m.term < self.term {
            match m.msg_type() {
                MessageType::MsgAppend
                | MessageType::MsgRequestPreVote
                | MessageType::MsgRequestVote => {
                    let mut resp = self.my_message();
                    resp.set_msg_type(resp_msg_type(m.msg_type()));
                    resp.to = m.from;
                    resp.accept = false;
                    self.send_message_to_id(&resp);
                }
                _ => {}
            }
            // info!("Raft id={} receive msg from={} lower_term={}", self.id, m.from, m.term);
            return Ok(());
        }
        // Now m.term needs no consideration
        match m.msg_type() {
            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote => {
                let can_vote = (self.vote == m.from) ||
                    (self.vote == INVALID_ID && self.leader_id == INVALID_ID) ||
                // multiple candidates never elect new leaders
                // as they already vote for themselves
                // so here, a pre-candidate can vote to other pre-candidate with larger term
                    (m.msg_type() == MessageType::MsgRequestPreVote && m.term > self.term);
                let mut resp = self.my_message();
                resp.set_msg_type(resp_msg_type(m.msg_type()));
                resp.to = m.from;
                if can_vote && self.log.is_up_to_date(m.log_term, m.log_index) {
                    /*
                    println!("raft id={} last_index={} thinks candidate id={} log_index={}, log_term={} is up-to-date",
                             self.id,
                             self.last_log_index(),
                             m.from,
                             m.log_index,
                             m.log_term,
                    );
                     */
                    if m.msg_type() == MessageType::MsgRequestVote {
                        self.vote = m.from;
                        self.persist();
                        self.election_elapsed = 0;
                    }
                    resp.term = m.term;
                    resp.accept = true;
                    info!(
                        "Raft id={} grant {:?} to {}",
                        self.id,
                        resp.msg_type(),
                        resp.to
                    );
                } else {
                    resp.accept = false;
                }
                /*
                info!(
                    "Raft id={} responds MsgRequestVote from id={} accept={}",
                    self.id, m.from, resp.accept
                );
                */
                self.send_message_to_id(&resp);
            }
            _ => match self.role {
                Role::Leader => self.step_leader(m)?,
                Role::Candidate | Role::PreCandidate => self.step_candidate(m)?,
                Role::Follower => self.step_follower(m)?,
            },
        }
        Ok(())
    }

    fn step_leader(&mut self, m: Message) -> Result<()> {
        match m.msg_type() {
            MessageType::MsgAppendResponse => {
                self.handle_append_response(m);
            }
            _ => {}
        }
        Ok(())
    }

    fn step_candidate(&mut self, m: Message) -> Result<()> {
        match m.msg_type() {
            MessageType::MsgRequestPreVoteResponse | MessageType::MsgRequestVoteResponse => {
                if self.role == Role::Candidate
                    && m.msg_type() == MessageType::MsgRequestPreVoteResponse
                    || self.role == Role::PreCandidate
                        && m.msg_type() == MessageType::MsgRequestVoteResponse
                {
                    return Ok(());
                }
                if !m.accept {
                    return Ok(());
                }
                self.voters.insert(m.from);
                info!(
                    "Candidate id={} receive {:?} from id={}, accept={}, total={}",
                    self.id,
                    m.msg_type(),
                    m.from,
                    m.accept,
                    self.voters.len()
                );
                if self.has_majority_voters() {
                    match self.role {
                        Role::PreCandidate => {
                            self.campaign(CampaignType::Election);
                        }
                        Role::Candidate => {
                            self.become_leader();
                            self.broadcast_append();
                        }
                        _ => unimplemented!(),
                    }
                }
            }
            MessageType::MsgAppend => {
                self.become_follower(m.term, m.from);
                self.handle_append(m);
            }
            _ => {}
        }
        Ok(())
    }

    fn step_follower(&mut self, m: Message) -> Result<()> {
        match m.msg_type() {
            MessageType::MsgAppend => {
                self.leader_id = m.from;
                self.handle_append(m);
            }
            _ => {}
        }
        Ok(())
    }

    fn tick(&mut self) {
        //        println!("raft id={} role={:?} ticks", self.id, self.role);
        match self.role {
            Role::Follower | Role::Candidate | Role::PreCandidate => self.tick_election(),
            Role::Leader => self.tick_heartbeat(),
        }
    }

    fn has_majority_voters(&self) -> bool {
        self.voters.len() >= (self.peers.len() + 1) / 2
    }
    fn last_log_index(&self) -> u64 {
        self.log.last_index()
    }

    /// If last_log_index is invalid, then last_log_term will be ignored
    /// So it can be any value
    fn last_log_term(&self) -> u64 {
        self.log.last_term()
    }

    /// m.from = self.id
    /// m.term = self.term
    fn my_message(&self) -> Message {
        let mut m = Message::default();
        m.from = self.id;
        m.term = self.term;
        m
    }

    fn become_pre_candidate(&mut self) {
        info!("Raft id={} become pre-candidate", self.id);
        self.voters.clear();
        self.election_elapsed = 0;
        self.leader_id = INVALID_ID;
        self.role = Role::PreCandidate;
        self.update_state();
        self.persist();
    }

    /// Follower will campaign for Leader
    fn campaign(&mut self, campaign_type: CampaignType) {
        info!("Raft id={} starts campaign", self.id);
        match campaign_type {
            CampaignType::PreElection => {
                self.term += 1;
                self.become_pre_candidate();
            }
            CampaignType::Election => self.become_candidate(),
        };
        self.voters.insert(self.id);
        self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.me)
            .for_each(|(i, peer)| {
                let mut msg = self.my_message();
                match campaign_type {
                    CampaignType::PreElection => msg.set_msg_type(MessageType::MsgRequestPreVote),
                    CampaignType::Election => msg.set_msg_type(MessageType::MsgRequestVote),
                }
                msg.to = Self::get_id(i);
                msg.log_index = self.last_log_index();
                msg.log_term = self.last_log_term();
                self.send_message(peer, &msg);
            });
    }

    fn tick_election(&mut self) {
        self.election_elapsed += 1;
        if self.election_elapsed > self.random_election_timeout {
            self.election_elapsed = 0;
            self.campaign(CampaignType::PreElection);
        }
    }

    fn has_majority_replications(&self, n: u64) -> bool {
        self.match_index
            .iter()
            .enumerate()
            .filter(|(i, index)| *i == self.me || **index >= n)
            .count()
            >= (self.peers.len() + 1) / 2
    }

    fn do_commit(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            let entry = self.log.get_entry(self.last_applied);
            let amsg = ApplyMsg {
                command_valid: true,
                command: entry.data.clone(),
                command_index: entry.index,
            };
            /*
            let e: ConfigEntry = labcodec::decode(&entry.data[..]).unwrap();
            info!(
                "Raft id={} commit {:?}, index={}",
                self.id, &e.x, entry.index
            );
             */
            info!("Raft id={} commit, index={}", self.id, entry.index);
            self.apply_ch.unbounded_send(amsg).unwrap();
        }
    }

    fn handle_append_response(&mut self, m: Message) {
        let i = m.from as usize - 1;
        if m.accept {
            self.next_index[i] = m.log_index + 1;
            self.match_index[i] = m.log_index;
            for n in self.commit_index + 1..self.next_index[i] {
                if self.has_majority_replications(n) && self.log.get_entry(n).term == self.term {
                    self.update_commit_index(n);
                }
            }
        } else {
            self.next_index[i] = m.log_index + 1;
        }
    }

    fn update_commit_index(&mut self, commit_index: u64) {
        self.commit_index = commit_index;
        self.do_commit();
    }

    /// Respond when m.msg_type() == MessageType::MsgAppend
    fn handle_append(&mut self, m: Message) {
        debug_assert_eq!(
            self.role,
            Role::Follower,
            "Not Follower when appending entries"
        );
        /*
        info!(
            "Raft id={} last_index={} is handling MsgAppend from={}",
            self.id,
            self.last_log_index(),
            m.from
        );
        */
        self.election_elapsed = 0;
        let mut resp = self.my_message();
        resp.set_msg_type(MessageType::MsgAppendResponse);
        resp.to = m.from;
        if !self.log.can_append(m.log_term, m.log_index) {
            resp.accept = false;
            resp.log_index = self.log.get_possible_prev_index(m.log_index);
        /*
        if !m.entries.is_empty() {
            if m.log_index > self.log.count() {
                info!(
                    "Raft reject MsgAppend: out of range, m.index={}",
                    m.log_index
                );
            } else {
                let e = self.log.get_entry(m.log_index);
                info!(
                    "Raft id={} reject MsgAppend: m.index={}, m.term={}, but log.term={}",
                    self.id, m.log_index, m.log_term, e.term
                );
            }
        }
        */
        } else {
            resp.accept = true;
            if !m.entries.is_empty() {
                // Get the newly commited log index from Leader
                // Set it to resp.log_index to let Leader knows
                resp.log_index = m.entries.last().unwrap().index;
                self.log
                    .try_append(m.log_term, m.log_index, m.entries.clone());
                self.persist();
                /*
                info!(
                    "Raft id={} accept MsgAppend from id={}, last_index={}, total_logs={}",
                    self.id,
                    resp.to,
                    m.entries.last().unwrap().index,
                    self.log.count(),
                );
                */
            }
            if m.commit > self.commit_index {
                // Leader always sends all entries to Follower
                // So the index of last new entry is never less than m.commit
                self.update_commit_index(m.commit);
            }
        }
        self.send_message_to_id(&resp);
    }

    fn broadcast_append(&self) {
        // info!("Raft id={} broadcast MsgAppend", self.id);
        self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.me)
            .for_each(|(i, peer)| {
                let mut msg = self.my_message();
                let prev_log_index = self.next_index[i] - 1;
                msg.set_msg_type(MessageType::MsgAppend);
                msg.to = Self::get_id(i);
                msg.log_index = prev_log_index;
                msg.log_term = self.log.term(prev_log_index).unwrap_or(0);
                msg.entries = self.log.get_entries_from(self.next_index[i]);
                /*
                if !msg.entries.is_empty() {
                    info!(
                        "MsgAppend broadcast from={}, pre_index={}, pre_term={}, r={}",
                        self.id,
                        msg.log_index,
                        msg.log_term,
                        msg.entries.last().unwrap().index
                    );
                }
                */
                msg.commit = self.commit_index;
                self.send_message(peer, &msg);
            });
    }

    fn tick_heartbeat(&mut self) {
        self.heartbeat_elapsed += 1;
        if self.heartbeat_elapsed > self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            self.broadcast_append();
        }
    }

    fn update_state(&mut self) {
        self.state = Arc::new(State {
            term: self.term,
            is_leader: self.role == Role::Leader,
        });
    }

    fn become_candidate(&mut self) {
        assert_ne!(
            self.role,
            Role::Leader,
            "invalid transfer: Leader -> Candidate"
        );
        info!("Raft id={} become candidate", self.id);
        self.leader_id = INVALID_ID;
        self.vote = self.id;
        self.voters.clear();
        self.election_elapsed = 0;
        self.role = Role::Candidate;
        self.update_state();
        self.persist();
    }

    fn become_leader(&mut self) {
        assert_eq!(
            self.role,
            Role::Candidate,
            "invalid transfer: {:?} -> Leader",
            self.role
        );
        info!("Raft id={} become Leader", self.id);
        self.vote = INVALID_ID;
        self.leader_id = self.id;
        for i in 0..self.peers.len() {
            self.next_index[i] = self.last_log_index() + 1;
            self.match_index[i] = self.last_log_index();
        }
        self.role = Role::Leader;
        self.update_state();
        self.persist();
    }

    fn become_follower(&mut self, term: u64, leader_id: u64) {
        info!(
            "Raft id={} become Follower term={}, leader_id={}",
            self.id, term, leader_id
        );
        self.term = term;
        self.leader_id = leader_id;
        self.vote = INVALID_ID;
        self.voters.clear();
        self.election_elapsed = 0;
        self.role = Role::Follower;
        self.update_state();
        self.persist();
    }

    fn send_message_to_id(&self, m: &Message) {
        let peer = self.get_peer_by_id(m.to);
        self.send_message(peer, m);
    }

    fn send_message(&self, peer: &RaftClient, m: &Message) {
        /*
        info!(
            "Raft id={} will send message to Raft id={},type={:?}",
            m.from,
            m.to,
            m.msg_type()
        );
        */
        peer.spawn(peer.handle_message(m).then(|_| Ok(())));
    }
    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    // Your code here if you want the rpc becomes async.
    // Example:
    // ```
    // let peer = &self.peers[server];
    // let (tx, rx) = channel();
    // peer.spawn(
    //     peer.request_vote(&args)
    //         .map_err(Error::Rpc)
    //         .then(move |res| {
    //             tx.send(res);
    //             Ok(())
    //         }),
    // );
    // rx
    // ```

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).
        if self.state.is_leader {
            let entry = Entry {
                term: self.term,
                index: self.last_log_index() + 1,
                data: buf,
            };
            info!("Raft id={} replicates data: index={}", self.id, entry.index);
            self.log.append_entry(entry.clone());
            self.persist();
            // self.broadcast_append();
            Ok((entry.index, entry.term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {}
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    pub raft: Arc<Mutex<Raft>>,
    pub tx_stop: Sender<()>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        info!("Raft id={} joins in", raft.id);
        let arf = Arc::new(Mutex::new(raft));
        let arf2 = arf.clone();
        let (tx, rx) = mpsc::channel();
        let mut instant = Instant::now();
        thread::spawn(move || loop {
            // no message after election timeout
            thread::sleep(Duration::from_millis(6));
            let mut rf = arf2.lock().unwrap();
            loop {
                match rf.msg_recv.try_recv() {
                    Ok(msg) => rf.step(msg).unwrap(),
                    Err(_) => break,
                }
            }
            // ticks every 40 ms(gcd of all operating durations)
            if instant.elapsed() > Duration::from_millis(30) {
                rf.tick();
                instant = Instant::now();
            }
            match rx.try_recv() {
                Ok(()) => return,
                Err(_) => {}
            }
        });
        Node {
            raft: arf,
            tx_stop: tx,
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().state.is_leader()
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        self.raft.lock().unwrap().state.deref().clone()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        let rf = self.raft.lock().unwrap();
        info!("Killing Raft id={}...", rf.id);
        self.tx_stop.send(()).unwrap();
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn handle_message(&self, m: Message) -> RpcFuture<()> {
        let rf = self.raft.lock().unwrap();
        rf.msg_send.lock().unwrap().send(m).unwrap();
        //            .map_err(|e| Error::Rpc(labrpc::Error::Other(
        //                    format!("pass rpc message through sender error: {:?}", e)
        //                )));
        Box::new(futures::future::ok(()))
    }
}
