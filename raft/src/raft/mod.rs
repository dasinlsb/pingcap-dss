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

pub const INVALID_ID: u64 = 0;
pub const INVALID_INDEX: u64 = 0;

const ELECTION_TIMEOUT: usize = 7;
const HEARTBEAT_TIMEOUT: usize = 4;

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
            random_election_timeout: ELECTION_TIMEOUT + rand::random::<usize>() % 4,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
            voters: HashSet::new(),
            next_index: vec![1; n],
            match_index: vec![0; n],
            msg_recv: rx,
            msg_send: Arc::new(Mutex::new(tx)),
        };
        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
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
        //        println!("[step] {:?}", &m);
        if m.term > self.term {
            if m.msg_type() == MessageType::MsgAppend {
                self.become_follower(m.term, m.from);
            } else {
                self.become_follower(m.term, INVALID_ID);
            }
        } else if m.term < self.term {
            if m.msg_type() == MessageType::MsgAppend {
                let mut resp = self.my_message();
                resp.set_msg_type(MessageType::MsgAppendResponse);
                resp.to = m.from;
                resp.accept = false;
                self.send_message_to_id(&resp);
                return Ok(());
            } else if m.msg_type() == MessageType::MsgRequestVote {
                let mut resp = self.my_message();
                resp.set_msg_type(MessageType::MsgRequestVoteResponse);
                resp.to = m.from;
                resp.accept = false;
                self.send_message_to_id(&resp);
                return Ok(());
            } else {
                // Respond nothing
            }
            return Ok(());
        }
        // Now m.term needs no consideration
        match m.msg_type() {
            MessageType::MsgRequestVote => {
                let can_vote = (self.vote == m.from) || (self.vote == INVALID_ID);
                self.vote = m.from;
                let mut resp = self.my_message();
                resp.set_msg_type(MessageType::MsgRequestVoteResponse);
                resp.to = m.from;
                if can_vote && self.log.is_up_to_date(m.term, m.log_index) {
                    resp.accept = true;
                } else {
                    resp.accept = false;
                }
                info!(
                    "Raft id={} responds MsgRequestVote from id={} accept={}",
                    self.id, m.from, resp.accept
                );
                self.send_message_to_id(&resp);
            }
            _ => match self.role {
                Role::Leader => self.step_leader(m)?,
                Role::Candidate => self.step_candidate(m)?,
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
            MessageType::MsgRequestVoteResponse => {
                if !m.accept {
                    return Ok(());
                }
                self.voters.insert(m.from);
                info!("Candidate id={} receive MsgRequestVoteResponse from id={}, accept={}, total={}",
                      self.id,
                      m.from,
                      m.accept,
                      self.voters.len()
                );
                if self.has_majority_voters() {
                    self.become_leader();
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
                self.handle_append(m);
            }
            _ => {}
        }
        Ok(())
    }

    fn tick(&mut self) {
        //        println!("raft id={} role={:?} ticks", self.id, self.role);
        match self.role {
            Role::Follower | Role::Candidate => self.tick_election(),
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

    /// Follower will campaign for Leader
    fn campaign(&mut self) {
        info!("Raft id={} starts campaign", self.id);
        self.become_candidate();
        self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.me)
            .for_each(|(i, peer)| {
                let mut msg = self.my_message();
                msg.set_msg_type(MessageType::MsgRequestVote);
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
            self.campaign();
        }
    }

    fn handle_append_response(&mut self, m: Message) {
        let i = m.from as usize - 1;
        if m.accept {
            self.next_index[i] = self.last_log_index() + 1;
            self.match_index[i] = self.last_log_index();
        } else {
            self.next_index[i] -= 1;
        }
    }

    /// Respond when m.msg_type() == MessageType::MsgAppend
    fn handle_append(&mut self, m: Message) {
        self.election_elapsed = 0;
        self.log.try_append(m.term, m.log_index, m.entries);
        let mut resp = self.my_message();
        resp.set_msg_type(MessageType::MsgAppendResponse);
        resp.to = m.from;
        resp.accept = true;
        self.send_message_to_id(&resp);
    }

    fn broadcast_append(&self) {
        info!("Raft id={} broadcast MsgAppend", self.id);
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
        self.term += 1;
        self.vote = self.id;
        self.voters.clear();
        self.voters.insert(self.id);
        self.election_elapsed = 0;
        self.role = Role::Candidate;
        self.update_state();
    }

    fn become_leader(&mut self) {
        assert_eq!(
            self.role,
            Role::Candidate,
            "invalid transfer: {:?} -> Leader",
            self.role
        );
        info!("Raft id={} become Leader", self.id);
        self.leader_id = self.id;
        self.role = Role::Leader;
        self.update_state();
    }

    fn become_follower(&mut self, term: u64, leader_id: u64) {
        info!(
            "Raft id={} become Follower term={}, leader_id={}",
            self.id, term, leader_id
        );
        self.role = Role::Follower;
        self.term = term;
        self.leader_id = leader_id;
        self.vote = INVALID_ID;
        self.voters.clear();
        self.election_elapsed = 0;
        for i in 0..self.peers.len() {
            self.next_index[i] = 1;
            self.match_index[i] = 0;
        }
        self.update_state();
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
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;

        let _ = &self.apply_ch;
        let _ = &self.log;
    }
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
            thread::sleep(Duration::from_millis(5));
            let mut rf = arf2.lock().unwrap();
            loop {
                match rf.msg_recv.try_recv() {
                    Ok(msg) => rf.step(msg).unwrap(),
                    Err(_) => break,
                }
            }
            // ticks every 50 ms(gcd of all operating durations)
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
        crate::your_code_here(command)
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
        self.tx_stop.send(()).unwrap();
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn handle_message(&self, m: Message) -> RpcFuture<()> {
        /*
        info!(
            "[Rpc] message from={} to={} type={:?}",
            m.from,
            m.to,
            m.msg_type()
        );
        */
        let rf = self.raft.lock().unwrap();
        rf.msg_send.lock().unwrap().send(m).unwrap();
        //            .map_err(|e| Error::Rpc(labrpc::Error::Other(
        //                    format!("pass rpc message through sender error: {:?}", e)
        //                )));
        Box::new(futures::future::ok(()))
    }
}
