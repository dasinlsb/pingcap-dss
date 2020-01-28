use crate::proto::raftpb::*;
use crate::raft::INVALID_INDEX;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub struct RaftLog {
    data: RefCell<Vec<Entry>>,
}

impl RaftLog {
    pub fn count(&self) -> u64 {
        self.data.borrow().len() as u64
    }
    pub fn term(&self, idx: u64) -> Option<u64> {
        if idx == INVALID_INDEX || idx > self.last_index() {
            None
        } else {
            Some(self.data.borrow()[(idx - 1) as usize].term)
        }
    }

    pub fn last_entry(&self) -> Option<Entry> {
        self.data.borrow().last().cloned()
    }
    pub fn last_index(&self) -> u64 {
        self.last_entry().map(|i| i.index).unwrap_or(INVALID_INDEX)
    }

    pub fn last_term(&self) -> u64 {
        self.term(self.last_index()).unwrap_or(0)
    }

    pub fn append_entries(&self, mut new_entries: Vec<Entry>) {
        for (i, entry) in new_entries.iter().enumerate() {
            debug_assert_eq!(
                self.count() + 1 + i as u64,
                entry.index,
                "raft-log append entries index is not count+1"
            );
        }
        self.data.borrow_mut().append(&mut new_entries);
    }

    pub fn append_entry(&self, entry: Entry) {
        debug_assert_eq!(
            entry.index,
            self.count() + 1,
            "raft-log append entry index is not count+1"
        );
        self.data.borrow_mut().push(entry);
    }

    pub fn get_entries_from(&self, l: u64) -> Vec<Entry> {
        debug_assert!(l >= 1, "next log index should >= 1");
        self.data.borrow()[(l - 1) as usize..].to_vec()
    }

    pub fn get_entry(&self, i: u64) -> Entry {
        debug_assert!(
            1 <= i && i <= self.count(),
            "raft-log get entry out of range"
        );
        self.data.borrow()[i as usize - 1].clone()
    }

    /// If the last entry(term, index) from candidate is up-to-date
    pub fn is_up_to_date(&self, term: u64, index: u64) -> bool {
        if self.last_index() == INVALID_INDEX {
            return true;
        }
        if index == INVALID_INDEX {
            return false;
        }
        term > self.last_term() || (term == self.last_term() && index >= self.last_index())
    }

    pub fn get_possible_prev_index(&self, index: u64) -> u64 {
        if index > self.count() {
            self.last_index()
        } else {
            let mut i = index;
            while i > 0 && self.get_entry(i).term == self.get_entry(index).term {
                i -= 1;
            }
            i
        }
    }

    pub fn can_append(&self, term: u64, index: u64) -> bool {
        if index == INVALID_INDEX {
            return true;
        }
        if index > self.count() {
            return false;
        }
        self.data.borrow()[index as usize - 1].term == term
    }

    pub fn restore(&mut self, data: Vec<Entry>) {
        self.data = RefCell::new(data);
    }

    pub fn try_append(&mut self, term: u64, index: u64, entries: Vec<Entry>) -> bool {
        if !self.can_append(term, index) {
            return false;
        }
        if entries.is_empty() {
            return true;
        }
        for i in 1..entries.len() {
            debug_assert_eq!(entries[i].index, entries[i - 1].index + 1);
        }

        // Find first invalid entry and remove everything following
        entries.iter().for_each(|e| {
            let i = e.index as usize - 1;
            let mut data = self.data.borrow_mut();
            if i >= data.len() {
                debug_assert_eq!(
                    i,
                    data.len(),
                    "try_append error: incoming entries with bigger index than expected"
                );
                data.push(e.clone());
            } else {
                data[i] = e.clone();
            }
            // Entries index must be continuous
            if i == 0 {
                debug_assert_eq!(data[i].index, 1);
            } else {
                debug_assert_eq!(data[i].index, data[i - 1].index + 1)
            }
        });
        true
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self {
            data: RefCell::new(vec![]),
        }
    }
}
