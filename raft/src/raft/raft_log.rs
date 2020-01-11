use crate::proto::raftpb::*;
use crate::raft::INVALID_INDEX;

#[derive(Debug, Clone)]
pub struct RaftLog {
    data: Vec<Entry>,
}

impl RaftLog {
    pub fn count(&self) -> u64 {
        self.data.len() as u64
    }
    pub fn term(&self, idx: u64) -> Option<u64> {
        if idx == INVALID_INDEX || idx > self.last_index() {
            None
        } else {
            Some(self.data[idx as usize].term)
        }
    }

    pub fn last_entry(&self) -> Option<&Entry> {
        self.data.last()
    }
    pub fn last_index(&self) -> u64 {
        self.last_entry().map(|i| i.index).unwrap_or(INVALID_INDEX)
    }

    pub fn last_term(&self) -> u64 {
        self.term(self.last_index()).unwrap_or(0)
    }

    pub fn find_last_entry(&self, index: u64) -> Option<&Entry> {
        for e in self.data.iter() {
            if e.index == index {
                return Some(e);
            }
        }
        None
    }

    /// new_entries - list (term, index)
    pub fn remove_duplicate_entries(&mut self, new_entries: Vec<(u64, u64)>) {
        if new_entries.is_empty() {
            return;
        }
    }

    pub fn append_entries(&mut self, mut new_entries: Vec<Entry>) {
        self.data.append(&mut new_entries);
    }

    pub fn get_entries_from(&self, l: u64) -> Vec<Entry> {
        debug_assert!(l >= 1, "next log index should >= 1");
        self.data[(l - 1) as usize..].to_vec()
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

    pub fn can_append(&self, term: u64, index: u64) -> bool {
        if index == INVALID_INDEX {
            return true;
        }
        if index > self.data.len() as u64 {
            return false;
        }
        self.data[index as usize - 1].term == term
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
            if i >= self.data.len() {
                debug_assert_eq!(
                    i,
                    self.data.len(),
                    "try_append error: incoming entries with bigger index than expected"
                );
                self.data.push(e.clone());
            } else {
                self.data[i] = e.clone();
            }
            // Entries index must be continuous
            if i == 0 {
                debug_assert_eq!(self.data[i].index, 1);
            } else {
                debug_assert_eq!(self.data[i].index, self.data[i - 1].index + 1)
            }
        });
        true
    }
}

impl Default for RaftLog {
    fn default() -> Self {
        Self {
            data: Vec::default(),
        }
    }
}
