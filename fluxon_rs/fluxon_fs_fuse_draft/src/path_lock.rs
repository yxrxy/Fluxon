use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathLockMode {
    Read,
    Write,
}

#[derive(Debug, Default)]
struct PathLockCounts {
    readers: usize,
    writer: bool,
}

#[derive(Debug)]
struct PathLockEntry {
    state: Mutex<PathLockCounts>,
    cv: Condvar,
}

impl PathLockEntry {
    fn new() -> Self {
        Self {
            state: Mutex::new(PathLockCounts::default()),
            cv: Condvar::new(),
        }
    }
}

#[derive(Debug, Default)]
pub struct PathLockManager {
    entries: Mutex<BTreeMap<String, Arc<PathLockEntry>>>,
}

impl PathLockManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(&self, key: &str, mode: PathLockMode) -> PathLockGuard {
        let entry = {
            let mut entries = self.entries.lock();
            entries
                .entry(key.to_string())
                .or_insert_with(|| Arc::new(PathLockEntry::new()))
                .clone()
        };
        let mut state = entry.state.lock();
        match mode {
            PathLockMode::Read => {
                while state.writer {
                    entry.cv.wait(&mut state);
                }
                state.readers += 1;
            }
            PathLockMode::Write => {
                while state.writer || state.readers > 0 {
                    entry.cv.wait(&mut state);
                }
                state.writer = true;
            }
        }
        drop(state);
        PathLockGuard { entry, mode }
    }
}

pub struct PathLockGuard {
    entry: Arc<PathLockEntry>,
    mode: PathLockMode,
}

impl Drop for PathLockGuard {
    fn drop(&mut self) {
        let mut state = self.entry.state.lock();
        match self.mode {
            PathLockMode::Read => {
                state.readers = state.readers.saturating_sub(1);
            }
            PathLockMode::Write => {
                state.writer = false;
            }
        }
        self.entry.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::{PathLockManager, PathLockMode};

    #[test]
    fn read_lock_is_reentrant_across_guards() {
        let manager = PathLockManager::new();
        let _g1 = manager.lock("a", PathLockMode::Read);
        let _g2 = manager.lock("a", PathLockMode::Read);
    }
}
