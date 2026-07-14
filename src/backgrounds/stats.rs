use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    pub forwarded: AtomicU64,
    pub dropped: AtomicU64,
    pub delayed: AtomicU64,
    pub duplicated: AtomicU64,
}

impl Stats {
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            forwarded: self.forwarded.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            delayed: self.delayed.load(Ordering::Relaxed),
            duplicated: self.duplicated.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub forwarded: u64,
    pub dropped: u64,
    pub delayed: u64,
    pub duplicated: u64,
}
