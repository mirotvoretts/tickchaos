use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    pub forwarded: AtomicU64,
    pub dropped: AtomicU64,
    pub delayed: AtomicU64,
    pub duplicated: AtomicU64,
    pub send_errors: AtomicU64,
    pub queue_overflows: AtomicU64,
}

impl Stats {
    #[must_use]
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            forwarded: self.forwarded.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            delayed: self.delayed.load(Ordering::Relaxed),
            duplicated: self.duplicated.load(Ordering::Relaxed),
            send_errors: self.send_errors.load(Ordering::Relaxed),
            queue_overflows: self.queue_overflows.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub forwarded: u64,
    pub dropped: u64,
    pub delayed: u64,
    pub duplicated: u64,
    pub send_errors: u64,
    pub queue_overflows: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_stats_have_zero_send_errors() {
        let stats = Stats::default();
        assert_eq!(stats.snapshot().send_errors, 0);
    }

    #[test]
    fn send_errors_reflected_in_snapshot() {
        let stats = Stats::default();
        stats.send_errors.fetch_add(1, Ordering::Relaxed);
        assert_eq!(stats.snapshot().send_errors, 1);
    }

    #[test]
    fn fresh_stats_have_zero_queue_overflows() {
        let stats = Stats::default();
        assert_eq!(stats.snapshot().queue_overflows, 0);
    }

    #[test]
    fn queue_overflows_reflected_in_snapshot() {
        let stats = Stats::default();
        stats.queue_overflows.fetch_add(2, Ordering::Relaxed);
        assert_eq!(stats.snapshot().queue_overflows, 2);
    }
}
