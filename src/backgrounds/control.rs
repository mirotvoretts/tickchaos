use crate::backgrounds::Stats;
use std::sync::Arc;

pub struct ControlPlane {
    stats: Arc<Stats>,
}

impl ControlPlane {
    #[must_use]
    pub fn new(stats: Arc<Stats>) -> Self {
        ControlPlane { stats }
    }

    pub fn stats(&self) -> &Arc<Stats> {
        &self.stats
    }
}
