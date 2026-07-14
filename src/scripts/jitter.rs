use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;
use std::time::Duration;

pub struct JitterDelay {
    min: Duration,
    max: Duration,
}

impl JitterDelay {
    #[must_use]
    pub fn new(min: Duration, max: Duration) -> Self {
        JitterDelay { min, max }
    }
}

impl Operator for JitterDelay {
    fn decide(&mut self, _packet: &Packet, _rng: &mut StdRng) -> Effect {
        let _ = (self.min, self.max);
        todo!("jitter: uniform delay in [min, max] via seeded rng")
    }

    fn name(&self) -> &'static str {
        "jitter"
    }
}
