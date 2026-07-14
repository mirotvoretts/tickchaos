use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;

pub struct RateLimiter {
    packets_per_sec: u32,
}

impl RateLimiter {
    #[must_use]
    pub fn new(packets_per_sec: u32) -> Self {
        RateLimiter { packets_per_sec }
    }
}

impl Operator for RateLimiter {
    fn decide(&mut self, _packet: &Packet, _rng: &mut StdRng) -> Effect {
        let _ = self.packets_per_sec;
        todo!("rate limit: token bucket, Drop when bucket empty")
    }

    fn name(&self) -> &'static str {
        "rate_limiter"
    }
}
