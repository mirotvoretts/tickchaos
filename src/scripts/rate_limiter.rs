use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;
use std::time::Instant;

pub struct RateLimiter {
    capacity: u32,
    tokens: f64,
    last_refill: Option<Instant>,
}

impl RateLimiter {
    #[must_use]
    pub fn new(packets_per_sec: u32) -> Self {
        RateLimiter {
            capacity: packets_per_sec,
            tokens: 0.0,
            last_refill: None,
        }
    }
}

impl Operator for RateLimiter {
    fn decide(&mut self, packet: &Packet, _rng: &mut StdRng) -> Effect {
        match self.last_refill {
            None => {
                self.tokens = f64::from(self.capacity);
                self.last_refill = Some(packet.received_at);
            }
            Some(last_refill) => {
                let elapsed = packet.received_at.saturating_duration_since(last_refill);
                self.tokens = (self.tokens + elapsed.as_secs_f64() * f64::from(self.capacity))
                    .min(f64::from(self.capacity));
                self.last_refill = Some(packet.received_at);
            }
        }

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            Effect::Forward
        } else {
            Effect::Drop
        }
    }

    fn name(&self) -> &'static str {
        "rate_limiter"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rand::SeedableRng;
    use std::time::Duration;

    fn packet_at(base: Instant, millis_offset: u64) -> Packet {
        Packet::new(
            Bytes::from_static(b"tick"),
            base + Duration::from_millis(millis_offset),
        )
    }

    #[test]
    fn burst_within_capacity_forwards() {
        let mut limiter = RateLimiter::new(5);
        let mut rng = StdRng::seed_from_u64(0);
        let base = Instant::now();
        for i in 0..5 {
            assert_eq!(
                limiter.decide(&packet_at(base, i), &mut rng),
                Effect::Forward
            );
        }
    }

    #[test]
    fn burst_over_capacity_drops() {
        let mut limiter = RateLimiter::new(5);
        let mut rng = StdRng::seed_from_u64(0);
        let base = Instant::now();
        for i in 0..5 {
            limiter.decide(&packet_at(base, i), &mut rng);
        }
        assert_eq!(limiter.decide(&packet_at(base, 5), &mut rng), Effect::Drop);
    }

    #[test]
    fn refill_after_one_second() {
        let mut limiter = RateLimiter::new(5);
        let mut rng = StdRng::seed_from_u64(0);
        let base = Instant::now();
        for i in 0..5 {
            limiter.decide(&packet_at(base, i), &mut rng);
        }
        assert_eq!(limiter.decide(&packet_at(base, 5), &mut rng), Effect::Drop);
        let after_refill = packet_at(base, 1_005);
        assert_eq!(limiter.decide(&after_refill, &mut rng), Effect::Forward);
    }

    #[test]
    fn steady_rate_never_drops() {
        let mut limiter = RateLimiter::new(10);
        let mut rng = StdRng::seed_from_u64(0);
        let base = Instant::now();
        let step_ms = 100;
        for i in 0..50u64 {
            let packet = packet_at(base, i * step_ms);
            assert_eq!(limiter.decide(&packet, &mut rng), Effect::Forward);
        }
    }
}
