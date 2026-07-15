use crate::domain::{Effect, Operator, Packet, ProxyError};
use rand::rngs::StdRng;
use rand::Rng;
use std::time::Duration;

pub struct JitterDelay {
    min: Duration,
    max: Duration,
}

impl JitterDelay {
    pub fn new(min: Duration, max: Duration) -> Result<Self, ProxyError> {
        if min > max {
            return Err(ProxyError::Config(format!(
                "jitter min must not exceed max, got {min:?} > {max:?}"
            )));
        }
        Ok(JitterDelay { min, max })
    }
}

impl Operator for JitterDelay {
    fn decide(&mut self, _packet: &Packet, rng: &mut StdRng) -> Effect {
        let min_nanos = u64::try_from(self.min.as_nanos()).unwrap_or(u64::MAX);
        let max_nanos = u64::try_from(self.max.as_nanos()).unwrap_or(u64::MAX);
        Effect::DelayBy(Duration::from_nanos(rng.gen_range(min_nanos..=max_nanos)))
    }

    fn name(&self) -> &'static str {
        "jitter"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;
    use rand::SeedableRng;
    use std::time::Instant;

    fn decisions(min: Duration, max: Duration, seed: u64, count: usize) -> Vec<Effect> {
        let mut jitter = JitterDelay::new(min, max).unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        let packet = Packet::new(Bytes::from_static(b"tick"), Instant::now());
        (0..count)
            .map(|_| jitter.decide(&packet, &mut rng))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 2 } else { 256 },
            ..ProptestConfig::default()
        })]

        #[test]
        fn delay_within_bounds(seed: u64, min_ms in 0u64..50, span_ms in 0u64..50) {
            let min = Duration::from_millis(min_ms);
            let max = Duration::from_millis(min_ms + span_ms);
            let within =
                |effect: &Effect| matches!(effect, Effect::DelayBy(d) if *d >= min && *d <= max);
            prop_assert!(decisions(min, max, seed, 100).iter().all(within));
        }
    }

    #[test]
    fn min_eq_max_exact() {
        let exact = Duration::from_millis(7);
        assert!(decisions(exact, exact, 42, 100)
            .iter()
            .all(|effect| *effect == Effect::DelayBy(exact)));
    }

    #[test]
    fn deterministic_per_seed() {
        let min = Duration::from_millis(1);
        let max = Duration::from_millis(5);
        assert_eq!(decisions(min, max, 42, 1000), decisions(min, max, 42, 1000));
    }

    #[test]
    fn invalid_range_rejected() {
        assert!(JitterDelay::new(Duration::from_millis(5), Duration::from_millis(1)).is_err());
    }
}
