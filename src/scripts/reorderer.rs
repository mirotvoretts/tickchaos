use crate::domain::{Effect, Operator, Packet, ProxyError};
use rand::rngs::StdRng;
use rand::Rng;
use std::time::Duration;

pub struct Reorderer {
    probability: f64,
    hold: Duration,
}

impl Reorderer {
    pub fn new(probability: f64, hold: Duration) -> Result<Self, ProxyError> {
        if probability.is_nan() || !(0.0..=1.0).contains(&probability) {
            return Err(ProxyError::Config(format!(
                "reorder probability must be in 0.0..=1.0, got {probability}"
            )));
        }
        Ok(Reorderer { probability, hold })
    }
}

impl Operator for Reorderer {
    fn decide(&mut self, _packet: &Packet, rng: &mut StdRng) -> Effect {
        if rng.gen_bool(self.probability) {
            Effect::DelayBy(self.hold)
        } else {
            Effect::Forward
        }
    }

    fn name(&self) -> &'static str {
        "reorderer"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;
    use rand::SeedableRng;

    fn decisions(probability: f64, hold: Duration, seed: u64, count: usize) -> Vec<Effect> {
        let mut reorderer = Reorderer::new(probability, hold).unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        let packet = Packet::new(Bytes::from_static(b"tick"), std::time::Instant::now());
        (0..count)
            .map(|_| reorderer.decide(&packet, &mut rng))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 2 } else { 256 },
            ..ProptestConfig::default()
        })]

        #[test]
        fn prob_zero_never_holds(seed: u64) {
            prop_assert!(decisions(0.0, Duration::from_millis(10), seed, 100)
                .iter()
                .all(|effect| *effect == Effect::Forward));
        }

        #[test]
        fn prob_one_always_holds(seed: u64) {
            prop_assert!(decisions(1.0, Duration::from_millis(10), seed, 100)
                .iter()
                .all(|effect| *effect == Effect::DelayBy(Duration::from_millis(10))));
        }
    }

    #[test]
    fn deterministic_per_seed() {
        assert_eq!(
            decisions(0.5, Duration::from_millis(5), 42, 1000),
            decisions(0.5, Duration::from_millis(5), 42, 1000)
        );
    }

    #[test]
    fn invalid_probability_rejected() {
        assert!(Reorderer::new(f64::NAN, Duration::ZERO).is_err());
        assert!(Reorderer::new(-0.1, Duration::ZERO).is_err());
        assert!(Reorderer::new(1.1, Duration::ZERO).is_err());
    }

    #[test]
    fn hold_duration_passed_through() {
        let hold = Duration::from_millis(37);
        let effects = decisions(1.0, hold, 1, 10);
        assert!(effects
            .iter()
            .all(|effect| *effect == Effect::DelayBy(hold)));
    }
}
