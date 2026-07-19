use crate::domain::{Effect, Operator, Packet, ProxyError};
use rand::rngs::StdRng;
use rand::Rng;
use std::time::Duration;

pub struct Duplicator {
    probability: f64,
}

impl Duplicator {
    pub fn new(probability: f64) -> Result<Self, ProxyError> {
        if probability.is_nan() || !(0.0..=1.0).contains(&probability) {
            return Err(ProxyError::Config(format!(
                "duplicate probability must be in 0.0..=1.0, got {probability}"
            )));
        }
        Ok(Duplicator { probability })
    }
}

impl Operator for Duplicator {
    fn decide(&mut self, _packet: &Packet, rng: &mut StdRng) -> Effect {
        if rng.gen_bool(self.probability) {
            Effect::DuplicateAfter(Duration::ZERO)
        } else {
            Effect::Forward
        }
    }

    fn name(&self) -> &'static str {
        "duplicator"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use proptest::prelude::*;
    use rand::SeedableRng;

    fn decisions(probability: f64, seed: u64, count: usize) -> Vec<Effect> {
        let mut duplicator = Duplicator::new(probability).unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        let packet = Packet::new(Bytes::from_static(b"tick"), std::time::Instant::now());
        (0..count)
            .map(|_| duplicator.decide(&packet, &mut rng))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 2 } else { 256 },
            ..ProptestConfig::default()
        })]

        #[test]
        fn prob_zero_never_duplicates(seed: u64) {
            prop_assert!(decisions(0.0, seed, 100)
                .iter()
                .all(|effect| *effect == Effect::Forward));
        }

        #[test]
        fn prob_one_always_duplicates(seed: u64) {
            prop_assert!(decisions(1.0, seed, 100)
                .iter()
                .all(|effect| *effect == Effect::DuplicateAfter(Duration::ZERO)));
        }
    }

    #[test]
    fn deterministic_per_seed() {
        assert_eq!(decisions(0.5, 42, 1000), decisions(0.5, 42, 1000));
    }

    #[test]
    fn invalid_probability_rejected() {
        assert!(Duplicator::new(f64::NAN).is_err());
        assert!(Duplicator::new(-0.1).is_err());
        assert!(Duplicator::new(1.1).is_err());
    }
}
