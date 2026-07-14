use crate::domain::{Effect, Operator, Packet, ProxyError};
use rand::rngs::StdRng;
use rand::Rng;

pub struct Dropper {
    probability: f64,
}

impl Dropper {
    pub fn new(probability: f64) -> Result<Self, ProxyError> {
        if probability.is_nan() || !(0.0..=1.0).contains(&probability) {
            return Err(ProxyError::Config(format!(
                "drop probability must be in 0.0..=1.0, got {probability}"
            )));
        }
        Ok(Dropper { probability })
    }
}

impl Operator for Dropper {
    fn decide(&mut self, _packet: &Packet, rng: &mut StdRng) -> Effect {
        if rng.gen_bool(self.probability) {
            Effect::Drop
        } else {
            Effect::Forward
        }
    }

    fn name(&self) -> &'static str {
        "dropper"
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
        let mut dropper = Dropper::new(probability).unwrap();
        let mut rng = StdRng::seed_from_u64(seed);
        let packet = Packet::new(Bytes::from_static(b"tick"));
        (0..count)
            .map(|_| dropper.decide(&packet, &mut rng))
            .collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 2 } else { 256 },
            ..ProptestConfig::default()
        })]

        #[test]
        fn prob_zero_always_forwards(seed: u64) {
            prop_assert!(decisions(0.0, seed, 100)
                .iter()
                .all(|effect| *effect == Effect::Forward));
        }

        #[test]
        fn prob_one_always_drops(seed: u64) {
            prop_assert!(decisions(1.0, seed, 100)
                .iter()
                .all(|effect| *effect == Effect::Drop));
        }
    }

    #[test]
    fn same_seed_same_decisions() {
        assert_eq!(decisions(0.5, 42, 1000), decisions(0.5, 42, 1000));
    }

    #[test]
    fn nan_rejected() {
        assert!(Dropper::new(f64::NAN).is_err());
    }

    #[test]
    fn negative_rejected() {
        assert!(Dropper::new(-0.1).is_err());
    }

    #[test]
    fn above_one_rejected() {
        assert!(Dropper::new(1.1).is_err());
    }
}
