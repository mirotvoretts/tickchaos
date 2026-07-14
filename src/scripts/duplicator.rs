use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;

pub struct Duplicator {
    probability: f64,
}

impl Duplicator {
    #[must_use]
    pub fn new(probability: f64) -> Self {
        Duplicator {
            probability: probability.clamp(0.0, 1.0),
        }
    }
}

impl Operator for Duplicator {
    fn decide(&mut self, _packet: &Packet, _rng: &mut StdRng) -> Effect {
        let _ = self.probability;
        todo!("duplicate: with probability, DuplicateAfter(Duration::ZERO)")
    }

    fn name(&self) -> &'static str {
        "duplicator"
    }
}
