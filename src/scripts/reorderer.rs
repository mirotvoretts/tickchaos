use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;
use std::time::Duration;

pub struct Reorderer {
    probability: f64,
    hold: Duration,
}

impl Reorderer {
    #[must_use]
    pub fn new(probability: f64, hold: Duration) -> Self {
        Reorderer {
            probability: probability.clamp(0.0, 1.0),
            hold,
        }
    }
}

impl Operator for Reorderer {
    fn decide(&mut self, _packet: &Packet, _rng: &mut StdRng) -> Effect {
        let _ = (self.probability, self.hold);
        todo!("reorder: with probability, DelayBy(hold) to push packet behind later ones")
    }

    fn name(&self) -> &'static str {
        "reorderer"
    }
}
