use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;
use rand::Rng;

pub struct Dropper {
    probability: f64,
}

impl Dropper {
    #[must_use]
    pub fn new(probability: f64) -> Self {
        Dropper {
            probability: probability.clamp(0.0, 1.0),
        }
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
