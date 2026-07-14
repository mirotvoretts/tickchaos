use crate::domain::{Effect, Operator, Packet};
use rand::rngs::StdRng;

pub struct Flow {
    operators: Vec<Box<dyn Operator>>,
}

impl Flow {
    #[must_use]
    pub fn new(operators: Vec<Box<dyn Operator>>) -> Self {
        Flow { operators }
    }

    pub fn decide(&mut self, packet: &Packet, rng: &mut StdRng) -> Effect {
        for operator in &mut self.operators {
            match operator.decide(packet, rng) {
                Effect::Forward => continue,
                effect => return effect,
            }
        }
        Effect::Forward
    }
}
