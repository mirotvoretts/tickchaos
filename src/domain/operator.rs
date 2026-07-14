use crate::domain::Packet;
use rand::rngs::StdRng;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    Forward,
    Drop,
    DelayBy(Duration),
    DuplicateAfter(Duration),
}

pub trait Operator: Send {
    fn decide(&mut self, packet: &Packet, rng: &mut StdRng) -> Effect;

    fn name(&self) -> &'static str;
}
