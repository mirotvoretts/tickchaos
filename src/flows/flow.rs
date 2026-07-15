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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rand::SeedableRng;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    struct StubOperator {
        name: &'static str,
        effect: Effect,
        calls: Option<Arc<Mutex<Vec<&'static str>>>>,
    }

    impl Operator for StubOperator {
        fn decide(&mut self, _packet: &Packet, _rng: &mut StdRng) -> Effect {
            if let Some(calls) = &self.calls {
                calls.lock().unwrap().push(self.name);
            }
            self.effect.clone()
        }

        fn name(&self) -> &'static str {
            self.name
        }
    }

    fn recording_stub(
        name: &'static str,
        effect: Effect,
        calls: &Arc<Mutex<Vec<&'static str>>>,
    ) -> Box<dyn Operator> {
        Box::new(StubOperator {
            name,
            effect,
            calls: Some(Arc::clone(calls)),
        })
    }

    fn packet() -> Packet {
        Packet::new(Bytes::from_static(b"tick"), Instant::now())
    }

    fn rng() -> StdRng {
        StdRng::seed_from_u64(42)
    }

    #[test]
    fn empty_flow_forwards() {
        let mut flow = Flow::new(vec![]);
        assert_eq!(flow.decide(&packet(), &mut rng()), Effect::Forward);
    }

    #[test]
    fn first_non_forward_wins() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut flow = Flow::new(vec![
            recording_stub("forward", Effect::Forward, &calls),
            recording_stub("drop", Effect::Drop, &calls),
            recording_stub("delay", Effect::DelayBy(Duration::from_millis(5)), &calls),
        ]);
        assert_eq!(flow.decide(&packet(), &mut rng()), Effect::Drop);
        assert_eq!(*calls.lock().unwrap(), vec!["forward", "drop"]);
    }

    #[test]
    fn operators_called_in_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut flow = Flow::new(vec![
            recording_stub("first", Effect::Forward, &calls),
            recording_stub("second", Effect::Forward, &calls),
            recording_stub("third", Effect::Forward, &calls),
        ]);
        assert_eq!(flow.decide(&packet(), &mut rng()), Effect::Forward);
        assert_eq!(*calls.lock().unwrap(), vec!["first", "second", "third"]);
    }
}
