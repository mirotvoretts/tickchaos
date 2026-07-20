#![allow(clippy::unwrap_used)]

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::time::{Duration, Instant};
use tickchaos::domain::Packet;
use tickchaos::flows::Flow;
use tickchaos::scripts::{Dropper, Duplicator, JitterDelay};
use tickchaos::Effect;

fn build_flow() -> Flow {
    Flow::new(vec![
        Box::new(Dropper::new(0.3).unwrap()),
        Box::new(Duplicator::new(0.2).unwrap()),
        Box::new(JitterDelay::new(Duration::from_millis(1), Duration::from_millis(5)).unwrap()),
    ])
}

fn run(seed: u64, count: usize) -> Vec<Effect> {
    let mut flow = build_flow();
    let mut rng = StdRng::seed_from_u64(seed);
    let base = Instant::now();
    (0..count)
        .map(|i| {
            let packet = Packet::new(
                Bytes::from_static(b"tick"),
                base + Duration::from_millis(i as u64),
            );
            flow.decide(&packet, &mut rng)
        })
        .collect()
}

#[test]
fn same_seed_same_effects() {
    assert_eq!(run(42, 10_000), run(42, 10_000));
}

#[test]
fn different_seed_different_effects() {
    assert_ne!(run(42, 10_000), run(43, 10_000));
}
