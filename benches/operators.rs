#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::hint::black_box;
use std::time::{Duration, Instant};
use tickchaos::domain::{Operator, Packet, SeqNum};
use tickchaos::flows::Flow;
use tickchaos::scripts::{
    DropSeq, Dropper, Duplicator, GapBurst, JitterDelay, RateLimiter, Reorderer,
};

const TARGET_SEQNUM: u64 = 500;
const MISSED_SEQNUM: u64 = 999_999;

fn packet(seqnum: u64) -> Packet {
    let mut packet = Packet::new(
        bytes::Bytes::from_static(&[0u8; 1400]),
        Instant::now() - Duration::from_millis(1),
    );
    packet.seqnum = Some(SeqNum(seqnum));
    packet
}

fn bench_operator(c: &mut Criterion, name: &str, mut operator: Box<dyn Operator>, seqnum: u64) {
    let mut rng = StdRng::seed_from_u64(42);
    let packet = packet(seqnum);
    c.bench_function(name, |b| {
        b.iter(|| operator.decide(black_box(&packet), &mut rng));
    });
}

fn operators(c: &mut Criterion) {
    bench_operator(
        c,
        "dropper",
        Box::new(Dropper::new(0.05).unwrap()),
        TARGET_SEQNUM,
    );
    bench_operator(
        c,
        "duplicator",
        Box::new(Duplicator::new(0.05).unwrap()),
        TARGET_SEQNUM,
    );
    bench_operator(
        c,
        "jitter",
        Box::new(JitterDelay::new(Duration::from_millis(1), Duration::from_millis(5)).unwrap()),
        TARGET_SEQNUM,
    );
    bench_operator(
        c,
        "reorderer",
        Box::new(Reorderer::new(0.05, Duration::from_millis(5)).unwrap()),
        TARGET_SEQNUM,
    );
    bench_operator(
        c,
        "rate_limiter",
        Box::new(RateLimiter::new(100_000)),
        TARGET_SEQNUM,
    );

    let targets: Vec<SeqNum> = (0..1000).map(SeqNum).collect();
    bench_operator(
        c,
        "drop_seq/hit",
        Box::new(DropSeq::new(targets.clone())),
        TARGET_SEQNUM,
    );
    bench_operator(
        c,
        "drop_seq/miss",
        Box::new(DropSeq::new(targets)),
        MISSED_SEQNUM,
    );
    bench_operator(
        c,
        "gap_burst",
        Box::new(GapBurst::new(SeqNum(TARGET_SEQNUM), 50)),
        TARGET_SEQNUM,
    );
}

fn flow(c: &mut Criterion) {
    let mut flow = Flow::new(vec![
        Box::new(Dropper::new(0.05).unwrap()),
        Box::new(DropSeq::new((0..1000).map(SeqNum).collect())),
        Box::new(JitterDelay::new(Duration::from_millis(1), Duration::from_millis(5)).unwrap()),
    ]);
    let mut rng = StdRng::seed_from_u64(42);
    let packet = packet(MISSED_SEQNUM);
    c.bench_function("flow/three_operators", |b| {
        b.iter(|| flow.decide(black_box(&packet), &mut rng));
    });
}

criterion_group!(benches, operators, flow);
criterion_main!(benches);
