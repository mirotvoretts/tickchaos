use crate::backgrounds::Stats;
use crate::domain::{Effect, Packet, ProxyError, Seed, SequenceExtractor};
use crate::flows::Flow;
use crate::transport::PacketTransport;
use futures::StreamExt;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_util::time::DelayQueue;

/// How many time-shifted packets may wait in the delay queue at once.
///
/// Beyond this the proxy would hold an unbounded share of a market-open burst
/// in memory and become the outage it is meant to simulate; excess packets are
/// counted as `queue_overflows` and dropped instead.
pub const DEFAULT_MAX_IN_FLIGHT: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(limit) => limit,
    None => NonZeroUsize::MIN,
};

pub struct Proxy<T: PacketTransport> {
    transport: T,
    flow: Flow,
    rng: StdRng,
    upstream: SocketAddr,
    stats: Arc<Stats>,
    extractor: Box<dyn SequenceExtractor>,
    max_in_flight: usize,
}

impl<T: PacketTransport> Proxy<T> {
    #[must_use]
    pub fn new(
        transport: T,
        flow: Flow,
        seed: Seed,
        upstream: SocketAddr,
        stats: Arc<Stats>,
        extractor: Box<dyn SequenceExtractor>,
        max_in_flight: NonZeroUsize,
    ) -> Self {
        Proxy {
            transport,
            flow,
            rng: StdRng::seed_from_u64(seed.0),
            upstream,
            stats,
            extractor,
            max_in_flight: max_in_flight.get(),
        }
    }

    pub async fn run(&mut self) -> Result<(), ProxyError> {
        let mut delay_queue: DelayQueue<(Packet, SocketAddr)> = DelayQueue::new();

        loop {
            tokio::select! {
                received = self.transport.recv() => {
                    let mut packet = received?;
                    packet.seqnum = self.extractor.extract(&packet.payload);
                    match self.flow.decide(&packet, &mut self.rng) {
                        Effect::Forward => {
                            Self::send_and_count(&self.transport, &self.stats, &packet, self.upstream).await;
                        }
                        Effect::Drop => {
                            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                        }
                        Effect::DelayBy(delay) => {
                            if self.enqueue(&mut delay_queue, packet, delay) {
                                self.stats.delayed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Effect::DuplicateAfter(delay) => {
                            Self::send_and_count(&self.transport, &self.stats, &packet, self.upstream).await;
                            if self.enqueue(&mut delay_queue, packet, delay) {
                                self.stats.duplicated.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Some(expired) = delay_queue.next(), if !delay_queue.is_empty() => {
                    let (packet, to) = expired.into_inner();
                    Self::send_and_count(&self.transport, &self.stats, &packet, to).await;
                }
            }
        }
    }

    fn enqueue(
        &self,
        delay_queue: &mut DelayQueue<(Packet, SocketAddr)>,
        packet: Packet,
        delay: std::time::Duration,
    ) -> bool {
        if delay_queue.len() >= self.max_in_flight {
            self.stats.queue_overflows.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        delay_queue.insert((packet, self.upstream), delay);
        true
    }

    async fn send_and_count(transport: &T, stats: &Stats, packet: &Packet, to: SocketAddr) {
        match transport.send(packet, to).await {
            Ok(()) => {
                stats.forwarded.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                stats.send_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::{NoopExtractor, Operator, Packet, SeqNum};
    use crate::protocols::MoldUdp64Extractor;
    use bytes::Bytes;
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const MOCK_SEQNUM: u64 = 4711;

    fn moldudp64_packet() -> Packet {
        let mut payload = b"SESSION001".to_vec();
        payload.extend_from_slice(&MOCK_SEQNUM.to_be_bytes());
        payload.extend_from_slice(&1u16.to_be_bytes());
        Packet::new(Bytes::from(payload), Instant::now())
    }

    struct MockTransport {
        recv_calls: AtomicUsize,
        send_calls: AtomicUsize,
        fail_first_send: bool,
    }

    impl MockTransport {
        fn new(fail_first_send: bool) -> Self {
            MockTransport {
                recv_calls: AtomicUsize::new(0),
                send_calls: AtomicUsize::new(0),
                fail_first_send,
            }
        }
    }

    struct FixedEffect(Effect);

    impl Operator for FixedEffect {
        fn decide(&mut self, _packet: &Packet, _rng: &mut StdRng) -> Effect {
            self.0.clone()
        }

        fn name(&self) -> &'static str {
            "fixed_effect"
        }
    }

    impl PacketTransport for MockTransport {
        async fn recv(&mut self) -> Result<Packet, ProxyError> {
            let call = self.recv_calls.fetch_add(1, Ordering::Relaxed) + 1;
            if call <= 2 {
                Ok(moldudp64_packet())
            } else {
                Err(ProxyError::Io(io::Error::other("closed")))
            }
        }

        async fn send(&self, _packet: &Packet, _to: SocketAddr) -> Result<(), ProxyError> {
            let call = self.send_calls.fetch_add(1, Ordering::Relaxed) + 1;
            if self.fail_first_send && call == 1 {
                Err(ProxyError::Io(io::Error::other("send failed")))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn send_error_is_survived_and_counted() {
        let stats = Arc::new(Stats::default());
        let transport = MockTransport::new(true);
        let mut proxy = Proxy::new(
            transport,
            Flow::new(vec![]),
            Seed(1),
            "127.0.0.1:9999".parse().unwrap(),
            Arc::clone(&stats),
            Box::new(NoopExtractor),
            DEFAULT_MAX_IN_FLIGHT,
        );

        let result = proxy.run().await;

        assert!(result.is_err());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.send_errors, 1);
        assert_eq!(snapshot.forwarded, 1);
    }

    struct SeqNumSpy {
        seen: Arc<Mutex<Vec<Option<SeqNum>>>>,
    }

    impl Operator for SeqNumSpy {
        fn decide(&mut self, packet: &Packet, _rng: &mut StdRng) -> Effect {
            self.seen.lock().unwrap().push(packet.seqnum);
            Effect::Forward
        }

        fn name(&self) -> &'static str {
            "seqnum_spy"
        }
    }

    #[tokio::test]
    async fn delay_queue_overflow_is_counted_instead_of_growing() {
        let stats = Arc::new(Stats::default());
        let transport = MockTransport::new(false);
        let mut proxy = Proxy::new(
            transport,
            Flow::new(vec![Box::new(FixedEffect(Effect::DelayBy(
                Duration::from_secs(5),
            )))]),
            Seed(1),
            "127.0.0.1:9999".parse().unwrap(),
            Arc::clone(&stats),
            Box::new(NoopExtractor),
            NonZeroUsize::MIN,
        );

        let _ = proxy.run().await;

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.delayed, 1);
        assert_eq!(snapshot.queue_overflows, 1);
        assert_eq!(snapshot.forwarded, 0);
    }

    #[tokio::test]
    async fn duplicate_is_still_forwarded_when_queue_is_full() {
        let stats = Arc::new(Stats::default());
        let transport = MockTransport::new(false);
        let mut proxy = Proxy::new(
            transport,
            Flow::new(vec![Box::new(FixedEffect(Effect::DuplicateAfter(
                Duration::from_secs(5),
            )))]),
            Seed(1),
            "127.0.0.1:9999".parse().unwrap(),
            Arc::clone(&stats),
            Box::new(NoopExtractor),
            NonZeroUsize::MIN,
        );

        let _ = proxy.run().await;

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.forwarded, 2);
        assert_eq!(snapshot.duplicated, 1);
        assert_eq!(snapshot.queue_overflows, 1);
    }

    #[tokio::test]
    async fn extracted_seqnum_is_visible_to_operators() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let transport = MockTransport::new(false);
        let mut proxy = Proxy::new(
            transport,
            Flow::new(vec![Box::new(SeqNumSpy {
                seen: Arc::clone(&seen),
            })]),
            Seed(1),
            "127.0.0.1:9999".parse().unwrap(),
            Arc::new(Stats::default()),
            Box::new(MoldUdp64Extractor),
            DEFAULT_MAX_IN_FLIGHT,
        );

        let _ = proxy.run().await;

        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some(SeqNum(MOCK_SEQNUM)), Some(SeqNum(MOCK_SEQNUM))]
        );
    }

    #[tokio::test]
    async fn noop_extractor_leaves_seqnum_unset() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let transport = MockTransport::new(false);
        let mut proxy = Proxy::new(
            transport,
            Flow::new(vec![Box::new(SeqNumSpy {
                seen: Arc::clone(&seen),
            })]),
            Seed(1),
            "127.0.0.1:9999".parse().unwrap(),
            Arc::new(Stats::default()),
            Box::new(NoopExtractor),
            DEFAULT_MAX_IN_FLIGHT,
        );

        let _ = proxy.run().await;

        assert_eq!(*seen.lock().unwrap(), vec![None, None]);
    }
}
