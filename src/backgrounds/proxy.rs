use crate::backgrounds::Stats;
use crate::domain::{Effect, ProxyError, Seed};
use crate::flows::Flow;
use crate::transport::PacketTransport;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct Proxy<T: PacketTransport> {
    transport: T,
    flow: Flow,
    rng: StdRng,
    upstream: SocketAddr,
    stats: Arc<Stats>,
}

impl<T: PacketTransport> Proxy<T> {
    #[must_use]
    pub fn new(
        transport: T,
        flow: Flow,
        seed: Seed,
        upstream: SocketAddr,
        stats: Arc<Stats>,
    ) -> Self {
        Proxy {
            transport,
            flow,
            rng: StdRng::seed_from_u64(seed.0),
            upstream,
            stats,
        }
    }

    pub async fn run(&mut self) -> Result<(), ProxyError> {
        loop {
            let packet = self.transport.recv().await?;
            match self.flow.decide(&packet, &mut self.rng) {
                Effect::Forward => match self.transport.send(&packet, self.upstream).await {
                    Ok(()) => {
                        self.stats.forwarded.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                },
                Effect::Drop => {
                    self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Effect::DelayBy(_delay) => {
                    todo!("schedule delayed send on timing wheel, count delayed")
                }
                Effect::DuplicateAfter(_delay) => {
                    todo!("send now + schedule duplicate, count duplicated")
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::Packet;
    use bytes::Bytes;
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    struct MockTransport {
        recv_calls: AtomicUsize,
        send_calls: AtomicUsize,
    }

    impl PacketTransport for MockTransport {
        async fn recv(&mut self) -> Result<Packet, ProxyError> {
            let call = self.recv_calls.fetch_add(1, Ordering::Relaxed) + 1;
            if call <= 2 {
                Ok(Packet::new(Bytes::from_static(b"tick"), Instant::now()))
            } else {
                Err(ProxyError::Io(io::Error::other("closed")))
            }
        }

        async fn send(&self, _packet: &Packet, _to: SocketAddr) -> Result<(), ProxyError> {
            let call = self.send_calls.fetch_add(1, Ordering::Relaxed) + 1;
            if call == 1 {
                Err(ProxyError::Io(io::Error::other("send failed")))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn send_error_is_survived_and_counted() {
        let stats = Arc::new(Stats::default());
        let transport = MockTransport {
            recv_calls: AtomicUsize::new(0),
            send_calls: AtomicUsize::new(0),
        };
        let mut proxy = Proxy::new(
            transport,
            Flow::new(vec![]),
            Seed(1),
            "127.0.0.1:9999".parse().unwrap(),
            Arc::clone(&stats),
        );

        let result = proxy.run().await;

        assert!(result.is_err());
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.send_errors, 1);
        assert_eq!(snapshot.forwarded, 1);
    }
}
