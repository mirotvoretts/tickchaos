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
                Effect::Forward => {
                    self.transport.send(&packet, self.upstream).await?;
                    self.stats.forwarded.fetch_add(1, Ordering::Relaxed);
                }
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
