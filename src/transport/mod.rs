mod udp;

pub use udp::UdpTransport;

use crate::domain::{Packet, ProxyError};
use std::net::SocketAddr;

pub trait PacketTransport {
    fn recv(&mut self)
        -> impl std::future::Future<Output = Result<Packet, ProxyError>> + Send;

    fn send(
        &self,
        packet: &Packet,
        to: SocketAddr,
    ) -> impl std::future::Future<Output = Result<(), ProxyError>> + Send;
}
