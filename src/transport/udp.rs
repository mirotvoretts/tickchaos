use crate::domain::{Packet, ProxyError};
use crate::transport::PacketTransport;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct UdpTransport {
    socket: UdpSocket,
    recv_buf: Vec<u8>,
}

impl UdpTransport {
    pub fn bind(_listen: SocketAddr, _recv_buf_bytes: usize) -> Result<Self, ProxyError> {
        todo!("socket2: SO_REUSEADDR, set recv buffer size, optional join_multicast_v4, wrap into tokio UdpSocket")
    }
}

impl PacketTransport for UdpTransport {
    async fn recv(&mut self) -> Result<Packet, ProxyError> {
        let _ = (&self.socket, &self.recv_buf);
        todo!("recv_from into reusable buf, wrap payload in Bytes, return Packet")
    }

    async fn send(&self, _packet: &Packet, _to: SocketAddr) -> Result<(), ProxyError> {
        todo!("send_to upstream/downstream")
    }
}
