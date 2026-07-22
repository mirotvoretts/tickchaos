use crate::domain::{Packet, ProxyError};
use crate::transport::PacketTransport;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

#[derive(Debug)]
pub struct UdpTransport {
    socket: UdpSocket,
    recv_buf: Vec<u8>,
}

impl UdpTransport {
    pub fn bind(
        listen: SocketAddr,
        recv_buf_bytes: usize,
        multicast: Option<SocketAddr>,
    ) -> Result<Self, ProxyError> {
        let socket =
            Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).map_err(|source| {
                ProxyError::Bind {
                    addr: listen,
                    source,
                }
            })?;
        socket
            .set_reuse_address(true)
            .map_err(|source| ProxyError::Bind {
                addr: listen,
                source,
            })?;
        socket
            .set_recv_buffer_size(recv_buf_bytes)
            .map_err(|source| ProxyError::Bind {
                addr: listen,
                source,
            })?;
        socket
            .bind(&listen.into())
            .map_err(|source| ProxyError::Bind {
                addr: listen,
                source,
            })?;
        socket
            .set_nonblocking(true)
            .map_err(|source| ProxyError::Bind {
                addr: listen,
                source,
            })?;

        if let Some(group) = multicast {
            let SocketAddr::V4(group_v4) = group else {
                return Err(ProxyError::MulticastJoin {
                    group,
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "multicast group must be IPv4",
                    ),
                });
            };
            socket
                .join_multicast_v4(group_v4.ip(), &std::net::Ipv4Addr::UNSPECIFIED)
                .map_err(|source| ProxyError::MulticastJoin { group, source })?;
        }

        let std_socket: std::net::UdpSocket = socket.into();
        let socket = UdpSocket::from_std(std_socket).map_err(|source| ProxyError::Bind {
            addr: listen,
            source,
        })?;

        Ok(UdpTransport {
            socket,
            recv_buf: vec![0u8; 65536],
        })
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

#[cfg(test)]
#[cfg(not(miri))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_ephemeral_port_succeeds() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        assert!(UdpTransport::bind(addr, 65536, None).is_ok());
    }

    #[tokio::test]
    async fn bind_error_reports_addr() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let held = std::net::UdpSocket::bind(addr).unwrap();
        let taken_addr = held.local_addr().unwrap();

        let err = UdpTransport::bind(taken_addr, 65536, None).unwrap_err();
        let message = err.to_string();
        assert!(message.contains(&taken_addr.to_string()));
    }
}
