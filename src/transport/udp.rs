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

    pub fn local_addr(&self) -> Result<SocketAddr, ProxyError> {
        self.socket.local_addr().map_err(ProxyError::Io)
    }
}

impl PacketTransport for UdpTransport {
    async fn recv(&mut self) -> Result<Packet, ProxyError> {
        let (n, _from) = self
            .socket
            .recv_from(&mut self.recv_buf)
            .await
            .map_err(ProxyError::Io)?;
        let payload = bytes::Bytes::copy_from_slice(&self.recv_buf[..n]);
        Ok(Packet::new(payload, std::time::Instant::now()))
    }

    async fn send(&self, packet: &Packet, to: SocketAddr) -> Result<(), ProxyError> {
        self.socket
            .send_to(&packet.payload, to)
            .await
            .map_err(ProxyError::Io)?;
        Ok(())
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

    #[tokio::test]
    async fn roundtrip_loopback() {
        let mut receiver = UdpTransport::bind("127.0.0.1:0".parse().unwrap(), 65536, None).unwrap();
        let recv_addr = receiver.local_addr().unwrap();

        let sender = UdpTransport::bind("127.0.0.1:0".parse().unwrap(), 65536, None).unwrap();

        let payload = bytes::Bytes::from_static(b"tickchaos");
        let packet = Packet::new(payload.clone(), std::time::Instant::now());
        sender.send(&packet, recv_addr).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.payload.as_ref(), payload.as_ref());
    }

    #[test]
    fn payload_not_corrupted() {
        use proptest::prelude::*;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mut runner = proptest::test_runner::TestRunner::default();
        runner
            .run(&proptest::collection::vec(any::<u8>(), 0..=1400), |data| {
                rt.block_on(async {
                    let mut transport =
                        UdpTransport::bind("127.0.0.1:0".parse().unwrap(), 65536, None).unwrap();
                    let addr = transport.local_addr().unwrap();

                    let sender =
                        UdpTransport::bind("127.0.0.1:0".parse().unwrap(), 65536, None).unwrap();
                    let payload = bytes::Bytes::from(data.clone());
                    let packet = Packet::new(payload.clone(), std::time::Instant::now());
                    sender.send(&packet, addr).await.unwrap();

                    let received = transport.recv().await.unwrap();
                    prop_assert_eq!(received.payload.as_ref(), payload.as_ref());
                    Ok(())
                })
            })
            .unwrap();
    }
}
