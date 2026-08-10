use crate::domain::{Direction, FixMessage, ProxyError};
use crate::protocols::FixFramer;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

const READ_CHUNK_BYTES: usize = 8 * 1024;
const LISTEN_BACKLOG: u32 = 1024;

/// Explicit socket buffer sizes for one FIX connection.
///
/// Left to the system defaults a burst produces TCP backpressure that the
/// client under test would read as latency injected by the proxy rather than
/// by the scenario, which makes the run unusable as evidence.
#[derive(Debug, Clone, Copy)]
pub struct TcpBuffers {
    pub recv_bytes: u32,
    pub send_bytes: u32,
}

impl Default for TcpBuffers {
    fn default() -> Self {
        TcpBuffers {
            recv_bytes: 256 * 1024,
            send_bytes: 256 * 1024,
        }
    }
}

/// Bind a listener whose accepted connections inherit `buffers`.
///
/// Receive buffer sizing has to happen before the handshake for window scaling
/// to be negotiated against it, which is why it is set here rather than on each
/// accepted stream.
pub fn listen(addr: SocketAddr, buffers: TcpBuffers) -> Result<TcpListener, ProxyError> {
    let bind_error = |source: std::io::Error| ProxyError::Bind { addr, source };

    let socket = TcpSocket::new_v4().map_err(bind_error)?;
    socket.set_reuseaddr(true).map_err(bind_error)?;
    socket
        .set_recv_buffer_size(buffers.recv_bytes)
        .map_err(bind_error)?;
    socket
        .set_send_buffer_size(buffers.send_bytes)
        .map_err(bind_error)?;
    socket.bind(addr).map_err(bind_error)?;
    socket.listen(LISTEN_BACKLOG).map_err(bind_error)
}

/// One FIX session leg: a TCP stream with its own framer.
#[derive(Debug)]
pub struct FixConnection {
    stream: TcpStream,
    framer: FixFramer,
    read_buf: Vec<u8>,
}

impl FixConnection {
    pub async fn connect(
        addr: SocketAddr,
        direction: Direction,
        buffers: TcpBuffers,
    ) -> Result<Self, ProxyError> {
        let bind_error = |source: std::io::Error| ProxyError::Bind { addr, source };

        let socket = TcpSocket::new_v4().map_err(bind_error)?;
        socket
            .set_recv_buffer_size(buffers.recv_bytes)
            .map_err(bind_error)?;
        socket
            .set_send_buffer_size(buffers.send_bytes)
            .map_err(bind_error)?;
        let stream = socket.connect(addr).await.map_err(bind_error)?;
        stream.set_nodelay(true).map_err(bind_error)?;

        Ok(Self::wrap(stream, direction))
    }

    pub fn from_accepted(stream: TcpStream, direction: Direction) -> Result<Self, ProxyError> {
        stream.set_nodelay(true).map_err(ProxyError::Io)?;
        Ok(Self::wrap(stream, direction))
    }

    fn wrap(stream: TcpStream, direction: Direction) -> Self {
        FixConnection {
            stream,
            framer: FixFramer::new(direction),
            read_buf: vec![0u8; READ_CHUNK_BYTES],
        }
    }

    pub fn nodelay(&self) -> Result<bool, ProxyError> {
        self.stream.nodelay().map_err(ProxyError::Io)
    }

    pub fn peer_addr(&self) -> Result<SocketAddr, ProxyError> {
        self.stream.peer_addr().map_err(ProxyError::Io)
    }

    /// Next complete message, or `None` once the peer closed cleanly on a
    /// message boundary.
    pub async fn recv_message(&mut self) -> Result<Option<FixMessage>, ProxyError> {
        loop {
            if let Some(message) = next_framed(&mut self.framer)? {
                return Ok(Some(message));
            }
            let read = self
                .stream
                .read(&mut self.read_buf)
                .await
                .map_err(ProxyError::Io)?;
            if read == 0 {
                return closed_at_boundary(&self.framer);
            }
            self.framer
                .extend(self.read_buf.get(..read).unwrap_or_default());
        }
    }

    pub async fn send_raw(&mut self, bytes: &[u8]) -> Result<(), ProxyError> {
        self.stream.write_all(bytes).await.map_err(ProxyError::Io)
    }

    /// Split into halves so a relay task can read one leg while another task
    /// writes it, which a single `&mut self` cannot express.
    #[must_use]
    pub fn split(self) -> (FixReader, FixWriter) {
        let (read_half, write_half) = self.stream.into_split();
        (
            FixReader {
                half: read_half,
                framer: self.framer,
                read_buf: self.read_buf,
            },
            FixWriter { half: write_half },
        )
    }
}

pub struct FixReader {
    half: OwnedReadHalf,
    framer: FixFramer,
    read_buf: Vec<u8>,
}

impl FixReader {
    pub async fn recv_message(&mut self) -> Result<Option<FixMessage>, ProxyError> {
        loop {
            if let Some(message) = next_framed(&mut self.framer)? {
                return Ok(Some(message));
            }
            let read = self
                .half
                .read(&mut self.read_buf)
                .await
                .map_err(ProxyError::Io)?;
            if read == 0 {
                return closed_at_boundary(&self.framer);
            }
            self.framer
                .extend(self.read_buf.get(..read).unwrap_or_default());
        }
    }
}

pub struct FixWriter {
    half: OwnedWriteHalf,
}

impl FixWriter {
    pub async fn send_raw(&mut self, bytes: &[u8]) -> Result<(), ProxyError> {
        self.half.write_all(bytes).await.map_err(ProxyError::Io)
    }

    pub async fn shutdown(&mut self) -> Result<(), ProxyError> {
        self.half.shutdown().await.map_err(ProxyError::Io)
    }
}

fn next_framed(framer: &mut FixFramer) -> Result<Option<FixMessage>, ProxyError> {
    framer
        .next_message(Instant::now())
        .map_err(|error| ProxyError::Framing(error.to_string()))
}

fn closed_at_boundary(framer: &FixFramer) -> Result<Option<FixMessage>, ProxyError> {
    if framer.buffered() == 0 {
        return Ok(None);
    }
    Err(ProxyError::Framing(format!(
        "peer closed with {} buffered bytes of an incomplete message",
        framer.buffered()
    )))
}

#[cfg(test)]
#[cfg(not(miri))]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::domain::MsgSeqNum;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    fn buffers() -> TcpBuffers {
        TcpBuffers {
            recv_bytes: 65_536,
            send_bytes: 65_536,
        }
    }

    fn frame(body: &str) -> Vec<u8> {
        let body = body.replace('|', "\x01");
        let mut message = format!("8=FIX.4.2\x019={}\x01{}", body.len(), body);
        let checksum: u32 = message.bytes().map(u32::from).sum::<u32>() % 256;
        message.push_str(&format!("10={checksum:03}\x01"));
        message.into_bytes()
    }

    async fn connected_pair() -> (FixConnection, TcpStream) {
        let listener = listen("127.0.0.1:0".parse().unwrap(), buffers()).unwrap();
        let addr = listener.local_addr().unwrap();

        let accept = tokio::spawn(async move { listener.accept().await });
        let client = TcpStream::connect(addr).await.unwrap();
        let (accepted, _) = accept.await.unwrap().unwrap();

        let connection =
            FixConnection::from_accepted(accepted, Direction::UpstreamToClient).unwrap();
        (connection, client)
    }

    #[tokio::test]
    async fn connect_reports_addr_on_failure() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed = listener.local_addr().unwrap();
        drop(listener);

        let err = FixConnection::connect(closed, Direction::ClientToUpstream, buffers())
            .await
            .unwrap_err();
        assert!(err.to_string().contains(&closed.to_string()));
    }

    #[tokio::test]
    async fn both_ends_disable_nagle() {
        let listener = listen("127.0.0.1:0".parse().unwrap(), buffers()).unwrap();
        let addr = listener.local_addr().unwrap();

        let accept = tokio::spawn(async move { listener.accept().await });
        let dialled = FixConnection::connect(addr, Direction::ClientToUpstream, buffers())
            .await
            .unwrap();
        let (accepted, _) = accept.await.unwrap().unwrap();
        let accepted = FixConnection::from_accepted(accepted, Direction::UpstreamToClient).unwrap();

        assert!(dialled.nodelay().unwrap(), "dialled end kept Nagle on");
        assert!(accepted.nodelay().unwrap(), "accepted end kept Nagle on");
    }

    #[tokio::test]
    async fn requested_socket_buffers_are_applied() {
        let requested = TcpBuffers {
            recv_bytes: 65_536,
            send_bytes: 65_536,
        };
        let listener = listen("127.0.0.1:0".parse().unwrap(), requested).unwrap();
        let addr = listener.local_addr().unwrap();

        let accept = tokio::spawn(async move { listener.accept().await });
        let dialled = FixConnection::connect(addr, Direction::ClientToUpstream, requested)
            .await
            .unwrap();
        drop(accept.await.unwrap().unwrap());

        let sock = socket2::SockRef::from(&dialled.stream);
        assert!(sock.recv_buffer_size().unwrap() >= requested.recv_bytes as usize);
        assert!(sock.send_buffer_size().unwrap() >= requested.send_bytes as usize);
    }

    #[tokio::test]
    async fn reads_a_single_framed_message() {
        let (mut connection, mut client) = connected_pair().await;
        let raw = frame("35=0|49=A|");
        client.write_all(&raw).await.unwrap();

        let message = connection.recv_message().await.unwrap().unwrap();
        assert_eq!(message.raw.as_ref(), raw.as_slice());
        assert_eq!(message.msg_type(), Some(b"0".as_slice()));
        assert_eq!(message.direction, Direction::UpstreamToClient);
    }

    #[tokio::test]
    async fn message_split_across_reads_is_reassembled() {
        let (mut connection, mut client) = connected_pair().await;
        let raw = frame("35=A|34=1|49=INITIATOR|");
        let (head, tail) = raw.split_at(9);

        client.write_all(head).await.unwrap();
        client.flush().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        client.write_all(tail).await.unwrap();

        let message = connection.recv_message().await.unwrap().unwrap();
        assert_eq!(message.raw.as_ref(), raw.as_slice());
        assert_eq!(message.msg_seq_num(), Some(MsgSeqNum(1)));
    }

    #[tokio::test]
    async fn two_messages_in_one_write_are_framed_separately() {
        let (mut connection, mut client) = connected_pair().await;
        let mut both = frame("35=0|34=7|");
        both.extend_from_slice(&frame("35=1|34=8|"));
        client.write_all(&both).await.unwrap();

        let first = connection.recv_message().await.unwrap().unwrap();
        let second = connection.recv_message().await.unwrap().unwrap();
        assert_eq!(first.msg_seq_num(), Some(MsgSeqNum(7)));
        assert_eq!(second.msg_seq_num(), Some(MsgSeqNum(8)));
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let (mut connection, client) = connected_pair().await;
        drop(client);

        assert!(connection.recv_message().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn eof_mid_message_is_a_framing_error() {
        let (mut connection, mut client) = connected_pair().await;
        let raw = frame("35=0|49=A|");
        client.write_all(raw.get(..12).unwrap()).await.unwrap();
        client.shutdown().await.unwrap();
        drop(client);

        let err = connection.recv_message().await.unwrap_err();
        assert!(matches!(err, ProxyError::Framing(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn garbage_stream_is_a_framing_error() {
        let (mut connection, mut client) = connected_pair().await;
        client
            .write_all(b"this is not a FIX stream at all")
            .await
            .unwrap();

        let err = connection.recv_message().await.unwrap_err();
        assert!(matches!(err, ProxyError::Framing(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn sent_bytes_reach_the_peer() {
        let (mut connection, mut client) = connected_pair().await;
        let raw = frame("35=0|34=3|");
        connection.send_raw(&raw).await.unwrap();

        let mut received = vec![0u8; raw.len()];
        tokio::io::AsyncReadExt::read_exact(&mut client, &mut received)
            .await
            .unwrap();
        assert_eq!(received, raw);
    }

    #[tokio::test]
    async fn split_halves_relay_in_both_directions() {
        let (connection, mut client) = connected_pair().await;
        let (mut reader, mut writer) = connection.split();

        let raw = frame("35=0|34=9|");
        client.write_all(&raw).await.unwrap();
        let message = reader.recv_message().await.unwrap().unwrap();
        assert_eq!(message.msg_seq_num(), Some(MsgSeqNum(9)));

        writer.send_raw(&raw).await.unwrap();
        let mut echoed = vec![0u8; raw.len()];
        tokio::io::AsyncReadExt::read_exact(&mut client, &mut echoed)
            .await
            .unwrap();
        assert_eq!(echoed, raw);
    }
}
