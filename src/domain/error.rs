use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("socket bind failed on {addr}: {source}")]
    Bind {
        addr: std::net::SocketAddr,
        source: std::io::Error,
    },

    #[error("multicast join failed for {group}: {source}")]
    MulticastJoin {
        group: std::net::SocketAddr,
        source: std::io::Error,
    },

    #[error("invalid scenario config: {0}")]
    Config(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// A framing failure on a stream transport.
    ///
    /// Carries the rendered `FrameError` rather than the type itself: `domain`
    /// must not depend on `protocols`. Per the FIX design doc this is fatal for
    /// the connection it came from - once boundaries are lost there is no safe
    /// resynchronisation point - but never for the proxy process.
    #[error("fix framing failed: {0}")]
    Framing(String),
}
