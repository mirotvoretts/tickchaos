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
}
