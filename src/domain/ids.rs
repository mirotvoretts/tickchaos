use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeqNum(pub u64);

impl SeqNum {
    #[must_use]
    pub fn next(self) -> Self {
        SeqNum(self.0.wrapping_add(1))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProxyPort(pub SocketAddr);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seed(pub u64);
