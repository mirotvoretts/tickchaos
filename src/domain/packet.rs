use crate::domain::SeqNum;
use bytes::Bytes;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Packet {
    pub payload: Bytes,
    pub seqnum: Option<SeqNum>,
    pub received_at: Instant,
}

impl Packet {
    #[must_use]
    pub fn new(payload: Bytes, received_at: Instant) -> Self {
        Packet {
            payload,
            seqnum: None,
            received_at,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn stores_arrival_timestamp() {
        let base = Instant::now();
        let received_at = base + Duration::from_millis(7);
        let packet = Packet::new(Bytes::from_static(b"tick"), received_at);
        assert_eq!(packet.received_at, received_at);
    }
}
