use crate::domain::SeqNum;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Packet {
    pub payload: Bytes,
    pub seqnum: Option<SeqNum>,
}

impl Packet {
    #[must_use]
    pub fn new(payload: Bytes) -> Self {
        Packet {
            payload,
            seqnum: None,
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
