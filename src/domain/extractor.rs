use crate::domain::SeqNum;

/// Pulls a feed's sequence number out of a raw datagram payload.
///
/// Implementations must never panic on malformed input — an unparseable payload
/// yields `None`, and the packet is treated as sequence-less by the operators.
pub trait SequenceExtractor: Send {
    fn extract(&self, payload: &[u8]) -> Option<SeqNum>;
}

pub struct NoopExtractor;

impl SequenceExtractor for NoopExtractor {
    fn extract(&self, _payload: &[u8]) -> Option<SeqNum> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_returns_none() {
        let extractor = NoopExtractor;
        assert_eq!(extractor.extract(b"anything"), None);
    }

    #[test]
    fn noop_returns_none_on_empty_payload() {
        let extractor = NoopExtractor;
        assert_eq!(extractor.extract(&[]), None);
    }
}
