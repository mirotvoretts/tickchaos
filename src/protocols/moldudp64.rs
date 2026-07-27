use crate::domain::{SeqNum, SequenceExtractor};

const HEADER_LEN: usize = 20;
const SEQUENCE_RANGE: std::ops::Range<usize> = 10..18;

/// Reads the sequence number out of a MoldUDP64 downstream packet header.
///
/// Header layout: 10-byte ASCII session, 8-byte big-endian sequence number,
/// 2-byte big-endian message count. Anything shorter than the header yields `None`.
pub struct MoldUdp64Extractor;

impl SequenceExtractor for MoldUdp64Extractor {
    fn extract(&self, payload: &[u8]) -> Option<SeqNum> {
        if payload.len() < HEADER_LEN {
            return None;
        }
        let sequence = payload.get(SEQUENCE_RANGE)?;
        Some(SeqNum(u64::from_be_bytes(sequence.try_into().ok()?)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn header(session: &[u8; 10], sequence: u64, message_count: u16) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN);
        bytes.extend_from_slice(session);
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&message_count.to_be_bytes());
        bytes
    }

    #[test]
    fn extracts_seqnum_from_valid_header() {
        let payload = header(b"SESSION001", 777, 1);
        assert_eq!(MoldUdp64Extractor.extract(&payload), Some(SeqNum(777)));
    }

    #[test]
    fn extracts_seqnum_ignoring_trailing_messages() {
        let mut payload = header(b"SESSION001", u64::MAX, 3);
        payload.extend_from_slice(b"trailing message bytes");
        assert_eq!(MoldUdp64Extractor.extract(&payload), Some(SeqNum(u64::MAX)));
    }

    #[test]
    fn short_payload_returns_none() {
        assert_eq!(MoldUdp64Extractor.extract(&[]), None);
        assert_eq!(MoldUdp64Extractor.extract(&[0u8; HEADER_LEN - 1]), None);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None,
            cases: if cfg!(miri) { 2 } else { 256 },
            ..ProptestConfig::default()
        })]

        #[test]
        fn never_panics_on_arbitrary_bytes(payload: Vec<u8>) {
            let extracted = MoldUdp64Extractor.extract(&payload);
            prop_assert_eq!(extracted.is_some(), payload.len() >= HEADER_LEN);
        }

        #[test]
        fn reads_big_endian_sequence_at_fixed_offset(sequence: u64, message_count: u16) {
            let payload = header(b"SESSION042", sequence, message_count);
            prop_assert_eq!(MoldUdp64Extractor.extract(&payload), Some(SeqNum(sequence)));
        }
    }
}
