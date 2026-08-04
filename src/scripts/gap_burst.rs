use crate::domain::{Effect, Operator, Packet, SeqNum};
use rand::rngs::StdRng;
use std::ops::RangeInclusive;

/// Drops a contiguous run of `len` sequence numbers starting at `start`,
/// simulating a burst loss the feed handler must recover from.
///
/// A run reaching past `u64::MAX` is clamped to the last representable
/// sequence number; wrap-around gaps are out of scope. Packets without a
/// sequence number are forwarded untouched.
pub struct GapBurst {
    range: Option<RangeInclusive<u64>>,
}

impl GapBurst {
    #[must_use]
    pub fn new(start: SeqNum, len: u32) -> Self {
        let range = u64::from(len)
            .checked_sub(1)
            .map(|offset| start.0..=start.0.saturating_add(offset));
        GapBurst { range }
    }
}

impl Operator for GapBurst {
    fn decide(&mut self, packet: &Packet, _rng: &mut StdRng) -> Effect {
        match (&self.range, packet.seqnum) {
            (Some(range), Some(seqnum)) if range.contains(&seqnum.0) => Effect::Drop,
            _ => Effect::Forward,
        }
    }

    fn name(&self) -> &'static str {
        "gap_burst"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rand::SeedableRng;
    use std::time::Instant;

    const START: u64 = 1000;
    const LEN: u32 = 50;

    fn decide(operator: &mut GapBurst, seqnum: Option<SeqNum>) -> Effect {
        let mut rng = StdRng::seed_from_u64(42);
        let mut packet = Packet::new(Bytes::from_static(b"tick"), Instant::now());
        packet.seqnum = seqnum;
        operator.decide(&packet, &mut rng)
    }

    fn burst() -> GapBurst {
        GapBurst::new(SeqNum(START), LEN)
    }

    #[test]
    fn before_start_forwards() {
        assert_eq!(
            decide(&mut burst(), Some(SeqNum(START - 1))),
            Effect::Forward
        );
    }

    #[test]
    fn at_start_drops() {
        assert_eq!(decide(&mut burst(), Some(SeqNum(START))), Effect::Drop);
    }

    #[test]
    fn at_last_in_range_drops() {
        assert_eq!(
            decide(&mut burst(), Some(SeqNum(START + u64::from(LEN) - 1))),
            Effect::Drop
        );
    }

    #[test]
    fn after_range_forwards() {
        assert_eq!(
            decide(&mut burst(), Some(SeqNum(START + u64::from(LEN)))),
            Effect::Forward
        );
    }

    #[test]
    fn zero_len_drops_nothing() {
        let mut operator = GapBurst::new(SeqNum(START), 0);
        assert_eq!(decide(&mut operator, Some(SeqNum(START))), Effect::Forward);
    }

    #[test]
    fn forwards_when_no_seqnum() {
        assert_eq!(decide(&mut burst(), None), Effect::Forward);
    }

    #[test]
    fn range_ending_past_u64_max_drops_tail_without_overflow() {
        let mut operator = GapBurst::new(SeqNum(u64::MAX - 1), LEN);
        assert_eq!(decide(&mut operator, Some(SeqNum(u64::MAX))), Effect::Drop);
        assert_eq!(
            decide(&mut operator, Some(SeqNum(u64::MAX - 2))),
            Effect::Forward
        );
    }
}
