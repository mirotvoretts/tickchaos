use crate::domain::{Effect, Operator, Packet, SeqNum};
use rand::rngs::StdRng;
use std::collections::HashSet;

/// Drops packets whose feed sequence number is one of the configured targets.
///
/// Packets without an extracted sequence number are forwarded untouched — a
/// scenario that targets sequence numbers must not disturb unrelated traffic.
pub struct DropSeq {
    targets: HashSet<SeqNum>,
}

impl DropSeq {
    #[must_use]
    pub fn new(targets: Vec<SeqNum>) -> Self {
        DropSeq {
            targets: targets.into_iter().collect(),
        }
    }
}

impl Operator for DropSeq {
    fn decide(&mut self, packet: &Packet, _rng: &mut StdRng) -> Effect {
        match packet.seqnum {
            Some(seqnum) if self.targets.contains(&seqnum) => Effect::Drop,
            _ => Effect::Forward,
        }
    }

    fn name(&self) -> &'static str {
        "drop_seq"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rand::SeedableRng;
    use std::time::Instant;

    fn packet_with_seqnum(seqnum: Option<SeqNum>) -> Packet {
        let mut packet = Packet::new(Bytes::from_static(b"tick"), Instant::now());
        packet.seqnum = seqnum;
        packet
    }

    fn decide(operator: &mut DropSeq, seqnum: Option<SeqNum>) -> Effect {
        let mut rng = StdRng::seed_from_u64(42);
        operator.decide(&packet_with_seqnum(seqnum), &mut rng)
    }

    #[test]
    fn drops_exact_target() {
        let mut operator = DropSeq::new(vec![SeqNum(100), SeqNum(102)]);
        assert_eq!(decide(&mut operator, Some(SeqNum(100))), Effect::Drop);
        assert_eq!(decide(&mut operator, Some(SeqNum(102))), Effect::Drop);
    }

    #[test]
    fn forwards_non_target() {
        let mut operator = DropSeq::new(vec![SeqNum(100)]);
        assert_eq!(decide(&mut operator, Some(SeqNum(99))), Effect::Forward);
        assert_eq!(decide(&mut operator, Some(SeqNum(101))), Effect::Forward);
    }

    #[test]
    fn forwards_when_no_seqnum() {
        let mut operator = DropSeq::new(vec![SeqNum(100)]);
        assert_eq!(decide(&mut operator, None), Effect::Forward);
    }

    #[test]
    fn empty_targets_forward_everything() {
        let mut operator = DropSeq::new(vec![]);
        assert_eq!(decide(&mut operator, Some(SeqNum(100))), Effect::Forward);
        assert_eq!(decide(&mut operator, None), Effect::Forward);
    }

    #[test]
    fn decision_is_independent_of_rng_state() {
        let mut operator = DropSeq::new(vec![SeqNum(7)]);
        let mut rng = StdRng::seed_from_u64(1);
        let packet = packet_with_seqnum(Some(SeqNum(7)));
        for _ in 0..1000 {
            assert_eq!(operator.decide(&packet, &mut rng), Effect::Drop);
        }
    }
}
