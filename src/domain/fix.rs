use bytes::Bytes;
use std::ops::Range;
use std::time::Instant;

/// Session-level sequence number of a FIX message (tag 34).
///
/// Deliberately distinct from [`SeqNum`](crate::domain::SeqNum): that one counts
/// datagrams on a UDP feed, this one counts messages inside a FIX session, and
/// the two are never interchangeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MsgSeqNum(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    UpstreamToClient,
    ClientToUpstream,
}

impl Direction {
    #[must_use]
    pub fn tag(self) -> u64 {
        match self {
            Direction::UpstreamToClient => 0,
            Direction::ClientToUpstream => 1,
        }
    }
}

/// Session fields lifted out of a framed message while it is being cut.
///
/// Each field is independently optional: a structurally valid frame may still be
/// missing tag 34 or tag 35, and an operator has to tell "absent" apart from
/// "unparseable". `msg_type` is a range into [`FixMessage::raw`] rather than a
/// copy, which keeps framing allocation-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixHeader {
    pub msg_seq_num: Option<MsgSeqNum>,
    pub msg_type: Option<Range<usize>>,
    pub poss_dup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixMessage {
    pub raw: Bytes,
    pub header: FixHeader,
    pub direction: Direction,
    pub received_at: Instant,
}

impl FixMessage {
    #[must_use]
    pub fn msg_type(&self) -> Option<&[u8]> {
        let range = self.header.msg_type.clone()?;
        self.raw.get(range)
    }

    #[must_use]
    pub fn msg_seq_num(&self) -> Option<MsgSeqNum> {
        self.header.msg_seq_num
    }

    #[must_use]
    pub fn is_poss_dup(&self) -> bool {
        self.header.poss_dup
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(raw: &'static [u8], msg_type: Option<Range<usize>>) -> FixMessage {
        FixMessage {
            raw: Bytes::from_static(raw),
            header: FixHeader {
                msg_seq_num: Some(MsgSeqNum(7)),
                msg_type,
                poss_dup: false,
            },
            direction: Direction::UpstreamToClient,
            received_at: Instant::now(),
        }
    }

    #[test]
    fn msg_type_resolves_range_into_raw() {
        let message = message(b"8=FIX.4.2\x0135=AE\x01", Some(13..15));
        assert_eq!(message.msg_type(), Some(b"AE".as_slice()));
    }

    #[test]
    fn msg_type_absent_when_range_unset() {
        let message = message(b"8=FIX.4.2\x01", None);
        assert_eq!(message.msg_type(), None);
    }

    #[test]
    fn msg_type_out_of_bounds_range_yields_none() {
        let message = message(b"8=FIX.4.2\x01", Some(100..200));
        assert_eq!(message.msg_type(), None);
    }

    #[test]
    fn direction_tags_are_distinct() {
        assert_ne!(
            Direction::UpstreamToClient.tag(),
            Direction::ClientToUpstream.tag()
        );
    }
}
