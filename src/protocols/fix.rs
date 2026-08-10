use crate::domain::{Direction, FixHeader, FixMessage, MsgSeqNum};
use bytes::BytesMut;
use std::ops::Range;
use std::time::Instant;

/// Largest FIX message the framer will assemble.
///
/// `BodyLength` arrives from an untrusted stream: without a cap, `9=999999999`
/// makes the framer wait for a tail that never comes while its buffer grows —
/// the proxy would become the outage it exists to simulate. Real session-level
/// messages are hundreds of bytes; 16 KiB leaves room for bulk reports.
pub const MAX_MESSAGE_LEN: usize = 16 * 1024;

/// How far into the stream `8=`/`9=` must appear before it is declared non-FIX.
const MAX_HEADER_SCAN: usize = 32;

/// `10=` plus three checksum digits plus `SOH`.
const TAIL_LEN: usize = 7;

const SOH: u8 = 0x01;
const BEGIN_STRING: &[u8] = b"8=";
const BODY_LENGTH: &[u8] = b"9=";
const CHECKSUM: &[u8] = b"10=";

/// A framing failure, which is fatal for the connection but never for the process.
///
/// Unlike a corrupt UDP datagram, a desynchronised TCP stream cannot be resumed:
/// every subsequent byte is uninterpretable and re-synchronising risks splicing
/// two messages into one. The connection is torn down; the proxy keeps accepting
/// new ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    #[error("stream does not begin with a FIX BeginString")]
    NotFixStream,

    #[error("no BeginString/BodyLength terminator within {MAX_HEADER_SCAN} bytes")]
    HeaderTooLong,

    #[error("BodyLength field is missing or not numeric")]
    BadBodyLength,

    #[error("message exceeds the {MAX_MESSAGE_LEN} byte cap")]
    MessageTooLong,

    #[error("no well-formed CheckSum field where BodyLength said it would be")]
    BadChecksumField,
}

struct Layout {
    frame_len: usize,
    body: Range<usize>,
}

/// Cuts FIX messages out of a byte stream, lifting session fields in the same pass.
///
/// Feed bytes with [`extend`](Self::extend), then drain with
/// [`next_message`](Self::next_message) until it yields `None`. The retained
/// buffer holds at most one incomplete message, which the `BodyLength` and
/// header-scan caps bound to [`MAX_MESSAGE_LEN`].
///
/// Once framing fails the framer stays failed: a desynchronised stream has no
/// safe resynchronisation point, so every later call repeats the same error.
#[derive(Debug)]
pub struct FixFramer {
    buf: BytesMut,
    direction: Direction,
    failed: Option<FrameError>,
}

impl FixFramer {
    #[must_use]
    pub fn new(direction: Direction) -> Self {
        FixFramer {
            buf: BytesMut::with_capacity(MAX_MESSAGE_LEN),
            direction,
            failed: None,
        }
    }

    pub fn extend(&mut self, chunk: &[u8]) {
        if self.failed.is_some() {
            return;
        }
        self.buf.extend_from_slice(chunk);
    }

    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    pub fn next_message(&mut self, received_at: Instant) -> Result<Option<FixMessage>, FrameError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        match Self::layout(&self.buf) {
            Ok(None) => Ok(None),
            Ok(Some(layout)) => {
                let raw = self.buf.split_to(layout.frame_len).freeze();
                let header = parse_header(&raw, layout.body);
                Ok(Some(FixMessage {
                    raw,
                    header,
                    direction: self.direction,
                    received_at,
                }))
            }
            Err(error) => {
                self.failed = Some(error);
                self.buf.clear();
                Err(error)
            }
        }
    }

    fn layout(buf: &[u8]) -> Result<Option<Layout>, FrameError> {
        if !starts_with_prefix_of(buf, BEGIN_STRING) {
            return Err(FrameError::NotFixStream);
        }
        let Some(begin_end) = scan_soh(buf, 0)? else {
            return Ok(None);
        };

        let length_start = begin_end.saturating_add(1);
        let Some(length_field) = buf.get(length_start..) else {
            return Ok(None);
        };
        if !starts_with_prefix_of(length_field, BODY_LENGTH) {
            return Err(FrameError::BadBodyLength);
        }
        let Some(length_end) = scan_soh(buf, length_start)? else {
            return Ok(None);
        };

        let digits_start = length_start.saturating_add(BODY_LENGTH.len());
        let digits = buf.get(digits_start..length_end).unwrap_or_default();
        let body_length = usize::try_from(parse_digits(digits).ok_or(FrameError::BadBodyLength)?)
            .map_err(|_| FrameError::MessageTooLong)?;

        let header_len = length_end.saturating_add(1);
        let body_end = header_len
            .checked_add(body_length)
            .ok_or(FrameError::MessageTooLong)?;
        let frame_len = body_end
            .checked_add(TAIL_LEN)
            .ok_or(FrameError::MessageTooLong)?;
        if frame_len > MAX_MESSAGE_LEN {
            return Err(FrameError::MessageTooLong);
        }
        if buf.len() < frame_len {
            return Ok(None);
        }

        let tail = buf
            .get(body_end..frame_len)
            .ok_or(FrameError::BadChecksumField)?;
        if !is_checksum_field(tail) {
            return Err(FrameError::BadChecksumField);
        }

        Ok(Some(Layout {
            frame_len,
            body: header_len..body_end,
        }))
    }
}

/// `BodyLength` counts the bytes after its own `SOH` up to and including the
/// `SOH` before `10=`, so a well-formed tail is exactly `10=DDD<SOH>`.
///
/// Checking the shape — not the value — is what catches a lying `BodyLength`
/// before it splices two messages into one. The value itself is left alone on
/// purpose: a deliberately corrupted checksum is a valid frame the client engine
/// is supposed to reject.
fn is_checksum_field(tail: &[u8]) -> bool {
    let Some(digits) = tail.get(CHECKSUM.len()..CHECKSUM.len().saturating_add(3)) else {
        return false;
    };
    tail.starts_with(CHECKSUM)
        && digits.iter().all(u8::is_ascii_digit)
        && tail.get(TAIL_LEN.saturating_sub(1)) == Some(&SOH)
}

fn starts_with_prefix_of(buf: &[u8], prefix: &[u8]) -> bool {
    let compared = buf.len().min(prefix.len());
    buf.get(..compared) == prefix.get(..compared)
}

fn scan_soh(buf: &[u8], from: usize) -> Result<Option<usize>, FrameError> {
    let window = buf
        .get(from..buf.len().min(MAX_HEADER_SCAN))
        .unwrap_or_default();
    match window.iter().position(|&byte| byte == SOH) {
        Some(offset) => Ok(Some(from.saturating_add(offset))),
        None if buf.len() >= MAX_HEADER_SCAN => Err(FrameError::HeaderTooLong),
        None => Ok(None),
    }
}

fn parse_digits(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    for &byte in bytes {
        let digit = byte.checked_sub(b'0').filter(|digit| *digit < 10)?;
        value = value.checked_mul(10)?.checked_add(u64::from(digit))?;
    }
    Some(value)
}

/// Lifts tags 34, 35 and 43 out of the framed body in a single linear pass.
///
/// A structurally sound frame may still lack any of them, so every field stays
/// independently optional — an operator has to tell "no sequence number" apart
/// from "not a FIX message".
fn parse_header(raw: &[u8], body: Range<usize>) -> FixHeader {
    let mut header = FixHeader {
        msg_seq_num: None,
        msg_type: None,
        poss_dup: false,
    };

    let mut start = body.start;
    while start < body.end {
        let field = raw.get(start..body.end).unwrap_or_default();
        let end = field
            .iter()
            .position(|&byte| byte == SOH)
            .map_or(body.end, |offset| start.saturating_add(offset));

        if let Some(separator) = raw
            .get(start..end)
            .and_then(|field| field.iter().position(|&byte| byte == b'='))
        {
            let value_start = start.saturating_add(separator).saturating_add(1);
            let tag = raw.get(start..start.saturating_add(separator));
            let value = raw.get(value_start..end).unwrap_or_default();
            match tag {
                Some(b"34") => header.msg_seq_num = parse_digits(value).map(MsgSeqNum),
                Some(b"35") => header.msg_type = Some(value_start..end),
                Some(b"43") => header.poss_dup = value == b"Y",
                _ => {}
            }
        }

        start = end.saturating_add(1);
    }

    header
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn frame(body: &str) -> Vec<u8> {
        let body = body.replace('|', "\x01");
        let mut message = format!("8=FIX.4.2\x019={}\x01{}", body.len(), body);
        let checksum: u32 = message.bytes().map(u32::from).sum::<u32>() % 256;
        message.push_str(&format!("10={checksum:03}\x01"));
        message.into_bytes()
    }

    fn framer() -> FixFramer {
        FixFramer::new(Direction::UpstreamToClient)
    }

    fn next(framer: &mut FixFramer) -> Result<Option<FixMessage>, FrameError> {
        framer.next_message(Instant::now())
    }

    #[test]
    fn frames_the_design_doc_vector() {
        let raw = frame("35=0|49=A|");
        assert_eq!(raw.len(), 32);
        assert_eq!(
            raw.as_slice(),
            b"8=FIX.4.2\x019=10\x0135=0\x0149=A\x0110=185\x01"
        );

        let mut framer = framer();
        framer.extend(&raw);
        let message = next(&mut framer).unwrap().unwrap();

        assert_eq!(message.raw.as_ref(), raw.as_slice());
        assert_eq!(message.msg_type(), Some(b"0".as_slice()));
        assert!(next(&mut framer).unwrap().is_none());
    }

    #[test]
    fn extracts_seqnum_type_and_poss_dup() {
        let mut framer = framer();
        framer.extend(&frame("35=AE|34=4711|43=Y|49=ACME|"));
        let message = next(&mut framer).unwrap().unwrap();

        assert_eq!(message.msg_seq_num(), Some(MsgSeqNum(4711)));
        assert_eq!(message.msg_type(), Some(b"AE".as_slice()));
        assert!(message.is_poss_dup());
    }

    #[test]
    fn poss_dup_only_when_flag_is_yes() {
        let mut framer = framer();
        framer.extend(&frame("35=D|34=1|43=N|"));
        assert!(!next(&mut framer).unwrap().unwrap().is_poss_dup());
    }

    #[test]
    fn absent_seqnum_is_none_not_an_error() {
        let mut framer = framer();
        framer.extend(&frame("35=A|49=ACME|"));
        let message = next(&mut framer).unwrap().unwrap();
        assert_eq!(message.msg_seq_num(), None);
        assert_eq!(message.msg_type(), Some(b"A".as_slice()));
    }

    #[test]
    fn unparseable_seqnum_is_none_not_an_error() {
        let mut framer = framer();
        framer.extend(&frame("35=D|34=abc|"));
        assert_eq!(next(&mut framer).unwrap().unwrap().msg_seq_num(), None);
    }

    #[test]
    fn incomplete_frame_yields_none_until_completed() {
        let raw = frame("35=D|34=9|");

        for split in [1usize, 5, 15, raw.len() - 1] {
            let mut partial = framer();
            partial.extend(raw.get(..split).unwrap());
            assert!(
                next(&mut partial).unwrap().is_none(),
                "prefix of {split} bytes must not frame"
            );
            partial.extend(raw.get(split..).unwrap());
            assert!(next(&mut partial).unwrap().is_some());
        }
    }

    #[test]
    fn drains_several_messages_from_one_chunk() {
        let mut chunk = frame("35=A|34=1|");
        chunk.extend_from_slice(&frame("35=D|34=2|"));
        chunk.extend_from_slice(&frame("35=8|34=3|"));

        let mut framer = framer();
        framer.extend(&chunk);

        let mut seqnums = Vec::new();
        while let Some(message) = next(&mut framer).unwrap() {
            seqnums.push(message.msg_seq_num());
        }
        assert_eq!(
            seqnums,
            vec![Some(MsgSeqNum(1)), Some(MsgSeqNum(2)), Some(MsgSeqNum(3))]
        );
    }

    #[test]
    fn keeps_partial_tail_for_the_next_read() {
        let first = frame("35=A|34=1|");
        let second = frame("35=D|34=2|");
        let mut chunk = first.clone();
        chunk.extend_from_slice(second.get(..4).unwrap());

        let mut framer = framer();
        framer.extend(&chunk);
        assert_eq!(
            next(&mut framer).unwrap().unwrap().msg_seq_num(),
            Some(MsgSeqNum(1))
        );
        assert!(next(&mut framer).unwrap().is_none());

        framer.extend(second.get(4..).unwrap());
        assert_eq!(
            next(&mut framer).unwrap().unwrap().msg_seq_num(),
            Some(MsgSeqNum(2))
        );
    }

    #[test]
    fn rejects_stream_that_does_not_start_with_begin_string() {
        let mut framer = framer();
        framer.extend(b"GET / HTTP/1.1\r\n");
        assert_eq!(next(&mut framer), Err(FrameError::NotFixStream));
    }

    #[test]
    fn rejects_begin_string_without_soh_in_scan_window() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2.................................");
        assert_eq!(next(&mut framer), Err(FrameError::HeaderTooLong));
    }

    #[test]
    fn rejects_non_numeric_body_length() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2\x019=abc\x0135=D\x0110=000\x01");
        assert_eq!(next(&mut framer), Err(FrameError::BadBodyLength));
    }

    #[test]
    fn rejects_second_field_that_is_not_body_length() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2\x0135=D\x019=10\x0110=000\x01");
        assert_eq!(next(&mut framer), Err(FrameError::BadBodyLength));
    }

    #[test]
    fn rejects_body_length_over_the_cap_without_buffering_it() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2\x019=999999999\x01");
        assert_eq!(next(&mut framer), Err(FrameError::MessageTooLong));
        assert!(framer.buffered() < MAX_MESSAGE_LEN);
    }

    #[test]
    fn rejects_lying_body_length_that_would_splice_messages() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2\x019=8\x0135=0\x0149=A\x0110=185\x01");
        assert_eq!(next(&mut framer), Err(FrameError::BadChecksumField));
    }

    #[test]
    fn rejects_checksum_field_with_wrong_shape() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2\x019=10\x0135=0\x0149=A\x0110=18X\x01");
        assert_eq!(next(&mut framer), Err(FrameError::BadChecksumField));
    }

    #[test]
    fn does_not_validate_the_checksum_value() {
        let mut framer = framer();
        framer.extend(b"8=FIX.4.2\x019=10\x0135=0\x0149=A\x0110=000\x01");
        assert!(next(&mut framer).unwrap().is_some());
    }

    #[test]
    fn frame_error_is_sticky() {
        let mut framer = framer();
        framer.extend(b"GET / HTTP/1.1\r\n");
        assert!(next(&mut framer).is_err());

        framer.extend(&frame("35=A|34=1|"));
        assert_eq!(next(&mut framer), Err(FrameError::NotFixStream));
    }

    #[test]
    fn direction_is_stamped_on_every_message() {
        let mut framer = FixFramer::new(Direction::ClientToUpstream);
        framer.extend(&frame("35=A|34=1|"));
        assert_eq!(
            next(&mut framer).unwrap().unwrap().direction,
            Direction::ClientToUpstream
        );
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config {
            failure_persistence: None,
            cases: if cfg!(miri) { 2 } else { 256 },
            ..proptest::test_runner::Config::default()
        })]

        #[test]
        fn never_panics_on_arbitrary_bytes(chunk: Vec<u8>) {
            let mut framer = framer();
            framer.extend(&chunk);
            let _ = next(&mut framer);
        }

        #[test]
        fn never_panics_on_arbitrary_bytes_after_valid_prefix(chunk: Vec<u8>) {
            let mut framer = framer();
            framer.extend(b"8=FIX.4.2\x019=10\x01");
            framer.extend(&chunk);
            let _ = next(&mut framer);
        }

        #[test]
        fn incomplete_buffer_never_exceeds_the_cap(chunk: Vec<u8>) {
            let mut framer = framer();
            framer.extend(&chunk);
            if matches!(next(&mut framer), Ok(None)) {
                proptest::prop_assert!(framer.buffered() <= MAX_MESSAGE_LEN);
            }
        }

        #[test]
        fn arbitrary_body_survives_a_roundtrip(tag in 100u32..999, value in "[a-zA-Z0-9]{0,64}") {
            let raw = frame(&format!("35=D|34=42|{tag}={value}|"));
            let mut framer = framer();
            framer.extend(&raw);
            let message = next(&mut framer).unwrap().unwrap();
            proptest::prop_assert_eq!(message.raw.as_ref(), raw.as_slice());
            proptest::prop_assert_eq!(message.msg_seq_num(), Some(MsgSeqNum(42)));
        }
    }
}
