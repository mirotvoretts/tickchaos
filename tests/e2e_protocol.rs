#![cfg(not(miri))]
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::indexing_slicing
)]

use bytes::Bytes;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tickchaos::backgrounds::{Proxy, Stats, DEFAULT_MAX_IN_FLIGHT};
use tickchaos::domain::{Packet, Seed, SeqNum};
use tickchaos::flows::Flow;
use tickchaos::protocols::MoldUdp64Extractor;
use tickchaos::scripts::DropSeq;
use tickchaos::transport::{PacketTransport, UdpTransport};

const FIRST_SEQ: u64 = 1;
const LAST_SEQ: u64 = 10;
const TARGET_SEQ: u64 = 5;

fn moldudp64_datagram(sequence: u64) -> Bytes {
    let mut payload = b"SESSION001".to_vec();
    payload.extend_from_slice(&sequence.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    Bytes::from(payload)
}

fn sequence_of(payload: &[u8]) -> u64 {
    let bytes: [u8; 8] = payload[10..18].try_into().unwrap();
    u64::from_be_bytes(bytes)
}

#[tokio::test]
async fn drop_seq_removes_only_the_targeted_sequence() {
    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut receiver = UdpTransport::bind(loopback, 65536, None).unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    let proxy_transport = UdpTransport::bind(loopback, 65536, None).unwrap();
    let proxy_addr = proxy_transport.local_addr().unwrap();

    let stats = Arc::new(Stats::default());
    let mut proxy = Proxy::new(
        proxy_transport,
        Flow::new(vec![Box::new(DropSeq::new(vec![SeqNum(TARGET_SEQ)]))]),
        Seed(1),
        receiver_addr,
        Arc::clone(&stats),
        Box::new(MoldUdp64Extractor),
        DEFAULT_MAX_IN_FLIGHT,
    );
    let proxy_handle = tokio::spawn(async move { proxy.run().await });

    let sender = UdpTransport::bind(loopback, 65536, None).unwrap();
    for sequence in FIRST_SEQ..=LAST_SEQ {
        let packet = Packet::new(moldudp64_datagram(sequence), std::time::Instant::now());
        sender.send(&packet, proxy_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let expected: HashSet<u64> = (FIRST_SEQ..=LAST_SEQ)
        .filter(|sequence| *sequence != TARGET_SEQ)
        .collect();

    let mut received: HashSet<u64> = HashSet::new();
    while received.len() < expected.len() {
        let packet = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for packets: got {received:?}, stats {:?}",
                    stats.snapshot()
                )
            })
            .unwrap();
        received.insert(sequence_of(&packet.payload));
    }

    assert_eq!(received, expected);

    let leaked = tokio::time::timeout(Duration::from_millis(200), receiver.recv()).await;
    assert!(leaked.is_err(), "dropped sequence must never be delivered");

    let snapshot = stats.snapshot();
    assert_eq!(snapshot.dropped, 1);
    assert_eq!(snapshot.forwarded, expected.len() as u64);

    proxy_handle.abort();
}

#[tokio::test]
async fn sequence_less_traffic_is_untouched_by_drop_seq() {
    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut receiver = UdpTransport::bind(loopback, 65536, None).unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    let proxy_transport = UdpTransport::bind(loopback, 65536, None).unwrap();
    let proxy_addr = proxy_transport.local_addr().unwrap();

    let stats = Arc::new(Stats::default());
    let mut proxy = Proxy::new(
        proxy_transport,
        Flow::new(vec![Box::new(DropSeq::new(vec![SeqNum(TARGET_SEQ)]))]),
        Seed(1),
        receiver_addr,
        Arc::clone(&stats),
        Box::new(MoldUdp64Extractor),
        DEFAULT_MAX_IN_FLIGHT,
    );
    let proxy_handle = tokio::spawn(async move { proxy.run().await });

    let sender = UdpTransport::bind(loopback, 65536, None).unwrap();
    let packet = Packet::new(Bytes::from_static(b"short"), std::time::Instant::now());
    sender.send(&packet, proxy_addr).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("timed out waiting for sequence-less packet")
        .unwrap();

    assert_eq!(&received.payload[..], b"short");
    assert_eq!(stats.snapshot().dropped, 0);

    proxy_handle.abort();
}
