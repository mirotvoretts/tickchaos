#![cfg(not(miri))]
#![allow(clippy::unwrap_used, clippy::panic)]

use bytes::Bytes;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tickchaos::backgrounds::{Proxy, Stats};
use tickchaos::domain::{Packet, Seed};
use tickchaos::flows::Flow;
use tickchaos::scripts::Dropper;
use tickchaos::transport::{PacketTransport, UdpTransport};

const PACKET_COUNT: usize = 100;

async fn send_indexed_packets(sender: &UdpTransport, to: SocketAddr) {
    for i in 0..PACKET_COUNT as u64 {
        let packet = Packet::new(
            Bytes::copy_from_slice(&i.to_le_bytes()),
            std::time::Instant::now(),
        );
        sender.send(&packet, to).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn passthrough_delivers_all() {
    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut receiver = UdpTransport::bind(loopback, 65536, None).unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    let proxy_transport = UdpTransport::bind(loopback, 65536, None).unwrap();
    let proxy_addr = proxy_transport.local_addr().unwrap();

    let stats = Arc::new(Stats::default());
    let mut proxy = Proxy::new(
        proxy_transport,
        Flow::new(vec![]),
        Seed(1),
        receiver_addr,
        Arc::clone(&stats),
    );
    let proxy_handle = tokio::spawn(async move { proxy.run().await });

    let sender = UdpTransport::bind(loopback, 65536, None).unwrap();
    send_indexed_packets(&sender, proxy_addr).await;

    let mut received: HashSet<u64> = HashSet::new();
    while received.len() < PACKET_COUNT {
        let packet = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for packets: got {} of {PACKET_COUNT}",
                    received.len()
                )
            })
            .unwrap();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&packet.payload);
        received.insert(u64::from_le_bytes(bytes));
    }

    assert_eq!(received, (0..PACKET_COUNT as u64).collect());

    proxy_handle.abort();
}

#[tokio::test]
async fn full_drop_delivers_none() {
    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let mut receiver = UdpTransport::bind(loopback, 65536, None).unwrap();
    let receiver_addr = receiver.local_addr().unwrap();

    let proxy_transport = UdpTransport::bind(loopback, 65536, None).unwrap();
    let proxy_addr = proxy_transport.local_addr().unwrap();

    let stats = Arc::new(Stats::default());
    let mut proxy = Proxy::new(
        proxy_transport,
        Flow::new(vec![Box::new(Dropper::new(1.0).unwrap())]),
        Seed(1),
        receiver_addr,
        Arc::clone(&stats),
    );
    let proxy_handle = tokio::spawn(async move { proxy.run().await });

    let sender = UdpTransport::bind(loopback, 65536, None).unwrap();
    send_indexed_packets(&sender, proxy_addr).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let result = tokio::time::timeout(Duration::from_millis(50), receiver.recv()).await;
    assert!(result.is_err(), "expected no packets to be delivered");

    assert_eq!(stats.snapshot().dropped, PACKET_COUNT as u64);

    proxy_handle.abort();
}
