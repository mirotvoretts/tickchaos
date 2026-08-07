#![cfg(not(miri))]
#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

use arc_swap::ArcSwapOption;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use tickchaos::backgrounds::{control_plane, Stats};
use tickchaos::flows::Flow;

struct HttpResponse {
    status: u16,
    body: String,
}

fn send_request(addr: SocketAddr, method: &str, path: &str, body: &str) -> HttpResponse {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).unwrap();

    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();

    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    HttpResponse {
        status,
        body: body.to_owned(),
    }
}

async fn spawn_control_plane(stats: Arc<Stats>, reload: Arc<ArcSwapOption<Flow>>) -> SocketAddr {
    let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (listener, addr) = control_plane::bind(loopback).await.unwrap();
    tokio::spawn(control_plane::run(listener, stats, reload));
    addr
}

#[tokio::test]
async fn metrics_endpoint_returns_snapshot() {
    let stats = Arc::new(Stats::default());
    stats
        .forwarded
        .fetch_add(7, std::sync::atomic::Ordering::Relaxed);
    stats
        .dropped
        .fetch_add(3, std::sync::atomic::Ordering::Relaxed);
    let reload = Arc::new(ArcSwapOption::empty());

    let addr = spawn_control_plane(Arc::clone(&stats), reload).await;

    let response = tokio::task::spawn_blocking(move || send_request(addr, "GET", "/metrics", ""))
        .await
        .unwrap();

    assert_eq!(response.status, 200);
    let snapshot: tickchaos::backgrounds::StatsSnapshot =
        serde_json::from_str(&response.body).unwrap();
    assert_eq!(snapshot.forwarded, 7);
    assert_eq!(snapshot.dropped, 3);
}

#[tokio::test]
async fn reload_swaps_flow() {
    let stats = Arc::new(Stats::default());
    let reload = Arc::new(ArcSwapOption::empty());

    let addr = spawn_control_plane(Arc::clone(&stats), Arc::clone(&reload)).await;

    let toml_body = r#"
        seed = 1
        listen = "127.0.0.1:9000"
        upstream = "127.0.0.1:9001"

        [[operators]]
        type = "drop_seq"
        seqnums = [1]
    "#;

    let response = tokio::task::spawn_blocking({
        let body = toml_body.to_owned();
        move || send_request(addr, "POST", "/reload", &body)
    })
    .await
    .unwrap();

    assert_eq!(response.status, 200);
    assert!(reload.load().is_some());
}

#[tokio::test]
async fn reload_invalid_toml_rejected() {
    let stats = Arc::new(Stats::default());
    let reload = Arc::new(ArcSwapOption::empty());

    let addr = spawn_control_plane(Arc::clone(&stats), Arc::clone(&reload)).await;

    let response = tokio::task::spawn_blocking(move || {
        send_request(addr, "POST", "/reload", "this is not valid toml {{{")
    })
    .await
    .unwrap();

    assert_eq!(response.status, 400);
    assert!(reload.load().is_none());
}
