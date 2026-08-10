use anyhow::{anyhow, Result};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tickchaos::backgrounds::{Proxy, Stats, DEFAULT_MAX_IN_FLIGHT};
use tickchaos::domain::{NoopExtractor, Seed};
use tickchaos::flows::Flow;
use tickchaos::transport::UdpTransport;

const PACKETS: usize = 20_000;
const WARMUP: usize = 2_000;
const RATE_PPS: u64 = 10_000;
const PAYLOAD_BYTES: usize = 64;
const RECV_BUF_BYTES: usize = 4 * 1024 * 1024;
const THROUGHPUT_PACKETS: usize = 200_000;

struct Percentiles {
    p50: Duration,
    p99: Duration,
    p999: Duration,
    max: Duration,
}

fn percentiles(mut samples: Vec<Duration>) -> Result<Percentiles> {
    if samples.is_empty() {
        return Err(anyhow!("no samples collected"));
    }
    samples.sort_unstable();
    let last = samples.len() - 1;
    let at = |q: f64| -> Duration {
        let index = ((samples.len() as f64) * q) as usize;
        samples.get(index.min(last)).copied().unwrap_or_default()
    };
    Ok(Percentiles {
        p50: at(0.50),
        p99: at(0.99),
        p999: at(0.999),
        max: samples.get(last).copied().unwrap_or_default(),
    })
}

fn spin_until(deadline: Instant) {
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

fn spawn_sender(
    target: SocketAddr,
    epoch: Instant,
    count: usize,
    rate_pps: Option<u64>,
) -> Result<thread::JoinHandle<Result<()>>> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    Ok(thread::spawn(move || -> Result<()> {
        let interval = rate_pps.map(|rate| Duration::from_nanos(1_000_000_000 / rate));
        let start = Instant::now();
        let mut payload = [0u8; PAYLOAD_BYTES];
        for index in 0..count {
            if let Some(interval) = interval {
                spin_until(start + interval * (index as u32));
            }
            let stamp = epoch.elapsed().as_nanos() as u64;
            payload
                .get_mut(..8)
                .ok_or_else(|| anyhow!("payload too small for timestamp"))?
                .copy_from_slice(&stamp.to_le_bytes());
            socket.send_to(&payload, target)?;
        }
        Ok(())
    }))
}

fn collect_latencies(
    socket: &UdpSocket,
    epoch: Instant,
    count: usize,
    warmup: usize,
) -> Result<Vec<Duration>> {
    let mut buffer = [0u8; 2048];
    let mut samples = Vec::with_capacity(count.saturating_sub(warmup));
    for index in 0..count {
        let (read, _) = socket.recv_from(&mut buffer)?;
        let received = epoch.elapsed();
        if index < warmup {
            continue;
        }
        let stamp = buffer
            .get(..8)
            .filter(|_| read >= 8)
            .ok_or_else(|| anyhow!("short packet: {read} bytes"))?;
        let mut raw = [0u8; 8];
        raw.copy_from_slice(stamp);
        let sent = Duration::from_nanos(u64::from_le_bytes(raw));
        samples.push(received.saturating_sub(sent));
    }
    Ok(samples)
}

fn bind_receiver() -> Result<(UdpSocket, SocketAddr)> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    socket.set_read_timeout(Some(Duration::from_secs(5)))?;
    let addr = socket.local_addr()?;
    Ok((socket, addr))
}

fn measure_direct(epoch: Instant) -> Result<Vec<Duration>> {
    let (receiver, receiver_addr) = bind_receiver()?;
    let sender = spawn_sender(receiver_addr, epoch, PACKETS, Some(RATE_PPS))?;
    let samples = collect_latencies(&receiver, epoch, PACKETS, WARMUP)?;
    sender
        .join()
        .map_err(|_| anyhow!("sender thread panicked"))??;
    Ok(samples)
}

fn measure_proxied(runtime: &tokio::runtime::Runtime, epoch: Instant) -> Result<Vec<Duration>> {
    let (receiver, receiver_addr) = bind_receiver()?;
    let listen: SocketAddr = "127.0.0.1:0".parse()?;

    let guard = runtime.enter();
    let transport = UdpTransport::bind(listen, RECV_BUF_BYTES, None)?;
    let proxy_addr = transport.local_addr()?;
    let stats = Arc::new(Stats::default());
    let mut proxy = Proxy::new(
        transport,
        Flow::new(vec![]),
        Seed(1),
        receiver_addr,
        Arc::clone(&stats),
        Box::new(NoopExtractor),
        DEFAULT_MAX_IN_FLIGHT,
    );
    let handle = runtime.spawn(async move { proxy.run().await });
    drop(guard);

    let sender = spawn_sender(proxy_addr, epoch, PACKETS, Some(RATE_PPS))?;
    let samples = collect_latencies(&receiver, epoch, PACKETS, WARMUP)?;
    sender
        .join()
        .map_err(|_| anyhow!("sender thread panicked"))??;
    handle.abort();
    Ok(samples)
}

fn measure_throughput(
    runtime: &tokio::runtime::Runtime,
    epoch: Instant,
) -> Result<(f64, u64, u64)> {
    let (receiver, receiver_addr) = bind_receiver()?;
    receiver.set_read_timeout(Some(Duration::from_millis(500)))?;
    let listen: SocketAddr = "127.0.0.1:0".parse()?;

    let guard = runtime.enter();
    let transport = UdpTransport::bind(listen, RECV_BUF_BYTES, None)?;
    let proxy_addr = transport.local_addr()?;
    let stats = Arc::new(Stats::default());
    let mut proxy = Proxy::new(
        transport,
        Flow::new(vec![]),
        Seed(1),
        receiver_addr,
        Arc::clone(&stats),
        Box::new(NoopExtractor),
        DEFAULT_MAX_IN_FLIGHT,
    );
    let handle = runtime.spawn(async move { proxy.run().await });
    drop(guard);

    let done = Arc::new(AtomicBool::new(false));
    let drain_flag = Arc::clone(&done);
    let drain = thread::spawn(move || -> u64 {
        let mut buffer = [0u8; 2048];
        let mut seen = 0u64;
        loop {
            match receiver.recv_from(&mut buffer) {
                Ok(_) => seen += 1,
                Err(_) if drain_flag.load(Ordering::Relaxed) => break,
                Err(_) => continue,
            }
        }
        seen
    });

    let started = Instant::now();
    let sender = spawn_sender(proxy_addr, epoch, THROUGHPUT_PACKETS, None)?;
    sender
        .join()
        .map_err(|_| anyhow!("sender thread panicked"))??;
    let elapsed = started.elapsed();
    thread::sleep(Duration::from_millis(500));
    done.store(true, Ordering::Relaxed);
    let delivered = drain.join().map_err(|_| anyhow!("drain thread panicked"))?;
    handle.abort();

    let offered = THROUGHPUT_PACKETS as f64 / elapsed.as_secs_f64();
    Ok((offered, delivered, stats.snapshot().forwarded))
}

fn report(label: &str, stats: &Percentiles) {
    println!(
        "{label:<22} p50 {:>9.1?}  p99 {:>9.1?}  p99.9 {:>9.1?}  max {:>9.1?}",
        stats.p50, stats.p99, stats.p999, stats.max
    );
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let epoch = Instant::now();

    println!("tickchaos latency harness");
    println!("packets {PACKETS} (warmup {WARMUP}), offered rate {RATE_PPS} pps, payload {PAYLOAD_BYTES} B, loopback\n");

    let direct = percentiles(measure_direct(epoch)?)?;
    let proxied = percentiles(measure_proxied(&runtime, epoch)?)?;

    report("direct socket", &direct);
    report("through tickchaos", &proxied);
    println!(
        "\nadded by proxy         p50 {:>9.1?}  p99 {:>9.1?}",
        proxied.p50.saturating_sub(direct.p50),
        proxied.p99.saturating_sub(direct.p99),
    );
    println!("(tail deltas past p99 are dominated by OS scheduling noise, not the proxy)");

    let (offered, delivered, forwarded) = measure_throughput(&runtime, epoch)?;
    println!(
        "\nsaturation: offered {offered:.0} pps, proxy forwarded {forwarded}, receiver saw {delivered} of {THROUGHPUT_PACKETS}"
    );

    Ok(())
}
