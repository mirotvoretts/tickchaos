use anyhow::{Context, Result};
use clap::Parser;
use serde::Serialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tickchaos::backgrounds::{control_plane, Proxy, Stats, StatsSnapshot};
use tickchaos::config::Scenario;
use tickchaos::domain::Seed;
use tickchaos::transport::UdpTransport;

#[derive(Parser)]
#[command(name = "tickchaos", about = "UDP market-data degradation proxy")]
struct Cli {
    #[arg(short, long)]
    scenario: PathBuf,

    /// Enable the HTTP control plane (GET /metrics, POST /reload) on this address,
    /// e.g. 127.0.0.1:9090. Omit to run without a control plane.
    #[arg(long)]
    control_addr: Option<SocketAddr>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let raw = std::fs::read_to_string(&cli.scenario)
        .with_context(|| format!("reading scenario {}", cli.scenario.display()))?;
    let scenario: Scenario = toml::from_str(&raw).context("parsing scenario toml")?;

    tracing::info!(seed = scenario.seed, listen = %scenario.listen, "starting proxy");

    let stats = Arc::new(Stats::default());
    let flow = scenario.build_flow()?;
    let extractor = scenario.build_extractor()?;
    let transport = UdpTransport::bind(
        scenario.listen,
        scenario.recv_buf_bytes,
        scenario.multicast_group,
    )?;

    let mut proxy = Proxy::new(
        transport,
        flow,
        Seed(scenario.seed),
        scenario.upstream,
        Arc::clone(&stats),
        extractor,
        scenario.max_in_flight,
    );
    let reload_handle = proxy.reload_handle();

    let control_plane = async {
        match cli.control_addr {
            Some(addr) => control_plane::serve(addr, Arc::clone(&stats), reload_handle).await,
            None => std::future::pending().await,
        }
    };

    let started = Instant::now();
    let outcome = tokio::select! {
        result = proxy.run() => result.context("proxy loop"),
        result = control_plane => result.context("control plane"),
        signal = tokio::signal::ctrl_c() => signal.context("waiting for shutdown signal"),
    };

    let report = RunReport::new(Seed(scenario.seed), started.elapsed(), &stats.snapshot());
    print!("{}", report.render().context("rendering run report")?);

    outcome
}

/// Machine-readable summary of a single run, printed to stdout on shutdown.
///
/// The seed belongs here because it is the only thing needed to replay the run
/// bit for bit.
#[derive(Serialize)]
struct RunReport {
    seed: u64,
    uptime_secs: f64,
    forwarded: u64,
    dropped: u64,
    delayed: u64,
    duplicated: u64,
    send_errors: u64,
    queue_overflows: u64,
}

impl RunReport {
    fn new(seed: Seed, uptime: Duration, stats: &StatsSnapshot) -> Self {
        RunReport {
            seed: seed.0,
            uptime_secs: uptime.as_secs_f64(),
            forwarded: stats.forwarded,
            dropped: stats.dropped,
            delayed: stats.delayed,
            duplicated: stats.duplicated,
            send_errors: stats.send_errors,
            queue_overflows: stats.queue_overflows,
        }
    }

    fn render(&self) -> Result<String, toml::ser::Error> {
        toml::to_string(self)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn sample_snapshot() -> StatsSnapshot {
        StatsSnapshot {
            forwarded: 900,
            dropped: 42,
            delayed: 17,
            duplicated: 3,
            send_errors: 1,
            queue_overflows: 5,
        }
    }

    fn rendered(report: &RunReport) -> toml::Value {
        report.render().unwrap().parse().unwrap()
    }

    #[test]
    fn report_carries_seed() {
        let report = RunReport::new(Seed(4242), Duration::from_secs(1), &sample_snapshot());

        assert_eq!(rendered(&report)["seed"].as_integer(), Some(4242));
    }

    #[test]
    fn report_carries_every_counter() {
        let report = RunReport::new(Seed(1), Duration::from_secs(1), &sample_snapshot());
        let value = rendered(&report);

        for (field, expected) in [
            ("forwarded", 900),
            ("dropped", 42),
            ("delayed", 17),
            ("duplicated", 3),
            ("send_errors", 1),
            ("queue_overflows", 5),
        ] {
            assert_eq!(value[field].as_integer(), Some(expected), "field {field}");
        }
    }

    #[test]
    fn report_carries_uptime_in_seconds() {
        let report = RunReport::new(Seed(1), Duration::from_millis(2500), &sample_snapshot());

        assert_eq!(rendered(&report)["uptime_secs"].as_float(), Some(2.5));
    }
}
