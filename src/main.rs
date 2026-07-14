use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tickchaos::backgrounds::{Proxy, Stats};
use tickchaos::config::Scenario;
use tickchaos::domain::Seed;
use tickchaos::transport::UdpTransport;

#[derive(Parser)]
#[command(name = "tickchaos", about = "UDP market-data degradation proxy")]
struct Cli {
    #[arg(short, long)]
    scenario: PathBuf,
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
    let flow = scenario.build_flow();
    let transport = UdpTransport::bind(scenario.listen, scenario.recv_buf_bytes)?;

    let mut proxy = Proxy::new(
        transport,
        flow,
        Seed(scenario.seed),
        scenario.upstream,
        Arc::clone(&stats),
    );

    proxy.run().await?;
    Ok(())
}
