use crate::domain::Operator;
use crate::flows::Flow;
use crate::scripts::{Dropper, Duplicator, JitterDelay, RateLimiter, Reorderer};
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Scenario {
    pub seed: u64,
    pub listen: SocketAddr,
    pub upstream: SocketAddr,
    #[serde(default)]
    pub multicast_group: Option<SocketAddr>,
    #[serde(default = "default_recv_buf")]
    pub recv_buf_bytes: usize,
    #[serde(default)]
    pub operators: Vec<OperatorConfig>,
}

fn default_recv_buf() -> usize {
    4 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OperatorConfig {
    Drop { probability: f64 },
    Duplicate { probability: f64 },
    Jitter { min_ms: u64, max_ms: u64 },
    Reorder { probability: f64, hold_ms: u64 },
    RateLimit { packets_per_sec: u32 },
}

impl Scenario {
    #[must_use]
    pub fn build_flow(&self) -> Flow {
        let operators = self.operators.iter().map(OperatorConfig::build).collect();
        Flow::new(operators)
    }
}

impl OperatorConfig {
    fn build(&self) -> Box<dyn Operator> {
        match *self {
            OperatorConfig::Drop { probability } => Box::new(Dropper::new(probability)),
            OperatorConfig::Duplicate { probability } => Box::new(Duplicator::new(probability)),
            OperatorConfig::Jitter { min_ms, max_ms } => Box::new(JitterDelay::new(
                Duration::from_millis(min_ms),
                Duration::from_millis(max_ms),
            )),
            OperatorConfig::Reorder {
                probability,
                hold_ms,
            } => Box::new(Reorderer::new(probability, Duration::from_millis(hold_ms))),
            OperatorConfig::RateLimit { packets_per_sec } => {
                Box::new(RateLimiter::new(packets_per_sec))
            }
        }
    }
}
