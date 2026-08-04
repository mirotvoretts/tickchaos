use crate::backgrounds::DEFAULT_MAX_IN_FLIGHT;
use crate::domain::{NoopExtractor, Operator, ProxyError, SeqNum, SequenceExtractor};
use crate::flows::Flow;
use crate::protocols::MoldUdp64Extractor;
use crate::scripts::{DropSeq, Dropper, Duplicator, GapBurst, JitterDelay, RateLimiter, Reorderer};
use serde::Deserialize;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
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
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: NonZeroUsize,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub operators: Vec<OperatorConfig>,
}

fn default_recv_buf() -> usize {
    4 * 1024 * 1024
}

fn default_max_in_flight() -> NonZeroUsize {
    DEFAULT_MAX_IN_FLIGHT
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OperatorConfig {
    Drop { probability: f64 },
    Duplicate { probability: f64 },
    Jitter { min_ms: u64, max_ms: u64 },
    Reorder { probability: f64, hold_ms: u64 },
    RateLimit { packets_per_sec: u32 },
    DropSeq { seqnums: Vec<u64> },
    GapBurst { start: u64, len: u32 },
}

impl Scenario {
    pub fn build_flow(&self) -> Result<Flow, ProxyError> {
        let operators = self
            .operators
            .iter()
            .map(OperatorConfig::build)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Flow::new(operators))
    }

    pub fn build_extractor(&self) -> Result<Box<dyn SequenceExtractor>, ProxyError> {
        match self.protocol.as_deref() {
            None | Some("none") => Ok(Box::new(NoopExtractor)),
            Some("moldudp64") => Ok(Box::new(MoldUdp64Extractor)),
            Some(unknown) => Err(ProxyError::Config(format!("unknown protocol: {unknown}"))),
        }
    }
}

impl OperatorConfig {
    fn build(&self) -> Result<Box<dyn Operator>, ProxyError> {
        let operator: Box<dyn Operator> = match self {
            OperatorConfig::Drop { probability } => Box::new(Dropper::new(*probability)?),
            OperatorConfig::Duplicate { probability } => Box::new(Duplicator::new(*probability)?),
            OperatorConfig::Jitter { min_ms, max_ms } => Box::new(JitterDelay::new(
                Duration::from_millis(*min_ms),
                Duration::from_millis(*max_ms),
            )?),
            OperatorConfig::Reorder {
                probability,
                hold_ms,
            } => Box::new(Reorderer::new(
                *probability,
                Duration::from_millis(*hold_ms),
            )?),
            OperatorConfig::RateLimit { packets_per_sec } => {
                Box::new(RateLimiter::new(*packets_per_sec))
            }
            OperatorConfig::DropSeq { seqnums } => {
                Box::new(DropSeq::new(seqnums.iter().copied().map(SeqNum).collect()))
            }
            OperatorConfig::GapBurst { start, len } => {
                Box::new(GapBurst::new(SeqNum(*start), *len))
            }
        };
        Ok(operator)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn scenario_with_protocol(protocol: Option<&str>) -> Scenario {
        Scenario {
            seed: 1,
            listen: "127.0.0.1:9000".parse().unwrap(),
            upstream: "127.0.0.1:9001".parse().unwrap(),
            multicast_group: None,
            recv_buf_bytes: default_recv_buf(),
            max_in_flight: default_max_in_flight(),
            protocol: protocol.map(str::to_owned),
            operators: vec![],
        }
    }

    fn moldudp64_header(sequence: u64) -> Vec<u8> {
        let mut bytes = b"SESSION001".to_vec();
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes
    }

    #[test]
    fn missing_protocol_builds_noop_extractor() {
        let extractor = scenario_with_protocol(None).build_extractor().unwrap();
        assert_eq!(extractor.extract(&moldudp64_header(42)), None);
    }

    #[test]
    fn none_protocol_builds_noop_extractor() {
        let extractor = scenario_with_protocol(Some("none"))
            .build_extractor()
            .unwrap();
        assert_eq!(extractor.extract(&moldudp64_header(42)), None);
    }

    #[test]
    fn moldudp64_protocol_builds_moldudp64_extractor() {
        let extractor = scenario_with_protocol(Some("moldudp64"))
            .build_extractor()
            .unwrap();
        assert_eq!(extractor.extract(&moldudp64_header(42)), Some(SeqNum(42)));
    }

    #[test]
    fn drop_seq_operator_parsed_from_toml() {
        let raw = r#"
            seed = 7
            listen = "127.0.0.1:9000"
            upstream = "127.0.0.1:9001"
            protocol = "moldudp64"

            [[operators]]
            type = "drop_seq"
            seqnums = [100, 101, 102]
        "#;

        let scenario: Scenario = toml::from_str(raw).unwrap();

        assert!(matches!(
            scenario.operators.as_slice(),
            [OperatorConfig::DropSeq { seqnums }] if seqnums == &[100, 101, 102]
        ));
        assert!(scenario.build_flow().is_ok());
    }

    #[test]
    fn gap_burst_operator_parsed_from_toml() {
        let raw = r#"
            seed = 7
            listen = "127.0.0.1:9000"
            upstream = "127.0.0.1:9001"
            protocol = "moldudp64"

            [[operators]]
            type = "gap_burst"
            start = 1000
            len = 50
        "#;

        let scenario: Scenario = toml::from_str(raw).unwrap();

        assert!(matches!(
            scenario.operators.as_slice(),
            [OperatorConfig::GapBurst {
                start: 1000,
                len: 50
            }]
        ));
        assert!(scenario.build_flow().is_ok());
    }

    #[test]
    fn unknown_protocol_rejected() {
        let result = scenario_with_protocol(Some("itch50")).build_extractor();
        assert!(matches!(result, Err(ProxyError::Config(message)) if message.contains("itch50")));
    }
}
