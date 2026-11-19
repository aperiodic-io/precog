//! S2 REST v1 JSON shapes (raw format: UTF-8 bodies) shared by the gateway and the drivers.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq)]
pub struct Pos {
    pub seq_num: u64,
    pub timestamp: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AppendRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub body: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct AppendInput {
    pub records: Vec<AppendRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_seq_num: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fencing_token: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AppendAck {
    pub start: Pos,
    pub end: Pos,
    pub tail: Pos,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SequencedRecord {
    pub seq_num: u64,
    pub timestamp: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub body: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ReadBatch {
    pub records: Vec<SequencedRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail: Option<Pos>,
}

pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// Percentile of sorted samples.
pub fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}
