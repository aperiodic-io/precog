//! Precog mesh POC: leaderless, per-writer-log replication over TCP.
//!
//! See `docs/ARCHITECTURE.md` at the repository root.

pub mod client;
pub mod node;
pub mod proto;
pub mod rpc;
pub mod store;
pub mod transport;
pub mod wal;
