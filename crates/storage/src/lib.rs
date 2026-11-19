//! Node-local durable segment log (docs/PLAN.md §5).
pub mod log;
pub mod record;

pub use log::{Config, Entry, Failpoints, Flush, Log, Reader, Record, SEG_HEADER, Sealed, Stats};

/// Node-local log sequence number: one per record, contiguous, never reused.
pub type Lsn = u64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt: {0}")]
    Corrupt(String),
    #[error("fatal: log refused further writes after: {0}")]
    Fatal(String),
    #[error("invalid argument: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, Error>;
