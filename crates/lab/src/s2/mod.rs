//! S2 stream semantics on top of a Precog writer log (evidence for docs/S2-PLAN.md).
//!
//! One S2 stream is one writer log whose sequence numbers *are* the stream's `seq_num`s. A
//! single [`Sequencer`] per stream (the stream's owner) assigns them, checks `match_seq_num` and
//! the fencing token, and stamps monotone timestamps; the writer log's 2-of-3 durability and its
//! conflict rule (any overlap with other bytes is refused) are left to the mesh.

pub mod record;
pub mod sequencer;

pub use record::Record;
pub use sequencer::{Assigned, Reject, Sequencer};
