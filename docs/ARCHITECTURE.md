# Precog mesh POC: architecture and guarantees

## The idea in one paragraph

Apache Iggy 0.10 replicates every partition with Viewstamped Replication
(VSR). Each partition has one primary, and every produce for that partition goes
through it. The primary assigns offsets, sends `prepare` to its backups,
waits for a quorum and then commits. The mesh removes consensus from the data
path. The unit of replication is a **writer log** `(stream, writer, epoch)` with
exactly one writer. The writer numbers its own events, so there is nothing to
agree on. Any node accepts an append for any log. It writes the batch to its own
WAL and, in parallel, sends the same bytes to one peer per other failure domain.
It acknowledges once `copies` distinct domains report a durable copy. Readers
merge logs by **event time**, a timestamp the source supplies. Offsets assigned
on arrival play no part in read order.

```
        Iggy VSR (per partition)                     precog mesh (per writer log)

 client ──► primary(P) ──prepare──► backup         client ──► any node ─┬─► WAL fsync (local)
              │ fsync  ◄─prepare_ok─ │ fsync                             ├─► peer B: WAL fsync ─► ack
              │ commit when quorum   │                                   └─► peer C: WAL fsync ─► ack
              ▼                                                          ack when `copies` domains durable
            reply                                                        (the third copy completes in background)
 failover: view change (≥ heartbeat_timeout)       failover: client retries the same bytes on another node
```

## Data model

| Concept | Definition |
|---|---|
| Writer log | `(stream, writer, epoch)`. A single writer owns it. A writer that restarts without its sequence state takes a new epoch. |
| Batch | Immutable, covers sequences `first_seq .. first_seq+count`, crc32c over the whole batch (`crates/mesh/src/proto.rs`) |
| Identity | `(stream, writer, epoch, first_seq)`. A retry with identical bytes is idempotent. Any other overlap is a **conflict**, even if only the timestamp differs. |
| Event time | `u64` µs per event, used **only** for read order: `(event_time, writer, epoch, seq)`. Every node with the same content returns the same order. |
| Durability receipt | `Ack { status, copies, nodes bitmask, crc }`. `copies` counts distinct failure-domain labels holding an fsynced copy. |

## Components (all implemented, tested)

| File | What it does |
|---|---|
| `proto.rs` | Batch codec (zero re-encoding: client bytes go unchanged to WALs and peers), RPC framing, acks, control messages |
| `wal.rs` | One WAL per node with **cross-writer group commit**: a dedicated thread drains every queued append into one `write` + one `fdatasync`. Exclusive file lock. Recovery truncates a torn tail and fails closed on interior corruption. An I/O error fences the WAL. |
| `store.rs` | Index over the segment log: per-log `BTreeMap<first_seq, batch>` for the active segment, sealed segments' batches read from their index files (see below), reservation of pending ranges (concurrent duplicates coalesce), durable-only visibility, frontiers (`contiguous` prefix + `high`), event-time snapshot read |
| `node.rs` | Append = local WAL ‖ replicate to one peer per other domain. **Early completion** at `copies`. Replicate is accepted only from authenticated peers and does not fan out further. Anti-entropy repair pulls each peer's durable inventory incrementally (cursor over the peer's WAL order). |
| `rpc.rs` | Multiplexed request/response over one byte stream. The writer task coalesces frames. |
| `transport.rs` | TCP. QUIC/Iroh was removed: TCP measured 1.4–1.6x faster. |
| `client.rs` | `Writer` owns the sequence numbers. On a timeout, a transport error or an `Unknown` ack it retries the **same bytes** on the next node until a deadline. Failover sticks to the node that answered. |

Tests are in `crates/mesh/src/*` (unit) and `crates/mesh/tests/cluster.rs`
(multi-node over real TCP sockets). Run them with `cargo test`.

## What a node keeps in RAM

A node retains tens of millions of small batches (live-ingestion keeps ~74 GiB per node, and its
producers flush one batch per writer log every 100 ms), so RAM per retained batch decides
whether it fits its pod. `crates/mesh/tests/memory.rs` measures it and fails past a budget.

| Kept in RAM | Why |
|---|---|
| Per writer log: its id (once, interned), `trimmed`, `contiguous`, `high`, and per sealed segment holding its batches their sequence range | Frontiers, repair and `Expired` answer from these alone; the ranges say which segment indexes a lookup must read |
| Per stream: its logs, its own retention floor, and per sealed segment its bytes, batches, events and newest append time | Per-stream retention (bytes and age) and the totals without reading the log |
| Batches of the active segment, pending batches and their waiters | Appends, duplicate checks of recent retries and live subscribers touch only these |
| The segment log's index of the active segment | Appends and the tail |

Everything else is on disk. A sealed segment's index file (`seg-<id>.idx`, version 2) holds a
fixed-size entry per record in LSN order and the same entries grouped by key, each key's by
`first_seq`. Once a segment is sealed and every batch in it durable, `Store::absorb` (run with
retention) drops its batches from RAM. A lookup of an older batch (a late retry, `has`/`get`,
repair's `FetchLog`, a cursor reading old data, retention by stream) reads, per segment whose
range covers it, that log's run from the segment's index: one read. Recovery streams the index
a chunk at a time and absorbs each sealed segment as it goes, so it never holds the whole
index either.

Measured (`tests/memory.rs`, 200k batches, heap per retained batch): 1092 B before this
layout, 338 B with interned logs and waiters only while pending, 6 B with sealed segments on
disk. The active segment's batches still cost ~340 B each: with 1 GiB segments and 2 KiB
batches that is ~170 MB, independent of retention.

## Guarantees (and what is traded away)

What the mesh provides:

* An acknowledged batch with `copies = k` is fsynced on `k` nodes whose failure-domain
  labels differ. The ack says which nodes hold it.
* Per writer log, order equals sequence order. There are no gaps unless the writer
  leaves them, and frontiers make gaps visible.
* Exact retries are idempotent on any node, so at-least-once from the client becomes
  exactly-once storage.
* A batch that returned `Unknown` may still exist. The client retries the same
  bytes, and repair propagates whatever copies exist.
* Every node converges to the union of all copies (anti-entropy). Given the same
  content, reads are deterministic.

What Iggy's VSR provides and the mesh deliberately does not:

* **One total order per partition across many producers.** In the mesh the only
  total order is per writer. A cross-writer order is derived from event time, which
  the source controls. Late events are possible, and a reader has to know when it is
  "complete enough". The `coverage`/frontier fields exist for that, but
  finalized watermarks are not implemented.
* **Linearizable consumer offsets and consumer groups.** Not implemented. The plan
  keeps a small consensus control plane for these, off the data path.
* **Reads that never go backwards across nodes.** A read from one node covers what
  that node has. After repair, all nodes agree. A reader that needs a particular
  receipt has to pass the ids it requires (planned; frontiers are already available).

## Not implemented (POC limits)

* Data is also held in RAM. There is no segment engine, retention, compaction or
  erasure-coded cold tier. The production plan keeps the WAL as the only
  synchronous write and builds segments and indexes off the ack path.
* Placement is static: every node targets 3 copies, chosen as the first peer per
  distinct domain. There is no controller, membership changes, rebalancing or
  rack/zone awareness beyond labels.
* Security: one shared token. A token holder can claim any peer id, and there
  is no per-writer authorization.
* Live subscriptions (`Kind::Subscribe` / `Push`, `client::Subscription`) push each batch once it is durable on the serving node, in local durable order. The client restores per-log order. There is no resume or failover to another node yet. Repair pulls the full inventory after a restart
  (cursor 0), not Merkle ranges.
