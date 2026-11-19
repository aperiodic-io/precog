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
| `transport.rs` | TCP. |
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
| Per stream: its logs, its own retention floor, and per sealed segment its bytes, batches, events, newest append time and (a lower bound of) its oldest | Per-stream retention (bytes and age, oldest first) and the totals without reading the log |
| Batches of the active segment, pending batches and their waiters | Appends, duplicate checks of recent retries and live subscribers touch only these |
| The segment log's index of the active segment | Appends and the tail |

Everything else is on disk. A sealed segment's index file (`seg-<id>.idx`, version 2) holds a
fixed-size entry per record in LSN order and the same entries grouped by key, each key's by
`first_seq`. Once a segment is sealed and every batch in it durable, `Store::absorb` (run with
retention) drops its batches from RAM. A lookup of an older batch (a late retry, `has`/`get`,
repair's `FetchLog`, a cursor reading old data, retention by stream) reads, per segment whose
range covers it, that log's run from the segment's index: one read. A consumer replaying old
data (a cursor, a seek by time, the event-time read) does those reads on a copy of the stream's
metadata, without the store's lock, so it does not stall appends. Recovery streams the index
a chunk at a time and absorbs each sealed segment as it goes, so it never holds the whole
index either.

Measured (`tests/memory.rs`, 200k batches, heap per retained batch): 1092 B before this
layout, 338 B with interned logs and waiters only while pending, 6 B with sealed segments on
disk. The active segment's batches still cost ~340 B each: with 1 GiB segments and 2 KiB
batches that is ~170 MB, independent of retention.

## The segment cleaner: each stream keeps its own retention

A node keeps every stream in one segment log, and its byte cap (`retention_bytes`) drops the
oldest sealed segments. Without more, a stream far below its own budget (topicsync's
`retention.bytes` / `retention.ms`, `StreamConfig`) loses its history at the node's time horizon
(cap / total ingest rate), which high-volume streams set. A Redpanda topic keeps its own
retention whatever the others write. So before the node drops segments, it copies forward what
streams still retain in them (`Node::apply_retention`):

1. Each stream's own retention runs first: it forgets the stream's oldest batches, oldest first
   by (append time, LSN), logically (`Store::trim_stream`).
2. The node takes the segments its cap (and age limit) drops, absorbed ones only, and as many
   more as the copies need room for: after the copy and the drop the log is within the cap.
3. What a stream with a retention of its own still retains in those segments is copied: read
   from the old segment, appended at the head as it was (`Wal::copy`: key, sequence range,
   checksum, append time and payload unchanged, so only the LSN changes), in groups of at most
   4 MiB, each its own write to the log. Appends go on between the groups.
4. The store takes each copy in place of its batch (`Store::moved`): the old record is never
   served again, and the copy is in RAM until its segment is sealed and absorbed.
5. The node syncs the log (write caching: the copies were not synced yet) and only then drops
   the segments, and the store forgets what went with them.

Streams without a retention of their own keep what the node cap leaves, as before. A compacted
stream is not copied: it keeps its newest value per key in its snapshot, as before. The node's
age limit (`retention_ms`) stays a ceiling: nothing older is copied.

**Identity.** A copy is the same batch: identity `(stream, writer, epoch, first_seq)`, bytes and
append time. Cursors, `read_cursor`, `cursor_at`, the event-time read, `has`/`get`, frontiers and
repair key on identity and sequence and answer as before (`store::tests::a_batch_copied_forward_answers_every_query_as_before`).
Peers see the same frontiers and bytes, so repair fetches nothing. A live feed drops a batch below
its log's next sequence, so it never pushes a copy again; a replay delivers each batch once
(`a_batch_copied_forward_is_not_new_to_feeds_or_peers`). A stream trims oldest first by append
time, which a copy keeps, so a copy does not outlive the batches logged after it. Append times
never go back (the WAL stamps the later of now and the newest time it gave, seeded from the log),
so a clock stepped back does not put a new batch below a stream's trim floor. A cursor read
holds a log's batches that a copy put ahead of their predecessor, and a seek by time takes each
log's lowest sequence logged at or after the time.

**Crash safety.** The old record stays until the copy is on stable storage: the copy is appended
through the log like any group (fsynced, or under write caching synced before the drop), and the
segment is dropped only after that. A crash before the sync leaves the old record (and maybe the
copy); one after the manifest names the drop leaves the copy. Recovery keeps the first of two
records of one identity with the same checksum, so a batch logged twice is kept once, and the
next pass copies it again and drops the old segment. Tests: `a_crash_anywhere_between_copy_and_delete_keeps_every_retained_batch_once`
(storage failpoints at the copy's fsync, the sync before the drop, before and after the manifest
write).

**Bounds.** Copying never takes the log past the cap: the node drops as many segments as the
copies need room for (a group's bytes plus a segment header per group, at most). If what streams
retain exceeds the cap, the cap wins: it cuts the oldest batches of the stream furthest over its
share (the cap divided equally among the streams with a retention of their own: the one retaining
the most bytes, down to the next one, then both), counted in
`precog_retention_forced_total{stream}`. A sealed segment not absorbed yet (it holds a batch not
marked durable yet) waits a pass; its bytes take no room from the copies, so the log can be over
the cap by them for that pass, but nothing retained is cut for them. A retained record that does
not read back (a checksum failure) goes with its segment, logged, as before copying: the pass
goes on and the cap keeps dropping. Copy volume per pass is at most what streams retain in
the dropped segments, written in groups of at most 4 MiB, so no write holds the log longer than
an ordinary group. Steady state, a retained batch is copied once per turnover of the log: copy
volume is about what streams retain beyond the newest segments per cap's worth written. Metrics:
`precog_retention_copied_bytes_total`, `precog_retention_copied_batches_total`,
`precog_log_bytes`.

RAM: the copies of a pass stay in RAM as the active segment's batches do, until absorbed; the
old records' LSNs are held until their segment is dropped (the same pass).

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

* Storage is one segment log per node (`crates/storage`): segments, index files, a
  manifest, recovery, the node cap and each stream's own retention (the segment cleaner
  above). A compacted stream keeps each key's newest value in a snapshot; segments are not
  rewritten by key. There is no cold tier (erasure-coded or object store): a node keeps what
  it retains on its own disk.
* Placement is static: every node targets 3 copies, chosen as the first peer per
  distinct domain. There is no controller, membership changes, rebalancing or
  rack/zone awareness beyond labels.
* Security: one shared token. A token holder can claim any peer id, and there
  is no per-writer authorization.
* Live subscriptions (`Kind::Subscribe` / `Push`, `client::Subscription`) push each batch once it is durable on the serving node, in local durable order. The client restores per-log order. There is no resume or failover to another node yet. Repair pulls the full inventory after a restart
  (cursor 0), not Merkle ranges.
