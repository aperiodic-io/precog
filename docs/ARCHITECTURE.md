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
| `node.rs` | Append = local WAL ‖ replicate to one peer per other domain. **Early completion** at `copies`. Replicate is accepted only from authenticated peers and does not fan out further. Anti-entropy repair follows each peer's log from a cursor (`Control::Inventory`) and compares only the frontiers of the logs it wrote since. |
| `summary.rs` | Dormant streams' summaries and the store's checkpoint on disk (`<data_dir>/streams.redb`, see "Dormant streams"). |
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
| Per writer log of a stream in RAM: its id (once, interned), `trimmed`, `contiguous`, `high`, and per sealed segment holding its batches their sequence range | Frontiers, repair and `Expired` answer from these alone; the ranges say which segment indexes a lookup must read |
| Per stream in RAM: its logs, its own retention floor, and per sealed segment its bytes, batches, events, newest append time and (a lower bound of) its oldest | Per-stream retention (bytes and age, oldest first) and the totals without reading the log |
| Per sealed segment: its totals over every stream, and per key in its index the key's place in the file and its run's start (8 bytes; the key itself is read from the index file) | Dropping a segment subtracts what dormant streams held in it without waking them; a lookup by key is a binary search over the file |
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

## Dormant streams

A stream's metadata in RAM costs ~1.6 KB (its logs, their frontiers, its totals per segment), so
a node of millions of streams that each write now and then would hold them all (docs/S2-PLAN.md,
F12). A stream nobody uses leaves RAM instead and is a summary on disk until it is used again.

**When.** At the end of each retention pass (`Node::apply_retention`, every 10 s), after the
checkpoint below, a stream leaves RAM (`Store::evict`) if it has no batch in RAM (all of its
batches are in absorbed segments, none pending) and its summary on disk is current, and either
nothing used it for `dormant_after_ms` (default 10 minutes: past the dedup window) or the
streams in RAM cost more than `hot_streams_bytes` (default 256 MiB, least recently used first).
A use is an append, a read (cursor, seek, `get`, event-time read) or a live feed's poll;
retention, repair and metrics wake a stream without counting as one. No stream leaves RAM while
a checkpoint is in flight. live-ingestion's 62 streams are written every 100 ms: none leaves.

**What stays in RAM per dormant stream: nothing** of the store's (its log indexes are reused),
plus 8 bytes per sealed segment holding its batches (that segment's key table) and its dedup
ids until they expire (`dedup::Streams::sweep` drops a stream's index once it saw no id for a
window). Measured by `tests/dormant_memory.rs`: 37 B of heap per dormant stream, 2,649 B
before.

**On disk.** One redb database per node (`<data_dir>/streams.redb`; redb was already a
dependency of the workspace, pure Rust, copy-on-write pages, its commit atomic; quick repair is
on, so a crash costs no walk of the file). Table `streams`: per stream one value in this crate's
format (`summary::encode_stream`: version byte, numbers as LEB128, CRC-32C; 62 bytes for a
stream of one log in one segment): its retention floor and bytes, per
absorbed segment its totals and age bounds, per writer log its frontier (`trimmed`, `contig`,
`high`, out-of-order flag) and its sequence range per absorbed segment. Table `meta`: the
checkpoint (`summary::Checkpoint`): `cold_end` and `contiguous`, the LSNs of the batches in RAM
(as runs), the node's floor and totals, the totals per absorbed segment, the dead and forgotten
LSNs, repair's position in each peer's log, and this node's boots. A version mismatch refuses
to open, loudly. redb over index files or a file per stream: millions of small values updated in
place with crash safety is what a B-tree with copy-on-write commits does; index files are
written once per segment, and a file per stream is millions of inodes and fsyncs.

**Waking** (`Inner::wake`): one read of the stream's summary (a cached read-only table, ~0.5 µs
from the page cache), no index read. What retention did meanwhile catches up then
(`Inner::reconcile`: whole segments dropped since go, as `drop_segment` did to the streams in
RAM; a floor inside a segment wakes every stream first). Any operation that needs the stream
wakes it, under the store's lock: reserve, dup and conflict checks, `has`/`get`, cursors,
`cursor_at`, `read`, `live_start`, `log_batches` (repair), `frontier`, retention by stream, the
cleaner's copies. `tail` reads a dormant stream's floor from its summary without waking it;
`frontiers(None)` and `live_start(None)` (every log: `Status`, a feed of every stream) read every
summary.

**The checkpoint** (`Store::begin_checkpoint` under the lock, `end_checkpoint` without it):
once per pass, and before a pass drops segments, so it always covers the log past the first
LSN. Each changed stream's summary as RAM holds it (its batches in RAM counted), the checkpoint
above, and in between the log syncs what it covers (write caching: the checkpoint never counts
a batch a power loss can take). A failed write marks its streams changed again.

**Restart** (`Store::open`): the checkpoint, the segments dropped since subtracted, then the
batches in RAM at the checkpoint put back (counted already), then the log from `cold_end` on
replayed: a record below `contiguous` not in RAM was forgotten (skipped), one past it is new
unless dead or forgotten. Streams the replay does not touch stay dormant: the start reads the
log's tail (the unabsorbed segments), not every stream (`a_restart_does_not_rebuild_dormant_
streams_so_it_costs_the_same_for_more_of_them`). The log's own open still checks each sealed
index file (its CRC and key table: a few bytes per stream). An older data directory (no
`streams.redb`) is rebuilt from the whole log, its `floors` file restored, as before.

**Repair** follows each peer's log instead of comparing every log's frontier: `Control::Inventory
{from, max, incarnation}` returns the frontiers of the logs with a record at LSNs `from..` (at
most `max` records), and the follower fetches where the peer holds sequences it lacks. A pass
costs what the peer logged since the last one; a caught-up pass wakes nothing. The cursor is in
the follower's checkpoint (a new data directory follows every peer from its start). A peer may
lose an unsynced tail in a crash and log other records at those LSNs: it keeps its boots
(incarnation, where its log ended at the open) and answers a follower of an older boot from the
lowest open since (`Store::rejoin`, `spec/mesh/Inventory.tla`). `Status` still lists every log
(reading every summary): nodes no longer poll it.

**Metrics**: `precog_streams_hot`, `precog_streams_dormant`, `precog_stream_loads_total`,
`precog_stream_evictions_total`, `precog_stream_load_microseconds_total` (its rate over the
loads' is the mean load time); `Status` has `streams_hot` and `streams_dormant`.

**Config**: `dormant_after_ms` (default 600,000), `hot_streams_bytes` (268,435,456),
`summary_cache_bytes` (redb's page cache, 67,108,864). For many streams: keep the defaults, and
size `segment_size` so the active segment's batches fit RAM (they stay until sealed: ~340 B each).

Tests: `store::model_dormant` (a twin store, see spec/README.md), the `dormant` reruns of the
store's tests (every stream out of RAM after every batch and before every query),
`absorbed_segments_answer_every_query_as_ram_does` and `a_batch_copied_forward_answers_every_query_as_before`
(also dozing and restarted from the checkpoint), `tests/dormant.rs` (wakes, crashes at every
failpoint of a pass, appends racing evictions, restart cost), `repair_follows_a_peers_log_and_wakes_only_the_streams_it_wrote_to`,
`node::props_cleaner` (dozing), and `features/dormant.feature` (3 nodes, a restart, a replaced
node).

## The segment cleaner: each stream keeps its own retention

A node keeps every stream in one segment log, and its byte cap (`retention_bytes`) drops the
oldest sealed segments. Without more, a stream far below its own budget (topicsync's
`retention.bytes` / `retention.ms`, `StreamConfig`) loses its history at the node's time horizon
(cap / total ingest rate), which high-volume streams set. A Redpanda topic keeps its own
retention whatever the others write. So before the node drops segments, it copies forward what
streams still retain in them (`Node::apply_retention`):

1. Each stream's own retention runs first: it forgets the stream's oldest batches, oldest first
   by (append time, sequence, LSN), logically (`Store::trim_stream`).
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
next pass copies it again and drops the old segment. What a stream's own retention forgot stays
forgotten across a restart: a copy of it can outlive the segments of its next batches, so each
stream's floor (append time, sequence, LSN) is written to `<data_dir>/floors` (format `PCF1`,
CRC-32C; a temporary file, fsynced, renamed, then the directory) before a pass drops segments,
and restored when the node opens (`a_restart_does_not_bring_back_what_a_stream_s_retention_forgot`).
The streams' floors are now in their summaries (the checkpoint before the drop writes them); the
`floors` file is read only to rebuild an older data directory. Tests: `a_crash_anywhere_between_copy_and_delete_keeps_every_retained_batch_once`
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

## Stream classes: retention for streams no config names

A stream per agent session cannot be named in a node's config. A `streams` key ending in `*` is a
**class**: it configures every stream whose name starts with what is before the `*`
(`"acme/*": {"retention_ms": 2592000000}` is every stream named `acme/...`). One function decides
(`NodeConfig::stream`): a stream's own entry, else the class with the longest prefix it starts
with, else none. No class covers a stream named `__...` (the node's own, such as reader
positions). A class cannot be `compacted` (a compacted stream's newest values are kept in RAM
per configured name): the node refuses to open.

A class may have millions of streams, most of them idle on disk, so nothing walks them:

| When | What is trimmed |
|---|---|
| Every retention pass | The streams in RAM (`Store::streams_in_ram`). A stream grows only by appends, which put it in RAM, so every stream over its bytes is among them |
| When the node starts | The streams its replay loaded, before the start's checkpoint covers them (`Node::open`). After that checkpoint a restart replays nothing of them, so one left over its bytes would sit on disk, out of every pass's sight, until it is read or written (a counterexample of `props_cleaner`: a crash between a start and its first pass's checkpoint) |
| Before a read is served (`Node::expire`: a cursor read, a seek, the event-time read, a fetch of one log or by batch ids, a live feed of one stream) | That stream, loaded if it was idle, to its bytes and its age limit. Nothing past a stream's age limit is served, whether a pass saw it or not. This holds for a stream named in the config too |
| Before the node cap drops a segment (`Node::class_streams`) | The class streams with batches in that segment, which its index names. What they still retain is copied forward like a named stream's |

A trim with nothing to forget reads no index and walks no batches (`Store::trim_stream`): the
stream's bytes, the oldest each absorbed segment still holds of it (where the last trim
stopped) and a bound on the oldest of its batches in RAM are kept, so a read pays for expiry
only when something expired. It does take the store's lock once more than before.

A read's trim runs on the reader's task, so it can forget a batch between the cleaner's check
that the batch is still retained and the end of its copy's write. The store takes such a copy as
a record it never serves (`Store::moved`; `spec/store/Cleaner.tla`), where it used to require
the batch to be there.

Limits.
- A live feed of every stream is in log order and does not load idle streams: it can serve a
  batch past its class's age limit until that stream is next read or its segment is dropped.
- **Size the node cap above what the classes retain.** A named stream over the cap is an
  operator's choice; a class gets there by its tenants writing. When streams retain more than
  the cap, the cap wins as before, cutting the stream furthest over its share, but then every
  pass takes every absorbed segment, loads every class stream in them, cuts in a loop that is
  quadratic in the streams being cut, and rewrites what is left. Nothing limits what a class
  holds in all (a per-class quota is not built): watch `precog_retention_forced_total`.
- The cleaner reads the whole index of each segment it is about to drop for the class streams
  in it, and loads each of them (about 1.6 KB of RAM until the pass ends). `precog_retention_forced_total` counts a class's cuts under the class's key, one
series however many streams it has.

Tests: `node::props_classes` (the lookup against the rule; every read of an idle stream past its
age limit; a pass that loads no idle stream; the metric), `node::props_cleaner` rerun with the
streams' retention from classes, `store::model_trim` (a trim against a reference, and asked
again it reads nothing), `store::tests::a_trim_with_nothing_to_forget_reads_no_index`,
`a_batch_forgotten_while_its_copy_was_being_written_is_never_served_and_holds_nothing_up`, and
`features/storage.feature` (3 nodes, a class stream through the node cap).

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
