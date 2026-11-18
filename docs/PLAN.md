# Precog plan

Precog is a durable event broker for many independent sources. The design point is
**leaderless ingest**:
- Each source owns a writer log `(stream, writer, epoch)` and numbers its own events.
- Any node accepts any append.
- A node acknowledges once `k` failure domains hold an fsynced copy.
- No consensus runs per message.

Consensus is kept for what needs it: membership, metadata and an optional global order. It runs
off the write path.

This document is the plan of record. Evidence behind each decision:

| Question | Where it was answered |
|---|---|
| Is the design faster than current Apache Iggy, and why? | `README.md`, `results/RESULTS.md`, `docs/ARCHITECTURE.md` |
| How do we add a total order without trusting source clocks? | `reports/Leaderless log ordering options.md` |
| Which storage engine and libraries? | `reports/Rust storage engine for precog.md`, `results/STORAGE.md` |

## 1. What we are building

| | |
|---|---|
| Users | Producers we control (market data, telemetry, audit logs), many writers each with an independent sequence. Consumers that need per-source order, and optionally a global order. |
| Interfaces | Rust client (`Writer`, `Subscription`) over TCP or Iroh QUIC. Other languages and a Kafka-compatible gateway come later and are optional. |
| Deployment | 3+ commodity nodes in distinct failure domains, local disks, no object store required. |

### Guarantees (contract)

1. **Durability.** An append acknowledged with `copies = k` is fsynced on `k` nodes with distinct
   failure-domain labels. The ack names them.
2. **Per-writer order and exactly-once storage.** Within a writer log, order is sequence order.
   A byte-identical retry on any node is idempotent, and a conflicting reuse of an identity is
   rejected.
3. **Convergence.** Every node converges to the union of all copies (anti-entropy repair).
4. **Global order, opt-in (M5).** All readers see one final cross-writer order, decided by
   committed cuts. Real-time order across writers is given only with the ordered-ack option.

### Non-goals

- Per-message consensus or one leader per partition.
- Trusting producer clocks for ordering.
- Byzantine faults.
- Wire compatibility with Kafka or Iggy in the core. A gateway may come later.

## 2. Architecture

```
            ┌──────── node (one per failure domain) ─────────────────────────────┐
 Writer ──► │ rpc ──► append ──► storage (segment log, group commit) ─► fsync    │
            │            └─────► replicate to 1 peer per other domain ───► ack@k │
 Subscriber◄│ push ◄── tail cache ◄── storage reads (index → pread)              │
            │ repair: pull peers' inventories, fetch missing                     │
            └────────────────────────────────────────────────────────────────────┘
 control plane (M4): small Raft group: membership, streams, placement, cuts (M5)
```

| Crate | Role |
|---|---|
| `precog-storage` | Node-local segment log: group commit, recycled preallocated segments, CRC-32C framing with generations, per-segment index files, manifest, recovery, retention, tail cache. No network, no async. |
| `precog-mesh` | Node, replication, repair, RPC, TCP and Iroh transports, client (`Writer`, `Subscription`), CLI. |
| `precog-bench` | Same-harness benchmarks against Apache Iggy. |
| `storage-bench` | 13-backend storage comparison (evidence for the storage design). |

## 3. Milestones

Every milestone is test-first (AGENTS.md) and lands behind the gates in §4. Exit criteria are the
milestone's acceptance scenarios plus its performance gates.

| # | Milestone | Scope | Exit criteria | Status |
|---|---|---|---|---|
| M0 | Proof of concept | Mesh, WAL, replication, repair, subscriptions, Iroh; benchmarks against Iggy 0.10 | Durable-2 throughput ≥ 2× Iggy; failover stalls in ms instead of seconds; results reproducible | **done** |
| M1 | Engineering foundation | AGENTS.md, CI on `ubuntu-latest`: fmt, clippy, tests + coverage gate, acceptance suite, mutation testing, instruction-count bench gate, wall-clock perf gate, nightly | CI required on `main`; gates fail on a deliberate regression | in progress |
| M2 | Storage phases 0 + 1 | `precog-storage` replaces the in-memory WAL and store data (details in §5) | Acked data survives SIGKILL at any point; restart replays only the active segment; storage line coverage ≥ 90%; no throughput regression vs M0 | in progress |
| M3 | Consumers | Resumable subscriptions keyed by per-writer frontiers, subscription failover between nodes, stored consumer positions, consumer groups that assign writer logs or keys | A subscriber killed or moved mid-stream loses and repeats nothing; group rebalance acceptance scenarios | planned |
| M4 | Control plane | 3-node Raft (openraft) for membership, streams and topics, placement beyond 3 nodes, users and tokens; per-writer authorization | Add or remove a node without downtime; placement survives the loss of a control-plane node | planned |
| M5 | Global order (optional) | Origin-node cuts (3 integers every T ms) committed through the control plane; lazy binding; ordered subscriptions with global offsets; optional ordered-ack | All readers see the same order; leader kill pauses ordered reads only; ordered e2e p99 ≤ 10 ms at T = 1–2 ms | planned |
| M6 | Client and throughput | Several batches in flight per writer, client linger batching, vectored writes, Iroh multi-stream, sending to `copies-1` peers first | Per-producer throughput ≥ 3× M2 at the same durability | planned |
| M7 | Operations and hardening | Metrics, TLS, CLI, config, containers, versioned wire format, fault-injection and Jepsen-style checks, 72 h soak | Production readiness review | planned |
| M8 | Storage phase 2 and cold tier | Optional O_DIRECT writer with its own cache, io_uring with a blocking fallback, erasure-coded (3+2) sealed history, compression per writer batch | ≥ 40% less disk for history at equal fault tolerance | planned |

## 4. Quality gates (every pull request)

| Gate | Tool | Threshold |
|---|---|---|
| Formatting and lints | `cargo fmt --check`, `cargo clippy -D warnings` | clean |
| Unit, property and integration tests | `cargo nextest` | all pass |
| Coverage | `cargo llvm-cov` + `scripts/coverage-gate.sh` | `precog-storage` ≥ 90% lines, `precog-mesh` ≥ its recorded floor |
| Acceptance | cucumber features in `crates/mesh/tests/acceptance` | all non-`@nightly` scenarios |
| Mutation testing | `cargo mutants --in-diff` | no missed mutant in changed code |
| Instruction-count bench gate | gungraun (callgrind) | ≤ 2% more instructions than the merge base |
| Wall-clock perf gate | `bench/perf-gate/gate.py`, base and head on one runner | median throughput ≥ 90% of base |

Nightly runs the whole acceptance suite including `@stress`, all mutants, and the Iggy comparison.

## 5. Storage engine (M2) design

The evidence is `reports/Rust storage engine for precog.md`: recycled pre-written segments beat
O_APPEND by 1.08–1.33×; embedded databases reach 0.31–0.71× of that at 27 KB.

**Phase 0: the write path.**
- One log per node, made of 1 GiB segment files.
- Segments are preallocated by writing zeros, or recycled from retention, on a background thread
  that keeps a spare ready.
- Writes are buffered `pwrite`, then `fdatasync`; the group commit spans all writers.
- Record framing: `magic | len | crc32c | generation | group_start | payload`. The CRC covers
  everything after itself.
- The segment generation rejects stale records left in recycled files.
- `group_start` separates a torn tail, which is truncated, from mid-log damage, which fails closed.
- Any fsync error is fatal: the log refuses further writes and the process recovers from disk and
  peers.
- The directory is fsynced after every create, rename and delete.

**Phase 1: recovery and reads.**
- Sealing a segment writes an index file next to it: one entry per record with position,
  offset, length, key, first sequence, count and append time.
- A two-slot checksummed manifest records the segments, their generations and states, and the
  retention floor.
- Startup loads the manifest and the index files, and scans only the active segment.
- Retention by age and size drops the oldest sealed segments and recycles their files.
- An in-memory tail cache of recent records serves live subscribers.
- The mesh keeps per-batch metadata in RAM and reads payloads through the index.

## 6. Risks and open questions

| Risk | Mitigation |
|---|---|
| Event-time merge is not an order | Per-writer order is the contract until M5 cuts |
| A writer's retry lands on another node after failover | Hold-back by `(writer, epoch)` predecessor in M5; idempotent identity today |
| Per-batch metadata in RAM grows with history | Bounded by retention; move the per-log index to disk (redb or index files) if it exceeds budget |
| Shared WAL with mixed retention per stream | One retention policy per node in M2; per-stream segment groups if needed |
| CI runner disk and CPU noise | Perf gate compares base and head on the same runner, alternating runs, best of 5, 15% band |
| Iroh 30–40% slower than TCP | M6 multi-stream; TCP stays for trusted networks |

## 7. Replacing `redpanda-raw-internal` (live-ingestion)

Decided with the owner (2026-11-18): Precog replaces live-ingestion's raw cluster first, through
its **native protocol** (clients change; no Kafka wire compatibility). `redpanda-metrics-out`
(customer-facing: TLS, SASL/SCRAM, ACLs, quotas) and `redpanda-metrics` stay on Redpanda until
this is done. Precog must be at least **3× Redpanda** on all three measures in §7.3.

### 7.1 What talks to the raw cluster today (production manifests, 2026-11-18)

| Client | Repo, language | Count | Uses |
|---|---|---|---|
| Producers | `live-producers`, Go (franz-go) | 35 Deployments, 2 replicas each | produce `raw.<datatype>.<exchange>` keyed by symbol, `acks=all`, idempotence off, zstd, `redpanda-dedup-key` header per event; compacted state topics |
| Raw archivers | `archiver`, Rust | 44 | consumer group per topic, commit only after the Parquet upload; a new group starts at `now - 2h` (`rpk group seek` init container) |
| Metric pipelines | `brrrrr` (via `fastka`), Rust | 21 | read every partition of their raw topics; positions in brrrrr's own checkpoints (keyed `(topic, partition) → offset`); sorts each source by event time with a 50 ms hold (ADR-0013), so per-writer order is enough |
| derived-seed | `live-ingestion/cmd/derived_seed`, Go | 4 | reads raw topics |
| Offset seeding | `rpk` image | 46 init containers | `rpk group seek --to now-2h` |
| Topic management | `topicsync` (kadm) | Job + hourly CronJob | declares topics: partitions, RF 3, `retention.bytes`, `retention.ms`, `cleanup.policy` (`delete`, `compact` for state topics), `segment.bytes`, dedup window and header; never deletes |
| Lag monitoring | kafka-exporter + vmalert rules | | per-group lag, "every raw topic has a live consumer" |
| Dedup | Redpanda fork (`dream-faster/redpanda`) | every raw topic | `redpanda.dedup.window.ms = 300000` keyed by the `redpanda-dedup-key` header; index capped at 8M entries/partition, 16M/shard, ~37 B/entry |

### 7.2 What Precog adds for it

| ID | Feature | Done when |
|---|---|---|
| R1 | Streams declared with retention by bytes and time each (per-stream segment groups or per-stream trimming of a shared log) | Two streams with different retention on one node keep exactly their own |
| R2 | Resumable cursors (M3): a position is a per-writer frontier `{(writer, epoch) → next seq}`, valid on every node; start at the beginning, the end, or a time (`now - 2h`) through a per-log time index | A reader killed and resumed on another node loses and repeats nothing (acceptance) |
| R3 | Named consumer positions (groups), stored and replicated by the mesh, committed by the reader after its upload | An archiver restarted on another node resumes at its commit; a new group starts at `now - 2h` |
| R4 | Dedup by an event-key header within a window (§7.4) | Same drop rate as the fork on its own loadgen; bounded memory; no full-log replay on restart |
| R5 | Compacted streams (latest record per key) for the producers' state topics | A reader of a compacted stream sees the newest value of every key after restart and retention |
| R6 | Clients: Go (live-producers, derived-seed) and Rust (archiver, brrrrr source like its `iggy.rs`) | Each repo's tests pass against a local Precog; dual-write shadow on staging |
| R7 | Operations: StatefulSet and chart, Prometheus metrics (append, fsync, repair, dedup, per-group lag), a declarative stream config applied like topicsync (never deletes), replacing the kafka-exporter rules | Staging runs on Precog for 7 days with the alerts green |
| R8 | Cutover: producers dual-write, readers switch one by one, Redpanda raw cluster retired | Archive parity (row counts per hour) between the two paths for 72 h |

### 7.3 The 3× target

Measured on the raw brokers' size (1 CPU, 4 GiB each, 3 nodes, 3 copies, every append durable on
at least 2 before the ack), dedup on, readers attached, with
`live-ingestion/benchmarks/redpanda-dedup/loadgen`'s workload shape (concurrent producer workers,
symbol keys, a per-event dedup header, 0% and 20% resends) against the pinned fork image:

1. sustained append throughput ≥ 3× Redpanda's;
2. throughput per broker CPU core ≥ 3×;
3. p99 ack latency at staging's peak rate ≤ ⅓ of Redpanda's.

Both brokers run in the same harness on the same machine; results go in `results/` with the
commands to reproduce them.

### 7.4 Dedup design

- **Where:** at ingest, on the node that accepts the append. Copies replicated from a peer carry
  the origin's decision and are never deduplicated again, so every node stores the same batches.
  The fork's crashes came from rebuilding its index by replaying the whole log; Precog rebuilds
  from the last window only, through the per-log time index.
- **Routing:** producers send every event of a stream to the same preferred node (by stream), so
  both producer replicas' copies of one event meet one index. During a failover two nodes may
  each keep one copy: the same at-least-once outcome as the fork's evictions, which live-ingestion's
  readers already tolerate.
- **Index:** per stream, a ring of time buckets (window / 8 each), each an open-addressing table
  of 64-bit hashes (xxh3) of the dedup key: 8 bytes per entry instead of ~37, no allocation per
  entry, expiry by dropping a whole bucket. A lookup probes the live buckets. A false positive
  (dropping a distinct event) has probability n / 2⁶⁴ per event, about 10⁻¹² at 8M live keys.
  Memory is capped per node; at the cap the oldest bucket goes first, so the window shortens
  instead of the node failing, and a metric reports the effective window.
