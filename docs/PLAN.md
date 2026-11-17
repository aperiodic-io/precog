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
| CI runner disk and CPU noise | Perf gate compares base and head on the same runner, median of 3, 10% band |
| Iroh 30–40% slower than TCP | M6 multi-stream; TCP stays for trusted networks |
