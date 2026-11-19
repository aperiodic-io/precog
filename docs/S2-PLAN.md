# Precog against S2: a plan to compete with s2.dev

Status: proposal (2026-11-19). It is evidence-backed but not adopted: it does not change
[`PLAN.md`](PLAN.md), whose first customer, live-ingestion's raw cluster, comes first. Raw
numbers and caveats are in [`results/s2.md`](../results/s2.md). The research summary with sources
is in [`reports/S2 comparison.md`](../reports/S2%20comparison.md). The POCs are in `crates/lab/src/s2`
(sequencer, run by CI) and `crates/s2-lab` (gateway and drivers, an evidence crate outside the
workspace).

## 0. Headline

1. **Build S2's API as a layer, not a rewrite.** An S2 stream maps onto one Precog writer log
   whose sequence numbers *are* the stream's `seq_num`s. A per-stream owner (a sequencer task, as
   in s2-lite) assigns them and checks `match_seq_num` and fencing tokens. The mesh supplies 2-of-3
   durability unchanged. The POC gateway serves S2's REST API this way at
   **229k records/s at 5.0 ms p50** durable on 2 of 3 nodes, against s2-lite's **31k at 51 ms**
   on S3-compatible storage (99k at 14 ms with a 5 ms flush), through the same HTTP/JSON client. The leaderless writer-log path stays as it is for
   the workloads it was built for.
2. **The zombie-owner problem is already solved by the mesh's conflict rule, provided acks need a
   majority.** Two owners numbering the same seq_num through different nodes: at 2 of 3, 200 of
   200 races had exactly one winner. At 1 of 3, both were acknowledged in 132 of 200
   (`crates/lab/tests/s2_zombie.rs`). So a stream-level sequence needs no consensus on the data
   path. It needs an owner lease (control plane), `copies` ≥ a majority, and cleanup of losing
   minority copies before repair spreads them.
3. **Object storage should be a tier, not the ack path, by default.** Local-disk quorum acks are
   26x faster at p50 than s2-lite on S3 at its default flush (1.98 against 51.2 ms at 10k records/s; 3x against a 5 ms flush), and they don't pay per-PUT
   charges, which force object-storage WALs to batch across streams. Long retention, though, is
   where S2's design wins on AWS: $0.023/GB-month on S3, against ~$0.24 for three gp3 copies. On
   rented HDD boxes with erasure coding the tier costs ~$3/TB-month (`reports/S2 cold storage.md`).
   Recommendation: tier sealed segments to S3 (M8 already plans a cold tier). Offer an opt-in
   "standard" class that acks after a PUT, for users who want S2's no-local-state property. The
   POC's S3 group commit acks at 4.5 ms p50 on local Silo.
4. **Unlimited streams is the deepest gap.** Precog keeps per-log state in RAM on every node,
   and the million-stream probe measures 3.9 KB per stream per node, all resident (3.8 GiB at 1M, 8.9 s restart), against s2-lite's 2.2 KB while hot and 44 MiB after a restart.
   Competing needs dormant streams off-heap (index on disk, state loaded on demand). That is a
   redesign of the store's indexes, not of the write path.
5. **Multi-tenancy (basins, scoped access tokens, quotas, metering) is a control-plane project.**
   It is the largest piece of work, and it is a product decision before it is an engineering one.
   It can coexist with the current deployment: one gateway tier in front of the same mesh.

6. **Unit economics on bare metal (asked for; `reports/S2 bare-metal economics.md`).** On
   Hetzner, a GiB written costs Precog about $0.003, a GiB read under $0.001 and a GiB-month
   stored about $0.003 (HDD boxes, erasure coded; ~$0.007 on object storage), against s2.dev's list of $0.05 each. The reasons are free internal
   traffic and cheap NVMe and HDD capacity. Priced at about half of s2.dev on bytes, the gross
   margin is 71–98% per line. Two things stand in the way. A stream costs more than s2.dev charges
   until dormant streams leave RAM (C7). And the fixed floor (a minimal fleet plus on-call staff)
   needs ~$50k/month of revenue. On AWS the same design pays ~$0.048/GiB of inter-AZ copies, so
   the edge exists only on bare metal, and only for customers who don't pay cloud egress to reach
   it.

What Precog keeps that S2 lacks: broker-side dedup, compacted streams, reader groups with stored
positions, many independent writers per stream without coordination, millisecond failover with no
election, local-disk latency, and no dependency on an object store.

## 1. What S2 is (short; sources in the report)

| | S2 (cloud) | s2-lite (self-hosted, MIT, Rust) |
|---|---|---|
| Model | account → basin → stream → record. Dense u64 `seq_num` and monotone ms `timestamp` assigned per stream. Headers and body. Command records `fence` and `trim` | same API |
| Ops | append (1–1000 records, ≤ 1 MiB, atomic), read (from `seq_num`, `timestamp` or `tail_offset`; `count`/`bytes`/`until`; `clamp`; `wait` ≤ 60 s), check-tail, `match_seq_num`, fencing token, trim | same |
| Consistency | linearizable per stream. Trim is eventually consistent | single node: trivially linearizable |
| Durability | ack after a chunk is on object storage. Standard: S3 Standard, ack ≤ 400 ms. Express: quorum of 3 S3 Express One Zone buckets, ≤ 40 ms. Both multi-AZ | SlateDB on a bucket, local disk, or memory |
| Surfaces | REST v1 (JSON/protobuf), S2S sessions over HTTP/2, SSE reads; SDKs: TS, Python, Go, Rust; CLI; Terraform. gRPC retired 2026-04 | REST, S2S, SSE. No access tokens, no metrics API |
| Tenancy | access tokens scoped by prefix/exact on basins, streams, tokens; 24 ops; auto-prefixing; 429 rate limits | none (the token is ignored) |
| Scale | unlimited streams (dormant streams are metadata plus segments); 100 MiB/s per stream | one node |
| Price | $0.05/GiB-month stored, $0.05 (Standard) or $0.075 (Express) per GiB written, $0.05–0.10/GiB read, $0.001 per 1000 stream ops, $0.01 per 1000 streams per month | free |
| Design | frontend → channel (multiplexes many streams into WAL chunks on S3) → per-stream segments; cachey read-through cache; cop (placement), bgtask; DST (Turmoil, Antithesis), Porcupine linearizability checks | axum; one streamer task per stream owns the tail, pipelines appends, broadcasts acked records |

## 2. Feature gap: S2 against Precog today

✅ have · 🟡 partial or different · ❌ missing

| # | S2 feature | Precog today | Gap |
|---|---|---|---|
| F1 | Basins, streams; create/list/delete/reconfigure; create on append/read; `delete_on_empty` | Streams are names in writer-log ids. Per-stream retention/compaction comes from static node config (`streams:`) | ❌ API and dynamic metadata |
| F2 | Dense, linearizable per-stream `seq_num` | Order per writer log `(stream, writer, epoch)`; cross-writer order only by event time (M5 plans cuts) | 🟡 POC: layered (§3.2) |
| F3 | `match_seq_num` conditional append | Batch identity is idempotent; a conflicting overlap is refused (a CAS per seq range, not on the tail) | 🟡 POC: in the sequencer |
| F4 | Fencing tokens (`fence` command record) | none | 🟡 POC: in the sequencer |
| F5 | Timestamps: arrival or client (`client-prefer`/`require`/`arrival`), monotone, capped; read from a timestamp | Event time per event (client supplied, not monotone); `cursor_at(time)` uses batch append time | 🟡 POC: monotone stamping; per-stream time index missing |
| F6 | Trim command; retention by age or infinite, per stream, reconfigurable | Retention by bytes/time per stream (static); `Store::trim_stream` exists | 🟡 trim record → `trim_stream`; dynamic config |
| F7 | Read API: start at seq/timestamp/tail offset, clamp, bounds, long-poll `wait`; check-tail | Cursor reads (per-writer frontier), start at beginning/end/time; `Client::end` reads 2 nodes | 🟡 POC: gateway; linearizable check-tail needs the owner |
| F8 | Append sessions (pipelined, ordered, poisoned on failure), read sessions (S2S, SSE) | Native protocol pipelines (8 in flight per producer) and pushes subscriptions | 🟡 POC: SSE; S2S not built |
| F9 | REST/JSON/protobuf surface; SDKs in 4 languages; CLI | Native TCP protocol; Rust and Go SDKs; CLI | ❌ POC: REST subset over Precog |
| F10 | Access tokens (scoped, auto-prefix, expiry, revocation), 429 quotas | One shared token; peer auth only | ❌ control plane |
| F11 | Durability on object storage; storage classes; cheap unlimited retention | Node-local disks, k of n nodes fsynced (or write caching) | ❌ POC: S3 group commit; tier not built |
| F12 | Unlimited streams; dormant streams cost no memory | Per-log maps in RAM on every node, rebuilt at start | ❌ measured (§4.4) |
| F13 | Usage metering per basin/stream, metrics API, dashboard | Prometheus `/metrics` per node, per reader group lag | 🟡 |
| F14 | Stream encryption (`aegis-256`, `aes-256-gcm`, client-held key) | none (TLS planned, M7) | ❌ |
| F15 | Elastic, multi-cell, placement by `cop`, stream migration between channels | Static membership; every node targets 3 copies; control plane planned (M4) | ❌ M4 |
| F16 | DST and linearizability checking (Turmoil, Antithesis, Porcupine) | Acceptance suite, proptests, mutants, fault injection; no DST | 🟡 |

Precog features S2 has no counterpart for: dedup by key within a window (R4), compacted streams
(R5), reader groups with committed positions (R3), cursors valid on every node, write caching
(`flush_delay_ms`), event-time merge across writers, any node accepting any append.

## 3. Per missing feature: what it needs and what it forces

### 3.1 The redesign choices, and whether each can coexist

| Choice | Forced by | Coexist with leaderless node-local? | Cost (eng-weeks, one senior) | Risk |
|---|---|---|---|---|
| **C1 Stream-level seq via per-stream owner over a writer log** | F2–F5, F7, F8 | **Yes, optional per stream.** S2 streams are writer logs with one writer (the owner), so the mesh is untouched; ordinary streams keep many writers | 4–6 (owner leases, failover, poison/re-epoch, check-tail) | Owner failover correctness (§3.2); one owner caps a stream's throughput |
| **C2 Control plane (Raft)**: namespaces, stream configs, owner leases, tokens, placement | F1, F6, F10, F12, F15, C1 leases | **Yes.** Already M4. Off the data path | 8–12 (shared with M4) | Metadata-store failures are where Jepsen finds bugs (Bufstream); keep it off the append path |
| **C3 S2-compatible gateway** (REST, SSE, then S2S) | F7–F9 | **Yes.** A stateless tier (stateful only for owned streams) in front of the mesh | 3 (REST+SSE), +3 (S2S, protobuf) | API drift from S2's (pin a version; run S2's SDK test suites against it) |
| **C4 Multi-tenancy**: basins, scoped tokens, quotas, metering | F10, F13 | **Yes.** At the gateway, with per-basin limits in the control plane | 6–8 | Security surface; noisy neighbours on shared disks |
| **C5 Object storage as a tier** (sealed segments to S3, read through a cache) | F11 (cost of retention) | **Yes, optional per stream** (retention beyond the local budget). The ack path stays local | 6–8 (upload, manifest, read path, GC); M8 overlaps | Read latency of cold data (100+ ms first byte); manifest consistency |
| **C6 Object storage as the durability (ack after PUT)** | F11 (no local state; S2's "standard") | **Optional per stream, but a second write path.** Coexists only if the owner's batches go to a WAL object rather than to peers | 6–10 | PUT cost forces cross-stream aggregation (§5); latency floor = PUT + flush; two durability models to test |
| **C7 Dormant streams off-heap** | F12 | **Replaces** the in-RAM per-log index (internal; the API is unchanged) | 6–10 | The read path and repair both walk per-log maps today; repair by per-log frontiers needs a scalable summary (Merkle or range) |
| **C8 Encryption at rest per stream** | F14 | Yes: batch payloads encrypted by the client or the gateway; the mesh sees opaque bytes already | 2–3 | Key handling; dedup and compaction cannot see encrypted keys |

### 3.2 F2–F5: a stream-level sequence on writer logs (POC built)

**Needs.** One owner per stream. It assigns `seq_num`s densely, stamps monotone timestamps,
checks `match_seq_num` against its tail (in-flight appends included, as S2 does in a session),
checks the fencing token, and records `fence`/`trim` command records in the stream itself so a new
owner recovers them. Batches go to the writer log `(basin/stream, "s2", epoch)` with
`first_seq = seq_num`, many in flight. Acks go out in seq order, only once every earlier batch is
durable.

**Built.** `precog_lab::s2::Sequencer` (unit tests: dense numbering, `match_seq_num`, fence takes
effect on the next append, fencing checked before `match_seq_num`, S2's limits, capped and
monotone timestamps, trim point, recovery from the log). The gateway (`crates/s2-lab`) wraps it in
a streamer task per stream, like s2-lite's.

**Owner failover, the risky part.** A zombie owner (paused, partitioned) may keep numbering.
Precog's conflict rule refuses any overlap with different bytes on every node. With
`copies` ≥ ⌊n/2⌋+1 of the stream's replica set, two different batches at one seq range can't both
be acknowledged: their quorums intersect on a node that took only one of them. The test shows this
on a real 3-node cluster (200/200 at 2-of-3, both win 132/200 at 1-of-3). What is still needed:

- **Takeover.** The new owner reads the tail from a majority (`Client::end` already merges 2 of
  3), replays from the last snapshot to recover the token and clock, and continues. A stricter
  variant bumps the epoch, with nodes refusing older epochs of an owned stream (a per-log epoch
  floor), so a zombie stops at its next batch instead of at a conflict.
- **Holes and minority copies.** A batch that failed or timed out after the owner numbered past it
  leaves a hole or a 1-copy loser. The POC poisons the stream (S2 also poisons a session on
  failure). A product re-reads the majority tail and re-sequences under a new epoch. Losers must
  be truncated before anti-entropy copies them; repair already treats conflicts as errors, so they
  would surface rather than spread.
- **Leases.** Ownership comes from the control plane (C2), held under a lease shorter than the
  RPC timeout. Without C2, consistent hashing of streams to nodes plus the conflict guard is
  enough for a single-region deployment.

**Optional?** Yes. A stream is an "S2 stream" (owned, dense, conditional) or an "ingest stream"
(many writers, leaderless). Both are writer logs in the same mesh.

**Ordering semantics.** S2: one total order per stream, real-time (linearizable). Precog ingest
streams: per-writer order plus event-time merge. Owned streams: S2's order, at the price of one
owner per stream (S2 has the same: one channel per stream), and with failover measured in lease
time, not in the 71 ms of the leaderless path.

### 3.3 F7–F9: the API surface (POC built: REST subset and SSE)

Needs: S2 REST v1 (records and stream endpoints first), SSE, then S2S (HTTP/2 framing with
protobuf) so the official SDKs' sessions work; JSON `raw` and `base64` formats. Precog's
`ReadCursor` serves catch-up reads; the owner's broadcast serves live tails (as s2-lite does).
**Recommendation:** implement S2's API exactly, so S2's own CLI and SDKs are the clients. That buys
four SDKs and a CLI for the cost of a gateway. The POC answers s2-lite's JSON shapes, and the same
driver runs against both.

### 3.4 F11: object storage (POC built: S3 group-commit WAL on Silo)

Three distinct things are called "object storage":

| Mode | Ack after | Latency floor | Coexists? | When |
|---|---|---|---|---|
| Tier (C5) | k local fsyncs (as today) | today's (~1–3 ms) | yes | long or infinite retention; cheap history |
| WAL to S3 (C6, S2 standard) | a PUT of a group commit to S3 Standard | PUT p50 (S3: tens of ms) + flush interval | optional, second path | no local state; nodes become cattle |
| WAL to 3 one-zone buckets (S2 express) | PUTs to a quorum of 3 S3 Express buckets in 3 AZs | ~5–10 ms + flush | as above | multi-AZ durability at single-digit ms |

The POC's `gw-s3` sink is C6: everything queued while a PUT slot is busy goes in one object. On
local Silo it acks at **4.5 ms p50** (2.25 ms idle). That is Silo's PUT on loopback, not S3's: on S3 Standard the
same design would sit at tens of ms, which is S2's 400 ms budget for Standard with a large margin.
The real constraint is the PUT bill (§5): an object WAL has to aggregate across all streams on a
node (S2's channels do exactly that), and the flush interval trades latency against dollars.

### 3.5 F12: unlimited streams (measured)

Every Precog node keeps, per writer log, a `BTreeMap` of batches, contiguous frontiers, and the
stream's LSN order, and rebuilds them at start from index files. S2 keeps a dormant stream as
metadata plus segments, and s2-lite keeps it as SlateDB keys. The probe creates N streams with
one record each. At 1M streams a Precog node holds 3.8 GiB (3.9 KB per stream) and takes 8.9 s to restart,
rebuilding every stream. s2-lite holds 2.1 GiB while they are hot, but only **44 MiB after a
restart** (1.3 s), because it loads streams lazily. The POC gateway adds ~8.8 KB per stream in its
streamer tasks (`results/s2.md`). Needs: per-log index on disk (the plan's risk table already names
redb or index files), an LRU of hot logs, and repair that compares stream summaries instead of
every log's frontier. Cost 6–10 weeks. It replaces the internal index; the API is unchanged.

### 3.6 F10, F13: tenancy and metering

Needs: basins as namespaces owned by an account; tokens with prefix/exact scopes over basins,
streams and tokens, op groups, expiry, revocation, auto-prefix; per-basin rate limits (429) and
storage quotas; metering of bytes written, stored and read and ops per basin and stream. Placement:
token checks and rate limits at the gateway; definitions in the control plane; meters as per-node
counters aggregated by the control plane. It coexists: one internal tenant is today's deployment.

### 3.7 F14–F16

Encryption: the gateway encrypts record bodies with a client-held key, and the mesh already stores
opaque batches (dedup and compaction must then be off for that stream). Elasticity: M4's control
plane plus owner leases give stream migration (move a lease); data placement beyond 3 nodes is M4.
Testing: add deterministic simulation for the owner/lease protocol (S2's `mad-turmoil` is MIT) and
a Porcupine-style linearizability check of owned streams against the gateway.

## 4. Tradeoffs: what each design gives up

| Dimension | S2 (object-storage durability) | Precog (local-disk quorum) |
|---|---|---|
| Write latency | Express ≤ 40 ms, Standard ≤ 400 ms (SLO); s2-lite on Silo 51 ms p50 at its default 50 ms flush, 6.2 ms at 5 ms (10k records/s) | 1.98 ms p50, 5.0 ms p99 for 2-of-3 through the same gateway and client (10k records/s) |
| Durability domain | multi-AZ object storage (11 nines) | k nodes in distinct failure domains; losing k disks loses data |
| Cost per GB stored | S2 list price $0.05/GiB-month (S3 raw ~$0.023) | AWS: 3 × gp3 ≈ $0.24/GB-month. Hetzner: ~$0.003/GiB-month on SX65 HDD boxes, erasure coded 3+2; ~$0.007 on Hetzner Object Storage, iDrive e2 or B2 (`reports/S2 cold storage.md`) |
| Cost per GB written | $0.05–0.075/GiB list; S3 PUTs amortized across streams | AWS: ~$0.048/GiB, almost all inter-AZ copies. Hetzner: ~$0.003/GiB (internal traffic free) |
| Request cost | PUT-bound: 200 PUT/s per writer node ≈ $2,600/month on S3 Standard at $0.005/1000 | none |
| Stream count | unlimited; dormant streams cost metadata only | bounded by RAM per node (3.8 GiB per million streams per node, all resident) |
| Ordering | dense per-stream order, linearizable; conditional appends | per-writer order; any number of writers per stream with no coordination; optional owned streams (C1) |
| Availability on a node loss | stream moves to another channel (speculative migration) | no election: clients retry on another node (≤ 71 ms measured) |
| Operations | stateless compute; state in S3 | stateful nodes; repair after rejoin; disks to watch |
| Hot-path dependencies | object store (and metadata for inactive streams) | none outside the cluster |

Precog's design wins where the workload is ingest from many sources, latency matters, data is
kept for hours to days, and the operator owns the hardware (live-ingestion). S2's wins where
streams are many, small and long-lived, history is long, and the operator wants no stateful
fleet. The tier (C5) and owned streams (C1) move Precog toward S2's strengths without giving up
its own. C6 and C7 are the expensive moves, and only a product decision to sell a hosted
service justifies them.

## 5. Order of work

Precondition: live-ingestion's cutover (PLAN.md §7) finishes first. None of this blocks it.

| Phase | Work | Choices | Weeks | Exit criteria |
|---|---|---|---|---|
| S0 | Decide whether to compete as a hosted service or as a self-hosted S2-compatible broker | — | — | owner decision |
| S1 | Owned streams in the product: owner task in the node, conditional appends, fencing, trim → `trim_stream`, timestamps; poison and re-epoch; `copies` ≥ majority enforced for owned streams | C1 | 4–6 | linearizability check of owned streams under SIGKILL/SIGSTOP of owners; S2 semantics acceptance scenarios |
| S2 | S2 REST + SSE gateway; the official CLI and SDK test suites pass against it | C3 | 3 | `s2 bench` and S2's SDK examples run unchanged against Precog |
| S3 | Control plane: namespaces, stream configs, owner leases, placement (M4) | C2 | 8–12 | add/remove node without downtime; lease failover ≤ 2 s |
| S4 | Object-storage tier for sealed segments, read through a cache | C5 | 6–8 | infinite retention at ≤ 40% of 3-copy disk cost; cold read p99 ≤ 300 ms |
| S5 | Dormant streams off-heap | C7 | 6–10 | 10M streams per node within 2 GiB RAM; start time independent of stream count |
| S6 | Tenancy: tokens, quotas, metering; S2S sessions | C4, C3 | 9–11 | S2's access-token semantics; per-basin 429s; usage export |
| S7 | Optional: object-storage durability class (ack after PUT, 1- or 3-bucket) | C6 | 6–10 | ack p99 ≤ 400 ms (standard) / ≤ 40 ms (express-like) on AWS |

Roughly 9–12 engineer-months to S6. S1–S2 alone (about 2 months) gives a self-hostable,
S2-compatible broker that beats s2-lite on latency, which is the cheapest credible offer.

## 6. Risks

| Risk | Mitigation |
|---|---|
| Owner failover bugs (lost or duplicated seq_nums) | Majority `copies` + conflict rule (tested); epoch floor per owned log; DST of the lease protocol; Porcupine checks |
| A minority loser copy spread by repair | Truncate unacknowledged conflicting tails at takeover; repair refuses conflicts (it does) |
| S2 API churn (gRPC was retired 2026-04) | Pin a version; run S2's open SDK tests in CI |
| Per-stream owner throughput ceiling | S2 caps at 100 MiB/s per stream too; owner batches many appends per mesh batch |
| Object-store request bills | Aggregate across streams per node; flush on size or interval; tier sealed segments only |
| Scope creep away from live-ingestion | Phase S0 decision; S1–S2 only until the cutover is done |
