# S2 and Precog: what S2 is, where Precog stands, what the POCs showed

Short report behind [`docs/S2-PLAN.md`](../docs/S2-PLAN.md). Sources were gathered on 2026-11-19
from s2.dev's docs (the `.md` pages listed in [llms.txt](https://s2.dev/docs/llms.txt)), its
pricing page and blog, and the open repository
[s2-streamstore/s2](https://github.com/s2-streamstore/s2) (v0.47.0). Measurements are in
[`results/s2.md`](../results/s2.md).

## S2 in one page

- **Model.** An account owns basins, a basin owns streams, and a stream holds records. A record
  has a u64 `seq_num` and a ms `timestamp`, both assigned per stream, plus byte headers and a
  byte body. A record whose single header has an empty name is a command: `fence` sets a token of
  up to 36 bytes, and `trim` carries an 8-byte big-endian seq_num
  ([records](https://s2.dev/docs/concepts/records.md),
  [command records](https://s2.dev/docs/concepts/command-records.md)).
- **Appends.** One append is 1–1000 records and at most 1 MiB of metered bytes (8 + 2 per header +
  bytes), and it is atomic. It is acked only once durable. `match_seq_num` and `fencing_token` fail
  with 412 `seq_num_mismatch` / `fencing_token_mismatch`. Fencing is cooperative: an append without
  a token passes ([append](https://s2.dev/docs/api/records/append.md),
  [concurrency control](https://s2.dev/docs/concepts/concurrency-control.md)).
- **Reads.** Start at `seq_num`, `timestamp` or `tail_offset`. Optionally `clamp`; bounds
  `count`/`bytes`/`until`; long-poll `wait` up to 60 s. Check-tail. Stream operations are
  linearizable, trim is eventually consistent ([read](https://s2.dev/docs/api/records/read.md),
  [reads](https://s2.dev/docs/concepts/reads.md)).
- **Timestamps.** Modes are `client-prefer` (default), `client-require` and `arrival`. Timestamps
  are monotone, and capped at arrival unless `uncapped`
  ([configs](https://s2.dev/docs/concepts/configs.md)).
- **Config.** Retention by age (default 7 days) or infinite; `delete_on_empty`; create on append or
  read; per-basin stream cipher. Storage classes: **Standard acks within 400 ms** (S3 Standard),
  **Express within 40 ms** (a quorum of three S3 Express One Zone buckets). Both are multi-AZ before
  the ack ([architecture](https://s2.dev/docs/platform/architecture.md),
  [appends](https://s2.dev/docs/concepts/appends.md)).
- **Tenancy.** Access tokens are scoped by prefix or exact match on basins, streams and tokens,
  with op groups or 24 named ops, expiry, and auto-prefixing. A token can issue only narrower ones
  ([access tokens](https://s2.dev/docs/concepts/access-tokens.md)).
- **Surfaces.** REST v1 (JSON or protobuf), S2S sessions over HTTP/2 (a 3-byte length, a flag
  byte, protobuf), SSE reads (`batch` events with `id: seq,count,bytes`, `ping`, `error`). SDKs
  for TS, Python, Go and Rust; the `s2` CLI. gRPC was retired on 2026-04-29
  ([protocol](https://s2.dev/docs/api/protocol.md), [updates](https://s2.dev/docs/updates.md)).
- **Price.** $0.05/GiB-month stored; $0.05 (Standard) or $0.075 (Express) per GiB written;
  $0.05–0.10/GiB read; $0.001 per 1000 stream ops; $0.01 per 1000 streams per month. Streams are
  unlimited; appends are limited to 100 MiB/s per stream ([pricing](https://s2.dev/pricing),
  [limits](https://s2.dev/docs/platform/limits.md)).
- **Architecture.** Frontends route an append to the stream's *channel*. A channel multiplexes
  many streams into WAL chunks on object storage and acks once the chunk is durable; records are
  later rewritten into per-stream segments. Reads go through `cachey`, a per-zone read-through
  cache. `cop` places and migrates streams. Dormant streams exist only as metadata plus segments,
  which is what makes streams unlimited. Correctness is tested with deterministic simulation
  (Turmoil plus MadSim-style overrides, and Antithesis) and Porcupine linearizability checks
  ([architecture](https://s2.dev/docs/platform/architecture.md), [DST](https://s2.dev/blog/dst),
  [linearizability](https://s2.dev/blog/linearizability)).
- **s2-lite.** Single-node, MIT, Rust. Shipped as `s2 lite` in the CLI and in the image
  `ghcr.io/s2-streamstore/s2`. Storage is SlateDB on a bucket (`--bucket`), local disk
  (`--local-root`) or memory. One streamer task per stream owns the tail and pipelines appends.
  The SlateDB flush interval defaults to 50 ms on S3 and 5 ms otherwise (`SL8_FLUSH_INTERVAL`). No
  access tokens or metrics API ([README](https://github.com/s2-streamstore/s2#s2-lite),
  [s2-lite docs](https://s2.dev/docs/s2-lite.md)).

## Where Precog stands

Precog already has the hard parts of a durable log: a group-commit segment log with self-validating
records, k-of-n durable acks with no leader, repair, retention, compaction, dedup, cursors valid on
every node, and reader groups. What it lacks is everything that makes S2 a *product*:
- the stream-level contract (dense `seq_num`, conditional appends, fencing);
- the API surface and SDKs;
- tenancy;
- object-storage economics;
- a stream count bounded by disk rather than RAM.

The plan's gap table (§2) lists them feature by feature.

## What the POCs showed

1. **S2's contract layers onto writer logs.** A per-stream sequencer
   (`crates/lab/src/s2/sequencer.rs`, unit-tested) assigns seq_nums that are also the writer log's
   sequence numbers. The mesh's existing rule, that any overlap with other bytes is a conflict, is
   enough to fence a zombie owner when acks need a majority: 200 of 200 races had one winner at 2
   of 3, and at 1 of 3 both owners were acked in 132 of 200 (`crates/lab/tests/s2_zombie.rs`).
2. **An S2-compatible gateway is cheap.** About 600 lines (`crates/s2-lab/src/bin/gateway.rs`)
   serve S2's append, read (unary and SSE) and check-tail endpoints over Precog. One driver runs
   the same JSON workload against s2-lite and the gateway, and the results table compares them.
3. **Latency follows durability placement.** At 10k records/s through the same HTTP layer:
   - 2-of-3 local fsync acks at 1.98 ms p50 / 5.0 ms p99.
   - s2-lite on S3-compatible Silo acks at 51 ms after SlateDB's default 50 ms flush, or 6.2 ms
     with a 5 ms flush.
   - Closed loop, Precog carried 229k records/s at 6.9 µs of CPU per record, against s2-lite's
     31k–99k at 10–12 µs.
   - An object-storage group commit in the gateway acks at 4.5 ms on loopback Silo; on real S3 it
     would be tens of milliseconds.
4. **Stream count is RAM-bound in Precog.** At 1M streams a Precog node holds 3.8 GiB with every
   stream resident and restarts in 8.9 s. s2-lite holds 44 MiB after a restart, because it loads
   streams lazily.
5. **Unit economics** ([`S2 bare-metal economics.md`](S2%20bare-metal%20economics.md)). On
   Hetzner a GiB costs Precog about $0.003 written, under $0.001 read and $0.007 stored-month,
   against s2.dev's $0.05 each. The edge needs free internal traffic, which AWS does not offer:
   there, inter-AZ copies alone cost ~$0.048/GiB written.

## Recommendation

Build owned streams and the S2 REST/SSE gateway first (about two months). Precog then becomes a
self-hostable, S2-compatible broker that keeps its latency lead and its ingest features. Tier
sealed segments to object storage for retention. Take on the control plane, tenancy, off-heap
dormant streams and an object-storage durability class only after deciding to sell a hosted
service.
