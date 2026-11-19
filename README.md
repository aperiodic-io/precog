# Precog

A leaderless, durable event broker built to replace `redpanda-raw-internal`, the raw-data Redpanda
cluster in live-ingestion. Each source owns a writer log and numbers its own events. Any node accepts
any append, and an append is acknowledged once 2 of 3 nodes hold it. No consensus runs per message,
and no partition has a leader to fail over.

On the same machine, the same workload and the same durability, Precog beats the Redpanda build
live-ingestion runs by **4.4–4.8x on throughput** and **3.9–7.4x on ack latency**, using **2.3–3.0x
less broker CPU per record** ([results](#results-against-redpanda)).

Design and guarantees: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Plan of record:
[docs/PLAN.md](docs/PLAN.md).

## What it does

Only what live-ingestion uses is built.

| Feature | What it replaces in Redpanda |
|---|---|
| **Streams**, named like the raw topics, each with retention by bytes and by age | topics with `retention.bytes` / `retention.ms` |
| **Compacted streams**: the newest value of every key survives retention and restarts | `cleanup.policy=compact` (the collectors' `<name>.state` topics) |
| **Broker-side dedup** by each event's dedup id, kept for a window (5 minutes on the raw streams) | the dedup fork's `redpanda.dedup.window.ms` keyed by `redpanda-dedup-key` |
| **Durable acks on 2 of 3 nodes**, fsync before the ack by default | `acks=all` with RF=3 |
| **Write caching** (`flush_delay_ms`, `flush_bytes`): ack once written on 2 nodes, fsync within 100 ms / 256 KiB; a failed fsync still fences the node | `write_caching_default=true`, as live-ingestion runs it |
| **Anti-entropy repair**: every node converges to the union of all copies, so a restarted node catches up by itself | partition replication and recovery |
| **Cursors valid on every node** and **reader groups** with committed positions: consumers fail over between nodes and resume where they committed | consumer groups and committed offsets |
| **A stream's end on a quorum** (`Client::end`), for readers that must prove they have read everything acked | high watermark |
| **Lag and health**: `/metrics` (`precog_group_lag_events`, `precog_group_commit_age_seconds`) and `/health` | Redpanda's metrics endpoint |
| **Kubernetes deployment**: StatefulSet config from DNS peers, the node id from the pod's ordinal, image `ghcr.io/aperiodic-io/precog` | the Redpanda StatefulSet |

Deliberately not built: users, ACLs and TLS (one shared in-cluster token), a Kafka-compatible API,
transactions, and membership changes beyond the 3 configured nodes.

## Clients

- **Rust SDK, `precog-client`** (`crates/client`):
  - `Producer` batches the events of one writer log, each with a dedup id. It keeps up to 8 batches in flight and retries a failed append with the same bytes on the next node, which is idempotent.
  - `Consumer` reads from a cursor, starting at a committed position, the beginning, the end or a time, and commits.
  - `Client::latest` returns a compacted stream's newest value of every key.
- **Go client** (`clients/go`): the same protocol, used by live-producers' Go collectors and derived-seed.

The users in live-ingestion:
- the producers and collectors (aperiodic-io/live-producers);
- brrrrr's `--precog` source (aperiodic-io/brrrrr);
- the Rust archivers;
- maritime-analytics.

## Results against Redpanda

[`bench/compare/run.sh`](bench/compare/run.sh) on one 4-vCPU VM. Each system runs 3 brokers in containers capped at 1 CPU and
4000Mi, the size of live-ingestion's raw brokers. Each batch is kept on 3 nodes and acknowledged at 2, with a 5-minute dedup
window and 20% resends. The workload is live-ingestion's own: 8 producers × 3 symbols of ~230-byte JSON trades, 2,000 records
in flight, each load generator in its own 1-CPU container. Redpanda is the dedup fork image live-ingestion pins, driven by
live-ingestion's franz-go load generator. All numbers are medians of 3+ runs alternating the two systems. Full tables, per-run
numbers and caveats are in [results/compare.md](results/compare.md).

| Durability | | records/s | p50 ack | p99 ack | broker CPU / record |
|---|---|---:|---:|---:|---:|
| **Write caching** (as live-ingestion runs Redpanda) | Precog | 650,700 | 2.85 ms | 6.2 ms | 1.12 µs |
| | Redpanda | 135,000 | 11.1 ms | 45.6 ms | 3.39 µs |
| | **ratio** | **4.8x** | **3.9x** | **7.4x** | **3.0x** |
| **fsync before every ack** | Precog | 696,600 | 2.71 ms | 5.49 ms | 1.39 µs |
| | Redpanda | 159,000 | 11.5 ms | 21.3 ms | 3.26 µs |
| | **ratio** | **4.4x** | **4.3x** | **3.9x** | **2.3x** |

At these caps the load generator saturates before the brokers, which run at 25–50% of a core.

With every broker capped at 0.5 CPU, so the brokers are the bottleneck:
- Precog does 3.1x the throughput and 8x lower p99.
- It uses 2.4–2.6x less CPU per record.

Dedup drops exactly the resent share on both systems (Redpanda 19.99%).

Where Precog's CPU goes and what was tried to lower it is also in `results/compare.md`. CPU per record is the one figure under 3x:
- it is 3.0x with write caching, with no margin;
- it is 2.3x with an fsync before every ack.

## The wire protocol

The protocol was chosen by measurement. [results/protocol.md](results/protocol.md) compares more than a dozen append-path proofs of concept
(`crates/lab`), each changing one idea:
- pipelining;
- batching on backlog, with linger;
- cumulative acks;
- interned stream and writer ids;
- fixed record columns with a dedup-hash column;
- varint and dictionary columns;
- client-side lz4;
- zero-copy buffers;
- standby shadows.

The research behind them is in [reports/Native protocol design space.md](reports/Native%20protocol%20design%20space.md).

The kept ideas compose:
- pipelining (8 batches in flight);
- backlog frames with linger;
- cumulative acks;
- fixed 14-byte record columns with the dedup hash outside the compressed payload;
- per-batch lz4 that the broker never decompresses.

Varint and dictionary columns and the zero-copy buffer management bought too little and were dropped.

## Layout

| Path | What |
|---|---|
| `crates/storage` | `precog-storage`: segment log, group commit across all writers, recycled preallocated segments, CRC-32C framing with generations, index files, manifest, recovery, retention, tail cache |
| `crates/mesh` | `precog-mesh`: node, replication, repair, dedup, RPC, TCP transport, the node binary (`precog-node`), acceptance tests |
| `crates/client` | `precog-client`, the Rust SDK, and the `loadgen` example used by the benchmarks |
| `clients/go` | Go client |
| `crates/lab` | protocol and dedup proofs of concept (evidence, not part of the product) |
| `crates/bench`, `crates/storage-bench` | the earlier Apache Iggy comparison and the storage-backend comparison (evidence) |
| `bench/compare`, `bench/redpanda`, `bench/perf-gate` | the Redpanda comparison, its baseline, and CI's wall-clock gate |
| `docs/`, `reports/`, `results/` | plan, architecture, research reports, raw benchmark results |

## Build, test, run

```bash
cargo build --release
```

```bash
cargo nextest run --workspace --exclude storage-bench
```

```bash
cargo test -p precog-mesh --test acceptance
```

CI runs all of these, plus the following ([AGENTS.md](AGENTS.md) has the commands):
- coverage gate;
- mutation testing of every PR's diff;
- instruction-count bench gate;
- wall-clock perf gate;
- the Go client's tests.

A local 3-node cluster:

```bash
target/release/precog-node init --nodes 3 --dir run/demo
```

```bash
for i in 0 1 2; do target/release/precog-node serve --config run/demo/node-$i.json & done
```

```bash
target/release/precog-node status --cluster run/demo/cluster.json
```

The Redpanda comparison needs Docker, the release binaries and live-ingestion's franz-go load generator (`benchmarks/redpanda-dedup/loadgen`
there). Precog first, then Redpanda at equal durability:

```bash
RETENTION=600000000 WARMUP=20 BATCH=250 FLUSH_MS=100 bench/compare/run.sh precog-wc precog 0.2 2000
```

```bash
RP_WRITE_CACHING=true WARMUP=20 FRANZ_LOADGEN=/path/to/loadgen bench/compare/run.sh redpanda-wc redpanda 0.2 2000
```

## Status

- Merged to `main`; the image workflow publishes `ghcr.io/aperiodic-io/precog:sha-<commit>` from every push.
- live-ingestion deploys Precog on staging beside `redpanda-raw-internal`, pinned to a published commit. From there it moves the
  producers to dual-write and then switches every raw-topic client with one setting (`rawBroker: precog`).
- The rollout steps and rollbacks are in live-ingestion's `docs/operations.md`, section "Precog".

## History

Precog started as a proof of concept against Apache Iggy 0.10. Durable on 2 of 3 nodes, it measured 2.1–3.3x Iggy's throughput
and failed over in tens of milliseconds instead of seconds. Those numbers and their harness are kept in
[results/RESULTS.md](results/RESULTS.md) and `crates/bench`.
