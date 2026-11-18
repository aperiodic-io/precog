# Precog against the Redpanda dedup fork (2026-11-18)

`bench/compare/run.sh`, one 4-vCPU VM. Each system: 3 brokers in containers capped at 1 CPU and
4000Mi (live-ingestion's raw brokers), 3 copies acknowledged at 2, a 5-minute dedup window keyed
by a per-event id, 20% resends. Each load generator runs in its own 1-CPU container with
live-ingestion's workload (8 producers x 3 symbols, ~230-byte JSON trades), 2,000 records in
flight overall, payloads compressed by the producer (franz-go: snappy by default; Precog: lz4).

- Redpanda: `ghcr.io/dream-faster/redpanda:71f80ccdf79917925ec1e3fb4e958017480a65fc` (the image
  live-ingestion pins), live-ingestion's franz-go loadgen (10 ms linger, one request in flight per
  broker), topic as `internal/pipeline/topics.go` declares a raw topic (3 partitions,
  `retention.bytes=1252698794`, `redpanda.dedup.window.ms=300000`).
- Precog: `crates/client/examples/loadgen.rs`, 250-record batches (all 2,000 records in flight,
  as franz-go's batches fill the same budget), node retention 600 MB so the measured window is
  at steady state (segments recycled, as on a raw broker that has reached its retention).
- Broker CPU per record: the three containers' cgroup CPU time over the 30 s measured window,
  divided by the records acknowledged in it.

## Results (medians of 2-3 runs)

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| Precog | 623,000 | 3.0 ms | 7.1 ms | 1.38 us |
| Redpanda, as live-ingestion runs it (`write_caching_default=true`: acks before fsync) | 147,000 | 11.1 ms | 36.8 ms | 3.42 us |
| Redpanda, fsync before every ack (Precog's guarantee) | 140,000 | 12.3 ms | 34.9 ms | 4.46 us |

| Precog vs Redpanda | throughput | p50 | p99 | CPU per record |
|---|---:|---:|---:|---:|
| at equal durability | 4.5x | 4.1x | 4.9x | 3.2x |
| against write caching (weaker durability) | 4.2x | 3.7x | 5.2x | 2.5x |

Precog at a fixed 100k records/s (the latency target's shape; franz-go's loadgen cannot pace),
60 s across a segment roll: p50 0.8 ms, p99 10 ms.

Dedup: both drop exactly the resent share (Redpanda 19.99%, Precog its `dedup_dropped_total`).

## How Precog got here (same harness, 250-record batches unless noted)

| change | CPU/record |
|---|---:|
| first run (50-record batches) | 4.23 us |
| dedup index on SwissTables, per stream, unindexed on replicas (was 22% of a replica) | 2.45 -> 1.63 us (1000-record batches) |
| repair compares per-log frontiers instead of every batch id (was 17%) | 3.67 -> 3.44 us (50-record batches) |
| one map per stream instead of generations; one probe per lookup | 2.19 -> 2.06 us |
| payloads compressed in one lz4 block, keys outside it | 2.12 -> 1.8 us |
| steady state (retention reached: no zero-fill of new segments) | 1.38 us |
| pace the background zero-fill (fixed 100k/s across a roll) | p99 456 -> 10 ms |

## Caveats

- The VM is noisy: identical runs differ by up to ~20%; the tables give medians, the runs are in
  the PRs' history.
- Overlayfs container filesystems on one virtual disk, not the raw brokers' NVMe; fsync costs
  differ in production.
- At write caching, Redpanda's CPU per record is measured at ~0.5 of 3 cores busy; Precog's at
  ~0.9. Precog's remaining costs: scheduling (thread switches and wake-ups, ~25%), fsync (~17%
  of a replica), the dedup map on the accepting node (~13%), copies.
- Not measured here: readers' load (archiver, brrrrr) on the brokers.
