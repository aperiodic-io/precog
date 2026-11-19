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

## Write caching on both sides, at steady state (2026-11-18, later)

live-ingestion runs Redpanda with `write_caching_default: true`: it acks once the batch is on a majority, and the fsync
follows on a 100 ms / 256 KiB cadence. Precog now has the same mode (`flush_delay_ms: 100`, `flush_bytes: 256 KiB`):
a batch is acked once written on 2 of 3 nodes, the fsync follows within the delay, and a failed fsync still fences the
node. This makes the comparison like-for-like with what production runs. Same harness, `WARMUP=20` so that both
systems are measured after Precog's log reaches its retention (with `WARMUP=5`, the window still included the
log's first 600 MB, with segments zero-filled as it grew). Medians of 3 runs, alternating Precog and Redpanda:

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| Precog, write caching (`FLUSH_MS=100`) | 650,700 | 2.85 ms | 6.2 ms | 1.12 us |
| Redpanda, write caching (as live-ingestion runs it) | 135,000 | 11.1 ms | 45.6 ms | 3.39 us |
| **Precog vs Redpanda** | **4.8x** | **3.9x** | **7.4x** | **3.0x** |

Runs: Precog 655k/2.83/6.2/1.123, 651k/2.85/6.36/1.149, 644k/2.93/5.92/1.107; Redpanda 125k/13.3/27.6/3.234,
147k/11.0/45.6/3.39, 135k/11.1/158.6/3.612. CPU is at 3.0x with no margin, and identical runs on this VM vary by up to
~20%.

Equal durability at steady state (2026-11-19): fsync before every ack on both sides (Precog `FLUSH_MS=0`, Redpanda
`write_caching_default=false`), same harness and `WARMUP=20`, alternating, medians of 3 Precog and 5 Redpanda runs:

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| Precog, fsync before every ack | 696,600 | 2.71 ms | 5.49 ms | 1.39 us |
| Redpanda, fsync before every ack | 159,000 | 11.5 ms | 21.3 ms | 3.26 us |
| **Precog vs Redpanda** | **4.4x** | **4.3x** | **3.9x** | **2.3x** |

Runs: Precog 703k/2.67/5.49/1.337, 697k/2.71/5.48/1.386, 647k/2.93/6.05/1.551; Redpanda 150k/11.9/16.9/3.036,
160k/11.5/19.2/3.576, 159k/11.5/25.1/3.789, 165k/11.3/21.3/3.256, 128k/13.3/23.6/2.713. CPU per record is under 3x
here: Redpanda's fsync-per-ack cost is lower than the earlier non-steady-state run showed (4.46 us), and Precog's group
commit does an fsync per batch where write caching batches them (1.39 against 1.12 us).

A recycling log no longer zero-fills spares: with the log at its retention, a roll used to wake the preallocator, which
wrote a whole segment of zeros (11% of a node's CPU at 100 MB/s) that the next retention pass then deleted, having
recycled a segment anyway. The preallocator now waits up to 5 s for that recycle.

## Lowering CPU per record further: what was tried (2026-11-19)

Goal: <= ~1.0 us/record in both modes (3x Redpanda's 3.26 / 3.39 with margin). Not reached; nothing measurably
moved the end-to-end number on this VM. Same harness (`WARMUP=20 BATCH=250`, 20% resends, 2,000 in flight).

Runs at `FLUSH_MS=0`, base interleaved with each candidate (records/s, p50 ms, p99 ms, us/record):

- base: 714.6k/2.64/5.35/1.274, 692.6k/2.72/5.46/1.343, 685.6k/2.74/5.83/1.301, 672.6k/2.81/5.73/1.428,
  711.2k/2.65/5.48/1.324, 720.2k/2.63/5.08/1.266, 737.3k/2.56/5.25/1.216, 749.3k/2.54/4.98/1.173,
  763.5k/2.47/4.79/1.126, 769.5k/2.45/4.76/1.113, 770.9k/2.48/4.74/1.218. Median 1.266.
- tail cache sharing the appended `Bytes` + dedup pending as a deque: 1.225, 1.328, 1.303, 1.398, 1.484, 1.311,
  1.139, 1.328 (median 1.316); the replica's user time in `Log::append` fell from 10.1% to 5.5% of its samples.
- writers to peers coalescing the acks of one group commit (requeue behind the runnable tasks): 1.196, 1.169, 1.244,
  1.121, 1.318, 1.196 (median 1.196); all connections: 1.296, 1.269; `yield_now` in every writer: 1.313, 1.199,
  1.249 at 612-640k records/s (throughput -10%).
- glibc huge pages for the dedup table (`GLIBC_TUNABLES=glibc.malloc.hugetlb=1`): 1.415, 1.367 against 1.38, 1.551.

The same binary ranges 1.11-1.50 us over a session, which is wider than any of these effects. Only the deque
(pending replicated dedup ids: dropping the oldest quarter at the cap memmoved ~6M entries, 2.1% of a replica's
samples) is committed. The rest was dropped: none measured above noise, and the tail-cache change adds a dependency
and lets cached batches pin their network read buffers.

Where the CPU goes (FLUSH_MS=0, perf with DWARF stacks, share of each node's samples):

- accepting node: socket sends 14.6%, dedup lookups 13.3%, fsync 13.1%, tokio and other user code 11.7%,
  `epoll_wait`/park 7.5%, recv 5.8%, rewriting batches without their duplicates 5.3%, WAL user code 5.3%,
  pwrite 4.8%, index 4.7%, malloc/free 3.5%.
- replica: fsync 20.8%, WAL user code 10.1% (5.5% with the tail-cache change), pwrite 9.5%, sends 9.3%,
  `epoll_wait` 8.0%, index 8.0%, other user code 7.7%, dedup recording 6.7%, recv 5.5%, WAL wake-ups 5.6%.
- At `FLUSH_MS=100`, the WAL handoff (worker -> WAL thread futex, the WAL thread's sleep, its eventfd wake of the
  worker) is ~10% of every node: the WAL thread wakes for nearly every batch.
- About a fifth of every node's samples land in context switches and wake-ups (`finish_task_switch`, IPIs): the
  brokers run at 25-50% of a core because the load generator is CPU-bound (100% of its 1-CPU container), so each node
  sleeps and wakes per event.

What could reach the target, not attempted: appends written on the worker thread under write caching, with only
the deferred fsync on another thread (~10% at `FLUSH_MS=100`; it touches the durability path and segment rolls
would block the worker); coalescing sends at some latency cost; a prefetching dedup table (results/dedup.md found
little over SwissTable).

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
- `run.sh` removed the Redpanda containers without their anonymous volumes (fixed: `docker rm -v`). Earlier runs left
  them behind and filled the disk.
