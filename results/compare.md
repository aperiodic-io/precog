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
| **Precog vs Redpanda** | **4.4x** | **4.2x** | **3.9x** | **2.3x** |

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

### Appends written on the worker under write caching (2026-11-19, later)

Under write caching (`flush_delay_ms > 0`) the network worker now writes its batch into the segment under the log's
lock and has its LSN at once; the WAL thread only runs the deferred fsync (taken out of the log with
`begin_flush`/`end_flush`, so writers go on during it), retention, and is woken only when the flush deadline moves (the
first unsynced write, or `flush_bytes` crossed), not per batch. Rolls happen on the worker under the lock (spares are
recycled, so a roll is renames, a header and the manifest). `flush_delay_ms = 0` keeps the queue to the WAL thread
and its group commit.

The disk had ~3 GB free this session (the 600 MB retention filled it and fenced the run), so these runs use
`SEGMENT=128 MiB RETENTION=256000000` (a scratch copy of run.sh with the segment size as a variable), otherwise
`WARMUP=20 BATCH=250`, 20% resends, 2,000 in flight. That rolls each node every ~1.5 s (4x as often as 256 MiB
segments), and absolute numbers are higher than the earlier sections' (this session's VM also ran the same binary at
~430k records/s where the earlier ones saw ~650k); compare A and B only. Base (A) is d89bfc5, alternating with the
candidate (B), 5 runs each (records/s, p50 ms, p99 ms, us/record):

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| A, `FLUSH_MS=100` | 426,400 | 4.21 ms | 10.30 ms | 1.372 us |
| B, `FLUSH_MS=100` | 446,500 | 4.04 ms | 9.33 ms | 1.278 us (-6.9%) |
| A, `FLUSH_MS=0` | 444,000 | 4.21 ms | 9.15 ms | 1.756 us |
| B, `FLUSH_MS=0` | 443,800 | 4.19 ms | 9.22 ms | 1.758 us |

- A, 100: 428.2k/4.21/10.50/1.421, 423.6k/4.21/9.30/1.371, 441.4k/4.18/9.48/1.397, 412.7k/4.35/10.99/1.372,
  426.4k/4.24/10.30/1.342
- B, 100: 446.5k/3.96/9.33/1.278, 438.0k/4.09/9.66/1.269, 452.3k/4.04/9.05/1.305, 426.4k/4.12/10.27/1.222,
  465.4k/3.93/8.35/1.309
- A, 0: 389.4k/4.63/11.84/1.713, 444.0k/4.21/9.15/1.805, 457.4k/4.14/8.69/1.742, 441.6k/4.22/9.44/1.785,
  449.6k/4.15/9.10/1.756
- B, 0: 370.8k/4.73/15.03/1.683, 436.3k/4.22/9.64/1.812, 443.8k/4.19/8.91/1.761, 455.4k/4.12/8.94/1.758,
  449.3k/4.15/9.22/1.755

At `FLUSH_MS=100` every candidate run is below every base run; throughput is +4.7% and p99 lower, so the
rolls on the worker did not show up as stalls. At `FLUSH_MS=0` nothing moved, as expected.
Profile of one replica-and-acceptor node (perf, 12 s at steady state, `FLUSH_MS=100`): the `precog-wal` thread fell
from 21.3% to 4.7% of the node's samples, futex syscalls from 6.9% to 1.0%, context switches (`finish_task_switch`)
from 7.2% to 5.2%, and the node took 9% fewer samples while moving 5% more records. What remains: dedup lookups
(16.6%, memory-bound probes), socket sends (16%), pwrite (7.9%), recv (6.6%), `epoll_wait` (5.8%), fsync (3.6%).

The target (<= ~1.0 us) is not reached on this VM: -7% at write caching. Ack coalescing per read was not attempted:
the RPC writer task already coalesces every queued frame into one write, and with the append inline a replica's ack is
ready in the same poll as its request.

## Brokers at saturation (2026-11-19)

With brokers at 1 CPU each, the 1-CPU load generator is the bottleneck: the brokers run at 25-50% of a core and about a
fifth of their CPU is spent sleeping and waking per event. To measure busy brokers, the same harness with every broker
capped at **0.5 CPU** (both systems: `run.sh` has no setting for this, so its broker `docker run` lines were edited from
`--cpus 1` to `--cpus 0.5`; the load generators stay at 1 CPU), `WARMUP=20`, medians of 3 runs alternating Precog and Redpanda. The
last runs overlapped other builds on the VM, so treat the spread as wider than usual.

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| Precog, fsync before every ack | 348,500 | 4.87 ms | 15.2 ms | 1.73 us |
| Redpanda, fsync before every ack | 114,100 | 12.1 ms | 128 ms | 4.20 us |
| **Precog vs Redpanda** | **3.1x** | **2.5x** | **8.4x** | **2.4x** |
| Precog, write caching | 388,700 | 4.32 ms | 14.2 ms | 1.36 us |
| Redpanda, write caching | 125,000 | 11.3 ms | 114 ms | 3.48 us |
| **Precog vs Redpanda** | **3.1x** | **2.6x** | **8.0x** | **2.6x** |

Runs (records/s, p50, p99, us/record): Precog fsync 348k/4.10/15.0/1.728, 356k/4.87/15.2/1.579, 336k/5.08/16.3/1.800;
Redpanda fsync 64k/11.9/380/4.203, 114k/12.1/128/4.210, 130k/12.5/32.9/4.166; Precog write caching 380k/4.09/30.1/1.424,
389k/4.37/14.2/1.352, 403k/4.32/12.1/1.360; Redpanda write caching 125k/11.0/140/3.477, 128k/11.3/114/3.606,
94k/13.2/51.8/3.451. CPU per record rises on both sides under the tighter quota, and the ratio stays at 2.4-2.6x:
saturation does not change it.

## Re-measured on main after #16 and #25 (2026-11-19, 12:10-12:45 UTC)

Same harness and standard settings (brokers at 1 CPU / 4000Mi, `RETENTION=600000000 WARMUP=20 BATCH=250`, 20% resends,
2,000 in flight), alternating Precog main (44741d2, with appends on the worker under write caching, #16), Precog #25 (RAM
per retained batch off the heap) and Redpanda, 3 rounds per durability mode. The VM was slower than in the sections above:
the build measured there (26ed4c7) ran 1.60-1.68 us/record against 1.12 then, in an A/B alternating with main (1.47-1.66),
so absolute numbers are not comparable across sections, and main did not regress.

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| Precog #25, write caching | 450,200 | 3.98 ms | 9.38 ms | 1.42 us |
| Precog main, write caching | 425,800 | 4.21 ms | 10.4 ms | 1.50 us |
| Redpanda, write caching | 138,700 | 11.1 ms | 44.3 ms | 3.81 us |
| **Precog #25 vs Redpanda** | **3.2x** | **2.8x** | **4.7x** | **2.7x** |
| Precog #25, fsync before every ack | 423,700 | 4.28 ms | 13.5 ms | 1.89 us |
| Precog main, fsync before every ack | 405,600 | 4.46 ms | 16.0 ms | 1.96 us |
| Redpanda, fsync before every ack | 138,500 | 11.8 ms | 40.4 ms | 4.45 us |
| **Precog #25 vs Redpanda** | **3.1x** | **2.8x** | **3.0x** | **2.4x** |

Runs (records/s, p50, p99, us/record):

- **Write caching:**
  - Precog #25: 454k/3.98/9.37/1.411, 450k/3.98/9.38/1.418, 434k/4.06/10.08/1.460.
  - Precog main: 454k/3.98/9.26/1.418, 426k/4.21/10.36/1.495, 408k/4.27/10.76/1.522.
  - Redpanda: 139k/11.03/44.3/3.803, 143k/11.47/35.3/3.810, 129k/11.12/57.5/4.043.
- **fsync before every ack:**
  - Precog #25: 424k/4.28/13.45/1.906, 413k/4.31/15.35/1.890, 437k/4.10/13.48/1.868.
  - Precog main: 409k/4.46/12.44/1.926, 406k/4.42/16.02/1.956, 391k/4.48/18.18/2.134.
  - Redpanda: 138k/11.89/42.9/4.529, 123k/11.81/40.4/4.452, 143k/11.78/36.1/4.401.

Precog's CPU per record rose more than Redpanda's on the slower VM (about +40% against +12-36%), so the ratios narrow:
throughput and p99 stay at 3x or more, CPU per record is 2.4-2.7x.

A further attempt to cut CPU, sending each batch to `copies - 1` peers first with the third copy later and coalesced
(hedged after 10 ms), was measured and not merged. Five alternating runs per build:

- **Choosing the hot peer per writer log:** fsync per ack 1.965 → 1.977 us; write caching 1.524 → 1.475 us. Both are
  within noise. Each replica's group commit still fsyncs back to back, because per-log choice keeps both replicas
  busy.
- **Choosing it per stream:** 11% less in both modes, but only because this benchmark has one stream on one accepting
  node, which leaves one replica idle. When every node accepts streams, that saving goes away.

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
