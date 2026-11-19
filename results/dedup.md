# Dedup index prototypes (round 1, 2026-11-18)

`cargo run --release -p precog-lab --bin dedup` on a 4-vCPU VM: a 5-minute window, ~26.7k new ids/s (8M live ids), 15 simulated minutes, measured over the last 5. ns/event includes the driver (id formatting and xxh3: row 0). "wrong" counts decisions that differ from the ground truth (row 0 has no index, so its wrong count is the number of resends). Batches of 100 events.

| index | resends | ns/event | MiB at end | bytes/live id | wrong |
|---|---:|---:|---:|---:|---:|
| 0 no index (driver cost) | 0% | 56.6 | 0 | 0.0 | 0 |
| F packed 12-byte slots + prefetch | 0% | 289.7 | 132 | 17.2 | 0 |
| A generations k=2 | 0% | 199.4 | 256 | 33.6 | 0 |
| A generations k=4 | 0% | 474.0 | 128 | 16.8 | 0 |
| A generations k=8 | 0% | 581.4 | 128 | 16.8 | 0 |
| B stamped table | 0% | 277.5 | 192 | 25.2 | 0 |
| C hashmap + fifo | 0% | 222.8 | 296 | 38.9 | 0 |
| D stamped + batch prefetch | 0% | 218.4 | 192 | 25.2 | 0 |
| E generations k=2 + batch prefetch | 0% | 152.4 | 256 | 33.6 | 0 |
| 0 no index (driver cost) | 20% | 58.1 | 0 | 0.0 | 6011425 |
| F packed 12-byte slots + prefetch | 20% | 250.4 | 132 | 17.2 | 0 |
| A generations k=2 | 20% | 215.1 | 256 | 33.6 | 0 |
| A generations k=4 | 20% | 379.5 | 128 | 16.8 | 0 |
| A generations k=8 | 20% | 515.3 | 128 | 16.8 | 0 |
| B stamped table | 20% | 245.9 | 192 | 25.2 | 0 |
| C hashmap + fifo | 20% | 218.9 | 296 | 38.8 | 0 |
| D stamped + batch prefetch | 20% | 176.3 | 192 | 25.2 | 0 |
| E generations k=2 + batch prefetch | 20% | 144.2 | 256 | 33.6 | 0 |
| 0 no index (driver cost) | 50% | 69.2 | 0 | 0.0 | 24025696 |
| F packed 12-byte slots + prefetch | 50% | 200.1 | 132 | 17.2 | 0 |
| A generations k=2 | 50% | 194.7 | 256 | 33.6 | 0 |
| A generations k=4 | 50% | 318.7 | 128 | 16.8 | 0 |
| A generations k=8 | 50% | 381.4 | 128 | 16.8 | 0 |
| B stamped table | 50% | 188.2 | 192 | 25.2 | 0 |
| C hashmap + fifo | 50% | 177.3 | 297 | 38.9 | 0 |
| D stamped + batch prefetch | 50% | 156.3 | 192 | 25.2 | 0 |
| E generations k=2 + batch prefetch | 50% | 125.4 | 256 | 33.6 | 0 |

## Round 2: tables sized exactly (multiply-shift), k = 3 and 4 with prefetch, SwissTable generations (H)

Quiet machine. Exact sizing cuts memory (generations k=2: 33.6 → 20.1 bytes/id) but slows lookups:
at ~80% load a new id walks a long linear-probing run. H (hashbrown, SIMD tag probes) is fastest but
its power-of-two capacity costs 28-38 bytes/id.

| index | resends | ns/event | MiB at end | bytes/live id | wrong |
|---|---:|---:|---:|---:|---:|
| 0 no index (driver cost) | 0% | 67.1 | 0 | 0.0 | 0 |
| F packed 12-byte slots + prefetch | 0% | 300.6 | 132 | 17.2 | 0 |
| H swiss generations k=2 | 0% | 152.0 | 288 | 37.7 | 0 |
| H swiss generations k=3 | 0% | 139.8 | 216 | 28.3 | 0 |
| A generations k=2 | 0% | 316.4 | 154 | 20.1 | 0 |
| A generations k=4 | 0% | 513.5 | 102 | 13.4 | 0 |
| A generations k=8 | 0% | 960.3 | 88 | 11.5 | 0 |
| B stamped table | 0% | 264.4 | 192 | 25.2 | 0 |
| C hashmap + fifo | 0% | 183.8 | 296 | 38.9 | 0 |
| D stamped + batch prefetch | 0% | 198.5 | 192 | 25.2 | 0 |
| E generations k=2 + batch prefetch | 0% | 219.1 | 154 | 20.1 | 0 |
| E generations k=3 + batch prefetch | 0% | 324.0 | 115 | 15.1 | 0 |
| E generations k=4 + batch prefetch | 0% | 384.9 | 102 | 13.4 | 0 |
| 0 no index (driver cost) | 20% | 58.1 | 0 | 0.0 | 6011425 |
| F packed 12-byte slots + prefetch | 20% | 266.3 | 132 | 17.2 | 0 |
| H swiss generations k=2 | 20% | 142.7 | 288 | 37.7 | 0 |
| H swiss generations k=3 | 20% | 149.5 | 216 | 28.3 | 0 |
| A generations k=2 | 20% | 277.1 | 154 | 20.1 | 0 |
| A generations k=4 | 20% | 474.2 | 102 | 13.4 | 0 |
| A generations k=8 | 20% | 838.3 | 88 | 11.5 | 0 |
| B stamped table | 20% | 243.3 | 192 | 25.2 | 0 |
| C hashmap + fifo | 20% | 247.2 | 296 | 38.9 | 0 |
| D stamped + batch prefetch | 20% | 212.8 | 192 | 25.2 | 0 |
| E generations k=2 + batch prefetch | 20% | 218.4 | 154 | 20.1 | 0 |
| E generations k=3 + batch prefetch | 20% | 284.1 | 115 | 15.1 | 0 |
| E generations k=4 + batch prefetch | 20% | 359.5 | 102 | 13.4 | 0 |
| 0 no index (driver cost) | 50% | 73.0 | 0 | 0.0 | 24025696 |
| F packed 12-byte slots + prefetch | 50% | 213.0 | 132 | 17.2 | 0 |
| H swiss generations k=2 | 50% | 217.5 | 288 | 37.7 | 0 |
| H swiss generations k=3 | 50% | 184.5 | 216 | 28.3 | 0 |
| A generations k=2 | 50% | 220.1 | 154 | 20.1 | 0 |
| A generations k=4 | 50% | 333.6 | 102 | 13.4 | 0 |
| A generations k=8 | 50% | 614.5 | 88 | 11.5 | 0 |
| B stamped table | 50% | 190.5 | 192 | 25.2 | 0 |
| C hashmap + fifo | 50% | 169.9 | 297 | 38.9 | 0 |
| D stamped + batch prefetch | 50% | 178.1 | 192 | 25.2 | 0 |
| E generations k=2 + batch prefetch | 50% | 167.6 | 154 | 20.1 | 0 |
| E generations k=3 + batch prefetch | 50% | 216.8 | 115 | 15.1 | 0 |
| E generations k=4 + batch prefetch | 50% | 244.5 | 102 | 13.4 | 0 |

## Decision

- **Ingest-side, not on read.** Two producer replicas send every event, so about half the traffic is
  duplicates: deduplicating on read would store and replicate that half three times and run an index
  in every reader (2-3 per stream); at ingest it is dropped once, before replication.
- **CPU does not decide it.** Every exact index costs ~80-250 ns/event net of the driver, 3-8% of the
  ~3.3 µs per record the brokers spend today (reports/Native protocol design space.md).
- **Memory does.** Generations of plain digest tables, sized exactly and probed with batch prefetch:
  k = 3 by default (15.1 bytes per live id, ~230-300 ns), k = 4 when memory matters more (13.4 bytes,
  below the fork's 14), k = 2 for speed (20.1 bytes). They remember between the window and
  window × k / (k - 1), so every id is remembered for at least the window.
- **Next, at the source:** the standby producer replica sending 12-byte hashes instead of payloads
  (report, experiment 3) would remove ~45% of bytes before the index is even reached.
