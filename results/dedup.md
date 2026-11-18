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
