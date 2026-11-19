### Single node, fsync before ack (Iggy `persisted` vs mesh copies=1)

| run | batch x size | msg/s (median) | range | MB/s | p50 ms | p99 ms | p99.9 ms | server CPU µs/msg | vs Iggy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| iggy-persisted/p1 | 100x256B | 72,010 | 72k-74k | 18.4 | 1.30 | 2.58 | 6.15 | 8.05 | baseline |
| mesh-c1/p1 | 100x256B | 160,910 | 156k-161k | 41.2 | 0.57 | 1.35 | 2.79 | 2.45 | 2.23x |
| iggy-persisted/p4 | 100x256B | 217,800 | 200k-229k | 55.8 | 1.74 | 3.26 | 8.81 | 5.35 | baseline |
| mesh-c1/p4 | 100x256B | 429,850 | 430k-445k | 110.0 | 0.86 | 1.78 | 4.45 | 1.53 | 1.97x |
| iggy-persisted/p16 | 100x256B | 344,430 | 343k-363k | 88.2 | 4.46 | 8.17 | 12.75 | 4.38 | baseline |
| mesh-c1/p16 | 100x256B | 906,900 | 868k-957k | 232.2 | 1.66 | 3.31 | 12.26 | 1.02 | 2.63x |

### 3 nodes, ack after 2 durable copies, 3 copies targeted (Iggy VSR `persisted` RF=3 vs mesh copies=2)

| run | batch x size | msg/s (median) | range | MB/s | p50 ms | p99 ms | p99.9 ms | server CPU µs/msg | vs Iggy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| iggy-persisted-rf3/p1 | 100x256B | 58,290 | 52k-59k | 14.9 | 1.64 | 2.96 | 6.46 | 20.81 | baseline |
| mesh-c2-tcp/p1 | 100x256B | 123,850 | 115k-124k | 31.7 | 0.77 | 1.43 | 3.75 | 8.47 | 2.12x |
| iggy-persisted-rf3/p6 | 100x256B | 128,910 | 126k-130k | 33.0 | 4.51 | 7.72 | 12.18 | 15.55 | baseline |
| mesh-c2-tcp/p6 | 100x256B | 365,540 | 361k-377k | 93.6 | 1.53 | 3.77 | 11.66 | 4.81 | 2.84x |
| iggy-persisted-rf3/p24 | 100x256B | 179,030 | 174k-180k | 45.8 | 13.10 | 20.56 | 47.90 | 13.24 | baseline |
| mesh-c2-tcp/p24 | 100x256B | 596,000 | 577k-610k | 152.6 | 3.62 | 13.69 | 42.98 | 3.41 | 3.33x |

### Iggy configurations that let its per-partition group commit work (24 producers, 3 nodes)

| run | batch x size | msg/s (median) | range | MB/s | p50 ms | p99 ms | p99.9 ms | server CPU µs/msg | vs Iggy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| iggy-persisted-rf3-1part/p24 | 100x256B | 372,650 | 371k-388k | 95.4 | 5.97 | 18.66 | 38.40 | 4.43 | baseline |
| iggy-persisted-rf3-3part/p24 | 100x256B | 299,540 | 290k-314k | 76.7 | 7.55 | 19.44 | 51.81 | 6.70 | baseline |
| iggy-persisted-rf3-gc300us/p24 | 100x256B | 179,890 | 177k-182k | 46.1 | 13.02 | 20.88 | 31.87 | 13.28 | baseline |
| mesh-c2-tcp/p24 | 100x256B | 618,750 | 591k-647k | 158.4 | 3.51 | 14.18 | 36.58 | 3.32 | 1.66x |

### Batch-size sensitivity (3 nodes, 6 producers, durable-2)

| run | batch x size | msg/s (median) | range | MB/s | p50 ms | p99 ms | p99.9 ms | server CPU µs/msg | vs Iggy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| iggy-persisted-rf3/b10 | 10x256B | 17,260 | 16k-17k | 4.4 | 3.36 | 6.09 | 12.36 | 128.09 | baseline |
| mesh-c2-tcp/b10 | 10x256B | 60,571 | 59k-62k | 15.5 | 0.90 | 2.12 | 14.65 | 31.75 | 3.51x |
| iggy-persisted-rf3/b1000 | 1000x256B | 555,700 | 550k-556k | 142.3 | 10.18 | 24.40 | 49.44 | 2.95 | baseline |
| mesh-c2-tcp/b1000 | 1000x256B | 743,200 | 636k-759k | 190.3 | 7.12 | 23.33 | 80.51 | 2.45 | 1.34x |

### No fsync barrier (Iggy `replicated` vs mesh `--no-sync`), 3 nodes, 6 producers

| run | batch x size | msg/s (median) | range | MB/s | p50 ms | p99 ms | p99.9 ms | server CPU µs/msg | vs Iggy |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| iggy-replicated-rf3/p6 | 100x256B | 619,960 | 611k-644k | 158.7 | 0.56 | 4.15 | 12.80 | 4.54 | baseline |
| mesh-c2-nosync-tcp/p6 | 100x256B | 672,110 | 646k-686k | 172.1 | 0.69 | 3.42 | 19.18 | 3.92 | 1.08x |

### Live feed: S subscribers each receive the whole stream (3 nodes, 6 producers, durable-2)

End-to-end latency = producer stamp before send -> message handed to the subscriber. `rate60k` = fixed 60k msg/s offered by both systems; `max` = producers as fast as acks allow.

| run | subs | produced msg/s | delivered msg/s per sub | delivered total | e2e p50 ms | e2e p99 ms | e2e p99.9 ms | e2e max ms | complete | server CPU µs/produced msg |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|
| iggy-persisted-rf3 rate60k-s1 | 1 | 60,060 | 60,060 | 60,060 | 2.68 | 5.88 | 16.85 | 29.70 | 3/3 runs | 17.70 |
| mesh-c2-tcp rate60k-s1 | 1 | 60,060 | 60,060 | 60,060 | 0.95 | 2.64 | 23.39 | 24.56 | 3/3 runs | 7.19 |
| iggy-persisted-rf3 rate60k-s4 | 4 | 60,040 | 60,040 | 240,160 | 2.66 | 5.51 | 10.13 | 24.34 | 3/3 runs | 19.51 |
| mesh-c2-tcp rate60k-s4 | 4 | 60,050 | 60,050 | 240,200 | 1.03 | 3.04 | 20.94 | 23.57 | 3/3 runs | 6.76 |
| iggy-persisted-rf3 rate60k-s16 | 16 | 60,050 | 60,050 | 960,800 | 3.98 | 9.27 | 16.54 | 31.39 | 3/3 runs | 24.54 |
| mesh-c2-tcp rate60k-s16 | 16 | 60,060 | 60,060 | 960,960 | 1.33 | 3.46 | 18.32 | 25.57 | 3/3 runs | 7.81 |
| iggy-persisted-rf3 rate60k-s32 | 32 | 59,920 | 59,920 | 1,917,440 | 6.57 | 18.78 | 36.67 | 291.84 | 3/3 runs | 30.27 |
| mesh-c2-tcp rate60k-s32 | 32 | 60,050 | 60,050 | 1,921,600 | 1.38 | 3.29 | 14.57 | 23.66 | 3/3 runs | 10.54 |
| iggy-persisted-rf3 max-s16 | 16 | 90,200 | 90,200 | 1,443,200 | 7.07 | 15.57 | 27.74 | 53.92 | 3/3 runs | 23.02 |
| mesh-c2-tcp max-s16 | 16 | 283,830 | 283,830 | 4,541,280 | 1.97 | 6.11 | 15.46 | 27.45 | 3/3 runs | 5.22 |
