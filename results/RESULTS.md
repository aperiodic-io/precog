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
| mesh-c2-iroh/p1 | 100x256B | 77,850 | 77k-79k | 19.9 | 1.23 | 2.25 | 4.66 | 16.02 | 1.34x |
| iggy-persisted-rf3/p6 | 100x256B | 128,910 | 126k-130k | 33.0 | 4.51 | 7.72 | 12.18 | 15.55 | baseline |
| mesh-c2-tcp/p6 | 100x256B | 365,540 | 361k-377k | 93.6 | 1.53 | 3.77 | 11.66 | 4.81 | 2.84x |
| mesh-c2-iroh/p6 | 100x256B | 258,780 | 245k-269k | 66.2 | 2.14 | 5.71 | 10.26 | 8.49 | 2.01x |
| iggy-persisted-rf3/p24 | 100x256B | 179,030 | 174k-180k | 45.8 | 13.10 | 20.56 | 47.90 | 13.24 | baseline |
| mesh-c2-tcp/p24 | 100x256B | 596,000 | 577k-610k | 152.6 | 3.62 | 13.69 | 42.98 | 3.41 | 3.33x |
| mesh-c2-iroh/p24 | 100x256B | 379,240 | 376k-396k | 97.1 | 5.62 | 15.63 | 26.41 | 6.64 | 2.12x |

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

### Failover (3 nodes, 6 producers, durable-2; node 0 faulted at t=8s)

| run | msg/s over run | errors | per-producer longest ack gap (ms) | stored vs acked |
|---|---:|---:|---|---|
| iggy-kill-n0 | 144,680 | 0 | [2363, 66, 722, 2065, 66, 576] | topic reports 3113200 messages; producers acked 3113200 |
| iggy-stop-n0 | 138,525 | 1 | [574, 967, 30001, 1303, 598, 781] | topic reports 2770600 messages; producers acked 2770500 |
| mesh-kill-n0 | 337,270 | 0 | [71, 57, 57, 71, 46, 57] | new events per node [n0=down n1=7484300 n2=7484300]; producers acked 7484300 |
| mesh-stop-n0 | 349,205 | 0 | [1002, 54, 54, 1000, 54, 60] | new events per node [n0=down n1=7678200 n2=7678200]; producers acked 7678200 |
| iggy-kill-n0 | 140,130 | 0 | [2655, 110, 85, 1807, 108, 168] | topic reports 2802800 messages; producers acked 2802800 |
| iggy-stop-n0 | 124,995 | 2 | [30001, 702, 812, 30001, 795, 874] | topic reports 2500100 messages; producers acked 2499900 |
| mesh-kill-n0 | 339,550 | 0 | [52, 49, 49, 52, 49, 49] | new events per node [n0=down n1=7425700 n2=7425700]; producers acked 7425700 |
| mesh-stop-n0 | 348,135 | 0 | [1002, 66, 66, 1002, 67, 68] | new events per node [n0=down n1=7655900 n2=7655900]; producers acked 7655900 |
| iggy-kill-n0 | 139,760 | 0 | [2582, 85, 84, 2045, 49, 64] | topic reports 2822000 messages; producers acked 2822000 |
| iggy-stop-n0 | 127,345 | 2 | [30001, 74, 231, 30001, 267, 278] | topic reports 2547200 messages; producers acked 2547000 |
| mesh-kill-n0 | 341,400 | 0 | [64, 45, 46, 64, 46, 48] | new events per node [n0=down n1=7554800 n2=7554800]; producers acked 7554800 |
| mesh-stop-n0 | 352,230 | 0 | [1001, 87, 60, 1000, 86, 60] | new events per node [n0=down n1=7797200 n2=7797200]; producers acked 7797200 |
