# Native protocol proofs of concept (round 1, 2026-11-18)

`cargo run --release -p precog-lab --bin proto -- --reps 3 --n 3000000 --secs 3` on a 4-vCPU
Firecracker VM (Xeon @ 2.30 GHz). Each POC is one client-to-server append path over loopback TCP
with std threads and blocking sockets. The server parses each batch, validates it and acks it,
and writes nothing to disk. Validation covers CRC-32C, structure and bounds, sequence contiguity,
known stream and writer ids, and one dedup hash per record: xxh3 of the id string for PCB2 rows,
or the u64 read from the hash column. There is no dedup index, replication or disk, so the
protocol is measured on its own.

- **Records.** 262,144 deterministic synthetic market-data records, cycled: protobuf trades (70%) and book tickers that carry the exchange's raw JSON, 24 symbols with a skewed mix, and dedup ids like `binance-futures-p3-123456`. The mean is 211 B of payload plus a 25 B id (`precog_lab::proto::records`).
- **Pinning.** Server connection threads run on CPU 0 (the second replica's on CPU 1). The client's writer thread runs on CPU 2 and its ack reader on CPU 3. In the pair rows (10, 11) both writers share CPU 2 and both readers share CPU 3.
- **CPU.** `getrusage(RUSAGE_THREAD)`, user plus system, on exactly those threads. Records/s and CPU come from saturation runs of 3M events, where the client sends as fast as acks allow.
- **Latency.** The @100k columns come from separate runs that offer 100k records/s for 3 s. Latency runs from a record's scheduled arrival to its ack, so batching delay is included and coordinated omission is avoided.
- **Bytes.** Client-to-server bytes per record (frames, hello included). Ack bytes are listed separately.
- **Pairs.** Rows 10 and 11 count per **unique** event. Two replicas each send all 3M events, so every CPU and byte figure is the pair's total divided by 3M.
- **Repetitions.** 3 per POC, interleaved across POCs, after one discarded warm-up run. Each cell is the median of its column, and the ranges are listed below.

Each row changes one idea against the row it builds on: 1→2→3→4→…→9→10. Row 3b is a side
branch of row 3 (linger is not one of the ten ideas, but row 3 showed it is needed). Row 11
combines the winners.

| POC | records/s | server µs/rec | client µs/rec | server µs/rec @100k | p50 µs @100k | p99 µs @100k | B/rec up | B/rec up @100k | B/rec acks | rec/frame | rec/frame @100k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 baseline: PCB2 x100, 1 in flight | 1934004 | 0.175 | 0.297 | 0.335 | 713 | 1339 | 251.2 | 251.1 | 0.25 | 100 | 100.0 |
| 2 pipelining: 8 in flight | 3491765 | 0.155 | 0.308 | 0.389 | 688 | 1478 | 251.2 | 251.1 | 0.25 | 100 | 100.0 |
| 3 batch-on-backlog (<=1 MiB) | 6535409 | 0.073 | 0.152 | 4.416 | 106 | 2169 | 250.1 | 302.8 | 0.01 | 4161 | 2.0 |
| 3b (side branch) 3 + linger 1 ms | 5329736 | 0.087 | 0.177 | 0.403 | 677 | 1393 | 250.1 | 251.1 | 0.01 | 4161 | 108.5 |
| 3b with linger 5 ms (`--linger-ms 5`) | 5763731 | 0.087 | 0.139 | 0.240 | 2944 | 7450 | 250.1 | 250.3 | 0.01 | 4161 | 516.4 |
| 4 cumulative acks | 5692614 | 0.075 | 0.165 | 4.248 | 89 | 554 | 250.1 | 310.6 | 0.01 | 4161 | 1.7 |
| 5 session-interned names | 5446137 | 0.080 | 0.181 | 4.125 | 113 | 892 | 250.1 | 275.3 | 0.01 | 4161 | 2.0 |
| 6 compact 14 B/record, hash column | 7265539 | 0.058 | 0.137 | 4.140 | 90 | 817 | 225.1 | 268.7 | 0.01 | 4161 | 1.7 |
| 7 columnar: varint deltas, symbol dict | 5195742 | 0.057 | 0.177 | 4.287 | 100 | 3437 | 223.2 | 270.3 | 0.01 | 4161 | 1.9 |
| 8 lz4 payload column | 2619349 | 0.034 | 0.385 | 4.158 | 107 | 3973 | 73.6 | 185.0 | 0.01 | 4161 | 2.1 |
| 9 zero-copy: in-place parse, writev | 2762311 | 0.030 | 0.367 | 4.066 | 102 | 302 | 73.6 | 194.9 | 0.01 | 4161 | 1.9 |
| 10a two replicas, both full | 1353273 | 0.064 | 0.745 | 4.928 | 129 | 768 | 147.3 | 299.5 | 0.01 | 4161 | 3.2 |
| 10b active full + standby hashes | 2426812 | 0.039 | 0.414 | 4.849 | 143 | 2677 | 81.7 | 171.6 | 0.01 | 4161 | 3.7 |
| 11 best: 2+3b+4+5+6+9+10b (linger 1 ms) | 5596247 | 0.069 | 0.180 | 0.724 | 680 | 1323 | 233.2 | 234.5 | 0.01 | 4161 | 108.6 |
| 11b best + lz4 (8), linger 1 ms | 2270892 | 0.042 | 0.449 | 0.550 | 684 | 1326 | 81.7 | 89.2 | 0.01 | 4161 | 108.3 |
| 11 best, linger 5 ms | 4260929 | 0.107 | 0.221 | 0.356 | 2874 | 6755 | 233.2 | 233.4 | 0.01 | 4161 | 512.8 |
| 11b best + lz4, linger 5 ms | 2193693 | 0.046 | 0.453 | 0.303 | 3013 | 7222 | 81.7 | 82.9 | 0.01 | 4161 | 512.8 |

The rows "11" and "11b" take the 10b pair: the active replica sends full batches, and the standby
sends hash-only shadow batches on its own connection. The rec/frame columns are the active's.

**Spread** (min-max of the 3 repetitions). This VM is noisy. Differences in server µs/rec smaller
than about 0.02 µs at saturation, or about 0.3 µs at 100k/s, are inside the noise. Read p99 at
100k/s as "how often a thread is descheduled", not as protocol cost.

| POC | records/s range | server µs/rec range | client µs/rec range | server µs/rec @100k range | p99 µs @100k range |
|---|---:|---:|---:|---:|---:|
| 1 baseline | 1780892-2009249 | 0.169-0.190 | 0.290-0.331 | 0.327-0.359 | 1319-2107 |
| 2 pipelining | 3456530-4310855 | 0.120-0.165 | 0.243-0.319 | 0.322-0.598 | 1265-4347 |
| 3 batch-on-backlog | 5300214-6858614 | 0.062-0.074 | 0.146-0.180 | 3.741-4.656 | 234-6377 |
| 3b + linger 1 ms | 4735882-7166022 | 0.072-0.091 | 0.139-0.183 | 0.345-0.630 | 1356-4380 |
| 4 cumulative acks | 5343309-6349431 | 0.072-0.090 | 0.156-0.176 | 3.880-4.513 | 240-5744 |
| 5 interned names | 4842482-8590400 | 0.050-0.085 | 0.116-0.184 | 3.520-4.294 | 739-5517 |
| 6 compact | 6637556-7421085 | 0.044-0.068 | 0.136-0.144 | 3.668-4.257 | 171-5395 |
| 7 columnar | 4425854-6726024 | 0.054-0.060 | 0.149-0.223 | 3.206-4.457 | 465-7274 |
| 8 lz4 | 2330657-2661072 | 0.029-0.035 | 0.384-0.430 | 3.525-4.227 | 1123-5712 |
| 9 zero-copy | 2578930-2789595 | 0.022-0.033 | 0.363-0.388 | 3.428-4.405 | 289-5619 |
| 10a two full | 1267050-1367252 | 0.051-0.072 | 0.739-0.788 | 4.475-6.412 | 340-5111 |
| 10b standby hashes | 2217669-2463744 | 0.031-0.048 | 0.408-0.449 | 4.778-6.350 | 958-4615 |
| 11 best | 4636310-6299434 | 0.065-0.078 | 0.159-0.202 | 0.506-1.209 | 1277-2470 |
| 11b best + lz4 | 2091152-2320018 | 0.041-0.054 | 0.430-0.476 | 0.494-1.204 | 1288-2964 |

## What each idea bought

- **1 Baseline** (today's PCB2 at 100 records per batch, 1 in flight, the copies of `encode_append` and `write_loop`, and `rpc::read_frame`'s `BytesMut` split). It costs 0.175 µs of server CPU per record at saturation and 0.34 µs at 100k/s. The protocol alone is already below the 0.37 µs per node budget at these batch sizes, so the remaining cost is elsewhere (WAL, replication, the dedup index), as the report suspected.
- **2 Pipelining (8 in flight)** gives 1.8× throughput, because the client no longer waits a round trip per batch. Server CPU per record barely moves (inside the noise). At 100k/s it changes nothing, because one batch fills every 1 ms and the round trip is far shorter.
- **3 Batch-on-backlog** halves server CPU per record at saturation (0.155 → 0.073) by building ~4,160-record frames, and gives 3.4× the baseline's throughput. At 100k/s it is the worst row by far: with no linger, the client sends whatever has queued, about 2 records per frame, and server CPU rises to 4.4 µs/record (13× the baseline). Interned names were not yet in place, so the per-batch names add about 50 B/record. The cost of a frame (a wake-up, a read and an ack write) dominates. From rows 3b at 1 ms and 5 ms, a frame costs about 20 µs plus about 0.2 µs per record on this VM when the server sleeps between frames.
- **3b Linger** (a side branch) fixes this. At 100k/s, 1 ms gives 108 rec/frame and 0.40 µs/record, and 5 ms gives 516 rec/frame and 0.24 µs/record, at a cost of p50 latency (0.7 ms and 2.9 ms). Linger never applies at saturation. **At the production rate, the batching policy decides server CPU more than any encoding does.**
- **4 Cumulative acks** show no measurable change in this harness. The server already writes one ack per read, and each connection has one writer, so one cumulative ack per read equals one ack per batch whenever a read holds one frame. Acks were already 0.01 B/record at backlog batch sizes. The idea pays off with many writers per connection and with group commit (one ack per commit instead of one per batch). It costs nothing, so keep it.
- **5 Session-interned names** show no CPU change at large frames: two small allocations and about 57 B per 4,160 records are invisible. On small frames they save 35 B/record (310 → 275 at 100k/s with no linger). Keep them, because they are cheap and remove string compares from the hot path.
- **6 Compact 14 B/record with a hash column** saves 25 B/record (250 → 225). Server CPU drops from 0.080 to 0.058 µs/record (the ranges overlap a little), because the broker reads a u64 instead of hashing a 25-byte string, and the client pays the xxh3 at no visible cost. It also makes the broker's per-record work fixed-stride, with no varints. Keep.
- **7 Columnar with varint deltas and a per-batch symbol dictionary** saves only 2 B/record more (225 → 223), even though it adds a broker-visible symbol key column that PCB2 lacks. Server CPU is unchanged and client CPU rises by about 0.04 µs. Drop it: fixed columns are simpler and as fast.
- **8 lz4 of the payload column** cuts bytes 3.0× (223 → 74 B/record at saturation, 89 B at 1 ms linger, 83 B at 5 ms). Server CPU also drops (0.057 → 0.034) because the broker reads and CRCs a third of the bytes, and it never decompresses. The client pays +0.21 µs/record, and one writer thread now limits throughput to about 2.6M records/s, which is still 10× the cluster's peak. On ~2-record frames (no linger) the ratio collapses (185 B/record).
- **9 Zero-copy** (in-place parse in one 8 MiB buffer, and `writev` of head and payload column instead of two copies) shows no measurable change (0.034 → 0.030 µs, inside the noise). After lz4 there are only 74 B/record left to copy, and today's `BytesMut` split already avoids a server-side copy. Not worth building now. Revisit if payloads stay uncompressed and frames are forwarded to peers.
- **10 Standby-replica hashes**: for the pair, against two full senders (10a), bytes drop 45% (147 → 82 B per unique event), server CPU drops 39% (0.064 → 0.039 µs), client CPU drops 44%, and saturation throughput is 1.8× (both writers share one CPU). The broker still sees every hash, so dedup lookups stay the same. The "want payloads for unmatched hashes" round trip (report §5.3 c) is not exercised.
- **11 Best** (11 / 11b) is pipelining, backlog frames with a 1-5 ms linger, cumulative acks, interned ids, the compact layout, and the standby shadow, with or without lz4. They compose: no row regressed another's gain. The pair costs 0.04-0.07 µs of server CPU per unique event at saturation and 0.30-0.72 µs at 100k/s. At 100k/s two connections each pay the per-frame cost, so the shadow should linger longer than the active replica.

# Round 3: transport and I/O (2026-11-20)

Rounds 1-2 measured the encoding and batching layer over plain blocking TCP. Round 3 holds that
layer fixed and changes only the I/O underneath it: event loops, io_uring, runtimes, zero-copy,
batched syscalls, socket knobs, offloads, replication topology, ack clocking, codecs and shared
memory. The question is whether a materially better transport or I/O design exists for the
broker, whose profile shows about a fifth of every node's CPU in context switches and wake-ups,
socket sends at about 15%, and dedup lookups at about 17%.

`cargo run --release -p precog-lab --bin transport -- --reps 3 --n 3000000 --secs 2` (and
`--group repl`, and `--bin codecs -- --reps 5`) on the same 4-vCPU Firecracker VM.

- **Records, batches, work.** Round 1's 262,144 market-data records, sent in round 1's frames and compact batches (14 B/record, hash column, payloads uncompressed: 225 B/record on the wire), pipelined (8 frames in flight), backlog frames up to 1 MiB, and a **1 ms linger** at fixed rates (round 1's winner). The server parses in place, validates (CRC-32C, structure, bounds, interned ids, sequence contiguity, one dedup hash per record) and sends cumulative acks (`precog_lab::net::Conn`, unit-tested against any cut of the stream and against damage).
- **Pinning.** Server threads run on CPU 0 (the SQPOLL kernel thread on CPU 1), client writers on CPU 2, ack readers on CPU 3. Replication rows put the leader on 0, the followers on 1 and 2, and the client on 3.
- **CPU.** `getrusage(RUSAGE_THREAD)` of exactly those threads. Tokio and quinn workers are metered from `on_thread_start`/`on_thread_stop`. io_uring kernel threads (`iou-sqp-*`, `iou-wrk-*`) are added from `/proc/self/task`.
- **Context switches.** From the same `getrusage`.
- **Syscalls.** From a per-thread perf counter on `raw_syscalls:sys_enter` (tracefs mounted).
- **Latency.** From each record's scheduled arrival to its ack (coordinated-omission-free), at 100k/s and 1M/s offered for 2 s each. p50, p99 and p99.9 are over all records.
- **Repetitions.** 3 per row, interleaved across all rows, after a discarded warm-up. Cells are medians. Full tables, ranges and per-run lines are in `results/round3/` (`transport.md`, `transport.log`, `repl.md`, `codecs.md`, `e2e*.jsonl`).
- **Networks.** Loopback, and a veth pair into a network namespace (`plab`, 10.88.0.0/24), which adds the neighbour, veth and backlog-queue path that a container network takes.

## What this VM supports (POC 12, and why some rows cannot exist)

| | here | consequence |
|---|---|---|
| NIC | `virtio_net` (Firecracker), 1 combined queue, TSO/GSO/GRO and checksum offload on, no LRO, no hardware timestamps | GSO/GRO and TSO apply (measured below); no multi-queue steering |
| io_uring | enabled (`io_uring_disabled=0`), kernel 6.18 (multishot recv, provided-buffer rings, `SEND_ZC`, `DEFER_TASKRUN` all work) | measured below |
| io_uring in a container | **refused**: `io_uring_setup` returns EPERM under Docker's default seccomp profile (measured with a static probe; works with `--security-opt seccomp=unconfined`) | an io_uring broker must run with a custom seccomp profile; check the production runtime's default profile before relying on it |
| NAPI on lo and veth | none: `SO_INCOMING_NAPI_ID` is 0 on loopback and on veth even with GRO on | `SO_BUSY_POLL` and epoll busy-poll cannot apply here; measured anyway as a check |
| AF_XDP | `xsk_bind` compiled in; virtio_net offers only copy-mode XDP on one queue | not measured (below) |
| RDMA | no `/sys/class/infiniband`, no rxe in the kernel | not measured (below) |
| DPDK | would have to take over the VM's only NIC (no IOMMU/vfio passthrough in Firecracker) | not measured (below) |
| huge pages | no hugetlbfs mounted; THP `madvise` | not used |

## Headline results

Server and client CPU per record; syscalls and context switches per 1,000 records (server side).
`sat` is saturation (3M records as fast as acks allow); `@100k` and `@1M` are fixed loads with
1 ms linger, except the *no linger* rows, where frames hold whatever has queued (about 2 records
at 100k/s): those rows isolate the **per-frame** cost of each I/O design.

| POC | records/s sat | server µs/rec sat | server µs/rec @100k | p50 / p99 / p99.9 µs @100k | server µs/rec @1M | p50 / p99 / p99.9 µs @1M | syscalls/krec @100k | cs/krec @100k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| std blocking thread (rounds 1-2) | 6.32M | 0.058 | 0.426 | 800 / 2985 / 5296 | 0.086 | 911 / 3207 / 5978 | 17.4 | 8.7 |
| epoll, one thread | 6.05M | 0.055 | 0.479 | 774 / 1682 / 3868 | 0.083 | 893 / 2537 / 5291 | 35.8 | 8.9 |
| io_uring recv, batched submit (`DEFER_TASKRUN`) | 6.09M | 0.053 | 0.461 | 742 / 1884 / 8151 | 0.079 | 879 / 2087 / 3751 | 18.0 | 8.9 |
| io_uring multishot + buf ring + fixed files | 6.89M | 0.065 | 0.472 | 747 / 2284 / 4334 | 0.104 | 909 / 3471 / 11912 | 18.0 | 8.9 |
| io_uring `READ_FIXED` (registered buffers) | 5.88M | 0.056 | 0.451 | 749 / 1970 / 10930 | 0.083 | 885 / 3156 / 5646 | 18.1 | 9.0 |
| io_uring multishot, SQPOLL | 7.84M | 0.162 | **10.162** | 742 / 2320 / 4808 | 1.035 | 893 / 2886 / 6071 | 18.0 | 17.8 |
| tokio multi-thread, `rpc.rs` shape (today) | 7.34M | 0.057 | 0.514 | 760 / 1468 / 3755 | 0.095 | 896 / 2221 / 5143 | 27.2 | 8.9 |
| tokio current-thread, `rpc.rs` shape | 7.28M | 0.058 | 0.477 | 747 / 1863 / 5749 | 0.103 | 909 / 2558 / 5671 | 26.9 | 8.9 |
| tokio current-thread, inline | 6.12M | 0.059 | 0.492 | 745 / 1611 / 3093 | 0.082 | 868 / 1886 / 5593 | 26.9 | 8.9 |
| monoio (thread-per-core, io_uring) | 6.51M | 0.057 | 0.445 | 735 / 1539 / 3459 | 0.080 | 851 / 1748 / 3410 | 18.1 | 9.0 |
| shared-memory ring (upper bound) | 9.59M | 0.044 | 0.564 | 693 / 1498 / 3024 | 0.088 | 829 / 1514 / 2541 | 18.0 | 9.0 |
| QUIC (quinn 0.11, GSO), one stream | 2.28M | 0.326 | 1.259 | 906 / 2224 / 3878 | 0.402 | 1305 / 5087 / 8719 | 78.3 | 41.0 |
| *no linger:* std blocking thread | 6.47M | 0.051 | 4.535 | 92 / 1684 / 5570 | 0.381 | 86 / 1289 / 3232 | 880.9 | 304.4 |
| *no linger:* epoll, one thread | 6.33M | 0.058 | 4.647 | 95 / 697 / 2526 | 0.431 | 78 / 1190 / 2829 | 1634.8 | 321.5 |
| *no linger:* epoll, adaptive spin 50 µs | 8.28M | 0.054 | 9.734 | 62 / 786 / 3067 | 0.953 | 66 / 1300 / 3898 | 15649.4 | 3.8 |
| *no linger:* io_uring multishot | 8.24M | 0.066 | 4.845 | 86 / 875 / 4557 | 0.437 | 85 / 1261 / 3341 | 756.2 | 314.1 |
| *no linger:* io_uring `READ_FIXED` | 6.69M | 0.054 | 5.257 | 88 / 1026 / 2726 | 0.476 | 80 / 1834 / 5010 | 746.2 | 255.2 |
| *no linger:* tokio multi-thread, `rpc.rs` shape (today) | 7.68M | 0.057 | **7.487** | 97 / 1361 / 3035 | **0.668** | 93 / 2803 / 5319 | 1433.2 | **412.7** |
| *no linger:* tokio current-thread, `rpc.rs` shape | 7.51M | 0.056 | **5.647** | 93 / 877 / 2932 | **0.524** | 79 / 1934 / 6545 | 1115.0 | **202.4** |
| *no linger:* tokio current-thread, inline | 6.47M | 0.052 | 5.562 | 85 / 615 / 2799 | 0.444 | 86 / 1372 / 3956 | 1311.6 | 254.6 |
| *no linger:* monoio | 6.75M | 0.055 | 5.301 | 85 / 1295 / 3849 | 0.453 | 86 / 844 / 2534 | 880.2 | 276.1 |
| *no linger:* shared-memory ring | 8.68M | 0.045 | 9.540 | 11 / 1499 / 3334 | 0.953 | 2 / 1829 / 3302 | 4.2 | 1.9 |

Spread (min-max of 3, the full table is in `results/round3/transport.md`): server µs/rec at
saturation varies by about 0.005-0.015 within a row, at 100k/s with linger by about 0.05-0.1,
and at 100k/s without linger by about 0.3-0.8. p99 at 100k/s ranges over 2-10x within a row
(one descheduling of a pinned thread is a millisecond). Differences smaller than these are noise.

## Knobs, batching, zero-copy, offloads (same harness)

| POC | records/s sat | server µs/rec sat | client µs/rec sat | server µs/rec @100k | p99 µs @100k | server µs/rec @1M | p99 µs @1M | client syscalls/krec sat |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| std blocking thread (reference) | 6.32M | 0.058 | 0.153 | 0.426 | 2985 | 0.086 | 3207 | 0.43 |
| epoll, busy spin | 7.57M | 0.126 | 0.128 | 9.828 | 1891 | 0.990 | 1759 | 0.43 |
| epoll, adaptive spin 50 µs then park | 7.47M | 0.054 | 0.133 | 0.569 | 1688 | 0.083 | 1977 | 0.43 |
| `SO_BUSY_POLL` 50 µs (no NAPI id: cannot apply) | 7.01M | 0.053 | 0.144 | 0.426 | 2254 | 0.074 | 1665 | 0.43 |
| `TCP_QUICKACK` after every read | 7.07M | 0.051 | 0.141 | 0.465 | 1672 | 0.079 | 1992 | 0.43 |
| Nagle on (no `TCP_NODELAY`) | 7.10M | 0.052 | 0.137 | 0.416 | 1790 | 0.079 | 2413 | 0.43 |
| `TCP_CORK` around each send | 6.19M | 0.051 | 0.159 | 0.418 | 1660 | 0.082 | 2737 | 0.87 |
| 64 KiB socket buffers | 5.56M | 0.065 | 0.163 | 0.415 | 1438 | 0.098 | 2869 | 0.44 |
| 8 MiB socket buffers | 6.08M | 0.061 | 0.150 | 0.397 | 1635 | 0.076 | 2005 | 0.43 |
| 250-record frames, a `write` each | 5.51M | 0.088 | 0.194 | 0.251 | 2844 | 0.128 | 1164 | 7.58 |
| 250-record frames, `writev` of 8 | **9.14M** | **0.047** | **0.112** | 0.257 | 2935 | 0.129 | 1762 | 1.26 |
| 250-record frames, `sendmmsg` of 8 | 6.71M | 0.056 | 0.158 | 0.244 | 3070 | 0.133 | 1328 | 1.33 |
| 64 KiB frames, `write` | 6.06M | 0.087 | 0.176 | 4.813 | 563 | 0.396 | 1184 | 6.60 |
| 64 KiB frames, `MSG_ZEROCOPY` | 4.90M | 0.105 | 0.216 | 4.281 | 1417 | 0.395 | 1400 | 13.59 |
| 64 KiB frames, io_uring `SEND_ZC` | 4.06M | 0.126 | 0.265 | 4.772 | 2044 | 0.477 | 1800 | 10.08 |
| 256 KiB frames, `write` | 6.14M | 0.058 | 0.161 | 4.637 | 1469 | 0.391 | 2095 | 1.72 |
| 256 KiB frames, `MSG_ZEROCOPY` | 5.41M | 0.070 | 0.186 | 4.299 | 588 | 0.427 | 1052 | 3.37 |
| 256 KiB frames, io_uring `SEND_ZC` | 5.32M | 0.071 | 0.188 | 4.808 | 1431 | 0.466 | 2140 | 2.55 |
| 1 MiB frames, `write` | 7.54M | 0.056 | 0.133 | 4.491 | 1430 | 0.369 | 1894 | 0.43 |
| 1 MiB frames, `MSG_ZEROCOPY` | 6.22M | 0.072 | 0.159 | 4.350 | 1142 | 0.372 | 1808 | 0.83 |
| 1 MiB frames, io_uring `SEND_ZC` | 5.43M | 0.068 | 0.178 | 5.251 | 1372 | 0.449 | 2149 | 0.65 |
| loopback, TSO/GSO/GRO off | 6.96M | 0.054 | 0.144 | 0.441 | 1794 | 0.080 | 2146 | 0.43 |
| veth: std blocking thread | 5.21M | 0.064 | 0.191 | 0.443 | 1574 | 0.084 | 2504 | 0.43 |
| veth: epoll, one thread | 5.03M | 0.068 | 0.200 | 0.489 | 1532 | 0.091 | 1780 | 0.43 |
| veth: io_uring multishot | 4.76M | 0.077 | 0.208 | 0.493 | 1439 | 0.108 | 3500 | 0.43 |
| veth: tokio multi-thread, `rpc.rs` shape | 4.91M | 0.067 | 0.205 | 0.593 | 1833 | 0.093 | 2093 | 0.43 |
| veth: 1 MiB frames, `MSG_ZEROCOPY` | 5.15M | 0.086 | 0.185 | 4.095 | 1259 | 0.429 | 1816 | 0.82 |
| veth: GRO on + `SO_BUSY_POLL` 50 µs (still no NAPI id) | 5.13M | 0.059 | 0.193 | 0.431 | 1571 | 0.087 | 4453 | 0.44 |
| veth: TSO/GSO off | **3.05M** | **0.137** | **0.308** | 0.455 | 2007 | 0.137 | 2599 | 0.43 |

The fixed-frame-size rows (64 KiB to 1 MiB) have no linger, so at fixed loads their frames are
small; read their zero-copy comparison at saturation. `MSG_ZEROCOPY` completions came back with
`SO_EE_CODE_ZEROCOPY_COPIED` on loopback and on veth: the kernel copies whenever the receiver is
local, so these rows measure the bookkeeping cost, not the saving.

## Eight connections, one wake-up, and acks clocked by group commit

Eight producer connections at the same total rate. The *commit* rows add a group-commit thread
that "fsyncs" (sleeps 250 µs, no CPU) back to back and then releases acks. *Ack writer per conn*
wakes one ack thread per connection per commit; *coalesced* writes every connection's ack from the
commit thread in one wake-up. *Clocked* clients have 1 frame in flight and no linger: they send
their whole backlog when the previous commit's ack arrives.

| POC | records/s sat | server µs/rec sat | server µs/rec @100k | p50 / p99 / p99.9 µs @100k | server µs/rec @1M | p50 / p99 / p99.9 µs @1M | server cs/krec @100k | client µs/rec @100k |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| std thread per connection | 7.23M | 0.058 | 1.758 | 663 / 2140 / 5758 | 0.203 | 699 / 4230 / 13033 | 76.0 | 5.466 |
| epoll, one thread | 7.33M | 0.071 | 1.777 | 668 / 16593 / 34069 | 0.176 | 687 / 24555 / 38324 | 68.3 | 5.371 |
| io_uring multishot, one submit per wake-up | 7.61M | 0.083 | 1.690 | 679 / 13186 / 30653 | 0.183 | 696 / 20706 / 34633 | 65.8 | 5.333 |
| tokio multi-thread, `rpc.rs` shape | 7.54M | 0.056 | 1.757 | 658 / 1892 / 3579 | 0.195 | 687 / 1825 / 3378 | 73.3 | 5.366 |
| monoio | 7.01M | 0.073 | 1.993 | 690 / 13805 / 31346 | 0.235 | 702 / 17357 / 31914 | 66.0 | 5.380 |
| commit, linger 1 ms, ack writer per conn | 7.32M | 0.067 | 2.056 | 1203 / 3113 / 10569 | 0.254 | 1222 / 4391 / 14190 | 203.4 | 5.634 |
| commit, linger 1 ms, **acks coalesced** | 6.75M | 0.071 | **1.759** | 1212 / 3335 / 8492 | **0.214** | 1223 / 3106 / 9760 | 106.6 | 5.463 |
| commit, clocked, ack writer per conn | 7.94M | 0.068 | 2.484 | 1021 / 3430 / 11413 | 0.273 | 969 / 3153 / 9602 | 279.1 | 9.230 |
| commit, clocked, **acks coalesced** | 7.01M | 0.063 | 1.787 | 1017 / 2788 / 8208 | 0.209 | 976 / 2934 / 9623 | 140.7 | 8.975 |

## Replication topology (POC 7): 3 nodes, ack at 2 copies

The client sends to the leader. The leader validates and forwards each frame, and acks the
client once its own copy and one follower's are validated. *Chain*: the leader sends to f1, and
f1 forwards each whole frame to f2. *Pipelined chain*: f1 forwards every received chunk before
parsing it. Bytes count every link, acks included.

| topology | records/s sat | leader µs/rec sat | all nodes µs/rec sat | B/rec, all links | leader µs/rec @100k | all µs/rec @100k | p50 / p99 / p99.9 µs @100k | leader µs/rec @1M | all µs/rec @1M | p50 / p99 / p99.9 µs @1M |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| star (today) | 5.53M | 0.156 | 0.266 | 675.4 | 0.908 | 1.694 | 858 / 2711 / 5280 | 0.184 | 0.340 | 1040 / 5852 / 20169 |
| chain, store and forward | 6.09M | 0.109 | 0.273 | 675.4 | 0.729 | 1.840 | 906 / 3389 / 8152 | 0.132 | 0.341 | 1106 / 3372 / 5937 |
| pipelined chain | 6.47M | 0.106 | 0.277 | 675.4 | 0.707 | 1.905 | 900 / 2651 / 5032 | 0.127 | 0.338 | 1094 / 3183 / 5515 |

Ranges: leader µs/rec at saturation 0.152-0.165 (star) against 0.097-0.116 (chains); at 100k/s
0.860-1.082 against 0.650-0.843.

## Codecs (POC 9): the payload column of round 1's records

Payloads compressed as one block per batch. Bytes per record include the 14 B of fixed columns
and the 60 B batch header (amortised). The `none` row's "compress" is the copy of the column.
CPU is one pinned thread, 5 repetitions.

| codec | B/rec, 100-record batches | compress µs/rec @100 | decompress µs/rec @100 | B/rec, 4096-record batches | compress µs/rec @4096 | decompress µs/rec @4096 |
|---|---:|---:|---:|---:|---:|---:|
| none | 225.7 | 0.126 | 0.015 | 225.1 | 0.122 | 0.015 |
| lz4 (today) | 80.9 | 0.352 | 0.076 | 75.6 | 0.396 | 0.076 |
| zstd 1 | 51.9 | 0.559 | 0.230 | 50.9 | 0.531 | 0.195 |
| zstd 3 | 51.5 | 0.671 | 0.228 | 51.0 | 0.693 | 0.196 |
| zstd 1 + 16 KiB dictionary | 51.1 | 0.811 | 0.258 | 50.1 | 0.576 | 0.244 |
| zstd 3 + 16 KiB dictionary | 51.3 | 0.991 | 0.257 | 51.1 | 0.896 | 0.197 |

The dictionary (trained in 337 ms on 20,000 records from another seed) buys nothing here. The
synthetic payloads repeat within a batch even at 100 records, so this is a lower bound on what a
dictionary does for real, more varied raw JSON.

## What each idea bought

1. **io_uring**:
   - **Overall:** no material change. Recv, multishot with a provided-buffer ring and fixed files, and `READ_FIXED` into registered buffers all land on the blocking thread's CPU per record, at saturation and at both loads.
   - **Syscalls:** they halve against epoll (18 against 36 per 1,000 records at 100k/s). Context switches do not change (8.9/krec), because each frame still wakes the server once.
   - **Per frame (no-linger rows):** 4.8-5.3 µs/rec against 4.5 for a blocking thread and 4.6 for epoll. A wake-up costs the same whether `epoll_wait` or `io_uring_enter` sleeps.
   - **SQPOLL:** a kernel thread polls the submission queue for its whole idle time. That is 10 µs/rec at 100k/s: a whole core to save syscalls the server does not spend.
   - **Batching acks:** batched submission (every connection's ack and the next receives in one `io_uring_enter`) works (139 against 293 syscalls/krec at 8 connections), but CPU per record is unchanged.
   - **Containers:** Docker's default seccomp profile refuses `io_uring_setup`.
2. **`MSG_ZEROCOPY` (64 KiB-1 MiB):**
   - **On loopback and veth:** it costs more than a copy (client 0.159-0.216 against 0.133-0.176 µs/rec at saturation) and saves nothing, because the kernel copies anyway when the receiver is local, and says so in every completion.
   - **Over a real NIC:** de Bruijn and Dumazet (netdev 2.1) report up to 92% of process cycles (39% systemwide) on a netperf micro-benchmark and 5-8% on production workloads. The kernel documentation says it only pays above about 10 KB per write.
   - **What the broker would save:** its batches are 250 records × ~80 B lz4 ≈ 20 KB per replicate frame, so the expected saving is at the bottom of that range, on the sending side only. io_uring `SEND_ZC` is no better here.
3. **Batched syscalls:**
   - **`writev`:** 8 frames per `writev` is the largest single effect in this round. At saturation with 250-record frames it gives 1.66× throughput, server CPU −47% (0.088 → 0.047 µs/rec) and client CPU −42%, because the server then reads 8 frames per wake-up.
   - **`sendmmsg`:** on TCP it is just N `sendmsg`s and gets half of that.
   - **At fixed loads:** nothing changes, because only one frame is ready per send.
   - **`recvmmsg`:** not built. On a stream socket one `read` already returns every queued byte up to the buffer (≈1 read per frame at saturation, 0.99 syscalls/krec), so it cannot read more per call.
   - **The broker** already coalesces every queued frame into one write (`rpc::write_loop`). A variant that `writev`s the frames' `Bytes` instead of copying them into one buffer was measured end to end (below) and did not help.
4. **Thread-per-core against Tokio, same work:**
   - **Saturation:** no difference (0.055-0.059 µs/rec).
   - **Per frame:** the multi-thread runtime in the broker's `rpc.rs` shape (a task per request, an mpsc to a writer task) costs **7.5 µs/rec against 5.6 for the same shape on a current-thread runtime** at 100k/s without linger, and 0.67 against 0.52 at 1M/s. It has twice the context switches (413 against 202 per 1,000 records). Ranges do not overlap (7.48-8.05 against 5.44-6.23).
   - **Inline handling:** doing the work inline instead of spawning (5.56) buys nothing on top. The cost is the hop between threads, not the task.
   - **monoio:** about the same as current-thread tokio (5.3), and it needs io_uring.
   - **With a 1 ms linger:** the gap shrinks to 0.51 against 0.48 (within noise), because frames are 100× larger.
5. **Latency knobs:**
   - **Busy spinning:** pins a core: 9.8 µs/rec at 100k/s for a p50 30 µs lower (no-linger rows).
   - **Adaptive spin-then-park (50 µs):**
     - **Large frames:** costs +0.09 µs/rec at 100k/s with linger, for no latency gain.
     - **Small frames:** doubles CPU (9.7 against 4.6 µs/rec) to take p50 from 95 to 62 µs. The broker's latency is milliseconds of linger and group commit, not tens of microseconds of wake-up, so there is nothing for it to buy.
   - **`SO_BUSY_POLL`:** cannot work without a NAPI id. There is none on lo or veth, and a virtio NIC queue would need `net.core.busy_poll` and per-queue NAPI.
   - **`TCP_QUICKACK`, Nagle, cork, buffer sizes:** inside the noise. One exception: 64 KiB buffers cost 12% throughput at saturation.
6. **Offloads:**
   - **veth:** turning TSO/GSO off costs 41% throughput and doubles server and client CPU per record at saturation. Segmentation offload already does its job and must stay on: check it in production (`ethtool -k`).
   - **Loopback:** turning the offloads off changes nothing, since lo moves 64 KiB segments either way.
   - **UDP GSO:** quinn uses it (below).
7. **Replication topology:**
   - **Leader:** a chain takes 30% off the leader (0.156 → 0.109 µs/rec at saturation, 0.91 → 0.73 at 100k/s).
   - **Cluster:** total CPU over the three nodes and total bytes are the same (0.27 µs/rec, 675 B/rec), because the work moves to f1.
   - **Latency at ack-at-2:** the same as a star (the second copy is still one hop away). A chain only lengthens the third copy.
   - **Precog:** every node accepts streams, so in a balanced cluster each node is the leader for a third of the traffic. A chain then saves nothing per node and adds a reconfiguration protocol on failure. Keep the star.
8. **Ack and frame clocking:**
   - **Coalesced acks:** writing every connection's ack from the commit thread in one wake-up instead of waking a writer per connection saves 14-16% of server CPU (2.06 → 1.76 µs/rec at 100k/s, 0.254 → 0.214 at 1M/s, ranges disjoint) and halves context switches.
   - **The broker** already gets this: tokio's per-connection writer tasks run on the same worker that the WAL's completion wakes, so the wake-ups are task polls, not thread switches. `results/compare.md` measured explicit coalescing at 1.196 against 1.266 µs/rec, within noise.
   - **Clocking by group commit:** clocking frames by the commit ack instead of a linger timer lowers p50 by about 200 µs (the linger is replaced by the commit). It costs the client 60% more CPU per record (smaller frames), and server CPU is the same once acks are coalesced.
   - **Recommendation:** keep the linger, and let a producer with a commit-sized backlog send early.
9. **Codecs:**
   - **Bytes and CPU:** zstd 1 sends a third fewer bytes than lz4 (51 against 76-81 B/record). It costs +0.14-0.21 µs/rec to compress and +0.12-0.15 µs/rec to decompress. Level 3 and dictionaries add CPU and no bytes.
   - **Broker saving, estimated:** the broker never decompresses, so its saving is only the bytes it receives, writes and forwards. When payloads went to lz4, 145 B/record fewer took broker CPU from 2.12 to 1.80 µs/rec (`results/compare.md`), about 0.0022 µs per byte. 25-30 B fewer would save about 0.06 µs/rec (≈4%), inside this VM's noise.
   - **Not taken end to end:** a codec change is a batch-format version and a Go-client change.
   - **When it would pay:** if network or disk bytes (not CPU) become the limit, zstd 1 is the codec to add as a per-batch codec value.
10. **Shared memory:**
    - **Upper bound:** 0.044 µs/rec at saturation against 0.055-0.058 for TCP. The whole kernel TCP path on loopback costs at most ~0.015 µs/record when frames are large.
    - **Per frame:** shared memory still pays a wake-up (park/unpark): at 100k/s with linger it costs about the same as TCP (0.56).
    - **Scope:** it only helps co-located clients, and live-ingestion's producers are not co-located with brokers. Not worth building.
11. **QUIC (quinn 0.11 with GSO):**
    - **CPU:** 5.6× the server CPU of TCP at saturation (0.326 against 0.058 µs/rec), 3× at 100k/s, and a third of the throughput. TLS and user-space congestion control cost far more than the multiplexing saves on one stream.
    - **0-RTT and many streams:** not measured. Connections are long-lived (0-RTT only shortens a reconnect, which TCP does in one round trip plus a hello). One stream per writer log would avoid head-of-line blocking between logs, but no row here has more than one log per connection. Not competitive, dropped again.
12. **Infeasible here:**
    - **AF_XDP:** published numbers are 3.7-6.5 Mpps per 1-2 cores for a full packet pipeline on a 25 GbE NIC (arXiv 2608.14589, i40e), and about 13 Mpps transmit per core for raw frames. It bypasses the kernel's TCP: a broker would need its own user-space TCP or its own reliable protocol over UDP, plus a zero-copy-capable NIC driver. This VM's virtio_net gives copy mode only, on one queue.
    - **DPDK:** same trade, a user-space TCP stack (mTCP, NSDI '14: up to 25× Linux for small-message transactions) and a dedicated NIC per node.
    - **RDMA:** FaRM (NSDI '14) gets 14.5× lower latency than TCP at peak and replicates with one-sided writes that use no backup CPU. It needs RoCE or InfiniBand NICs and a lossless fabric.
    - **Why none of them matter here:** all three cut per-packet kernel cost, and that is not where the broker's CPU goes. At 250-record batches a node handles ~2,000-4,000 frames/s, and its profile's socket sends (15%) are dominated by copying payload bytes into the stack, which these remove only with zero-copy NICs.

## End to end: the top candidates in the broker

`bench/compare/run.sh` at standard settings (`RETENTION=600000000 WARMUP=20 BATCH=250
FLUSH_MS=100`, `<label> precog 0.2 2000`, Docker, brokers at 1 CPU / 4000Mi), alternating builds
or flags every run. Candidates:

- **A, current-thread runtime.** From POC 4, the largest per-frame effect. The node runs its whole runtime on one thread: today, under a 1-CPU quota, it has one tokio worker plus the main thread.
- **B, vectored `write_loop`.** From POC 3. Every queued frame goes out in one `writev`, and bodies are not copied into a coalescing buffer.
- **A with a shorter `event_interval` (8).** A try at A's saturation latency.

A and B were built as environment switches for the experiment; only A was kept, as a flag.

**At saturation (2,000 appends in flight), 3 rounds each, alternating:**

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| main | 424,700 | 4.23 ms | 10.07 ms | 1.513 us |
| A, current-thread | 355,600 | 5.47 ms | 9.64 ms | **1.192 us (-21%)** |
| B, `writev` | 434,100 | 4.16 ms | 9.17 ms | 1.463 us (-3%) |

Runs (records/s, p50, p99, us/record): main 420.9k/4.24/10.07/1.517, 424.7k/4.23/10.41/1.513,
440.2k/4.09/9.57/1.487; A 338.3k/5.63/10.08/1.219, 355.6k/5.47/9.64/1.192, 382.7k/5.28/8.70/1.136;
B 431.6k/4.17/9.37/1.463, 434.1k/4.16/9.17/1.466, 467.1k/3.84/9.04/1.365. A second set: main
422.9k/4.30/9.40/1.504, 444.8k/4.07/9.65/1.389, 465.5k/3.86/8.92/1.371; A 347.7k/5.57/10.70/1.231,
324.8k/5.83/11.22/1.289, 363.8k/5.41/9.39/1.176; A with `event_interval` 8 358.2k/5.43/10.22/1.170,
365.1k/5.41/9.11/1.172, 364.1k/5.40/9.86/1.169.

**At saturation, A trades throughput for CPU.**
- **What happens:** A is 16-20% lower in throughput, and every A run is below every main run in CPU.
- **Not CPU-bound:** the brokers run at ~0.2 cores each in both builds. The load generator holds 2,000 appends in flight, so a higher per-append latency (p50 +1.2 ms) is lower throughput by Little's law.
- **Why the latency rises:** on one thread, each request waits behind every task queued before it. A shorter `event_interval` does not change that.

**At a fixed rate the comparison is at equal work:**

| | records/s | p50 ack | p99 ack | broker CPU per record |
|---|---:|---:|---:|---:|
| main, 300k/s | 300,000 | 2.39 ms | 11.26 ms | 1.831 us |
| A, 300k/s | 300,000 | 2.30 ms | 10.77 ms | **1.682 us (-8.1%)** |
| B, 300k/s | 300,000 | 2.78 ms | 11.79 ms | 1.902 us (+3.9%) |
| main, 100k/s | 100,000 | 2.05 ms | 5.74 ms | 2.226 us |
| A, 100k/s | 100,000 | 1.97 ms | 6.04 ms | 2.115 us (-5%) |

Runs:
- **300k/s:** main 1.831, 1.835, 1.812; A 1.669, 1.682, 1.688 (ranges disjoint); B 1.890, 1.902, 1.906.
- **With the shipped flag (`RUNTIME=current-thread`):** main 1.852, 1.887; A 1.615, 1.713.
- **100k/s:** main 2.164, 2.246, 2.226; A 2.306, 2.098, 2.115 (ranges overlap: noise). Latency is the same in every case.

**Where A's saving lands.**
- **Profile:** perf of one replica-and-acceptor node, 12 s at 300k/s. The node took 3.92 s of CPU on main and 3.61 s with A (−7.7%).
- **Wake-ups:** kernel time in wake-ups and switches (`_raw_spin_unlock_irqrestore` under `try_to_wake_up`, and `finish_task_switch`) fell from 15.5% to 13.3% of samples, 0.61 s → 0.48 s. That is about 40% of the saving.
- **Spread:** the rest is spread over tokio's cross-thread notification (no inject queue or unpark between the main thread and the worker).
- **Unchanged:** dedup lookups (19-20%), page-cache writes and CRC are the same.

**B does not help.** The copy it removes is small next to the kernel's own copy of the same bytes,
and the vectored writes cost more per call. Dropped.

## Recommendation (round 3)

**Do not change the transport.**
- **Measured:** TCP, today's frames and today's coalescing writer are within noise of io_uring, thread-per-core runtimes, zero-copy sends, batched syscalls, busy polling and every socket knob measured. The upper bound for any local transport, shared memory, is ~0.015 µs/record better at large frames.
- **Not measured, and not where the CPU goes:** kernel bypass (AF_XDP, DPDK, RDMA) cannot run here, and it targets per-packet cost, which is not where the broker spends.
- **Rejected:** QUIC is 3-6× worse. Chain replication only moves CPU between nodes.

**What the broker can use:**
- **Opt-in current-thread runtime.** `precog-node serve --runtime current-thread` is now a flag, off by default. It takes 8% off broker CPU per record at a fixed 300k/s with the same latency, and nothing measurable at 100k/s. At a fixed number of appends in flight, it lowers peak throughput by 16-20%. Use it where nodes run well below saturation and CPU is the constraint. Keep the default where peak throughput or queueing latency matters.
- **Keep segmentation offload on,** and keep socket buffers at their defaults or larger.
- **Keep the linger and large frames.** Per-frame cost (about 10-15 µs of wake-up per frame here) is still the lever:
  - one connection per producer process for all its streams;
  - acks written in the same wake-up as the commit that releases them (which tokio's single worker already does);
  - frames that grow with backlog.
- **Revisit:**
  - zstd 1 if bytes become the limit;
  - `MSG_ZEROCOPY` for replicate frames if they grow past ~64 KiB on a real NIC;
  - io_uring only once the brokers run under a seccomp profile that allows it and a profile shows syscall entry, not wake-ups, as the cost.

**Staging checks, if the flag is turned on.**
- **Merging changes nothing on staging.** The flag is off by default, and the node is not given `--runtime current-thread` unless its StatefulSet args (`serve --config … --member-from-hostname`, as in `deploy/k8s/precog.yaml`) add it.
- **Before and after**, with someone who has cluster access. Compare the same traffic window:
  - **CPU:** each pod's CPU (`container_cpu_usage_seconds_total`) per acknowledged record. The expected change is −5 to −10% at a steady rate.
  - **Latency:** the client-side ack p50 and p99. They should stay within ±10%. A sustained p99 rise means the node is near saturation, where this mode loses throughput.
  - **Health:** `precog_*` dedup and append counters (unchanged ratios), restarts (zero), memory (unchanged).
- **Rollback:** drop the argument, or move back to the previous image pin. Neither changes data or wire format.

**Caveats.**
- **Loopback and veth:** both charge the receiver's TCP processing to the sender's `write` (softirq in the sender's context), as in round 1. On a NIC the server pays it.
- **One VM:** single-run noise is about ±10% end to end and much more at p99. Every comparison above alternates candidates and reports ranges.
- **8-connection single-thread rows:** the single-thread event loops (epoll, io_uring, monoio) show a p99 of 13-25 ms that thread-per-connection and tokio do not. It was not investigated: it does not change CPU per record, and no recommendation rests on those rows.
- **Lab payloads:** uncompressed (225 B/record), so per-byte effects are larger in the lab than in a broker that receives lz4 payloads.

## Recommended wire design

Rounds 1-2 (encoding and batching). Round 3 (transport and I/O) changed nothing below: keep TCP, today's frames and today's coalescing writer; see "Recommendation (round 3)" above.

```text
frame  := u32 len | u8 kind | u64 req_id | body                 (today's 13 B frame; unchanged)
hello  := u16 n | n × (u32 id | u16 len | name)                 (once per connection: stream and writer ids)
append := u8 copies | batch                                     (up to ~8 frames / 8 MiB in flight)
batch  := "PCB3" | ver u8 | codec u8 | flags u16 | stream_id u32 | writer_id u32 | epoch u64
        | first_seq u64 | count u32 | base_time_us u64 | cols_len u32 | raw_len u32 | blob_len u32
        | crc32c u32                                            (60 B, fixed)
        | u32 time_delta_us[count] | u64 xxh3(dedup id)[count] | u16 len[count]   (14 B/record, never compressed)
        | blob: payloads, lz4 block when codec = 2               (the broker never decompresses)
shadow := the same header with codec = 3 and only u64 hash[count] (the standby replica)
ack    := u32 writer_id | u64 acked_through                     (cumulative: one per writer per read or group commit)
```

- **Client sending.** Send the queued backlog, up to 1 MiB per frame, with up to 8 frames in flight and a **linger of 1-5 ms**. Without linger the protocol loses at the operating rate.
- **What to keep (composes).** Pipelining (2), backlog frames with linger (3 + 3b), cumulative acks (4), interned ids (5), the fixed 14-byte columns with the hash column (6), lz4 or another client codec as a per-batch flag (8), and standby shadows (10).
- **What to drop.** Varint and dictionary columns (7) and the zero-copy buffer management (9).
- **u16 lengths.** These cap a record at 64 KiB. If that is not acceptable, a flag bit can select u32 lengths.
- **Expected server CPU for the protocol path (parse, validate, ack, per node receiving the client stream).** About **0.03-0.05 µs/record** with lz4 when frames are large, and **0.25-0.4 µs/record per connection at 100k/s with 1-5 ms linger** on this VM. The 100k/s figure is dominated by about 20 µs of wake-up, read and ack per frame. That is most of the 0.37 µs/node budget, so the product should let frames grow:
  - longer linger, or frames clocked by group commit;
  - one connection per producer process for all its streams;
  - an event loop that serves many ready sockets per wake-up.

  The encoding choice moves only about 0.02-0.04 µs/record. The batching policy and the standby shadow move about 10×.

**Caveats.**
- **Loopback CPU attribution.** On loopback the kernel's TCP receive processing runs in the sender's `write` context. Part of the server's real receive cost is therefore charged to the client writer here, and part of the client's ack-receive cost to the server's ack write. Over a NIC it would be softirq time on the server.
- **One connection per replica.** Wake-up amortisation across many connections is not measured.
- **Synthetic payloads.** The lz4 ratio depends on the payload, which carries raw JSON. Real raw records should be checked with experiment 17's sample before the 3× is relied on.
