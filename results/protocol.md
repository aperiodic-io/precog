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

## Recommended wire design

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
