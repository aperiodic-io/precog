# Precog broker mesh POC vs Apache Iggy

A proof of concept for a **leaderless writer-log mesh**, benchmarked against
**current Apache Iggy**: server 0.10.0 (`apache/iggy:latest`, Nov 2026) with its
official Rust SDK 0.12.0 and its VSR cluster. Both systems are driven by the
same benchmark harness.

* `crates/mesh`: the mesh node, client and CLI. It has a group-commit WAL, direct
  peer replication with durable-N acks, anti-entropy repair, event-time reads, and
  TCP or **Iroh 1.4.1** QUIC transport. Design and guarantees are in
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
* `crates/bench`: one closed-loop producer benchmark with two backends, Iggy
  through `iggy::IggyClient::send_messages` and the mesh through `Writer::append`.
* `scripts/`: start native Iggy (single or 3-node VSR cluster) and mesh
  clusters, the benchmark suite, the failover experiment and the summariser.
* `results/`: raw JSONL, the [full tables](results/RESULTS.md) and the logs from the runs below.

The previous agent's patch was missing, so this POC is a fresh implementation of
the handover's design: writer logs, durable-2 out of 3 target copies, event-time
merge, and no consensus on the data path. It is a standalone data path, **not** an Iggy fork.

## Headline results

4 vCPU Xeon @ 2.1 GHz VM, ext4 on virtio disk (fdatasync ≈ 180 µs p50).
Everything runs on one machine over loopback: nodes, client, all of it. Median of
3 runs × 10 s after 2 s warm-up, 256 B messages, 100 per batch, one batch in flight
per producer. Each Iggy producer owns a partition; each mesh producer owns a writer log.
Iggy nodes run 1 shard each and mesh nodes 1 Tokio worker each.

| Durable acknowledgement (fsync) | Iggy 0.10 | Mesh (TCP) | Mesh (Iroh) |
|---|---:|---:|---:|
| 1 node, 16 producers | 344k msg/s, p50 4.5 ms | **907k msg/s, p50 1.7 ms (2.6×)** | — |
| 3 nodes, 2-of-3 durable, 6 producers | 129k msg/s, p50 4.5 ms | **366k, p50 1.5 ms (2.8×)** | 259k, p50 2.1 ms (2.0×) |
| 3 nodes, 2-of-3 durable, 24 producers | 179k msg/s, p50 13.1 ms | **596k, p50 3.6 ms (3.3×)** | 379k, p50 5.6 ms (2.1×) |
| …same, Iggy tuned: all producers on **1 partition** | 373k msg/s, p50 6.0 ms | **619k, p50 3.5 ms (1.66×)** | |
| 3 nodes, small batches (10 msgs), 6 producers | 17k msg/s, p50 3.4 ms | **61k, p50 0.9 ms (3.5×)** | |
| 3 nodes, large batches (1000 msgs), 6 producers | 556k msg/s, p50 10.2 ms | **743k, p50 7.1 ms (1.34×)** | |
| Server CPU per message (3 nodes, 6 producers) | 15.6 µs | **4.8 µs** | 8.5 µs |
| **Control: no fsync** (Iggy `replicated`, mesh `--no-sync`) | 620k msg/s | 672k (**1.08×**) | |

Failover, 3 nodes, 6 producers, durable-2. Node 0 is killed or frozen 8 s into a
20 s run. The numbers are the worst per-producer time without an ack, from 3 runs:

| Fault on node 0 | Iggy 0.10 VSR (heartbeat 2 s, its minimum) | Mesh |
|---|---|---|
| `SIGKILL` (crash, peers see RST) | 2 producers stall **1.8–2.7 s** (view change); 0 errors | all producers ≤ **71 ms**; 0 errors |
| `SIGSTOP` (hung host / partition) | 2 producers stall **30 s** (SDK request timeout) then error; 1–2 batches committed but never acked to the client | 2 producers stall **1.0 s** (= client RPC timeout, tunable) then fail over; 0 errors |
| Rejoin after crash | — | restarted node pulled ~4.5 M missed events by anti-entropy in ~24 s |

Every mesh run was checked after the fact: all three nodes hold exactly the
acknowledged event count. Iggy's topic counter matched the acknowledged count,
except for the unknown-outcome batches noted in the SIGSTOP row.

## Why the mesh is faster here, and what it costs

**1. The gain is in the durability path, not in a faster server.** With fsync
off, the two are within 8 % of each other (the control row). Everything above
that comes from how many fsync barriers and round trips each acknowledged byte
pays for:

* **Group commit across all writers.** The mesh has one WAL per node, and every
  append queued while an fdatasync runs joins the next one. That works out to
  about 2 batches per sync at 4 producers, more as load grows. Iggy groups only
  *within one partition*, because each partition has its own VSR group and WAL.
  With one producer per partition there is nothing to group. Forcing all 24
  producers onto one partition doubles Iggy's throughput (179k → 373k) and cuts
  its CPU per message by 3×. That is the strongest evidence for this
  explanation, and it shows that partitioning for parallelism actively hurts
  Iggy's durable throughput. Iggy's `wal_group_commit_delay_micros = 300` made no
  difference in this workload.
* **A shorter commit path.** The mesh writes locally and to its peers in
  parallel, then acks at the second durable copy: one network RTT plus one
  fsync. Iggy's persisted path goes through the primary's prepare pipeline,
  waits for quorum `prepare_ok` *with* stable-storage proofs, then commits and
  applies. Its p50 at one producer is 1.6 ms against 0.8 ms for the mesh.
* **Less work per message.** A mesh batch is opaque bytes plus a checksum. The
  node never re-encodes it, assigns no offsets and keeps no index on the ack path.

**2. Availability comes from having no leader.** In Iggy a partition can only
be written through its primary, so a dead primary means a view change: at least
one heartbeat timeout, and 2 s is the floor Iggy accepts. In the mesh any node
accepts any writer log, and a retry of the same bytes is idempotent. The client
moves on as soon as it notices the failure: immediately on a reset, or after its
own RPC timeout on a hang. Nothing has to be elected. A hung node is the worst
case for Iggy here, because clients seeded with that node waited out the full
30 s SDK timeout. That last part is client behaviour and could be tuned. The
view-change floor could not be tuned further.

**3. What is given up.** These are not free wins. The mesh has **no per-partition
total order across producers**. It orders within each writer log, and readers
merge by source event time, so late data and completeness windows become the
reader's problem. Consumer groups, linearizable offsets, retention, segment
storage, and placement or membership changes are not built. Iggy does all of
these, and some of them would add cost to the mesh: segment indexing off the ack
path, a small control plane. The mesh POC also keeps all data in RAM. The
fair claim is therefore narrower:

> For ingest workloads with many independent sources, where the order you need
> is per source plus an event-time merge, removing per-partition consensus from
> the write path gives **~2–3.5× durable throughput, ~2–4× lower median latency,
> ~3× less server CPU per message, and fault recovery in tens of ms instead of
> seconds** on commodity hardware. The gap narrows to ~1.3–1.7× when Iggy's
> workload is shaped to amortize its fsyncs (big batches, few hot partitions).

**4. Iroh costs about 30–40 % against raw TCP** on loopback (encryption, QUIC
congestion and ack processing, and one stream per peer). It still beats Iggy's
TCP path by 1.3–2.1× while giving authenticated, encrypted, NAT-traversing
connectivity. Pooling several streams per peer is the obvious next optimization.

## Caveats (read before quoting numbers)

* Everything ran on one 4-vCPU VM. Three nodes and the client share the CPUs,
  and all nodes **share one disk**, so "2 independent durable copies" here does
  not mean independent hardware. Loopback has about 0 RTT. Real cross-machine
  RTT adds to both systems' replication path. It would add more to Iggy when
  the partition primary is remote from the client, since the mesh writes to
  the nearest node, but **this was not measured**.
* The mesh POC holds data in memory and has no read-path, consumer or retention
  load. Iggy is a complete broker. Neither system's tail read latency was benchmarked.
* The Iggy configuration is defaults plus topic `durability`, `replication_factor`,
  1 shard per node and a 2 s heartbeat. Other tuning, such as more shards or
  pinning, may change its numbers. The `iggyfair` section is the attempt to give
  it its best layout.
* The harness counts message payload bytes only. Iggy messages carry a 64 B
  header each, mesh events 12 B.

## Reproduce

```bash
cargo test                       # 11 unit + 6 multi-node tests (TCP and Iroh)
cargo build --release
```

Iggy binaries come from the official image:

```bash
mkdir -p run/bin && id=$(docker create apache/iggy:latest) && docker cp $id:/usr/local/bin/iggy-server run/bin/ && docker rm $id
```

```bash
REPS=3 DUR=10 scripts/bench_suite.sh          # ~25 min -> results/suite.jsonl
```

```bash
scripts/failover.sh iggy kill 0; scripts/failover.sh mesh stop 0   # etc.
```

```bash
python3 scripts/summarize.py > results/RESULTS.md
```

Try the mesh by hand:

```bash
target/release/precog-node init --nodes 3 --dir run/demo --transport iroh
```

```bash
for i in 0 1 2; do target/release/precog-node serve --config run/demo/node-$i.json & done
```

```bash
target/release/precog-node demo --cluster run/demo/cluster.json --count 5 --copies 2
```

```bash
target/release/precog-node read --cluster run/demo/cluster.json --node 2   # event-time order
```

## Suggested next steps

1. Run on 3 separate machines and inject RTT/jitter with `tc netem` between
   nodes to measure the remote-primary effect.
2. Build a segment engine off the ack path and benchmark reads and tailing against
   Iggy polls.
3. Add event-time watermarks and completeness signals for readers, plus a minimal
   control plane for consumer offsets.
4. Iroh: multiple streams per peer, a test of relay mode, descriptor exchange
   for multi-host setups.
