# Shared-log and ordering-decoupled log designs as ways to add total order to precog

Scope: academic and industry-research shared logs that separate ordering from data dissemination and durability, judged as ways to add a global total order on top of precog. Precog's model is per-writer logs, any-node accept, a node-wide group-commit WAL, direct peer replication, and an ack at 2 of 3 durable copies with no leader on the data path. The primary sources are the papers' PDFs, which were downloaded and text-extracted (Nov 2026). "Reported" means a number stated in the paper; "Inference" means my own reasoning or estimate.

Key to sources (all claims below link to these):
- Scalog (NSDI'20): https://www.usenix.org/system/files/nsdi20-paper-ding.pdf
- LazyLog (SOSP'24): https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf (extended TOCS version: https://shreesha00.github.io/assets/papers/lazylog_tocs.pdf)
- SpecLog/Belfast (OSDI'25): https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf ; https://www.usenix.org/conference/osdi25/presentation/bhat ; artifact https://github.com/dassl-uiuc/speclog-artifact
- Corfu (NSDI'12): https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf
- Tango (SOSP'13): https://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf
- vCorfu (NSDI'17): https://www.usenix.org/system/files/conference/nsdi17/nsdi17-wei-michael.pdf
- Delos / Virtual Consensus (OSDI'20): https://www.usenix.org/system/files/osdi20-balakrishnan.pdf
- FuzzyLog (OSDI'18): https://www.usenix.org/system/files/osdi18-lockerman.pdf
- Boki (SOSP'21): https://par.nsf.gov/servlets/purl/10357172 ; code https://github.com/ut-osa/boki
- Calvin (SIGMOD'12): https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf

---

## Q1. Per-design mechanics: how ordering is decoupled, who assigns positions and when, what consensus is used, what guarantee results, the reported numbers, and how each maps onto precog

### Takeaway
The designs fall into four families. In the first, a token is assigned before the write (Corfu, Tango, vCorfu, Delos NativeLoglet); this puts a sequencer round trip on every append, caps throughput at about 0.2 to 0.6M tokens/s, and introduces holes. In the second, order is assigned after durability by periodic cuts over per-server counts (Scalog, Boki, Belfast), so consensus runs once per interval rather than once per record. In the third, order is bound lazily after the ack and before the read (LazyLog/Erwin). In the fourth, the total order is given up for partial orders (FuzzyLog). Precog already is a "durability-first" data layer, so Scalog-style cuts and LazyLog-style lazy binding are the natural fits. Token sequencers would add a pre-write round trip that precog currently avoids.

### Cited Findings

**Scalog (NSDI 2020)**
- Mechanism. A client sends APPEND to a storage server, which replicates the record to the backups in its shard. Each server periodically sends a *local cut* (the count of records in each log segment it holds) to the ordering layer. The ordering layer commits a *global cut* and sends it back to the servers, and **only then do servers ACK the client** (steps 1 to 5 in Fig. 2). — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- A cut has m·n entries (m shards × n servers per shard). Each entry maps a server ID to the sequence number of the latest durable record in that server's segment. Because replication is FIFO, each local-cut integer is an exact count. The durable count for a segment is the element-wise minimum over the replicas' reports. Global positions come from a deterministic interleaving of the cut, and summing a cut gives the total number of records ordered through it. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- The ordering layer is a Paxos-replicated service plus a tree of stateless *aggregators* that relay and combine local cuts so that the Paxos leader is not overwhelmed by connections. A failed aggregator is replaced by creating a new one. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Interleaving interval. The evaluation fixed it at **0.1 ms** "to match Corfu's write latency". A larger interval lets the ordering layer manage more shards at the cost of latency. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Reported numbers. The hardware was CloudLab with 2× Xeon E5-2630 v3 8-core 2.4 GHz, 128 GB RAM, a 10 Gbps network, SSDs and 4 KB records. One shard peaks at **18.7K writes/s** (their Corfu reaches 13.9K). With 17 shards × 2 servers at 15K writes/s each, the real prototype reaches **255K totally ordered writes/s**. **Emulation** gives about **3,500 shards / ~52M writes/s at ~1.6 ms latency**: client latency is 1.3 ms with no aggregators, plus about 0.1 ms per aggregator layer across three layers. This is an emulated estimate, not a measurement. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- In the same paper, Corfu's maximum throughput is capped by its sequencer at **530K writes/s**. The paper also quotes the optimized Tango sequencer at about 570K/s. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Ordering-layer failure. "Scalog suffers no net throughput loss because of ordering layer failures." While the Paxos leader is down, records "are temporarily unable to commit" but storage servers keep accepting and storing them. After a new leader is elected, throughput "temporarily spikes to catch up … although latency suffers until a new leader is elected" (Fig. 4, about 50K writes/s workload, 6 shards × 2 servers). By contrast, Corfu's throughput halts on a sequencer failure and only returns to normal afterwards. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Reconfiguration. Shards are added or finalized through the ordering layer, which totally orders the finalization operations along with the cuts. The paper says Scalog loses no throughput while doing this. In the same experiment, "throughput in Corfu ceases for roughly 30 ms" (Fig. 3). On a storage-server failure under Finalize & Add, records not included in the last cut must be re-sent by clients to other shards. Under Remove & Replace, the affected shard "is temporarily unavailable for writes (but continues to serve reads)". — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Holes. Cuts only count records that are already durable, so there are no reserved-but-unwritten positions and no hole-filling. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf) (design). LazyLog's authors describe Scalog's append latency as 1 to 2 ms "even in low-throughput regimes" because the ack waits for batching plus coordination. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)

**LazyLog / Erwin (SOSP 2024, best paper; extended in ACM TOCS)**
- Abstraction. An append "does not eagerly bind it to a log position; it only provides durability and a guarantee that the record will be eventually bound to its correct position that respects linearizability". Order is bound before a position can be read. The motivating workloads are activity logging, event sourcing and CQRS, message queues, journaling, and DBs with decoupled readers, in which readers lag behind writers. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Erwin-bb (black-box). Clients write records **only to the sequencing layer**, which has f+1 replicas and **no cross-replica coordination**. The client waits for acks from *all* sequencing replicas in the same view, which makes the append 1 RTT. In the background, the sequencing leader takes batches from its local log, assigns positions starting at last-ordered-gp+1, and pushes them to unmodified shards (primary-backup, or Kafka). When the shards ack, the records are garbage-collected from all sequencing replicas and `stable-gp` advances. The paper says this design can be "bolt[ed]-on … atop existing per-shard-order systems like Kafka". — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Why real-time order holds without consensus. If append b starts after append a completes, then b appears after a in *every* sequencing replica's log, because the client wrote to all of them. Only concurrent appends can differ in order across replicas, so any replica's log is a valid linearizable order. On leader failure, any follower's log is used for unordered records. Order is exposed to readers only up to `stable-gp`. A read past `stable-gp` "takes a slow path waiting for stable-gp to advance". — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Erwin-st (scalable throughput). The client writes **data directly to the shards and only metadata to the sequencing layer, in parallel, in 1 RTT**. Background ordering sequences the metadata. If metadata reaches a shard before the data, the shard primary waits for a timeout to receive the record from the client. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Reported numbers. The hardware was CloudLab xl170 (10-core E5-2640v4, 64 GB, 25 Gb ConnectX-4, SATA SSD), with eRPC and RDMA used for garbage collection (Scalog uses gRPC, a noted confound). The paper reports ~4× lower append latency than their Corfu at equal throughput and "two orders of magnitude" lower than open-source Scalog. Erwin-bb reaches ~1M appends/s with 100 B records and ~700K/s with 4 KB records on 10 shards, then flattens with large records. The sequencing layer reaches **1.34M metadata-appends/s**. Erwin-st has **29 µs latency at 700K appends/s**. Reads that lag appends by about 3 ms see no extra latency (read latency ≈ Corfu's). When readers read immediately with no lag, latency rises but stays close to eager systems (~70 µs for Corfu, Fig. 9). A single reader keeps up to about 37K reads/s. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Failure. When a sequencing replica crashes, operations are affected for about **15 ms** and then resume. Most of that time is failure detection plus writing the new view through **ZooKeeper**, and the authors say a faster configuration service could cut reconfiguration to about **1 ms**. A single active controller is ensured by ZooKeeper. Writing to *all* sequencing replicas inflates tail latency when a replica straggles. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Size. Erwin is about 8K LOC of C++. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)

**SpecLog / Belfast (OSDI 2025, same UIUC group)**
- Problem. In Scalog-style durability-first logs, *delivery* to consumers waits for the cut. SpecLog delivers records with a *speculated* global position so that application compute overlaps with coordination, and confirms the position later. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf)
- Fix-ante ordering. The sequencing layer **predetermines** a sequence of cuts, giving each shard a *quota* per report. Each shard makes durable and reports **exactly** its quota: it pads with **no-op** records when short and defers extra records to the next report. Because every shard knows every other shard's quota, it can compute its records' global positions before coordination. The paper notes this "does not mean order is established before durability". Misspeculation happens only if an entire shard fails or no replica of a shard can reach the sequencer. Appends are still not acked before coordination, since acking earlier would violate linearizability. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf)
- Belfast sets quotas from measured ingestion rates. It changes cuts and quotas, and adds or removes shards, only at the boundaries of a "speculation lease window" so that all shards switch together. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf)
- Reported numbers. Records are delivered about 3× earlier than in Scalog, and e2e latency is **1.4× to 1.66× lower than Scalog** (2 and 10 shards, 1.2 ms compute). It scales throughput with shards like Scalog. A naive variant without quota control loaded the sequencer about 2× more. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf); [OSDI'25 page](https://www.usenix.org/conference/osdi25/presentation/bhat)

**Corfu (NSDI 2012) and Tango (SOSP 2013)**
- Corfu. The client obtains a token (a log position) from an off-path sequencer, then writes the payload directly to the flash units that own that address. The order is therefore assigned **before** the write. — [Corfu](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf); description also in [FuzzyLog](https://www.usenix.org/system/files/osdi18-lockerman.pdf)
- Sequencer throughput. The user-space sequencer ran at **200K tokens/s** in Corfu. In Tango the new sequencer plateaus at **~570K requests/s** without batching (32-core machine in a separate rack) and reaches **>2M/s with batch size 4**, at a cost in append latency. — [Corfu](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf); [Tango](https://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf)
- Holes. A client that takes a token and then crashes or stalls leaves a hole, and strict-order consumers "can make no progress until the status of the hole is resolved". Any client may *fill* the position with junk, which takes under 1 ms. A late writer then races with the filler. Tango fills holes after a tunable **100 ms** timeout by default. — [Corfu](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf); [Tango](https://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf)
- Failure and reconfiguration. A Vertical-Paxos-style protocol seals the current *projection* at a subset of units and writes the next projection to an auxiliary. Corfu recovers from a crashed drive in about **30 ms**. The sequencer identity lives in the projection, and the new sequencer's counter is computed from the highest offsets returned by the seal. Tango replaced a failed sequencer **within 10 ms** in an 18-node deployment. Appends are blocked during that window because they need tokens. — [Corfu](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf); [Tango](https://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf)
- vCorfu adds per-stream *materialized stream replicas* so that streams are dense and free of holes. Its authors say backpointers make sequencer recovery require "the entire log [to] be read", and that a hole can force a linear scan (about 0.3 ms per entry). vCorfu detects a failure and reconfigures into degraded mode in about 1 ms. — [vCorfu](https://www.usenix.org/system/files/conference/nsdi17/nsdi17-wei-michael.pdf). The Scalog authors say vCorfu "is at risk of losing data" under certain failure combinations and pays a ~40% single-shard throughput penalty for its commit bit (9.3K vs 13.6K writes/s). — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)

**Delos / VirtualLog / Loglets (OSDI 2020, Meta)**
- Virtual consensus. A VirtualLog is a chain of Loglets, and only the last one is appendable. A Loglet does **not** need fault-tolerant consensus, only a fault-tolerant `seal` and a `checkTail`. Reconfiguration has three steps: seal the active Loglet, call checkTail, and write the new chain to a MetaStore. Leader election and reconfiguration are a reusable layer above the Loglets. — [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)
- NativeLoglet. One server runs a sequencer that assigns a position to each command and forwards it to all LogServers. A write commits at a majority, and the sequencer retries until success or seal. It is "unavailable for appends if the sequencer fails" but can be sealed while a majority is alive. It "took just under 4 months to implement and deploy". Migrating online from ZKLoglet to NativeLoglet gave a 10× end-to-end latency improvement in production. — [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)
- StripedLoglet composes Loglets into rotating sequencers or sharded acceptors. A 9-node NativeLoglet bottlenecks on its single sequencer, and striping removes that. A disaggregated Loglet scaled to 150K ops/s under a 15 ms p99 SLA, against 15K for the converged NativeLoglet on shared-SSD production hardware. — [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)

**Boki (SOSP 2021)**
- The *metalog* is a small log of ordering decisions and state transitions per physical log. It gives ordering (by appending metalog entries, using **Scalog's ordering protocol**), read consistency (functions track metalog positions and indices are updated independently), and fault tolerance (by sealing the metalog, using **Delos's sealing protocol**). The metalog is kept by n=3 meta-sequencers under a **primary-driven** protocol: the primary appends and replicates to secondaries, and an entry is appended once a quorum acks. Trim is a metalog command. — [Boki](https://par.nsf.gov/servlets/purl/10357172)
- Reported numbers. The paper reports **1.2M appends/s in one LogBook with 6.4 ms p99 latency**. BokiQueue reaches 1.23× the throughput of Pulsar with up to 2× lower latency. — [Boki](https://par.nsf.gov/servlets/purl/10357172). Code: [ut-osa/boki](https://github.com/ut-osa/boki)

**FuzzyLog (OSDI 2018)**
- FuzzyLog replaces the total order with a **partial order**: there is a per-color chain, and a cross-color append orders only the colors involved. The authors argue that a centralized sequencer cannot keep up with modern network bandwidth. A 600K ops/s sequencer suffices for 20 servers on 1 Gbps, but 40 Gbps would need about 24M ops/s, or about 1B ops/s with 100 B payloads. They cite published sequencer rates of ~200K (Corfu), ~250K (NOPaxos) and ~600K (Tango), and a standalone RDMA counter at 122M ops/s. — [FuzzyLog](https://www.usenix.org/system/files/osdi18-lockerman.pdf)
- Reported numbers. The Dapple implementation reaches 3M/s single-key writes and 150K/s cross-shard renames for a ZooKeeper clone on 6 nodes, and an AtomicMap reaches 5.5M puts/s on 16 servers. Emulated Tango stops scaling beyond four clients because of its sequencer. Experiments ran on EC2. — [FuzzyLog](https://www.usenix.org/system/files/osdi18-lockerman.pdf)

**Calvin (SIGMOD 2012)**
- Calvin divides time into **10 ms epochs**. Each node's sequencer batches the requests it receives in an epoch and replicates the batch, either asynchronously or with Paxos agreeing on a combined batch per epoch. It sends the batch to the schedulers, and the global order is a deterministic interleaving of the epoch's batches by (epoch, sequencer ID). Calvin documents the complexity of failing over a master sequencer under async replication. — [Calvin](https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf). Scalog describes Calvin as interleaving batches in round-robin order. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)

### Inferences
- **Precog already is Scalog's data layer.** Each precog node's accepted appends form a FIFO "log segment" that is replicated to peers, which is exactly Scalog's per-server segment. The cut can therefore be indexed per *origin node* (a vector of length 3), not per writer log. That keeps cut size and Raft traffic tiny regardless of how many (stream, writer, epoch) logs exist. Inference.
- **One important difference: precog acks before order; Scalog acks after the cut.** Precog's semantics already match LazyLog's "durable now, bound to position later". Scalog's ordering layer can still be used to bind positions lazily, with readers waiting for the cut. Inference.
- **Real-time and per-writer order hazard.** Suppose a precog writer's seq N is acked by node A and seq N+1 is then sent to node B. A per-node-segment interleaving could place N+1 in an earlier cut than N, breaking both per-writer FIFO and linearizability (append-after-ack ordered earlier). Scalog avoids this by acking only after the cut. LazyLog avoids it by having the client write to all sequencing replicas. Precog would need one of these fixes:
  - (a) Sort each writer log's entries within and across cuts by (epoch, seq), for example by deferring an entry until its predecessor is in a cut.
  - (b) Pin a writer to one node per epoch.
  - (c) Accept that total order is not real-time order and state that explicitly.

  Inference; worth verifying against precog's writer-epoch rules.
- **Precog's 2-of-3 ack (with each node reporting its durable frontier) maps onto Scalog's element-wise min.** With 3 nodes and f=1, "durable" should mean at least 2 copies, i.e. the 2nd-highest reported frontier for a segment, not the min over all replicas. Inference.

### Gaps
- Delos log-structured protocols (SOSP'21), Hyder (CIDR'11) and Chariots (EDBT'15) were not read from primary sources in this pass. The Chariots URL I fetched returned a different paper. No claims are made about them here.
- No primary source found for Delos reconfiguration time in milliseconds, or for Boki's reconfiguration time or append latency breakdown. The Boki PDF text was partly unextractable.
- Borges (CXL memory/SSD shared log, ACM 2025) was found only as a search-result snippet that positions it against LazyLog and SpecLog. Its full text was 403-blocked: https://dl.acm.org/doi/pdf/10.1145/3830418.3843906

---

## Q2. Which designs order only metadata (counts, frontiers, index) rather than data, and what does that cost in ordered-read latency?

### Takeaway
Scalog, Boki, Belfast and Erwin-st order only metadata: counts or cuts, metalog entries, or record IDs. Their ordered-read latency equals the time to the next committed cut or `stable-gp`. In Scalog that is about 1 to 2 ms end-to-end at a 0.1 ms interval. LazyLog shows that readers lagging by about 3 ms pay nothing, and SpecLog cuts consumer-visible delay further by speculation.

### Cited Findings
- Scalog's ordering layer sees only per-server counts. Readers compute positions from the latest cut, and a record is readable in order once its cut arrives. Append (= ordered) latency is about 1.3 to 1.6 ms. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Erwin-st writes only metadata to the sequencing layer, which reaches 1.34M metadata-appends/s. Reads that lag by about 3 ms match Corfu's read latency, and reads past `stable-gp` wait. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Boki's metalog orders records, and the data stays on storage nodes. Its p99 at 1.2M appends/s is 6.4 ms. — [Boki](https://par.nsf.gov/servlets/purl/10357172)
- Belfast delivers about 3× earlier than Scalog through speculated positions, giving 1.4 to 1.66× lower e2e latency. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf)
- Erwin-bb, Corfu's sequencer and the Delos NativeLoglet sequencer handle tokens or data per record. Erwin-bb flattens with large records because the data passes through the sequencing layer. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf); [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)

### Inferences
- For precog with T = 1 to 10 ms (the Scalog default was 0.1 ms; Calvin used 10 ms), the expected added ordered-read latency is about T/2 + one Raft commit + one fan-out, roughly 1 to 12 ms on a LAN. This is my estimate, not measured.

### Gaps
- No paper reports metadata-only ordering at a T of several milliseconds with a 3-node Raft group, which would be precog's exact configuration.

---

## Q3. Which designs let writes be durable and acknowledged before order is assigned, and how do they serve readers that need order?

### Takeaway
Only LazyLog/Erwin acks before order is bound, and it is the closest model to precog. Readers read only up to `stable-gp`; beyond it they wait, or the tail is served via the leader. Scalog, Boki and Belfast are durability-first but ack only after the cut. Corfu, Tango and Delos assign order before the write.

### Cited Findings
- LazyLog acks after durability on all sequencing replicas and binds position in the background. The order is exposed only up to `stable-gp`, which "will remain unchanged regardless of future failures". Reads beyond it take a slow path, and checkTail is served by the sequencing leader. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Scalog servers keep accepting and storing records while the ordering layer is down, but the client ACK comes after the global cut. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- SpecLog states that acking before coordination would violate linearizability, so Belfast's acks still wait for the cut. Speculation helps delivery to consumers, not the ack. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf)

### Inferences
- Precog can keep its 2-of-3 ack unchanged and add a "global-order frontier" that ordered readers block on. This is LazyLog's contract built with Scalog's mechanism. Unordered (per-writer) readers would be unaffected. Inference.
- LazyLog gets linearizable order without consensus because every append reaches *every* sequencing replica before the ack. Precog acks at 2 of 3, so concurrent-versus-sequential appends are not visible identically on all replicas. Precog would therefore need the cut or Raft step, or a per-writer FIFO rule, to recover real-time order. Inference.

### Gaps
- None of the papers evaluates an any-node-accept, 2-of-3 ack mesh like precog directly.

---

## Q4. Measured ordering ceilings and latencies, and what happens on orderer failure

### Takeaway
Token sequencers top out at about 0.2 to 0.6M/s (more than 2M/s with batching), and a sequencer failure blocks appends for about 10 to 30 ms. Cut-based ordering scales with shards: 255K/s measured and about 52M/s emulated. An orderer failure there blocks only ordering and acks, never ingestion, and throughput catches up afterwards. LazyLog's sequencing-replica failure costs about 15 ms, with about 1 ms said to be possible.

### Cited Findings
| System | Ordering ceiling (reported) | Latency (reported) | Orderer failure (reported) | Source |
|---|---|---|---|---|
| Corfu | 200K tokens/s | sub-1 ms appends (FPGA flash) | sequencer replaced via reconfiguration; drive recovery ~30 ms | [Corfu](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final30.pdf) |
| Tango | ~570K/s unbatched; >2M/s batch 4 | – | failed sequencer replaced in ≤10 ms (18 nodes); appends need tokens | [Tango](https://www.cs.cornell.edu/~taozou/sosp13/tangososp.pdf) |
| Scalog | 255K/s real (17 shards); ~52M/s emulated | ~1.3 to 1.6 ms | ingest continues; commit pauses until Paxos re-election; no net throughput loss | [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf) |
| Erwin-bb / Erwin-st | ~1M/s (100 B) / 1.34M metadata-appends/s | 29 µs at 700K/s (st) | ~15 ms disruption (ZooKeeper-bound) | [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf) |
| Boki | 1.2M appends/s per LogBook | p99 6.4 ms | reconfigure by sealing metalog | [Boki](https://par.nsf.gov/servlets/purl/10357172) |
| Belfast | scales like Scalog | 1.4 to 1.66× lower e2e than Scalog | misspeculation only on whole-shard failure | [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf) |
| Delos NativeLoglet | single-sequencer bound; striping removes it | 10× lower than ZKLoglet in prod | appends unavailable until seal + new Loglet | [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf) |

### Inferences
- Scalog's property that an orderer failure blocks only ordering and acks, never ingestion, matches precog's goal of no leader on the data path. Precog does better still, because it acks before the cut, so a Raft outage delays only ordered reads. Inference.

### Gaps
- Scalog's paper gives Paxos-leader recovery only as a plot (Fig. 4, seconds scale). No exact millisecond figure was extracted.

---

## Q5. Which design is simplest to graft onto an existing leaderless durable-write layer?

### Takeaway
The simplest graft is Scalog-style cuts over **per-origin-node frontiers**, with positions bound lazily and ordered reads gated on the cut (LazyLog semantics). Boki shows the same idea packaged as a "metalog" with Delos-style sealing for reconfiguration. Token sequencers (Corfu, Delos NativeLoglet) and Erwin-bb would require changing precog's write path.

### Cited Findings
- Scalog's ordering layer needs only local cuts from servers. The ordering logic is a deterministic function of the cut, and aggregators are stateless. — [Scalog](https://www.usenix.org/system/files/nsdi20-paper-ding.pdf)
- Boki builds ordering by reusing Scalog's protocol and fault tolerance by reusing Delos's seal on a 3-replica primary-driven metalog. — [Boki](https://par.nsf.gov/servlets/purl/10357172)
- Delos says a sealable, non-consensus Loglet is "much easier to implement than fault-tolerant consensus" and took about 4 months to reach production. — [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)
- Erwin-bb puts the sequencing layer in the write path, so it could be bolted onto Kafka, but every client writes to all f+1 sequencing replicas. — [LazyLog](https://dassl-uiuc.github.io/pdfs/papers/lazylog.pdf)
- Belfast's fix-ante quotas add no-op padding and lease windows on top of Scalog. — [SpecLog](https://dassl-uiuc.github.io/pdfs/papers/speclog.pdf)

### Inferences
- Recommended sketch, all inference:
  1. Each node reports its origin-segment durable count, which is its group-commit WAL position for records it originated, every T ms.
  2. A 3-node Raft group commits the cut vector ⟨c_A, c_B, c_C⟩ where each c_X is computed from the replica acks, and uses the cut stream as a Boki-style metalog that also carries epoch and membership changes.
  3. Global order is cut by cut, then by node ID, then by offset within each segment, plus the per-writer FIFO fix-up from Q1.

  One Raft entry per T ms is trivial load: 100 to 1,000 commits/s. The main costs are added ordered-read latency of about T/2 plus a Raft RTT, and the per-writer ordering rule.
- One simplification beyond Scalog would be to drop Raft entirely and use a Calvin-style fixed epoch with deterministic merge. Without agreement, though, it cannot decide when a straggling or partitioned node's epoch is complete. That is exactly what Scalog's consensus on cuts provides. Inference.
- SpecLog-style quotas could later cut consumer-visible latency, but they add no-op records and lease windows, so they are not a first step. Inference.

### Gaps
- No public numbers exist for a Scalog-style ordering layer with T in the 1 to 10 ms range on 3 nodes. Precog would need to benchmark this itself.
- The availability of the open-source Scalog code (github.com/scalog) and its maintenance status were not verified in this pass.
