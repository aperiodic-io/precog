# Production systems: how streaming/log systems assign order and offsets ("order metadata, not data", centralized sequencers)

Scope note: the sources were fetched on 2026-11-17. Items marked **[not re-verified]** are well-established background that I did not fetch this session. The report writer should treat them as lower-confidence or re-check them before quoting. Vendor claims are labelled **(vendor)**, and independent sources are labelled **(independent)**.

## Q1. Which production systems write durable data first and assign order later through a metadata commit, and what latency and ceiling do they report?

### Takeaway
This pattern is now the mainstream design for object-storage Kafka. WarpStream, Bufstream, Ursa (StreamNative) and Apache Kafka's proposed Diskless topics (KIP-1150/1163/1164) all write data to durable storage first. A central metadata service then assigns offsets per partition before the producer gets an ack. S2 does the same at the per-stream level. The ordering commit typically costs about 10–50 ms on top of the data write. Overall produce latency is dominated by buffering and object-storage PUTs: P99 is about 400 ms on S3 Standard and about 150–170 ms on S3 Express One Zone. Precog's local-WAL plus peer replication would make the metadata commit the dominant cost, which is the same lesson WarpStream reports.

### Cited Findings

**WarpStream (vendor docs and blogs)**
- Agents are stateless and diskless. They write "a few files per second" to object storage, and each file can hold records from many topics and partitions. Background agents compact small files later. — [WarpStream architecture docs](https://docs.warpstream.com/warpstream/overview/architecture)
- The "Virtual Cluster" is the metadata store. It maps object-storage files to offset ranges per topic-partition. Every metadata operation is journaled to strongly consistent log storage before it is executed and acknowledged. Kafka API operations within a Virtual Cluster are atomic, including produces that span multiple topics or partitions. — [WarpStream architecture docs](https://docs.warpstream.com/warpstream/overview/architecture)
- The classic produce path is: buffer → write one file to object storage → commit the file metadata to the Control Plane → ack with offsets. The commit is on the critical path because agents are "stateless and leaderless", so the Control Plane must sequence records to assign deterministic offsets. — [WarpStream, "The Art of Being Lazy(log)"](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing)
- The metadata store is "a custom metadata database that was written from the ground up". An ack happens only after data is "durably persisted in S3 and committed to our cloud control plane". Default P99 produce is about 400 ms, and producer-to-consumer P99 is about 1 s. — [WarpStream, "Kafka is dead, long live Kafka"](https://www.warpstream.com/blog/kafka-is-dead-long-live-kafka)
- In a low-latency configuration (50 ms batch, S3 Express One Zone), the budget is about 30 ms buffering, about 20 ms writing to a quorum of S3EOZ buckets, and about 50 ms for the Control Plane commit. The Control Plane "also batches the requests for a short amount of time". With fast storage and a low batch timeout, "the Control Plane latency dominates". — [WarpStream Lazy(log) post](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing)
- With S3EOZ, WarpStream reports a median of 105 ms and a P99 of about 170 ms (169 ms in its 2025 benchmark). With Lightning Topics plus S3EOZ, it reports a 33 ms median and a P99 of about 50 ms in a small OpenMessaging Benchmark run (vendor). — [WarpStream Lazy(log) post](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing); [WarpStream low-latency clusters docs](https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/low-latency-clusters)
- Combining a reduced batch timeout with Rapid Buckets can bring produce P99 below 150 ms (vendor docs). — summarized from [WarpStream low-latency clusters docs](https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/low-latency-clusters) via search snippet

**Bufstream (Buf; acquired by CoreWeave)**
- Bufstream uses object storage for data and a metadata store (Postgres, Google Cloud Spanner, or etcd) for coordination, with no ZooKeeper or KRaft. — [Bufstream docs](https://buf.build/docs/bufstream); [Bufstream architecture](https://buf.build/docs/bufstream/architecture)
- Produce requests are combined into "intake" files. Each partition in an intake file needs its own metadata entry, so metadata grows without bound if archiving lags. Etcd's 8 GiB ceiling is why stale intake files get "shelved". Archiving later rewrites intake data per partition into Kafka-like logs or Parquet/Iceberg. — [Bufstream docs](https://buf.build/docs/bufstream/architecture) (search snippet)
- Jepsen describes the agents as stateless and the coordinator (etcd at the time) as the component that "helps agents establish which storage chunks are committed and what the order of records should be". Offsets are per partition, monotonically increasing and possibly sparse, because some offsets carry transaction metadata. Jepsen does not say exactly which component computes offsets. — [Jepsen, Bufstream 0.1.0](https://jepsen.io/analyses/bufstream-0.1.0) (independent)
- Benchmark (vendor): OpenMessaging, one topic with 288 partitions, 1 GiB/s of symmetric reads and writes, and a 3-node etcd cluster on m6in.large. End-to-end median was 260 ms and P99 was 500 ms, at a cost of $11,147 per month with 7-day retention. — [Bufstream docs](https://buf.build/docs/bufstream/cost) (search snippet)
- Spanner-backed sizing (vendor): under $1M per month for 100 GiB/s of writes and 300 GiB/s of reads. No latency figures were given. — [Buf, Bufstream on Spanner](https://buf.build/blog/bufstream-on-spanner)
- Tuning guidance: lowering the flush interval reduces produce latency but increases object-storage write cost. — [Bufstream tuning performance](https://buf.build/docs/bufstream/deployment/tuning-performance)

**Apache Kafka Diskless (KIP-1150 / KIP-1163 / KIP-1164)**
- KIP-1163 sets out the order of operations. Brokers upload shared WAL objects without coordinating with each other. They then commit the batch coordinates to the Diskless/Batch Coordinator, which "assigns offsets to the written batches, persists the batch coordinates, and responds". Offsets are assigned after the upload. Replicas inject the offsets into the batch data at fetch time. — [KIP-1163](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core)
- Batch data has no inherent order, and each broker's coordinates are ordered only relative to that broker. The coordinator "establishes a global order of batches" per partition. A broker may upload several objects in parallel but must commit them in sequence. — [KIP-1163](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core)
- The KIP-1163 latency targets are design targets, not measurements:
  - Buffering: up to 250 ms or 4 MiB (`diskless.append.commit.interval.ms`, `diskless.append.buffer.max.bytes`).
  - Upload: P50 about 100 ms, P99 200–400 ms.
  - Coordinator commit: P50 about 10 ms, P99 20–50 ms, "depending on the number of batches".
  - End-to-end produce: P50 about 500 ms, P99 1–2 s.
  — [KIP-1163](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core)
- KIP-1164 implementation:
  - Coordinator state lives in an internal topic, `__diskless_metadata`, with multiple partitions. Each partition leader acts as a coordinator, found through `FindCoordinator`. A user partition is bound to one coordinator when it is created.
  - Each coordinator materializes state into a local SQLite database, which is "a local metadata cache, not the source of truth" and is expected to reach hundreds of MB to GB. Snapshots follow KRaft (KIP-630).
  — [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)
- A `DisklessCommitFile` request carries an array of batches, and per-batch results come back in order. The coordinator validates the request against committed and pending state, assigns `AssignedBaseOffset`/`AssignedLastOffset`, and appends metadata records. It responds only after those records are replicated to the ISR (the high watermark advances) and applied locally. — [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)
- Because one WAL file can contain partitions owned by several coordinators, a broker may make up to `n_dcs` commit calls per file. "Produce gateway" brokers per rack (`max(1, n_brokers / n_dcs / n_racks)`) bound this fan-out. — [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)
- Status: KIP-1150 was accepted (2026-03-02, per a third-party blog). KIP-1164 is still "Under Discussion", with the page last updated 2026-09-30, and its performance testing is listed as planned with defaults marked TBD. — [youngju.dev blog](https://www.youngju.dev/blog/2026-07-16-kafka-diskless-topics-kip-1150.en) (secondary); [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)

**Ursa (StreamNative; VLDB 2025 Best Industry Paper)**
- Brokers are "stateless and leaderless" and delegate "data persistence and offset sequencing to the metadata service and the storage layer". Broker failures do not trigger leader elections. — [Ursa, PVLDB 18(12)](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf)
- Write protocol:
  1. Append the entry to the WAL. The WAL is an object-storage object for the cost-optimized profile, or a BookKeeper blob for the latency-optimized profile.
  2. Run an Oxia transaction per stream: `last = tx.get_last_key(stream_id)`, then put the key `(stream_id, last.Offset_end + MsgCnt, last.CumulativeSize + Size)` with a pointer to the WAL object.
  The metadata service needs either an "atomic increment" primitive or transactions. Orphaned WAL objects (data written, metadata commit failed) are cleaned up by compaction. — [Ursa paper](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf)
- Oxia is "the single authoritative source of offsets and transaction state". Its shards are Raft-style leader plus quorum-replicated. — [Ursa paper](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf)
- Measured (vendor paper): P99 publish latency stayed under 1 s at a 1 GB/s workload across fan-out ratios of 1:1 to 1:5, with P999 rising at high fan-out. Throughput was about 5 GB/s regardless of partition count. The pluggable latency-optimized WAL targets P99 "from the single-digit to the low-hundreds of milliseconds". — [Ursa paper](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf)
- StreamNative says metadata traffic is "significantly smaller than the actual data movement, making it negligible" (vendor). — [StreamNative blog](https://streamnative.io/blog/leaderless-architecture-and-lakehouse-native-storage-for-reducing-kafka-cost)

**S2 (s2.dev)**
- Architecture:
  - `channel` pods "sequence writes through a WAL of chunks" and later batch records into per-stream segments.
  - Chunks go to S3 Standard (the Standard class) or to a quorum of three S3 Express zonal buckets (the Express class).
  - The stream metadata store is "not in the write hot path".
  - `cop` (the channel operator) migrates streams between channels using a "speculation-based protocol".
  - Chunks flush on "a combination of elapsed time and accumulated size". No numbers are given.
  — [S2 architecture docs](https://s2.dev/docs/platform/architecture)
- "Every write to S2 is durable on object storage before it is acknowledged." — [S2 durability & consistency](https://s2.dev/docs/concepts/durability-consistency)
- Each stream gets dense sequence numbers. An append ack returns a range such as `seq 0..3`. A batch is atomic: either all of it becomes durable or none of it does. S2 provides `match_seq_num` (compare-and-swap) and `fence` tokens. S2 tests linearizability with Porcupine and deterministic simulation testing. — [S2, linearizability blog](https://s2.dev/blog/linearizability)
- S2 is the closest analogue to precog's per-writer logs. Order is per stream and assigned by the single channel that owns the stream at the time, not by a global metadata commit. — [S2 architecture docs](https://s2.dev/docs/platform/architecture)

### Inferences
- In every production system above, the ordering decision is a per-partition or per-stream offset-range allocation made after durability. No system orders a merged cross-producer stream after the fact. They all serialize producers into a partition at commit time. Scalog-style cuts over many independent per-writer logs, as precog proposes, have no direct production Kafka-alike precedent in these sources. The closest relatives are Ursa's per-stream Oxia transaction and the KIP-1163/1164 batch coordinator.
- With a fast durable tier (precog's local group-commit WAL plus peer replication, or S3EOZ), WarpStream's data shows the metadata commit and its batching becoming the dominant latency term, at about 50 ms of about 100 ms total. A cut interval T in precog's design adds roughly T/2 on average and T at worst to time-to-order, on top of the Raft commit.

### Gaps
- I found no published independent (non-vendor) latency measurement for WarpStream, Ursa, or Kafka Diskless.
- None of these systems publishes the throughput ceiling of its metadata store, such as commits per second per coordinator.
- I did not fetch the S2 per-stream throughput limits or its append latency figures.
- I did not research AutoMQ in this session. My understanding is that AutoMQ keeps Kafka's partition-leader model with a WAL on EBS or S3 and uses KRaft for metadata only, so the leader assigns offsets **[not re-verified]**.

## Q2. How large are the ordering batches, and what is the tail-latency cost?

### Takeaway
The de facto standard is a 250 ms or 4 MB buffer before the data write (WarpStream default, KIP-1163 default). After that, a metadata commit batches many files or batches into one request. WarpStream can go as low as 25 ms. Batching inside the metadata store is short and not publicly quantified.

### Cited Findings
- WarpStream agents use a default batch timeout of 250 ms or 4 MB, configurable down to 25 ms. The Control Plane additionally batches commit requests for "a short amount of time". — [WarpStream Lazy(log) post](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing)
- WarpStream's architecture page says a file per partition every 100 ms would cost about $130 per month in S3 PUTs per partition. This is why files mix many partitions, and why one file yields many per-partition commits. — [WarpStream architecture docs](https://docs.warpstream.com/warpstream/overview/architecture)
- KIP-1163 buffers for 250 ms or 4 MiB. Its commit P99 target of 20–50 ms depends on "the number of batches" in the request. — [KIP-1163](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core)
- In KIP-1164, a single `DisklessCommitFile` request carries an array of batches for every partition that coordinator owns in the file. — [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)
- Bufstream lets you lower the flush interval to cut produce latency, at the cost of more object writes. — [Bufstream tuning](https://buf.build/docs/bufstream/deployment/tuning-performance)
- FoundationDB:
  - Proxies group many client transactions into one batch and request a single commit version from the singleton Sequencer, which lets proxies commit "tens of thousands of transactions per second".
  - Read-version requests are batched over a window of about 1 ms in one description.
  - This comes from secondary summaries of the SIGMOD 2021 paper. — [Murat Demirbas on FDB paper](https://muratbuffalo.blogspot.com/2022/03/foundationdb-distributed-unbundled.html); [FDB read/write path](https://apple.github.io/foundationdb/read-write-path.html); [xinglin notes](https://xinglin.github.io/fdb/)
- Scalog reports totally ordering up to 52 million records per second (4 KB records, 10 Gbps, SSDs) by ordering shard frontier cuts rather than records. — [Scalog, NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/ding)

### Inferences
- With durable writes in about 5–20 ms, the closest analogue to precog's per-node frontier reporting is WarpStream's low-latency numbers: 25–50 ms batch plus about 50 ms commit. A T of 5–20 ms plus a Raft commit of a few ms in-region could plausibly beat those numbers, because precog orders only frontier vectors (O(nodes × active logs)), not per-file batch descriptors.
- The FDB and Scalog precedent says a single sequencer is not the throughput ceiling once requests are batched. The ceiling is the size of the metadata payload per commit.

### Gaps
- I did not obtain primary-source numbers from the FoundationDB SIGMOD 2021 paper (the PDF was not fetched) for sequencer request rates or the batch window.
- I did not fetch Corfu's sequencer throughput numbers. The Corfu NSDI 2012 paper reports a sequencer of roughly 200K tokens per second, and Tango/vCorfu later reported higher rates **[not re-verified]**.
- I did not obtain the Scalog cut interval or its latency from the full paper.

## Q3. What happens on coordinator or metadata-store failure? Do producer writes stall?

### Takeaway
By default, yes. In every system that orders through a metadata commit, produce stalls or fails while the metadata store is unavailable, because the ack waits on the commit. WarpStream is the only system with a production escape hatch: Lightning Topics plus Ripcord ack after durability alone and sequence later, which gives up offsets, idempotence, transactions and external consistency. Consumers stall either way.

### Cited Findings
- By default, a WarpStream agent that loses its Control Plane connection rejects Produce requests. Ripcord mode keeps accepting produces during Control Plane outages. — [WarpStream Ripcord docs](https://docs.warpstream.com/warpstream/kafka/advanced-agent-deployment-options/ripcord)
- Ripcord treats all topics as Lightning Topics. Agents fall back to object storage for service discovery, and caches favour availability.
- In a roughly one-hour disruption test, produce continued and the backlog drained after reconnection.
- During an outage, these remain blocked: all consumption, topic management, consumer-group management, and starting or restarting agents (their caches warm only from the Control Plane). — [WarpStream Lazy(log) post](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing)
- Lightning Topics recovery (slow path): if the async commit fails or the agent crashes after the ack, a Control Plane scheduler assigns scan jobs. Agents list uncommitted files in a "sequence" (a folder of about 1,000 consecutive files from one agent), download each file's metadata section, and commit them. WarpStream says all journaled data is eventually sequenced. — [WarpStream Lazy(log) post](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing)
- Kafka Diskless (KIP-1163): if the upload succeeds but the coordinator doesn't respond, the produce returns an error. A retry is safe only with idempotent produce. A garbage object is left for asynchronous cleanup, because the coordinator "may not be available". — [KIP-1163](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core)
- Kafka Diskless (KIP-1164): a non-leader returns `NOT_COORDINATOR`. The new leader has already read the same log and resumes from its snapshot plus the log. Followers can serve possibly stale read-only `DisklessFindBatches`. The KIP does not state a write-stall duration. — [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)
- Ursa (vendor paper):
  - Broker failure is detected by heartbeat loss after about 5 s, and its partitions are remapped with zero copy. A brown-out (latency above P99 + 3σ for 30 s) evicts the node.
  - When an Oxia shard leader fails, a caught-up follower is promoted.
  - Clients see "typically one retry RTT (<100ms when the TCP connection closes)", or up to about 5 s in a silent failure.
  - Ursa claims no acked-record loss, no duplicates, and that linearizability is preserved.
  — [Ursa paper](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf)
- Bufstream (independent, Jepsen 2024):
  - etcd pauses or partitions caused metastable failures: `InitProducerId` timeouts, `listOffsets` failures, and empty polls. Restarting the nodes fixed them.
  - Lease-tied keys were deleted without the agents being reliably told.
  - Agents "kill themselves" on storage or coordinator loss and rely on Kubernetes to restart them. Jepsen recommended they retry instead.
  — [Jepsen, Bufstream 0.1.0](https://jepsen.io/analyses/bufstream-0.1.0)
- FoundationDB: recovery (detect, shut down the transaction system, recover with a new sequencer generation) is "usually less than five seconds" in production. — [CACM, FoundationDB](https://cacmb4.acm.org/magazines/2023/6/273229-foundationdb-a-distributed-key-value-store) (via search snippet)
- Scalog claims reconfiguration "with no loss in availability". — [Scalog NSDI 2020](https://www.usenix.org/conference/nsdi20/presentation/ding)

### Inferences
- Precog's design already acks at 2 of 3 durable copies without the ordering layer. That makes it a Lightning-Topics/LazyLog-style system by construction: writes keep flowing when the cut Raft group is down, and only ordered reads stall. This is exactly the availability property WarpStream had to bolt on. WarpStream's lesson is that offsets returned at ack time, idempotent-producer semantics and external consistency must then come from the per-writer `(writer, epoch, seq)` key, not from the global order.

### Gaps
- I found no published Control Plane outage recovery times for WarpStream or Bufstream.

## Q4. What guarantee do they give across producers (total order per partition), and is it linearizable?

### Takeaway
All of these systems give a total order per partition or stream across all producers, and order across partitions only in limited ways (WarpStream offers atomic multi-partition operations). Ursa and S2 explicitly claim linearizable appends. The exception is WarpStream's Lightning Topics, which drops external consistency: a later-acked record can get a lower offset.

### Cited Findings
- KIP-1163: the coordinator establishes a global order of batches per partition, and replicas see identical offsets. — [KIP-1163](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1163%3A+Diskless+Core)
- Ursa claims "Linearizable writes" (Oxia per-shard total order), read-your-writes (brokers stamp cache entries with the Oxia revision), and durable commits (the index is committed only after the WAL write succeeds). — [Ursa paper](https://www.vldb.org/pvldb/vol18/p5184-guo.pdf)
- S2's stream API must be linearizable: an acknowledged append is visible to later appends, reads and check-tails, and S2 checks this with Porcupine. An indefinitely failed append leaves an indeterminate outcome, resolved by retrying with the same `match_seq_num`. — [S2 linearizability blog](https://s2.dev/blog/linearizability)
- In WarpStream, a Kafka API operation within a Virtual Cluster is atomic, including produces across multiple topics and partitions. — [WarpStream architecture docs](https://docs.warpstream.com/warpstream/overview/architecture)
- Lightning Topics return offset `0` in acks and reject idempotent and transactional producers. "A record acknowledged later can end up with a lower offset than an earlier one" if the earlier one went through the slow path. — [WarpStream Lazy(log) post](https://www.warpstream.com/blog/the-art-of-being-lazy-log-lower-latency-and-higher-availability-with-delayed-sequencing)
- Kafka idempotent producer: the KIP-1164 commit carries `ProducerId`, `ProducerEpoch`, `BaseSequence` and `LastSequence` per batch, and the coordinator returns `IsDuplicate`. Dedup therefore moves from the partition leader to the coordinator. — [KIP-1164](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1164:+Diskless+Coordinator)
- Classic Kafka idempotence requires `max.in.flight.requests.per.connection` ≤ 5. The partition leader keeps state for the last 5 batches per producer ID and rejects out-of-order sequences **[not re-verified]**. — [Kafka producer configs](https://kafka.apache.org/documentation/#producerconfigs_enable.idempotence)
- Pulsar and BookKeeper: a single owner broker per topic is the only writer of the current ledger and assigns entry IDs sequentially. Ordering therefore comes from single-writer ownership, not a metadata commit, and ZooKeeper or Oxia stores ledger lists only **[not re-verified]**. — [BookKeeper concepts](https://bookkeeper.apache.org/docs/getting-started/concepts)
- Redpanda: Raft per partition, where the leader assigns offsets before replication, as in classic Kafka **[not re-verified]**. — [Redpanda architecture](https://docs.redpanda.com/current/get-started/architecture/)

### Inferences
- Ursa, S2 and KIP-1164 all use a per-partition or per-stream sequencer: one Oxia shard or coordinator partition per stream, not a single global one. Throughput then scales by sharding streams across sequencers. If precog needs only per-stream order across writers, the closest production precedent is a per-stream offset allocation, for example sharded by stream hash across several small Raft groups. That would replace one global cut.

### Gaps
- I found no source claiming linearizable order for WarpStream classic topics, beyond Kafka semantics plus atomic commits.
- I did not verify a Jepsen analysis of WarpStream, Ursa, S2 or AutoMQ, and I am not aware of one.

## Q5. Lessons or anomalies from Jepsen or postmortems

### Takeaway
The Jepsen Bufstream analysis found the metadata-store integration to be the main source of failures, not the ordering logic itself. The failures were metastable stalls on etcd lease loss, stale cached watermarks, and an error-path offset bug. It also showed that Kafka's transaction protocol has anomalies inherited by every Kafka-compatible system.

### Cited Findings
All findings in this section are from [Jepsen, Bufstream 0.1.0](https://jepsen.io/analyses/bufstream-0.1.0) (independent).

- **Stuck consumers:** a stale cached last stable offset and high watermark after restart stalled consumers for over an hour. Affected 0.1.0 through 0.1.3-rc.8, fixed in rc.6.
- **Stuck producers and consumers:** missed etcd lease-deletion updates after pauses or partitions. Fixed in 0.1.3-rc.8.
- **Spurious zero offsets:**
  - Error responses omitted offset `-1`, and the Java client read `0` as success.
  - Writes appeared at offset 0 on send and at a higher offset on poll, reproduced about every 5 minutes under etcd or Bufstream faults.
  - Fixed in 0.1.3-rc.6.
- **Lost transactional writes:**
  - Caused by an epoch-tracking bug in 0.1.2.
  - In one run, 240 records from committed transactions were lost out of 6,761 transactions in 100 s, with a 1 s transaction timeout.
  - Fixed in 0.1.3-rc.2.
- **Lost writes from server-side response-size filtering:**
  - 22 of 16,770 acked records were never polled.
  - Fixed in 0.1.3-rc.12.
- **Upstream Kafka issues:**
  - KAFKA-17754: a delayed `EndTxn` can commit or abort the wrong transaction, causing aborted reads, lost writes and torn transactions. Bufstream added etcd-revision ordering to reduce it, but it persists; KIP-890 is the upstream fix.
  - KAFKA-17582: unpredictable consumer rewind after an abort.
  - KIP-588: a misleading `ProducerFencedException`.
  - KAFKA-17734: `Consumer.close()` blocks.

### Inferences
- For precog, the analogous risks are:
  - nodes acting on a cut or frontier lease they have lost (the lease-deletion stall);
  - stale cached committed cuts after restart (the stuck-consumer bug);
  - error paths that return default offset or sequence values.
- Fencing tokens such as S2's `fence` and `match_seq_num`, or epoch checks on cut application, would directly address the first two.

### Gaps
- I did not find a Jepsen report for WarpStream, S2, AutoMQ, Ursa or Redpanda's newer versions. Redpanda had a 2022 Jepsen analysis that I did not fetch.
- I found no public postmortems of metadata-store outages for these vendors.
