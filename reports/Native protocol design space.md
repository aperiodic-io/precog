# Native protocol design space: bytes and batches, not encodings

For live-ingestion's raw workload, Precog's native protocol beats Kafka mainly by sending fewer bytes and fewer batches, and by never making the broker look inside a batch. Clever encodings matter much less. Five decisions carry most of the expected gain, in this order:

1. **Batches large enough to amortise fixed costs, and several in flight.** Precog's own measurements show server CPU per message falling from 31.8 µs at 10 events/batch to 4.8 µs at 100 and 2.5 µs at 1,000 (`results/RESULTS.md`).
2. **A dedup id sent as a fixed 8-byte hash in an uncompressed column**, instead of a ~30-byte string header inside each (compressed) record. The Redpanda fork that runs today has to decompress every compressed batch to find the header. When it drops a duplicate it rebuilds the batch and compresses it again (`dream-faster/redpanda` `src/v/cluster/dedup_window_filter.cc`, lines 420-426 and 663-668).
3. **Not sending the second producer replica's copy at all**, or only its 8-byte hashes, as in exchange A/B feed arbitration. Today about half of all ingress is duplicates.
4. **One cumulative ack per writer log per group commit**, instead of one per batch.
5. **Payload bytes forwarded unchanged** to WALs, peers and subscribers, with `writev` of reference-counted buffers rather than a copy into a send buffer.

Encoding style comes after these. The choice between varints and fixed layout, or SBE, FlatBuffers and Cap'n Proto, matters only for the roughly 60-byte batch header and a few bytes per record, because the payload is an opaque protobuf the broker never parses. Of the I/O techniques, some are measured as not helping on this path:
- **Zero-copy send** (`MSG_ZEROCOPY`, io_uring `SEND_ZC`) is documented to pay off only above about 10 KB per write and gives no gain on loopback or veth ([kernel doc](https://docs.kernel.org/networking/msg_zerocopy.html)).
- **io_uring for sockets** cuts syscalls sharply. Measured throughput gains at low connection counts are small or negative ([monoio benchmark](https://github.com/ukernel/monoio/blob/master/docs/en/benchmark.md); [joakimthun echo server](https://github.com/joakimthun/io-uring-echo-server)).
- **Busy-polling** trades CPU for latency, which is the wrong trade on 1-CPU broker pods.

A rough budget sets the scale. In the one docker-stats snapshot of the Redpanda fork baseline, three brokers used 0.80 cores in total (18%, 17%, 45%) at 245k records/s, about **3.3 µs of broker CPU per record cluster-wide** (`results/redpanda-baseline/rp-peak-dup20.stats`; it is a single sample under a client-limited run, so treat it as ±50%). Precog's best durable-2 run is 3.3-4.8 µs per message cluster-wide (`results/RESULTS.md`). **On CPU per record, Precog today is roughly at parity with Redpanda, not 3× better.** The §7.3 target therefore needs about 1.1 µs per record across the cluster, or about 0.37 µs per node. That is far too little for per-record work beyond a hash probe and a bounds check.

The batch-size runs also imply a marginal cost of about 2.2 µs per extra 256-byte record cluster-wide (derived from the b100 and b1000 rows below). That is above the budget, so batching alone cannot reach it. Bytes moved per record (5 socket crossings, 3 WAL writes and several user-space copies) are the likely cause. This is an inference to verify with experiment 1, not a measurement. If it holds, **fewer bytes is a CPU lever as well as a bandwidth lever**: hashed dedup ids, not sending duplicates, compression before send, and no extra copies. It also matters that the Redpanda baseline received snappy-compressed batches, while Precog's benchmarks send uncompressed bytes.

The rest of the report covers what existing systems do (§1), encoding (§2), I/O (§3), flow control and acks (§4), and points specific to this workload (§5). It ends with a recommended shape (§6) and 28 ranked proof-of-concept experiments (§7).

---

## 0. The workload and Precog's current wire format

| Property | Value | Source |
|---|---|---|
| Records | protobuf, ~100-300 B, keyed by symbol, one dedup id per event | task brief; `live-ingestion/docs/architecture.md` |
| Rate | 20-40k/s per busy stream at peak; 100-250k/s per cluster; `raw.quotes.binance-futures` 22k/s p99 and ~42k/s peak | brief; `live-ingestion/docs/architecture.md` |
| Duplicates | 2 producer replicas send the same events: ~50% duplicates, deduplicated within 5 min | `docs/PLAN.md` §7.1 |
| Producers today | franz-go, `acks=all`, idempotence **off** (otherwise the fork skips dedup), zstd, one header per event | `live-ingestion/k8sx/e2e.go` (comment on `VerifyRawTopicDeduplication`); `docs/PLAN.md` §7.1 |
| franz-go defaults that matter | `maxProduceInflight: 1` per broker when idempotence is off; linger 10 ms on current master (older releases differ; check the version live-producers pins); max batch 1,000,012 B; 50,000 buffered records | [franz-go `pkg/kgo/config.go`](https://github.com/twmb/franz-go/blob/master/pkg/kgo/config.go) |
| Consumers | Parquet archiver (batch reads, commit after upload), brrrrr (every partition, own checkpoints, 50 ms event-time hold), Go seeder | `docs/PLAN.md` §7.1 |
| Brokers | 1 CPU, 4 GiB each, 3 nodes, 3 copies, ack after 2 durable | `docs/PLAN.md` §7.3 |

Precog's current formats (`crates/mesh/src/proto.rs`, `rpc.rs`):

- **Frame:** `u32 len | u8 kind | u64 req_id | body`, so 13 bytes. Many requests can be in flight and are matched by `req_id`. The writer task coalesces every queued frame into one `write` of up to 4 MiB.
- **Batch (PCB2):**
  - header: `magic | u16+stream | u16+writer | u64 epoch | u64 first_seq | u32 count`
  - each event: `u64 event_time | u16 key_len | u32 payload_len | key | payload`
  - trailer: `u32 crc32c`

  That is 14 bytes plus the dedup-key string per event, and about 32 bytes plus both names per batch.
- **Ack:** 10-byte body (`status, copies, nodes bitmask, crc`), one per Append.
- **Copies on the send path:**
  - `encode_append` and `encode_replicate` each copy the batch once to prepend 1-4 bytes.
  - `write_loop` then copies every frame into a `BytesMut` before `write_all`.

  So each hop pays two user-space copies on top of the kernel copy. This is cheap at 25 KB batches (about 1-2 µs per copy, my estimate), but it is per-byte work of the kind the budget above says to remove.
- **Storage record (PCR1):** one 64-byte header per stored record, which is one client batch, followed by the batch bytes, in **one log per node shared by all writers** (`crates/storage/src/record.rs`). This matters for sendfile (§3.6).

---

## 1. How existing systems frame, batch and acknowledge

### 1.1 Kafka

**Record batch v2.**
- **Batch header:** 61 bytes: `baseOffset i64, batchLength i32, partitionLeaderEpoch i32, magic i8, crc u32 (CRC-32C), attributes i16, lastOffsetDelta i32, baseTimestamp i64, maxTimestamp i64, producerId i64, producerEpoch i16, baseSequence i32, recordsCount i32`.
- **Each record:** varints: `length, attributes i8, timestampDelta varlong, offsetDelta, keyLength, key, valueLength, value, headersCount`, then each header as `keyLen, key string, valueLen, value`.
- **Compression:** applies to the whole records section. The CRC covers attributes through the end, compressed bytes included ([Kafka message format](https://kafka.apache.org/42/implementation/message-format/)).

KIP-98 introduced this format and quantifies it:
- The old format cost a fixed 34 bytes per message.
- v2 costs 53 bytes per batch plus 6-25 bytes per record.
- A 100-record batch drops from 3,400 to 753 bytes of overhead.
- About 7 bytes per added record for close timestamps ([KIP-98, Space Comparison](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)).

**Headers are the expensive part for this workload.** Each record repeats the header *key string* (`redpanda-dedup-key`, 18 bytes) and the value (~30 bytes). That is about 50 bytes of uncompressed overhead per record before the symbol key; compression shrinks the repeated key.

**Shared format, sendfile, end-to-end compression.**
- Kafka's design goal is "a standardized binary message format" shared by producer, broker and consumer, so log chunks move without modification.
- Consumers are served with `sendfile`, from page cache to socket. This path is not available under TLS.
- "Efficient compression requires compressing multiple messages together." Batches stay compressed on disk and reach consumers compressed; the broker decompresses only to validate ([Kafka design, Efficiency](https://kafka.apache.org/42/design/design/)).
- The IBM study behind the sendfile argument measured about a 65% time reduction for `transferTo` ([Palaniappan & Nagaraja 2008, mirror](https://www.linux.com/news/efficient-data-transfer-through-zero-copy/)).

**Pipelining.** "The broker's request processing allows only a single in-flight request per connection in order to guarantee this ordering". Clients may still send while waiting, and responses return in request order ([Kafka protocol guide](https://kafka.apache.org/42/design/protocol/)). franz-go caps non-idempotent producers at 1 produce request in flight per broker by default ([config.go](https://github.com/twmb/franz-go/blob/master/pkg/kgo/config.go)). With `acks=all`, each produce request therefore costs a full replication round trip before the next one leaves. Batches grow under load because records pile up behind the in-flight request ([Redpanda batch tuning](https://www.redpanda.com/blog/batch-tuning-redpanda-performance-part-1)).

**Fetch and KIP-227.**
- Fetch is a long-poll pull: `fetch.min.bytes`, `fetch.max.wait.ms`, per-partition offsets.
- Request and response size grew linearly with the number of partitions. KIP-227 adds fetch sessions (32-bit session id plus epoch). Incremental requests carry only changed partitions, and incremental responses only partitions with new data or a changed watermark.
- It reports no measurements, only the O(partitions) to O(changes) argument ([KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability)).

**Lesson for Precog:** keep per-subscriber state on the server, so steady-state messages carry no repeated cursor or topic lists. Precog's push subscription already does this.

### 1.2 Redpanda

- Redpanda keeps the Kafka batch as its on-disk format and runs a Seastar thread per core. Each core handles network polling, I/O and compute, with a stated blocking budget under 500 µs.
- A request arriving on one core is routed to the core that owns the partition, through `iobuf`. That is "a ref-counted, fragmented-buffer-chain with deferred deletes", allocated in 15 fixed fragment sizes from 512 B to 128 KiB.
- Zero-copy applies mainly to sharing between cores. A contiguous view is made by copying fragments into an exact-size buffer ([Redpanda, thread-per-core buffers](https://www.redpanda.com/blog/tpc-buffers)).
- Its batching guidance: each request has a fixed cost plus a variable cost, so fewer, larger requests reduce broker CPU and backlog-driven latency. Adding partitions or keyed messages shrinks batches; more producers mean fewer messages per producer ([batch tuning part 1](https://www.redpanda.com/blog/batch-tuning-redpanda-performance-part-1)).
- Fetch read coalescing deduplicates identical concurrent fetches. It is off by default and helps only at high fan-out ([docs](https://docs.redpanda.com/streaming/current/manage/cluster-maintenance/fetch-read-coalescing/)).

**The fork's dedup path** (`src/v/cluster/dedup_window_filter.cc`):
- `decompress_batch_sync` runs on every compressed batch to read per-record headers.
- When records are dropped, the batch is rebuilt and recompressed with `compress_batch_sync`.
- Every replica keeps an index at about 37 bytes per entry (`live-ingestion/docs/architecture.md`).

This is the clearest protocol lesson in this report: **put anything the broker must read outside the compressed region, in a form it can read without parsing records.**

### 1.3 Apache Iggy (0.11 binary protocol)

- **Transport header:** every frame has a fixed 256-byte header, a `#[repr(C)]` struct decoded by pointer cast, with `size` at offset 48 ([framing](https://iggy.apache.org/docs/binary-protocol/framing.md)).
- **One message layout:** a 256-byte batch header followed by message frames, each with a 48-byte header: `checksum u64 (XxHash3), id u128, offset_delta u32, timestamp_delta u32, user_headers_length u32, payload_length u32, reserved u64`.
- **Same bytes everywhere:** the same layout serves the send body, replication, the persisted segment and the poll reply. "There is no other message encoding."
- **Dedup:** uses `producer_id/epoch/base_sequence` in the batch header ([messages](https://iggy.apache.org/docs/binary-protocol/messages.md)).

**Lessons:**
- Iggy chose fixed layout and identical wire, disk and replication bytes. Precog already does the same.
- 48 bytes per message plus 256 per batch is heavy for 100-300 byte records: 16-48% overhead.
- The per-message u128 id is a fixed-width dedup identity, but the page does not say the server deduplicates by it.

### 1.4 NATS and JetStream

**Core NATS** is a text protocol:
- `PUB <subject> [reply] <#bytes>\r\n<payload>\r\n`; `HPUB` adds `NATS/1.0` headers.
- Flow control is a hard cutoff: a connection whose pending data passes a limit (10 MB by default) gets a non-recoverable "Slow Consumer" error ([protocol](https://docs.nats.io/reference/reference-protocols/nats-protocol)).

**JetStream** deduplicates by a `Nats-Msg-Id` header within a duplicate window, 2 minutes by default ([streams](https://docs.nats.io/nats-concepts/jetstream/streams)). This is the same model as the fork, and it pays the same per-message string cost.

**ADR-50 (fast-ingest batch publishing)** is the most relevant recent design ([ADR-50](https://raw.githubusercontent.com/nats-io/nats-architecture-and-design/main/adr/ADR-50.md)):
- **Why:** async publish "costs a lot to manage acknowledgements on both client and server".
- **Server-paced acks:** the client asks for an ack every N messages. The server may start lower, double N toward the client's maximum, or halve it to slow a client down.
- **Cumulative acks:** "an ack for sequence 30 implies that earlier acks were received". The client keeps 1-3 outstanding acks, 2 being "generally optimal".
- **Gaps:** reported by explicit `BatchFlowGap` messages.

This is credit-based flow control expressed through ack frequency, and it maps directly onto Precog's per-writer sequences.

### 1.5 Pulsar

- **Frames:** `totalSize u32 | commandSize u32 | protobuf command`. Payload commands add `magic 0x0e01 | crc32c | metadataSize | metadata | payload`.
- **Batches:** a batch holds per-entry `SingleMessageMetadata` protobufs and is compressed as a whole.
- **Consumers:** pushed to, under **message permits** (`Flow` command). The client re-grants half its queue once half is consumed; the default queue is 1,000 messages.
- **Acks:** individual or cumulative ([binary protocol](https://pulsar.apache.org/docs/next/developing-binary-protocol/)). Clients group acks for 100 ms or 1,000 messages by default ([C++ ConsumerConfiguration](https://pulsar.apache.org/api/cpp/3.3.x/classpulsar_1_1_consumer_configuration.html)).

**Lessons:** use push with credits rather than long-poll, and send cumulative acks. Protobuf metadata per entry is a cost Precog should not copy.

### 1.6 Aeron

- **Data frame:** a 32-byte header: `frame length, version, flags (B/E/S), type, term offset, session id, stream id, term id, reserved u64`. Frames are 32-byte aligned, so a 19-byte payload uses 64 bytes on the wire.
- **Position:** messages live in power-of-two **term buffers**; the position is `(term id, term offset)`, so the byte offset *is* the sequence.
- **Flow control:** the receiver sends **Status Messages** carrying a byte **receiver window**, like TCP's, and recommends an initial window of at most a quarter of the term. Loss is repaired by NAK ([transport spec](https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification)).
- **Measured latency:** AWS reports p50/p99/p99.9 of 30/57/206 µs for the open-source Java driver at 1M msg/s on c6in.16xlarge ([AWS 2025](https://aws.amazon.com/blogs/industries/aeron-on-aws-2025-performance-benchmark-results)). That is vendor-run, on UDP, and not durable.

**Lessons:**
- Byte-window credits are cheap to compute and naturally bound memory.
- A byte-addressed log lets an ack be one integer.
- For Precog the equivalent integer is the per-writer sequence frontier.

### 1.7 Chronicle Queue and Wire, the LMAX Disruptor

- **Chronicle Queue** frames each excerpt with a 4-byte header, length plus state bits, in memory-mapped files. It reports 99.9th-percentile latency of 1.2-1.5 µs on the same machine at 10-100M events/min.
- **Chronicle Enterprise replication** runs over TCP. Its README notes "a latency step near 10 million messages/second occurs because messages begin to batch" ([Chronicle Queue README](https://github.com/OpenHFT/Chronicle-Queue)).
- **Chronicle's delta wire** sends only changed fields as numeric deltas: about 10 bytes against 205 for its binary YAML, and 554 ns against 1,473 ns to serialise, in its own test ([vanilla-java 2016](https://vanilla-java.github.io/2016/07/22/Improving-performance-with-simple-compression.html)).
- **LMAX Disruptor:** one producer and one consumer reach 26M ops/s against 5.3M for `ArrayBlockingQueue`. Mean latency through a three-stage pipeline is 52 ns against 32,757 ns. The "batching effect" lets a lagging consumer process everything up to the cursor without touching concurrency primitives, so latency stays flat with load ([Disruptor paper](https://lmax-exchange.github.io/disruptor/disruptor.html)).

**Lesson for Precog's in-process path** (rpc to append to group commit to replicate): use single-writer queues and drain everything available per wake-up.

### 1.8 ZeroMQ

- **ZMTP 3.0 frames:** a flags byte plus a 1-byte size (bodies up to 255 B) or an 8-byte size. Minimum overhead is 2 bytes ([RFC 23](https://rfc.zeromq.org/spec/23/)).
- **Opportunistic batching:** batching can raise throughput "up to two orders of magnitude". ZeroMQ batches only when messages queue up: "Batch only when new data are arriving faster than they can be processed". With no queue, a message goes out at once ([AOSA ZeroMQ chapter](https://aosabook.org/en/v2/zeromq.html)).
- **Small messages:** "copying is much cheaper than allocating memory". Very small messages are stored inside the handle.

**Lesson:** batch on backlog, not on a fixed timer. Precog's `write_loop` already does this per connection; the client `Writer` should do the same.

### 1.9 Kafka on object storage (WarpStream, Bufstream, AutoMQ)

These systems trade latency for cost, so they are relevant mostly as warnings and for their metadata-commit designs:
- **WarpStream:** agents buffer for 250 ms or 4 MB by default and acknowledge after the object is written. p99 is 400-500 ms on S3 Standard, 170 ms on S3 Express, and 50 ms with "Lightning Topics" ([WarpStream docs](https://docs.warpstream.com/warpstream/kafka/configure-kafka-client/tuning-for-performance)).
- **Bufstream:** leaderless brokers that accept writes anywhere. OMB median and p99 are 260 and 500 ms. It validates protobuf at the broker, and the cost of that parsing pushed Buf to write `hyperpb` ([Bufstream](https://buf.build/docs/bufstream); [hyperpb](https://buf.build/blog/hyperpb)).
- **AutoMQ:** a WAL on EBS or FSx; produce p99 is 13-17 ms in vendor tests ([AutoMQ benchmarks](https://docs.automq.com/automq/benchmarks)).

**Lessons for Precog:**
- A leaderless "any node accepts" design is in production elsewhere, in Bufstream.
- Parsing payloads at the broker is expensive enough to need its own engineering. Keep the payload opaque.

### 1.10 Fluvio, Seastar RPC, Cap'n Proto RPC

- **Fluvio** uses Kafka-like record batches with batch-level compression codecs and producer linger and batch size, plus WASM SmartModules on the broker ([docs.rs/fluvio](https://docs.rs/fluvio/)). There is no distinctive wire idea for this workload.
- **Seastar RPC** negotiates features on connect (magic plus feature list), frames requests with length plus id, and offers LZ4 stream compression without linearising buffers ([seastar rpc.md](https://github.com/scylladb/seastar/blob/c5f3fa06a9281122fdc175ca12c49ec28a4a8d9c/doc/rpc.md); [rpc compression](https://docs.seastar.io/master/md_rpc_compression.html)).
- **ScyllaDB internode compression:** added zstd next to lz4 for node-to-node traffic. It caps zstd CPU at a configured fraction, 0.05 suggested, and replaces per-message compression with a **dictionary trained on live RPC traffic on one node and distributed to the others** ([ScyllaDB docs](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/config-change/advanced-internode-compression.html)). That is directly applicable to small market-data records (§2.5).
- **Cap'n Proto RPC:** its "promise pipelining" saves round trips for chained calls, which Precog does not have. Its packing (a tag byte per 8-byte word that elides zero bytes) is a cheap pre-pass that helps zero-heavy fixed layouts ([encoding](https://capnproto.org/encoding.html)).

### 1.11 Market-data wire formats

These are designed for the same kind of payload Precog carries:
- **MoldUDP64** (Nasdaq ITCH transport): a 20-byte packet header of `session[10], sequence u64, count u16`, then `u16 length | message` per message. The sequence is that of the first message; the rest are implicit ([JNX MoldUDP64 spec](https://www.japannext.co.jp/pub_data/pub_onboarding/JNX_MoldUDP64_Specification_1.02.pdf)). This is exactly Precog's `(first_seq, count)` batch identity.
- **CME MDP 3.0** sends every packet on two feeds, A and B. Receivers "process packets by incremental sequence number" and "discard a packet if the sequence number has already been processed". A gap seen on both feeds triggers recovery ([CME MDP 3.0](https://cmegroupclientsite.atlassian.net/wiki/x/zIZHGw)). Precog's two producer replicas are an A/B feed (§5.3).
- **Databento DBN** uses fixed-width records: a 16-byte header of `length in 32-bit words, rtype, publisher_id, instrument_id, ts_event`, with integer instrument ids instead of symbol strings and the symbol mapping in the metadata. The layout is the same in memory, on the wire and on disk ([DBN](https://databento.com/docs/standards-and-conventions/databento-binary-encoding)).

---

## 2. Encoding

### 2.1 Fixed layout or varints for headers

- **Varints save bytes when values are small and vary.** Kafka's per-record deltas are 1-2 bytes. The cost is branchy, serial decoding, since each length must be read before the next field can be found.
- **Separating lengths from data speeds decoding.** Stream VByte stores control bytes apart from data and decodes over 4 billion integers per second with SIMD, up to 2× faster than varint-G8IU, which is itself much faster than plain VByte ([Lemire et al.](https://arxiv.org/abs/1709.08990v2)).
- **Fixed layout costs bytes but allows O(1) field access and pointer-cast decoding**, as in Iggy's `#[repr(C)]` headers, SBE and DBN. SBE's principles are copy-free, native types, allocation-free, streaming and word-aligned access ([SBE design principles](https://github.com/aeron-io/simple-binary-encoding/wiki/Design-Principles)). Its authors report 20-50× the throughput of protobuf on market-data ticks, a 2013 figure from people close to the project ([Adaptive](https://weareadaptive.com/trading-resources/blog/new-simple-binary-encoding/)).

**For Precog:**
- The per-batch header is a few tens of bytes amortised over 50-500 records, so fixed layout is the right choice there.
- Per record, the broker needs only the length, dedup hash and event time. Stored as **columns** (`u16 len[count]`, `u64 hash[count]`, `u32 time_delta[count]`), they decode in a simple loop or SIMD and are fully validated by one prefix sum.
- That is 14 bytes per record, against Kafka's ~6-10 bytes of varints *plus* ~50 bytes of header strings.

### 2.2 SBE, FlatBuffers, Cap'n Proto, rkyv for the control plane

The [rust_serialization_benchmark](https://github.com/djkoloski/rust_serialization_benchmark) (`log` dataset) gives:

| Library | Serialize | Deserialize | Zero-copy access | Size |
|---|---|---|---|---|
| rkyv | 222 µs | — | 340 µs | 1.01 MB |
| bitcode | 175 µs | 1.44 ms | — | 0.70 MB |
| postcard | 256 µs | 1.95 ms | — | — |
| prost | 0.77-2.27 ms | 2.7 ms | — | 0.88 MB |
| capnp | — | — | 43 ns | 1.44 MB |
| flatbuffers | — | — | — | 1.28 MB |

These results matter only for **control** messages (subscribe, commit, metadata), and those are rare, so any of them, or the current hand-encoding, is fine. On the hot path the payload is the producer's protobuf, and the broker should never decode it. Bufstream's experience shows what decoding it costs (§1.9).

### 2.3 Per-batch or per-record headers

Every system that is fast on small records puts identity and time on the batch and derives per-record values:
- **Kafka:** base offset and base timestamp, with per-record deltas.
- **Iggy:** `offset_delta` and `timestamp_delta`.
- **MoldUDP64:** an implicit sequence.
- **Aeron:** position is a byte offset.

Precog already makes `seq = first_seq + i` implicit. What remains per record is:
- **The 8-byte absolute event time.** Make it a `u32` µs delta from a batch base, as Iggy does; that limits one batch to about 71 minutes.
- **The 2-byte key length and the key string.** Replace them with an 8-byte hash.

### 2.4 Columnar and delta encoding of market data inside a batch

**Measured results:**
- **Gorilla** (Facebook TSDB) compresses timestamps with delta-of-delta: "about 96% of all time stamps can be compressed to a single bit". Values are XOR-compressed, about 51% to one bit, for an average of 1.37 bytes per (timestamp, value) point against 16 raw, a 12× reduction ([Gorilla, VLDB 2015](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf)).
- **Chronicle's delta wire:** about 20× smaller and 2.7× faster to serialise than full binary for one changed field (§1.7).
- **DBN** replaces symbols with integer instrument ids (§1.11).

**What this means for raw trades and quotes, all guesses to be measured in experiment 17:**
- Within one batch of one stream, symbol, exchange and most fields repeat, and timestamps and prices move by small deltas.
- Transposing fields into columns (delta-of-delta time, fixed-point price deltas, dictionary-coded symbol) before zstd should beat zstd on row-wise protobuf by a large factor. I expect 2-4× smaller output.
- The cost is that it requires a schema. The broker stays schema-agnostic if this is a *client codec*: an opaque compressed blob plus the uncompressed broker columns (§2.1). Consumers must decode it, and brrrrr and the archiver build Arrow and Parquet anyway, so a columnar batch might even save their protobuf decoding. That last point is unverified.

### 2.5 Compression on small records

**Speed and ratio** (zstd README, Silesia corpus, i7-9700K; [zstd README](https://raw.githubusercontent.com/facebook/zstd/dev/README.md)):

| Codec | Ratio | Compress | Decompress |
|---|---|---|---|
| zstd -1 | 2.896 | 510 MB/s | 1,550 MB/s |
| zstd --fast=4 | 2.146 | 665 MB/s | 2,050 MB/s |
| lz4 | 2.101 | 675 MB/s | 3,850 MB/s |
| snappy | 2.089 | 520 MB/s | 1,500 MB/s |

**Kafka producer test** (KIP-390, JSON records):

| Codec | Output size | Messages/s |
|---|---|---|
| zstd level 1 | 26.9% of input | 6,318 |
| zstd level 3 (default) | 26.4% | 4,761 |
| lz4 | 39% | 1,695 |

So zstd at level 1 is both smaller and faster than zstd's default in that test ([KIP-390](https://cwiki.apache.org/confluence/display/KAFKA/KIP-390%3A+Support+Compression+Level)).

**Dictionaries.** zstd's README says a dictionary improves ratio on small data "dramatically", with faster compression and decompression, and that the gains matter "mostly in the first few KB". Its example set is about 10k records of roughly 1 KB.

**Implications for Precog:**
- At 20-40k records/s per stream and 1-5 ms linger, a batch is 20-200 records, or 4-60 KB. Batches at the low end are where a per-stream dictionary should help most.
- Distribution can copy ScyllaDB: train on one node, distribute, and refer to the dictionary by id in the batch header.
- Precog has a cheap place to keep dictionaries: the M4 control plane.

**Kafka's lesson on compression placement.** Compress whole batches, at the producer, and never at the broker: "Efficient compression requires compressing multiple messages together" ([Kafka design](https://kafka.apache.org/42/design/design/)). The fork breaks the second half by decompressing for dedup (§1.2).

---

## 3. I/O

### 3.1 Syscalls and vectored writes

`rpc.rs` already coalesces queued frames into one `write`. The remaining cost is the copy into `out`. `writev` of the `Bytes` frames, with header and body slices in an `IoSlice` array (up to `IOV_MAX` = 1024), removes one copy per byte. Precog's append body also adds a 1-byte prefix by copying the batch; sending the prefix as its own iovec removes that copy too.

On receive, one large `read_buf` (the current 64 KiB reserve) followed by `split_to`/`freeze` is already zero-copy into `Bytes`. This is good.

### 3.2 TCP_NODELAY, corking, and buffer sizing

- **Nagle and delayed ACK.** With application-level coalescing, Nagle's algorithm adds only delay, up to the delayed-ACK timer, so `TCP_NODELAY` plus explicit coalescing is standard.
- **Autocorking.** Linux merges small consecutive writes while earlier data is still queued in the device. It is on by default since 3.14 ([tcp(7)](https://man.archlinux.org/man/tcp.7.en.txt); [LWN](https://lwn.net/Articles/576263/)).
- **Socket buffers.**
  - Large buffers raise throughput, but data queued in the kernel adds latency to anything behind it, acks included.
  - `TCP_NOTSENT_LOWAT` bounds unsent bytes in the kernel, so the application keeps control of ordering ([ip-sysctl](https://docs.kernel.org/networking/ip-sysctl.html)).
  - For a connection that mixes acks and bulk pushes, a low threshold such as 64-128 KB should cut ack p99 under catch-up load. That is a guess; see experiment 15.

### 3.3 io_uring

- **Features:** multishot receive (6.0), ring-mapped provided buffers (5.19, "vastly more efficient" than legacy), `DEFER_TASKRUN` plus `SINGLE_ISSUER` (6.1), and `submit_and_wait` for batching ([liburing wiki](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023)).
- **Syscall counts:** about 0.03-0.04 syscalls per request with io_uring runtimes against about 2 with epoll ([giong](https://docs.rs/crate/giong/0.2.0)).
- **Throughput:** "within noise" at 50 connections. Monoio's own benchmark says io_uring is *slower* than Tokio with one core and few connections, and 2-3× faster at 4-16 cores ([monoio](https://github.com/ukernel/monoio/blob/master/docs/en/benchmark.md)). A C++ echo test found epoll 14% ahead at 512 connections ([joakimthun](https://github.com/joakimthun/io-uring-echo-server)).

**For 1-CPU brokers with a few dozen connections and syscalls already coalesced by batching, the expected gain is small.** Test it (experiment 13), but do not design the protocol around it. io_uring matters more for the storage path (M8).

### 3.4 Zero-copy send

- The kernel documentation says `MSG_ZEROCOPY` "is generally only effective at writes over around 10 KB", that copy avoidance "replaces per byte copy cost with page accounting and completion notification overhead", and that **packets looped to a local socket "will incur a deferred copy"**. Pod-to-pod traffic on one node goes through veth, which the document cites as showing no improvement ([kernel doc](https://docs.kernel.org/networking/msg_zerocopy.html)).
- On the io_uring `SEND_ZC` patch thread, testers saw 5-10% *worse* results at 1,200 bytes on a real NIC, and the non-zero-copy path ahead in a UDP test ([netdev 2024](https://lists.openwall.net/netdev/2024/04/15/344); [lkml 2025](https://lkml.indiana.edu/2502.2/02077.html)).

Use zero-copy send only for large pushes to consumers on another node, and only if measured.

### 3.5 Busy-polling and kernel bypass

- `SO_BUSY_POLL`, `SO_PREFER_BUSY_POLL` and epoll busy-poll trade CPU for latency ([NAPI doc](https://docs.kernel.org/networking/napi.html)). On a broker with a 1-CPU limit, spinning burns the budget the 3× target is measured on. Do not do it.
- Kernel bypass (DPDK, AF_XDP, Aeron Premium's 30/39/43 µs) is not available inside ordinary k8s pods and is irrelevant at millisecond ack targets.

### 3.6 sendfile and splice for reads from the segment log

- **How Kafka gets sendfile:** a partition's log is contiguous, so a fetch is one file range.
- **Why Precog cannot do the same today:** it keeps **one log per node shared by all writers**, with a 64-byte PCR1 header before each batch. One stream's batches are interleaved with every other stream's, so a sendfile-based catch-up read becomes many small ranges.
- **Options:**
  1. Ship ranges *including* PCR1 headers and let the client skip records of other streams. This wastes bytes in proportion to stream share.
  2. Issue one `sendfile` per batch run. Batches are 4-60 KB, so this is still far fewer syscalls than per record.
  3. Use per-stream segment groups, which R1 in `docs/PLAN.md` §7.2 already lists for retention. They make a stream contiguous and would allow Kafka-style large ranges.
- **The tail matters more.** Live subscribers (brrrrr) are served from the in-memory tail cache, where the right primitive is `writev` of the cached `Bytes`, not sendfile. Sendfile matters for catch-up only: an archiver starting at `now - 2h`, or brrrrr restarting from a checkpoint.
- **Conflicts with M8:** sendfile needs the page cache, and the M8 O_DIRECT writer would remove it.

### 3.7 Connections, multiplexing, head-of-line blocking

- **One TCP connection per client and node pair** multiplexes all streams. That is cheap, but a large push or fetch response queued ahead of an ack delays the ack: head-of-line blocking in the application and kernel queues.
- **HTTP/2's answer** is frame interleaving with a 16 KiB default maximum frame ([RFC 9113 §4.2](https://www.rfc-editor.org/rfc/rfc9113#section-4.2)).
- **Options for Precog:** cap push frames at about 64 KB and interleave them with acks, or open a second connection for bulk reads.
- **QUIC streams** remove the HOL blocking but were measured 1.4-1.6× slower here and were dropped.

---

## 4. Flow control and acknowledgements

| System | Producer side | Consumer side |
|---|---|---|
| Kafka | 1-5 requests in flight; each request is acked | long-poll fetch with `fetch.min.bytes`/`max.wait` and offsets per partition (sessions since KIP-227) |
| Pulsar | async sends with pending-queue limit | push under **message permits**, cumulative or grouped acks (100 ms / 1,000) |
| NATS JetStream (ADR-50) | **server-paced ack every N, cumulative, 1-3 outstanding** | pull or push with max ack pending |
| Aeron | publication limited by receiver **byte window** (Status Messages) | same |
| Precog today | per-batch `call` with timeout, a separate ack per batch | push of every durable batch; backpressure only through TCP (`rpc.rs`) |

Recommended for Precog:

1. **Producer window in bytes and batches**, with no fixed one-in-flight rule. Per writer log, keep up to *W* unacked batches, for example 8, or 4 MB. Per-writer sequence order makes pipelining safe.
   - A retry resends the same identity, which is idempotent.
   - A gap is impossible to commit out of order on one node: the store keeps a contiguous frontier, and later batches are simply held.
   - The risk is on failover: batches *k+1..k+W* may land on another node before *k*. The mesh already accepts this, and frontiers expose it.
2. **Cumulative acks per writer log**: `(writer_id, epoch, durable_through_seq, copies, nodes)`, sent once per group commit per connection. With *W* in flight this replaces up to *W* ack frames and *W* client wake-ups with one. ADR-50 shows the same idea in production: "an ack for sequence 30 implies earlier acks".
3. **Backpressure the server can steer.** As in ADR-50, the server puts the client's allowed window into each ack. It shrinks the window when the WAL queue or replication lag grows, and grows it otherwise. This is bounded, explicit and cheap; the alternative is relying on TCP buffers filling.
4. **Subscribers get push with a byte-credit window**, as with Aeron's receiver window or Pulsar's permits counted in bytes. The subscriber re-grants credit after handing data to the application. That replaces "a slow subscriber stalls the reader task and then TCP" (`rpc.rs`) with explicit, per-subscription credit, so one slow subscription cannot stall others on the same connection.
5. **Consumer positions are per-writer frontiers** (R2), committed by the archiver after upload as one frontier blob. No per-message ack is needed for raw consumers. brrrrr keeps its own checkpoints, so it needs none at all.

---

## 5. Workload-specific design

### 5.1 Client batching and linger

- **Batch size drives per-record CPU.** Precog's measurements, 3 nodes and durable-2:

  | Events per batch | Server CPU per message |
  |---|---|
  | 10 | 31.75 µs |
  | 100 | 4.81 µs |
  | 1,000 | 2.45 µs |

  (`results/RESULTS.md`; same at 6 producers.) Fitting the b100 and b1000 points gives about 260 µs per batch plus about 2.2 µs per record, cluster-wide. That is a derived estimate, and throughput differed between the runs.
- **Batch sizes at production rates:**
  - At 30k records/s per stream, a 2 ms linger gives about 60 records per batch.
  - Five partitions per raw topic today split that further. Precog has no partitions, so a stream's events from one producer replica form one writer log and one batch.
- **Opportunistic batching** (ZeroMQ) fills batches during backlog without adding linger at idle: send when the window has room, otherwise accumulate. Combined with a short maximum linger, it gives large batches at peak and low latency off-peak.
- **The fixed-linger alternative** adds half the linger to average latency, per Redpanda's guidance ([part 1](https://www.redpanda.com/blog/batch-tuning-redpanda-performance-part-1)).
- **Per-connection aggregation.** One producer pod usually publishes a few streams, for one exchange and data type. Batches of different streams should share frames and syscalls. They should not share batch identity, since a batch belongs to one writer log.

### 5.2 Sending dedup ids compactly

- **Format.** The client computes `xxh3_64(dedup_id)` and sends it as a `u64` column outside the compressed region. Both replicas hash the same id, so their hashes match.
- **Collisions.** The broker already plans to index 64-bit xxh3 hashes (`docs/PLAN.md` §7.4), so sending the hash loses nothing. The false-drop probability is n/2⁶⁴ per event, about 10⁻¹² at 8M live keys, as computed there.
- **Savings:**
  - Bytes: about 22-40 bytes per record. The id `live-producer-<exchange>-<n>-<trade_id>` is about 30-45 bytes in the loadgen (`benchmarks/redpanda-dedup/loadgen/main.go`). That is 10-15% of a 250-byte record.
  - Broker CPU: one string hash per record and the key-length parse.
  - Decompression: none is needed to find the id.
- **Retained ids.** Keeping the original id string for debugging is optional; it can live inside the compressed payload, where the broker never reads it.
- **Dropped duplicates inside a compressed batch.** Do not rewrite the batch. Store it as is, with a drop bitmap (`count` bits) in the stored record's metadata, and let readers skip dropped indices. The cost is disk for the duplicate payloads, which §5.3 is designed to avoid. A fully duplicate batch, where every hash is already present, is dropped whole.

### 5.3 Not sending duplicates at all

Two replicas publishing the same events are an A/B feed. Exchanges have the receiver arbitrate by sequence number (§1.11). Here the "sequence" is the dedup hash. Three designs, from simple to complete:

| Design | Ingress bytes | Correctness risk | Complexity |
|---|---|---|---|
| (a) **Both send full events** (today) | 2× | none (union of both) | none |
| (b) **Lease: active replica sends, standby buffers ~10 s and publishes from the lease-loss point** | ~1× | events only the standby saw are lost while the active is healthy (a websocket message one replica missed); overlap on failover is covered by dedup | lease in the mesh control plane or via the broker |
| (c) **Shadow hashes: active sends full events; standby sends only `(hash, event_time)` columns**. The broker matches them against the active's index and asks the standby for payloads of hashes still unmatched after T ms (for example 1-2 s); the standby keeps a ring buffer of payloads | ~1× + ~12 B per event | none beyond (a): union semantics kept | broker "request payloads" message, standby ring buffer |

- **Design (c) keeps today's guarantee**, the union of both feeds, and removes about 45% of ingress bytes, replication bytes and WAL bytes for the raw cluster. That is ~50% duplicates, minus the 12-byte shadows.
- **Dedup lookups stay at the same count**, but they become probes on the 8-byte hash column, with no payload copy, CRC, fsync or replication for the duplicate.
- **The broker can choose which replica is active per stream** and tell producers in the Hello or ack, so no separate lease service is needed. A replica that stops receiving acks for its shadows becomes active on its own after a timeout. That last part is my design; it is not taken from any cited system.
- **Risks:**
  - Event time and ordering: the standby's late-filled events arrive T ms late, so brrrrr's 50 ms hold could miss them. They would be handled as late events, which the system already sees on failover today.
  - Shadow traffic must go to the same node as the active stream. The preferred-node-by-stream routing in `docs/PLAN.md` §7.4 already ensures this.

### 5.4 The consumer read path

- **Live tail.** Push the stored batch bytes exactly as received, with batch header and columns intact and no re-encoding (Precog already does this), plus the drop bitmap. Use `writev` from the tail cache.
- **Catch-up.** Use the per-stream index to find batch runs, then `sendfile` each run, or ship whole contiguous segment ranges if per-stream segment groups land (§3.6).
- **The archiver** wants batches of N MB to write Parquet. A pull with "up to N bytes from frontier F" is enough; it needs no per-record acks and commits its frontier after upload (R3).
- **brrrrr** reads every stream. One subscription per stream, multiplexed on one connection with per-subscription credit, mirrors its current `(topic, partition)` positions. A frontier-keyed checkpoint replaces `(topic, partition) → offset`.

---

## 6. Recommended protocol shape (a sketch to test, not a spec)

```text
frame   := u32 len | u8 kind | u8 flags | u16 reserved | u64 req_or_sub_id | body     (16 B, 8-aligned)

hello   := version, client_id, [stream name -> u32 stream_id], [writer -> u32 writer_id],
           dictionary ids, initial window             (names sent once per session, cf. KIP-227)

batch   := magic u32 | ver u8 | codec u8 | dict_id u16
         | stream_id u32 | writer_id u32 | epoch u64 | first_seq u64
         | count u32 | base_time_us u64 | blob_len u32
         | crc32c u32                                              (56 B fixed)
         | u32 time_delta_us[count] | u64 dedup_hash[count] | u16|u32 len[count]   (uncompressed)
         | blob (payloads concatenated; optionally zstd/lz4 with dict)

append  := frame(kind=Append) | u8 copies | batch*      (many batches per frame, per connection)
ack     := frame(kind=Ack)    | n | n × (u32 writer_id, u64 epoch, u64 durable_through, u8 copies,
                                       u32 nodes, u32 window_bytes)      (cumulative, per group commit)
push    := frame(kind=Push, id=subscription) | stored batch bytes | drop bitmap
credit  := frame(kind=Credit, id=subscription) | u32 bytes
shadow  := frame(kind=Shadow) | stream_id | writer_id | count | u64 hash[count] | u32 time_delta[count]
want    := frame(kind=Want)   | stream_id | u64 hash[n]          (broker -> standby)
```

**Per-record wire overhead, uncompressed:**

| Format | Bytes per record |
|---|---|
| Proposed | 14 (time delta 4, hash 8, length 2) |
| Precog PCB2 today | 14 + ~35 (dedup string) ≈ 49 |
| Kafka v2 | ~6-10 varints + symbol key ~8 + header key 18 + header value ~35 + 3 lengths ≈ 70-75 |
| Iggy | 48 + user headers |

The proposed and PCB2 figures are derived from the formats above. The Kafka figure is an estimate from the field list, and compression reduces its repeated strings. Interning stream and writer names drops about 40 bytes per batch, and their string compares, from the hot path.

**The broker's per-record work in this shape:** sum the lengths, probe one hash, and keep the CRC over the batch, at hardware crc32c speeds. It never decompresses or parses a payload.

---

## 7. Ranked proof-of-concept experiments

**Shared harness**, built once (about 300 lines):
- A loopback TCP client and server in Rust with tokio, each pinned to one core with `taskset`, to mimic 1-CPU brokers.
- A synthetic market-data generator: protobuf-like 100-300 byte records, symbol-keyed, with realistic deltas. Better, replay a sample of real staging records; reading staging's raw topics is allowed read-only.
- Optional 50% duplicates from a second "replica".
- Measures for every experiment: records/s, server CPU µs per record (`getrusage` or `/proc/self/stat`), client CPU µs per record, p50/p99/p99.9 ack latency at a fixed offered rate (30k/s per stream, 150k/s total) and at saturation, wire bytes per record (counted at the socket), and syscalls per record (`strace -c` or `perf stat -e 'syscalls:*'`).

**Caveats for every result:**
- Loopback understates per-packet costs relative to veth and overlay networking in k8s.
- `MSG_ZEROCOPY` behaves differently on loopback (§3.4).

Each winner should be re-checked in the §7.3 3-node harness.

Ordered by expected impact on the §7.3 measures for this workload:

**1. Cost decomposition of today's append path.**
- *Hypothesis:* per-record cost is dominated by bytes moved (copies and socket crossings), not per-record logic, so byte reductions are CPU reductions.
- *Build:* drive `precog-mesh` with batch sizes 10, 30, 100, 300 and 1,000, and record sizes 100, 250 and 1,000 B. Profile with `perf record` and gungraun instruction counts.
- *Measure:* fit CPU = a·batches + b·records + c·bytes.
- *Expected:* c·bytes is a large share of b. The fit to RESULTS gives about 260 µs per batch and 2.2 µs per record cluster-wide (derived). Experiments 2-8 depend on this answer.

**2. Pipelined producer window with ack-clocked opportunistic batching.**
- *Hypothesis:* *W* = 4-8 batches in flight per writer, plus "send when the window has room, else accumulate up to a 2 ms maximum linger", gives batches of 100 or more at peak with lower p99 than fixed linger.
- *Build:* a client batcher (~250 lines).
- *Measure:* CPU per record and p99 at 30k/s and at saturation, against W=1 (franz-go's non-idempotent default) and against fixed linger of 1, 5 and 10 ms.
- *Expected:* 2-5× less server CPU per record than 10-30-record batches, from the batch-size table. p99 is dominated by group commit, not linger ([ZeroMQ](https://aosabook.org/en/v2/zeromq.html); [Redpanda](https://www.redpanda.com/blog/batch-tuning-redpanda-performance-part-1)).

**3. Shadow-hash duplicate suppression (§5.3 design c).**
- *Hypothesis:* the standby sends 12 bytes per event instead of ~260, cutting ingress, replication and WAL bytes by about 45% with no loss of union semantics.
- *Build:* a standby client with a ring buffer, plus broker `Shadow`/`Want` handling with a timer (~400 lines).
- *Measure:* bytes and CPU per *unique* record, the fill latency for events only the standby saw, and correctness under random loss of 0.1% on one replica.
- *Expected:* about 40-45% less server CPU per unique record, if experiment 1 confirms that cost scales with bytes. Fill latency equals T.

**4. Dedup id as an uncompressed u64 hash column.**
- *Hypothesis:* moving from per-record id strings to a hash column saves 22-40 bytes per record and removes per-record string hashing and any need to decompress.
- *Build:* two encoders and a broker dedup probe (~200 lines).
- *Measure:* bytes per record and broker CPU per record, with and without zstd.
- *Expected:* 10-15% fewer wire bytes uncompressed and less after compression. A large CPU win against the "decompress to find headers" path the fork uses (`dedup_window_filter.cc`).

**5. Cumulative per-writer acks once per group commit.**
- *Hypothesis:* one ack per writer per commit, instead of one per batch, cuts server sends and client wake-ups by a factor of *W*.
- *Build:* ack aggregation in the commit callback (~150 lines).
- *Measure:* ack frames per second, client CPU per record, and p99.
- *Expected:* a small to moderate server CPU win and a larger client CPU win. p99 unchanged or better ([ADR-50](https://raw.githubusercontent.com/nats-io/nats-architecture-and-design/main/adr/ADR-50.md)).

**6. writev of `Bytes` without user-space copies.**
- *Hypothesis:* removing the `encode_append`/`encode_replicate` copies and the `write_loop` copy cuts per-byte CPU on every hop.
- *Build:* a `write_vectored` loop over header and body slices (~150 lines).
- *Measure:* CPU per MB at 4-60 KB batches.
- *Expected:* 5-15% less CPU on the replicate path. That is a guess: three copies of a 25 KB batch is about 75 KB of memcpy per batch.

**7. Many batches per frame, across streams on one connection.**
- *Hypothesis:* explicit multi-batch Append frames remove per-frame dispatch, map lookups and task wake-ups.
- *Build:* a frame carrying N batches with N result slots (~150 lines).
- *Measure:* CPU per batch with 1, 4 and 16 streams per producer connection.
- *Expected:* per-batch fixed cost falls by roughly the share that is frame dispatch, which experiment 1 measures.

**8. Producer-side batch compression, broker never decompresses.**
- *Hypothesis:* zstd level 1 or lz4 on 4-60 KB batches of real protobuf records cuts wire and disk bytes 2-4× at a client CPU cost smaller than the broker CPU it saves.
- *Build:* a codec flag and a compressed blob with uncompressed columns (~150 lines).
- *Measure:* ratio, client compression µs per record, and broker CPU per record, on real staging records.
- *Expected:* a net broker CPU reduction, by experiment 1's per-byte term. Redpanda's baseline already got this effect from snappy ([zstd](https://raw.githubusercontent.com/facebook/zstd/dev/README.md); [KIP-390](https://cwiki.apache.org/confluence/display/KAFKA/KIP-390%3A+Support+Compression+Level)).

**9. Interned stream and writer ids in the session.**
- *Hypothesis:* `u32` ids instead of names save about 40 bytes per batch and the string compare or hash per batch in store lookups.
- *Build:* a Hello mapping and id-keyed maps (~150 lines).
- *Measure:* CPU per batch at small batches.
- *Expected:* small at 100-record batches and visible at 10-30 records. The analogue is KIP-227's removal of repeated per-partition data ([KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability)).

**10. Go producer client cost.**
- *Hypothesis:* a Go encoder for the §6 batch, with a preallocated buffer, columns appended and xxh3 from `github.com/zeebo/xxh3`, uses much less client CPU per record than franz-go's record-plus-headers batching.
- *Build:* about 300 lines of Go against the Rust server.
- *Measure:* client CPU per record and allocations per record, using `go test -bench`.
- *Expected:* 2× or better lower client CPU. That is a guess: no per-record header structs and one hash.

**11. Byte-credit push for subscribers against long-poll fetch.**
- *Hypothesis:* push with a 1-4 MB credit window gives lower end-to-end p99 and lower CPU than repeated fetch requests, and isolates a slow subscription.
- *Build:* a Credit frame and a per-subscription window (~200 lines).
- *Measure:* end-to-end p99 at 150k/s with 1, 4 and 16 subscribers, plus one deliberately slow subscriber.
- *Expected:* the p99 win over fetch is roughly the poll interval ([Pulsar flow](https://pulsar.apache.org/docs/next/developing-binary-protocol/); [Aeron](https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification)).

**12. Server-paced producer window.**
- *Hypothesis:* putting `window_bytes` in each ack and halving it when the WAL queue exceeds a target keeps p99 bounded under overload, where TCP-only backpressure lets queues grow.
- *Build:* about 150 lines.
- *Measure:* p99 and p99.9 at 1.5× capacity.
- *Expected:* bounded tail instead of a J-curve ([ADR-50](https://raw.githubusercontent.com/nats-io/nats-architecture-and-design/main/adr/ADR-50.md); [Disruptor on J-curves](https://lmax-exchange.github.io/disruptor/disruptor.html)).

**13. io_uring receive and send against tokio epoll.**
- *Hypothesis:* multishot receive with a provided buffer ring, plus batched sends, cuts syscalls per record but changes CPU little once batching is on.
- *Build:* the server's network loop on `io-uring` or `tokio-uring` (~400 lines).
- *Measure:* syscalls per record, CPU per record, and p99 at 1 core.
- *Expected:* within ±10% CPU at a few dozen connections ([monoio](https://github.com/ukernel/monoio/blob/master/docs/en/benchmark.md); [giong](https://docs.rs/crate/giong/0.2.0)).

**14. TCP_NODELAY with application coalescing, against Nagle and against TCP_CORK.**
- *Hypothesis:* NODELAY plus Precog's own coalescing is best. Nagle adds delayed-ACK stalls, and cork adds nothing.
- *Build:* socket option flags (~50 lines).
- *Measure:* p99 and packets per record ([tcp(7)](https://man.archlinux.org/man/tcp.7.en.txt)).
- *Expected:* confirms the current choice. This is a cheap guard.

**15. TCP_NOTSENT_LOWAT and socket buffer sizes on a mixed connection.**
- *Hypothesis:* during a catch-up push on the same connection, a 128 KB low-water mark cuts ack p99 several-fold without lowering throughput.
- *Build:* about 80 lines.
- *Measure:* ack p99 during a 1 GB catch-up ([ip-sysctl](https://docs.kernel.org/networking/ip-sysctl.html)).
- *Expected:* large p99 improvement under mixed load and none otherwise. That is a guess.

**16. Head-of-line blocking: chunked push frames against a separate bulk connection.**
- *Hypothesis:* capping push frames at 64 KB and interleaving acks, or using a second connection, keeps ack p99 flat during catch-up.
- *Build:* about 150 lines.
- *Measure:* ack p99 with and without a concurrent bulk reader ([RFC 9113](https://www.rfc-editor.org/rfc/rfc9113#section-4.2)).
- *Expected:* comparable fixes. The second connection is simpler.

**17. Columnar market-data codec on the client.**
- *Hypothesis:* transposing trade and quote fields (delta-of-delta time, fixed-point price deltas, dictionary symbol), then zstd, is 2-4× smaller than zstd on row-wise protobuf. That figure is a guess.
- *Build:* a codec for two message types (~400 lines).
- *Measure:* bytes per record and encode and decode µs per record on real records.
- *Expected:* large byte savings, but a consumer-visible format. Gate it on whether brrrrr and the archiver can consume columns directly ([Gorilla](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf); [Chronicle delta](https://vanilla-java.github.io/2016/07/22/Improving-performance-with-simple-compression.html); [DBN](https://databento.com/docs/standards-and-conventions/databento-binary-encoding)).

**18. Per-stream zstd dictionaries for small batches.**
- *Hypothesis:* at 4-16 KB batches, a 64 KB dictionary trained on the stream raises the ratio substantially and speeds compression.
- *Build:* train with `zstd::dict::from_samples` and refer to it by id (~150 lines).
- *Measure:* ratio and speed by batch size.
- *Expected:* a large gain below about 8 KB and little above 64 KB ([zstd README](https://raw.githubusercontent.com/facebook/zstd/dev/README.md); [ScyllaDB](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/config-change/advanced-internode-compression.html)).

**19. Replication frames batched per peer per group commit.**
- *Hypothesis:* sending each peer one Replicate frame per commit with all batches, and getting back one cumulative durable-ack, cuts inter-node frames and wake-ups by the number of batches per commit.
- *Build:* about 200 lines.
- *Measure:* inter-node frames per second and server CPU per record.
- *Expected:* moderate server CPU win at many writers per node.

**20. Columnar per-record fields against interleaved fixed fields against varints.**
- *Hypothesis:* `len[]`, `hash[]` and `time[]` columns validate and index faster than PCB2's interleaved per-event headers or Kafka-style varints.
- *Build:* three parsers (~200 lines).
- *Measure:* ns per record to validate and build an index, and bytes per record.
- *Expected:* columns at least as fast as interleaved fields, 2× or more faster than varints, at 1-4 more bytes per record ([Stream VByte](https://arxiv.org/abs/1709.08990v2); [SBE](https://github.com/aeron-io/simple-binary-encoding/wiki/Design-Principles)).

**21. Batched, prefetched dedup probes.**
- *Hypothesis:* with a hash column, probing a batch's hashes with software prefetch, or two passes, beats one probe at a time once the index exceeds cache: 8M entries × 8 B = 64 MB.
- *Build:* about 150 lines.
- *Measure:* ns per probe at 1M, 8M and 16M entries.
- *Expected:* 1.5-3× faster probes at sizes beyond cache. That is a guess from standard hash-table prefetching results.

**22. Catch-up reads by sendfile against pread plus write against the tail cache.**
- *Hypothesis:* for a 2-hour catch-up, `sendfile` of batch runs from the shared log uses less CPU per GB than pread plus write. With 4-60 KB runs, the syscall count stays acceptable.
- *Build:* about 250 lines over a `precog-storage` segment.
- *Measure:* CPU per GB and MB/s for one stream at 1%, 10% and 50% of the log.
- *Expected:* a sendfile gain that grows with contiguity, which motivates per-stream segment groups ([Kafka design](https://kafka.apache.org/42/design/design/); [IBM](https://www.linux.com/news/efficient-data-transfer-through-zero-copy/)).

**23. Lease-based active/standby (design b) against shadow hashes (design c).**
- *Hypothesis:* the lease is simpler and saves slightly more bytes, but loses events that only the standby saw. Measure how often that happens on real feeds.
- *Build:* compare the two replicas' dedup ids on staging over 24 hours, offline (~150 lines).
- *Measure:* the fraction of ids seen by only one replica.
- *Expected:* this decides whether design (b) is acceptable. Unknown until measured.

**24. Thread-per-core with connections pinned, against tokio work-stealing.**
- *Hypothesis:* on a 1-CPU pod it makes no difference. On larger pods, pinning connections to a core avoids cross-core wake-ups.
- *Build:* a `current_thread` runtime per core with `SO_REUSEPORT` (~200 lines).
- *Measure:* CPU per record and p99 at 1, 2 and 4 cores ([Redpanda TPC](https://www.redpanda.com/blog/tpc-buffers)).
- *Expected:* nil at 1 core. Run it only if pods grow.

**25. CRC cost: one hardware crc32c per batch per hop, against per-record checksums.**
- *Hypothesis:* a batch CRC at roughly 10+ GB/s (crc32c SSE4.2) is under 3% of the per-record budget, so Iggy-style per-record checksums are unnecessary.
- *Build:* about 80 lines.
- *Measure:* ns per KB.
- *Expected:* confirms keeping the batch CRC only, and verifying it once per receiving node ([Iggy messages](https://iggy.apache.org/docs/binary-protocol/messages.md)).

**26. Zero-copy send (`SEND_ZC`, `MSG_ZEROCOPY`) for large pushes, as a negative control.**
- *Hypothesis:* no gain on loopback or veth, and a loss below 10 KB.
- *Build:* about 150 lines.
- *Measure:* CPU per GB at 4 KB, 64 KB and 1 MB.
- *Expected:* no gain, per the kernel documentation. This confirms that the protocol need not depend on it ([kernel doc](https://docs.kernel.org/networking/msg_zerocopy.html)).

**27. Busy-polling, as a negative control.**
- *Hypothesis:* `SO_BUSY_POLL` lowers p50 slightly but costs a large share of a 1-CPU budget.
- *Build:* socket options (~50 lines).
- *Measure:* CPU per record and p99 ([NAPI](https://docs.kernel.org/networking/napi.html)).
- *Expected:* reject it for this deployment.

**28. Cap'n Proto-style zero-byte packing of the uncompressed columns.**
- *Hypothesis:* time-delta and length columns are zero-heavy, so packing them, or using u16 time deltas with an escape, saves 3-5 bytes per record cheaply when the blob is uncompressed.
- *Build:* about 120 lines.
- *Measure:* bytes per record and ns per record ([Cap'n Proto encoding](https://capnproto.org/encoding.html)).
- *Expected:* a small win, only relevant when compression is off.

### How the experiments combine

Experiments 1-8 together, plus 9, 11 and 19, form the expected path to the 3× target. The arithmetic below is a guess to be confirmed:
1. Batches of 100 or more with W in flight: experiment 2.
2. Remove about 45% of bytes through shadow hashes (experiment 3) and the hash column (experiment 4).
3. Remove user-space copies: experiment 6.
4. Compress before send: experiment 8.

Together these should bring cluster CPU per record from about 3.3-4.8 µs toward the ~1.1 µs budget. Experiments 13-16 and 20-28 are refinements and guards. Experiment 17 is the largest potential byte win, but it also changes what consumers receive.
