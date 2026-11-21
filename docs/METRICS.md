# Metrics Precog should export

This lists what a node's `/metrics` lacks, judged against what operating it beside Redpanda needs. Plan R7 asks for
"Prometheus metrics (append, fsync, repair, dedup, per-group lag)". Today only dedup and per-group lag are there.

It was written from the staging shadow run of live-ingestion#1388 (2026-11-21, image `sha-c4241b61`). There, 35
producers wrote every raw event to both `redpanda-raw-internal` and Precog for hours. Each gap below is one we
hit: a question we could not answer from Precog's own metrics, so we answered it with a `du` in the pod, a
model of the dedup code, or not at all. The matching Redpanda metric is named where live-ingestion already
uses one.

## Today

| Metric | Type | Notes |
| --- | --- | --- |
| `precog_batches`, `precog_events`, `precog_bytes` | gauge | What the store holds now. Retention lowers them, so their `rate()` is not ingest |
| `precog_wal_bytes`, `precog_log_bytes`, `precog_segments` | gauge | The one segment log |
| `precog_fsyncs_total` | counter | Group commits; count only, no duration |
| `precog_recovery_scanned_bytes` | gauge | |
| `precog_dedup_dropped_total`, `precog_dedup_entries`, `precog_dedup_window_seconds` | counter, gauge, gauge | Node totals |
| `precog_fatal` | gauge | Storage fenced; `/health` answers 503 |
| `precog_streams_hot`, `precog_streams_dormant`, `precog_stream_loads_total`, `precog_stream_evictions_total`, `precog_stream_load_microseconds_total` | gauges, counters | |
| `precog_retention_copied_bytes_total`, `precog_retention_copied_batches_total`, `precog_retention_forced_total{stream}` | counters | |
| `precog_group_lag_events{group,stream}`, `precog_group_commit_age_seconds{group,stream}` | gauges | |

## Missing, by priority

### P1: needed before readers move (rawBroker: precog)

**Append latency.** Nothing shows how long an append takes, so a slow node is invisible until producers' queues
back up. Redpanda's `redpanda_kafka_request_latency_seconds{redpanda_request="produce"}` gave us broker-side
p99 2.0-2.3 ms in the run. For Precog we had nothing to set beside it.
- `precog_append_seconds` (histogram): from a client batch's arrival to its ack, so durable on `copies` domains.
  Label `outcome` = `ok|duplicate|rejected|error`. Buckets from 0.5 ms to 5 s.
- `precog_replicate_seconds{peer}` (histogram): from sending a batch to a peer to its durable reply. This is
  the cross-domain half of the ack, and it shows which peer is slow.

**fsync duration.** `precog_fsyncs_total` counts group commits but cannot show a slow disk. On `local-path`
volumes that share a node with Redpanda, disk contention is the likely cause of a slow node.
- `precog_fsync_seconds` (histogram): one group commit's `write` plus `fdatasync`.
- `precog_fsync_batch_bytes` (histogram): bytes per group commit, to show whether group commit is grouping.

**Ingest counters per stream.** The comparison with Redpanda had to use the producers' own counters
(`ingestion_shadow_messages_published_total`), because Precog's `precog_events` is a gauge of what is stored.
Its rate is not ingest once retention runs, and it is a node total. Step 5 of the rollout (Precog-only
producers) has no producer-side counter left to fall back on. Redpanda equivalents:
`redpanda_kafka_records_produced_total` and kafka-exporter's per-partition offsets.
- `precog_appended_events_total{stream}`, `precog_appended_bytes_total{stream}` (counters): events and bytes
  kept on the node that accepted the append, after dedup.
- `precog_replicated_events_total{stream}`, `precog_replicated_bytes_total{stream}` (counters): the same for
  copies this node stored for a peer.
- `precog_dedup_dropped_total{stream}`: the same counter, split by stream. Node totals made the per-node
  imbalance (13.8M, 52.9M and 42.8M drops in an hour) look like a fault. It is only which streams each node
  accepts, and a per-stream split shows that directly.
- Cardinality: one series per stream per metric. Staging configures 62 streams (48 had traffic in the run). Dormant streams keep their series
  (counters).

**Copies below target.** `target_copies` is 3 and an ack needs `copies` (2). Nothing shows a batch that
stayed at 2: a peer down, or repair behind. Redpanda equivalent:
`kafka_topic_partition_under_replicated_partition`, which live-ingestion alerts on.
- `precog_under_replicated_batches` (gauge): batches this node accepted that have fewer than `target_copies`
  durable copies it knows of.
- `precog_repair_fetched_bytes_total{peer}` (counter), `precog_repair_behind_bytes{peer}` (gauge): what
  anti-entropy fetched, and what it still has to fetch, per peer.
- `precog_peer_up{peer}` (gauge, 0/1): the node's own view of each peer's connection.

**Volume space.** On `local-path`, `kubelet_volume_stats_*` reports the whole node filesystem, which here is
shared with Redpanda's volume. Precog's growth (+15 GiB/h per node until the node cap) could only be measured
with `du -sk /var/lib/precog` in the pod.
- `precog_data_dir_bytes` (gauge): bytes on disk under `data_dir` (segments, snapshots, the streams table).
- `precog_data_dir_free_bytes`, `precog_data_dir_capacity_bytes` (gauges): `statvfs` of the volume.
- `precog_retention_bytes_limit` (gauge): the node cap in force, so a disk alert can compare against it.

### P2: needed to run it unattended

**Memory by owner.** Working set rose from 240 to 1610 MiB per node in an hour. From the metrics alone we could
not tell the dedup index from the store's batch index from hot streams. The cause turned out to be replicas'
recorded dedup ids, which never expired (precog#37). `precog_dedup_entries` hid it, because it sums indexed
and pending ids. Redpanda equivalent: `vectorized_dedup_partition_index_memory_bytes`.
- `precog_dedup_entries{state="indexed|pending"}` (gauge): split the existing gauge.
- `precog_dedup_memory_bytes` (gauge): the maps' and deques' allocated capacity, not their length.
- `precog_store_index_bytes` (gauge): the batch index (`order` and the per-log maps) held in RAM.
- `process_resident_memory_bytes`, `process_cpu_seconds_total`, `process_open_fds` (standard process
  collector). The cluster's cAdvisor scrape has no RSS series, so we read `memory.stat` in the pod.

**Client side.**
- `precog_client_connections` (gauge): open client connections, by `role` = `producer|reader`.
- `precog_requests_total{kind,outcome}` (counter): appends, reads, commits and controls, with ok/error. This
  includes authentication failures (`outcome="unauthenticated"`), which are invisible today.
- `precog_read_bytes_total{stream}` (counter): what readers fetched. This replaces
  `redpanda_kafka_request_bytes_total{redpanda_request="consume"}` once the archivers and brrrrr read Precog.

**Retention in progress.** A node's first cap hit starts copy-forward. On staging that is about 4.5 h after
start, when the 74 GiB cap fills.
- `precog_retention_runs_total` (counter), `precog_retention_seconds` (histogram): how often it ran and how
  long it took.
- `precog_stream_retained_bytes{stream}` (gauge): what each stream holds against its own `retention_bytes`.
  This replaces Redpanda's per-topic size that topicsync's budgets are checked against.

**Identity.**
- `precog_build_info{version,commit}` (gauge, 1): which image a node runs. During a roll the pin and the pods
  disagree for a while.
- `precog_node_info{node,domain}` (gauge, 1).

## Conventions

- Names: `precog_` prefix, `_total` counters, base units (`_seconds`, `_bytes`).
- Labels: only `stream`, `group`, `peer`, `outcome`, `kind`, `state`, `role`. Never a writer, epoch or
  key: writers change on every producer restart.
- Histograms: fixed buckets. 0.0005 to 5 s for append, replicate and fsync; powers of 4 from 4 KiB to 64 MiB
  for fsync batch bytes.
- Every counter is node-local and starts at 0 on restart (Prometheus handles resets). Gauges are computed at
  scrape time from state the node already holds, like `precog_group_lag_events`.
