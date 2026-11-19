# Redpanda dedup fork baseline (2026-11-18)

`ghcr.io/dream-faster/redpanda:71f80ccdf79917925ec1e3fb4e958017480a65fc`, the image pinned for
live-ingestion's `redpanda-raw-internal`. 3 brokers at 1 CPU / 4000Mi, RF 3, topic `raw.bench.trades`
(3 partitions, 5-minute dedup window keyed by the `dedup-id` header), started by
`bench/redpanda/cluster.sh`. Load from `live-ingestion/benchmarks/redpanda-dedup/loadgen`
(franz-go, `acks=all`, snappy, idempotence off, 8 producers x 3 symbols, ~232-byte records),
in a container on the same 4-vCPU host.

| run | resends | in flight | records/s | p50 ms | p99 ms | dropped | broker CPU |
|---|---|---|---:|---:|---:|---:|---|
| `rp-dup0` | 0% | 2,000 | 151,365 | 10.9 | 19.3 | 0% | |
| `rp-dup20` | 20% | 2,000 | 142,797 | 11.6 | 19.1 | 20.01% | |
| `rp-peak-dup20` | 20% | 20,000 (16 producers) | 245,413 | 46.4 | 223.4 | 19.97% | 18%, 17%, 45% |
| `rp-peak2-dup20` | 20% | 50,000 (32 producers) | 48,066 | 16.0 | 150.2 | 19.98% | ~1% each |

The load generator shares the host and gets about one core: in both peak runs the brokers were
far from their 1 CPU, so the peak numbers are the client's limit, not Redpanda's (the 32-producer
run is the client collapsing). Comparisons with Precog therefore use broker CPU per record (per
core) and p99 at a fixed rate, and a peak only with a load generator that leaves the brokers the
bottleneck.

Reproduce: `IMAGE=... bench/redpanda/cluster.sh up`, `bench/redpanda/cluster.sh topic raw.bench.trades 300000`,
then the loadgen with `-brokers=$(bench/redpanda/cluster.sh brokers)` from a container on network
`precog-rp-bench`.
