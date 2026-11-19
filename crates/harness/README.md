# precog-harness

Broker scenarios against a real 3-node cluster in Docker: Precog, or the Redpanda dedup fork
(`ghcr.io/dream-faster/redpanda:71f80cc…`) for the scenarios that apply to both. It is a port of
the mechanics of live-ingestion's `benchmarks/redpanda-dedup` (loadgen, capacitycheck,
upgradecheck, shardcheck), extended with fault and edge-case scenarios. Its workload has the same
shape, with synthetic payloads only: ~230-byte JSON trades, 8 producers × 3 symbols, per-event
dedup ids `live-producer-<exchange>-<n>-<trade>`, resends of one of the 64 latest records byte for
byte, 20k records/s, and ~256 KiB records every Nth where a scenario asks for them.

```bash
cargo build --release -p precog-mesh --bin precog-node -p precog-harness
sudo target/release/precog-harness list
sudo target/release/precog-harness run                                  # every Precog scenario
sudo target/release/precog-harness run --only kill-mid-append,disk-full
sudo target/release/precog-harness run --system redpanda                # the shared scenarios
```

Root is needed for the network cuts (iptables on the host's `DOCKER-USER` chain, so no image needs
iptables) and the full disk (a host tmpfs under a node's data dir). The results go to `--out`
(default `results/harness`): `results.json` (every check with its detail, plus metrics),
`summary.md`, and every node's log per scenario. The exit status is 1 if a scenario fails.

## What every scenario asserts

After the faults are healed (nodes restarted, resumed, the network restored) and Precog's nodes
are repaired until they agree:

- **acked implies readable**: every acknowledged event is on every node (Redpanda: in the topic);
- **no extra copies**: one copy per event, unless the scenario allows more (an empty id, a resend
  after the window);
- **order per writer log**: sequence numbers (Redpanda: offsets) only increase in read order;
- **no phantoms**: nothing is read that no producer sent;
- **the nodes agree** (Precog): every node holds the same records.

Loss that is expected by design (write caching when every node dies) is reported as `info`, never
failed. `precog-harness list` names every scenario with a line on what it covers.

## Faults

| Fault | How |
|---|---|
| crash | `docker kill -s KILL`, then `docker start` on the same data dir |
| freeze | `docker kill -s STOP` / `CONT` (a hung process: connections stay open) |
| network | iptables DROP between node IPs (peers) and between the host and a node (clients) |
| full disk | the node's data dir is a tmpfs of a few MiB, remounted bigger to recover |
| clock skew | libfaketime (`FAKETIME=+600s`) in the node's container |
| fsync failure | not injected: needs a block device that fails on demand (dm-flakey); covered in-process by `crates/storage/tests/log.rs` |

## CI

`.github/workflows/harness.yml`, never a required gate: run it from the Actions tab
(`workflow_dispatch`, choosing the system and optionally scenarios), add the `harness` label to a
pull request, or let the nightly schedule run both systems. Results are uploaded as artifacts.
