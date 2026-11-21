# Precog as a durable session log: plan

Status: adopted for the session-log product (2026-11-21). It does not change
[`PLAN.md`](PLAN.md), whose first customer, live-ingestion's raw cluster, still comes first and is
the only source of production hours (§5, "Order of work"). It narrows
[`S2-PLAN.md`](S2-PLAN.md)'s direction to one product: Precog is the **durable log of agent
sessions**, the storage and transport underneath observability products, not one of them.
Dashboards, search, evals and analytics are out of scope. S2-PLAN's own open decision, S0 (a
hosted service or a self-hosted broker), stays open (§7).

Evidence for the gaps below: [`results/session-log.md`](../results/session-log.md) (measured on
main, 2026-11-21) and the code as it is on main.

## 1. The product

One stream per agent session. Every LLM call, tool call and state change is an event, appended
durably and readable live or later, from any position, for as long as the stream's retention says.

| | |
|---|---|
| Unit | A session stream: created by its first append, named `<tenant>/<session>` (any prefix scheme) |
| Scale | Millions of streams per cluster, most idle; events from 100 bytes to several MiB |
| Writers | One or a few per session (the agent, its tools, a gateway); a stream-level order is opt-in (owned streams, phase 2) |
| Readers | A live tail (a UI, a second device), a resumed reader (a reload), a replay or export |
| Retention | By class, hours to months, and a delete on request |
| Tenants | Many, each confined to its own prefix |

### Contract (added to PLAN.md's four guarantees)

5. **A stream's retention is its class's, within the node's cap.** A stream never serves a batch
   older than its class's age limit, and keeps what its class retains whatever other streams
   write, as long as what all streams retain fits the node's cap. Past the cap, the cap wins and
   cuts streams by their share (counted, and visible in `/metrics`).
6. **A tenant reaches only its own streams.** A token is confined to stream prefixes and to
   reading or writing; only a node's peers replicate.
7. **A deleted stream stays deleted on every node that has the delete.** Such a node serves
   nothing of it and takes nothing of it back, from a peer, a retry or its own restart, and
   repair brings the delete to a node that was down. Two limits: the bytes stay on disk until
   their segments are dropped (a delete is logical, like a retention trim), and a node away for
   longer than its peers keep the delete's record must be replaced empty.

## 2. Where main is today

| Gap | Today | Evidence |
|---|---|---|
| Retention per stream | Static node config keyed by the exact stream name. A stream not named there loses its oldest data at the node cap, whoever owns it | `NodeConfig::streams`, `Node::apply_retention` |
| Tenancy | One shared token. A token holder may claim any peer id. No scopes, no rate limits or quotas, no metering | `Node::authenticate` |
| Delete | None. A trim is local to a node and logical | `Store::trim_stream` |
| Transport security | Plain TCP | `transport.rs` |
| Isolation | One log and one group commit per node: a 256-byte writer's p99 ack went from 3 ms to 202 ms beside two writers of 4 MiB events | results, "Neighbours" |
| Event size | A batch is at most 8 MiB, refused above. The benchmarks so far used events of 64 to about 230 bytes | results, "Event sizes"; `results/compare.md`, `results/s2.md` |
| Stream-level order | Per writer log only. Owned streams are a POC (`crates/lab/src/s2`) | S2-PLAN §3.2 |
| API | Native TCP, Rust and Go SDKs. The REST and SSE gateway is a POC outside the workspace. No request lists streams | `crates/s2-lab`, `proto::Control` |
| Live tail | A feed starts at the stream's first batch or at now, not at a cursor: a reader that resumes reads by cursor, then polls. A feed of one stream walks the node's whole log, 1,024 records a step | `proto::Subscribe`, `Store::tail` |
| Re-sent context | Stored as sent: 350 MB for 64 sessions that hold 11 MB of new content | results, "Sessions" |
| Metrics | Reader lag is a series per (group, stream), and the cap's forced cuts one per stream cut: both grow with the number of sessions. Stream and group names go into labels unescaped | `Node::metrics` |
| Reader positions | One per (group, stream), kept in RAM and in a snapshot for good | `__positions`, `Node::commit_position` |
| Operations | No benchmark on separate machines, no soak, no backup, no upgrade or rollback of a running cluster (each format's older version has a read test). Operations and hardening are PLAN's M7, planned | `results/s2.md` (its caveats), PLAN M7 |
| Capacity | 3 static nodes, each holding everything; no cold tier | PLAN M4, M8 |

What is already there and stays as it is: durable acks on 2 of 3, idempotent retries, repair,
cursors valid on every node, reader groups, live feeds, dormant streams on disk (10M streams settle
at about 600 MiB of heap, after a peak of 1.6 to 4.7 GiB while they are created: `results/s2.md`),
the segment cleaner, and the verification gates.

## 3. Phases

Each row is one pull request in a stack (§5) unless it says otherwise. "Verified by" names the
formal check the PR adds, in the same PR (AGENTS.md); every row also gets acceptance scenarios
against real node processes for what it promises.

### Phase 1: the session-log core in the broker

| ID | Change | Done when | Verified by |
|---|---|---|---|
| A1 | **Stream classes.** A `streams` key ending in `*` configures every stream with that prefix; the longest match wins, an exact name wins over any class. Retention passes trim the class's streams in RAM, a stream is trimmed before a read serves it, and the cleaner copies forward what class streams retain | Two tenants' session streams keep exactly their class's retention on one node, through the node cap, idle streams and restarts; a pass costs nothing per idle stream | Property tests: class lookup against a reference; a store with classes against one with every stream named; a trim against a reference model, the cleaner's copy racing it (`spec/store/Cleaner.tla`) |
| A2 | **Scoped tokens.** Tokens in the node config, each with stream prefixes and `read`/`write`; a separate peer token; the shared token keeps working as an unscoped one | A tenant token cannot read, write, tail or commit on another prefix, cannot read the node's state, and cannot replicate; acceptance scenarios on 3 nodes (`features/tokens.feature`) | Property tests: the scope rule against the rule spelled out, every request kind's need, who a connection is; 3 nodes over TCP |
| A3 | **Stream delete.** `Control::Delete` sets the stream's epoch floor, a record the mesh replicates and repair follows; a node with the floor forgets and refuses the stream's writer logs below it | A stream deleted while a node is down is deleted there after repair and never returns; a new stream of the same name works (`features/delete.feature`) | TLA+: `spec/mesh/Delete.tla` (`DeletedStaysDeleted`, `AckedIsDeleted`, a counterexample config per rule); model tests of the store with deletes |
| A6 | **Bounded metrics and positions.** Reader lag per class in place of per stream for a class's streams (streams the config names keep their own series); counters per token (bytes and requests in and out, refusals: the basis for metering), per class (trims) and for deletes; names escaped in labels; a reader position expires after its class's age limit | `/metrics` has the same number of series at 10 streams and at 1M; positions older than their stream's age limit are gone after a pass (`features/metrics.feature`) | Property tests: the series set against the rule spelled out, position expiry against a reference through restarts |
| A5 | **Fair shares of the log.** A byte rate limit per token at the accepting node: an append over it waits, and is refused past a bound. The neighbour probe moves into the repo first (part of P1), since this row is measured by it | The neighbour test's small writer stays within 3x its p99 alone (measured: within 1.5x, results, "Neighbours, with a rate"); a token over its rate is delayed, not its neighbours (`features/limits.feature`) | Property tests of the limiter (what is admitted keeps to the rate over every stretch; a writer at its rate never waits). The neighbour test is reproducible from `bench/session/neighbours.sh` and not a perf-gate case: a p99 on a shared runner is not steady enough for a 15% gate |
| A7 | **TLS.** rustls on the client and peer transports (`tls://` addresses), certificates from files, read again when they change | A cluster over TLS replicates, repairs and restarts (`features/tls.feature`; the whole suite is not run twice); a plain client is refused; a certificate replaced on disk is served to new connections without a restart, and a broken one leaves the old one serving | Acceptance scenarios for each (the handshake is the library's); a property test of address parsing |
| A4 | **Events above a batch.** The SDK splits one event over several batches and its reader puts it back together; the batch cap stays (decision below) | A 64 MiB event round-trips through a node's restart (`features/large.feature`); a reader resuming inside an event restarts at its first chunk; a reader without the feature sees the chunks as events | Property tests of the chunk codec and the reader (round trip at any chunk size, any start, any chunks missing); the SDK's cluster model |

A4's decision: chunk in the SDK, do not raise the cap. An 8,000 KiB batch already takes 41 ms to
acknowledge alone and holds the node's one group commit while it is written (results, "Event
sizes" and "Neighbours"); a larger batch would hold it longer for everyone, and the replicate
and repair paths budget memory by the cap.

A5's limit is per node: a token that appends through all three nodes gets three times its rate.
A cluster-wide rate needs the control plane (phase 6); until then the configured rate is a third
of what the tenant is sold. A5 does not bound stored bytes or stream counts per tenant: quotas
need a per-tenant total, which the gateway's metering (phase 3) or the control plane supplies.

### Phase 2: a stream-level order (S2-PLAN C1)

| ID | Change | Done when | Verified by |
|---|---|---|---|
| B1 | **Owned streams in the node.** One owner task per stream assigns dense sequence numbers, checks `match_seq_num` and fencing tokens, records `fence` and `trim`; majority `copies` enforced | S2's semantics as acceptance scenarios | TLA+: owner takeover with a zombie owner; a Porcupine-style linearizability check under SIGKILL and SIGSTOP |
| B2 | **Owner failover.** An owner epoch per owned stream that fences a superseded owner, takeover from a majority tail, truncation of minority losers before repair spreads them. It reuses A3's mechanism (a stream's floor refuses writer logs by epoch) and must compose with a delete of the same stream | A zombie stops at its next batch; no sequence number is lost or reused; a delete during a takeover deletes | The same model, with the floor and a delete |

### Phase 3: the gateway (S2-PLAN C3), a workspace crate

| ID | Change | Done when | Verified by |
|---|---|---|---|
| C0 | **A feed from a cursor**, that reads its stream's batches and not the node's log | A reader resumes a live tail where it stopped; its cost is its stream's batches | The feed against cursor reads, as a model test; an acceptance scenario with a reload |
| C1 | **REST and SSE**, S2's shapes, over owned streams; token auth from A2; streams listed by prefix | S2's CLI and SDK examples run unchanged | S2's own conformance examples in CI; a property test of each request's mapping |
| C2 | **OTLP/HTTP ingest** mapped onto session streams by a resource attribute | The OpenTelemetry SDK's exporter fills session streams; a span is appended once under retries | A property test of the mapping; an acceptance scenario with the exporter under a node kill |
| C3 | **Python and TypeScript** through the gateway's API (no native clients); the Go client gets `Delete` | Their SDK examples in CI | The examples |
| C4 | **Metering**: logical bytes and requests per token at the gateway, stored bytes per prefix from the nodes | A tenant's bill is reproducible from the counters | A property test: the counters against the appends made |

### Phase 4: re-sent context stored once (agentwire's AWD1)

Opt-in. The codec runs in the client and the gateway, never in the broker, which stores opaque
bytes as now: an event's payload is a delta against the previous event of its writer log, with
the distance back to the last event stored whole. D2 adds the one thing the broker has to know,
where a chain starts, so that retention does not cut one in two.

| ID | Change | Done when | Verified by |
|---|---|---|---|
| D1 | `precog-client` feature `delta`: `Producer::set_delta`, a reader that starts anywhere by going back to the chain's first event | Byte-exact round trips; a mid-chain start within 20 ms at 64 events per chain | A property test: any event sequence round-trips from any start; agentwire's own conformance vectors |
| D2 | The broker knows a delta stream by its class: it trims only at chain boundaries, and refuses dedup keys and compaction on it | Retention never leaves a delta without its base | The trim model (A1) with chains; an acceptance scenario through the node cap |

### Phase 5: production readiness

| ID | Work | Done when |
|---|---|---|
| P1 | The session workload in the repo (`bench/session`): streams per session, event sizes to MiBs, neighbours. The neighbour part lands with A5 | Reproducible from one command; its numbers in results, with the stack's total cost against main (each PR's gates compare it with its base only) |
| P2 | A multi-machine benchmark: 3 machines, 3 disks | P1's tables on independent hardware: ack p50 and p99 by event size, the neighbour test, and the time to the first ack after a node is killed |
| P3 | Harness scenarios for A1 to A3 under faults (kill, pause, partition, disk loss) | Green in the optional `harness` job |
| P4 | A 72-hour soak at the session workload with retention turning the log over | No invariant violation, flat RSS; the cleaner's write amplification with class retention longer than the cap's turnover is measured and stated |
| P5 | Backup and restore of a node; an upgrade and a rollback of a 3-node cluster across A2 (the peers' token) and A3 (the summary's version, delete records an older node stores without applying) | A scenario for each, nodes of both versions serving together |
| P6 | Runbook (token rotation, adopting `peer_token`, replacing a node, a delete that a node missed), alerts, a security review of the token and TLS paths | Reviewed by the owner |

### Phase 6: capacity and cost (after production)

The cold tier for sealed segments (S2-PLAN C5; PLAN M8) and the control plane (PLAN M4:
membership, placement beyond 3 nodes, users and tokens). Neither blocks a first production
deployment bounded by one 3-node cluster's disks.

## 4. Designs for phase 1

### A1 Stream classes

- **Config.** `"streams": {"acme/*": {"retention_ms": 2592000000}}`. `NodeConfig::stream(name)`
  is the one lookup: the exact name, else the longest class whose prefix the name starts with.
  A class cannot be `compacted` (a compacted stream's newest values live in RAM per configured
  name): refused when the node opens.
- **Trimming without walking streams.** A stream's bytes grow only by appends, which put it in
  RAM, so the pass trims the class streams in RAM and no others. An idle stream ages on disk: it
  is trimmed when it is next loaded for a read, before the read is served, so nothing past its
  age limit is ever served, and what it no longer retains is not copied forward.
- **The cleaner.** For the segments the node cap is about to drop, it asks which streams hold
  batches there (the segment's index names them) instead of asking every configured stream, so a
  pass costs what those segments hold.
- **Known ceilings.** When what streams retain exceeds the node cap, the cap still wins, and it
  cuts streams by their share as today; with many class streams that loop is quadratic in the
  streams being cut. A class bounds each of its streams, not their sum: a storage quota per
  tenant is not part of A1. Both marked in the code or the architecture doc.

### A2 Scoped tokens

- **Config.** `"tokens": [{"token": "...", "prefixes": ["acme/"], "read": true, "write": true}]`
  and `"peer_token"`. A connection's `Hello` resolves to a peer (the peers' token only), or to a
  client with a scope. The old shared `token` stays valid as an unscoped client token, and as the
  peers' token while no scoped token is configured, so today's deployments run unchanged. A
  config that would not confine a token is refused when the node opens: an empty token or
  prefix, a token given twice, scoped tokens without a `peer_token`, a misspelt field.
- **Enforcement** where requests enter, by one rule (`need`): `Node::handle` for requests and
  `Node::push_loop` for live feeds. Appends go by the batch's stream; reads, cursors, commits,
  positions, latest values, `FetchLog` and a feed of one stream by the request's stream (a
  commit may name only writer logs the stream has, and is bounded in size); a delete needs
  `write`; a feed of every stream, `Status`, `Repair`, `Fetch` and `Inventory` need a peer or the
  unscoped token; `Replicate` needs a peer. No request lists streams, so a tenant cannot
  enumerate another's (or its own: phase 3).
- **Tokens are compared in constant time**, every configured one, so a miss costs what a hit does.
- **Not covered.** `/metrics` is a separate listener with no token: bind it where only the
  scraper reaches it. Tokens are read when the node starts: rotating one is a rolling restart
  with the old and the new token both configured, then one without the old. Adopting
  `peer_token` on a running cluster is not a rolling change (a node that has it refuses a peer
  that does not): P5 tests the procedure, P6 writes it down.

### A3 Stream delete

- A delete is an **epoch floor** for the stream: every writer log of it with a lower epoch is
  deleted, the ones the deleting node never saw included (a list of logs and sequences, the
  first design, cannot say that: a node that was down holds logs nobody listed). The floor is
  just above the epochs the nodes hold (no clock: an epoch is its writer's choice), so a writer
  with an epoch at or above it writes a new stream of the name.
- The floor is a record of the node's own `__floors` stream: it replicates and is repaired as
  any batch is, and each node applies it when it becomes durable there.
- The model (`spec/mesh/Delete.tla`, written before the code) shows that a batch in flight at
  the delete, a node down during it and a restart all end deleted, and which rules that takes:
  drop a batch below the floor when it becomes durable, and keep the floor with the stream's
  summary so a restart has it. A review of the code then found what the node keeps outside
  its log (a compacted stream's values, dedup ids), a replacement written across a delete, and a
  record stored without being applied; each has its test and, where it is an interleaving, its
  counterexample config. Details and limits in `docs/ARCHITECTURE.md`, "Deleting a stream".

## 5. How the work lands

- **Stacked pull requests.** One logical change each, each on its own branch based on the one
  before: `claude/confident-cray-gs3hev` (this plan), then `...-a1-stream-classes`,
  `...-a2-scoped-tokens`, and so on. Titles follow AGENTS.md. A change to a lower branch is
  merged up the stack, not rebased.
- **Test first.** Each PR says which test failed first.
- **Formal verification in the same PR**, recorded in `spec/README.md`, and acceptance scenarios
  for what the PR promises (AGENTS.md).
- **Every gate green before the next PR is opened**: lint, tests and coverage, formal coverage,
  TLA+, Kani, acceptance, the Go client, mutants, the bench gate and the perf gate. CI skips
  mutation testing on drafts, so each PR's mutants are also run locally (`cargo mutants
  --in-diff`, from a worktree nothing else touches) before it is marked ready.
- **Performance.** The bench gate counts instructions on the storage paths; the perf gate runs
  `precog-bench mesh` (6 producers, 2 copies, its default small messages) and the storage throughput
  example, each PR against its own base. That is live-ingestion's shape of workload, not the
  session workload, and a slowdown under each PR's threshold can add up across the stack: P1
  measures the whole stack against main, on both workloads.
- **Order of work.** PLAN's live-ingestion cutover keeps priority for review and for production
  hours. Phase 1 lands behind configuration, so it does not wait for the cutover; phases 2 and 3
  change the write path and the public API, and start once the cutover is done.

## 6. Production ready means

1. Phases 1 to 3 merged, each with its verification.
2. Phase 5 done: the soak, the multi-machine numbers, the fault scenarios, backup, upgrade and
   rollback.
3. live-ingestion's cutover finished (PLAN §7), so the broker has production hours.
4. The owner's review of the runbook and the security review.

## 7. Risks and open decisions

| Risk or decision | Position |
|---|---|
| Hosted service or self-hosted broker | Open (S2-PLAN S0). Phases 1 to 3 are the same either way; quotas and billing beyond A5's rates and C4's counters wait for it |
| Scope creep away from live-ingestion | Classes, tokens, deletes, rate limits and TLS are off until configured, and the gates hold the hot path. Two defaults do change: A1 checks a stream's retention before a read of it is served (a comparison when nothing is due), and A6 counts readers of a class's streams under the class instead of per stream (streams the config names, and streams no class covers, keep their series) |
| Owner failover (phase 2) is where the bugs are | Model it before building it; majority `copies` and the conflict rule already hold (S2-PLAN §3.2) |
| One cluster's disks bound capacity until phase 6 | Stated to design partners; delta encoding (phase 4) cut the bytes of re-sent context from 350 MB to 7.6 MB in the one synthetic workload measured, which is its best case (results, caveats) |
| Trim on load makes a read pay for expiry | Bounded by what expired, and nothing is read when nothing is due. Not benchmarked on its own: P1's workload covers it |
| Billing by stored bytes collapses under delta encoding | Meter logical bytes at the gateway (C4), stored bytes at the node |
| A rollback across a format change | A3 moves the stream summaries to version 3, which an older node refuses loudly: rolling back means rebuilding `streams.redb` from the log on that node. P5 tests it before any production upgrade |
| Clients other than Rust lag the broker | The Go client has no `Delete` and no chunked events until C3; the gateway is the path for every other language |

## 8. Status

| ID | Pull request | State |
|---|---|---|
| Plan | [#32](https://github.com/aperiodic-io/precog/pull/32) `docs: plan of record for Precog as a durable session log` | open |
| A1 | [#33](https://github.com/aperiodic-io/precog/pull/33) `feat(mesh): stream classes` | open |
| A2 | [#34](https://github.com/aperiodic-io/precog/pull/34) `feat(mesh): scoped tokens` | open |
| A3 | [#35](https://github.com/aperiodic-io/precog/pull/35) `feat(mesh): stream delete` | open |
| A6, A5, A7, A4 | in that order | next |
