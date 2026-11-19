# Margin opportunities for Precog as an agent-telemetry stream service (2026-11-19)

Gross margin on bytes is already 86–98% in the cost model (`bench/s2/economics.py`; inputs in
[`S2 bare-metal economics.md`](S2%20bare-metal%20economics.md)). Better margin comes from four
places:
- what a stream costs to hold, the only weak line;
- storage, at 87%;
- how much of the value we charge for;
- the fixed staff floor, which decides net margin.

Figures marked *model* come from `economics.py`. For compression, the byte-bound costs are
divided by the ratio; that is our approximation, and no agent-telemetry ratio has been measured
yet.

## Already done on main

Main's #25 serves sealed segments' batches from the index files: RAM per retained batch fell from
1,092 B to 6 B. Remeasured after rebasing on it (`results/s2-streams.jsonl`, 1M streams of one
record each, 1 node):

| | before #25 | after #25 |
|---|---:|---:|
| RAM per stream per node | ~3.9 KB | **~1.65 KB** |
| Node RSS at 1M streams | 3,778 MiB | 1,575 MiB |
| Restart with 1M streams | 8.9 s | 3.1 s |
| Cost per 1,000 streams-month (3 node copies + gateway owner) | $0.045 | **$0.030** |
| Margin at our $0.05 per 1,000 streams | 10% | 40% |

Of the remaining $0.030:
- the three node copies are $0.011;
- the POC gateway's per-stream streamer (~8.8 KB) is $0.019.

Evicting idle streamers from the gateway is now the largest per-stream lever.

## Cut infrastructure cost

| # | Opportunity | Effect | Effort, risk |
|---|---|---|---|
| 1 | Keep 1 hour of data on NVMe instead of 6; seal to the HDD tier sooner | GiB written $0.0028 → $0.0006 (model) | low; reads of the last hours come from HDD |
| 2 | Keep 2 NVMe copies, not 3: ack at 2 of 3, drop the third once the segment is erasure coded on HDD | hot NVMe bytes −33% | medium; one copy fewer while data is hot |
| 3 | Hot servers with cheaper NVMe per TB (Hetzner EX-line with 4 drives; top configurations assumed, not confirmed) | GiB written $0.0005–0.0013 (model) | low; some take weeks to deliver |
| 4 | zstd per batch, with per-stream dictionaries for repetitive agent JSON (the SDK already compresses each batch with lz4 by default) | NVMe, network and storage ÷ the ratio; at 3×, storage $0.0032 → ~$0.0011/GiB-month | medium; measure the ratio on real traces first |
| 5 | Evict idle streamers from the gateway, then move idle streams off RAM in the nodes | per 1,000 streams $0.030 → ~$0.011 (evicting streamers only), lower once nodes keep idle logs on disk | 2–3 weeks for eviction; 6–10 weeks for off-heap logs; the launch gate |
| 6 | Wider erasure coding (6+3, 8+3) across Hetzner plus OVH; own disks in colocation from ~10 PB | storage ~$3 → $2–2.7/TB-month | operational burden; drive prices up in 2026 |
| 7 | Run hotter: 65% target utilization instead of 50%; multiplex tenants and follow demand with hourly billing | ~25% fewer hot servers | less headroom for spikes |
| 8 | Bill accepted bytes, store deduplicated bytes (the broker already drops retried events) | storage −(duplicate share); live-ingestion's producers resend ~20% | low; only where clients retry |

CPU is not the binding resource (NVMe and network are), so making the gateway cheaper per record
raises throughput, not margin.

## Charge for more of the value

| # | Opportunity | Why it moves margin |
|---|---|---|
| 9 | Bill raw (uncompressed) bytes and keep compression as margin; advertise the egress saving instead ("send a third of the bytes out of AWS") | margin rises by the ratio while prices stay predictable and comparable to S2's metered bytes |
| 10 | Price per session for observability-adjacent buyers (for example $0.50 per 1,000 sessions) | LangSmith charges about $5 per 1,000 traces; a session costs us a fraction of a cent |
| 11 | Retention and compliance tier: immutable logs, export, EU residency attestations, customer-held keys; priced above plain storage | bought by CISOs and DPOs for risk, not cost per GB; aligns with the AI Act deadline (2 Dec 2027) |
| 12 | Plan minimums (for example a $49/month Pro plan with included usage) | the 93% of customers who are small pay for their support |
| 13 | Self-hosted or bring-your-own-cloud licence | software margin (90%+) with no fleet; fits EU regulated buyers and AWS-heavy customers who won't pay egress |
| 14 | Paid extras: dedicated servers, private networking, SLA tiers, Parquet export to the customer's bucket, replay and eval export | cheap to serve, priced on value |
| 15 | Annual prepay and commitments | cash up front, lower churn, cheaper capital |
| 16 | Charge for cold reads and internet egress (already $0.02/GiB on the list) | covers the expensive path |

## Lower the fixed floor (net margin)

| # | Opportunity | Effect |
|---|---|---|
| 17 | Make live-ingestion an anchor tenant on the same fleet | pays for the minimum fleet and on-call from day one |
| 18 | Automate operations: leaderless failover, self-healing repair, one binary | fewer pages, so a four-person rota covers more |
| 19 | One region until revenue justifies a second | defers the year-2 fleet step in `bench/s2/business_plan.py` ($12k a month) |
| 20 | Self-serve documentation, community support, OEM partners | lower support and sales cost per dollar of revenue |
| 21 | ISO 27001 before SOC 2 for EU buyers; SOC 2 when US deals need it | defers $50–100k a year |

## Priorities

1. **Per-stream cost (#5).** Main's #25 already took the node part from 3.9 KB to 1.65 KB per stream. Evicting idle gateway streamers takes the line to about $0.011 per 1,000 streams, near S2's $0.01 price.
2. **Bytes (#1, #4, #9).** A 1-hour NVMe window plus zstd, billed on raw bytes, could cut write and storage costs by roughly 5–10× without touching the price list. Measure the zstd ratio on agent traces before advertising one.
3. **A self-hosted licence (#13)** for buyers who won't pay cloud egress or want sovereignty.
4. **An anchor tenant (#17)**, so the staff floor is shared from the start.

Next step: make #1, #2, #4, #7, #12 and #13 inputs of `economics.py` and `business_plan.py`, so
the plan and the deck show the effect of each.
