# Unit economics: an S2-compatible service on rented bare metal against s2.dev's prices

Question: running Precog as an S2-compatible service on servers rented from Hetzner or
OneProvider, what would a GiB written, read and stored cost us, how would that price against
s2.dev, and what margin does a realistic markup leave?

- **Prices:** cited in [`S2 bare-metal prices.md`](S2%20bare-metal%20prices.md), seen 2026-11-19.
- **Capacity per server:** this repository's measurements (`results/s2.md`, `results/compare.md`,
  `results/s2-streams.jsonl`).
- **Model:** `bench/s2/economics.py`. It is deterministic except for its seeded Monte Carlo; every
  table below is its output.

## Answer

1. **On bytes, bare metal is an order of magnitude cheaper than s2.dev's prices:**
   - GiB written: about $0.003 against $0.05;
   - GiB read: under $0.001 against $0.05 (private) or $0.10 (internet);
   - GiB-month stored: about $0.003 against $0.05. That is Hetzner SX65 HDD boxes (4×16 TB,
     €84) with erasure coding 3+2; object storage (iDrive e2, B2, Hetzner) costs about $0.007.
     The options are in [`S2 cold storage.md`](S2%20cold%20storage.md).

   Two things drive this. Hetzner charges nothing for traffic between servers, including across
   locations, and €1/TB for egress beyond 20 TB per 10G server. Its NVMe and HDD capacity also
   rents at a fraction of cloud prices: €1.31/TB-month raw HDD on an SX65.
2. **The same design on AWS gains nothing on writes.** Two replica copies crossing availability
   zones cost $0.02/GB each, which puts a GiB written at about $0.048, the whole of s2.dev's
   $0.05. That is why S2 is built on S3. Precog's local-disk replication pays off only where
   internal traffic is free: bare metal, or one zone.
3. **Stream count is the line where Precog loses today.** Every stream is resident on every node
   (1.65 KB per copy at 1M streams after main's #25; 3.9 KB before) plus ~8.8 KB in the owning
   gateway. That comes to ~$0.030 per 1000 streams-month against s2.dev's $0.01. Off-heap dormant streams (C7 in the
   plan) bring it to ~$0.013. s2-lite already holds only 44 MiB after a restart with 1M streams.
4. **A realistic list price** is about half of s2.dev on bytes:
   - $0.02/GiB written;
   - $0.01/GiB read ($0.02 to the internet);
   - $0.025/GiB-month stored;
   - $0.05 per 1000 streams.

   That earns 86–98% gross margin on writes and reads and 87% on storage, which is in line with
   or above public infrastructure-software margins: Confluent 74%, MongoDB 73%, Snowflake 72%,
   Cloudflare 74.5%, Datadog ~80%, OVHcloud 65%. Customers save 30–90% against s2.dev's list,
   most on write- and read-heavy streams, least on long retention.
5. **The fixed floor decides viability, not the unit costs.**
   - Fleet: the smallest production fleet is $1.9k/month (4 hot servers plus 6 SX65 boxes for
     an erasure-coded cold tier), or $1.3k/month with Hetzner Object Storage as the cold tier.
   - Staff: a 24/7 service needs on-call staff, modelled at €15–45k/month.
   - Below ~$50k/month of revenue the business loses money in most draws. At $100k/month the
     median net margin is ~61%, and at $250k/month ~82%.
6. **For customers running in AWS, the advantage mostly disappears.** They pay AWS ~$0.09/GB to
   send data out. That is more than s2.dev's entire write price, so the service only wins for:
   - customers outside AWS (on-prem, Hetzner, OVH, other clouds' egress-free paths);
   - data that is produced in Europe;
   - self-hosting (the live-ingestion case).

   An AWS-region deployment of the same product would have AWS's cost structure, and point 2
   applies.

## Model

- **Hot tier:** Precog nodes with the S2 gateway on the same box. CPU per record is measured:
  5.17 µs for 3 nodes (3 copies, acked at 2) plus 1.75 µs for the gateway at 256 B records, plus
  a modelled 1 µs per extra KiB. Cores are taken as 1.5× the test VM's 2.1 GHz vCPU (swept
  1.0–2.2). 10G uplinks; replication runs over the free vSwitch. Data stays on NVMe for 6 hours.
- **Utilization:** 50% target (swept 30–65%), with one spare server per tier.
- **Cold tier:** sealed segments, with no NVMe and no archive class. The default is SX65 HDD boxes
  (4 × 16 TB), erasure coded 3+2 over 5+ servers. The other options (3 replicas, 6+3, Storage
  Boxes, SX295, OVH, Hetzner Object Storage, iDrive e2, B2, owned disks in colocation) are in
  section 1b and [`S2 cold storage.md`](S2%20cold%20storage.md).
- **Reads** are charged with the servers their own CPU and egress would need. At the margin they
  ride free on an NVMe-bound fleet, but a price must not count on that.
- **Setup fees** are amortized over 36 months. EUR = 1.08 USD (swept 1.0–1.2).
- **Customers** are a mix of five shapes, from 1 MiB/s app events to 50 MiB/s market-data ingest
  and a 1M-stream agent-sessions workload.

## Output of `python3 bench/s2/economics.py`

### 1. Marginal unit costs at fleet scale (USD), against s2.dev's list prices

Base: 50% target utilization, cores 1.5x the test VM's, 1 KiB records, 6 h on NVMe, EUR = 1.08 USD. Fractional servers: no minimum cluster, no spares (those are in section 3).

| configuration | per GiB written | per GiB read | per GiB-month stored | bound by |
|---|---:|---:|---:|---|
| AX102 hot, SX65 EC 3+2 cold | $0.0028 | $0.0002 | $0.0032 | NVMe |
| AX162-1 hot | $0.0062 | $0.0005 | $0.0032 | NVMe |
| EX63 4x3.84 TB hot (assumed config) | $0.0005 | $0.0001 | $0.0032 | NVMe |
| EX131 4x7.68 TB hot (assumed config) | $0.0013 | $0.0004 | $0.0032 | network |
| AX102, 1 h on NVMe | $0.0006 | $0.0002 | $0.0032 | network |
| AWS i7ie.2xlarge hot + inter-AZ copies (reference; cold column is Hetzner SX65) | $0.0480 | $0.0005 | $0.0032 | NVMe |
| AX102, 256 B records | $0.0028 | $0.0002 | $0.0032 | NVMe |
| AX102, 30% utilization, 1.0x cores | $0.0028 | $0.0004 | $0.0032 | NVMe |
| **s2.dev list** | $0.050 (express $0.075) | $0.050 (internet $0.10) | $0.050 | |

### 1b. Cold tier for sealed segments (readable in seconds; no NVMe, no archive class)

USD per GiB-month and per TB-month of data stored, and the smallest cold tier each needs (k+m servers in distinct failure domains plus one spare; object stores have no minimum).

| cold tier | per GiB-month | per TB-month | smallest cold tier per month | failure domains needed |
|---|---:|---:|---:|---|
| SX295, EC 3+2, 10G | $0.0073 | $6.82 | $6,054 | 5 (EC 3+2) |
| SX295, 3 replicas, 10G | $0.0132 | $12.28 | $4,036 | 3 (replicas) |
| SX65 4x16 TB, EC 3+2, 1G | $0.0032 | $2.99 | $551 | 5 (EC 3+2) |
| SX65 4x16 TB, EC 6+3, 1G (Hetzner + OVH) | $0.0029 | $2.69 | $919 | 9 (EC 6+3) |
| SX65 4x16 TB, 3 replicas, 1G | $0.0058 | $5.38 | $368 | 3 (replicas) |
| OVH RISE-STOR 8x14 TB, EC 3+2 (in stock, vRack) | $0.0053 | $4.95 | $1,596 | 5 (EC 3+2) |
| Storage Box BX41 20 TB, EC 3+2 over 5 boxes (fragment files over SFTP) | $0.0041 | $3.85 | $263 | 5 (EC 3+2) |
| Hetzner Object Storage (reads inside Hetzner) | $0.0074 | $6.86 | none | the provider's (one site or zone) |
| iDrive e2 | $0.0064 | $6.00 | none | the provider's (one site or zone) |
| Backblaze B2 | $0.0075 | $6.95 | none | the provider's (one site or zone) |
| OVH Object Storage 1-AZ Standard | $0.0081 | $7.55 | none | the provider's (one site or zone) |
| Owned 60-bay chassis in colo, EC 8+3, fully loaded (needs ~10 PB) | $0.0030 | $2.80 | none | 11 chassis at one site; ~10 PB minimum |
| **s2.dev list** | $0.0500 | $46.57 | | multi-AZ object storage |

Per 1000 streams-month: RAM for 3 copies of the per-stream state = $0.03 (3 node copies x 1,650 B + 8,800 B in the owner, every stream resident, all of the AX102's price charged to RAM); s2.dev charges $0.01. Stream operations: CPU is already in the per-GiB costs; s2.dev charges $0.001 per 1000.

### 2. A price list at 75% gross margin on marginal cost, and at 30% under s2.dev

| line | our marginal cost | our price at 75% GM | s2.dev list | ours vs s2.dev | our GM at s2.dev -30% |
|---|---:|---:|---:|---:|---:|
| GiB written | $0.0028 | $0.0114 | $0.0500 | -77% | 92% |
| GiB read (private) | $0.0002 | $0.0009 | $0.0500 | -98% | 99% |
| GiB read (internet) | $0.0014 | $0.0055 | $0.1000 | -94% | 98% |
| GiB-month stored | $0.0032 | $0.0128 | $0.0500 | -74% | 91% |
| 1000 streams-month | $0.0300 | $0.1202 | $0.0100 | +1102% | -329% |

### 2b. A proposed list price (about half of s2.dev on bytes) and its margin per line

| line | proposed | s2.dev | our marginal cost | GM |
|---|---:|---:|---:|---:|
| GiB written | $0.0200 | $0.0500 | $0.0028 | 86% |
| GiB read (private) | $0.0100 | $0.0500 | $0.0002 | 98% |
| GiB read (internet) | $0.0200 | $0.1000 | $0.0014 | 93% |
| GiB-month stored | $0.0250 | $0.0500 | $0.0032 | 87% |
| 1000 streams-month | $0.0500 | $0.0100 | $0.0300 | 40% |
| 1000 streams-month, dormant streams off-heap (~2 KB resident per copy, as s2-lite after a restart) | $0.0500 | $0.0100 | $0.0131 | 74% |

### 3. The fixed floor: the smallest production fleet, and staff

Smallest fleet (4 AX102 hot incl. one spare, 10G uplinks; 6 SX65 for EC 3+2 incl. one spare): $1,871/month. With Hetzner Object Storage for the cold tier instead: $1,324/month.

| fixed cost per month (fleet floor + staff) | revenue to break even at 75% contribution | at s2.dev -30% (blended contribution) |
|---|---:|---:|
| $1,871 (staff EUR 0) | $2,495 | $1,936 |
| $18,071 (staff EUR 15,000) | $24,095 | $18,698 |
| $34,271 (staff EUR 30,000) | $45,695 | $35,460 |
| $50,471 (staff EUR 45,000) | $67,295 | $52,222 |

### 4. Customer bills: s2.dev list, our list at 75% GM, the proposed list

| customer | s2.dev standard | our list (75% GM) | proposed list | proposed GM | our marginal cost | customer's AWS egress to reach us (if they run in AWS) |
|---|---:|---:|---:|---:|---:|---:|
| small: app events: 1 MiB/s, 7 d, reads 2x, 10,000 streams | $919 | $42.42 | $118 | 91% | $10.60 | $248 |
| telemetry: 10 MiB/s, 7 d, reads 2x, 100,000 streams | $5,407 | $424 | $1,179 | 91% | $106 | $2,480 |
| market-data ingest: 50 MiB/s, 3 d, reads 4x, 2,000 streams | $35,341 | $2,066 | $8,016 | 94% | $516 | $12,400 |
| agent sessions (many streams): 2 MiB/s, 30 d, reads 1x, 1,000,000 streams | $3,467 | $248 | $331 | 81% | $62.01 | $496 |
| archive-heavy: 20 MiB/s, 90 d, reads 0.5x, 10,000 streams | $11,759 | $2,558 | $5,081 | 87% | $640 | $4,960 |

### 5. Monte Carlo (20,000 draws): net margin of a business priced at s2.dev -30%

Each draw: utilization U(0.3, 0.65); core speed U(1.0, 2.2); record size log-uniform 256 B-8 KiB; EUR U(1.0, 1.2) USD; cold tier one of SX65 EC 3+2, SX65 3 replicas, Hetzner Object Storage, iDrive e2, Backblaze B2 (1/5 each); resident RAM per stream log-uniform 2-25 KB (2 KB: dormant streams off-heap, as s2-lite after a restart); staff EUR 15k-45k/month; customers drawn from the mix above. Net margin = (revenue - marginal cost - fleet floor - staff) / revenue.

| monthly revenue at s2.dev -30% | net margin p10 | p50 | p90 | P(net margin > 0) | P(net > 50%) |
|---|---:|---:|---:|---:|---:|
| $25,000 | -95% | -42% | 10% | 17% | 0% |
| $100,000 | 48% | 61% | 74% | 100% | 83% |
| $250,000 | 76% | 82% | 87% | 100% | 100% |
| $1,000,000 | 90% | 92% | 93% | 100% | 100% |

## What the model leaves out

- **Hetzner's limits:**
  - New dedicated servers are offered only in FSN1 and HEL1, with NBG1 in the auction only.
    Three failure domains therefore means one company, two countries and shared network
    management: correlated failures are more likely than across S3's AZs.
  - Durability is k copies on disks Hetzner does not back up, far below S3's eleven nines.
    Don't sell it as the same durability class without a second provider or an S3 copy of
    sealed segments.
  - Some models take weeks to provision (AX42-1, EX63-1). Capacity has to be bought ahead,
    which the utilization target only partly covers.
- **Price risk:** Hetzner raised prices three times in 2026 (dedicated servers +3–21% in April;
  higher monthly prices for new orders in June). At +30% on every server, a GiB written still
  costs under $0.004.
- **OneProvider:** its site refused automated access, so none of its prices are verified.
  Snippets suggest more expensive hardware per unit than Hetzner, for example a Ryzen 9 9900X
  with 64 GB and 2 × 500 GB NVMe at €180 against an EX44 at €59. Its possible value is
  locations: US, Asia, and a third failure domain outside Hetzner. Model it once real quotes
  exist.
- **Costs not in the model:**
  - compliance (SOC 2, ~$50–100k/year);
  - support and sales;
  - DDoS protection, monitoring and backups of metadata;
  - the control plane's servers (small);
  - payment and billing tooling;
  - over-provisioning for growth.

  They belong in the fixed line. Section 3 of the output shows how fast the fixed line moves
  break-even.
- **s2.dev's own costs** are unknown. Its storage price, $0.05/GiB-month, is only ~2× S3
  Standard ($0.0247/GiB-month), so its margin on storage is thin. Its write and read prices are
  where it earns. Undercutting it on bytes is the strong position. Undercutting it on storage is
  modest, and on stream count impossible until dormant streams leave RAM.

## Recommendation

Price at about half of s2.dev on bytes. Do it in Europe, for customers outside AWS (or for
self-hosted deployments), and only once revenue can carry an on-call team, ~$50k/month.

Before that: build the cold tier on object storage (no floor; the smallest fleet is
$1.3k/month), switch to SX65 boxes with 3+2 past ~100 TB of cold data (~$3/TB-month), and move
dormant streams off-heap (C7). Those two items change the economics more
than any hardware choice. Among hot boxes, NVMe per TB decides the cost. EX-line servers with
4 NVMe drives, if their top configurations are as assumed, cut the write cost by another 3–5×
against the AX102.
