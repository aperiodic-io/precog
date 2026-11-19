# Bare-metal and reference cloud pricing, as seen 2026-11-19

All figures were fetched on **2026-11-19** unless a row says otherwise. Each one is tagged with its source type:

- **[P]**: the provider's own page or machine-readable price feed.
- **[3P]**: a third-party source.
- **[E]**: my own arithmetic or estimate.
- **[U]**: could not be verified.

Hetzner prices are EUR excluding VAT. AWS prices are USD for us-east-1.

Monthly figures for AWS use 730 h/month. Hetzner's own hourly caps work out to roughly 624 h/month (for example, AX102: 0.4150 x 624 = 259).

---

## 1. Hetzner dedicated (root servers)

### Context: three price changes in 2026

1. **2026-04-01:** prices rose for all customers. Dedicated went up 3-21%, cloud 30-37% and storage 30% [3P: webhosting.today, heise].
2. **2026-04-29:** setup fees were adjusted [3P].
3. **2026-06-15, new orders only:** the portfolio was standardized into fixed `-1/-2/-3` configurations, with no more free choice of RAM and disk. There is also a `-1-Ltd` tier, built from cheaper hardware and sold in limited quantity. Monthly prices went up and setup fees went down. Server Auction, IPs, storage products and Object Storage were excluded from this round [P: hetzner.com/pressroom/standardization-and-price-adjustment-of-our-server-products].

Rented servers keep their old price, so any older blog figure (for example "AX102 = €104") is stale.

The listed monthly prices **include the primary IPv4 at €1.70**. Hetzner's own price table lists AX102-1 at €257.30 and AX162-1 at €612.30 *without* IPv4 [3P snippet of the official table; consistent with the €259 and €614 on the product pages].

### Current models (product pages, base `-1` configuration)

| Model | CPU | Cores / threads | RAM | Disks | €/month | Setup | Hourly | Source |
|---|---|---|---|---|---|---|---|---|
| EX44 | Intel Core i5-13500 | 14C (6P+8E) / 20T [E: P-cores have HT] | 64 GB DDR4 | 2x 512 GB NVMe | **59.00** | 0 | 0.0945 | [P] /dedicated-rootserver/ex44 |
| AX42 | AMD Ryzen 7 PRO 8700GE | 8C / 16T | 64 GB DDR5 ECC (max) | 2x 512 GB NVMe | **99.00** | 49 | 0.1586 | [P] /ax42 |
| EX63 | Intel Core Ultra 7 265 | 20C (8P+12E) / 20T [E] | 64-192 GB DDR5 ECC | 2x 1 TB to 4x 3.84 TB NVMe DC | **99-149** | 74 | 0.2388 | [P] matrix-ex |
| AX102 | AMD Ryzen 9 7950X3D | 16C / 32T | 128 GB DDR5 ECC | 2x 1.92 TB NVMe Gen4 DC (up to 2x 7.68 TB) | **259.00** | 129 | 0.4150 | [P] /ax102 |
| EX131 | Intel Xeon Gold 6731P | 32C / 64T [E] | 128-256 GB DDR5 ECC reg | 2x 1.92 TB to 4x 7.68 TB NVMe DC | **269-559** | 279 | 0.8958 | [P] matrix-ex |
| AX162 | AMD EPYC 9454P (Genoa) | 48C / 96T | 128-512 GB DDR5 ECC reg | 2x 1.92 TB to 2x 15.36 TB NVMe DC | **614.00** (AX162-1; range 319-614 on the matrix page) | 304 | 0.9840 | [P] /ax162, matrix-ax |
| SX65 | AMD Ryzen 7 3700X | 8C / 16T | 64 GB (up to 128) DDR4 ECC | 4x 16-22 TB SATA HDD + 2x 1 TB NVMe | **84.00** | 39 | 0.1346 | [P] /sx65 |
| SX135 | AMD Ryzen 9 3900 | 12C / 24T | 128 GB DDR4 ECC | 8x 16-22 TB SATA HDD + 2x 1.92 TB NVMe DC | **289.00** | 144 | 0.4631 | [P] /sx135 |
| SX295 | AMD EPYC 7502P (Rome) | 32C / 64T | 256 GB DDR4 ECC reg | 14x 22 TB SATA HDD + 2x 7.68 TB NVMe DC | **879.00** | 439 | 1.4086 | [P] /sx295 |

Other variant prices on the matrix pages:

- AX-line matrix: AX41 (Ryzen 5 3600, 64 GB, 2x 512 GB NVMe) at €59, no setup fee; AX42 shown as €79-99.
- `-2` and `-3` variants, from a [3P] snippet of the official table: AX102-2 €547.30, AX162-2 €842.30, AX162-3 €1,597.30.
- SX-line matrix: SX65 "€159 and €84"; SX135 "€309 and €289"; SX295 "€599 and €879".

[U] The matrix pages do not tie each price or disk option to a variant, so which HDD size (16 TB or 22 TB) ships at the base price is unverified. The 22 TB on SX295 is fixed.

**Raw HDD cost per TB** [E]:

| Model | Raw capacity | €/TB-month |
|---|---|---|
| SX65 at 4x16 TB | 64 TB | ≈1.31 |
| SX135 at 8x16 TB | 128 TB | ≈2.26 |
| SX135 at 8x22 TB | 176 TB | ≈1.64 |
| SX295 | 308 TB | ≈2.85 |

These are raw figures. Divide by the replication factor, or apply the RAID or erasure-coding overhead.

### Network, traffic and add-ons

**Port and traffic**
- Every server has a 1 Gbit/s port with 1 Gbit/s guaranteed. Traffic is **unlimited and free** [P on every product page; docs.hetzner.com/robot/general/traffic].
- **10G uplink add-on:** €43.00/month [P: docs price-server-addons, "10 Gbit Uplink"]. It is billed differently:
  - unlimited traffic no longer applies;
  - **20 TB/month outgoing is included, then €1.00/TB**, with no bandwidth limit [P: traffic docs, product pages];
  - Hetzner treats a 10G order as customized, so delivery takes **at least 2-3 business days** [P: order-processing].
- Only **outgoing** traffic is metered. **Incoming and internal traffic are free** [P: traffic docs].

**Private network (vSwitch)**
- The docs list no base fee [P: docs vSwitch]. Public subnets routed on a vSwitch include 1 TB, then €1/TB.
- Servers in **different locations can join the same vSwitch, and traffic between them is free**.
- Limits: 100 servers per vSwitch; recommended MTU 1400 [P].
- Dedicated LAN or switch add-ons, monthly: LAN connection 10G €3.50, 25G €7.50; 12-port 10G switch €61; 8-port 25G switch €68; dual 25G NIC €12 [P: price-server-addons].

**Extra drives, monthly** [P: price-server-addons]. The page lists no setup fee for drives; whether a drive fits depends on the model and location.

| Drive | €/month |
|---|---|
| 512 GB NVMe | 11 |
| 1 TB NVMe | 30 |
| 2 TB NVMe | 54 |
| NVMe DC 960 GB | 38 |
| NVMe DC 1.92 TB | 70 |
| NVMe DC 3.84 TB | 96 |
| NVMe DC 7.68 TB | 170 |
| NVMe DC 15.36 TB | 320 |
| HDD 16 TB | 26 |
| HDD 22 TB | 36 |

**RAM upgrades** are expensive after the 2026 hikes. Examples: DDR5 ECC 64→128 GB is €147/month, and each 64 GB DDR5 reg ECC step is €111/month [P].

**Other**
- Additional IPv4: €1.70/month + €4.90 setup.
- Failover IP: €4.20/month.
- Remote Hands: €21 per 15 minutes [P].

### Contract, locations and provisioning

- **Contract:** "No minimum contract term", and cancellation takes effect immediately. Billing is hourly, capped at the monthly price [P: product pages].
- **Locations:**
  - New-order product pages list **FSN1 (Falkenstein, DE)** and **HEL1 (Helsinki, FI)** only, at **one price for both locations** [P].
  - **NBG1 (Nuremberg)** appears only for auction servers (NBG1-DC1, NBG1-DC6 in the 2026-11-19 feed).
  - **Hetzner offers no dedicated servers in the US.** US is cloud-only (Ashburn, Hillsboro) [P: product pages list no US location; cloud page].
  - Each site has several buildings: the auction feed shows FSN1-DC1 through DC18 and HEL1-DC2, DC5, DC6 and DC12. Robot shows the building, but you cannot choose one when you order [U].
- **Failure domains** [E]: realistically three, namely FSN1, NBG1 (auction only) and HEL1. FSN1 and NBG1 are about 150 km apart in the same country and the same company network.
- **Provisioning**, from [P: docs.hetzner.com/general/infrastructure-and-availability/order-processing]. All times are rough guidance and vary by variant:

  | Configuration | DE | FI |
  |---|---|---|
  | AX162-1..3, EX131-1..3, SX295-1 | a few minutes | a few minutes |
  | AX102-2/-3 | a few minutes | a few minutes |
  | AX102-1 | 10-12 workdays | 10-12 workdays |
  | AX42-1, EX63-1 | several weeks | several weeks |
  | SX65-1 | a few minutes | a few minutes |
  | SX135-1 | 10-12 workdays | a few minutes |
  | Auction | a few minutes | a few minutes |

  A [3P] LowEndBox report measured about 4 minutes for an auction server.

### Hetzner Server Auction

This is a snapshot of the live feed `hetzner.com/_resources/app/data/app/live_data_sb.json`, fetched 2026-11-19 [P].

- **Only 69 servers were listed**, which is thin inventory.
- No setup fee, 1 Gbit/s port, unlimited traffic, one IPv4.
- The auction landing page still says "from €78.70", but the feed's cheapest server was €64.
- Auction servers were exempt from the June increase. Even so, older auction hardware now costs **more** than new EX44 or AX41 servers at €59.

| Class | Examples (CPU, RAM, disks, DC) | €/month |
|---|---|---|
| Cheapest overall | Ryzen 5 3600, 64 GB, 2x 512 GB NVMe + 1.92 TB SATA DC SSD, FSN1-DC13 | 64 |
| 2x 512 GB NVMe, 64 GB (old i7-6700/7700, Xeon E3) | many in FSN1, HEL1 | 79-91 |
| 2x 960 GB-1 TB NVMe, 64-128 GB | i7-8700 2x1 TB 64 GB €105; E-2176G ECC 2x960 GB €115-118; i7-8700 128 GB 2x1 TB €137; Xeon W-2145 128 GB 2x960 GB €222 | 105-222 |
| 2x 1.92-3.84 TB NVMe, 64-128 GB ECC | i5-13500 64 GB 2x1.92 TB €233; Ryzen 9 3900 128 GB 2x1.92 TB €264; Ryzen 9 5950X 128 GB 2x3.84 TB (HEL1-DC5) €368 | 233-368 |
| 2x HDD (2-8 TB) | i7 2x4 TB €79-82; i7-8700 2x8 TB €95-98 | 79-127 |
| 4x HDD | Xeon E3 32 GB 4x10 TB €115-119; Ryzen 5 3600 ECC 64 GB **4x16 TB €145-146** (FSN1-DC18 / HEL1-DC12) | 115-146 |
| 10x 16 TB HDD + 2x 960 GB NVMe, 128 GB ECC | Ryzen 7 3700X €343-351; Ryzen 9 3900 €369 (HEL1-DC6, FSN1-DC14) | 343-369 |
| High end | Xeon Gold 5412U, 128 GB, 2x 1.92 TB NVMe, NBG1-DC6 | 1,010 |

Raw HDD cost per TB at auction [E]: 4x16 TB is about **€2.27/TB-month**; 10x16 TB is about **€2.14-2.31/TB-month**.

---

## 2. Hetzner Object Storage and Storage Box

### Object Storage [P: hetzner.com/storage/object-storage, docs overview]

| Item | Price / detail |
|---|---|
| Base | **€6.49/month**, including 1 TB storage (744 TB-hours) and 1 TB egress. Billed hourly at €0.0104/h while any bucket exists. |
| Extra storage | **€0.0087 per TB-hour**, about **€6.35/TB-month** at 730 h [E] (€6.47 at 744 h) |
| Extra egress | **€1.00/TB**. The page literally prints "per hour per TB", which looks like a labelling glitch [U]; it matches the €1/TB traffic rate elsewhere. |
| Free | Ingress, internal traffic within eu-central, and all S3 requests (PUT, GET, DELETE) |
| Minimum object size | Objects under 64 KB are billed as 64 KB |
| Locations | FSN1, NBG1, HEL1. No US location for object storage on the page. |
| Limits | 750 req/s per bucket and per source IP; 10 Gbit/s per bucket; 100 TB and 50 M objects per bucket; 100 buckets per account |

### Storage Box [P: hetzner.com/storage/storage-box]

Storage Box is backup-style storage (SFTP, SMB, Borg and similar), with a limit of 10 concurrent connections and unlimited traffic. Locations are DE or FI.

| Plan | Capacity | €/month | €/TB-month [E] |
|---|---|---|---|
| BX11 | 1 TB | 3.20 | 3.20 |
| BX21 | 5 TB | 10.90 | 2.18 |
| BX31 | 10 TB | 20.80 | 2.08 |
| BX41 | 20 TB | 40.60 | 2.03 |

---

## 3. Hetzner Cloud dedicated-vCPU (CCX), reference [P: hetzner.com/cloud/general-purpose]

Prices are after the April 2026 increase: CCX13 went from €11.99 to €15.99 before IPv4 and location pricing, per [3P].

| Plan | vCPU / RAM / NVMe | DE (NBG1) €/mo | FI (HEL1) €/mo | US (ASH/HIL) €/mo | Included traffic, EU / US |
|---|---|---|---|---|---|
| CCX13 | 2 / 8 GB / 80 GB | 43.49 | 36.99 | 43.99 | 20 TB / 1 TB |
| CCX23 | 4 / 16 / 160 | 86.49 | 73.49 | 87.99 | 20 / 2 |
| CCX33 | 8 / 32 / 240 | 138.99 | 117.99 | 141.49 | 30 / 3 |
| CCX43 | 16 / 64 / 360 | 276.49 | 234.99 | 279.99 | 40 / 4 |
| CCX53 | 32 / 128 / 600 | 533.99 | 453.49 | 538.99 | 50 / 6 |
| CCX63 | 48 / 192 / 960 | 853.99 | 725.49 | 860.49 | 60 / 8 |

Traffic overage is **€1.00/TB** in the EU and US and **€7.40/TB** in Singapore [P].

---

## 4. OneProvider

**Verification status.** oneprovider.com sits behind a Cloudflare bot challenge: every page returned HTTP 403 to both the fetch tool and curl on 2026-11-19. **I could not read any OneProvider page directly [U].** The figures below come from search-engine snippets of oneprovider.com pages, which are undated and possibly stale cached snapshots, and from third-party aggregators. Treat all of them as indicative.

### Servers seen in snippets

| Location | Config | Port / bandwidth | Price | Setup | Source |
|---|---|---|---|---|---|
| Frankfurt | Ryzen 9 9900X, 64 GB DDR5, 2x 500 GB NVMe | 1 Gbps unmetered, 1 Gbps guaranteed | **€179.99/mo** | €0 | [3P: search snippet of oneprovider.com/en/dedicated-servers/frankfurt-germany] |
| Frankfurt | Ryzen 9 5900X, 32 GB DDR4, 2x 500 GB NVMe | 1 Gbps unmetered | **€99.99** (another snippet: €109.99) | €0 | same [3P] |
| Frankfurt | Ryzen 7 9800X3D, 64 GB DDR5, 2x 960 GB NVMe | 1 or 5 Gbps unmetered | not extractable [U] | | same [3P] |
| Paris | NVMe configurations, for example 2x 2 TB NVMe on 10 Gbps unmetered; one listing at €279/mo | 1 and 10 Gbps unmetered | €279 (example) | €0 on several | [3P snippet of /paris-france] |
| Amsterdam | entry NVMe plans | unmetered on some | €25 and €36/mo | | [3P snippet; page metadata about 2 years old, stale] |
| Amsterdam | Xeon, 4x 6 TB SATA HDD | 1 Gbps | €39.99 | €0 | [3P snippet, stale] |
| Amsterdam | Xeon E5-1410 v2, 4x 12 TB SATA HDD | 1 Gbps guaranteed | €279 | €0 | [3P snippet, stale] |
| Amsterdam | Xeon E-2236, 4x 8 TB HDD | | not shown [U] | | [3P snippet] |
| unspecified | 16C/32T, 256 GB DDR4 | **unmetered @ 10 Gbps** | **$420.35/mo** | $0 | [3P snippet of oneprovider.com/en/configure/dediconf/6488] |
| Belgrade | Xeon E-2234, 8 GB, 1x 1 TB HDD | 1 Gbps, 5 TB traffic | $122.94 | free | [3P: whtop.com, listing added 2025-10-09] |

### General terms

- **Reach:** OneProvider is a Montreal-based reseller over regional datacenters in "140+ cities across six continents". Hubs include Paris, Amsterdam, Frankfurt, Lille, many US cities (New York, Dallas, LA, Miami, Portland), Singapore and others [3P: Valebyte review, 2026-07-06; city URLs on oneprovider.com seen in search].
- **Price bands** [3P: Valebyte 2026-07-06]:
  - clearance Atom or Xeon E3 machines (10+ years old) at about $4-9/mo;
  - $25-35/mo for 8-core Atom or Xeon E3/D, with NVMe options in FR, DE, CA and SG;
  - **standard servers in major hubs at $30-90/mo**.
- **Bandwidth:** most listings say "unmetered" at 1 Gbps; some offer 5 or 10 Gbps unmetered; some locations are metered (for example 5 TB in Belgrade) [3P]. I could not read any fair-use policy text [U].
- **Setup fee:** €0 at many popular DCs. Smaller facilities charge a one-time fee that can exceed the first month, and it shows only at checkout [3P: Valebyte].
- **Contract:** billed monthly with no proration [3P: Valebyte]. Optional 3-, 6- or 12-month terms are non-refundable if you cancel early. Cancellation needs at least 7 days' notice before the invoice due date. There is a 10-day money-back guarantee only for servers that were never delivered, and none on purchases discounted 25% or more [3P: search snippets of oneprovider.com ToS and FAQ; the ToS page is about 2.3 years old].
- **Provisioning:** 3-6 h in Paris, Amsterdam and Frankfurt; same day in the US (longer if the server has to be built); 24 h or more in Africa, Asia and LatAm. Hardware changes such as RAID or a reinstall take 3-4 days [3P: Valebyte].
- **Support quality:** first responses reported at up to 15 h; whtop user rating 1/10 from 16 ratings [3P]. This is relevant for failure-domain and repair assumptions.
- **Private network:** a "Dedicated Servers With Private Network" filter exists on oneprovider.com, but price and topology are unknown [U].

---

## 5. Reference cloud prices

### AWS, us-east-1

Source: the AWS public Price List API, fetched 2026-11-19 [P]:

- `AmazonS3/current/us-east-1/index.json` (publicationDate 2026-11-18)
- `AWSDataTransfer/...` (2026-11-17)
- the EC2 on-demand metered-unit map (2026-11-18)

**S3 Standard**

| Item | Price |
|---|---|
| Storage | **$0.023/GB-month** for the first 50 TB, $0.022 for the next 450 TB, $0.021 above 500 TB |
| PUT, COPY, POST, LIST | **$0.005 per 1,000** |
| GET and other requests | **$0.0004 per 1,000** |

**S3 Express One Zone** (reflects the April 2025 cut)

| Item | Price |
|---|---|
| Storage | **$0.11/GB-month** |
| PUT, COPY, POST, LIST | **$0.00113 per 1,000** |
| GET and other requests | **$0.00003 per 1,000** |
| Upload | **$0.0032/GB** |
| Retrieval | **$0.0006/GB** |

The per-GB upload and retrieval charges apply only to the part of a request above 512 KB. That rule is from AWS documentation, not the price feed [3P].

**Data transfer**

- **Inter-AZ:** $0.010/GB, labelled "in/out/between EC2 AZs". AWS bills it on both sides, so it is effectively about **$0.02/GB** for each GB moved between AZs [P rate; the two-sided billing is from AWS documentation, 3P]. The feed also contains `IntraRegion-xAZ-In/Out` rows at $0.00; their scope is unclear [U].
- **Internet egress:** 100 GB/month free (global), then **$0.09/GB for the first 10 TB**, $0.085 for the next 40 TB, $0.07 for the next 100 TB, and $0.05 beyond 150 TB.

**EBS gp3:** **$0.08/GB-month**, including 3,000 IOPS and 125 MB/s. Extra IOPS cost $0.005 per IOPS-month and extra throughput $0.06 per MB/s-month. These are AWS's example figures, and the page does not name the region [P: aws.amazon.com/ebs/pricing]. $0.08 is the long-standing us-east-1 rate [3P].

**EC2 instances with local NVMe**, on-demand Linux. Monthly prices are hourly x 730 [E].

| Instance | vCPU / RAM | Local NVMe | $/h | ≈$/month |
|---|---|---|---|---|
| i4i.xlarge | 4 / 32 GiB | 1x 937 GB | 0.343 | 250 |
| i4i.4xlarge | 16 / 128 | 1x 3,750 GB | 1.373 | 1,002 |
| i7i.xlarge | 4 / 32 | 1x 937 GB | 0.3775 | 276 |
| i7ie.xlarge | 4 / 32 | 1x 2,500 GB | 0.5198 | 379 |
| i7ie.2xlarge | 8 / 64 | 2x 2,500 GB | 1.0396 | 759 |
| im4gn.xlarge (Graviton) | 4 / 16 | 1x 1,875 GB | 0.36379 | 266 |
| im4gn.4xlarge | 16 / 64 | 1x 7,500 GB | 1.45517 | 1,062 |
| i8g.4xlarge (Graviton4) | 16 / 128 | 1x 3,750 GB | 1.3728 | 1,002 |
| i3en.2xlarge | 8 / 64 | 2x 2,500 GB | 0.904 | 660 |

### Cloudflare R2 [P: developers.cloudflare.com/r2/pricing]

| Item | Standard | Infrequent Access |
|---|---|---|
| Storage | **$0.015/GB-month (about $15/TB)** | $0.01/GB-month |
| Class A operations | $4.50 per million | $9.00 per million |
| Class B operations | $0.36 per million | $0.90 per million |

**Egress is free.** The free tier is 10 GB, 1 M Class A and 10 M Class B operations per month, and applies to Standard only.

### Backblaze B2 [P: backblaze.com/cloud-storage/pricing]

- Storage: **$6.95/TB-month**, with the first 10 GB free.
- Egress is free up to 3x the average stored volume, then $0.01/GB.
- API classes A, B and C are free. Class D costs $0.004 per 10,000 calls.
- No minimum object size and no minimum storage duration.

---

## 6. s2.dev pricing [P: https://s2.dev/pricing, 2026-11-19; the page shows no effective date]

| Item | Price |
|---|---|
| Storage (data retained) | **$0.05/GiB-month** ($0.00006849315068/GiB-hour). No separate storage classes listed. |
| Writes, Standard | **$0.05/GiB** |
| Writes, Express | **$0.075/GiB** |
| Reads, private network | **$0.05/GiB** |
| Reads, internet | **$0.10/GiB** |
| Stream operations (append, read, check-tail) | **$0.001 per 1,000**. A session counts as 1 op per minute per stream per connection. |
| Basin operations | $0.010 per 1,000 |
| Account operations | $0.100 per 1,000 |
| Streams | $0.01 per 1,000 stream-months |
| Basins | $0.01 per basin-month |
| Access tokens | $0.01 per 1,000 token-months |
| Free tier | **$10 in credits for new accounts**. No ongoing free plan is listed. |
| Included | 99.99% availability SLO, SOC 2, GDPR DPA, HIPAA BAA on request |

The page shows no plan tiers, minimum commitments or retention limits.

**s2 compared with the other storage prices** [E]: $0.05/GiB-month is about **$51/TiB-month**, roughly:

| Compared with | Price | s2 multiple |
|---|---|---|
| S3 Standard | $0.023/GB | 2.0x |
| R2 | $0.015/GB | 3.4x |
| Hetzner Object Storage | about €6.35/TB | about 7x |
| Hetzner SX raw HDD | about €1.3-2.9/TB | about 18-40x; about 6-13x after 3x replication |

---

## 7. Gross-margin calibration

### Infrastructure and data SaaS

| Company | Period | GAAP gross margin | Non-GAAP gross margin | Source |
|---|---|---|---|---|
| Confluent | Q1 2025 | 74.5% | 78.6% | [P: SEC 8-K exhibits via search] |
| Confluent | Q2 2025 | 73.7% | 78.3% | same |
| Confluent | 9 months to Sep 2025 | | 78.3% | same |

Notes on Confluent:
- I did not retrieve the full-year FY2025 figure; the BusinessWire release returned 403 [U].
- FY2025 revenue was $1,166.7 M, of which Confluent Cloud was $624 M (+27%).
- IBM is acquiring Confluent at $31/share, expected to close by mid-2026 [3P/P snippets]. FY2025 may be its last standalone report.

| Company | Period | GAAP gross margin | Non-GAAP gross margin | Source |
|---|---|---|---|---|
| MongoDB | Q4 FY2026 (to Jan 2026) | **73%** | **75%** | [P: SEC 8-K / IR release via search] |
| Snowflake | FY2026 (to Jan 2026), product gross margin | 72% | **75.8%**; FY27 guide 75% | [P: IR release via search] |
| Datadog | Q4 2025 | | **81.4%** | [3P: fintool] |
| Datadog | FY2025 | about 80% [E from 3P edgar.tools: $2,740 M gross profit on $3,427 M revenue] | | |
| Cloudflare | FY2025 | **74.5%** | 75.8% | [P: Cloudflare Q4-25 release] |

Context for each:
- **MongoDB:** Atlas, its managed cloud service running on hyperscalers, was 72% of revenue [3P].
- **Snowflake:** product revenue runs mostly on hyperscaler compute.
- **Cloudflare:** owns its own hardware and network.

### Infrastructure providers

| Company | Period | Gross margin | Other | Source |
|---|---|---|---|---|
| DigitalOcean | FY2025 | **60%** | | [P release via search] |
| OVHcloud | FY2025 (to Aug 2025) | **65.4%** | adjusted EBITDA margin 40.4% | [P: OVHcloud FY25 release via search] |

### Cloud reseller markups [3P, low quality, vendor blogs]

- Pure hyperscaler resale earns a **2-5% net margin** (another vendor quotes 3-12%). About 15% gross margin is cited as typical for thin resale.
- MSPs that add services earn 20-40% EBITDA; services gross margin is about 37% (a Forrester-based summary).
- One serverless-hosting example marks up 11-50% over the underlying infrastructure.
- AWS CPPO marketplace fee: about 3.5%.

### Calibration [E]

- Managed data and streaming products running on hyperscalers settle at **about 72-79% gross margin**: Confluent, MongoDB, Snowflake.
- Products running on owned infrastructure reach about 75-80%: Cloudflare, Datadog.
- Commodity IaaS sits at 60-65%: DigitalOcean, OVH.
- A pure reseller with no added product makes low single digits to about 15%.
- So a "realistic markup" for a differentiated durable-streams product is a **price of roughly 3.5-5x cost of goods (COGS)**, which gives 70-80% gross margin. A commodity price of about 2.5-3x COGS gives 60-65%.

---

## Caveats

- Hetzner product pages and matrix pages disagree in places:
  - The AX42 matrix range is €79-99 against €99 on the product page.
  - The AX162 description mentions "4x 3.84 TB" while the spec table says 2x 1.92 TB.

  I used the product-page figures and hourly caps, which agree with each other.
- Hetzner prices changed three times in 2026. Recheck before committing, especially any `-1-Ltd` offers, which are limited stock.
- None of the OneProvider numbers were verified against the live site.
