# Cold storage for sealed segments: the cheapest options (2026-11-19)

The question: where should Precog keep old data (sealed log segments) for days to months? It has
to be readable within seconds, so no NVMe and no archive class. The answer feeds
`bench/s2/economics.py` and [`S2 bare-metal economics.md`](S2%20bare-metal%20economics.md).

**Sources.** Two research passes, reproduced in full below with their citations:
- **Hetzner and OVH:** from the providers' own price APIs and order catalogs.
- **Several other providers:** their sites block automated fetches, so those figures are marked
  `[3P]` or `[U]`.

EUR is converted at 1.15 USD in these notes. The model uses 1.08 and sweeps 1.0–1.2.

## Conclusion

| | € or $ per usable TB-month | orderable | smallest footprint | catch |
|---|---:|---|---|---|
| **Hetzner SX65 (4×16 TB, €84), erasure coded 3+2** | **$2.99** | yes, FSN1/NBG1/HEL1 | 6 servers ≈ $551/mo | 1 Gbit ports; 5 failure domains, so two of them must be buildings at one Hetzner site or a second provider |
| SX65, erasure coded 6+3 | $2.69 | yes, but needs 9 domains | 10 servers ≈ $919/mo | Hetzner plus OVH |
| Storage Box BX41 (20 TB, €40.60), 3+2 fragment files over SFTP | $3.85 | yes | 5 boxes ≈ $263/mo | 10 connections per box, no compute, unguaranteed bandwidth |
| SX65, 3 replicas | $5.38 | yes | 4 servers ≈ $368/mo | simplest |
| iDrive e2 | $6.00 | yes | none | one provider site; 3× free egress |
| Hetzner Object Storage (reads inside Hetzner) | $6.86 | yes | none | 100 TB, 750 req/s and 10 Gbit/s per bucket; single site |
| Backblaze B2 | $6.95 | yes | none | 3× free egress |
| SX295 (14×22 TB), 3+2, 10G (the earlier model) | $6.82 | yes | 6 servers ≈ $6,054/mo | |
| Own 60-bay chassis in colo, 8+3 | ~$2.0–2.8 | Hetzner colo closed to new customers | ~10 PB, 11 chassis | drive prices +134% in 2026; operational burden |
| *s2.dev list* | *$46.57* | | | multi-AZ |

1. **Small HDD boxes beat big ones.** After Hetzner's 2026 price changes and the drive shortage,
   the SX65 is the cheapest disk you can order, at €1.31 per raw TB-month. That is less than the
   SX295 (€2.85), the auction (€2.14 at best) and every orderable OVH box. Small boxes also rebuild
   faster: a 64 TB box re-copies in days at 1 Gbit, a 308 TB one in weeks.
2. **Erasure coding is what makes it cheap.** 3+2 stores 1.67× the data; 6+3 stores 1.5× but needs
   9 failure domains, which Hetzner alone cannot give. Three replicas cost 1.8× as much as 3+2.
3. **Object storage costs about twice as much, but has no floor and no operations.** iDrive e2,
   B2 and Hetzner Object Storage all land at $6–7 per TB-month for this read pattern. Wasabi's
   90-day minimum puts it at $51 at a 14-day retention. The AWS, GCS and Azure infrequent tiers
   cost $60–300 here, almost all of it egress.
4. **Owning disks only pays from about 10 PB**, and today's drive prices make it worse. Hetzner
   colocation is closed to new customers.

**Recommendation:**
- **Start:** Hetzner Object Storage, or B2/iDrive e2. There is no floor, the cold tier is
  ~$6–7/TB, and the whole fleet starts at ~$1.3k/month.
- **Past ~100 TB of cold data:** move to SX65 boxes with 3+2 (~$3/TB), keeping one copy of the
  manifests in object storage.
- **Later:** add OVH boxes for 6+3 and real site diversity.

None of this needs NVMe or an archive class: segments are written once, sequentially, and read
back sequentially.

---

## Research: rented HDD servers

# Cheap HDD-heavy rented servers for a cold segment tier (snapshot 2026-11-19)

Use case: sealed log segments, written sequentially once, read now and then as whole segments
(50-200 MB/s per server), kept for days to months, erasure coded (3+2, 6+3) or 3-way replicated
across failure domains. Data must be readable in seconds, so archive and tape classes are out.

Tags: no tag = read today (2026-11-19) from the provider's own page or its public pricing or
catalog API. **[3P]** = third-party listing or search-engine snippet. **[E]** = my own arithmetic.
**[U]** = could not verify.
Prices are monthly, with no commitment and excluding VAT unless noted. USD to EUR uses an assumed
1 EUR = 1.15 USD **[E]**. "EUR/TB" means EUR per raw TB per month **[E]**: drive TB are decimal,
and the boot SSD/NVMe is excluded.

Context: HDD prices rose steeply through 2026 (one tracker reports +40-90% since March; Leaseweb
blogs of 15-35% hosting price rises) **[3P]** ([pr.com](https://www.pr.com/press-release/978414),
[Leaseweb blog](https://blog.leaseweb.com/2026/07/16/rising-server-cloud-prices-2026/)). Older
"EUR 1/TB" folklore numbers no longer hold, and the Hetzner auction is now *more* expensive per TB
than Hetzner's new SX65.

---

## 1. Hetzner (DE: FSN1, NBG1; FI: HEL1)

Sources:
- SX matrix page <https://www.hetzner.com/dedicated-rootserver/matrix-sx/> and the product pages
  `/sx65/`, `/sx135/`, `/sx295/`.
- The website price API `https://website-price-api.hetzner.com/api/v1/products/ROBOT_17xx`. This is
  what the pages render from. It returns prices without the IPv4, and the page adds the €1.70 primary
  IPv4 to them.
- Variant definitions `https://www.hetzner.com/_resources/app/data/product/SX65_en.json` (and the
  SX135 and SX295 equivalents).

Common to all SX servers:
- 1 Gbit/s port, 1 Gbit/s guaranteed bandwidth, traffic unlimited and free. The 10G uplink add-on
  loses "unlimited": outgoing traffic over 20 TB costs €1/TB.
- Hourly billing, capped at the monthly price (monthly = hourly × 624 h **[E]**).
- Primary IPv4 is optional (€1.70/mo).
- No commitment. Setup fee as listed.
- Private network: **vSwitch** (VLAN, free, up to 100 servers, internal traffic across locations
  free). It **shares the main 1 Gbit NIC** and uses MTU 1400
  ([docs](https://docs.hetzner.com/robot/dedicated-server/network/vswitch/)).
- The price API lists FSN1, NBG1 and HEL1 for every SX variant. The product pages show FSN1 and
  HEL1.

| Model / variant | CPU | RAM | HDD | NVMe | €/mo (incl. IPv4) | Setup | EUR/TB |
|---|---|---|---|---|---|---|---|
| **SX65 (SX65-2, base)** ROBOT_1737 | Ryzen 7 3700X 8c | 64 GB ECC | **4×16 TB SATA** | 2×1 TB (page)¹ | **84.00** (82.30 without IPv4) | 39 | **1.31** |
| SX65 4×22 TB, "limited" price ROBOT_1740² | Ryzen 7 3700X | 64 GB | 4×22 TB | 2×1 TB | 159.00 | 39 | 1.81 |
| SX65-1 4×22 TB, regular ROBOT_1738 | Ryzen 7 3700X | 64 GB | 4×22 TB | 2×1 TB | 274.00 | 134 | 3.11 |
| SX65-3 4×22 TB, 128 GB ROBOT_1739 | Ryzen 7 3700X | 128 GB | 4×22 TB | 2×1 TB | 294.00 | 144 | 3.34 |
| **SX135-2 (base)** ROBOT_1741 | Ryzen 9 3900 12c | 128 GB ECC | 8×16 TB SATA, hot-swap | 2×1.92 TB DC | 289.00 | 144 | 2.26 |
| SX135 8×22 TB, "limited" ROBOT_1743² | Ryzen 9 3900 | 128 GB | 8×22 TB | 2×1.92 TB | 309.00 | 39 | 1.76 |
| SX135-1 8×22 TB, regular ROBOT_1742 | Ryzen 9 3900 | 128 GB | 8×22 TB | 2×1.92 TB | 449.00 | 224 | 2.55 |
| **SX295** ROBOT_1744 | EPYC 7502P 32c | 256 GB ECC reg | 14×22 TB SATA | 2×7.68 TB DC | 879.00 | 439 | 2.85 |
| SX295, "limited" ROBOT_1745² | EPYC 7502P | 256 GB | 14×22 TB | 2×7.68 TB | 599.00 | 59 | 1.94 |

Notes on the table:
- **Which drive size the base price includes.** The SX65 at €84 and the SX135 at €289 include
  **16 TB** drives. The 22 TB drives are separate variants, not add-ons: the drive upgrade costs
  +€190/mo on the SX65 and +€160/mo on the SX135 at regular price **[E]**. The pages show no
  per-drive add-on price, and the SX line cannot add extra drives. The matrix page states maxima
  of "4x 16 TB up to 4x 22 TB", "8x 16 TB up to 8x 22 TB" and "14x 22 TB".
- ¹ The SX65-2 variant JSON lists only RAM plus 4×16 TB SATA. The page says 2×1 TB NVMe. Which
  is right is **[U]**.
- ² The "limited" product keys appear as the `limit-price` element on the matrix page. That
  element is hidden by default, and the visible text reads "starting from … € 159.00 … € 84.00".
  They look like limited-quantity prices with a reduced setup fee. Their terms and how long they
  last are **[U]**.

### Hetzner Server Auction (refurbished)

Source: `https://www.hetzner.com/_resources/app/data/app/live_data_sb.json`, read today. 68
servers were listed, 28 of them with HDDs.

Auction servers have no setup fee, monthly terms, a 1 Gbit port and unlimited traffic, and the
IPv4 costs €1.70 extra. Prices fall over time (the `next_reduce` timer). The cheapest HDD
listings per TB:

| CPU | RAM | HDD | DC | €/mo | EUR/TB |
|---|---|---|---|---|---|
| Ryzen 7 3700X | 128 GB ECC | 10×16 TB | HEL1-DC6 | 343 | **2.14** (lowest today) |
| Ryzen 7 3700X | 128 GB ECC | 10×16 TB | FSN1-DC14 | 350 | 2.19 |
| Ryzen 5 3600 | 64 GB ECC | 4×16 TB | HEL1-DC8 | 145 | 2.27 |
| Ryzen 5 3600 | 64 GB ECC | 4×16 TB | FSN1-DC18 | 146 | 2.28 |
| Ryzen 9 3900 | 128 GB ECC | 10×16 TB | HEL1-DC6 | 384 | 2.40 |
| Xeon E3-1271v3 | 32 GB ECC | 4×10 TB | FSN1-DC1 | 115 | 2.88 |

There were no 15×10 TB or 14×16 TB boxes today. Everything at 2×8 TB or smaller costs €6/TB or
more. **Verdict:** the auction is beaten by the new SX65 (€1.31/TB) and by the limited SX
variants.

### Hetzner Storage Box (managed, not a server)

Sources: <https://www.hetzner.com/storage/storage-box/> and
[docs](https://docs.hetzner.com/storage/storage-box/general).

| Tier | TB | €/mo | EUR/TB |
|---|---|---|---|
| BX11 | 1 | 3.20 | 3.20 |
| BX21 | 5 | 10.90 | 2.18 |
| BX31 | 10 | 20.80 | 2.08 |
| BX41 | 20 | 40.60 | **2.03** (TB here are usable; RAID is on Hetzner's side) |

- The setup fee is listed as "one-time", but the amount was not shown **[U]**.
- Traffic is unlimited.
- Locations: Germany or Finland.
- Protocols: FTP/FTPS, SFTP/SCP, SMB/CIFS, rsync over SSH, BorgBackup, restic, rclone, WebDAV,
  HTTPS.
- Limits (docs): **10 simultaneous connections per account**, 100 sub-accounts, 10 to 40
  snapshots. Bandwidth is "up to 1-10 Gbit/s per host server", shared, and "may vary over time".
- Usable as a segment store? Only as a passive remote file store. Precog would write whole sealed
  segments over SFTP or SMB and read them back. The catches:
  - There is no block device and no compute.
  - The 10-connection cap and the shared, variable bandwidth (no throughput SLA) rule it out for
    many concurrent catch-up readers.
  - Each box sits in one location, so it is one failure domain.
  - Per usable TB it is about the same price as SX65 data under 3+2 erasure coding **[E]**.
  - It is a reasonable "one shard per box" target, or a backup of a backup.

---

## 2. OVHcloud (Kimsufi, So you Start, Rise = "Eco"; Advance, Scale/HGR = bare metal)

Sources:
- Public order catalogs `https://eu.api.ovh.com/v1/order/catalog/public/eco?ovhSubsidiary=IE` and
  `.../baremetalServers?ovhSubsidiary=IE`. Prices there are € excluding VAT, and the IE catalog
  carries 23% VAT separately.
- Stock from `https://eu.api.ovh.com/v1/dedicated/server/datacenter/availabilities?planCode=…`,
  read today.
- <https://www.ovhcloud.com/en-ie/bare-metal/storage/> and <https://eco.ovhcloud.com/en-ie/>.

Terms:
- The setup fee equals one month's rent with no commitment. It is waived on 12- or 24-month
  upfront payment, which is also about 5% cheaper (e.g. SYS-STOR-LE-1 €376.09 per 12 months).
- Black Friday promo BF26_ECO_FREESETUP (17 Nov to 1 Dec 2026) waives the RISE-STOR setup fee.
- Eco traffic is "unlimited incoming and outgoing" (Eco page footnote).

Bandwidth and private networking (from the catalog addon codes):
- KS (Kimsufi): 500 Mbit/s, **no vRack**.
- SYS (So you Start): 1 Gbit/s public, vRack 500 Mbit/s (1 Gbit/s for +€10).
- RISE: 1 Gbit/s public (2 Gbit/s for +€100, 3 Gbit/s for +€150), vRack 1 Gbit/s burst to 2
  Gbit/s (2-4 Gbit/s for +€10).
- ADVANCE: 3 Gbit/s public, vRack 25 Gbit/s.
- HGR-STOR: 10 Gbit/s public, vRack 50 Gbit/s.

vRack is OVH's private L2 network spanning datacenters.

| Plan (planCode) | CPU | RAM | HDD | €/mo | Setup | Stock today | EUR/TB |
|---|---|---|---|---|---|---|---|
| **SYS-STOR-LE-1** (27sysstorle01-v1), Limited Edition | Xeon D-2141I 8c | 32 GB ECC | 4×14 TB SATA + 512 GB NVMe | 34.99 | 32.99 | **none available** | **0.62** |
| SYS-STOR-LE-1 | same | same | 4×12 TB + 512 GB NVMe | 32.99 | 32.99 | none | 0.69 |
| **KS-STOR** (24skstor01-v1) | Xeon-D 1521 4c | 16 GB ECC | 6×12 TB SATA | 103.99 | 23.99 | "unknown" in bhs/fra/gra/lon/rbx/sbg/waw | **1.44** |
| KS-STOR | same | same | 6×8 TB | 83.99 | 23.99 | "unknown" | 1.75 |
| KS-STOR | same | same | 4×4 TB + 500 GB NVMe | 23.99 | 23.99 | not listed | 1.50 |
| KS-LE-3 (27skle03-v1) | Xeon E-2274G 4c | 32 GB | 2×8 TB + 2×480 GB SSD | 24.99 | 19.99 | not listed | 1.56 |
| KS-LE-3 | same | same | 3×4 TB | 21.99 | 19.99 | bhs, fra, gra (1H-low) | 1.83 |
| SYS-STOR (24sysstor01-v3) | Ryzen 5 Pro 3600 6c | 32 GB | 6×6 TB + 2×512 GB NVMe | 66.99 | 54.99 | none | 1.86 |
| **RISE-STOR** (24risestor01-v1) | Ryzen 7 Pro 3700 8c | 32 GB | 8×14 TB **SAS** + 2×480 GB SSD | 239.99 | 164.99 (0 until 1 Dec) | bhs/fra/lon/waw (72H; lon, waw 1H) | 2.14 |
| RISE-STOR | same | same | 4×14 TB SAS | 164.99 | same | yes | 2.95 |
| **HGR-STOR-1** (24hgstor01-v2) | Xeon Gold 6554S 36c | 128 GB | 24×22 TB SAS + 2×960 GB NVMe | 1,464.99 | 1,464.99 | fra/lon/rbx (72H; rbx 1H-low) | 2.77 |
| HGR-STOR-1 | same | same | 36×22 TB SAS | 2,444.99 | same | yes | 3.09 |
| HGR-STOR-1 (26hgstor01-v2, new gen) | EPYC 9135 | 128 GB | 24×24 TB SAS | 2,785.99 | 2,073.99 | many DCs (72H) | 4.84 |
| ADVANCE-STOR (24advstor01-v3) | EPYC 4344P 8c | 32 GB | 8×22 TB SAS + 2×960 GB NVMe | 800.99 | 264.99 | all EU DCs (72H) | 4.55 |
| ADVANCE-STOR (26advstor01-v7) | EPYC 4345P | 32 GB | 8×24 TB SAS | 920.99 | 384.99 | all EU DCs | 4.80 |

- "LE" = Limited Edition, a refurbished batch. "unknown" and "72H" are OVH's own availability
  codes. OVH only shows stock when you order, and SYS-STOR-LE-1 is listed in the catalog but sold
  out.
- Disk replacement: tickets are scheduled 24/7. A cold swap means downtime, and OVH requires a
  backup and data-loss acknowledgement. Hot-swap needs a RAID card or HG-class servers
  ([disk-replacement](https://docs.ovhcloud.com/en/guides/bare-metal-cloud/dedicated-servers/disk-replacement),
  [hotswap](https://docs.ovhcloud.com/en/guides/bare-metal-cloud/dedicated-servers/hotswap-raid-hard)).
  Eco-range intervention SLAs **[U]**.

---

## 3. Scaleway Dedibox (Online.net), FR (DC3, DC5) and NL

The scaleway.com pages return 403 to automated clients, so every figure here comes from search
snippets of Scaleway's own pages **[3P]**. Scaleway says Store prices "are updated daily based on
demand, stock and component/energy prices". Commitment discounts are 5% for 12 months and 10% for
24 months; the options page **[3P]**.

| Model | CPU | RAM | HDD | €/mo | EUR/TB |
|---|---|---|---|---|---|
| Store-2-L-144TB | Xeon E5-1620v4 | 64 GB | 12×12 TB | 299.99 (254.99 with 36-month commitment, Sep/Oct 2026 promo page) | 2.08 (1.77) |
| Store-8-S | Xeon E5-1410v2 | 96 GB | 4×12 TB | 131.99 | 2.75 |
| Store-9-M-HDD | **[U]** | **[U]** | 3×12 TB + 2×1 TB NVMe | 239.99 | 6.67 |

- Setup has historically been about one month's rent **[3P]**.
- Port up to 1 Gbit/s, unmetered **[3P]**.
- Private network: RPN, the Dedibox private network **[U]** on pricing.

---

## 4. Other dedicated providers

| Provider | Offer | Location | CPU / RAM | HDD | Price | Setup / term | Port / traffic | Private net | EUR/TB |
|---|---|---|---|---|---|---|---|---|---|
| **Worldstream** ([storage page](https://www.worldstream.com/en/dedicated-servers/storage/)) | Dell T640 "Storage Deal" | NL (Naaldwijk) | 2× Xeon Gold 5120, 128 GB | 18×12 TB + 2×960 GB NVMe | €538 (struck-through €712) | No setup; term **[U]** | 1 Gbit/s guaranteed, 100 TB/mo | **[U]** | 2.49 |
| Worldstream | Fujitsu RX2540 M2 | NL | 2× E5-2620v4, 96 GB | 10×10 TB + 2×960 GB SSD | €285 (€362) | No setup | 1 Gbit/s, 100 TB | **[U]** | 2.85 |
| Worldstream | Fujitsu RX1330 M3 | NL | E3-1230v6, 32 GB | 4×10 TB + 1 TB NVMe | €115 (€182) | No setup | 1 Gbit/s, 100 TB | **[U]** | 2.88 |
| **ReliableSite** [3P] ([whtop](https://www.whtop.com/plans/reliablesite.net/140886), added 2025-11-29) | HDD Storage 80TB | NYC, Miami, LA | 8c, 32 GB | 4×20 TB | $169 | Free setup | Unmetered; port **[U]** | **[U]** | ~1.84 |
| ReliableSite [3P] | HDD Storage 40TB / 32TB | US | **[U]** | 2×20 TB / **[U]** | $99 | free | unmetered | **[U]** | ~2.15 / ~2.69 |
| **servarica** ([site data](https://servarica.com/dedicated/)) | Orangutan "NON RAID - LOADED" | Montreal | 20 vCPU (E5 v2-era), 128 GB | 113 TB HDD | $145 | **out of stock** | 1 Gbit/s ("Unlimited 250Mbps+") **[U]** | **[U]** | ~1.12 |
| servarica | Panda / Gorilla / Orangutan / Elephant | Montreal | various | 32 / 32 / 43 / 192 TB | $69 / $79 / $95 / $448.5 | all **out of stock** | Elephant: 200 TB/mo | | 1.88-2.15 |
| **FDC Servers** ([upgrade pricing](https://www.fdcservers.net/upgrade-pricing)) | HDD add-on on any dedicated server | many | | +HDD | **$6/TB/mo** (or $36/TB one-time) | | Storage page: "25 Gbps unmetered" stack | | ~5.2 for the add-on alone |
| **DataPacket** ([pricing](https://www.datapacket.com/pricing)) | Maximum HDD option is 2×24 TB (+$133/mo) | 60+ PoPs | servers start at about $1.2k/mo in the default view | 2×24 TB | | | Bandwidth-oriented | | not competitive |
| **Latitude.sh** ([pricing](https://www.latitude.sh/pricing/bare-metal)) | NVMe only, no HDD plans | | | | from $296 | | 20 TB out free, 2×10 Gbps or more | | n/a |
| **Cherry Servers** | Dedicated catalog renders client-side; no HDD storage server found on page | LT, NL, DE, US, SG | | | | | | | **[U]** |
| **Leaseweb** | Site renders client-side and gives no list prices; old 4×3 TB AMS €57.59 listing **[3P]** | AMS, FRA, US, APAC | | | **[U]** | | | Private network | **[U]** |
| **Servers.com, Hivelocity, Psychz, InterServer dedicated, OneProvider** | Site blocked (403) or quote-only; no current HDD-server price found | | | | **[U]** | | | | **[U]** |
| **netcup** ([storage space](https://www.netcup.com/en/server/storage-space)) | NetApp NFS add-on, tariff C: 1 TB included, €0.024/extra GB | DE | | | €18.23/mo for 1 TB | | | | ~18-24 (not cheap) |

---

## 5. Storage VPS and block-storage offers (oversold or shared; caveats below)

| Provider | Offer | €/TB-mo | Source | Notes |
|---|---|---|---|---|
| servarica | Killer Whale 3.5 TB $8.5; Polar Bear 2 TB $5; Mammoth 8 TB $20; Chimera Hybrid 2-16 TB | ~2.1-2.2 | site JSON, today | Montreal. **All out of stock today.** RAIDZ2 "Big Storage". 12-24 TB/mo transfer caps. |
| Hetzner Storage Box BX41 | 20 TB | 2.03 | official | Managed. 10 connections. See §1. |
| InterServer Storage VPS | "slices" | ~2.6 ($3/TB) | **[3P]** | NJ/LA. Shared HDD. |
| AlphaVPS Storage VPS | 1-16 TB, RAID-60 HDD + NVMe cache | 3.33 (annual billing) | AlphaVPS page via snippet **[3P]** | Sofia, BG. Small single-provider operation. |
| BuyVM Block Storage Slab | 256 GB steps, up to 10 TB per slab, 8 slabs per slice | ~4.35 ($5/TB) | [official](https://buyvm.net/block-storage-slabs/) | Needs a BuyVM KVM slice. LV, NY, LU, MIA. Frequently sold out. |
| HostHatch Storage | 1 TB to 10 TB plans | ~4.35 ($5/TB) | **[3P]** (site is client-rendered) | Many locations. Promos as low as about $22-26/TB-year are posted on LowEndTalk **[3P]**. |
| TNA Hosting | 4 TB $60/yr | ~1.09 | **[3P]** | Chicago. Tiny provider. |
| netcup local block storage | HDD volumes ≤ 8 TB | ~12 | **[3P]** | |
| Contabo Storage VPS | SSD, up to 1.4 TB | ~13-26 | **[3P]** (site 403) | SSD, not HDD. Poor per TB. |
| RackNerd | no multi-TB HDD storage VPS found | n/a | **[3P]** | Largest HDD plan seen: 750 GB. |

Storage VPS caveats:
- Disks are shared and oversold, with noisy-neighbour IOPS. The 50-200 MB/s sequential target is
  not guaranteed.
- Monthly transfer caps of 6-24 TB are common, which is a problem for re-replication.
- Many are one-person or small operations with frequent "out of stock". Several promo prices are
  annual or prepaid only.
- Host failure takes all of a provider's VPSs on that node with it.
- Use them, at most, as one extra failure domain for parity shards, never as the only copy.

---

## 6. Ranking: real dedicated hardware, EUR per raw TB-month **[E]**

The "usable" columns are raw price ÷ 0.6 (3+2), ÷ 0.667 (6+3) and × 3 (3-way replication).

| # | Offer | €/mo | Raw TB | €/raw TB | 3+2 | 6+3 | 3× | Orderable today? |
|---|---|---|---|---|---|---|---|---|
| 1 | OVH SYS-STOR-LE-1 4×14 TB | 34.99 | 56 | **0.62** | 1.04 | 0.94 | 1.87 | No (sold out) |
| 2 | OVH SYS-STOR-LE-1 4×12 TB | 32.99 | 48 | 0.69 | 1.15 | 1.03 | 2.06 | No |
| 3 | servarica Orangutan 113 TB | ~126 | 113 | 1.12 | 1.86 | 1.67 | 3.35 | No |
| 4 | **Hetzner SX65 4×16 TB** | 84.00 | 64 | **1.31** | 2.19 | 1.97 | 3.94 | **Yes** (FSN1, NBG1, HEL1) |
| 5 | OVH KS-STOR 6×12 TB | 103.99 | 72 | 1.44 | 2.41 | 2.17 | 4.33 | Unclear ("unknown") |
| 6 | OVH KS-STOR 6×8 TB | 83.99 | 48 | 1.75 | 2.92 | 2.62 | 5.25 | Unclear |
| 7 | Hetzner SX135 8×22 TB (limited price) | 309.00 | 176 | 1.76 | 2.93 | 2.63 | 5.27 | Listed, limited **[U]** |
| 8 | Scaleway Store-2-L 12×12 TB, 36-month commitment **[3P]** | 254.99 | 144 | 1.77 | 2.95 | 2.66 | 5.31 | **[U]** |
| 9 | Hetzner SX65 4×22 TB (limited price) | 159.00 | 88 | 1.81 | 3.01 | 2.71 | 5.42 | Listed, limited **[U]** |
| 10 | ReliableSite 4×20 TB **[3P]** | ~147 | 80 | 1.84 | 3.06 | 2.76 | 5.51 | **[U]** |

Next after the top 10:

| Offer | €/raw TB | Note |
|---|---|---|
| OVH KS-LE-3 3×4 TB | 1.83 | in stock, too small |
| Hetzner SX295 (limited) | 1.94 | |
| Scaleway Store-2-L, no commitment **[3P]** | 2.08 | |
| OVH RISE-STOR 8×14 TB SAS | 2.14 | in stock; best OVH option with vRack |
| Hetzner auction 10×16 TB | 2.14 | |
| Hetzner SX135-2 | 2.26 | |
| Worldstream 18×12 TB | 2.49 | |
| OVH HGR-STOR-1 24×22 TB | 2.77 | 10G public, 50G vRack |
| Hetzner SX295 (regular) | 2.85 | |

**Cheapest you can order today with confidence:**
- Hetzner SX65 at €1.31/TB.
- Then OVH RISE-STOR 8×14 TB at €2.14/TB, the cheapest per TB with a real private network.
- For bandwidth, OVH HGR-STOR-1 at €2.77/TB (10 Gbit/s public, 50 Gbit/s vRack).

## 7. Ranking: storage VPS / managed, EUR per TB-month (usable as sold) **[E]**

| # | Offer | €/TB | Status |
|---|---|---|---|
| 1 | TNA Hosting 4 TB **[3P]** | ~1.09 | tiny provider, annual |
| 2 | Hetzner Storage Box BX41 | 2.03 | in stock, official |
| 3 | servarica storage VPS | ~2.1-2.2 | out of stock |
| 4 | InterServer storage VPS **[3P]** | ~2.6 | |
| 5 | AlphaVPS storage **[3P]** | 3.33 | annual billing |
| 6 | BuyVM slab | ~4.35 | plus a slice; often sold out |
| 7 | HostHatch storage **[3P]** | ~4.35 | |
| 8 | netcup block storage **[3P]** | ~12 | |
| 9 | Contabo Storage VPS (SSD) **[3P]** | ~13+ | |
| 10 | netcup storage space (NFS) | ~18 | |

---

## 8. Risks for a cold segment tier

**Throughput vs. port.**
- 1 Gbit/s is about 119 MB/s. That serves the low end of the 50-200 MB/s read target per server
  but not 200 MB/s.
- Kimsufi's 500 Mbit/s (about 60 MB/s) misses most of the target.
- On Hetzner, vSwitch shares the same 1 Gbit NIC, so replication traffic and reader traffic
  compete. Hetzner's 10G uplink loses unlimited traffic (€1/TB over 20 TB).
- OVH RISE (1-3 Gbit/s plus a separate vRack), ADVANCE (3 Gbit/s plus 25 Gbit/s vRack) and HGR
  (10 Gbit/s plus 50 Gbit/s vRack) separate internal from public traffic.

**Re-replication after a loss** **[E]**, at full line rate with no other traffic:

| Link | One 16 TB disk | One 22 TB disk | Whole 100 TB |
|---|---|---|---|
| 500 Mbit/s (Kimsufi) | 71 h | 98 h | **18.5 days** |
| 1 Gbit/s (Hetzner SX, SYS, RISE base) | 36 h | 49 h | **9.3 days** |
| 3 Gbit/s (OVH ADVANCE public) | 12 h | 16 h | 3.1 days |
| 10 Gbit/s (HGR public) | 3.6 h | 4.9 h | 22 h |
| 25 Gbit/s (OVH vRack ADV) | 1.4 h | 2.0 h | 9 h |

With erasure coding, rebuilding one lost shard reads k shards: 3× (3+2) or 6× (6+3) the lost
bytes cross the network, spread over the surviving nodes. At 1 Gbit/s, a dead 14×22 TB SX295
(308 TB) therefore takes weeks to rebuild. Prefer more, smaller boxes (e.g. SX65 4×16 TB = 64 TB
per failure unit) or 10G+ private links. Also budget the gap between a disk failing and the
provider replacing it.

**Failure-domain diversity.**
- Hetzner gives about 3 sites: FSN1 (many DC buildings in one park), NBG1 (near FSN, the same
  region and network zone) and HEL1 (Finland).
- OVH EU gives up to about 7 sites: RBX, GRA, SBG, FRA, LON, WAW, plus BHS in Canada. Eco stock
  varies by site.
- Scaleway: Paris DC3/DC5 plus Amsterdam. Worldstream: one NL campus.
- 3+2 needs 5 independent domains and 6+3 needs 9. Neither fits one provider's distinct cities;
  you would have to treat DC buildings or racks as domains (weaker), or mix providers. A
  practical mix is Hetzner ×3 sites + OVH ×3-4 sites, with optionally Scaleway or Worldstream as
  a 5th or 6th.
- Cross-provider traffic leaves the private networks. It is free and unmetered on Hetzner, OVH
  Eco and Scaleway, but Worldstream caps it at 100 TB/mo.

**Disk failure handling.**
- Every provider here replaces failed drives free on a ticket. You give the serial number and,
  at OVH, confirm a backup exists.
- At Hetzner and in OVH Eco, a swap means a short outage unless the chassis and RAID card
  support hot-swap. The SX135 bays are hot-swap; OVH only hot-swaps HG-class servers or those
  with a RAID card.
- No Eco or budget provider publishes a drive-replacement SLA **[U]**. Assume hours to a day,
  plus your own rebuild.

**Minimum terms and setup.**
- Hetzner: monthly (hourly-capped). Setup €39-439, or €39-59 on the limited prices.
- OVH: monthly. Setup = 1 month's rent, waived with a 12- or 24-month upfront payment.
- Scaleway: monthly with setup, or a 12/24/36-month commitment for a discount.
- Worldstream: no setup; term **[U]**.
- Storage VPS: often annual prepaid.

**Stock risk.** The cheapest per-TB offers (OVH LE, servarica, KS-STOR) are limited batches that
are sold out or of unknown stock today. Plan on the SX65 and RISE/HGR prices, and treat LE stock
as opportunistic.

---

## Research: object storage and owned disks in colocation

# Cold tier for sealed log segments: object storage vs. owning disks (as of 2026-11-19)

Workload: sealed segments, 128 MB objects, written once, read back on average **once**, by a server at a
different provider (e.g. Hetzner), deleted after **14 days** (case A) or **60 days** (case B). Must be
readable within seconds, so no Glacier Flexible/Deep Archive, tape, OVH Cold Archive, Scaleway Glacier,
Azure Archive or GCS Archive.

Legend: unmarked = read on the provider's own pricing page or API on 2026-11-19 (URL in Sources);
**[3P]** = third-party figure; **[E]** = my arithmetic or assumption; **[U]** = could not verify.

Conventions [E]: TB = 10^12 bytes; per-GiB prices × 931.3 = per TB; per-hour prices × 730 = per month;
**1 EUR = 1.15 USD** (assumed, not a quoted rate); all prices excl. VAT.

---

## Part 1 - S3-compatible object storage

### 1.1 Provider facts

| Provider / class | Storage (list) | Min. duration / early delete | Min. object size | Egress | Requests | Notes: regions, durability, limits, minimum spend |
|---|---|---|---|---|---|---|
| **Hetzner Object Storage** | €6.49/mo base includes 1 TB storage + 1 TB egress; overage €0.0087 per TB-hour (≈ €6.35/TB-mo [E]) | None; billed hourly | 64 KB | Overage €1.00/TB. **Free within Hetzner's `eu-central` network zone** | PUT/GET/DELETE free; ingress free | FSN1, NBG1, HEL1. Ceph; no durability figure or replica/EC detail published; one location per endpoint. 750 req/s per bucket and per source IP, 256 TCP sessions per IP, ≤10 Gbit/s per bucket, ≤100 TB and 50 M objects per bucket, 100 buckets per account. Base fee charged per hour while any bucket exists |
| **Backblaze B2** | $6.95/TB-mo (byte-hour billing, first 10 GB free) | None | None | Free up to 3× average monthly storage, then $0.01/GB; free unlimited through partner CDNs/compute (Cloudflare, Fastly, CoreWeave...) | Class A/B/C free (pay-as-you-go); Class D $0.004/10k | US East (Reston), US West (Sacramento + Phoenix), EU Central (Amsterdam), CA East (Toronto). Durability not stated on pricing or regions page [U] |
| **Wasabi** (Pay-as-you-go) | $7.99/TB-mo | **90 days** (Timed Deleted Storage for remaining days); RCS (reserved) 30 days | 4 KB | No egress fee, **fair use: monthly egress ≤ active storage**; repeated overage may lead to limits or suspension | No API fees | 1 TB minimum charge. RCS: 25 TB+, 1/3/5-yr terms, price on quote. Durability not stated [U] |
| **iDrive e2** | $6/TB-mo pay-as-you-go ($5 for Veeam/MSP/reseller); yearly plans from $59.50/TB-yr | None | Not stated | Free up to 3× active storage, then $0.01/GB | No API charges | 1 TB minimum billing. Regions not listed on pricing page [U] |
| **Cloudflare R2 Standard** | $15/TB-mo ($0.015/GB) | None | Not stated | **Free** | Class A $4.50/M, Class B $0.36/M | 11 nines design durability; data across several data centers in a region, synchronous persist before ack; 99.9% availability SLA |
| **Cloudflare R2 Infrequent Access** | $10/TB-mo | **30 days** | Not stated | Free | Class A $9/M, Class B $0.90/M; **retrieval $0.01/GB** | No free tier on IA |
| **Storj** (Standard) | $7/TB-mo (Advanced: $10/TB) | **30 days** (Standard) | 50 KB | **$7/TB** | Segment fees removed on new tiers | Globally distributed nodes; $5 minimum invoice |
| **Scaleway** Standard One Zone | €0.000011/GB-hour ≈ €8.03/TB-mo [E] | None | Not stated | 75 GB/mo free, then **€0.01/GB** (€10/TB) | Free (requests and ingress included) | fr-par, nl-ams, pl-waw, it-mil; single AZ. New hourly rates from 2026-06-01 per Scaleway blog; an older EN pricing page showed €0.0000103/GB-h [3P via search]. Pricing page blocked our fetch (HTTP 403); figures from Scaleway's public product-catalog API |
| **Scaleway** Standard Multi-AZ | €0.000022/GB-hour ≈ €16.06/TB-mo [E] | None | Not stated | as above | Free | fr-par, nl-ams, pl-waw, it-mil; 3 AZ. (Glacier class excluded: restore needed) |
| **OVHcloud 1-AZ Standard** | $0.0081103/GiB-mo ≈ **$7.55/TB-mo** [E] | None | Not stated | **Free** (internal and external traffic) | Free | HDD; 99.9% availability SLA; ms TTFB |
| **OVHcloud 1-AZ High Performance** | $0.0202794/GiB-mo ≈ $18.89/TB-mo [E] | None | Not stated | Free | Free | NVMe; 1-AZ only |
| **OVHcloud 1-AZ Infrequent Access** | $0.0043581/GiB-mo ≈ **$4.06/TB-mo** [E] | **30 days** (prorated hours × hourly rate) | Not stated | Free | Free; **retrieval $0.00436/GiB** (≈ $4.06/TB [E]) on GET | ms TTFB |
| **OVHcloud 3-AZ Standard** | $0.0157/GiB first 50 TiB, $0.0140 next 450 TiB, **$0.0123 over 500 TiB** (≈ $14.63 / 13.06 / 11.49 per TB [E]) | None | Not stated | Free | Free | 3-AZ (Paris etc.); 99.99% availability SLA |
| **OVHcloud 3-AZ Infrequent Access** | $0.0103514/GiB-mo ≈ $9.64/TB [E] | 30 days | Not stated | Free | retrieval $0.00436/GiB | |
| **OVHcloud 3-AZ Active Archive** | $0.0053217/GiB-mo ≈ $4.96/TB [E] | **90 days** | Not stated | Free | retrieval $0.0212/GiB | ms TTFB (instant, not tape); 3-AZ only. (Cold Archive is tape, hours, excluded) |
| **Contabo** | ≈ €9.96/TB-mo (sold in 250 GB blocks at €2.49) **[3P]** | None [3P] | [U] | Free, unlimited **[3P]** | Free [3P] | Contabo pages returned HTTP 403 to us; all figures [3P]. Contabo's own blog quotes ≈ $11.96/TB [3P]. Bandwidth cap per bucket [U] |
| **Impossible Cloud** | €7.99/TB-mo pay-as-you-go; reserved from 25 TB, 1/3/5 yr, quote | None (monthly) | Not stated | Free under fair use (contact if egress "greatly exceeds" stored volume over a sustained period) | Free | European DCs (not named on page); 11 nines, erasure coding |
| **Akamai / Linode** | $5/mo includes 250 GB + 1 TB transfer pool; overage $0.02/GB ($20/TB) | None stated | Not stated | Overage **$0.005/GB** ($5/TB) most DCs; $0.015 Jakarta, $0.007 São Paulo | Not stated | Rate limits not stated on pricing page [U] |
| **DigitalOcean Spaces** | $5/mo includes 250 GiB + 1 TiB egress; overage $0.02/GiB (≈ $18.63/TB [E]) | None (Standard) | Not stated | 1,024 GiB/mo included, then $0.01/GiB (≈ $9.31/TB [E]); free to Droplets in same region | Not stated | 13+ regions (NYC3, AMS3, FRA1, ...) |
| **DigitalOcean Spaces Cold Storage** | $0.007/GiB-mo (≈ $6.52/TB [E]) | **30 days** | 128 KiB (and 128 KiB minimum per read) | as Spaces | **Retrieval $0.01/GiB**, free up to average daily Cold Storage usage for the month | Instant access [U] |
| **Vultr** Standard | $18/mo includes 1,000 GB storage + 1,000 GB egress; overage $0.018/GB | Not stated | Not stated | Overage $0.01/GB | Not stated | Premium $36, Performance $50, Accelerated $100 per 1 TB; inbound free |
| **UpCloud** Managed Object Storage | ≈ $20/TB-mo **[3P]** | [U] | [U] | Zero-cost egress under a Fair Transfer Policy (official); 3P reports a 1-48 TB fair-usage cap [3P] | [U] | UpCloud pricing page returned 403/404 to us |
| **Exoscale SOS** | €0.0000275/GiB-hour ≈ €18.70/TB-mo [E] (≤512 TiB; €0.00001875 to 1 PiB; €0.00001389 above) | None | Not stated | **€0.02/GB** | Not stated | 8 European zones. From Exoscale's public pricing API |
| **IONOS Object Storage** | per GB-hour, rate only in price lists; ≈ $0.00487/GB-30 d **[3P]**, ≈ €0.007/GB-mo **[3P]** | [U] | [U] | Tiered by account cumulative volume; rates not on docs page [U] | API free | Contract-owned buckets: egress to VMs in same DC is free; user-owned: all egress billed as public. Not ranked: no first-party rate |
| **Synology C2 Object** | $7.99/TB-mo (C2 OneStorage, from 2026-06-22) **[3P]**; older $6.99/TB [3P] | [U] | [U] | No egress or API charge **[3P]** | Free [3P] | Seattle or Frankfurt [3P]. Pricing page showed no rates to us |
| **Filebase** | Pro $7.50/mo includes 500 GB; extra $0.015/GB ($15/TB) | Not stated | Not stated | **Object egress free** | Not stated | Backed by decentralized networks (IPFS etc.) |
| **Tebi** | $500/mo includes 5 TB + 50 TB egress; extra $0.02/GB storage, $0.008/GB egress **[3P]** | [U] | [U] | [3P] | [U] | tebi.io unreachable through our proxy (502). Not ranked |
| **AWS S3 Standard-IA** (us-east-1) | $0.0125/GB-mo | **30 days** | **128 KB** | $0.09/GB first 10 TB/mo (100 GB/mo free across AWS), $0.085 next 40 TB, $0.07 next 100 TB, $0.05 above 150 TB | PUT $0.01/1k, GET $0.01/10k; **retrieval $0.01/GB** | Multi-AZ. From AWS Price List API |
| **AWS S3 One Zone-IA** | $0.01/GB-mo | 30 days | 128 KB | same | same as SIA; retrieval $0.01/GB | Single AZ |
| **AWS S3 Glacier Instant Retrieval** | $0.004/GB-mo | **90 days** | 128 KB | same | PUT $0.02/1k, GET $0.10/10k; **retrieval $0.03/GB** | ms access, multi-AZ |
| **GCS Nearline** (regional, e.g. us-central1) | $0.000013699/GiB-hour ≈ $0.010/GiB-mo | 30 days | Not stated | Internet $0.12/GiB first 10 TiB, $0.11 to 150 TiB, $0.08 above | Class A $0.01/1k, Class B $0.001/1k; retrieval $0.01/GiB | |
| **GCS Coldline** (regional) | ≈ $0.004/GiB-mo | 90 days | Not stated | same | Class A $0.02/1k, B $0.01/1k; retrieval $0.02/GiB | |
| **Azure Blob Cool LRS** (East US) | $0.0152/GB-mo | 30 days (prorated early delete) | Not stated | $0.087/GB after 100 GB (Microsoft network routing) | Write $0.10/10k, read $0.01/10k; retrieval $0.01/GB | From Azure Retail Prices API |
| **Azure Blob Cold LRS** (East US) | $0.0036/GB-mo | 90 days | Not stated | same | Write $0.18/10k, read $0.10/10k; retrieval $0.03/GB | |

### 1.2 Effective $/TB-month for our pattern [E]

Model: one TB written costs `price × max(retention, min_duration)/30` for storage, plus egress and
retrieval for one full read. Requests are 7,812 objects per TB × (1 PUT + 1 GET). The total is divided
by `retention/30` to give **$ per TB held at steady state, per month**. At steady state the monthly read
volume is 30/R × stored: **2.14×** at 14 days, **0.5×** at 60 days. That decides free-egress allowances:
B2 and iDrive (3×) stay free in both cases. Wasabi's fair use (≤1×) is **broken at 14 days**.
Fixed monthly base fees are ignored because they are negligible at TB scale.
Requests never exceed $0.08/TB for 128 MB objects (R2 IA, the worst case).

| Rank | Option | 14 d: $/TB-mo | 60 d: $/TB-mo | Durability scope |
|---|---|---|---|---|
| 1 | iDrive e2 | **6.00** | **6.00** | [U] |
| 2 | Backblaze B2 | **6.95** | 6.95 | single DC per region (US West: 2 DCs) |
| 3 | OVH 1-AZ Standard | 7.55 | 7.55 | 1 AZ |
| - | OVH 1-AZ Infrequent Access | 17.40 | **6.09** (2nd at 60 d) | 1 AZ |
| 4 | Hetzner OS, reader **inside** Hetzner eu-central | 7.30 | 7.30 | 1 location, Ceph |
| 5 | Synology C2 [3P] | 7.99 | 7.99 | [U] |
| 6 | Impossible Cloud | 9.19 (fair-use risk at 2.14× egress) | 9.19 | EC, 11 nines |
| 7 | Hetzner OS, reader **outside** Hetzner (€1/TB egress) | 9.77 | 7.88 | 1 location |
| 8 | Contabo [3P] | 11.45 | 11.45 | [U] |
| 9 | OVH 3-AZ Standard (>500 TiB) | 11.49 | 11.49 | 3 AZ |
| - | OVH 3-AZ IA | 29.36 | 11.67 | 3 AZ |
| - | Wasabi PAYG | 51.36 (90-day minimum = 6.4× storage; also breaks fair use) | 11.99 | [U] |
| - | Storj Standard | 30.00 | 10.50 | global nodes |
| - | Scaleway One Zone | 33.88 | 14.98 | 1 AZ |
| - | Filebase | 15.00 | 15.00 | [U] |
| - | Cloudflare R2 Standard | 15.08 | 15.02 | multi-DC region, 11 nines |
| - | Cloudflare R2 IA | 43.02 | 15.04 | same |
| - | DigitalOcean Spaces Cold | 44.57 | 11.18 | [U] |
| - | OVH 3-AZ Active Archive | 74.17 | 17.31 | 3 AZ |
| - | UpCloud [3P] | 20.00 | 20.00 | [U] |
| - | Linode/Akamai | 30.71 | 22.50 | [U] |
| - | Vultr Standard | 39.43 | 23.00 | [U] |
| - | DigitalOcean Spaces | 38.58 | 23.28 | [U] |
| - | Scaleway Multi-AZ | 43.11 | 24.22 | 3 AZ |
| - | Exoscale SOS | 70.79 | 33.00 | [U] |
| ref | AWS S3 One Zone-IA | 235.90 | 60.04 | 1 AZ |
| ref | AWS S3 Standard-IA | 241.26 | 62.54 | multi-AZ |
| ref | Azure Cool LRS | 240.61 | 63.74 | 1 DC (LRS) |
| ref | Azure Cold LRS | 274.33 | 64.01 | 1 DC |
| ref | GCS Nearline | 279.58 | 69.89 | region |
| ref | AWS S3 Glacier IR | 283.36 | 66.12 | multi-AZ |
| ref | GCS Coldline | 303.85 | 70.90 | region |

Hyperscaler cost is about 90% internet egress. At >150 TB/mo AWS egress falls to $0.05/GB, which still
gives about $130/TB-mo at 14 days [E].

What drives the ranking:
- **No minimum duration and free or large egress allowance** win: iDrive, B2, OVH Standard, Hetzner.
  Minimum-duration classes (Wasabi 90 d, OVH Active Archive 90 d, GIR 90 d, IA classes 30 d) cost 2-6×
  their list price at 14-day retention.
- **OVH 1-AZ IA** has the lowest list price among seconds-readable classes ($4.06/TB). It wins at 60 days
  but loses at 14 days because of the 30-day minimum and the $4.06/TB retrieval charge.
- **Hetzner** is the natural pick if the reader runs on Hetzner servers in eu-central (egress free). Its
  ceilings: 100 TB per bucket, 750 req/s per bucket and 10 Gbit/s per bucket, so stripe across buckets.

## Part 2 - owning the disks (buy and colocate)

### 2.1 Inputs

**Drives (2026 is a shortage year).**
- serverpartdeals, manufacturer-recertified, on 2026-11-19: WD HC580 24 TB **$749 ($31.21/TB)**,
  Exos 26 TB $799 ($30.73/TB), Exos 28 TB $899-910 (~$32/TB), Exos 22 TB $749, HC560 20 TB $649. No new
  20-28 TB drives were on the first listing page.
- New Exos X24 24 TB at Amazon: $670 (~$27.9/TB) on 2026-08-28 **[3P]**.
- A recertified Exos 24 TB went from $298.99 (Aug 2025) to $699 (Aug 2026), +134%. Channel recert prices
  went from $12-16/TB (summer 2025) to $24-31/TB **[3P]** (PCSP blog, a drive reseller).
- Seagate and WD say nearline capacity is allocated through 2026. Seagate says most is allocated through
  2028 (July 2026) **[3P]**. Lead times exceeded 52 weeks per TrendForce, Sept 2025 **[3P]**. Toshiba MG
  was not found in 20 TB+ on serverpartdeals [U].

**Power per drive.**
- Exos X24 SATA: 6.3 W idle, 8.9 W operating (Seagate figures as quoted by retailers) **[3P]**.
- SAS variant: 6.5 W idle, 9.8 W read **[3P]**.
- Model [E]: 60 × 7 W + ~300 W server, fans and expanders ≈ **0.75 kW average per 4U60**.

**Chassis.**
- New Supermicro SSG-640SP-E1CR60 (4U, 60-bay top-loader, dual-node): $10,474 (Anafra) to $11,890
  (Newegg, out of stock) **[3P]**. SSG-640SP-DE2CR60: £9,422 / €11,329 ex VAT **[3P]**.
- Model [E]: **$14,000** with CPUs, RAM, NICs and HBA.
- Used: NetApp DS460C 60-bay JBOD shells about €340-760 in EU listings; $1,597 + $348 shipping with
  2× IOM12 in the US **[3P]**. Used Supermicro SC847 36-bay chassis $379-699 **[3P]**.
- Model [E]: **$2,500** for a used 60-bay JBOD plus a used head server.

**Colocation (monthly, excl. VAT).**

| Provider | Offer | Power | Network | Other |
|---|---|---|---|---|
| **Hetzner** (NBG, FSN, HEL) | 1/3 rack (14U) from €100, setup €199; Rack Basic (42U) €167.23; Rack Advanced (47U) from €251.26 | Metered: **€0.33/kWh** in Germany, **€0.22/kWh** in Finland. 1/3 rack: 2×16 A, recommended max 2×10 A | 1 Gbit/s port, 2-10 TB included, then **€1/TB** | Remote hands €21 per 15 min. **Currently not accepting new colocation** ("until further notice") |
| **xaweho** (Düsseldorf) | 4U €66.89 net; full rack (45U, 2×16 A up to 7.2 kW) €462.18 net + €167.23 setup | **€327.73 per kW of average draw** net | | Remote hands €21.01 net per 15 min |
| **Voxility** at Templus AMS01 (Amsterdam), 1-yr term | Full rack 47U with 2 kW: **$1,099.84**; +3 kW $405.44, +4 kW $810.88, +6 kW $1,622.88 | Over-commit $106.40 per 0.1 kW | | Remote hands $56/h. Prices dated 2026-11-19 |
| **Voxility** at Digital Realty (Interxion) FRA8 (Frankfurt), 1-yr term | Full rack with 3 kW: **$2,564.80**; +4/5/6 kW: $611.52 / $1,223.04 / $1,835.68 | | | Remote hands $56/h |
| **Hurricane Electric** (Fremont) | 42U cabinet including power and 1 Gbps: **$600** (one per customer) | Amps not stated [U] | 1 Gbps included | |
| **Joe's Datacenter** (Kansas City) | Full 42U $1,200 incl. 1 Gbps and 33 TB transfer; half $650; quarter $295 **[3P]** | Amps not stated [U] | 1 Gbps, 33 TB transfer | Remote hands $40/h [3P] |
| MIRhosting (Germany) | Full rack €380/mo **[3P]** | Billed per kW, 4 kW minimum [3P] | | |
| Wholesale reference (CBRE, Q1 2026, 250-500 kW deals) | | Frankfurt **$235-265/kW-mo**; Europe's big 4 markets $165-265; N. Virginia $190-235 **[3P]** | | |

Not found: NorthC, Datacenter Luxembourg, Equinix and Leaseweb publish no colocation prices
(quote-only) [U]. No Poland quote was found [U].

**Transit and failure rates.**
- Transit: 10GE IP transit at $0.07/Mbps-mo lowest, $0.08 weighted median in key US/EU cities
  (TeleGeography, Sep 2026) **[3P]**. HolyCloud 10 Gbps at €900/mo, 95th percentile **[3P]**.
- Failure rate: Backblaze Drive Stats, Q2 2026 snapshot: quarterly AFR **1.73%**, 2025 annual 1.36%,
  lifetime 1.41%. Q1 2026: drives of 20 TB and up at 0.85% AFR **[3P]** (press report of Backblaze data).
  Model [E]: **1.5% AFR**, with replacement at full drive price plus 30 min of remote hands.

### 2.2 Worked TCO: one 4U60 with 24 TB drives (1,440 TB raw) [E]

Assumptions:
- 5-year straight-line amortization, no cost of capital, no residual value.
- Usable capacity = raw × k/(k+m) × **85% fill** (rebuild headroom). That gives **890 TB** at 8+3 and
  **816 TB** at 6+3.
- One chassis per failure domain. 8+3 needs ≥ 11 chassis (≈ 9.8 PB usable) to survive 3 chassis losses.
  6+3 needs ≥ 9 chassis (≈ 7.3 PB usable).
- Colo share per chassis at 0.75 kW:
  - Hetzner: 1/3 rack shared by 3 chassis plus metered kWh.
  - xaweho: full rack shared by 9 chassis.
  - Voxility: 6 kW full rack shared by 8 chassis.
  - CBRE: midpoint $215/kW.

| Hardware scenario | Colo | Colo $/chassis-mo | Total $/chassis-mo | $/raw-TB-mo | $/usable-TB-mo 8+3 | 6+3 |
|---|---|---|---|---|---|---|
| New chassis $14k + $749 recert 24 TB | Hetzner HEL (€0.22/kWh) | 177 | 1,219 | 0.85 | **1.37** | 1.49 |
| | Hetzner NBG/FSN (€0.33/kWh) | 246 | 1,288 | 0.89 | 1.45 | 1.58 |
| | xaweho Düsseldorf | 342 | 1,384 | 0.96 | 1.55 | 1.70 |
| | Voxility Templus AMS01 | 340 | 1,382 | 0.96 | 1.55 | 1.69 |
| | Voxility DLR FRA8 | 550 | 1,592 | 1.11 | 1.79 | 1.95 |
| | Wholesale $215/kW [3P] | 161 | 1,203 | 0.84 | 1.35 | 1.47 |
| Used chassis $2.5k + $749 recert | Hetzner HEL | 177 | 1,027 | 0.71 | **1.15** | 1.26 |
| | Voxility AMS01 | 340 | 1,191 | 0.83 | 1.34 | 1.46 |
| | Voxility FRA8 | 550 | 1,401 | 0.97 | 1.57 | 1.72 |
| Used chassis + 2025 drive price ($299) | Hetzner HEL | 177 | 544 | 0.38 | 0.61 | 0.67 |
| | Voxility AMS01 | 340 | 707 | 0.49 | 0.79 | 0.87 |

At $749/drive, drives are **$0.52/raw-TB-mo** of the total ($44,940 per chassis over 60 months).
Replacements add about $60/chassis-mo at 1.5% AFR. Doubling AFR to 3% for recertified drives adds about
$0.06/usable-TB.

**Costs the table leaves out [E], which this read pattern makes significant.**
- **Bandwidth.** At 14 days, a full 8+3 chassis serves 890 × 2.14 ≈ **1.9 PB/mo** out and the same in,
  ≈ 5.9 Gbit/s sustained each way.
  - A 1 Gbit/s colo port (Hetzner, HE) carries ≤ ~330 TB/mo, so these offers do not fit as-is.
  - Hetzner's €1/TB traffic would add about $2.46/usable-TB.
  - 10G-class transit at $0.08/Mbps, provisioned at 2× average, adds about **$1.06/usable-TB at 14 days**
    and about $0.25 at 60 days.
- **Operations labour.** 0.25 FTE at about $12.5k/mo loaded for an 11-chassis cluster ≈ $0.32/usable-TB.
- **Switching and NICs.** About $0.04/usable-TB.
- **Fully loaded, used chassis, Amsterdam, 8+3:** ≈ **$2.8/usable-TB-mo at 14 days** and **$2.0 at 60 days**.
  With new chassis, about $0.2 more.

## Part 3 - overall ranking by effective $/usable-TB-month for this pattern [E]

| Rank | Option | 14 d | 60 d | Operational burden | Durability / failure scope |
|---|---|---|---|---|---|
| 1 | Own 4U60s in colo, 8+3 across ≥11 chassis, used chassis, fully loaded (AMS) | ~2.8 | ~2.0 | **High**: hardware buying in a shortage, >52-week lead times possible, drive swaps, EC/repair software, ~10 PB minimum to get chassis-level failure domains, 1-yr colo terms, ~6 Gbit/s per chassis of transit at 14 d | Tolerates 3 chassis losses; still a **single site** unless racks are split across sites (which multiplies transit) |
| 2 | Same, new chassis, Hetzner HEL power price (colo closed to new orders) | ~2.6-3.0 | ~1.8-2.2 | same; Hetzner 1 Gbit ports too small | single site |
| 3 | iDrive e2 | 6.00 | 6.00 | None | not published [U] |
| 4 | OVH 1-AZ Infrequent Access | 17.40 | **6.09** | None | 1 AZ |
| 5 | Backblaze B2 | 6.95 | 6.95 | None; 3× free egress covers both cases | regional; US West spans 2 DCs |
| 6 | Hetzner Object Storage (reader in Hetzner eu-central) | 7.30 | 7.30 | Low; stripe across ≤100 TB buckets; per-bucket 750 req/s and 10 Gbit/s | 1 location; no published durability |
| 7 | OVH 1-AZ Standard | 7.55 | 7.55 | None; no minimum duration, free egress | 1 AZ, 99.9% SLA |
| 8 | Synology C2 [3P] | 7.99 | 7.99 | None | [U] |
| 9 | Impossible Cloud | 9.19 | 9.19 | None; fair-use egress risk at 14 d | EC, 11 nines |
| 10 | Hetzner OS (reader outside Hetzner) | 9.77 | 7.88 | Low | 1 location |
| 11 | Contabo [3P] | 11.45 | 11.45 | [U] | [U] |
| 12 | OVH 3-AZ Standard (>500 TiB) | 11.49 | 11.49 | None | **3 AZ**, 99.99% SLA: cheapest multi-AZ option |
| 13 | Wasabi | 51.36 | 11.99 | 90-day minimum; fair use broken at 14 d | [U] |
| 14 | Storj | 30.00 | 10.50 | None | globally distributed |
| 15 | R2 Standard / IA | 15.08 / 43.02 | 15.02 / 15.04 | None | 11 nines, multi-DC |
| ... | Scaleway, Linode, DO, Vultr, UpCloud, Exoscale | 20-71 | 11-33 | | |
| ref | AWS / GCS / Azure cool classes | 236-304 | 60-71 | | multi-AZ (AWS SIA/GIR) or 1 zone |

**Bottom line [E].**
- Below roughly **5-10 PB**, buy object storage: iDrive e2 or Backblaze B2 for either retention, OVH 1-AZ
  Standard, or Hetzner if the readers sit in Hetzner eu-central. OVH 1-AZ IA wins at 60 days. OVH 3-AZ
  Standard (>500 TiB tier) at $11.5 is the cheapest multi-AZ option.
- Owning disks costs about $2-3/usable-TB-mo all-in, but only at about 10 PB (11 chassis for 8+3 with
  chassis failure domains). The 2026 drive shortage has roughly doubled the hardware share, so ~$0.6/TB
  at 2025 drive prices becomes ~$1.2-1.4/TB now. The 14-day read-back pattern makes transit and port
  capacity the second-largest cost.
- Everything ranked 3-11 is single-site or single-AZ. If a cold-tier copy must survive a site loss, compare
  OVH 3-AZ ($11.5) or two single-site providers (e.g. B2 + iDrive ≈ $13), not single-site colo.

---

## Sources (fetched 2026-11-19 unless noted)

First-party pages and APIs:
- Hetzner Object Storage: https://www.hetzner.com/storage/object-storage/ ; https://docs.hetzner.com/storage/object-storage/overview/
- Hetzner colocation: https://www.hetzner.com/colocation/
- Backblaze B2: https://www.backblaze.com/cloud-storage/pricing ; regions: https://www.backblaze.com/docs/cloud-storage-data-regions
- Backblaze Drive Stats (Q2 2026 snapshot): https://www.backblaze.com/blog/tag/hard-drive-stats/
- Wasabi: https://wasabi.com/pricing ; https://wasabi.com/pricing/faq ; https://docs.wasabi.com/docs/how-does-wasabis-minimum-storage-duration-policy-work
- iDrive e2: https://www.idrive.com/s3-storage-e2/pricing
- Cloudflare R2: https://developers.cloudflare.com/r2/pricing/ ; https://developers.cloudflare.com/r2/reference/durability/
- Storj: https://www.storj.io/pricing
- Scaleway product catalog API: https://api.scaleway.com/product-catalog/v2alpha1/public-catalog/products?product_types=object_storage ; price change: https://www.scaleway.com/en/blog/a-transparent-update-on-scaleway-pricing/
- OVHcloud: https://www.ovhcloud.com/en/public-cloud/prices/ ; https://docs.ovhcloud.com/en/guides/storage-and-backup/object-storage/s3-choosing-the-right-storage-class-for-your-needs
- Impossible Cloud: https://www.impossiblecloud.com/pricing
- Akamai/Linode: https://www.akamai.com/cloud/pricing/object-storage
- DigitalOcean: https://www.digitalocean.com/pricing/spaces-object-storage ; https://docs.digitalocean.com/products/spaces/details/pricing/
- Vultr: https://docs.vultr.com/support/platform/billing/how-is-object-storage-billed
- UpCloud (egress policy only): https://upcloud.com/docs/products/managed-object-storage/billing/
- Exoscale pricing API: https://portal.exoscale.com/api/pricing/sos ; https://portal.exoscale.com/api/pricing/opencompute
- IONOS model: https://docs.ionos.com/cloud/storage-and-backup/ionos-object-storage/overview/pricing
- Filebase: https://filebase.com/pricing/
- AWS Price List API: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json ; https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-east-1/index.json
- GCS: https://cloud.google.com/storage/pricing
- Azure Retail Prices API: https://prices.azure.com/api/retail/prices (Storage, General Block Blob v2, eastus; Bandwidth, eastus)
- serverpartdeals: https://serverpartdeals.com/collections/hard-drives
- Voxility colo quotes (PDF, dated 2026-11-19): https://www.voxility.com/colocation/prices/Full+Rack+Colocation+in+AtlasEdge+DC+Amsterdam+1?export=PDF (renders as Templus AMS01) ; https://www.voxility.com/colocation/prices/Full+Rack+Colocation+in+Digital+Realty+(Interxion)+FRA8+Frankfurt?export=PDF
- xaweho colocation: https://xaweho.de/en/colocation
- Hurricane Electric: https://he.net/colocation.html

Third-party [3P]:
- Contabo price aggregators: https://getdeploying.com/reference/object-storage ; https://contabo.com/blog/object-storage-feature-and-price-comparison/ (blocked to us; quoted via search)
- UpCloud $20/TB: https://getdeploying.com/hexabyte-vs-upcloud
- Synology C2 2026: https://www.blackvoid.club/synology-c2-cloud-service-changes-for-2026/
- Tebi: https://www.trustradius.com/products/tebi.io/pricing
- IONOS rates: https://sliplane.io/blog/ionos-object-storage-alternatives
- HDD market 2026: https://pcserverandparts.com/news/hard-drive-shortage-2026-what-to-do/
- Backblaze Q1 2026 (20 TB+ AFR): https://www.helpnetsecurity.com/2026/07/17/hard-drive-reliability-2026-4tb-20tb/
- Exos X24 power: https://www.storagereview.com/news/seagate-exos-x24-24tb-hdd-now-shipping and retailer spec pages
- Chassis prices: https://www.newegg.com/supermicro-gold-series-ssg-640sp-e1cr60-g1-rack/p/N82E16859152411 ; https://anafrashop.com ; https://www.senetic.co.uk/product/SSG-640SP-DE2CR60 ; https://smicro.eu/supermicro-ssg-640sp-de2cr60 ; eBay DS460C/SC847 listings (e.g. https://www.ebay.com/itm/155638106932)
- CBRE Global Data Center Trends 2026: https://www.cbre.com/insights/reports/global-data-center-trends-2026
- TeleGeography transit 2026: https://blog.telegeography.com/ip-transit-price-erosion-significant-regional-differences-remain ; HolyCloud: https://holycloud.fr/en/transit/ip
- Joe's Datacenter: https://joesdatacenter.com/products/colocation ; MIRhosting: https://www.whtop.com/plans/mirhosting.com/152188

Calculation script (reproduces the tables in 1.2 and 2.2, except the DigitalOcean Cold row, which was recomputed by hand for its "free retrieval up to 1× stored" rule; the script charges retrieval on every read): `bench/s2/cold_objects.py`
