#!/usr/bin/env python3
"""Unit economics of an S2-compatible service on rented bare metal (Precog) against s2.dev's
list prices. Prints Markdown: the inputs, per-scenario costs and prices, then a Monte Carlo over
the inputs nobody knows well. Every price is in reports/S2 bare-metal economics.md with its
source; capacity per server comes from this repository's measurements (results/).

    python3 bench/s2/economics.py [--seed 1] [--draws 20000]
"""
import argparse, math, random, statistics

EUR = 1.08  # USD per EUR (assumption; swept in the Monte Carlo)
GIB, TIB = 2**30, 2**40
SEC_PER_MONTH = 730 * 3600

# s2.dev list prices, USD (s2.dev/pricing, 2026-11-19)
S2 = dict(store_gib=0.05, write_std_gib=0.05, write_exp_gib=0.075, read_private_gib=0.05,
          read_internet_gib=0.10, stream_ops_k=0.001, streams_k_month=0.01)

# Hetzner dedicated, EUR/month ex VAT, after the 2026-06-15 price change (hetzner.com)
SERVERS = {
    # name: (eur_month, setup_eur, physical_cores, nvme_tb, hdd_tb, note)
    "AX102": (259, 129, 16, 3.84, 0, "Ryzen 9 7950X3D, 128 GB, 2x1.92 TB NVMe DC"),
    # top of each model's range; the pages do not tie the price to the disks: assumed
    "EX63 (4x3.84 TB, assumed)": (149, 74, 20, 15.36, 0, "Core Ultra 7 265, 8P+12E cores, up to 192 GB"),
    "EX131 (4x7.68 TB, assumed)": (559, 279, 32, 30.72, 0, "Xeon Gold 6731P, up to 256 GB"),
    "AX162-1": (614, 304, 48, 3.84, 0, "EPYC 9454P, 128 GB, 2x1.92 TB NVMe DC"),
    # AWS reference (us-east-1 on-demand, $759/month, priced here in EUR): 8 vCPU, 2x2.5 TB NVMe
    "AWS i7ie.2xlarge": (759 / 1.08, 0, 4, 5.0, 0, "8 vCPU = 4 cores, 64 GiB, 2x2.5 TB NVMe"),
    "SX295": (879, 439, 32, 15.36, 308, "EPYC 7502P, 256 GB, 14x22 TB HDD + 2x7.68 TB NVMe"),
}
UPLINK_10G = 43           # EUR/month: 20 TB egress included, then EUR 1/TB (internal vSwitch free)
EGRESS_EUR_TB = 1.0
HZ_OBJECT_EUR_TB = 6.35   # Hetzner Object Storage beyond the base (single site, provider-replicated)
# Cold tier for sealed segments: readable in seconds, no NVMe, no archive class
# (reports/S2 cold storage.md, prices seen 2026-11-19). Servers: (EUR/month, setup EUR, raw TB,
# uplink EUR/month, data shards k, parity shards m, sellable fill). k+m servers in distinct failure
# domains; 6+3 needs 9, which Hetzner alone (FSN1, NBG1, HEL1) cannot give: mix providers.
COLD_SERVERS = {
    "SX295, EC 3+2, 10G": (879, 439, 308, 43, 3, 2, 0.8),
    "SX295, 3 replicas, 10G": (879, 439, 308, 43, 1, 2, 0.8),
    "SX65 4x16 TB, EC 3+2, 1G": (84, 39, 64, 0, 3, 2, 0.8),
    "SX65 4x16 TB, EC 6+3, 1G (Hetzner + OVH)": (84, 39, 64, 0, 6, 3, 0.8),
    "SX65 4x16 TB, 3 replicas, 1G": (84, 39, 64, 0, 1, 2, 0.8),
    "OVH RISE-STOR 8x14 TB, EC 3+2 (in stock, vRack)": (2.14 * 112, 2.14 * 112, 112, 0, 3, 2, 0.8),
    "Storage Box BX41 20 TB, EC 3+2 over 5 boxes (fragment files over SFTP)": (40.60, 0, 20, 0, 3, 2, 0.95),
}
# Object stores, USD per usable TB-month for our pattern (128 MB objects, 14-day retention, each
# read back once into Hetzner: storage + minimum duration + egress + requests).
COLD_OBJECT_USD_TB = {
    "Hetzner Object Storage (reads inside Hetzner)": HZ_OBJECT_EUR_TB * EUR,
    "iDrive e2": 6.00,
    "Backblaze B2": 6.95,
    "OVH Object Storage 1-AZ Standard": 7.55,
    "Owned 60-bay chassis in colo, EC 8+3, fully loaded (needs ~10 PB)": 2.8,
}
COLD_DEFAULT = "SX65 4x16 TB, EC 3+2, 1G"
AWS_XAZ_USD_GB = 0.02     # inter-AZ, $0.01/GB billed on each side

EUR_PER_EXTRA_NVME_TB = 170 / 7.68  # 7.68 TB NVMe DC add-on, EUR 170/month

# Capacity, measured here (results/compare.md, results/s2.md) on 2.1 GHz Xeon vCPUs
CPU_US_REC_NATIVE = 5.17  # 3 nodes, 3 copies, acked at 2, 256 B uncompressed (results/s2.md: precog3 6.92 - gw-none 1.75)
CPU_US_REC_GATEWAY = 1.75  # S2 REST/JSON gateway per record at 256 B, 100 per append (results/s2.md: gw-none)
CPU_US_PER_KIB = 1.0      # model: copies, CRCs and JSON per KiB beyond the measured size
LINK_GBPS = 10            # with the 10G uplink add-on
DISK_FILL = 0.8           # usable share of disks
RAM_NODE_BYTES_PER_STREAM = 3_960  # per node holding the stream (results/s2-streams.jsonl, 1M streams)
RAM_GW_BYTES_PER_STREAM = 8_800    # the owning gateway's streamer (500k streams, node subtracted)
RAM_EUR_GB_MONTH = 259 / 128  # AX102: EUR 259 for 128 GB, all of it charged to RAM (an upper bound)
# A proposed list: about half of s2.dev on bytes, streams priced at today's resident-RAM cost
PROPOSED = dict(write=0.02, read=0.01, read_internet=0.02, store=0.025, ops_k=0.0, streams_k=0.05)
AWS_EGRESS_USD_GB = 0.09    # what a customer running in AWS pays to send a GB to Hetzner
STAFF_EUR = (15_000, 45_000)  # on-call SRE coverage and tooling per month: model, see the report


def s2_bill(sc, express=False, internet=False):
    w = sc["write_mib_s"] * 2**20 * SEC_PER_MONTH / GIB          # GiB written per month
    stored = sc["write_mib_s"] * 2**20 * sc["retention_d"] * 86400 / GIB
    reads = w * sc["read_x"]
    ops = (w * GIB / sc["append_bytes"]) * (1 + sc["read_x"])     # appends + read batches of the same size
    return (stored * S2["store_gib"] + w * (S2["write_exp_gib"] if express else S2["write_std_gib"])
            + reads * (S2["read_internet_gib"] if internet else S2["read_private_gib"])
            + ops / 1000 * S2["stream_ops_k"] + sc["streams"] / 1000 * S2["streams_k_month"])


def fleet(w_mib_s, retention_d, read_x, hot_h=6, util=0.5, speed=1.5, cold="ec", record_bytes=1024,
          eur=EUR, hot="AX102", integral=True, internet_egress=False):
    """Monthly EUR of the hot and cold tiers that carry `w_mib_s` of writes (all tenants).

    Hot tier: Precog nodes (owned streams + gateway on the same box), 3 copies, the last `hot_h`
    hours on NVMe, 10G uplinks (replication on the free internal vSwitch). Cold tier: sealed
    segments on SX295 storage servers, erasure coded 3+2 ("ec") or 3 replicas ("rep3"), or on
    Hetzner Object Storage ("obj"). `integral=False` gives fractional servers: the marginal cost
    of a large fleet, without minimum cluster sizes or spares.
    """
    price, setup, cores, nvme_tb, _, _ = SERVERS[hot]
    rec_s = w_mib_s * 2**20 / record_bytes
    us_rec = CPU_US_REC_NATIVE + CPU_US_REC_GATEWAY + CPU_US_PER_KIB * max(0, record_bytes - 256) / 1024
    # a read costs about a third of a write per record (tail cache, no fsync, no replication): model
    cpu_need = rec_s * us_rec * (1 + read_x / 3) / 1e6                  # 2.1 GHz vCPU-seconds per second
    n_cpu = cpu_need / (cores * speed * util)
    w_bps = w_mib_s * 2**20
    link = LINK_GBPS * 1e9 / 8 * util
    n_net = max(3 * w_bps, 2 * w_bps + w_bps * read_x) / link           # busiest direction, whole fleet
    hot_bytes = w_bps * min(retention_d * 24, hot_h) * 3600 * 3
    n_disk = hot_bytes / (nvme_tb * 1e12 * DISK_FILL)
    need = max(n_cpu, n_net, n_disk)
    n_hot = max(3, math.ceil(need)) + 1 if integral else need
    cold_bytes = max(0.0, w_bps * (retention_d * 86400 - hot_h * 3600))
    if cold == "ec":
        cold = COLD_DEFAULT
    cold_eur, n_cold = 0.0, 0
    if cold_bytes > 0:
        if cold in COLD_OBJECT_USD_TB:
            cold_eur = cold_bytes / 1e12 * COLD_OBJECT_USD_TB[cold] / eur
        else:
            c_price, c_setup, c_raw, c_uplink, k, m, fill = COLD_SERVERS[cold]
            raw = cold_bytes * (k + m) / k / (c_raw * 1e12 * fill)
            n_cold = max(k + m, math.ceil(raw)) + 1 if integral else raw
            cold_eur = n_cold * (c_price + c_uplink + c_setup / 36)
    egress_tb = w_bps * read_x * SEC_PER_MONTH / 1e12 if internet_egress else 0
    egress_eur = max(0.0, egress_tb - 20 * n_hot) * EGRESS_EUR_TB
    hot_eur = n_hot * ((0 if hot.startswith("AWS") else UPLINK_10G) + price + setup / 36)                 # setup amortized over 3 years
    uplink = 0 if hot.startswith("AWS") else UPLINK_10G
    box = price + uplink + setup / 36
    return dict(cpu_read=rec_s * us_rec * read_x / 3 / 1e6 / (cores * speed * util), net_read=w_bps * read_x / link,
                box_eur=box, hot_eur=hot_eur, cold_eur=cold_eur, egress_eur=egress_eur, total_usd=(hot_eur + cold_eur + egress_eur) * eur,
                hot=n_hot, cold=n_cold, cpu=n_cpu, net=n_net, disk=n_disk)


def unit_costs(**kw):
    """Marginal USD per GiB written, per GiB read and per GiB-month stored, at fleet scale."""
    w = 1024  # MiB/s: large enough that nothing rounds
    gib_month = w * 2**20 * SEC_PER_MONTH / GIB
    hot_h = kw.pop("hot_h", 6)
    base = fleet(w, hot_h / 24, 0, hot_h=hot_h, integral=False, **kw)
    with_reads = fleet(w, hot_h / 24, 1, hot_h=hot_h, integral=False, **kw)
    stored = fleet(w, 30 + hot_h / 24, 0, hot_h=hot_h, integral=False, **kw)
    stored_gib = w * 2**20 * 30 * 86400 / GIB
    eur = kw.get("eur", EUR)
    write = base["total_usd"] / gib_month
    # reads pay for the servers their own CPU and egress would need, even when a fleet bound by
    # NVMe carries them on spare capacity (it does at the margin: a price must not count on it)
    read = max(with_reads["cpu_read"], with_reads["net_read"]) * with_reads["box_eur"] * eur / gib_month
    store = (stored["cold_eur"] - base["cold_eur"]) * eur / stored_gib
    if kw.get("hot", "").startswith("AWS"):
        write += 2 * AWS_XAZ_USD_GB * GIB / 1e9   # two replica copies cross AZs
    return write, read, store, base


SCENARIOS = [
    # write MiB/s averaged over the month; retention days; reads as a multiple of writes;
    # average append size; hours kept on NVMe; streams
    dict(name="small: app events", write_mib_s=1, retention_d=7, read_x=2, append_bytes=16 << 10, streams=10_000),
    dict(name="telemetry", write_mib_s=10, retention_d=7, read_x=2, append_bytes=64 << 10, streams=100_000),
    dict(name="market-data ingest", write_mib_s=50, retention_d=3, read_x=4, append_bytes=256 << 10, streams=2_000),
    dict(name="agent sessions (many streams)", write_mib_s=2, retention_d=30, read_x=1, append_bytes=4 << 10, streams=1_000_000),
    dict(name="archive-heavy", write_mib_s=20, retention_d=90, read_x=0.5, append_bytes=256 << 10, streams=10_000),
]




def money(x):
    return f"${x:,.0f}" if abs(x) >= 100 else f"${x:,.2f}"


def bill(sc, write, read, store, ops_k, streams_k):
    """A month of `sc` at per-unit prices (USD per GiB written, read, stored-month; per 1000 ops and streams)."""
    w = sc["write_mib_s"] * 2**20 * SEC_PER_MONTH / GIB
    stored = sc["write_mib_s"] * 2**20 * sc["retention_d"] * 86400 / GIB
    ops = (w * GIB / sc["append_bytes"]) * (1 + sc["read_x"])
    return w * write + w * sc["read_x"] * read + stored * store + ops / 1000 * ops_k + sc["streams"] / 1000 * streams_k


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--draws", type=int, default=20000)
    a = ap.parse_args()
    gm = 0.75
    stream_cost_k = (3 * RAM_NODE_BYTES_PER_STREAM + RAM_GW_BYTES_PER_STREAM) * 1000 / 1e9 * RAM_EUR_GB_MONTH * EUR  # USD per 1000 streams-month, all resident

    print("### 1. Marginal unit costs at fleet scale (USD), against s2.dev's list prices\n")
    print("Base: 50% target utilization, cores 1.5x the test VM's, 1 KiB records, 6 h on NVMe, EUR = "
          f"{EUR} USD. Fractional servers: no minimum cluster, no spares (those are in section 3).\n")
    print("| configuration | per GiB written | per GiB read | per GiB-month stored | bound by |")
    print("|---|---:|---:|---:|---|")
    for label, kw in [("AX102 hot, SX65 EC 3+2 cold", {}),
                      ("AX162-1 hot", dict(hot="AX162-1")),
                      ("EX63 4x3.84 TB hot (assumed config)", dict(hot="EX63 (4x3.84 TB, assumed)")),
                      ("EX131 4x7.68 TB hot (assumed config)", dict(hot="EX131 (4x7.68 TB, assumed)")),
                      ("AX102, 1 h on NVMe", dict(hot_h=1)),
                      ("AWS i7ie.2xlarge hot + inter-AZ copies (reference; cold column is Hetzner SX65)", dict(hot="AWS i7ie.2xlarge")),
                      ("AX102, 256 B records", dict(record_bytes=256)),
                      ("AX102, 30% utilization, 1.0x cores", dict(util=0.3, speed=1.0))]:
        w, r, st, b = unit_costs(**kw)
        bound = max(("CPU", b["cpu"]), ("network", b["net"]), ("NVMe", b["disk"]), key=lambda x: x[1])[0]
        print(f"| {label} | ${w:.4f} | ${r:.4f} | ${st:.4f} | {bound} |")
    print(f"| **s2.dev list** | ${S2['write_std_gib']:.3f} (express ${S2['write_exp_gib']:.3f}) | "
          f"${S2['read_private_gib']:.3f} (internet ${S2['read_internet_gib']:.2f}) | ${S2['store_gib']:.3f} | |")

    print("\n### 1b. Cold tier for sealed segments (readable in seconds; no NVMe, no archive class)\n")
    print("USD per GiB-month and per TB-month of data stored, and the smallest cold tier each needs "
          "(k+m servers in distinct failure domains plus one spare; object stores have no minimum).\n")
    print("| cold tier | per GiB-month | per TB-month | smallest cold tier per month | failure domains needed |")
    print("|---|---:|---:|---:|---|")
    for name in list(COLD_SERVERS) + list(COLD_OBJECT_USD_TB):
        _, _, st_c, _ = unit_costs(cold=name)
        if name in COLD_SERVERS:
            c_price, c_setup, _, c_uplink, k, m, _ = COLD_SERVERS[name]
            floor = (k + m + 1) * (c_price + c_uplink + c_setup / 36) * EUR
            dom = f"{k + m} ({'replicas' if k == 1 else f'EC {k}+{m}'})"
        elif "colo" in name:
            floor, dom = None, "11 chassis at one site; ~10 PB minimum"
        else:
            floor, dom = None, "the provider's (one site or zone)"
        f = "none" if floor is None else money(floor)
        print(f"| {name} | ${st_c:.4f} | ${st_c * 1e12 / GIB:.2f} | {f} | {dom} |")
    print(f"| **s2.dev list** | ${S2['store_gib']:.4f} | ${S2['store_gib'] * 1e12 / GIB:.2f} | | multi-AZ object storage |")

    w, r, st, _ = unit_costs()
    print(f"\nPer 1000 streams-month: RAM for 3 copies of the per-stream state = {money(stream_cost_k)} "
          f"(3 node copies x {RAM_NODE_BYTES_PER_STREAM:,} B + {RAM_GW_BYTES_PER_STREAM:,} B in the owner, every stream resident, "
          f"all of the AX102's price charged to RAM); s2.dev charges ${S2['streams_k_month']:.2f}. "
          "Stream operations: CPU is already in the per-GiB costs; s2.dev charges $0.001 per 1000.\n")

    print("### 2. A price list at 75% gross margin on marginal cost, and at 30% under s2.dev\n")
    print("| line | our marginal cost | our price at 75% GM | s2.dev list | ours vs s2.dev | our GM at s2.dev -30% |")
    print("|---|---:|---:|---:|---:|---:|")
    lines = [("GiB written", w, S2["write_std_gib"]), ("GiB read (private)", r, S2["read_private_gib"]),
             ("GiB read (internet)", r + EGRESS_EUR_TB * EUR * GIB / 1e12, S2["read_internet_gib"]),
             ("GiB-month stored", st, S2["store_gib"]), ("1000 streams-month", stream_cost_k, S2["streams_k_month"])]
    for name, cost, s2p in lines:
        price = cost / (1 - gm)
        print(f"| {name} | ${cost:.4f} | ${price:.4f} | ${s2p:.4f} | {price / s2p - 1:+.0%} | {1 - cost / (0.7 * s2p):.0%} |")

    print("\n### 2b. A proposed list price (about half of s2.dev on bytes) and its margin per line\n")
    print("| line | proposed | s2.dev | our marginal cost | GM |")
    print("|---|---:|---:|---:|---:|")
    for (name, cost, s2p), key in zip(lines, ["write", "read", "read_internet", "store", "streams_k"]):
        print(f"| {name} | ${PROPOSED[key]:.4f} | ${s2p:.4f} | ${cost:.4f} | {1 - cost / PROPOSED[key]:.0%} |")
    off_heap = (3 * 2_000) * 1000 / 1e9 * RAM_EUR_GB_MONTH * EUR
    print(f"| 1000 streams-month, dormant streams off-heap (~2 KB resident per copy, as s2-lite after a restart) | "
          f"${PROPOSED['streams_k']:.4f} | ${S2['streams_k_month']:.4f} | ${off_heap:.4f} | {1 - off_heap / PROPOSED['streams_k']:.0%} |")

    print("\n### 3. The fixed floor: the smallest production fleet, and staff\n")
    small = fleet(1, 7, 2, integral=True)
    floor_usd = small["total_usd"]
    print(f"Smallest fleet (4 AX102 hot incl. one spare, 10G uplinks; 6 SX65 for EC 3+2 incl. one spare): "
          f"{money(floor_usd)}/month. With Hetzner Object Storage for the cold tier instead: "
          f"{money(fleet(1, 7, 2, integral=True, cold='Hetzner Object Storage (reads inside Hetzner)')['total_usd'])}/month.\n")
    print("| fixed cost per month (fleet floor + staff) | revenue to break even at 75% contribution | at s2.dev -30% (blended contribution) |")
    print("|---|---:|---:|")
    mix_cost = sum(bill(sc, w, r, st, 0, stream_cost_k) for sc in SCENARIOS)
    mix_s2 = sum(bill(sc, S2["write_std_gib"], S2["read_private_gib"], S2["store_gib"], S2["stream_ops_k"], S2["streams_k_month"]) for sc in SCENARIOS)
    contrib_30 = 1 - mix_cost / (0.7 * mix_s2)
    for staff in (0, 15_000, 30_000, 45_000):
        fixed = floor_usd + staff * EUR
        print(f"| {money(fixed)} (staff EUR {staff:,}) | {money(fixed / gm)} | {money(fixed / contrib_30)} |")

    print("\n### 4. Customer bills: s2.dev list, our list at 75% GM, the proposed list\n")
    print("| customer | s2.dev standard | our list (75% GM) | proposed list | proposed GM | our marginal cost | "
          "customer's AWS egress to reach us (if they run in AWS) |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for sc in SCENARIOS:
        b = bill(sc, S2["write_std_gib"], S2["read_private_gib"], S2["store_gib"], S2["stream_ops_k"], S2["streams_k_month"])
        c = bill(sc, w, r, st, 0, stream_cost_k)
        ours = c / (1 - gm)
        prop = bill(sc, PROPOSED["write"], PROPOSED["read"], PROPOSED["store"], PROPOSED["ops_k"], PROPOSED["streams_k"])
        egress = sc["write_mib_s"] * 2**20 * SEC_PER_MONTH / 1e9 * AWS_EGRESS_USD_GB
        print(f"| {sc['name']}: {sc['write_mib_s']} MiB/s, {sc['retention_d']} d, reads {sc['read_x']}x, "
              f"{sc['streams']:,} streams | {money(b)} | {money(ours)} | {money(prop)} | {1 - c / prop:.0%} | {money(c)} | {money(egress)} |")

    rng = random.Random(a.seed)
    print(f"\n### 5. Monte Carlo ({a.draws:,} draws): net margin of a business priced at s2.dev -30%\n")
    print("Each draw: utilization U(0.3, 0.65); core speed U(1.0, 2.2); record size log-uniform 256 B-8 KiB; "
          "EUR U(1.0, 1.2) USD; cold tier one of SX65 EC 3+2, SX65 3 replicas, Hetzner Object Storage, iDrive e2, Backblaze B2 (1/5 each); resident RAM per stream "
          "log-uniform 2-25 KB (2 KB: dormant streams off-heap, as s2-lite after a restart); staff EUR 15k-45k/month; customers drawn from the mix above. Net margin = "
          "(revenue - marginal cost - fleet floor - staff) / revenue.\n")
    print("| monthly revenue at s2.dev -30% | net margin p10 | p50 | p90 | P(net margin > 0) | P(net > 50%) |")
    print("|---|---:|---:|---:|---:|---:|")
    for revenue in (25_000, 100_000, 250_000, 1_000_000):
        nets = []
        for _ in range(a.draws):
            kw = dict(util=rng.uniform(0.3, 0.65), speed=rng.uniform(1.0, 2.2),
                      record_bytes=int(math.exp(rng.uniform(math.log(256), math.log(8192)))),
                      eur=rng.uniform(1.0, 1.2), cold=rng.choice(["SX65 4x16 TB, EC 3+2, 1G", "SX65 4x16 TB, 3 replicas, 1G",
                                                      "Hetzner Object Storage (reads inside Hetzner)", "iDrive e2", "Backblaze B2"]))
            uw, ur, us, _ = unit_costs(**kw)
            ram = math.exp(rng.uniform(math.log(2_000), math.log(25_000)))
            sk = ram * 1000 / 1e9 * RAM_EUR_GB_MONTH * kw["eur"]
            cost = sum(bill(sc, uw, ur, us, 0, sk) for sc in SCENARIOS)
            s2b = sum(bill(sc, S2["write_std_gib"], S2["read_private_gib"], S2["store_gib"], S2["stream_ops_k"], S2["streams_k_month"]) for sc in SCENARIOS)
            variable = cost / (0.7 * s2b)                     # marginal cost share of revenue
            fixed = (fleet(1, 7, 2, integral=True, **kw)["total_usd"] + rng.uniform(*STAFF_EUR) * kw["eur"]) / revenue
            nets.append(1 - variable - fixed)
        nets.sort()
        q = lambda p: nets[int(p * (len(nets) - 1))]
        print(f"| {money(revenue)} | {q(.1):.0%} | {q(.5):.0%} | {q(.9):.0%} | "
              f"{sum(n > 0 for n in nets) / len(nets):.0%} | {sum(n > 0.5 for n in nets) / len(nets):.0%} |")


if __name__ == "__main__":
    main()
