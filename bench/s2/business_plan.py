#!/usr/bin/env python3
"""Three-year plan for Precog as an agent-telemetry stream service (docs: the business plan).

Every number here is an assumption, named once: customers won per quarter per segment, their
monthly spend at the proposed price list (bench/s2/economics.py PROPOSED), churn, infrastructure
cost as a share of revenue plus the fleet floor, and headcount. Prints quarterly and yearly
tables as Markdown.

    python3 bench/s2/business_plan.py
"""

EUR = 1.08
# segment: (monthly spend per customer in USD, new customers per quarter for Q1..Q12, quarterly churn)
SEGMENTS = {
    "Agent startups (self-serve)": (300, [0, 15, 30, 45, 60, 75, 90, 110, 130, 150, 170, 190], 0.08),
    "Agent platforms and coding agents": (4_000, [0, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6], 0.03),
    "EU regulated enterprises": (3_500, [0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5], 0.02),
    "Observability and agent-tool vendors (OEM)": (15_000, [0, 0, 0, 1, 0, 1, 1, 1, 1, 1, 2, 2], 0.02),
}
EXPANSION = 0.06            # quarterly growth of a retained customer's usage
GROSS_COST_SHARE = 0.12     # marginal infrastructure as a share of revenue (model: 2-14%; 12% with support)
FLEET_FLOOR_USD = 1_900     # smallest production fleet per month (economics.py, SX65 cold tier)
FLOOR_STEP = 12_000         # one more floor-sized fleet (second region, staging) from year 2, USD/month
# headcount by quarter and EUR loaded cost per person-year
HEADCOUNT = [4, 5, 6, 6, 8, 9, 10, 11, 13, 15, 17, 19]
EUR_PER_HEAD = 115_000
OTHER_OPEX_SHARE = 0.25     # tools, travel, marketing, legal, SOC 2 as a share of payroll


def run(scale=1.0):
    """Quarterly rows. `scale` multiplies customers won (0.5 conservative, 1.5 upside)."""
    cohorts = []  # (segment, quarter won, customers won)
    rows = []
    for q in range(12):
        for seg, (_, new, _) in SEGMENTS.items():
            if new[q]:
                cohorts.append((seg, q, new[q] * scale))
        rev, by = 0.0, {seg: 0.0 for seg in SEGMENTS}
        for seg, q0, n in cohorts:
            spend, _, churn = SEGMENTS[seg]
            age = q - q0
            alive = n * (1 - churn) ** age
            by[seg] += alive
            rev += alive * spend * (1 + EXPANSION) ** age * 3
        infra = rev * GROSS_COST_SHARE + (FLEET_FLOOR_USD + (FLOOR_STEP if q >= 4 else 0)) * 3
        payroll = HEADCOUNT[q] * EUR_PER_HEAD * EUR / 4
        opex = payroll * (1 + OTHER_OPEX_SHARE)
        rows.append(dict(q=q + 1, customers=sum(by.values()), mrr=rev / 3, revenue=rev,
                         infra=infra, opex=opex, net=rev - infra - opex, head=HEADCOUNT[q], by=by))
    return rows


def years(rows):
    out, cum, low = [], 0.0, 0.0
    for y in range(3):
        ys = rows[4 * y:4 * y + 4]
        rev, infra, net = (sum(r[k] for r in ys) for k in ("revenue", "infra", "net"))
        for r in ys:
            cum += r["net"]
            low = min(low, cum)
        out.append(dict(rev=rev, gm=1 - infra / rev if rev else 0, net=net, arr=ys[-1]["mrr"] * 12, cash=-low))
    return out


def main():
    rows = run()
    print("| quarter | customers | MRR at quarter end | revenue | infrastructure | team and other | net | headcount |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        print(f"| Y{(r['q'] - 1) // 4 + 1} Q{(r['q'] - 1) % 4 + 1} | {r['customers']:,.0f} | ${r['mrr']:,.0f} | "
              f"${r['revenue']:,.0f} | ${r['infra']:,.0f} | ${r['opex']:,.0f} | ${r['net']:,.0f} | {r['head']} |")
    print("\n| year | revenue | gross margin | net | ARR at year end | cumulative cash need |")
    print("|---|---:|---:|---:|---:|---:|")
    for y, v in enumerate(years(rows)):
        print(f"| Y{y + 1} | ${v['rev']:,.0f} | {v['gm']:.0%} | ${v['net']:,.0f} | ${v['arr']:,.0f} | ${v['cash']:,.0f} |")
    print("\n| scenario (customers won x) | Y1 revenue | Y2 revenue | Y3 revenue | ARR end Y3 | first profitable quarter | cash need |")
    print("|---|---:|---:|---:|---:|---|---:|")
    for name, k in [("conservative (0.5x)", 0.5), ("base (1x)", 1.0), ("upside (1.5x)", 1.5)]:
        rs = run(k)
        ys = years(rs)
        first = next((f"Y{(r['q'] - 1) // 4 + 1} Q{(r['q'] - 1) % 4 + 1}" for r in rs if r["net"] > 0), "after Y3")
        print(f"| {name} | ${ys[0]['rev']:,.0f} | ${ys[1]['rev']:,.0f} | ${ys[2]['rev']:,.0f} | ${ys[2]['arr']:,.0f} | {first} | ${max(y['cash'] for y in ys):,.0f} |")
    print("\nCustomers at the end of Y3 by segment: " + ", ".join(f"{s}: {n:,.0f}" for s, n in rows[-1]["by"].items()))


if __name__ == "__main__":
    main()
