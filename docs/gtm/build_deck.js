// Builds docs/gtm/precog-agent-telemetry-gtm.pptx: the go-to-market deck for Precog as a durable
// log for agent sessions. Numbers come from the business plan doc, results/s2.md,
// reports/S2 bare-metal economics.md and bench/s2/business_plan.py.
//
//   NODE_PATH=<dir with pptxgenjs, react-icons, react, react-dom, sharp> node docs/gtm/build_deck.js
const path = require("path");
const pptxgen = require("pptxgenjs");
const React = require("react");
const ReactDOMServer = require("react-dom/server");
const sharp = require("sharp");
const fa = require("react-icons/fa");
const { applyTheme } = require(process.env.APPLY_THEME);

const THEME = {
  name: "Precog Signal",
  headFontFace: "Cambria",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "0F172A", lt1: "FFFFFF", dk2: "334155", lt2: "EEF2F6",
    accent1: "0D9488", accent2: "F59E0B", accent3: "64748B", accent4: "CBD5E1",
    accent5: "115E59", accent6: "B45309", hlink: "0D9488", folHlink: "115E59",
  },
};
const HEX = THEME.colors;
const OUT = path.join(__dirname, "precog-agent-telemetry-gtm.pptx");

async function icon(Comp, hex) {
  const svg = ReactDOMServer.renderToStaticMarkup(React.createElement(Comp, { color: "#" + hex, size: 256 }));
  const buf = await sharp(Buffer.from(svg)).resize(256, 256).png().toBuffer();
  return "image/png;base64," + buf.toString("base64");
}

async function main() {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_WIDE"; // 13.33 x 7.5 in
  pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
  pres.title = "Precog for agent telemetry: go-to-market";
  pres.author = "Precog";
  const C = pres.SchemeColor;

  pres.defineSlideMaster({
    title: "Title dark",
    background: { color: HEX.dk1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.8, y: 2.3, w: 11.7, h: 1.6, fontSize: 44, bold: true, color: C.background1, valign: "bottom", align: "left" }, text: "" } },
      { placeholder: { options: { name: "body", type: "body", x: 0.8, y: 4.05, w: 11.7, h: 1.2, fontSize: 20, color: C.accent4, valign: "top" }, text: "" } },
    ],
  });
  pres.defineSlideMaster({
    title: "Content",
    background: { color: HEX.lt1 },
    margin: [0.5, 0.6, 0.6, 0.6],
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.6, y: 0.4, w: 12.1, h: 1.0, fontSize: 28, bold: true, color: C.text1, valign: "top", align: "left" }, text: "" } },
      { text: { text: "Precog · agent telemetry go-to-market · Nov 2026", options: { x: 0.6, y: 7.0, w: 8, h: 0.3, fontSize: 10, color: C.accent3 } } },
    ],
    slideNumber: { x: 12.2, y: 7.0, w: 0.5, h: 0.3, fontSize: 10, color: C.accent3 },
  });

  const ico = {
    lost: await icon(fa.FaUnlink, HEX.accent1), money: await icon(fa.FaFileInvoiceDollar, HEX.accent1),
    shape: await icon(fa.FaStream, HEX.accent1), law: await icon(fa.FaBalanceScale, HEX.accent1),
    agent: await icon(fa.FaRobot, HEX.lt1), exit: await icon(fa.FaDoorOpen, HEX.lt1), eu: await icon(fa.FaEuroSign, HEX.lt1),
    code: await icon(fa.FaCode, HEX.lt1), layers: await icon(fa.FaLayerGroup, HEX.lt1), voice: await icon(fa.FaHeadset, HEX.lt1),
  };
  const footnote = (s, text) => s.addText(text, { x: 0.6, y: 6.55, w: 12.1, h: 0.35, fontSize: 10, color: C.accent3, isTextBox: true, margin: 0 });
  const card = (s, x, y, w, h, name, fill = HEX.lt2) =>
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.12, fill: { color: fill }, line: { color: fill }, objectName: name });
  const circleIcon = (s, x, y, d, data, name, fill = C.accent1) => {
    s.addShape(pres.shapes.OVAL, { x, y, w: d, h: d, fill: { color: fill }, line: { color: fill }, objectName: name + " circle" });
    s.addImage({ data, x: x + d * 0.25, y: y + d * 0.25, w: d * 0.5, h: d * 0.5, objectName: name + " icon", altText: name });
  };

  // 1. Title
  pres.addSection({ title: "Opening" });
  let s = pres.addSlide({ masterName: "Title dark", sectionTitle: "Opening" });
  s.addText("The durable log for agent sessions", { placeholder: "title" });
  s.addText("Precog for agent telemetry and logs: go-to-market plan, November 2026", { placeholder: "body" });
  s.addText("2 ms durable appends · S2-compatible API · hosted in the EU · half S2's price",
    { x: 0.8, y: 6.3, w: 11.7, h: 0.4, fontSize: 16, color: C.accent2, isTextBox: true, margin: 0 });
  s.addNotes("One stream per agent session: every LLM call, tool call, token and state change, appended durably and readable live or later.");

  // 2. Summary
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Opening" });
  s.addText("We can sell agent session logs at half S2's price and keep 86–98% margin", { placeholder: "title" });
  [["2 ms", "durable append, p50,\non 2 of 3 servers"], ["½", "S2's price per GiB\nwritten and stored"], ["86–98%", "gross margin on\nwrites and reads"], ["$4.4M", "ARR at end of year 2,\nbase case"]]
    .forEach(([big, small], i) => {
      const x = 0.6 + i * 3.1;
      card(s, x, 1.7, 2.8, 2.6, "stat " + i);
      s.addText(big, { x: x + 0.25, y: 1.95, w: 2.3, h: 1.1, fontSize: 44, bold: true, color: C.accent1, fontFace: THEME.headFontFace, isTextBox: true, margin: 0 });
      s.addText(small, { x: x + 0.25, y: 3.15, w: 2.3, h: 0.9, fontSize: 15, color: C.text2, isTextBox: true, margin: 0 });
    });
  s.addText([
    { text: "Who: ", options: { bold: true } }, { text: "AI app builders, agent platforms, EU voice and support agent vendors.", options: { breakLine: true } },
    { text: "Gate: ", options: { bold: true } }, { text: "idle streams must leave RAM before public launch.", options: { breakLine: true } },
    { text: "Ask: ", options: { bold: true } }, { text: "about $2M seed; 6 people in year 1." },
  ], { x: 0.6, y: 4.7, w: 12.1, h: 1.5, fontSize: 18, color: C.text1, paraSpaceAfter: 6, isTextBox: true, margin: 0 });
  footnote(s, "Latency measured on a 4-vCPU test VM (results/s2.md); margins modelled from Hetzner list prices (bench/s2/economics.py); ARR from bench/s2/business_plan.py.");

  // 3. Problem
  pres.addSection({ title: "Problem and product" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Problem and product" });
  s.addText("Agent sessions are ordered event streams, stored in tools priced for something else", { placeholder: "title" });
  [[ico.lost, "Streams get lost", "Tokens buffered in Redis with a TTL vanish on reload, a second device or a deploy."],
   [ico.money, "Observability pricing does not fit", "1M sessions of 4 MB kept 6 months: ~$3.5k on Langfuse, $12k on Braintrust, $17.5k+ on Datadog."],
   [ico.shape, "Logs are the wrong shape", "Sessions need exact order, resume from any point, replay and live fan-out — not search."],
   [ico.law, "Retention becomes a duty", "EU AI Act: high-risk systems log events and keep them 6+ months, from 2 Dec 2027."]]
    .forEach(([img, head, body], i) => {
      const y = 1.65 + i * 1.18;
      circleIcon(s, 0.6, y, 0.8, img, head, C.background2);
      s.addText(head, { x: 1.65, y: y - 0.02, w: 10.8, h: 0.4, fontSize: 20, bold: true, color: C.text1, isTextBox: true, margin: 0 });
      s.addText(body, { x: 1.65, y: y + 0.4, w: 10.8, h: 0.5, fontSize: 16, color: C.text2, isTextBox: true, margin: 0 });
    });
  footnote(s, "Session sizes: ~10 KB per simple trace, ~4 MB per 47-turn agent run, 1–60 MB per coding-agent session (dev.to, third-party). Prices: vendors' list prices, Nov 2026.");

  // 4. Why now
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Problem and product" });
  s.addText("Three things changed in 2026", { placeholder: "title" });
  [[ico.agent, "76%", "of 790T tokens on OpenRouter came from agents in October 2026"],
   [ico.exit, "Electric Cloud", "winds down after Electric joined Databricks (Aug 2026): its durable-stream users must move"],
   [ico.eu, "2 Dec 2027", "EU AI Act record-keeping applies to standalone high-risk systems (Digital Omnibus)"]]
    .forEach(([img, big, body], i) => {
      const x = 0.6 + i * 4.1;
      card(s, x, 1.7, 3.8, 4.4, "why " + i, HEX.dk1);
      circleIcon(s, x + 0.35, 2.0, 0.85, img, big);
      s.addText(big, { x: x + 0.35, y: 3.1, w: 3.1, h: 0.9, fontSize: 32, bold: true, color: C.accent2, fontFace: THEME.headFontFace, isTextBox: true, margin: 0 });
      s.addText(body, { x: x + 0.35, y: 4.05, w: 3.1, h: 1.8, fontSize: 16, color: C.background1, isTextBox: true, margin: 0 });
    });
  footnote(s, "Sources: openrouter.ai/data/october-2026; electric.ax blog 2026-08-11; Gibson Dunn on the AI Act Digital Omnibus.");

  // 5. Product
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Problem and product" });
  s.addText("v1: one durable stream per agent session, on S2's API", { placeholder: "title" });
  ["Owned streams: dense sequence numbers, conditional appends, fencing",
   "S2-compatible REST and SSE; resume from any position",
   "OpenTelemetry ingest mapped onto session streams",
   "Retention from hours to months; old data on HDD or object storage",
   "EU hosting by default; per-tenant tokens and quotas"]
    .forEach((t, i) => {
      const y = 1.65 + i * 0.85;
      s.addShape(pres.shapes.OVAL, { x: 0.6, y, w: 0.55, h: 0.55, fill: { color: C.accent1 }, line: { color: C.accent1 }, objectName: "step " + (i + 1) });
      s.addText(String(i + 1), { x: 0.6, y, w: 0.55, h: 0.55, fontSize: 18, bold: true, color: C.background1, align: "center", valign: "middle", isTextBox: true, margin: 0 });
      s.addText(t, { x: 1.4, y: y - 0.02, w: 6.6, h: 0.6, fontSize: 17, color: C.text1, valign: "middle", isTextBox: true, margin: 0 });
    });
  card(s, 8.5, 1.6, 4.2, 4.6, "left out");
  s.addText([
    { text: "Left out on purpose", options: { bold: true, fontSize: 20, color: C.text1, breakLine: true } },
    { text: "Dashboards, evals, prompt management and analytics: what LangSmith, Langfuse and Braintrust sell.", options: { fontSize: 16, color: C.text2, breakLine: true } },
    { text: " ", options: { fontSize: 8, breakLine: true } },
    { text: "We are the storage and transport underneath them.", options: { fontSize: 16, bold: true, color: C.accent1 } },
  ], { x: 8.8, y: 1.9, w: 3.6, h: 4.0, valign: "top", paraSpaceAfter: 6, isTextBox: true, margin: 0 });
  footnote(s, "Already in the broker: dedup of retried events, compacted state streams, reader groups, cursors valid on every node, leaderless failover in tens of ms.");

  // 6. Proof
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Problem and product" });
  s.addText("Through the same S2 REST client, Precog acks durable writes 3–26× faster", { placeholder: "title" });
  s.addChart(pres.charts.BAR, [{ name: "append p50 (ms)", labels: ["Precog, 2 of 3 servers", "s2-lite, local disk", "s2-lite on S3, 5 ms flush", "s2-lite on S3, default"], values: [1.98, 5.86, 6.24, 51.2] }], {
    x: 0.6, y: 1.55, w: 7.6, h: 4.8, barDir: "bar", chartColors: [HEX.accent1, HEX.accent4, HEX.accent4, HEX.accent4],
    invertedColors: [HEX.accent1, HEX.accent4, HEX.accent4, HEX.accent4], varyColors: true,
    showValue: true, dataLabelPosition: "outEnd", dataLabelFormatCode: "0.0\" ms\"", dataLabelFontSize: 14, dataLabelColor: HEX.dk1, dataLabelFontFace: "+mn-lt",
    catAxisLabelFontSize: 14, catAxisLabelColor: HEX.dk2, catAxisLabelFontFace: "+mn-lt", catAxisOrientation: "maxMin",
    valAxisHidden: true, valGridLine: { style: "none" }, catGridLine: { style: "none" }, showLegend: false,
    showTitle: true, title: "Append latency p50 at 10,000 records/s (ms)", titleFontSize: 14, titleColor: HEX.dk2, titleFontFace: "+mn-lt",
  });
  card(s, 8.6, 1.6, 4.1, 4.6, "throughput");
  s.addText([
    { text: "229k", options: { fontSize: 44, bold: true, color: C.accent1, fontFace: THEME.headFontFace, breakLine: true } },
    { text: "records/s through one gateway, closed loop (s2-lite on S3: 31k–99k)", options: { fontSize: 15, color: C.text2, breakLine: true } },
    { text: " ", options: { fontSize: 8, breakLine: true } },
    { text: "6.9 µs", options: { fontSize: 44, bold: true, color: C.accent1, fontFace: THEME.headFontFace, breakLine: true } },
    { text: "server CPU per record (s2-lite: 10–12 µs)", options: { fontSize: 15, color: C.text2 } },
  ], { x: 8.9, y: 1.85, w: 3.5, h: 4.1, valign: "top", isTextBox: true, margin: 0 });
  footnote(s, "One shared 4-vCPU VM, Silo as the S3 store on loopback, medians of 3–6 runs (results/s2.md). S2's cloud SLOs: 40 ms Express, 400 ms Standard.");

  // 7. Market
  pres.addSection({ title: "Market and customers" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Market and customers" });
  s.addText("A $1–2B observability layer on top, a much larger log market underneath", { placeholder: "title" });
  [["$1–2B", "LLM and agent observability, 2026, growing 25–36% a year (analyst range)"],
   ["$1.2B", "Datadog revenue in Q3 2026 alone, +35% year on year"],
   ["110B+", "observations a month logged by Langfuse — about 0.5 PB at 5 KB each"],
   ["40%", "of enterprise apps with task-specific agents by end of 2026 (Gartner forecast)"]]
    .forEach(([big, body], i) => {
      const x = 0.6 + (i % 2) * 6.1, y = 1.65 + Math.floor(i / 2) * 2.25;
      card(s, x, y, 5.8, 2.0, "market " + i);
      s.addText(big, { x: x + 0.3, y: y + 0.25, w: 2.4, h: 1.0, fontSize: 40, bold: true, color: C.accent1, fontFace: THEME.headFontFace, isTextBox: true, margin: 0 });
      s.addText(body, { x: x + 2.8, y: y + 0.3, w: 2.8, h: 1.5, fontSize: 15, color: C.text2, valign: "top", isTextBox: true, margin: 0 });
    });
  footnote(s, "2026 consolidation: ClickHouse bought Langfuse, Mintlify bought Helicone, ServiceNow bought Traceloop, IBM closed Confluent. Sources in the business plan.");

  // 8. Lead profiles
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Market and customers" });
  s.addText("Lead with three profiles: volume, distribution, and latency plus residency", { placeholder: "title" });
  [[ico.code, "AI app builders", "Vercel AI SDK, Mastra, TanStack", "Answers lost on reload; Redis buffers with a TTL", "Founding engineer, CTO", "First \"my answer disappeared\" bug", "$50–2k / month", "AI SDK and Mastra users; Electric Cloud switchers"],
   [ico.layers, "Agent platforms", "frameworks with a hosted tier", "Need a session log under their product, no Kafka", "Founder, head of engineering", "Hosted tier launch; millions of streams", "$2k–50k / month", "Mastra, LangGraph Platform, CrewAI, Letta, n8n"],
   [ico.voice, "Voice and support", "agent vendors, EU first", "Real-time events, transcripts, audit and retention", "CTO; CISO for retention", "Enterprise deal demands residency or audit", "$5k–100k / month", "Parloa, PolyAI, Vapi, Retell, Sierra, Decagon"]]
    .forEach(([img, name, sub, pain, buyer, trigger, spend, ex], i) => {
      const x = 0.6 + i * 4.1;
      card(s, x, 1.55, 3.8, 4.85, "profile " + name);
      circleIcon(s, x + 0.3, 1.8, 0.7, img, name);
      s.addText(name, { x: x + 1.15, y: 1.8, w: 2.5, h: 0.4, fontSize: 19, bold: true, color: C.text1, isTextBox: true, margin: 0 });
      s.addText(sub, { x: x + 1.15, y: 2.2, w: 2.5, h: 0.35, fontSize: 13, color: C.accent3, isTextBox: true, margin: 0 });
      s.addText([
        { text: "Pain  ", options: { bold: true, color: C.accent1 } }, { text: pain, options: { breakLine: true } },
        { text: "Buyer  ", options: { bold: true, color: C.accent1 } }, { text: buyer, options: { breakLine: true } },
        { text: "Trigger  ", options: { bold: true, color: C.accent1 } }, { text: trigger, options: { breakLine: true } },
        { text: "Spend  ", options: { bold: true, color: C.accent1 } }, { text: spend, options: { breakLine: true } },
        { text: "Prospects  ", options: { bold: true, color: C.accent1 } }, { text: ex },
      ], { x: x + 0.3, y: 2.75, w: 3.25, h: 3.5, fontSize: 14, color: C.text2, valign: "top", paraSpaceAfter: 6, isTextBox: true, margin: 0 });
    });
  footnote(s, "Spend ranges are estimates; named companies are public examples to prospect, not customers.");

  // 9. Later profiles
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Market and customers" });
  s.addText("Three more profiles follow once we have references", { placeholder: "title" });
  const hdr = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: HEX.dk1 } } });
  s.addTable([
    [hdr("Profile"), hdr("Why later"), hdr("Spend / month"), hdr("When"), hdr("Examples")],
    ["Coding-agent companies", "largest sessions (1–60 MB) and accounts, but long sales cycles and build-vs-buy", "$10k–200k", "year 2", "Cursor, Replit, Cognition, Lovable"],
    ["EU regulated enterprises", "AI Act logs from Dec 2027; procurement needs ISO 27001 / SOC 2", "€20k–250k a year", "late year 2", "banks, insurers, HR-tech in DE, FR, NL"],
    ["Observability vendors (OEM)", "need a cheap durable buffer or retention tier, not a query engine", "$10k–100k", "year 1–2, opportunistic", "Laminar, Opik, AgentOps"],
  ], { x: 0.6, y: 1.65, w: 12.1, colW: [2.6, 4.4, 1.7, 1.5, 1.9], fontSize: 14, color: C.text1, border: { type: "solid", pt: 0.75, color: HEX.accent4 }, rowH: 0.75, valign: "middle", fill: { color: HEX.lt1 } });
  s.addText([
    { text: "Not targeted: ", options: { bold: true } },
    { text: "teams that want dashboards and evals, and AWS-heavy customers for whom ~$0.09/GB egress outweighs our price." },
  ], { x: 0.6, y: 5.3, w: 12.1, h: 0.8, fontSize: 16, color: C.text2, isTextBox: true, margin: 0 });

  // 10. Competition
  pres.addSection({ title: "Competition and pricing" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Competition and pricing" });
  s.addText("For the same workload we cost 40% of S2 and a fraction of observability tools", { placeholder: "title" });
  const comp = [["Logfire", 330], ["Precog", 710], ["S2", 1740], ["Langfuse", 3500], ["Braintrust", 12000], ["Datadog (15-day)", 17500]];
  s.addChart(pres.charts.BAR, [{ name: "$ per month", labels: comp.map((r) => r[0]), values: comp.map((r) => r[1]) }], {
    x: 0.6, y: 1.55, w: 7.8, h: 4.85, barDir: "bar", varyColors: true,
    chartColors: comp.map((r) => (r[0] === "Precog" ? HEX.accent1 : HEX.accent4)), invertedColors: comp.map((r) => (r[0] === "Precog" ? HEX.accent1 : HEX.accent4)),
    showValue: true, dataLabelPosition: "outEnd", dataLabelFormatCode: "\"$\"#,##0", dataLabelFontSize: 14, dataLabelColor: HEX.dk1, dataLabelFontFace: "+mn-lt",
    catAxisLabelFontSize: 14, catAxisLabelColor: HEX.dk2, catAxisLabelFontFace: "+mn-lt", catAxisOrientation: "maxMin",
    valAxisHidden: true, valGridLine: { style: "none" }, catGridLine: { style: "none" }, showLegend: false,
    showTitle: true, title: "1M sessions a month, 4 MB each, kept 6 months ($ per month, list prices)", titleFontSize: 14, titleColor: HEX.dk2, titleFontFace: "+mn-lt",
  });
  card(s, 8.8, 1.6, 3.9, 4.6, "win lose");
  s.addText([
    { text: "We win on", options: { bold: true, fontSize: 18, color: C.accent1, breakLine: true } },
    { text: "price per GiB, write latency, EU residency, dedup and compaction", options: { fontSize: 15, color: C.text2, breakLine: true } },
    { text: " ", options: { fontSize: 8, breakLine: true } },
    { text: "We lose on", options: { bold: true, fontSize: 18, color: C.accent6, breakLine: true } },
    { text: "per-stream cost (until idle streams leave RAM), multi-AZ object-storage durability, and S2's head start with the AI SDK, Mastra and Anthropic's SDK", options: { fontSize: 15, color: C.text2 } },
  ], { x: 9.1, y: 1.85, w: 3.35, h: 4.1, valign: "top", paraSpaceAfter: 4, isTextBox: true, margin: 0 });
  footnote(s, "Our estimate from vendors' list prices (Nov 2026). Logfire is cheaper on small records but is observability, not a resumable stream.");

  // 11. Pricing
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Competition and pricing" });
  s.addText("Half S2's price on bytes still leaves 86–98% gross margin", { placeholder: "title" });
  const ph = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: HEX.dk1 }, align: "right" } });
  const pr = (cells) => cells.map((c, i) => ({ text: c, options: { align: i === 0 ? "left" : "right", bold: i === 4 } }));
  s.addTable([
    [{ text: "Line", options: { bold: true, color: C.background1, fill: { color: HEX.dk1 } } }, ph("Our price"), ph("s2.dev list"), ph("Our cost"), ph("Gross margin")],
    pr(["GiB written", "$0.020", "$0.050", "$0.0028", "86%"]),
    pr(["GiB read, private", "$0.010", "$0.050", "$0.0002", "98%"]),
    pr(["GiB read, internet", "$0.020", "$0.100", "$0.0014", "93%"]),
    pr(["GiB-month stored", "$0.025", "$0.050", "$0.0032", "87%"]),
    pr(["1,000 streams-month", "$0.050", "$0.010", "$0.045 → $0.013", "10% → 74%"]),
  ], { x: 0.6, y: 1.6, w: 7.9, colW: [2.5, 1.3, 1.3, 1.5, 1.3], fontSize: 15, color: C.text1, border: { type: "solid", pt: 0.75, color: HEX.accent4 }, rowH: 0.62, valign: "middle", fill: { color: HEX.lt1 } });
  card(s, 8.9, 1.6, 3.8, 4.6, "plans");
  s.addText([
    { text: "Free", options: { bold: true, fontSize: 18, color: C.accent1, breakLine: true } },
    { text: "10 GiB written and stored a month, 10k streams", options: { fontSize: 14, color: C.text2, breakLine: true } },
    { text: "Usage", options: { bold: true, fontSize: 18, color: C.accent1, breakLine: true } },
    { text: "the price list, monthly, EU region", options: { fontSize: 14, color: C.text2, breakLine: true } },
    { text: "Committed", options: { bold: true, fontSize: 18, color: C.accent1, breakLine: true } },
    { text: "from $24k a year; down to $0.012/GiB written; 1-year retention; dedicated servers; SLA", options: { fontSize: 14, color: C.text2 } },
  ], { x: 9.2, y: 1.85, w: 3.3, h: 4.1, valign: "top", paraSpaceAfter: 6, isTextBox: true, margin: 0 });
  footnote(s, "Costs modelled on Hetzner bare metal: free internal traffic, SX65 HDD servers with erasure coding for old data (~$3/TB-month). Streams: RAM today → off-heap.");

  // 12. Go-to-market motion
  pres.addSection({ title: "Go-to-market" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Go-to-market" });
  s.addText("Bottom-up through integrations, then sales to platforms and EU vendors", { placeholder: "title" });
  const phases = [["Build", "Months 0–3", "Owned streams, S2 API, AI SDK adapter, 10 design partners"],
    ["Launch", "Months 4–6", "Public endpoint, free tier, OTLP, MCP reader; HN and AI Engineer EU"],
    ["Self-serve", "Months 6–12", "Usage billing, 5 integrations, migration guides from S2 and Redis"],
    ["Sales-assisted", "Months 9–18", "AE and SE; platforms, voice, coding agents; commits from $24k"],
    ["Enterprise", "2027–2028", "AI Act log package, ISO 27001 and SOC 2, integrator partners"]];
  phases.forEach(([name, when, body], i) => {
    const x = 0.6 + i * 2.45;
    s.addShape(pres.shapes.CHEVRON, { x, y: 1.75, w: 2.4, h: 0.9, fill: { color: i === 0 ? HEX.accent1 : HEX.dk2 }, line: { color: i === 0 ? HEX.accent1 : HEX.dk2 }, objectName: "phase " + name });
    s.addText(name, { x: x + 0.55, y: 1.75, w: 1.6, h: 0.9, fontSize: 15, bold: true, color: C.background1, valign: "middle", isTextBox: true, margin: 0 });
    s.addText(when, { x, y: 2.85, w: 2.3, h: 0.35, fontSize: 14, bold: true, color: C.accent1, isTextBox: true, margin: 0 });
    s.addText(body, { x, y: 3.25, w: 2.3, h: 1.6, fontSize: 14, color: C.text2, valign: "top", isTextBox: true, margin: 0 });
  });
  card(s, 0.6, 5.1, 12.1, 1.2, "gate", HEX.lt2);
  s.addText([
    { text: "Gates:  ", options: { bold: true, color: C.accent1 } },
    { text: "idle streams off RAM and costs proven on Hetzner → 10 partners in production → ~$65k MRR at end of year 1 → first profitable quarter (base case Y2 Q2)" },
  ], { x: 0.9, y: 5.25, w: 11.5, h: 0.9, fontSize: 15, color: C.text1, valign: "middle", isTextBox: true, margin: 0 });

  // 13. Channels and messages
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Go-to-market" });
  s.addText("Integrations are the distribution; each profile hears one sentence", { placeholder: "title" });
  s.addText([
    { text: "Channels by month 12", options: { bold: true, fontSize: 20, color: C.text1, breakLine: true } },
    { text: "5 framework integrations listed in their docs (AI SDK, Mastra, LangGraph, OpenAI Agents SDK, OTel)", options: { bullet: true, breakLine: true } },
    { text: "S2 and Durable Streams compatibility: switch by changing a URL", options: { bullet: true, breakLine: true } },
    { text: "Open-source broker and reproducible benchmarks", options: { bullet: true, breakLine: true } },
    { text: "Founder-led sales at EU events: 15 qualified enterprise conversations", options: { bullet: true, breakLine: true } },
    { text: "Cost calculator and agent-telemetry cost reports", options: { bullet: true, breakLine: true } },
    { text: "One signed observability-vendor OEM", options: { bullet: true } },
  ], { x: 0.6, y: 1.6, w: 6.0, h: 4.7, fontSize: 15, color: C.text2, valign: "top", paraSpaceAfter: 8, isTextBox: true, margin: 0 });
  [["App builders", "“Never lose an agent's answer again. Resumable streams in one line; half S2's price.”"],
   ["Agent platforms", "“A session log per user, millions of them, under your product, from €0.02 a GiB.”"],
   ["Voice and support vendors", "“2 ms durable event logs, hosted in the EU, kept as long as your auditors ask.”"]]
    .forEach(([who, msg], i) => {
      const y = 1.6 + i * 1.6;
      card(s, 7.0, y, 5.7, 1.4, "message " + who, HEX.dk1);
      s.addText(who, { x: 7.3, y: y + 0.15, w: 5.1, h: 0.35, fontSize: 15, bold: true, color: C.accent2, isTextBox: true, margin: 0 });
      s.addText(msg, { x: 7.3, y: y + 0.5, w: 5.1, h: 0.8, fontSize: 15, italic: true, color: C.background1, valign: "top", isTextBox: true, margin: 0 });
    });

  // 14. Financials
  pres.addSection({ title: "Financials and ask" });
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Financials and ask" });
  s.addText("Revenue passes costs in Y2 Q2 in the base case; the conservative case needs $1.1M", { placeholder: "title" });
  const qs = ["Y1 Q1", "Y1 Q2", "Y1 Q3", "Y1 Q4", "Y2 Q1", "Y2 Q2", "Y2 Q3", "Y2 Q4", "Y3 Q1", "Y3 Q2", "Y3 Q3", "Y3 Q4"];
  s.addChart([
    { type: pres.charts.BAR, data: [{ name: "Revenue ($k)", labels: qs, values: [0, 14, 100, 201, 392, 547, 843, 1102, 1495, 1963, 2472, 3045] }], options: { chartColors: [HEX.accent1], barGapWidthPct: 60 } },
    { type: pres.charts.LINE, data: [{ name: "Total costs ($k)", labels: qs, values: [161, 201, 251, 263, 399, 457, 531, 601, 726, 859, 998, 1145] }], options: { chartColors: [HEX.accent3], lineSize: 2, lineDataSymbol: "circle", lineDataSymbolSize: 6 } },
  ], {
    x: 0.6, y: 1.55, w: 7.6, h: 4.85, showLegend: true, legendPos: "t", legendFontSize: 12, legendColor: HEX.dk2, legendFontFace: "+mn-lt",
    catAxisLabelFontSize: 11, catAxisLabelColor: HEX.dk2, catAxisLabelFontFace: "+mn-lt",
    valAxisLabelFontSize: 11, valAxisLabelColor: HEX.dk2, valAxisLabelFontFace: "+mn-lt", valAxisLabelFormatCode: "\"$\"#,##0\"k\"",
    valGridLine: { color: HEX.accent4, size: 0.5 }, catGridLine: { style: "none" },
  });
  const fh = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: HEX.dk1 }, align: "right" } });
  s.addTable([
    [{ text: "Scenario", options: { bold: true, color: C.background1, fill: { color: HEX.dk1 } } }, fh("ARR Y3"), fh("Profit"), fh("Cash")],
    [{ text: "Conservative" }, { text: "$6.1M", options: { align: "right" } }, { text: "Y2 Q4", options: { align: "right" } }, { text: "$1.09M", options: { align: "right", bold: true } }],
    [{ text: "Base" }, { text: "$12.2M", options: { align: "right" } }, { text: "Y2 Q2", options: { align: "right" } }, { text: "$0.57M", options: { align: "right" } }],
    [{ text: "Upside" }, { text: "$18.3M", options: { align: "right" } }, { text: "Y1 Q4", options: { align: "right" } }, { text: "$0.45M", options: { align: "right" } }],
  ], { x: 8.6, y: 1.6, w: 4.1, colW: [1.3, 1.0, 0.9, 0.9], fontSize: 13, color: C.text1, border: { type: "solid", pt: 0.75, color: HEX.accent4 }, rowH: 0.55, valign: "middle", fill: { color: HEX.lt1 } });
  s.addText("Gross margin 81–86% every year. Team of 6 in year 1, 11 in year 2, 19 in year 3. Self-serve app builders are 93% of customers but 29% of year-3 revenue.",
    { x: 8.6, y: 4.0, w: 4.1, h: 2.2, fontSize: 14, color: C.text2, valign: "top", isTextBox: true, margin: 0 });
  footnote(s, "bench/s2/business_plan.py: spend per customer at the low end of each profile's researched range; every assumption is named at the top of the script.");

  // 15. Risks
  s = pres.addSlide({ masterName: "Content", sectionTitle: "Financials and ask" });
  s.addText("The biggest risk is ours to fix: what a stream costs to hold", { placeholder: "title" });
  [["Per-stream cost", "6M streams cost ~$270/month of RAM against ~$75 of write revenue", "Idle streams off RAM before launch (6–10 weeks)"],
   ["S2 is first", "already integrated with the AI SDK, Mastra and Anthropic's SDK", "Speak S2's API; win on price, latency, EU"],
   ["AWS egress", "~$0.09/GB for AWS customers to reach us", "Target EU and non-AWS first; interconnect later"],
   ["Bundling", "Datadog and ClickHouse fold agent telemetry into platforms", "Sell underneath them (OEM)"]]
    .forEach(([name, why, fix], i) => {
      const x = 0.6 + (i % 2) * 6.1, y = 1.6 + Math.floor(i / 2) * 2.4;
      card(s, x, y, 5.8, 2.15, "risk " + name);
      s.addText(name, { x: x + 0.3, y: y + 0.2, w: 5.2, h: 0.4, fontSize: 19, bold: true, color: i === 0 ? C.accent6 : C.text1, isTextBox: true, margin: 0 });
      s.addText(why, { x: x + 0.3, y: y + 0.65, w: 5.2, h: 0.65, fontSize: 15, color: C.text2, valign: "top", isTextBox: true, margin: 0 });
      s.addText([{ text: "Mitigation  ", options: { bold: true, color: C.accent1 } }, { text: fix }], { x: x + 0.3, y: y + 1.35, w: 5.2, h: 0.6, fontSize: 15, color: C.text1, valign: "top", isTextBox: true, margin: 0 });
    });

  // 16. Ask
  s = pres.addSlide({ masterName: "Title dark", sectionTitle: "Financials and ask" });
  s.addText("The next 90 days", { placeholder: "title" });
  s.addText([
    { text: "Decide hosted vs self-hosted, and raise about $2M seed", options: { bullet: true, breakLine: true } },
    { text: "Ship owned streams, idle streams off RAM, and the S2 REST/SSE gateway", options: { bullet: true, breakLine: true } },
    { text: "Rent 3 AX102 + 6 SX65 on Hetzner; confirm the cost model on real hardware", options: { bullet: true, breakLine: true } },
    { text: "Sign 10 design partners: 5 app builders, 3 platforms, 2 EU voice or support vendors", options: { bullet: true } },
  ], { placeholder: "body", fontSize: 18, paraSpaceAfter: 8 });

  await pres.writeFile({ fileName: OUT });
  await applyTheme(OUT, THEME);
  console.log("wrote " + OUT);
}

main().catch((e) => { console.error(e); process.exit(1); });
