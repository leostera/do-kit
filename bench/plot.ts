// Draws the README graphs from bench/results/latest.json: one SVG per graph, in docs/bench/.
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

type Target = "single" | "split" | "presplit" | "legacy";
interface Point {
  target: Target;
  operation: "read" | "write";
  distribution: "uniform" | "hot";
  concurrency: number;
  payloadBytes: number;
  operationsPerSecond: number;
  mebibytesPerSecond: number;
  errors: number;
  latencyMs: { p50: number; p95: number; p99: number; max: number };
  topology: { leaves: number; rows: number; depth: number } | null;
}
interface SplitRun {
  target: "split" | "two-copy" | "legacy";
  bytes: number;
  servingMs: number;
  doneMs: number;
  errors: number;
  writeLatencyMs: { p50: number; p95: number; p99: number; max: number };
}
interface Results { suite: string; generatedAt: string; points: Point[]; splits?: SplitRun[] }

interface Series { label: string; color: string; dash?: string; points: [number, number][] }
interface Chart {
  title: string;
  subtitle: string;
  xLabel: string;
  yLabel: string;
  xLog?: boolean;
  yLog?: boolean;
  xTicks: number[];
  xFormat?: (x: number) => string;
  series: Series[];
}

const results = JSON.parse(await readFile(join(import.meta.dir, "results", "latest.json"), "utf8")) as Results;
const outDir = join(import.meta.dir, "..", "docs", "bench");
await mkdir(outDir, { recursive: true });

const COLORS: Record<Target | "two-copy", string> = { single: "#667085", split: "#e4572e", presplit: "#f79009", legacy: "#2e86ab", "two-copy": "#7a5af8" };
const SPLIT_NAMES = { split: "splittable-object: move one half (snapshot)", "two-copy": "freeze, snapshot into two new objects", legacy: "v0.1: copy rows to two new objects" };
const NAMES: Record<Target, string> = { single: "one Durable Object", split: "splittable-object", presplit: "splittable-object, 16 leaves from the start", legacy: "splittable-object v0.1 (row copy)" };
const W = 1200;
const PLOT_H = 380;
const L = 96;
const R = 40;
const T = 92;
const LEGEND_ROW = 28;

function niceMax(v: number): number {
  const p = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 2.5, 5, 10].map((m) => m * p).find((n) => n >= v)!;
}

function fmt(v: number): string {
  if (v >= 1000) return `${Number((v / 1000).toFixed(1))}k`;
  if (v > 0 && v < 1) return String(Number(v.toPrecision(2)));
  return v >= 10 || v === 0 ? String(Math.round(v)) : v.toFixed(1);
}

function chart(input: Chart): string {
  const c = { ...input, series: input.series.filter((s) => s.points.length > 0) };
  const all = c.series.flatMap((s) => s.points);
  const xs = all.map(([x]) => x);
  const ys = all.map(([, y]) => y);
  const xMin = Math.min(...c.xTicks, ...xs);
  const xMax = Math.max(...c.xTicks, ...xs);
  const yMin = c.yLog ? 10 ** Math.floor(Math.log10(Math.min(...ys.filter((y) => y > 0)))) : 0;
  const yMax = c.yLog ? 10 ** Math.ceil(Math.log10(Math.max(...ys) * 1.05)) : niceMax(Math.max(1, ...ys) * 1.08);
  const pw = W - L - R;
  const ph = PLOT_H;
  const perRow = 2;
  const H = T + ph + 84 + Math.ceil(c.series.length / perRow) * LEGEND_ROW + 12;
  const sx = (x: number) => L + (c.xLog ? Math.log2(x / xMin) / Math.log2(xMax / xMin) : (x - xMin) / (xMax - xMin)) * pw;
  const sy = (y: number) => T + ph - (c.yLog ? Math.log10(Math.max(y, yMin) / yMin) / Math.log10(yMax / yMin) : y / yMax) * ph;
  const yTicks = c.yLog ? Array.from({ length: Math.log10(yMax / yMin) + 1 }, (_, i) => yMin * 10 ** i) : Array.from({ length: 6 }, (_, i) => (yMax * i) / 5);
  const grid = yTicks.map((v) => `<line x1="${L}" x2="${L + pw}" y1="${sy(v)}" y2="${sy(v)}" class="grid"/><text x="${L - 12}" y="${sy(v) + 5}" text-anchor="end" class="tick">${fmt(v)}</text>`).join("");
  const xAxis = c.xTicks.map((v) => `<line x1="${sx(v)}" x2="${sx(v)}" y1="${T + ph}" y2="${T + ph + 6}" class="axisline"/><text x="${sx(v)}" y="${T + ph + 26}" text-anchor="middle" class="tick">${(c.xFormat ?? String)(v)}</text>`).join("");
  const lines = c.series
    .filter((s) => s.points.length)
    .map((s) => {
      const pts = [...s.points].sort((a, b) => a[0] - b[0]);
      const path = pts.map(([x, y]) => `${sx(x).toFixed(1)},${sy(y).toFixed(1)}`).join(" ");
      return `<polyline points="${path}" fill="none" stroke="${s.color}" stroke-width="4" stroke-linejoin="round" ${s.dash ? `stroke-dasharray="${s.dash}"` : ""}/>${pts.map(([x, y]) => `<circle cx="${sx(x).toFixed(1)}" cy="${sy(y).toFixed(1)}" r="5.5" fill="${s.color}"/>`).join("")}`;
    })
    .join("");
  const legend = c.series.map((s, i) => `<g transform="translate(${L + (i % perRow) * (pw / perRow)},${T + ph + 92 + Math.floor(i / perRow) * LEGEND_ROW})"><line x1="0" x2="30" y1="0" y2="0" stroke="${s.color}" stroke-width="4" ${s.dash ? `stroke-dasharray="${s.dash}"` : ""}/><text x="40" y="5" class="legend">${s.label}</text></g>`).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
<style>text{font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;fill:#101828}.title{font-size:26px;font-weight:700}.sub{font-size:15px;fill:#475467}.tick{font-size:14px;fill:#667085}.label{font-size:15px;font-weight:600;fill:#344054}.legend{font-size:15px}.grid{stroke:#eaecf0}.axisline{stroke:#344054}</style>
<rect width="${W}" height="${H}" fill="#fff"/>
<text x="${L}" y="40" class="title">${c.title}</text><text x="${L}" y="66" class="sub">${c.subtitle}</text>
${grid}<line x1="${L}" x2="${L + pw}" y1="${T + ph}" y2="${T + ph}" class="axisline"/>${xAxis}${lines}
<text x="${L + pw / 2}" y="${T + ph + 54}" text-anchor="middle" class="label">${c.xLabel}</text>
<text transform="translate(26,${T + ph / 2}) rotate(-90)" text-anchor="middle" class="label">${c.yLabel}</text>
${legend}
</svg>
`;
}

const uniform = results.points.filter((p) => p.distribution === "uniform");
const payload = uniform[0]?.payloadBytes ?? 1024;
const levels = [...new Set(uniform.map((p) => p.concurrency))].sort((a, b) => a - b);
const targets = (["single", "split", "presplit", "legacy"] as const).filter((t) => uniform.some((p) => p.target === t));
const sel = (target: Target, operation: "read" | "write", value: (p: Point) => number): [number, number][] => uniform.filter((p) => p.target === target && p.operation === operation).map((p) => [p.concurrency, value(p)]);
const runNote = `1 KiB values, uniform keys, 10 s per point, ${results.generatedAt.slice(0, 10)}`;

const graphs: Record<string, string> = {};

graphs.throughput = chart({
  title: "Throughput vs concurrency",
  subtitle: `Successful one-key operations per second. ${runNote}`,
  xLabel: "Concurrent in-flight operations",
  yLabel: "operations / s",
  xLog: true,
  xTicks: levels,
  series: targets.flatMap((t) => [
    { label: `${NAMES[t]} · write`, color: COLORS[t], points: sel(t, "write", (p) => p.operationsPerSecond) },
    { label: `${NAMES[t]} · read`, color: COLORS[t], dash: "8 6", points: sel(t, "read", (p) => p.operationsPerSecond) },
  ]),
});

for (const pct of ["p50", "p95", "p99", "max"] as const) {
  graphs[`latency-${pct}`] = chart({
    title: `Write latency ${pct} vs concurrency`,
    subtitle: `Client-side latency of one-key writes (log scale). ${runNote}`,
    xLabel: "Concurrent in-flight operations",
    yLabel: `${pct} latency (ms)`,
    xLog: true,
    yLog: true,
    xTicks: levels,
    series: targets.map((t) => ({ label: NAMES[t], color: COLORS[t], points: sel(t, "write", (p) => Math.max(1, p.latencyMs[pct])) })),
  });
}

graphs.errors = chart({
  title: "Failed calls vs concurrency",
  subtitle: `Calls that failed in 10 s (an overloaded object refuses them). ${runNote}`,
  xLabel: "Concurrent in-flight operations",
  yLabel: "failed calls",
  xLog: true,
  xTicks: levels,
  series: targets.flatMap((t) => [
    { label: `${NAMES[t]} · write`, color: COLORS[t], points: sel(t, "write", (p) => p.errors) },
    { label: `${NAMES[t]} · read`, color: COLORS[t], dash: "8 6", points: sel(t, "read", (p) => p.errors) },
  ]),
});

graphs.bandwidth = chart({
  title: "Data throughput vs concurrency",
  subtitle: `Value bytes moved per second. ${runNote}`,
  xLabel: "Concurrent in-flight operations",
  yLabel: "MiB / s",
  xLog: true,
  xTicks: levels,
  series: targets.flatMap((t) => [
    { label: `${NAMES[t]} · write`, color: COLORS[t], points: sel(t, "write", (p) => p.mebibytesPerSecond) },
    { label: `${NAMES[t]} · read`, color: COLORS[t], dash: "8 6", points: sel(t, "read", (p) => p.mebibytesPerSecond) },
  ]),
});

// Leaves the load produced, and the storage ceiling that gives (10 GB per SQLite-backed object).
const leafPoints = (t: Target) => uniform.filter((p) => p.target === t && p.operation === "write" && p.topology).map((p): [number, number] => [p.concurrency, p.topology!.leaves * 10]);
graphs.capacity = chart({
  title: "Storage ceiling vs load",
  subtitle: "Leaves the measured write load split into, × the 10 GB limit of one SQLite-backed Durable Object",
  xLabel: "Concurrent in-flight writes",
  yLabel: "storage ceiling (GB)",
  xLog: true,
  xTicks: levels,
  series: [
    { label: "one Durable Object", color: COLORS.single, points: levels.map((c) => [c, 10]) },
    { label: `${NAMES.split}: leaves × 10 GB`, color: COLORS.split, points: leafPoints("split") },
    ...(targets.includes("presplit") ? [{ label: `${NAMES.presplit}: leaves × 10 GB`, color: COLORS.presplit, points: leafPoints("presplit") }] : []),
  ],
});

const splits = results.splits ?? [];
if (splits.length) {
  const sizes = [...new Set(splits.map((s) => s.bytes / 1048576))].sort((a, b) => a - b);
  const mib = (s: SplitRun) => s.bytes / 1048576;
  const of = (t: SplitRun["target"], v: (s: SplitRun) => number): [number, number][] => splits.filter((s) => s.target === t).map((s) => [mib(s), v(s)]);
  const sizeFmt = (v: number) => (v >= 1024 ? `${v / 1024} GiB` : `${v} MiB`);
  // When the new owner serves with all its data: ours once the copy is claimed (deleting the
  // half it gave away continues in the background); two-copy at the end of its freeze; the row
  // copy once the last row has moved (its children serve sooner, by asking the parent per key).
  graphs["split-time"] = chart({
    title: "Time to split vs data size",
    subtitle: "Until the new owner serves with all of its data, under 8 concurrent writers (log scale)",
    xLabel: "Data in the object when it splits",
    yLabel: "seconds",
    xLog: true,
    yLog: true,
    xTicks: sizes,
    xFormat: sizeFmt,
    series: [
      { label: SPLIT_NAMES.split, color: COLORS.split, points: of("split", (s) => s.servingMs / 1000) },
      { label: SPLIT_NAMES["two-copy"], color: COLORS["two-copy"], points: of("two-copy", (s) => s.doneMs / 1000) },
      { label: SPLIT_NAMES.legacy, color: COLORS.legacy, points: of("legacy", (s) => s.doneMs / 1000) },
    ],
  });
  graphs["split-latency"] = chart({
    title: "Slowest write during a split",
    subtitle: "Maximum write latency from 1 s before the split until 1 s after it settles (log scale)",
    xLabel: "Data in the object when it splits",
    yLabel: "milliseconds",
    xLog: true,
    yLog: true,
    xTicks: sizes,
    xFormat: sizeFmt,
    series: [
      { label: SPLIT_NAMES.split, color: COLORS.split, points: of("split", (s) => Math.max(1, s.writeLatencyMs.max)) },
      { label: SPLIT_NAMES["two-copy"], color: COLORS["two-copy"], points: of("two-copy", (s) => Math.max(1, s.writeLatencyMs.max)) },
      { label: SPLIT_NAMES.legacy, color: COLORS.legacy, points: of("legacy", (s) => Math.max(1, s.writeLatencyMs.max)) },
    ],
  });
  graphs["split-p95"] = chart({
    title: "Write latency p95 during a split",
    subtitle: "From 1 s before the split until 1 s after it settles, including background cleanup (log scale)",
    xLabel: "Data in the object when it splits",
    yLabel: "milliseconds",
    xLog: true,
    yLog: true,
    xTicks: sizes,
    xFormat: sizeFmt,
    series: [
      { label: SPLIT_NAMES.split, color: COLORS.split, points: of("split", (s) => Math.max(1, s.writeLatencyMs.p95)) },
      { label: SPLIT_NAMES["two-copy"], color: COLORS["two-copy"], points: of("two-copy", (s) => Math.max(1, s.writeLatencyMs.p95)) },
      { label: SPLIT_NAMES.legacy, color: COLORS.legacy, points: of("legacy", (s) => Math.max(1, s.writeLatencyMs.p95)) },
    ],
  });
}

for (const [name, svg] of Object.entries(graphs)) await writeFile(join(outDir, `${name}.svg`), svg);
console.log(`Wrote ${Object.keys(graphs).length} graphs to docs/bench/`);

// ---- Cost estimates (Durable Objects pricing, Workers Paid, beyond the included usage) ----
// https://developers.cloudflare.com/durable-objects/platform/pricing/ (read 2026-10-29)
const PRICE = { request: 0.15e-6, rowWritten: 1e-6, rowRead: 0.001e-6, gbSecond: 12.5e-6, objectGb: 0.125 };

interface Bar { label: string; parts: { label: string; color: string; value: number }[] }
function barChart(title: string, subtitle: string, unit: (v: number) => string, bars: Bar[], note: string): string {
  const max = niceMax(Math.max(...bars.map((b) => b.parts.reduce((s, p) => s + p.value, 0))) * 1.1);
  const top = 100;
  const rowH = 64;
  const left = 330;
  const width = W - left - 140;
  const h = top + bars.length * rowH + 110;
  const partNames = [...new Map(bars.flatMap((b) => b.parts.map((p) => [p.label, p.color] as const))).entries()];
  const rows = bars
    .map((b, i) => {
      let x = left;
      const y = top + i * rowH;
      const total = b.parts.reduce((s, p) => s + p.value, 0);
      const rects = b.parts
        .map((p) => {
          const w = (p.value / max) * width;
          const r = `<rect x="${x.toFixed(1)}" y="${y}" width="${Math.max(0, w).toFixed(1)}" height="40" fill="${p.color}"/>`;
          x += w;
          return r;
        })
        .join("");
      return `<text x="${left - 16}" y="${y + 26}" text-anchor="end" class="legend">${b.label}</text>${rects}<text x="${x + 10}" y="${y + 26}" class="label">${unit(total)}</text>`;
    })
    .join("");
  const legend = partNames.map(([label, color], i) => `<g transform="translate(${left + i * 240},${top + bars.length * rowH + 30})"><rect width="18" height="18" y="-13" fill="${color}"/><text x="28" y="2" class="legend">${label}</text></g>`).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${h}" viewBox="0 0 ${W} ${h}">
<style>text{font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;fill:#101828}.title{font-size:26px;font-weight:700}.sub{font-size:15px;fill:#475467}.label{font-size:15px;font-weight:600;fill:#344054}.legend{font-size:15px}.note{font-size:13px;fill:#667085}</style>
<rect width="${W}" height="${h}" fill="#fff"/>
<text x="${L}" y="40" class="title">${title}</text><text x="${L}" y="66" class="sub">${subtitle}</text>
${rows}${legend}<text x="${L}" y="${h - 20}" class="note">${note}</text>
</svg>
`;
}

// Duration at saturation: objects busy the whole time, so object-seconds = writes / (writes/s) × objects.
const peak = (target: Target) => uniform.filter((p) => p.target === target && p.operation === "write").sort((a, b) => b.operationsPerSecond - a.operationsPerSecond)[0];
const singlePeak = peak("single");
const leafPeak = peak("presplit");
if (singlePeak && leafPeak) {
  // Dollars per million writes.
  const durationCost = (p: Point) => (1e6 / p.operationsPerSecond) * (p.topology?.leaves ?? 1) * PRICE.objectGb * PRICE.gbSecond;
  const C = { request: "#98a2b3", rows: "#e4572e", reads: "#f79009", duration: "#7a5af8" };
  const bar = (label: string, rowsWritten: number, rowsRead: number, p: Point): Bar => ({
    label,
    parts: [
      { label: "requests", color: C.request, value: PRICE.request * 1e6 },
      { label: "rows written", color: C.rows, value: rowsWritten * PRICE.rowWritten * 1e6 },
      { label: "rows read", color: C.reads, value: rowsRead * PRICE.rowRead * 1e6 },
      { label: "duration", color: C.duration, value: durationCost(p) },
    ],
  });
  graphs["cost-writes"] = barChart(
    "Estimated cost per million writes",
    `Durable Objects list prices beyond included usage; duration at the measured peak (${fmt(singlePeak.operationsPerSecond)} vs ${fmt(leafPeak.operationsPerSecond)} writes/s)`,
    (v) => `$${v.toFixed(2)}`,
    [
      bar("one Durable Object · update", 1, 0, singlePeak),
      bar("splittable-object · update", 1, 1, leafPeak),
      bar("one Durable Object · new key", 1, 0, singlePeak),
      bar("splittable-object · new key", 2, 1, leafPeak),
    ],
    "A new key also writes the hash index: one more row written. Updates leave it alone. Each write reads its row first. Duration is a fraction of a cent either way.",
  );
}

// One split, by data size: rows written for the copies and the deletes (1 KiB rows, one index each).
// splittable-object: the snapshot restore (not billed as rows) and both sides deleting the half they
// gave away: R deletes × 2 rows. Freeze into two: each child deletes half (R × 2) and the parent
// deletes everything (R × 2). v0.1 row copy: children insert R rows (× 2), mark R keys decided and
// clear them (R × 2), and the parent deletes R rows (× 2).
const splitSizes = [1, 16, 128, 1024, 10240];
const rowsIn = (mib: number) => (mib * 1048576) / 1024;
graphs["cost-split"] = chart({
  title: "Estimated cost of one split",
  subtitle: "Rows written to copy and clean up an object of 1 KiB rows, at $1.00 per million rows (log scale)",
  xLabel: "Data in the object when it splits",
  yLabel: "US dollars",
  xLog: true,
  yLog: true,
  xTicks: splitSizes,
  xFormat: (v) => (v >= 1024 ? `${v / 1024} GiB` : `${v} MiB`),
  series: [
    { label: SPLIT_NAMES.split, color: COLORS.split, points: splitSizes.map((m) => [m, 2 * rowsIn(m) * PRICE.rowWritten]) },
    { label: SPLIT_NAMES["two-copy"], color: COLORS["two-copy"], points: splitSizes.map((m) => [m, 4 * rowsIn(m) * PRICE.rowWritten]) },
    { label: SPLIT_NAMES.legacy, color: COLORS.legacy, points: splitSizes.map((m) => [m, 6 * rowsIn(m) * PRICE.rowWritten]) },
  ],
});
for (const name of ["cost-writes", "cost-split"]) if (graphs[name]) await writeFile(join(outDir, `${name}.svg`), graphs[name]);
