// Draws the README graphs from bench/results/latest.json: one SVG per graph, in docs/bench/.
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

type Target = "single" | "split" | "legacy";
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
  target: "split" | "legacy";
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

const COLORS: Record<Target, string> = { single: "#667085", split: "#e4572e", legacy: "#2e86ab" };
const NAMES: Record<Target, string> = { single: "one Durable Object", split: "splittable (snapshot splits)", legacy: "splittable (row-copy splits, v0.1)" };
const W = 1200;
const H = 560;
const L = 96;
const R = 40;
const T = 92;
const B = 120;

function niceMax(v: number): number {
  const p = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 2.5, 5, 10].map((m) => m * p).find((n) => n >= v)!;
}

function fmt(v: number): string {
  if (v >= 1000) return `${Number((v / 1000).toFixed(1))}k`;
  return v >= 10 || v === 0 ? String(Math.round(v)) : v.toFixed(1);
}

function chart(c: Chart): string {
  const all = c.series.flatMap((s) => s.points);
  const xs = all.map(([x]) => x);
  const ys = all.map(([, y]) => y);
  const xMin = Math.min(...c.xTicks, ...xs);
  const xMax = Math.max(...c.xTicks, ...xs);
  const yMin = c.yLog ? 10 ** Math.floor(Math.log10(Math.max(1, Math.min(...ys)))) : 0;
  const yMax = c.yLog ? 10 ** Math.ceil(Math.log10(Math.max(...ys) * 1.05)) : niceMax(Math.max(1, ...ys) * 1.08);
  const pw = W - L - R;
  const ph = H - T - B;
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
  const perRow = c.series.length > 4 ? 3 : 4;
  const legend = c.series.map((s, i) => `<g transform="translate(${L + (i % perRow) * (pw / perRow)},${H - 40 + Math.floor(i / perRow) * 24 - (Math.ceil(c.series.length / perRow) - 1) * 24})"><line x1="0" x2="30" y1="0" y2="0" stroke="${s.color}" stroke-width="4" ${s.dash ? `stroke-dasharray="${s.dash}"` : ""}/><text x="40" y="5" class="legend">${s.label}</text></g>`).join("");
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
const targets = (["single", "split", "legacy"] as const).filter((t) => uniform.some((p) => p.target === t));
const sel = (target: Target, operation: "read" | "write", value: (p: Point) => number): [number, number][] => uniform.filter((p) => p.target === target && p.operation === operation).map((p) => [p.concurrency, value(p)]);
const runNote = `${payload}-byte values, uniform keys, 10 s per point · run ${results.suite}, ${results.generatedAt.slice(0, 10)}`;

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
const leafPoints = uniform.filter((p) => p.target === "split" && p.operation === "write" && p.topology).map((p): [number, number] => [p.concurrency, p.topology!.leaves * 10]);
graphs.capacity = chart({
  title: "Storage ceiling vs load",
  subtitle: "Leaves the measured write load split into, × the 10 GB limit of one SQLite-backed Durable Object",
  xLabel: "Concurrent in-flight writes",
  yLabel: "storage ceiling (GB)",
  xLog: true,
  xTicks: levels,
  series: [
    { label: "one Durable Object", color: COLORS.single, points: levels.map((c) => [c, 10]) },
    { label: "splittable: leaves × 10 GB", color: COLORS.split, points: leafPoints },
  ],
});

const splits = results.splits ?? [];
if (splits.length) {
  const sizes = [...new Set(splits.map((s) => s.bytes / 1048576))].sort((a, b) => a - b);
  const mib = (s: SplitRun) => s.bytes / 1048576;
  const of = (t: "split" | "legacy", v: (s: SplitRun) => number): [number, number][] => splits.filter((s) => s.target === t).map((s) => [mib(s), v(s)]);
  const sizeFmt = (v: number) => (v >= 1024 ? `${v / 1024} GiB` : `${v} MiB`);
  graphs["split-time"] = chart({
    title: "Time to split vs data size",
    subtitle: "One split of a seeded object under 8 concurrent writers: until every row lives only at its new owner (log scale)",
    xLabel: "Data in the object when it splits",
    yLabel: "seconds",
    xLog: true,
    yLog: true,
    xTicks: sizes,
    xFormat: sizeFmt,
    series: [
      { label: "snapshot split", color: COLORS.split, points: of("split", (s) => Math.max(0.01, s.doneMs / 1000)) },
      { label: "row-copy split (v0.1)", color: COLORS.legacy, points: of("legacy", (s) => Math.max(0.01, s.doneMs / 1000)) },
    ],
  });
  graphs["split-latency"] = chart({
    title: "Write latency during a split",
    subtitle: "Writes to random keys, from 1 s before the split to 1 s after it settles (log scale)",
    xLabel: "Data in the object when it splits",
    yLabel: "latency (ms)",
    xLog: true,
    yLog: true,
    xTicks: sizes,
    xFormat: sizeFmt,
    series: [
      { label: "snapshot · p99", color: COLORS.split, points: of("split", (s) => Math.max(1, s.writeLatencyMs.p99)) },
      { label: "snapshot · max", color: COLORS.split, dash: "8 6", points: of("split", (s) => Math.max(1, s.writeLatencyMs.max)) },
      { label: "row copy · p99", color: COLORS.legacy, points: of("legacy", (s) => Math.max(1, s.writeLatencyMs.p99)) },
      { label: "row copy · max", color: COLORS.legacy, dash: "8 6", points: of("legacy", (s) => Math.max(1, s.writeLatencyMs.max)) },
    ],
  });
}

for (const [name, svg] of Object.entries(graphs)) await writeFile(join(outDir, `${name}.svg`), svg);
console.log(`Wrote ${Object.keys(graphs).length} graphs to docs/bench/`);
