// Charts of the work graph benchmarks (RFD0005), from bench/results/workgraph.json, into
// docs/bench/workgraph-*.svg. Linear axes from zero; runner counts evenly spaced.
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { planWorkGraph } from "../../src/workgraph/plan.ts";
import { graphOf, type Shape } from "./src/graphs.ts";

interface Point {
  suite: string;
  shape: string;
  runners: number;
  units: number;
  chunks: number;
  unitMs: number;
  chunkMs: number;
  work: number;
  span: number;
  elapsedMs: number;
  computed: number;
  expired: number;
  faults?: { evict?: number; lose?: number; leaseMs?: number };
}

const data = JSON.parse(await readFile(join(import.meta.dir, "..", "results", "workgraph.json"), "utf8")) as { points: Point[] };
const out = join(import.meta.dir, "..", "..", "docs", "bench");

interface Series {
  label: string;
  color: string;
  dash?: string;
  points: [number, number][];
}

const W = 1100;
const L = 110;
const R = 40;
const T = 100;
const PH = 460;

function niceMax(v: number): number {
  const p = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}

function fmt(v: number): string {
  return v >= 1000 ? `${Math.round(v / 100) / 10}k`.replace(".0k", "k") : Number.isInteger(v) ? String(v) : v.toFixed(1);
}

function chart(title: string, subtitle: string, xLabel: string, yLabel: string, xs: number[], series: Series[]): string {
  const ys = series.flatMap((s) => s.points.map(([, y]) => y));
  const yMax = niceMax(Math.max(1, ...ys) * 1.08);
  const pw = W - L - R;
  const inset = 50;
  const sx = (x: number) => L + inset + (xs.length > 1 ? xs.indexOf(x) / (xs.length - 1) : 0.5) * (pw - 2 * inset);
  const sy = (y: number) => T + PH - (y / yMax) * PH;
  const rows = Math.ceil(series.length / 2);
  const H = T + PH + 90 + rows * 30 + 10;
  const grid = Array.from({ length: 6 }, (_, i) => (yMax * i) / 5)
    .map((v) => `<line x1="${L}" x2="${L + pw}" y1="${sy(v)}" y2="${sy(v)}" stroke="#eaecf0"/><text x="${L - 12}" y="${sy(v) + 5}" text-anchor="end" class="tick">${fmt(v)}</text>`)
    .join("");
  const xAxis = xs.map((x) => `<text x="${sx(x)}" y="${T + PH + 26}" text-anchor="middle" class="tick">${x}</text>`).join("");
  const lines = series
    .map((s) => {
      const pts = [...s.points].sort((a, b) => a[0] - b[0]);
      const path = pts.map(([x, y]) => `${sx(x).toFixed(1)},${sy(y).toFixed(1)}`).join(" ");
      return `<polyline points="${path}" fill="none" stroke="${s.color}" stroke-width="4" ${s.dash ? `stroke-dasharray="${s.dash}"` : ""}/>${s.dash ? "" : pts.map(([x, y]) => `<circle cx="${sx(x).toFixed(1)}" cy="${sy(y).toFixed(1)}" r="5.5" fill="${s.color}"/>`).join("")}`;
    })
    .join("");
  const legend = series.map((s, i) => `<g transform="translate(${L + (i % 2) * (pw / 2)},${T + PH + 88 + Math.floor(i / 2) * 30})"><line x1="0" x2="30" y1="0" y2="0" stroke="${s.color}" stroke-width="4" ${s.dash ? `stroke-dasharray="${s.dash}"` : ""}/><text x="40" y="5" class="legend">${s.label}</text></g>`).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
<style>text{font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;fill:#101828}.title{font-size:26px;font-weight:700}.sub{font-size:15px;fill:#475467}.tick{font-size:14px;fill:#667085}.label{font-size:15px;font-weight:600;fill:#344054}.legend{font-size:15px}</style>
<rect width="${W}" height="${H}" fill="#fff"/>
<text x="${L}" y="40" class="title">${title}</text><text x="${L}" y="66" class="sub">${subtitle}</text>
${grid}<line x1="${L}" x2="${L + pw}" y1="${T + PH}" y2="${T + PH}" stroke="#344054"/>${xAxis}${lines}
<text x="${L + pw / 2}" y="${T + PH + 56}" text-anchor="middle" class="label">${xLabel}</text>
<text transform="translate(28,${T + PH / 2}) rotate(-90)" text-anchor="middle" class="label">${yLabel}</text>
${legend}
</svg>
`;
}

/** The median elapsed time of the runs of a shape on n runners (the 1-runner runs are repeated). */
function median(points: Point[]): number {
  const xs = points.map((p) => p.elapsedMs).sort((a, b) => a - b);
  return xs.length % 2 ? xs[(xs.length - 1) / 2]! : (xs[xs.length / 2 - 1]! + xs[xs.length / 2]!) / 2;
}

function bars(title: string, subtitle: string, yLabel: string, items: { label: string; value: number; color: string }[]): string {
  const yMax = niceMax(Math.max(...items.map((i) => i.value)) * 1.12);
  const pw = W - L - R;
  const H = T + PH + 80;
  const sy = (y: number) => T + PH - (y / yMax) * PH;
  const slot = pw / items.length;
  const grid = Array.from({ length: 6 }, (_, i) => (yMax * i) / 5)
    .map((v) => `<line x1="${L}" x2="${L + pw}" y1="${sy(v)}" y2="${sy(v)}" stroke="#eaecf0"/><text x="${L - 12}" y="${sy(v) + 5}" text-anchor="end" class="tick">${fmt(v)}</text>`)
    .join("");
  const rects = items
    .map((it, i) => {
      const x = L + i * slot + slot * 0.2;
      return `<rect x="${x}" y="${sy(it.value)}" width="${slot * 0.6}" height="${T + PH - sy(it.value)}" fill="${it.color}"/><text x="${x + slot * 0.3}" y="${sy(it.value) - 10}" text-anchor="middle" class="label">${it.value.toFixed(1)}</text><text x="${x + slot * 0.3}" y="${T + PH + 28}" text-anchor="middle" class="legend">${it.label}</text>`;
    })
    .join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">
<style>text{font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;fill:#101828}.title{font-size:26px;font-weight:700}.sub{font-size:15px;fill:#475467}.tick{font-size:14px;fill:#667085}.label{font-size:15px;font-weight:600;fill:#344054}.legend{font-size:15px}</style>
<rect width="${W}" height="${H}" fill="#fff"/>
<text x="${L}" y="40" class="title">${title}</text><text x="${L}" y="66" class="sub">${subtitle}</text>
${grid}<line x1="${L}" x2="${L + pw}" y1="${T + PH}" y2="${T + PH}" stroke="#344054"/>${rects}
<text transform="translate(28,${T + PH / 2}) rotate(-90)" text-anchor="middle" class="label">${yLabel}</text>
</svg>
`;
}

const SHAPES = [
  ["forest", "40,000 units in trees of 5", "#2e86ab"],
  ["tree", "one random tree of 40,000 units", "#e4572e"],
  ["chain", "one chain of 2,000 units", "#667085"],
] as const;
const scaling = data.points.filter((p) => p.suite === "scaling");
const runners = [...new Set(scaling.map((p) => p.runners))].sort((a, b) => a - b);
const graphs: Record<string, string> = {};

if (scaling.length) {
  const series: Series[] = [];
  for (const [shape, label, color] of SHAPES) {
    const pts = scaling.filter((p) => p.shape === shape);
    if (!pts.length) continue;
    const t1 = median(pts.filter((p) => p.runners === 1));
    // Runners take whole chunks: the bound's T∞ is the most expensive chain of chunks, which the
    // plan of the same shape gives (its structure is seeded, the same in every run).
    const p0 = pts[0]!;
    const span = planWorkGraph(graphOf(shape as Shape, p0.units, p0.unitMs, 1, "plot"), { chunkMs: p0.chunkMs }).chunkSpan;
    console.log(`${shape}: T₁ ${t1} ms, unit span ${p0.span} ms, chunk span ${span} ms`);
    series.push({ label: `${label}: measured`, color, points: runners.map((n) => [n, median(pts.filter((p) => p.runners === n)) / 1000]) });
    series.push({ label: `${label}: T₁/N + T∞`, color, dash: "8 7", points: runners.map((n) => [n, (t1 / n + span) / 1000]) });
  }
  graphs["workgraph-time"] = chart("Time to finish a graph, by runners", "Units of 5 ms CPU, chunks of 1 s. Measured on the edge, against T₁/N + T∞: T₁ the median 1-runner time, T∞ the planned chain of chunks", "Runners", "seconds", runners, series);
  const many = runners.filter((n) => n >= 8);
  graphs["workgraph-time-many"] = chart(
    "Time to finish a graph, 8 to 64 runners",
    "The same runs as above, from 8 runners",
    "Runners",
    "seconds",
    many,
    series.map((s) => ({ ...s, points: s.points.filter(([n]) => n >= 8) })),
  );
  graphs["workgraph-speedup"] = chart(
    "Speedup over one runner",
    "T₁ / T_N, measured. A chain's span is its work: no runner count helps it",
    "Runners",
    "speedup (×)",
    runners,
    SHAPES.flatMap(([shape, label, color]): Series[] => {
      const pts = scaling.filter((p) => p.shape === shape);
      if (!pts.length) return [];
      const t1 = median(pts.filter((p) => p.runners === 1));
      return [{ label, color, points: runners.map((n): [number, number] => [n, t1 / median(pts.filter((p) => p.runners === n))]) }];
    }).concat([{ label: "N: every runner busy all the time", color: "#98a2b3", dash: "4 6", points: runners.map((n): [number, number] => [n, n]) }]),
  );
}

const ceiling = data.points.filter((p) => p.suite === "ceiling");
if (ceiling.length) {
  const xs = [...new Set(ceiling.map((p) => p.runners))].sort((a, b) => a - b);
  graphs["workgraph-ceiling"] = chart("One coordinator: chunks handed out per second", "Chunks of 2 units that cost nothing: each is a lease, a read and a write of the results store, and a completion", "Runners", "chunks / s", xs, [{ label: "chunks done per second", color: "#2e86ab", points: ceiling.map((p) => [p.runners, p.chunks / (p.elapsedMs / 1000)]) }]);
}

const faults = data.points.filter((p) => p.suite === "faults");
if (faults.length) {
  const kind = (p: Point) => (p.faults?.evict ? "evict" : p.faults?.lose ? "lose" : "none");
  const of = (k: string) => faults.find((p) => kind(p) === k);
  const items = [
    ["none", "no faults", "#2e86ab"],
    ["evict", "20% of chunks evicted once", "#f2a541"],
    ["lose", "4 of 16 runners lost (10 s leases)", "#e4572e"],
  ]
    .filter(([k]) => of(k!))
    .map(([k, label, color]) => ({ label: label!, color: color!, value: of(k!)!.elapsedMs / 1000 }));
  graphs["workgraph-faults"] = bars("Faults during a run", "40,000 units of 5 ms in trees of 5, on 16 runners. Every run computes every unit", "seconds", items);
}

for (const [name, svg] of Object.entries(graphs)) await writeFile(join(out, `${name}.svg`), svg);
console.log(`Wrote ${Object.keys(graphs).length} graphs`);
