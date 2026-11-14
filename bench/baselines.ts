// A table series is published only with its baseline from the same run: every table-N point needs
// a plain point (bench/worker/src/plain.ts) of the same suite, operation, distribution and load, so
// what do-kit costs is read from two numbers measured together.

export interface SeriesPoint {
  readonly target: string;
  readonly operation: string;
  readonly distribution: string;
  readonly concurrency: number;
  readonly arrivalRate?: number;
}

/** The table points of a run that have no plain point at the same load: empty when the series may be published. */
export function missingBaselines(points: readonly SeriesPoint[]): string[] {
  const load = (p: SeriesPoint) => `${p.operation} ${p.distribution} ${p.arrivalRate ? `${p.arrivalRate}/s` : p.concurrency}`;
  const baselines = new Set(points.filter((p) => p.target.startsWith("plain-")).map(load));
  return points.filter((p) => p.target.startsWith("table-") && !baselines.has(load(p))).map((p) => `${p.target} ${load(p)}`);
}
