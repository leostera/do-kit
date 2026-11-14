// What a benchmark's answers must be, so a point that lost writes or skipped rows fails instead of
// publishing a good number. Pure: the bench Worker feeds it, and the tests check it directly.

/**
 * The writes to each key, in the order one writer sent them (a key has one writer): the last one
 * acknowledged, and those after it whose outcome is unknown (an error: applied or not). A key
 * then reads back as the last acknowledged value or a later unknown one; with none acknowledged,
 * as its value before (`before`) or an unknown one.
 */
export class WriteLog {
  private readonly keys = new Map<string, { acked: string | null; unknown: string[] }>();

  acked(key: string, value: string): void {
    this.keys.set(key, { acked: value, unknown: [] });
  }

  unknown(key: string, value: string): void {
    const k = this.keys.get(key) ?? { acked: null, unknown: [] };
    k.unknown.push(value);
    this.keys.set(key, k);
  }

  /** Every key written, acknowledged or not. */
  written(): string[] {
    return [...this.keys.keys()];
  }

  /** Keys whose value read back is not one the writes allow. */
  lost(read: ReadonlyMap<string, string | null>, before: (key: string) => string | null): string[] {
    const out: string[] = [];
    for (const [key, { acked, unknown }] of this.keys) {
      const value = read.get(key) ?? null;
      const allowed = new Set<string | null>([...unknown, acked ?? before(key)]);
      if (!allowed.has(value)) out.push(key);
    }
    return out;
  }
}

/** A walk's completeness: every row once, as many as the table counts. */
export function walkProblems(seen: readonly string[], counted: number): { duplicates: number; missing: number } {
  const distinct = new Set(seen).size;
  return { duplicates: seen.length - distinct, missing: Math.max(0, counted - distinct) };
}

/** Rows created by a point: what `count()` gained must lie between its successes and successes + errors. */
export function createProblem(before: number, after: number, successes: number, errors: number): string | null {
  const gained = after - before;
  if (gained < successes) return `count gained ${gained} rows for ${successes} creates acknowledged: rows lost`;
  if (gained > successes + errors) return `count gained ${gained} rows for ${successes + errors} creates sent: rows invented`;
  return null;
}
