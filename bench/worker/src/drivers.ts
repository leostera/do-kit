// How a point's load is generated: how many drivers it needs, and the open-loop schedule.
// Pure (no Workers APIs), so the tests check it directly.

/** Calls a driver keeps in flight at most, closed loop: about what one completes well. */
export const CALLS_PER_DRIVER = 16;
/** A send this late, or not made at all (every lane busy), was the driver's fault, not the target's. */
export const LATE_MS = 50;
/** Past this share of late or skipped sends, the point measured its drivers: `driverLimited`. */
export const DRIVER_LIMITED_SHARE = 0.01;

/**
 * The calls to Durable Objects one operation makes, so drivers scale with calls rather than
 * operations: a driver completes about the same calls a second whatever they are.
 */
export function callsPerOperation(operation: string, target: string): number {
  const shards = Number(/-(\d+)$/.exec(target)?.[1] ?? 1);
  const indexed = target.startsWith("indexed-");
  if (operation === "list" && target.startsWith("table-")) return 3 * shards; // a query-order page: rounds over every shard
  if (operation === "create" && indexed) return 2; // reserve, then write
  if (operation === "rmw") return 2; // get, then a conditional put (more on conflict)
  const atomic = /^atomic-(\d+)$/.exec(operation);
  if (atomic) return Number(atomic[1]) + 1; // a prepare on each shard, then the commit
  const multi = /^multi-(\d+)$/.exec(operation);
  if (multi) return Number(multi[1]);
  return 1;
}

/** Drivers for a point: enough that none keeps more than CALLS_PER_DRIVER calls in flight. */
export function driversFor(concurrency: number, callsPerOp: number, maxDrivers: number): number {
  return Math.min(maxDrivers, concurrency, Math.max(Math.min(16, concurrency), Math.ceil((concurrency * callsPerOp) / CALLS_PER_DRIVER)));
}

/** How a driver kept an open-loop schedule. */
export interface Schedule {
  /** Sends the schedule asked for. */
  readonly due: number;
  /** Sends made. */
  readonly sent: number;
  /** Sends made more than LATE_MS after they were due. */
  readonly late: number;
  /** Sends not made: every lane was busy. */
  readonly skipped: number;
}

/** Schedules merged across drivers, and whether the point measured its drivers. */
export function scheduleOf(schedules: readonly Schedule[]): Schedule & { readonly driverLimited: boolean } {
  const sum = (f: (s: Schedule) => number) => schedules.reduce((n, s) => n + f(s), 0);
  const due = sum((s) => s.due);
  const late = sum((s) => s.late);
  const skipped = sum((s) => s.skipped);
  return { due, sent: sum((s) => s.sent), late, skipped, driverLimited: due > 0 && (late + skipped) / due > DRIVER_LIMITED_SHARE };
}

/**
 * Sends calls at `perSecond` from `startedAt` to `stopAt`, whatever their answers: each to a free
 * lane (a lane makes one call at a time), skipped when none is free. Waits for the calls sent.
 * `sleep` and `now` are given so a test can run the schedule on a clock of its own.
 */
export async function openLoop(
  lanes: readonly (() => Promise<void>)[],
  perSecond: number,
  startedAt: number,
  stopAt: number,
  clock: { now(): number; sleep(ms: number): Promise<void> } = { now: () => Date.now(), sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)) },
): Promise<Schedule> {
  const free = [...lanes];
  const running = new Set<Promise<void>>();
  const interval = 1000 / perSecond;
  let due = 0;
  let sent = 0;
  let late = 0;
  let skipped = 0;
  for (;;) {
    const now = clock.now();
    if (now >= stopAt) break;
    // Every send due by now: at startedAt, startedAt + interval, ...
    const dueNow = Math.min(Math.floor((now - startedAt) / interval) + 1, Math.ceil((stopAt - startedAt) / interval));
    for (; due < dueNow; due++) {
      const call = free.pop();
      if (!call) {
        skipped++;
        continue;
      }
      if (now - (startedAt + due * interval) > LATE_MS) late++;
      sent++;
      const done: Promise<void> = call()
        .catch(() => {})
        .finally(() => {
          running.delete(done);
          free.push(call);
        });
      running.add(done);
    }
    const next = startedAt + due * interval;
    if (next >= stopAt) break;
    await clock.sleep(Math.max(1, next - clock.now()));
  }
  await Promise.all(running);
  return { due, sent, late, skipped };
}
