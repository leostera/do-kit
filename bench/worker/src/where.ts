// Where a Durable Object runs: its isolate (a module-level ID, one per isolate) and colo, and when
// a fixed burn of CPU started and ended there. Asked of many objects at once, objects that share
// an isolate share its one thread: their burns run one after another, not side by side.
// Made on first use: random values are not allowed at global scope.
let isolate: string | null = null;
let colo: string | null = null;

export interface Where {
  readonly isolate: string;
  readonly colo: string | null;
  /** Wall-clock times (ms) at which the burn started and ended. */
  readonly burnStart: number;
  readonly burnEnd: number;
}

/** The clock moves only with I/O: a zero timer brings it up to date. */
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

export async function where(burnIterations: number): Promise<Where> {
  if (colo === null) {
    try {
      colo = /^colo=(\w+)$/m.exec(await (await fetch("https://cloudflare.com/cdn-cgi/trace")).text())?.[1] ?? null;
    } catch {
      colo = null;
    }
  }
  await tick();
  const burnStart = Date.now();
  let x = 0;
  for (let i = 0; i < burnIterations; i++) x = (x * 31 + i) | 0;
  if (x === 42) console.log("");
  await tick();
  isolate ??= crypto.randomUUID().slice(0, 8);
  return { isolate, colo, burnStart, burnEnd: Date.now() };
}
