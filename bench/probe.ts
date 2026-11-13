// Probes for finding out why a benchmark behaves as it does, against a deployed bench Worker.
//
//   BENCH_URL=https://do-kit-bench.<your-zone> BENCH_TOKEN=… [BENCH_ACCESS_TOKEN=…] \
//     bun bench/probe.ts <command> [json]
//
// Commands:
//   placement {target, objectName, burnIterations?, drivers?}
//       Which isolate and colo each shard of a table (or each driver, target "drivers") runs in,
//       and when a fixed burn of CPU ran there. Objects that share an isolate share its one thread:
//       their burns run one after another.
//   runner-placement {run, runners, burnIterations?}   (on the work graph bench Worker)
//       The same, for a run's runners.
//   measure {target, operation, concurrency, durationSeconds, drivers?, …}
//       One /measure point. `drivers` overrides the default driver count, to tell the load
//       generator's limits from the target's.
//   timeline {run, runners, out?}   (on the work graph bench Worker)
//       Polls each runner's timeline (alarms, wakes, chunks started and computed) every 10 s until
//       the run ends, keeping each runner's longest: a done run wipes its runners.
//
// A Worker's error events: bench/errors.sh.
export {};

const [command, arg] = process.argv.slice(2);
const body = arg ? (JSON.parse(arg) as Record<string, unknown>) : {};

async function post<T>(path: string, payload: unknown): Promise<T> {
  const url = process.env.BENCH_URL?.replace(/\/$/, "");
  const token = process.env.BENCH_TOKEN;
  if (!url || !token) throw new Error("Set BENCH_URL and BENCH_TOKEN in the environment.");
  const access = process.env.BENCH_ACCESS_TOKEN;
  const response = await fetch(url + path, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json", ...(access ? { "cf-access-token": access } : {}) },
    body: JSON.stringify(payload),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${path}: ${response.status} ${text.slice(0, 300)}`);
  return JSON.parse(text) as T;
}

interface Placed {
  isolate: string;
  colo: string | null;
  burnStart: number;
  burnEnd: number;
  answeredAt: number;
}

function summarisePlacement(asked: number, objects: Placed[]): void {
  const byIsolate = new Map<string, Placed[]>();
  for (const o of objects) byIsolate.set(o.isolate, [...(byIsolate.get(o.isolate) ?? []), o]);
  console.log(`${objects.length} objects in ${byIsolate.size} isolates; colos ${[...new Set(objects.map((o) => o.colo))].sort().join(", ")}`);
  for (const [isolate, os] of byIsolate) console.log(`  ${isolate}: ${os.length} objects, burns ${os.map((o) => `${o.burnStart - asked}-${o.burnEnd - asked} ms`).join(" ")}`);
}

async function timeline(): Promise<void> {
  const out = String(body.out ?? "timeline.json");
  const best: Record<string, unknown[]> = {};
  for (;;) {
    const { timelines } = await post<{ timelines: unknown[][] }>("/timeline", { run: body.run, runners: body.runners });
    timelines.forEach((tl, i) => {
      if (tl.length >= (best[i]?.length ?? 0)) best[i] = tl;
    });
    await Bun.write(out, JSON.stringify(best));
    const status = await post<{ state: string }>("/status", { run: body.run }).catch(() => ({ state: "unknown" }));
    if (status.state !== "running" && status.state !== "unknown") break;
    await Bun.sleep(10_000);
  }
  console.log(`wrote ${out}`);
}

switch (command) {
  case "placement": {
    const { asked, objects } = await post<{ asked: number; objects: Placed[] }>("/placement", body);
    summarisePlacement(asked, objects);
    break;
  }
  case "runner-placement": {
    const { asked, objects } = await post<{ asked: number; objects: Placed[] }>("/placement", body);
    summarisePlacement(asked, objects);
    break;
  }
  case "measure": {
    const p = await post<{ target: string; operation: string; concurrency: number; driverCount?: number; operationsPerSecond: number | null; errors: number; latencyMs: { p50: number; p99: number }; errorSamples?: string[] }>("/measure", body);
    console.log(`${p.target} ${p.operation} c=${p.concurrency} drivers=${p.driverCount}: ${Math.round(p.operationsPerSecond ?? 0)}/s, ${p.errors} errors, p50 ${p.latencyMs.p50} ms, p99 ${p.latencyMs.p99} ms`);
    for (const sample of p.errorSamples?.slice(0, 3) ?? []) console.log(`  ${sample}`);
    break;
  }
  case "timeline":
    await timeline();
    break;
  default:
    throw new Error("usage: bun bench/probe.ts placement|runner-placement|measure|timeline '<json>'");
}
