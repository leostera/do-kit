// Ridiculous benchmark: 4,096 presplit shards (target "presplit-4096").
// BENCH_URL=… BENCH_TOKEN=… [BENCH_ACCESS_TOKEN=…] OUT=results.json [OPS=write,read] [WRITE_LEVELS=…] [READ_LEVELS=…] [OBJECT=…] bun bench/wide.ts
// Each level warms until throughput stops improving
// (clients learn the 4,095 split points lazily), then is measured once.
import { writeFileSync } from "node:fs";

const url = `${process.env.BENCH_URL!.replace(/\/$/, "")}/measure`;
const token = process.env.BENCH_TOKEN!;
const access = process.env.BENCH_ACCESS_TOKEN;
const levelsFor = (op: string) => (process.env[`${op.toUpperCase()}_LEVELS`] ?? "4096,16384,65536,131072").split(",").map(Number);
const ops = (process.env.OPS ?? "write,read").split(",");
const out = process.env.OUT!;
const points: unknown[] = [];

async function measure(operation: string, concurrency: number, objectName: string): Promise<any> {
  for (let attempt = 1; ; attempt++) {
    // Seeding 4,096 shards takes a while, but an answer that never comes is a failed attempt, not a hang.
    let r: Response;
    let text: string;
    try {
      r = await fetch(url, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, ...(access ? { "cf-access-token": access } : {}), "content-type": "application/json" },
        body: JSON.stringify({ target: "presplit-4096", operation, distribution: "uniform", concurrency, durationMs: 10_000, keyCount: 100_000, objectName }),
        timeout: false,
        signal: AbortSignal.timeout(300_000),
      } as RequestInit);
      text = await r.text();
    } catch (error) {
      console.error(JSON.stringify({ failed: { operation, concurrency }, error: String(error) }));
      if (attempt >= 2) return null;
      continue;
    }
    try {
      const j = JSON.parse(text);
      if (j.operationsPerSecond !== undefined) return j;
    } catch {}
    console.error(JSON.stringify({ failed: { operation, concurrency }, status: r.status, ray: r.headers.get("cf-ray"), body: (text.match(/<title>[^<]*<\/title>|error code: \d+|Error \d+[^<]*/g) ?? [text.slice(0, 120)]).join(" | ") }));
    if (attempt >= 2) return null;
  }
}

const brief = (j: any) => ({ samples: (j.errorSamples ?? []).slice(0, 2), c: j.concurrency, drivers: j.driverCount, failedDrivers: j.failedDrivers, ops: Math.round(j.operationsPerSecond), errors: j.errors, p50: j.latencyMs.p50, p99: j.latencyMs.p99 });
const suite = `wide-${Date.now()}`;
for (const operation of ops) {
  // Reads use the object the writes filled, so they read real 1 KiB values.
  const objectName = process.env.OBJECT ?? `${suite}-uniform`;
  for (const concurrency of levelsFor(operation)) {
    let last = 0;
    for (let w = 1; w <= 6; w++) {
      const j = await measure(operation, concurrency, objectName);
      if (!j) break;
      console.log(JSON.stringify({ warmup: w, operation, ...brief(j) }));
      if (j.operationsPerSecond < last * 1.1) break;
      last = j.operationsPerSecond;
    }
    const j = await measure(operation, concurrency, objectName);
    if (!j) continue;
    points.push(j);
    writeFileSync(out, JSON.stringify({ generatedAt: new Date().toISOString(), suite, target: "presplit-4096", note: "each level warmed until throughput improved by under 10% (at most 6 warmups), then measured once for 10 s", points }, null, 2) + "\n");
    console.log(JSON.stringify({ MEASURED: true, operation, ...brief(j) }));
  }
}
