import { test } from "bun:test";
import { call, useWorker } from "./harness.ts";
useWorker();
for (const t of ["r", "o"]) for (const size of [100, 1024, 4096]) test(`probe ${t} ${size}`, async () => {
  const name = `probe-${t}-${size}-${Date.now()}`;
  const n = Math.round(40_000 * 1024 / size / 2);
  for (const op of ["fill", "fill", "half", "refill"]) console.log(t, size, JSON.stringify(await call("m", name, "sizeProbe", ["k"], [{ op: op + "-" + t, n, size }])));
}, 300_000);
