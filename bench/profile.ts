// A CPU profile of the local bench Worker (cf dev), by function: where a workload's time goes.
//
//   bun bench/profile.ts <seconds> [inspector-port]      (start the load right after)
//
// Prints the functions with the most self time, and the same folded by source file. Only this
// repository's code and the runtime's own are in it: SQLite and I/O show as the calls that reach them.
const seconds = Number(process.argv[2] ?? 10);
const port = Number(process.argv[3] ?? 9229);
const targets = (await (await fetch(`http://localhost:${port}/json`)).json()) as { webSocketDebuggerUrl: string }[];
if (targets.length === 0) throw new Error(`no inspector target on port ${port}`);
const ws = new WebSocket(targets[0]!.webSocketDebuggerUrl);
let id = 0;
const waiting = new Map<number, (result: unknown) => void>();
ws.onmessage = (event) => {
  const msg = JSON.parse(String(event.data)) as { id?: number; result?: unknown; error?: unknown };
  if (msg.id !== undefined) waiting.get(msg.id)?.(msg.error ? { error: msg.error } : msg.result);
};
await new Promise((resolve) => (ws.onopen = resolve));
const send = <T>(method: string, params: unknown = {}): Promise<T> => new Promise((resolve) => (waiting.set(++id, resolve as never), ws.send(JSON.stringify({ id, method, params }))));

interface ProfileNode {
  id: number;
  callFrame: { functionName: string; url: string; lineNumber: number };
  children?: number[];
}
await send("Profiler.enable");
await send("Profiler.setSamplingInterval", { interval: 200 });
await send("Profiler.start");
console.error(`profiling for ${seconds} s: run the load now`);
await Bun.sleep(seconds * 1000);
const { profile } = await send<{ profile: { nodes: ProfileNode[]; samples: number[]; timeDeltas: number[] } }>("Profiler.stop");
ws.close();

const selfTime = new Map<number, number>();
profile.samples.forEach((node, i) => selfTime.set(node, (selfTime.get(node) ?? 0) + (profile.timeDeltas[i] ?? 0)));
const byFunction = new Map<string, number>();
const byFile = new Map<string, number>();
let total = 0;
for (const node of profile.nodes) {
  const us = selfTime.get(node.id) ?? 0;
  if (us === 0) continue;
  const file = node.callFrame.url.replace(/\?.*$/, "").split("/").slice(-3).join("/") || "(runtime)";
  const name = `${node.callFrame.functionName || "(anonymous)"}  ${file}:${node.callFrame.lineNumber + 1}`;
  if (name.startsWith("(idle)") || name.startsWith("(program)")) continue;
  total += us;
  byFunction.set(name, (byFunction.get(name) ?? 0) + us);
  byFile.set(file, (byFile.get(file) ?? 0) + us);
}
const topOf = (m: Map<string, number>, n: number) => [...m].sort((a, b) => b[1] - a[1]).slice(0, n);
console.log(`busy: ${(total / 1000).toFixed(0)} ms of ${seconds * 1000} ms`);
console.log("\nby function (self time):");
for (const [name, us] of topOf(byFunction, 30)) console.log(`${((us / total) * 100).toFixed(1).padStart(5)}%  ${(us / 1000).toFixed(0).padStart(6)} ms  ${name}`);
console.log("\nby file:");
for (const [name, us] of topOf(byFile, 12)) console.log(`${((us / total) * 100).toFixed(1).padStart(5)}%  ${(us / 1000).toFixed(0).padStart(6)} ms  ${name}`);

export {};
