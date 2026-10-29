// Starts the test Worker under cf dev (vite) for a test file, and talks to it over HTTP. With
// TEST_URL set, talks to that deployed Worker instead (where splits use real storage snapshots),
// sending TEST_TOKEN as a bearer token and TEST_ACCESS_TOKEN, if set, to pass Cloudflare Access.
import type { NodeDescription } from "../src/index.ts";
import { afterAll, beforeAll } from "bun:test";
import { join } from "node:path";

let base = "";

/** Starts the Worker before this file's tests and stops it after them. */
export function useWorker(): void {
  let stop = async () => {};
  beforeAll(async () => {
    if (process.env.TEST_URL) {
      base = process.env.TEST_URL.replace(/\/$/, "");
      return;
    }
    const probe = Bun.serve({ port: 0, fetch: () => new Response() });
    const port = probe.port!;
    probe.stop(true);
    const dir = join(import.meta.dir, "worker");
    const child = Bun.spawn([join(import.meta.dir, "../node_modules/.bin/vite"), "--mode", "test", "--port", String(port), "--strictPort"], { cwd: dir, stdout: "ignore", stderr: "ignore", env: { ...process.env, NO_COLOR: "1" } });
    base = `http://localhost:${port}`;
    for (let i = 0; ; i++) {
      try {
        if ((await fetch(`${base}/splits`, { method: "POST", body: JSON.stringify({ cls: "m", name: "probe" }) })).ok) break;
      } catch {
        // not up yet
      }
      if (i > 300) throw new Error("cf dev did not start");
      await Bun.sleep(100);
    }
    stop = async () => {
      child.kill();
      await child.exited;
    };
  }, 60_000);
  afterAll(() => stop());
}

/** The test Worker's URL. */
export const workerUrl = (): string => base;

/** Headers for the test Worker: its token and an Access token, when it is deployed. */
export function requestHeaders(): Record<string, string> {
  const headers: Record<string, string> = {};
  if (process.env.TEST_TOKEN) headers.authorization = `Bearer ${process.env.TEST_TOKEN}`;
  if (process.env.TEST_ACCESS_TOKEN) headers["cf-access-token"] = process.env.TEST_ACCESS_TOKEN;
  return headers;
}

export async function post<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(base + path, { method: "POST", body: JSON.stringify(body), headers: requestHeaders() });
  if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);
  return (await response.json()) as T;
}

export const call = <T>(cls: string, name: string, method: string, keys: string[], payloads: unknown[] | null = null) => post<T>("/call", { cls, name, method, keys, payloads });
export const describe = (cls: string, name: string, bits: string) => post<NodeDescription>("/describe", { cls, name, bits });

/** Shards reachable from the root, by asking objects (not the client's map). */
export async function shards(cls: string, name: string, bits = ""): Promise<string[]> {
  const node = await describe(cls, name, bits);
  // The object hosting `bits` hosts a deeper shard once `bits` has split.
  if (node.state === "new" || node.bits === bits) return [bits];
  return [...(await shards(cls, name, `${bits}0`)), ...(await shards(cls, name, `${bits}1`))];
}
