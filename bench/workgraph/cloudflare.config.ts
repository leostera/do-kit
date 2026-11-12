// The work graph benchmark Worker (RFD0005): a coordinator, runners and a results store, driven by
// bench/workgraph/run.ts. Only on a custom domain (BENCH_HOSTNAME) behind an Access application,
// and with a bearer token (BENCH_TOKEN); workers.dev and preview URLs are off.
import { bindings, defineConfig, exports } from "cf/config";
import * as gone from "../teardown/gone.ts" with { type: "cf-worker" };
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = process.env.BENCH_WORKER_NAME ?? "do-kit-workgraph-bench";
const HOSTNAME = process.env.BENCH_HOSTNAME;
// Deployed with BENCH_TEARDOWN=1 before the Worker is deleted: every class deleted, with its storage.
const TEARDOWN = process.env.BENCH_TEARDOWN === "1";
const CLASSES = ["BenchCoordinator", "BenchRunner", "BenchResults"] as const;

export default defineConfig(() => ({
  worker: {
    name: NAME,
    domains: HOSTNAME ? [HOSTNAME] : [],
    workersDev: false,
    previewUrls: false,
    compatibilityDate: "2026-10-27",
    compatibilityFlags: ["nodejs_compat"],
    entrypoint: TEARDOWN ? gone : entrypoint,
    // A chunk gets one invocation's CPU: up to 5 minutes.
    limits: { cpuMs: 300_000 },
    env: TEARDOWN ? {} : {
      COORDINATORS: bindings.durableObject({ worker: NAME, exportName: "BenchCoordinator" }),
      RUNNERS: bindings.durableObject({ worker: NAME, exportName: "BenchRunner" }),
      RESULTS: bindings.durableObject({ worker: NAME, exportName: "BenchResults" }),
      BENCH_TOKEN: bindings.secret(),
    },
    exports: Object.fromEntries(CLASSES.map((name) => [name, exports.durableObject(TEARDOWN ? { state: "deleted" } : { storage: "sqlite" })])),
    observability: { enabled: true, logs: { enabled: true, invocationLogs: false } },
  },
}));
