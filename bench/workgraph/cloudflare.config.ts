// The work graph benchmark Worker (RFD0005): a coordinator, runners and a results store, driven by
// bench/workgraph/run.ts. Only on its custom domain, behind the account's Access application, and
// with a bearer token (BENCH_TOKEN); workers.dev and preview URLs are off.
import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = "do-kit-workgraph-bench";
export const HOSTNAME = "do-kit-workgraph.lostera.me.cloudflare.dev";

export default defineConfig(() => ({
  worker: {
    name: NAME,
    domains: [HOSTNAME],
    workersDev: false,
    previewUrls: false,
    compatibilityDate: "2026-10-27",
    compatibilityFlags: ["nodejs_compat"],
    entrypoint,
    // A chunk gets one invocation's CPU: up to 5 minutes.
    limits: { cpuMs: 300_000 },
    env: {
      COORDINATORS: bindings.durableObject({ worker: NAME, exportName: "BenchCoordinator" }),
      RUNNERS: bindings.durableObject({ worker: NAME, exportName: "BenchRunner" }),
      RESULTS: bindings.durableObject({ worker: NAME, exportName: "BenchResults" }),
      BENCH_TOKEN: bindings.secret(),
    },
    exports: {
      BenchCoordinator: exports.durableObject({ storage: "sqlite" }),
      BenchRunner: exports.durableObject({ storage: "sqlite" }),
      BenchResults: exports.durableObject({ storage: "sqlite" }),
    },
    observability: { enabled: true, logs: { enabled: true, invocationLogs: false } },
  },
}));
