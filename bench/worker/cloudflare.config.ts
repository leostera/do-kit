// The load and split benchmark Worker, driven by bench/run.ts. Only on a custom domain
// (BENCH_HOSTNAME) behind an Access application, and with a bearer token (BENCH_TOKEN);
// workers.dev and preview URLs are off.
import { bindings, defineConfig, exports } from "cf/config";
import * as gone from "../teardown/gone.ts" with { type: "cf-worker" };
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = process.env.BENCH_WORKER_NAME ?? "do-kit-bench";
const HOSTNAME = process.env.BENCH_HOSTNAME;
// Deployed with BENCH_TEARDOWN=1 before the Worker is deleted: every class deleted, with its storage.
const TEARDOWN = process.env.BENCH_TEARDOWN === "1";

const EXPORTS = {
  BenchDriver: exports.durableObject({ storage: "sqlite" }),
  LeasedSource: exports.durableObject({ storage: "sqlite" }),
  LeasedRelay: exports.durableObject({ storage: "sqlite" }),
  SplitBenchJob: exports.durableObject({ storage: "sqlite" }),
  PresplitObject: exports.durableObject({ storage: "sqlite" }),
  WidePresplitObject: exports.durableObject({ storage: "sqlite" }),
  BenchFleet: exports.durableObject({ storage: "sqlite" }),
  Table1: exports.durableObject({ storage: "sqlite" }),
  Table4: exports.durableObject({ storage: "sqlite" }),
  Table16: exports.durableObject({ storage: "sqlite" }),
  Table64: exports.durableObject({ storage: "sqlite" }),
  Indexed1: exports.durableObject({ storage: "sqlite" }),
  Indexed4: exports.durableObject({ storage: "sqlite" }),
  Indexed16: exports.durableObject({ storage: "sqlite" }),
  Index1: exports.durableObject({ storage: "sqlite" }),
  Index4: exports.durableObject({ storage: "sqlite" }),
  Index16: exports.durableObject({ storage: "sqlite" }),
  SingleObject: exports.durableObject({ storage: "sqlite" }),
  KvServer: exports.durableObject({ storage: "sqlite" }),
  KvMachine: exports.durableObject({ storage: "sqlite" }),
  SplitObject: exports.durableObject({ storage: "sqlite" }),
  LegacySplitObject: exports.durableObject({ storage: "sqlite" }),
  ManualSplitObject: exports.durableObject({ storage: "sqlite" }),
  ManualLegacySplitObject: exports.durableObject({ storage: "sqlite" }),
  ManualTwoCopySplitObject: exports.durableObject({ storage: "sqlite" }),
};

/** Every class of `exports`, deleted: its objects and their storage go with the next deploy. */
function deleted(all: Record<string, unknown>) {
  return Object.fromEntries(Object.keys(all).map((name) => [name, exports.durableObject({ state: "deleted" })]));
}

export default defineConfig(() => ({
  worker: {
    name: NAME,
    domains: HOSTNAME ? [HOSTNAME] : [],
    workersDev: false,
    previewUrls: false,
    compatibilityDate: "2026-10-29",
    compatibilityFlags: ["nodejs_compat", "durable_object_snapshots"],
    // A split benchmark runs writers for minutes in one request, and a driver runs all of a
    // point's calls in one request: a listing of a 64-shard table is up to 192 calls.
    limits: { cpuMs: 300_000, subrequests: 1_000_000 },
    entrypoint: TEARDOWN ? gone : entrypoint,
    env: TEARDOWN ? {} : {
      BENCH_DRIVERS: bindings.durableObject({ worker: NAME, exportName: "BenchDriver" }),
      LEASED_SOURCE: bindings.durableObject({ worker: NAME, exportName: "LeasedSource" }),
      LEASED_RELAY: bindings.durableObject({ worker: NAME, exportName: "LeasedRelay" }),
      SPLIT_JOBS: bindings.durableObject({ worker: NAME, exportName: "SplitBenchJob" }),
      PRESPLIT: bindings.durableObject({ worker: NAME, exportName: "PresplitObject" }),
      PRESPLIT_4096: bindings.durableObject({ worker: NAME, exportName: "WidePresplitObject" }),
      BENCH_FLEETS: bindings.durableObject({ worker: NAME, exportName: "BenchFleet" }),
      TABLE_1: bindings.durableObject({ worker: NAME, exportName: "Table1" }),
      TABLE_4: bindings.durableObject({ worker: NAME, exportName: "Table4" }),
      TABLE_16: bindings.durableObject({ worker: NAME, exportName: "Table16" }),
      TABLE_64: bindings.durableObject({ worker: NAME, exportName: "Table64" }),
      INDEXED_1: bindings.durableObject({ worker: NAME, exportName: "Indexed1" }),
      INDEXED_4: bindings.durableObject({ worker: NAME, exportName: "Indexed4" }),
      INDEXED_16: bindings.durableObject({ worker: NAME, exportName: "Indexed16" }),
      INDEX_1: bindings.durableObject({ worker: NAME, exportName: "Index1" }),
      INDEX_4: bindings.durableObject({ worker: NAME, exportName: "Index4" }),
      INDEX_16: bindings.durableObject({ worker: NAME, exportName: "Index16" }),
      SINGLE: bindings.durableObject({ worker: NAME, exportName: "SingleObject" }),
      KV_SERVER: bindings.durableObject({ worker: NAME, exportName: "KvServer" }),
      KV_MACHINE: bindings.durableObject({ worker: NAME, exportName: "KvMachine" }),
      SPLIT: bindings.durableObject({ worker: NAME, exportName: "SplitObject" }),
      LEGACY: bindings.durableObject({ worker: NAME, exportName: "LegacySplitObject" }),
      SPLIT_MANUAL: bindings.durableObject({ worker: NAME, exportName: "ManualSplitObject" }),
      LEGACY_MANUAL: bindings.durableObject({ worker: NAME, exportName: "ManualLegacySplitObject" }),
      TWO_COPY_MANUAL: bindings.durableObject({ worker: NAME, exportName: "ManualTwoCopySplitObject" }),
      BENCH_TOKEN: bindings.secret(),
    },
    exports: TEARDOWN ? deleted(EXPORTS) : EXPORTS,
  },
}));
