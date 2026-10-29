import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = process.env.BENCH_WORKER_NAME ?? "do-kit-bench";

export default defineConfig(() => ({
  worker: {
    name: NAME,
    compatibilityDate: "2026-10-29",
    compatibilityFlags: ["nodejs_compat", "durable_object_snapshots"],
    // A split benchmark runs writers for minutes in one request.
    limits: { cpuMs: 300_000 },
    entrypoint,
    env: {
      BENCH_DRIVERS: bindings.durableObject({ worker: NAME, exportName: "BenchDriver" }),
      SPLIT_JOBS: bindings.durableObject({ worker: NAME, exportName: "SplitBenchJob" }),
      PRESPLIT: bindings.durableObject({ worker: NAME, exportName: "PresplitObject" }),
      PRESPLIT_4096: bindings.durableObject({ worker: NAME, exportName: "WidePresplitObject" }),
      BENCH_FLEETS: bindings.durableObject({ worker: NAME, exportName: "BenchFleet" }),
      TABLE_1: bindings.durableObject({ worker: NAME, exportName: "Table1" }),
      TABLE_4: bindings.durableObject({ worker: NAME, exportName: "Table4" }),
      TABLE_16: bindings.durableObject({ worker: NAME, exportName: "Table16" }),
      TABLE_64: bindings.durableObject({ worker: NAME, exportName: "Table64" }),
      SINGLE: bindings.durableObject({ worker: NAME, exportName: "SingleObject" }),
      SPLIT: bindings.durableObject({ worker: NAME, exportName: "SplitObject" }),
      LEGACY: bindings.durableObject({ worker: NAME, exportName: "LegacySplitObject" }),
      SPLIT_MANUAL: bindings.durableObject({ worker: NAME, exportName: "ManualSplitObject" }),
      LEGACY_MANUAL: bindings.durableObject({ worker: NAME, exportName: "ManualLegacySplitObject" }),
      TWO_COPY_MANUAL: bindings.durableObject({ worker: NAME, exportName: "ManualTwoCopySplitObject" }),
    },
    exports: {
      BenchDriver: exports.durableObject({ storage: "sqlite" }),
      SplitBenchJob: exports.durableObject({ storage: "sqlite" }),
      PresplitObject: exports.durableObject({ storage: "sqlite" }),
      WidePresplitObject: exports.durableObject({ storage: "sqlite" }),
      BenchFleet: exports.durableObject({ storage: "sqlite" }),
      Table1: exports.durableObject({ storage: "sqlite" }),
      Table4: exports.durableObject({ storage: "sqlite" }),
      Table16: exports.durableObject({ storage: "sqlite" }),
      Table64: exports.durableObject({ storage: "sqlite" }),
      SingleObject: exports.durableObject({ storage: "sqlite" }),
      SplitObject: exports.durableObject({ storage: "sqlite" }),
      LegacySplitObject: exports.durableObject({ storage: "sqlite" }),
      ManualSplitObject: exports.durableObject({ storage: "sqlite" }),
      ManualLegacySplitObject: exports.durableObject({ storage: "sqlite" }),
      ManualTwoCopySplitObject: exports.durableObject({ storage: "sqlite" }),
      // Its earlier name, retired.
      ManualRfd18SplitObject: exports.durableObject({ state: "deleted" }),
    },
  },
}));
