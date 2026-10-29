// A Worker for testing splittable objects under cf dev: two classes, one split by hand and one
// by load (a low threshold), driven over HTTP by the tests.
import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = "splittable-object-test";

export default defineConfig(() => ({
  worker: {
    name: NAME,
    compatibilityDate: "2026-10-27",
    compatibilityFlags: ["nodejs_compat"],
    entrypoint,
    env: {
      COUNTERS: bindings.durableObject({ worker: NAME, exportName: "Counters" }),
      AUTO_COUNTERS: bindings.durableObject({ worker: NAME, exportName: "AutoCounters" }),
      PRESPLIT_COUNTERS: bindings.durableObject({ worker: NAME, exportName: "PresplitCounters" }),
    },
    exports: {
      Counters: exports.durableObject({ storage: "sqlite" }),
      AutoCounters: exports.durableObject({ storage: "sqlite" }),
      PresplitCounters: exports.durableObject({ storage: "sqlite" }),
    },
  },
}));
