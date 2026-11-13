// A Worker for testing sharded objects under cf dev: two classes, one split by hand and one
// by load (a low threshold), driven over HTTP by the tests.
import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

// Deployed (to run the tests against real storage snapshots), it takes another name.
const NAME = process.env.TEST_WORKER_NAME ?? "do-kit-test";
/** Deployed, it is served only on this custom domain, which must be behind a Cloudflare Access application. */
const HOSTNAME = process.env.TEST_HOSTNAME;

export default defineConfig(() => ({
  worker: {
    name: NAME,
    // Never on workers.dev or preview URLs: deploy it on a custom domain behind Cloudflare Access.
    domains: HOSTNAME ? [HOSTNAME] : [],
    workersDev: false,
    previewUrls: false,
    compatibilityDate: "2026-10-27",
    compatibilityFlags: ["nodejs_compat", "durable_object_snapshots"],
    entrypoint,
    env: {
      COUNTERS: bindings.durableObject({ worker: NAME, exportName: "Counters" }),
      AUTO_COUNTERS: bindings.durableObject({ worker: NAME, exportName: "AutoCounters" }),
      PRESPLIT_COUNTERS: bindings.durableObject({ worker: NAME, exportName: "PresplitCounters" }),
      ROUTED_COUNTERS: bindings.durableObject({ worker: NAME, exportName: "RoutedCounters" }),
      ROUTED_DOCS: bindings.durableObject({ worker: NAME, exportName: "RoutedDocs" }),
      DOCS: bindings.durableObject({ worker: NAME, exportName: "Docs" }),
      EVOLVING: bindings.durableObject({ worker: NAME, exportName: "EvolvingDocs" }),
      FLAKY: bindings.durableObject({ worker: NAME, exportName: "FlakyDocs" }),
      OLD_ALARM: bindings.durableObject({ worker: NAME, exportName: "OldAlarm" }),
      USERS: bindings.durableObject({ worker: NAME, exportName: "Users" }),
      USER_INDEXES: bindings.durableObject({ worker: NAME, exportName: "UserIndexes" }),
      EVOLVING_USERS: bindings.durableObject({ worker: NAME, exportName: "EvolvingUsers" }),
      EVOLVING_USER_INDEXES: bindings.durableObject({ worker: NAME, exportName: "EvolvingUserIndexes" }),
      GRAPH_COORDINATORS: bindings.durableObject({ worker: NAME, exportName: "GraphCoordinators" }),
      GRAPH_RUNNERS: bindings.durableObject({ worker: NAME, exportName: "GraphRunners" }),
      GRAPH_RESULTS: bindings.durableObject({ worker: NAME, exportName: "GraphResults" }),
      CHANNELS: bindings.durableObject({ worker: NAME, exportName: "Channels" }),
      LOCKS: bindings.durableObject({ worker: NAME, exportName: "CodeLock" }),
      AUDITS: bindings.durableObject({ worker: NAME, exportName: "Audit" }),
      PLAINS: bindings.durableObject({ worker: NAME, exportName: "Plain" }),
      SEATS: bindings.durableObject({ worker: NAME, exportName: "Seat" }),
    },
    exports: {
      Counters: exports.durableObject({ storage: "sqlite" }),
      AutoCounters: exports.durableObject({ storage: "sqlite" }),
      PresplitCounters: exports.durableObject({ storage: "sqlite" }),
      RoutedCounters: exports.durableObject({ storage: "sqlite" }),
      RoutedDocs: exports.durableObject({ storage: "sqlite" }),
      Docs: exports.durableObject({ storage: "sqlite" }),
      EvolvingDocs: exports.durableObject({ storage: "sqlite" }),
      FlakyDocs: exports.durableObject({ storage: "sqlite" }),
      OldAlarm: exports.durableObject({ storage: "sqlite" }),
      Users: exports.durableObject({ storage: "sqlite" }),
      UserIndexes: exports.durableObject({ storage: "sqlite" }),
      EvolvingUsers: exports.durableObject({ storage: "sqlite" }),
      EvolvingUserIndexes: exports.durableObject({ storage: "sqlite" }),
      GraphCoordinators: exports.durableObject({ storage: "sqlite" }),
      GraphRunners: exports.durableObject({ storage: "sqlite" }),
      GraphResults: exports.durableObject({ storage: "sqlite" }),
      Channels: exports.durableObject({ storage: "sqlite" }),
      CodeLock: exports.durableObject({ storage: "sqlite" }),
      Audit: exports.durableObject({ storage: "sqlite" }),
      Plain: exports.durableObject({ storage: "sqlite" }),
      Seat: exports.durableObject({ storage: "sqlite" }),
    },
  },
}));
