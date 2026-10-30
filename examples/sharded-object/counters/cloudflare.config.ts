import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = "do-kit-example-counters";

export default defineConfig(() => ({
  worker: {
    name: NAME,
    compatibilityDate: "2026-10-27",
    // Snapshots make splits fast on Cloudflare; locally, splits copy rows instead.
    compatibilityFlags: ["nodejs_compat", "durable_object_snapshots"],
    entrypoint,
    // The page is static; the API under /api is the Worker.
    assets: { notFoundHandling: "single-page-application", runWorkerFirst: ["/api/*"] },
    env: { COUNTERS: bindings.durableObject({ worker: NAME, exportName: "Counters" }) },
    exports: { Counters: exports.durableObject({ storage: "sqlite" }) },
  },
}));
