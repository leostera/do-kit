import { bindings, defineConfig, exports } from "cf/config";
import * as entrypoint from "./src/index.ts" with { type: "cf-worker" };

const NAME = "do-kit-example-accounts";

export default defineConfig(() => ({
  worker: {
    name: NAME,
    // Never on workers.dev or preview URLs: deploy it on a custom domain behind Cloudflare Access.
    workersDev: false,
    previewUrls: false,
    compatibilityDate: "2026-10-27",
    // Snapshots make splits fast on Cloudflare; locally, splits copy rows instead.
    compatibilityFlags: ["nodejs_compat", "durable_object_snapshots"],
    entrypoint,
    // The page is static; the API under /api is the Worker.
    assets: { notFoundHandling: "single-page-application", runWorkerFirst: ["/api/*"] },
    env: {
      ACCOUNTS: bindings.durableObject({ worker: NAME, exportName: "Accounts" }),
      ACCOUNT_INDEXES: bindings.durableObject({ worker: NAME, exportName: "AccountIndexes" }),
    },
    exports: { Accounts: exports.durableObject({ storage: "sqlite" }), AccountIndexes: exports.durableObject({ storage: "sqlite" }) },
  },
}));
