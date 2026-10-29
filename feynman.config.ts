import { defineConfig } from "@leostera/feynman/config";

export default defineConfig({
  specs: ["specs/*.ts"],
  check: { depth: 40 },
});
