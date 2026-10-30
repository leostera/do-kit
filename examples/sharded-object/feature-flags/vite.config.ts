import { cloudflare } from "@cloudflare/vite-plugin";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react(), tailwindcss(), cloudflare()],
  // In this repository only: use the library's source. In your own project, delete `resolve` and
  // install @leostera/do-kit.
  resolve: { alias: { "@leostera/do-kit": fileURLToPath(new URL("../../../src/index.ts", import.meta.url)) } },
});
