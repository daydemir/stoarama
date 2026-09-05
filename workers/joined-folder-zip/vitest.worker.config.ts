import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: { bindings: { BACKEND_WORKER_TOKEN: "local-test-worker-token-at-least-32-bytes" } },
    }),
  ],
  test: { include: ["test/zip.test.ts"] },
});
