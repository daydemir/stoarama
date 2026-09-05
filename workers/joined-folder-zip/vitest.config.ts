import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      "./crc32.wasm": fileURLToPath(new URL("./test/crc32-wasm.ts", import.meta.url)),
    },
  },
  test: { include: ["test/zip-node.test.ts"] },
});
