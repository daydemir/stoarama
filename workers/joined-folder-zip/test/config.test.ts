import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";

describe("least-privilege deployment", () => {
  it("has no R2 binding or storage credential configuration", () => {
    const config = JSON.parse(readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8"));
    expect(config.r2_buckets).toBeUndefined();
    expect(config.secrets.required).toEqual(["BACKEND_WORKER_TOKEN"]);
    expect(JSON.stringify(config)).not.toMatch(/access.key|secret.access|bucket/i);
  });
});
