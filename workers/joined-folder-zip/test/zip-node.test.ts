import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { createJoinedZip } from "../src/zip";
import { bucketFor, bytes, file, options } from "./helpers";

describe("ZIP64 interoperability", () => {
  it("produces an archive accepted by an independent unzip implementation", async () => {
    const entry = file();
    const archive = await createJoinedZip(bucketFor([entry]), [entry], options);
    const output = await bytes(archive.body);
    await archive.completed;
    const directory = mkdtempSync(join(tmpdir(), "stoarama-joined-zip-test-"));
    const archivePath = join(directory, "joined.zip");
    try {
      writeFileSync(archivePath, output);
      expect(execFileSync("/usr/bin/unzip", ["-t", archivePath], { encoding: "utf8" })).toContain("No errors detected");
    } finally {
      rmSync(directory, { recursive: true });
    }
  });
});
