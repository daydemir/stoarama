import { describe, expect, it, vi } from "vitest";
import { createJoinedZip, type JoinedFile } from "../src/zip";

const file = (changes: Partial<JoinedFile> = {}): JoinedFile => ({
  artifact_id: "artifact-42",
  batch_id: "batch-7",
  sha256: "a".repeat(64),
  etag: "etag-a",
  version_id: "version-a",
  size_bytes: 3,
  relative_path: "camera/clip.mp4",
  content_type: "video/mp4",
  ...changes,
});

function objectFor(entry: JoinedFile, body = new Uint8Array([1, 2, 3])) {
  return {
    key: `joined/${entry.batch_id}/objects/${entry.sha256}.mp4`,
    size: entry.size_bytes,
    etag: entry.etag,
    version: entry.version_id,
    body: new ReadableStream({ start(controller) { controller.enqueue(body); controller.close(); } }),
  };
}

function bucketFor(entries: JoinedFile[]) {
  const objects = new Map(entries.map((entry) => [
    `joined/${entry.batch_id}/objects/${entry.sha256}.mp4`, objectFor(entry),
  ]));
  return {
    head: vi.fn(async (key: string) => objects.get(key) ?? null),
    get: vi.fn(async (key: string) => objects.get(key) ?? null),
  };
}

async function bytes(stream: ReadableStream<Uint8Array>) {
  const chunks: Uint8Array[] = [];
  const reader = stream.getReader();
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    chunks.push(result.value);
  }
  const output = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.length; }
  return output;
}

function signatures(value: Uint8Array) {
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const found: number[] = [];
  for (let index = 0; index <= value.length - 4; index++) {
    const signature = view.getUint32(index, true);
    if ([0x04034b50, 0x08074b50, 0x02014b50, 0x06064b50, 0x07064b50, 0x06054b50].includes(signature)) found.push(signature);
  }
  return found;
}

describe("createJoinedZip", () => {
  it("streams nested files, a safe manifest, data descriptors, and Zip64 records", async () => {
    const entry = file();
    const bucket = bucketFor([entry]);
    const output = await bytes((await createJoinedZip(bucket, [entry])).body);
    const text = new TextDecoder().decode(output);

    expect(text).toContain("joined-files.json");
    expect(text).toContain("camera/clip.mp4");
    expect(text).toContain('"artifact_id": "artifact-42"');
    expect(text).not.toContain('"r2_key"');
    expect(text).not.toContain('"batch_id"');
    expect(text).not.toContain('"etag"');
    expect(text).not.toContain('"version_id"');
    expect(signatures(output)).toEqual(expect.arrayContaining([
      0x04034b50, 0x08074b50, 0x02014b50, 0x06064b50, 0x07064b50, 0x06054b50,
    ]));
    expect(bucket.get).toHaveBeenCalledWith(
      `joined/${entry.batch_id}/objects/${entry.sha256}.mp4`,
      { onlyIf: { etagMatches: entry.etag } },
    );
  });

  it("represents a file larger than 4 GiB using Zip64 without allocating its size", async () => {
    const huge = file({ size_bytes: 0x1_0000_0000 + 9 });
    const bucket = bucketFor([huge]);
    const object = objectFor(huge);
    object.body = new ReadableStream({ pull(controller) { controller.error(new Error("synthetic stop")); } });
    bucket.head.mockResolvedValue(object);
    bucket.get.mockResolvedValue(object);
    const reader = (await createJoinedZip(bucket, [huge])).body.getReader();
    const chunks: Uint8Array[] = [];
    await expect((async () => {
      while (true) {
        const result = await reader.read();
        if (result.done) break;
        chunks.push(result.value);
      }
    })()).rejects.toThrow("synthetic stop");
    const output = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
    let offset = 0;
    for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.length; }
    const zip64Size = new Uint8Array([9, 0, 0, 0, 1, 0, 0, 0]);

    expect(bucket.head).toHaveBeenCalledOnce();
    expect(bucket.get).toHaveBeenCalledOnce();
    expect(findBytes(output, zip64Size)).toBeGreaterThanOrEqual(0);
  });

  it("computes the standard CRC32 across many source chunks", async () => {
    const entry = file({ size_bytes: 9 });
    const object = objectFor(entry);
    const source = new TextEncoder().encode("123456789");
    object.body = new ReadableStream({
      start(controller) {
        for (const byte of source) controller.enqueue(Uint8Array.of(byte));
        controller.close();
      },
    });
    const bucket = bucketFor([entry]);
    bucket.head.mockResolvedValue(object);
    bucket.get.mockResolvedValue(object);
    const output = await bytes((await createJoinedZip(bucket, [entry])).body);
    const descriptors = signatureOffsets(output, 0x08074b50);
    const mediaDescriptor = descriptors.at(-1);

    expect(mediaDescriptor).toBeDefined();
    expect(new DataView(output.buffer, output.byteOffset, output.byteLength).getUint32((mediaDescriptor ?? 0) + 4, true)).toBe(0xcbf43926);
  });

  it("rejects an identity mismatch during preflight before starting any gets", async () => {
    const entry = file();
    const bucket = bucketFor([entry]);
    bucket.head.mockResolvedValueOnce({ ...objectFor(entry), etag: "other" });

    await expect(createJoinedZip(bucket, [entry])).rejects.toThrow(/identity mismatch.*etag/i);
    expect(bucket.get).not.toHaveBeenCalled();
  });

  it.each([
    "../escape.mp4", "/absolute.mp4", "dir\\clip.mp4", "./clip.mp4",
    "C:/clip.mp4", "dir/con.txt", "dir/trailing. /clip.mp4",
  ])("rejects unsafe portable path %s", async (relative_path) => {
    const entry = file({ relative_path });
    await expect(createJoinedZip(bucketFor([entry]), [entry])).rejects.toThrow(/unsafe.*path/i);
  });

  it("rejects paths that collide on case-insensitive filesystems", async () => {
    const first = file({ relative_path: "A/clip.mp4" });
    const second = file({ sha256: "b".repeat(64), relative_path: "a/CLIP.mp4" });
    await expect(createJoinedZip(bucketFor([first, second]), [first, second])).rejects.toThrow(/collision/i);
  });

  it("cancels the current R2 body and never writes an end record", async () => {
    const entry = file({ size_bytes: 6 });
    const cancel = vi.fn();
    const object = objectFor(entry);
    object.body = new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel,
    });
    const bucket = bucketFor([entry]);
    bucket.head.mockResolvedValue(object);
    bucket.get.mockResolvedValue(object);
    const reader = (await createJoinedZip(bucket, [entry])).body.getReader();
    while (bucket.get.mock.calls.length === 0) await reader.read();
    await reader.read();
    await reader.cancel("stop");

    await vi.waitFor(() => expect(cancel).toHaveBeenCalled());
  });

  it("errors the stream and omits the end records when an R2 read fails", async () => {
    const entry = file();
    const object = objectFor(entry);
    object.body = new ReadableStream({ pull(controller) { controller.error(new Error("r2 read failed")); } });
    const bucket = bucketFor([entry]);
    bucket.head.mockResolvedValue(object);
    bucket.get.mockResolvedValue(object);
    const reader = (await createJoinedZip(bucket, [entry])).body.getReader();
    const seen: Uint8Array[] = [];
    await expect((async () => {
      while (true) {
        const result = await reader.read();
        if (result.done) return;
        seen.push(result.value);
      }
    })()).rejects.toThrow("r2 read failed");
    expect(signatures(await bytes(new ReadableStream({ start(c) { for (const chunk of seen) c.enqueue(chunk); c.close(); } })))).not.toContain(0x06054b50);
  });
});

function findBytes(haystack: Uint8Array, needle: Uint8Array): number {
  outer: for (let offset = 0; offset <= haystack.length - needle.length; offset++) {
    for (let index = 0; index < needle.length; index++) if (haystack[offset + index] !== needle[index]) continue outer;
    return offset;
  }
  return -1;
}

function signatureOffsets(value: Uint8Array, signature: number): number[] {
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const offsets: number[] = [];
  for (let index = 0; index <= value.length - 4; index++) {
    if (view.getUint32(index, true) === signature) offsets.push(index);
  }
  return offsets;
}
