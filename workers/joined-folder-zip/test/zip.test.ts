import { afterEach, describe, expect, it, vi } from "vitest";
import { ArchiveRequestError, loadManifest, readPresignedJoinedObject, release } from "../src/index";
import { createJoinedZip, type JoinedSource } from "../src/zip";
import { bytes, file, options, responseFor, sourceFor } from "./helpers";

describe("createJoinedZip", () => {
  it("streams a ZIP64 archive and redacts its public index", async () => {
    const entry = file();
    const source = sourceFor([entry]);
    const archive = await createJoinedZip(source, [entry], options);
    const output = await bytes(archive.body);
    await archive.completed;
    const text = new TextDecoder().decode(output);

    expect(text).toContain("joined-files.json");
    expect(text).toContain(entry.relative_path);
    expect(text).toContain('"size_bytes": 9');
    for (const forbidden of [
      "artifact_id", "batch_id", "clip_id", "recording_job_id", "storage_destination_id",
      "etag", "version_id", "sha256", "object_key", "endpoint", "access_key", "secret", "token",
      "cloudflarestorage.com", "joined/", "raw/",
    ]) {
      expect(text).not.toContain(forbidden);
    }
    expect(signatures(output)).toEqual(expect.arrayContaining([
      0x04034b50, 0x08074b50, 0x02014b50, 0x06064b50, 0x07064b50, 0x06054b50,
    ]));
    expect(source.heads).toHaveLength(1);
    expect(source.gets).toEqual([entry.get.url]);
  });

  it("computes CRC32 across source chunks", async () => {
    const entry = file();
    const source = new TextEncoder().encode("123456789");
    const body = new ReadableStream({
      start(controller) {
        for (const byte of source) controller.enqueue(Uint8Array.of(byte));
        controller.close();
      },
    });
    const sourceStore: JoinedSource = { read: async (request) => request.method === "HEAD" ? new Response(null, { headers: responseFor(entry).headers }) : new Response(body, { headers: responseFor(entry).headers }) };
    const archive = await createJoinedZip(sourceStore, [entry], options);
    const output = await bytes(archive.body);
    await archive.completed;
    const descriptor = signatureOffsets(output, 0x08074b50).at(-1);
    expect(descriptor).toBeDefined();
    expect(new DataView(output.buffer, output.byteOffset, output.byteLength).getUint32((descriptor ?? 0) + 4, true)).toBe(0xcbf43926);
  });

  it("rejects identity drift before returning a stream", async () => {
    const entry = file();
    const source = sourceFor([entry]);
    source.read = async (request) => {
      if (request.method === "GET") source.gets.push(request.url);
      return responseFor(entry, undefined, { "x-amz-version-id": "changed" });
    };
    await expect(createJoinedZip(source, [entry], options)).rejects.toThrow(/identity mismatch.*version/i);
    expect(source.gets).toHaveLength(0);
  });

  it("cancels a fetched body when identity changes after preflight", async () => {
    const entry = file();
    let cancelled = false;
    const changedBody = new ReadableStream<Uint8Array>({
        start(controller) { controller.enqueue(Uint8Array.of(1)); },
        cancel() { cancelled = true; },
      });
    const source: JoinedSource = { read: async (request) => request.method === "HEAD"
      ? new Response(null, { headers: responseFor(entry).headers })
      : new Response(changedBody, { headers: responseFor(entry, undefined, { "x-amz-version-id": "changed" }).headers }) };
    const archive = await createJoinedZip(source, [entry], options);
    await expect(bytes(archive.body)).rejects.toBeDefined();
    await expect(archive.completed).rejects.toThrow(/identity mismatch.*version/i);
    expect(cancelled).toBe(true);
  });

  it.each([
    "../escape.mp4", "/absolute.mp4", "dir\\clip.mp4", "./clip.mp4", "C:/clip.mp4", "dir/con.txt", "dir/COM1.tar.mp4", "dir/COM¹.tar.mp4", "dir/LPT³.data.mp4", "dir/trailing. /clip.mp4",
  ])("rejects unsafe extraction path %s", async (relative_path) => {
    const entry = file({ relative_path });
    await expect(createJoinedZip(sourceFor([entry]), [entry], options)).rejects.toThrow(/unsafe.*path|invalid media/i);
  });

  it("rejects portable path collisions and resource overages", async () => {
    const first = file({ relative_path: "Folder/clip.mp4" });
    const second = file({ sha256: "b".repeat(64), relative_path: "folder/CLIP.mp4" });
    await expect(createJoinedZip(sourceFor([first, second]), [first, second], options)).rejects.toThrow(/collision/i);
    await expect(createJoinedZip(sourceFor([first]), [first], { ...options, maxBytes: 8 })).rejects.toThrow(/byte limit/i);
    await expect(createJoinedZip(sourceFor([first]), [first], { ...options, maxFiles: 0 })).rejects.toThrow(/file count/i);
  });

  it("cancels the active R2 reader when the browser disconnects", async () => {
    const entry = file({ size_bytes: 100 });
    let cancelled = false;
    const body = new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel() { cancelled = true; },
    });
    let getStarted = false;
    const source: JoinedSource = { read: async (request) => {
      if (request.method === "HEAD") return new Response(null, { headers: responseFor(entry).headers });
      getStarted = true;
      return new Response(body, { headers: responseFor(entry).headers });
    } };
    const archive = await createJoinedZip(source, [entry], options);
    const reader = archive.body.getReader();
    while (!getStarted) await reader.read();
    await reader.read();
    await reader.cancel("stop");
    await expect(archive.completed).rejects.toBeDefined();
    expect(cancelled).toBe(true);
  });
});

describe("readPresignedJoinedObject", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("allows only exact R2 HEAD/GET capabilities and never follows redirects", async () => {
    const fetcher = vi.fn(async () => new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", fetcher);
    const entry = file();
    await readPresignedJoinedObject(entry.head);
    expect(fetcher).toHaveBeenCalledWith(expect.any(URL), expect.objectContaining({ method: "HEAD", redirect: "error" }));
    await expect(readPresignedJoinedObject({ ...entry.get, method: "PUT" } as never)).rejects.toThrow(/method/);
    await expect(readPresignedJoinedObject({ ...entry.get, method: "DELETE" } as never)).rejects.toThrow(/method/);
    await expect(readPresignedJoinedObject({ ...entry.get, method: "GET", url: "https://evil.example/object?signature=x" })).rejects.toThrow(/URL/);
    await expect(readPresignedJoinedObject({ ...entry.get, headers: { "If-Match": ['"etag-a"'], Authorization: ["no"] } })).rejects.toThrow(/headers/);
  });
});

describe("release", () => {
  it("retries failures and requires a successful response", async () => {
    const statuses = [new Error("temporary"), new Response(null, { status: 503 }), new Response(null, { status: 204 })];
    const waits: number[] = [];
    const limiter = {
      async fetch() {
        const result = statuses.shift();
        if (result instanceof Error) throw result;
        return result ?? new Response(null, { status: 500 });
      },
    };
    await release(limiter as unknown as DurableObjectStub, "a".repeat(43), async (delay) => { waits.push(delay); });
    expect(statuses).toHaveLength(0);
    expect(waits).toEqual([100, 500]);
  });
});

describe("loadManifest", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("maps only the backend capability status and bounds the request", async () => {
    const fetcher = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.redirect).toBe("error");
      expect(init?.signal).toBeInstanceOf(AbortSignal);
      return new Response(null, { status: 410 });
    });
    vi.stubGlobal("fetch", fetcher);
    const env = {
      BACKEND_MANIFEST_URL: "https://stoarama.example.test/api/v1/recording/joined/archive/manifest",
      BACKEND_WORKER_TOKEN: "worker-token",
    } as Env;
    await expect(loadManifest(env, "capability")).rejects.toMatchObject({ status: 410 });

    fetcher.mockResolvedValueOnce(new Response(null, { status: 401 }));
    await expect(loadManifest(env, "capability")).rejects.not.toBeInstanceOf(ArchiveRequestError);
  });
});

function signatures(value: Uint8Array): number[] {
  const wanted = new Set([0x04034b50, 0x08074b50, 0x02014b50, 0x06064b50, 0x07064b50, 0x06054b50]);
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const found: number[] = [];
  for (let index = 0; index <= value.length - 4; index++) {
    const signature = view.getUint32(index, true);
    if (wanted.has(signature)) found.push(signature);
  }
  return found;
}

function signatureOffsets(value: Uint8Array, signature: number): number[] {
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const offsets: number[] = [];
  for (let index = 0; index <= value.length - 4; index++) if (view.getUint32(index, true) === signature) offsets.push(index);
  return offsets;
}
