import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { ArchiveLimiter, type Env } from "../src/index";

const b64 = (bytes: ArrayBuffer) => Buffer.from(bytes).toString("base64url");

async function fixture() {
  const pair = await crypto.subtle.generateKey("Ed25519", true, ["sign", "verify"]) as CryptoKeyPair;
  const privateKey = b64(await crypto.subtle.exportKey("pkcs8", pair.privateKey) as ArrayBuffer);
  const sha = "a".repeat(64);
  const key = `joined/batch-1/objects/${sha}.mp4`;
  const object = { key, size: 3, etag: "etag", version: "version", body: new ReadableStream({ start(c) { c.enqueue(new Uint8Array([1, 2, 3])); c.close(); } }) };
  const limiterFetch = vi.fn(async () => new Response(null, { status: 204 }));
  const env = {
    BACKEND_MANIFEST_URL: "https://stoarama.com/api/v1/recording/joined/archive/manifest",
    BACKEND_SIGNING_PRIVATE_KEY: privateKey,
    JOINED: { head: vi.fn(async () => object), get: vi.fn(async () => object) },
    ARCHIVE_LIMITER: { idFromName: vi.fn(() => ({})), get: vi.fn(() => ({ fetch: limiterFetch })) },
  } as unknown as Env;
  const manifest = { schema_version: 1, archive_name: "recording.zip", expires_at: new Date(Date.now() + 60_000).toISOString(), total_bytes: 3,
	    files: [{ artifact_id: 1, batch_id: "batch-1", sha256: sha, etag: "etag", version_id: "version", size_bytes: 3, relative_path: "recording/May/Monday/clip.mp4", content_type: "video/mp4" }] };
  const token = `${Buffer.from(JSON.stringify({ account_id: 47, client_scope: "s".repeat(43), nonce: "n".repeat(22) })).toString("base64url")}.signature`;
  return { env, manifest, limiterFetch, token };
}

afterEach(() => vi.unstubAllGlobals());

describe("archive Worker", () => {
  it("authenticates the manifest, preflights R2, and streams a private ZIP", async () => {
    const { env, manifest, limiterFetch, token } = await fixture();
    const backend = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("X-Stoarama-Archive-Signature")).toMatch(/^[A-Za-z0-9_-]+$/);
      if (String(input).includes("/admission?")) return Response.json({ rate_scope: "r".repeat(43), capability_id: "c".repeat(43) });
      return Response.json(manifest);
    });
    vi.stubGlobal("fetch", backend);
    const pending: Promise<unknown>[] = [];
    const response = await worker.fetch(new Request(`https://download.example/archive?token=${token}`), env, { waitUntil(p: Promise<unknown>) { pending.push(p); } } as unknown as ExecutionContext);
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Disposition")).toBe('attachment; filename="recording.zip"');
    const bytes = new Uint8Array(await response.arrayBuffer());
    await Promise.all(pending);
    expect(new TextDecoder().decode(bytes)).toContain("joined-files.json");
    expect(backend).toHaveBeenCalledTimes(2);
    expect(limiterFetch).toHaveBeenCalledTimes(2);
  });

  it("returns a pre-header error and releases the limiter when R2 preflight fails", async () => {
    const { env, manifest, limiterFetch, token } = await fixture();
    (env.JOINED.head as unknown as ReturnType<typeof vi.fn>).mockResolvedValue(null);
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => String(input).includes("/admission?") ? Response.json({ rate_scope: "r".repeat(43), capability_id: "c".repeat(43) }) : Response.json(manifest)));
    const response = await worker.fetch(new Request(`https://download.example/archive?token=${token}`), env, { waitUntil() {} } as unknown as ExecutionContext);
    expect(response.status).toBe(502);
    expect(limiterFetch).toHaveBeenCalledTimes(2);
  });
});

describe("ArchiveLimiter", () => {
  it("makes capabilities single-use and releases active concurrency", async () => {
    let saved: unknown;
    const storage = {
      transaction: async (callback: (store: { get: () => Promise<unknown>; put: (_key: string, value: unknown) => Promise<void> }) => Promise<Response>) => callback({
        get: async () => saved,
        put: async (_key, value) => { saved = value; },
      }),
    };
    const limiter = new ArchiveLimiter({ storage } as unknown as DurableObjectState);
    const body = JSON.stringify({ capability_id: "a".repeat(43) });
    expect((await limiter.fetch(new Request("https://limiter/acquire", { method: "POST", body }))).status).toBe(204);
    expect((await limiter.fetch(new Request("https://limiter/acquire", { method: "POST", body }))).status).toBe(409);
    expect((await limiter.fetch(new Request("https://limiter/release", { method: "POST", body }))).status).toBe(204);
  });
});
