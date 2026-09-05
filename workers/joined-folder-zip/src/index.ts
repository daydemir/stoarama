import { createJoinedZip, type JoinedBucket, type JoinedFile } from "./zip";

interface ArchiveManifest {
  schema_version: number;
  archive_name: string;
  expires_at: string;
  total_bytes: number;
  files: JoinedFile[];
}

interface ArchiveAdmission { rate_scope: string; capability_id: string }

export interface Env {
  JOINED: R2Bucket;
  ARCHIVE_LIMITER: DurableObjectNamespace;
  BACKEND_MANIFEST_URL: string;
  BACKEND_SIGNING_PRIVATE_KEY: string;
}

const encoder = new TextEncoder();
// Two R2 subrequests per file plus admission/manifest/acquire/release stays below Workers' 10,000 limit.
const MAX_FILES = 4997;
const MAX_BYTES = 256 * 1024 * 1024 * 1024;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "GET" || url.pathname !== "/archive") return new Response("Not found", { status: 404 });
    const token = url.searchParams.get("token") ?? "";
    if (!token || token.length > 2048) return new Response("Invalid archive capability", { status: 400 });

    const admission = await loadAdmission(env, token);
    if (admission instanceof Response) return admission;
    const limiter = env.ARCHIVE_LIMITER.get(env.ARCHIVE_LIMITER.idFromName(admission.rate_scope));
    const acquired = await limiter.fetch("https://archive-limiter/acquire", {
      method: "POST", body: JSON.stringify({ capability_id: admission.capability_id }),
    });
    if (!acquired.ok) return new Response(await acquired.text(), { status: acquired.status, headers: { "Retry-After": "60" } });

    try {
      const manifest = await loadManifest(env, token);
      if (manifest instanceof Response) {
        await release(limiter, admission.capability_id);
        return manifest;
      }
      const archive = await createJoinedZip(r2Adapter(env.JOINED), manifest.files);
      const channel = new TransformStream<Uint8Array, Uint8Array>();
      ctx.waitUntil(archive.body.pipeTo(channel.writable).finally(async () => {
        await limiter.fetch("https://archive-limiter/release", {
          method: "POST", body: JSON.stringify({ capability_id: admission.capability_id }),
        });
      }));
      return new Response(channel.readable, {
        headers: {
          "Content-Type": "application/zip",
          "Content-Disposition": `attachment; filename="${manifest.archive_name}"`,
          "Cache-Control": "private, no-store",
          "X-Content-Type-Options": "nosniff",
        },
      });
    } catch (error) {
      await release(limiter, admission.capability_id);
      console.error("joined archive preflight failed", error instanceof Error ? error.message : "unknown");
      return new Response("Joined archive preflight failed", { status: 502 });
    }
  },
};

async function loadManifest(env: Env, token: string): Promise<ArchiveManifest | Response> {
  const response = await authenticatedBackendFetch(env, token, "manifest");
  if (!response.ok) return new Response("Archive capability was rejected", { status: response.status === 401 ? 401 : 503 });
  const manifest = await response.json() as ArchiveManifest;
  if (!validManifest(manifest)) return new Response("Archive manifest is invalid", { status: 502 });
  return manifest;
}

async function loadAdmission(env: Env, token: string): Promise<ArchiveAdmission | Response> {
  const response = await authenticatedBackendFetch(env, token, "admission");
  if (!response.ok) return new Response("Archive capability was rejected", { status: response.status === 401 ? 401 : 503 });
  const value = await response.json() as ArchiveAdmission;
  if (!/^[A-Za-z0-9_-]{32,128}$/.test(value?.rate_scope) || !/^[A-Za-z0-9_-]{32,128}$/.test(value?.capability_id)) return new Response("Archive admission is invalid", { status: 502 });
  return value;
}

async function authenticatedBackendFetch(env: Env, token: string, operation: "admission" | "manifest"): Promise<Response> {
  const endpoint = new URL(env.BACKEND_MANIFEST_URL.replace(/\/manifest$/, `/${operation}`));
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) return new Response("Archive service unavailable", { status: 503 });
  endpoint.searchParams.set("token", token);
  const timestamp = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(token));
  const message = `joined-folder-archive-v1\0${operation}\0${timestamp}\0${base64url(new Uint8Array(digest))}`;
  const keyBytes = decodeBase64url(env.BACKEND_SIGNING_PRIVATE_KEY);
  const key = await crypto.subtle.importKey("pkcs8", keyBytes, { name: "Ed25519" }, false, ["sign"]);
  const signature = await crypto.subtle.sign("Ed25519", key, encoder.encode(message));
  return fetch(endpoint, { headers: {
    "X-Stoarama-Archive-Timestamp": timestamp,
    "X-Stoarama-Archive-Signature": base64url(new Uint8Array(signature)),
  } });
}

function validManifest(value: ArchiveManifest): boolean {
  return value?.schema_version === 1 && /^[A-Za-z0-9_-]+\.zip$/.test(value.archive_name) &&
    Array.isArray(value.files) && value.files.length > 0 && value.files.length <= MAX_FILES &&
    Number.isSafeInteger(value.total_bytes) && value.total_bytes > 0 && value.total_bytes <= MAX_BYTES &&
    value.files.reduce((sum, file) => sum + file.size_bytes, 0) === value.total_bytes;
}

async function release(limiter: DurableObjectStub, capability: string): Promise<void> {
  await limiter.fetch("https://archive-limiter/release", { method: "POST", body: JSON.stringify({ capability_id: capability }) });
}

function r2Adapter(bucket: R2Bucket): JoinedBucket {
  return {
    async head(key) { const object = await bucket.head(key); return object && { key: object.key, size: object.size, etag: object.etag, version: object.version }; },
    async get(key, options) { const object = await bucket.get(key, options); return object && { key: object.key, size: object.size, etag: object.etag, version: object.version, ...("body" in object ? { body: object.body } : {}) }; },
  };
}

function decodeBase64url(value: string): Uint8Array {
  const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(normalized + "=".repeat((4 - normalized.length % 4) % 4));
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

function base64url(value: Uint8Array): string {
  let binary = "";
  for (const byte of value) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

interface LimiterState { active: Record<string, number>; used: Record<string, number>; starts: number[] }

export class ArchiveLimiter {
  constructor(private readonly state: DurableObjectState) {}

  async fetch(request: Request): Promise<Response> {
    const capability = String((await request.json<{ capability_id?: string }>()).capability_id ?? "");
    if (!/^[A-Za-z0-9_-]{32,128}$/.test(capability)) return new Response("Invalid capability", { status: 400 });
    const now = Date.now();
    return this.state.storage.transaction(async (storage) => {
      const current = await storage.get<LimiterState>("state") ?? { active: {}, used: {}, starts: [] };
      current.starts = current.starts.filter((started) => started > now - 3600_000);
      current.active = Object.fromEntries(Object.entries(current.active).filter(([, expiry]) => expiry > now));
      current.used = Object.fromEntries(Object.entries(current.used).filter(([, expiry]) => expiry > now));
      if (new URL(request.url).pathname === "/release") {
        delete current.active[capability]; await storage.put("state", current); return new Response(null, { status: 204 });
      }
      if (current.used[capability]) return new Response("Archive capability already used", { status: 409 });
      if (Object.keys(current.active).length >= 2 || current.starts.length >= 10) return new Response("Archive download limit reached", { status: 429 });
      current.active[capability] = now + 30 * 60_000;
      current.used[capability] = now + 10 * 60_000;
      current.starts.push(now);
      await storage.put("state", current);
      return new Response(null, { status: 204 });
    });
  }
}
