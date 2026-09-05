import { DurableObject } from "cloudflare:workers";
import { createJoinedZip, type JoinedFile } from "./zip";

interface ArchiveManifest {
  schema_version: 1;
  archive_name: string;
  expires_at: string;
  total_bytes: number;
  rate_scope: string;
  capability_id: string;
  files: JoinedFile[];
}

interface LimiterState {
  active: Record<string, number>;
  used: Record<string, number>;
  starts: number[];
}

const MAX_FILES = 4000;
const MAX_BYTES = 256 * 1024 * 1024 * 1024;
const MAX_MANIFEST_BYTES = 8 * 1024 * 1024;
const MAX_SUBREQUESTS = 10000;
const PREFLIGHT_CONCURRENCY = 4;
const ACTIVE_DOWNLOAD_TTL_MS = 12 * 60 * 60_000;
const encoder = new TextEncoder();

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "GET" || url.pathname !== "/archive") {
      return new Response("Not found", { status: 404 });
    }
    const token = url.searchParams.get("token") ?? "";
    if (!token || token.length > 4096) {
      return new Response("Invalid archive capability", { status: 400 });
    }

    try {
      const manifest = await loadManifest(env, token);
      const limiter = env.ARCHIVE_LIMITER.get(env.ARCHIVE_LIMITER.idFromName(manifest.rate_scope));
      const acquired = await limiter.fetch("https://archive-limiter/acquire", {
        method: "POST",
        body: JSON.stringify({ capability_id: manifest.capability_id }),
      });
      if (!acquired.ok) {
        await acquired.body?.cancel();
        return new Response("Archive download limit reached", {
          status: acquired.status === 409 ? 409 : 429,
          headers: { "Retry-After": "60" },
        });
      }

      try {
        const archive = await createJoinedZip(env.JOINED, manifest.files, {
          maxFiles: MAX_FILES,
          maxBytes: MAX_BYTES,
          preflightConcurrency: PREFLIGHT_CONCURRENCY,
        });
        ctx.waitUntil(archive.completed
          .catch((error: unknown) => {
            console.error(JSON.stringify({ message: "joined archive stream failed", error: errorMessage(error) }));
          })
          .finally(async () => {
            await release(limiter, manifest.capability_id);
          }));
        return new Response(archive.body, {
          headers: {
            "Content-Type": "application/zip",
            "Content-Disposition": `attachment; filename="${manifest.archive_name}"`,
            "Cache-Control": "private, no-store",
            "Referrer-Policy": "no-referrer",
            "X-Content-Type-Options": "nosniff",
          },
        });
      } catch (error) {
        await release(limiter, manifest.capability_id);
        throw error;
      }
    } catch (error) {
      console.error(JSON.stringify({ message: "joined archive request failed", error: errorMessage(error) }));
      return new Response("Joined archive is unavailable", { status: 502 });
    }
  },
} satisfies ExportedHandler<Env>;

async function loadManifest(env: Env, token: string): Promise<ArchiveManifest> {
  const endpoint = new URL(env.BACKEND_MANIFEST_URL);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw new Error("invalid backend manifest URL");
  }
  endpoint.searchParams.set("token", token);
  const response = await fetch(endpoint, {
    headers: { Authorization: `Bearer ${env.BACKEND_WORKER_TOKEN}` },
    redirect: "error",
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new Error(`backend rejected archive capability (${response.status})`);
  }
  const manifest = await readBoundedJSON(response, MAX_MANIFEST_BYTES);
  if (!validManifest(manifest)) {
    throw new Error("invalid archive manifest");
  }
  return manifest;
}

async function readBoundedJSON(response: Response, limit: number): Promise<unknown> {
  const declared = Number(response.headers.get("Content-Length"));
  if (Number.isFinite(declared) && declared > limit) {
    await response.body?.cancel();
    throw new Error("archive manifest exceeds byte limit");
  }
  if (!response.body) throw new Error("archive manifest has no body");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    total += result.value.byteLength;
    if (total > limit) {
      await reader.cancel("manifest byte limit exceeded");
      throw new Error("archive manifest exceeds byte limit");
    }
    chunks.push(result.value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(new TextDecoder().decode(bytes));
}

function validManifest(value: unknown): value is ArchiveManifest {
  if (!value || typeof value !== "object") return false;
  const manifest = value as Partial<ArchiveManifest>;
  const expiresAt = Date.parse(String(manifest.expires_at ?? ""));
  const now = Date.now();
  if (manifest.schema_version !== 1 || !/^[A-Za-z0-9_-]+\.zip$/.test(String(manifest.archive_name ?? "")) ||
    !Number.isFinite(expiresAt) || expiresAt <= now || expiresAt > now + 10 * 60_000 ||
    !Array.isArray(manifest.files) || manifest.files.length < 1 || manifest.files.length > MAX_FILES ||
    !Number.isSafeInteger(manifest.total_bytes) || Number(manifest.total_bytes) < 1 || Number(manifest.total_bytes) > MAX_BYTES ||
    !/^[A-Za-z0-9_-]{32,128}$/.test(String(manifest.rate_scope ?? "")) ||
    !/^[A-Za-z0-9_-]{32,128}$/.test(String(manifest.capability_id ?? ""))) {
    return false;
  }
  const total = manifest.files.reduce((sum, file) => sum + (Number.isSafeInteger(file?.size_bytes) ? file.size_bytes : Number.NaN), 0);
  return total === manifest.total_bytes && 2 * manifest.files.length + 3 <= MAX_SUBREQUESTS;
}

async function release(limiter: DurableObjectStub, capabilityID: string): Promise<void> {
  try {
    const response = await limiter.fetch("https://archive-limiter/release", {
      method: "POST",
      body: JSON.stringify({ capability_id: capabilityID }),
    });
    await response.body?.cancel();
  } catch (error) {
    console.error(JSON.stringify({ message: "joined archive limiter release failed", error: errorMessage(error) }));
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "unknown error";
}

export class ArchiveLimiter extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const pathname = new URL(request.url).pathname;
    if (request.method !== "POST" || (pathname !== "/acquire" && pathname !== "/release")) {
      return new Response("Not found", { status: 404 });
    }
    const body = await readLimiterBody(request);
    if (!body) return new Response("Invalid capability", { status: 400 });
    const now = Date.now();
    return this.ctx.storage.transaction(async (storage) => {
      const current = await storage.get<LimiterState>("state") ?? { active: {}, used: {}, starts: [] };
      current.starts = current.starts.filter((started) => started > now - 60 * 60_000);
      current.active = filterFuture(current.active, now);
      current.used = filterFuture(current.used, now);
      if (pathname === "/release") {
        delete current.active[body.capability_id];
        await storage.put("state", current);
        return new Response(null, { status: 204 });
      }
      if (current.used[body.capability_id]) return new Response("Archive capability already used", { status: 409 });
      if (Object.keys(current.active).length >= 2 || current.starts.length >= 10) {
        return new Response("Archive download limit reached", { status: 429 });
      }
      current.active[body.capability_id] = now + ACTIVE_DOWNLOAD_TTL_MS;
      current.used[body.capability_id] = now + 10 * 60_000;
      current.starts.push(now);
      await storage.put("state", current);
      return new Response(null, { status: 204 });
    });
  }
}

async function readLimiterBody(request: Request): Promise<{ capability_id: string } | null> {
  const length = Number(request.headers.get("Content-Length"));
  if (Number.isFinite(length) && length > 256) return null;
  try {
    const value = await request.json<unknown>();
    if (!value || typeof value !== "object") return null;
    const capabilityID = String((value as { capability_id?: unknown }).capability_id ?? "");
    return /^[A-Za-z0-9_-]{32,128}$/.test(capabilityID) ? { capability_id: capabilityID } : null;
  } catch {
    return null;
  }
}

function filterFuture(values: Record<string, number>, now: number): Record<string, number> {
  return Object.fromEntries(Object.entries(values).filter(([, expiry]) => expiry > now));
}
