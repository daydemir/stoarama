import type { JoinedFile, JoinedSource } from "../src/zip";

export const options = { maxFiles: 4000, maxBytes: 256 * 1024 * 1024 * 1024, preflightConcurrency: 4 };

export function file(changes: Partial<JoinedFile> = {}): JoinedFile {
  return {
    batch_id: "batch-7",
    sha256: "a".repeat(64),
    etag: "etag-a",
    version_id: "version-a",
    size_bytes: 9,
    relative_path: "377_Europe_Poland_Luban/August/Thursday/clip.mp4",
    content_type: "video/mp4",
    head: { url: "https://account.r2.cloudflarestorage.com/stoarama/object?X-Amz-Signature=head", method: "HEAD", headers: { "If-Match": ['"etag-a"'] } },
    get: { url: "https://account.r2.cloudflarestorage.com/stoarama/object?X-Amz-Signature=get", method: "GET", headers: { "If-Match": ['"etag-a"'] } },
    ...changes,
  };
}

export function responseFor(entry: JoinedFile, body = new TextEncoder().encode("123456789"), changes: Record<string, string> = {}): Response {
  return new Response(body, { headers: { "Content-Length": String(entry.size_bytes), ETag: `"${entry.etag}"`, "x-amz-version-id": entry.version_id, ...changes } });
}

export function sourceFor(entries: JoinedFile[]): JoinedSource & { heads: string[]; gets: string[] } {
  const heads: string[] = [];
  const gets: string[] = [];
  return {
    heads,
    gets,
    async read(request) {
      const entry = entries.find((candidate) => candidate[request.method.toLowerCase() as "head" | "get"].url === request.url);
      if (!entry) return new Response(null, { status: 404 });
      (request.method === "HEAD" ? heads : gets).push(request.url);
      const response = responseFor(entry);
      return request.method === "HEAD" ? new Response(null, { headers: response.headers }) : response;
    },
  };
}

export async function bytes(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  const reader = stream.getReader();
  while (true) {
    const result = await reader.read();
    if (result.done) break;
    chunks.push(result.value);
  }
  const output = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.byteLength; }
  return output;
}
