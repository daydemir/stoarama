import type { JoinedBucket, JoinedFile } from "../src/zip";

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
    ...changes,
  };
}

export function objectFor(entry: JoinedFile, body = new TextEncoder().encode("123456789")) {
  return {
    key: `joined/${entry.batch_id}/objects/${entry.sha256}.mp4`,
    size: entry.size_bytes,
    etag: entry.etag,
    version: entry.version_id,
    body: new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(body); controller.close(); } }),
  };
}

export function bucketFor(entries: JoinedFile[]): JoinedBucket & { heads: string[]; gets: string[] } {
  const objects = new Map(entries.map((entry) => [`joined/${entry.batch_id}/objects/${entry.sha256}.mp4`, objectFor(entry)]));
  const heads: string[] = [];
  const gets: string[] = [];
  return {
    heads,
    gets,
    async head(key) { heads.push(key); return objects.get(key) ?? null; },
    async get(key) { gets.push(key); return objects.get(key) ?? null; },
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
