import { createCRC32 } from "hash-wasm";

export interface JoinedFile {
  artifact_id: string;
  batch_id: string;
  sha256: string;
  etag: string;
  version_id: string;
  size_bytes: number;
  relative_path: string;
  content_type: string;
}

interface R2Identity {
  key: string;
  size: number;
  etag: string;
  version: string;
}

interface R2Body extends R2Identity {
  body: ReadableStream<Uint8Array>;
}

export interface JoinedBucket {
  head(key: string): Promise<R2Identity | null>;
  get(key: string, options: { onlyIf: { etagMatches: string } }): Promise<R2Body | R2Identity | null>;
}

export interface JoinedZip {
  body: ReadableStream<Uint8Array>;
  contentType: "application/zip";
}

interface ArchiveEntry {
  path: string;
  size: bigint;
  source?: JoinedFile;
  inline?: Uint8Array;
}

interface CentralEntry {
  path: Uint8Array;
  crc: number;
  size: bigint;
  offset: bigint;
}

const encoder = new TextEncoder();
const MAX_SAFE_SIZE = BigInt(Number.MAX_SAFE_INTEGER);
const RESERVED_NAME = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?$/i;

export async function createJoinedZip(
  bucket: JoinedBucket,
  manifest: readonly JoinedFile[],
  options: { preflightConcurrency?: number } = {},
): Promise<JoinedZip> {
  const entries = validateAndSort(manifest);
  await preflight(bucket, entries, options.preflightConcurrency ?? 8);

  const publicManifest = entries.map((entry) => ({
    artifact_id: entry.artifact_id,
    relative_path: entry.relative_path,
    content_type: entry.content_type,
    size_bytes: entry.size_bytes,
    sha256: entry.sha256,
  }));
  const manifestBytes = encoder.encode(`${JSON.stringify({ files: publicManifest }, null, 2)}\n`);
  const archiveEntries: ArchiveEntry[] = [
    { path: "joined-files.json", size: BigInt(manifestBytes.length), inline: manifestBytes },
    ...entries.map((source) => ({ path: source.relative_path, size: BigInt(source.size_bytes), source })),
  ];

  return { body: streamZip(bucket, archiveEntries), contentType: "application/zip" };
}

function validateAndSort(manifest: readonly JoinedFile[]): JoinedFile[] {
  const paths = new Map<string, string>();
  return [...manifest].map((entry) => {
    if (!entry.artifact_id) throw new Error(`Missing artifact_id for ${entry.relative_path}`);
    if (!/^[a-f0-9]{64}$/i.test(entry.sha256)) throw new Error(`Invalid sha256 for ${entry.relative_path}`);
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(entry.batch_id) || entry.batch_id === "." || entry.batch_id === "..") {
      throw new Error(`Invalid batch_id for ${entry.relative_path}`);
    }
    if (!entry.etag || !entry.version_id) throw new Error(`Missing object identity for ${entry.relative_path}`);
    if (!Number.isSafeInteger(entry.size_bytes) || entry.size_bytes < 0 || BigInt(entry.size_bytes) > MAX_SAFE_SIZE) {
      throw new Error(`Invalid size for ${entry.relative_path}`);
    }
    assertPortablePath(entry.relative_path);
    if (encoder.encode(entry.relative_path).length > 0xffff) throw new Error(`Unsafe portable path: ${entry.relative_path} is too long`);
    const collisionKey = entry.relative_path.normalize("NFC").toLocaleLowerCase("en-US");
    const prior = paths.get(collisionKey);
    if (prior) throw new Error(`Portable path collision: ${prior} and ${entry.relative_path}`);
    if (collisionKey === "joined-files.json") throw new Error("Portable path collision with joined-files.json");
    paths.set(collisionKey, entry.relative_path);
    return { ...entry };
  }).sort((a, b) => a.relative_path.localeCompare(b.relative_path, "en-US"));
}

function assertPortablePath(path: string): void {
  if (!path || path !== path.normalize("NFC") || path.startsWith("/") || path.includes("\\") || /[\u0000-\u001f\u007f]/.test(path)) {
    throw new Error(`Unsafe portable path: ${path}`);
  }
  const parts = path.split("/");
  if (parts.some((part) => !part || part === "." || part === ".." || /[<>:"|?*]/.test(part) || /[. ]$/.test(part) || RESERVED_NAME.test(part))) {
    throw new Error(`Unsafe portable path: ${path}`);
  }
}

async function preflight(bucket: JoinedBucket, entries: readonly JoinedFile[], concurrency: number): Promise<void> {
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 64) throw new Error("preflightConcurrency must be between 1 and 64");
  let next = 0;
  const workers = Array.from({ length: Math.min(concurrency, entries.length) }, async () => {
    while (next < entries.length) {
      const entry = entries[next++];
      if (!entry) return;
      const object = await bucket.head(keyFor(entry));
      assertIdentity(object, entry, "preflight");
    }
  });
  await Promise.all(workers);
}

function assertIdentity(object: R2Identity | null, entry: JoinedFile, stage: string): asserts object is R2Identity {
  if (!object) throw new Error(`R2 identity mismatch during ${stage}: missing ${entry.relative_path}`);
  const mismatches: string[] = [];
  if (object.key !== keyFor(entry)) mismatches.push("key");
  if (object.size !== entry.size_bytes) mismatches.push("size");
  if (normalizeEtag(object.etag) !== normalizeEtag(entry.etag)) mismatches.push("etag");
  if (object.version !== entry.version_id) mismatches.push("version");
  if (mismatches.length) throw new Error(`R2 identity mismatch during ${stage} for ${entry.relative_path}: ${mismatches.join(", ")}`);
}

function normalizeEtag(etag: string): string {
  return etag.startsWith('"') && etag.endsWith('"') ? etag.slice(1, -1) : etag;
}

function keyFor(entry: JoinedFile): string {
  return `joined/${entry.batch_id}/objects/${entry.sha256.toLowerCase()}.mp4`;
}

function streamZip(bucket: JoinedBucket, entries: readonly ArchiveEntry[]): ReadableStream<Uint8Array> {
  const channel = new TransformStream<Uint8Array, Uint8Array>();
  const writer = channel.writable.getWriter();
  let activeReader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let cancelled = false;
  writer.closed.catch(async () => {
    cancelled = true;
    if (activeReader) await activeReader.cancel("zip consumer cancelled").catch(() => undefined);
  });

  void (async () => {
    let offset = 0n;
    const central: CentralEntry[] = [];
    try {
      for (const entry of entries) {
        if (cancelled) throw new Error("zip consumer cancelled");
        let body: ReadableStream<Uint8Array>;
        if (entry.inline) {
          const inline = entry.inline;
          body = new ReadableStream({ start(controller) { controller.enqueue(inline); controller.close(); } });
        } else {
          const source = entry.source;
          if (!source) throw new Error("Archive source is missing");
          const object = await bucket.get(keyFor(source), { onlyIf: { etagMatches: source.etag } });
          assertIdentity(object, source, "download");
          if (!("body" in object)) throw new Error(`R2 conditional get failed for ${source.relative_path}`);
          body = object.body;
        }
        const path = encoder.encode(entry.path);
        const localOffset = offset;
        const local = localHeader(path, entry.size);
        await writer.write(local); offset += BigInt(local.length);
        activeReader = body.getReader();
        const crc32 = await createCRC32();
        let actual = 0n;
        while (true) {
          const result = await activeReader.read();
          if (result.done) break;
          crc32.update(result.value);
          actual += BigInt(result.value.length);
          if (actual > entry.size) throw new Error(`R2 body exceeds manifest size for ${entry.path}`);
          await writer.write(result.value); offset += BigInt(result.value.length);
        }
        activeReader = undefined;
        if (actual !== entry.size) throw new Error(`R2 body size mismatch for ${entry.path}: expected ${entry.size}, got ${actual}`);
        const crc = Number.parseInt(crc32.digest("hex") as string, 16) >>> 0;
        const descriptor = dataDescriptor(crc, actual);
        await writer.write(descriptor); offset += BigInt(descriptor.length);
        central.push({ path, crc, size: actual, offset: localOffset });
      }

      const centralOffset = offset;
      for (const entry of central) {
        const record = centralHeader(entry);
        await writer.write(record); offset += BigInt(record.length);
      }
      const centralSize = offset - centralOffset;
      const end = endRecords(BigInt(central.length), centralSize, centralOffset, offset);
      await writer.write(end);
      await writer.close();
    } catch (error) {
      if (activeReader) await activeReader.cancel(error).catch(() => undefined);
      await writer.abort(error).catch(() => undefined);
    }
  })();
  return channel.readable;
}

function localHeader(path: Uint8Array, size: bigint): Uint8Array {
  const extra = concat(u16(0x0001), u16(16), u64(size), u64(size));
  return concat(u32(0x04034b50), u16(45), u16(0x0808), u16(0), dosTime(), u32(0), u32(0xffffffff), u32(0xffffffff), u16(path.length), u16(extra.length), path, extra);
}

function dataDescriptor(crc: number, size: bigint): Uint8Array {
  return concat(u32(0x08074b50), u32(crc), u64(size), u64(size));
}

function centralHeader(entry: CentralEntry): Uint8Array {
  const extra = concat(u16(0x0001), u16(24), u64(entry.size), u64(entry.size), u64(entry.offset));
  return concat(
    u32(0x02014b50), u16(0x032d), u16(45), u16(0x0808), u16(0), dosTime(), u32(entry.crc),
    u32(0xffffffff), u32(0xffffffff), u16(entry.path.length), u16(extra.length), u16(0), u16(0), u16(0), u32(0),
    u32(0xffffffff), entry.path, extra,
  );
}

function endRecords(count: bigint, centralSize: bigint, centralOffset: bigint, zip64Offset: bigint): Uint8Array {
  const zip64 = concat(
    u32(0x06064b50), u64(44n), u16(0x032d), u16(45), u32(0), u32(0),
    u64(count), u64(count), u64(centralSize), u64(centralOffset),
  );
  const locator = concat(u32(0x07064b50), u32(0), u64(zip64Offset), u32(1));
  const eocd = concat(u32(0x06054b50), u16(0), u16(0), u16(0xffff), u16(0xffff), u32(0xffffffff), u32(0xffffffff), u16(0));
  return concat(zip64, locator, eocd);
}

function dosTime(): Uint8Array {
  return concat(u16(0), u16(0x0021));
}

function u16(value: number): Uint8Array {
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, value, true);
  return bytes;
}

function u32(value: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value >>> 0, true);
  return bytes;
}

function u64(value: bigint): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, value, true);
  return bytes;
}

function concat(...chunks: readonly Uint8Array[]): Uint8Array {
  const output = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) { output.set(chunk, offset); offset += chunk.length; }
  return output;
}
