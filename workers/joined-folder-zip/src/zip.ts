import { CRC32 } from "./crc32";

export interface JoinedFile {
  batch_id: string;
  sha256: string;
  etag: string;
  version_id: string;
  size_bytes: number;
  relative_path: string;
  content_type: string;
}

export interface JoinedZip {
  body: ReadableStream<Uint8Array>;
  completed: Promise<void>;
}

interface JoinedObject {
  key: string;
  version: string;
  size: number;
  etag: string;
  body?: ReadableStream<Uint8Array>;
}

export interface JoinedBucket {
  head(key: string): Promise<JoinedObject | null>;
  get(key: string, options: { onlyIf: { etagMatches: string } }): Promise<JoinedObject | null>;
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

interface Options {
  maxFiles: number;
  maxBytes: number;
  preflightConcurrency: number;
}

const encoder = new TextEncoder();
const MAX_SAFE_SIZE = BigInt(Number.MAX_SAFE_INTEGER);
const RESERVED_NAME = /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(\..*)?$/i;

export async function createJoinedZip(bucket: JoinedBucket, manifest: readonly JoinedFile[], options: Options): Promise<JoinedZip> {
  const entries = validateAndSort(manifest, options);
  await preflight(bucket, entries, options.preflightConcurrency);

  const publicManifest = entries.map((entry) => ({
    relative_path: entry.relative_path,
    content_type: entry.content_type,
    size_bytes: entry.size_bytes,
  }));
  const manifestBytes = encoder.encode(`${JSON.stringify({ schema_version: 1, files: publicManifest }, null, 2)}\n`);
  const archiveEntries: ArchiveEntry[] = [
    { path: "joined-files.json", size: BigInt(manifestBytes.length), inline: manifestBytes },
    ...entries.map((source) => ({ path: source.relative_path, size: BigInt(source.size_bytes), source })),
  ];
  return streamZip(bucket, archiveEntries);
}

function validateAndSort(manifest: readonly JoinedFile[], options: Options): JoinedFile[] {
  if (!Number.isInteger(options.maxFiles) || options.maxFiles < 1 || manifest.length < 1 || manifest.length > options.maxFiles) {
    throw new Error("invalid archive file count");
  }
  if (!Number.isSafeInteger(options.maxBytes) || options.maxBytes < 1) throw new Error("invalid archive byte limit");
  const paths = new Map<string, string>();
  let total = 0;
  return [...manifest].map((entry) => {
    if (!entry || typeof entry !== "object") throw new Error("invalid archive entry");
    if (typeof entry.relative_path !== "string") throw new Error("invalid archive path");
    if (typeof entry.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(entry.sha256)) throw new Error(`Invalid sha256 for ${entry.relative_path}`);
    if (typeof entry.batch_id !== "string" || !/^[a-z0-9](?:[a-z0-9-]{0,126}[a-z0-9])?$/.test(entry.batch_id)) throw new Error(`Invalid batch_id for ${entry.relative_path}`);
    if (typeof entry.etag !== "string" || !entry.etag || entry.etag.length > 256 || typeof entry.version_id !== "string" || entry.version_id.length > 256) throw new Error(`Missing object identity for ${entry.relative_path}`);
    if (entry.content_type !== "video/mp4" || !entry.relative_path.toLowerCase().endsWith(".mp4")) throw new Error(`Invalid media type for ${entry.relative_path}`);
    if (!Number.isSafeInteger(entry.size_bytes) || entry.size_bytes <= 0 || BigInt(entry.size_bytes) > MAX_SAFE_SIZE) throw new Error(`Invalid size for ${entry.relative_path}`);
    total += entry.size_bytes;
    if (!Number.isSafeInteger(total) || total > options.maxBytes) throw new Error("archive exceeds byte limit");
    assertPortablePath(entry.relative_path);
    if (encoder.encode(entry.relative_path).length > 0xffff) throw new Error(`Unsafe portable path: ${entry.relative_path} is too long`);
    const collisionKey = entry.relative_path.normalize("NFC").toLowerCase();
    const prior = paths.get(collisionKey);
    if (prior || collisionKey === "joined-files.json") throw new Error(`Portable path collision: ${prior ?? entry.relative_path}`);
    paths.set(collisionKey, entry.relative_path);
    return { ...entry };
  }).sort((a, b) => a.relative_path.localeCompare(b.relative_path, "en-US"));
}

function assertPortablePath(value: string): void {
  if (!value || value !== value.normalize("NFC") || value.startsWith("/") || value.includes("\\") || /[\u0000-\u001f\u007f]/.test(value)) {
    throw new Error(`Unsafe portable path: ${value}`);
  }
  const parts = value.split("/");
  if (parts.some((part) => !part || part === "." || part === ".." || /[<>:"|?*]/.test(part) || /[. ]$/.test(part) || RESERVED_NAME.test(part))) {
    throw new Error(`Unsafe portable path: ${value}`);
  }
}

async function preflight(bucket: JoinedBucket, entries: readonly JoinedFile[], concurrency: number): Promise<void> {
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 6) throw new Error("preflight concurrency must be between 1 and 6");
  let next = 0;
  const workers = Array.from({ length: Math.min(concurrency, entries.length) }, async () => {
    while (next < entries.length) {
      const entry = entries[next++];
      if (!entry) return;
      assertIdentity(await bucket.head(keyFor(entry)), entry, "preflight");
    }
  });
  await Promise.all(workers);
}

function assertIdentity(object: JoinedObject | null, entry: JoinedFile, stage: string): asserts object is JoinedObject {
  if (!object) throw new Error(`R2 identity mismatch during ${stage}: missing ${entry.relative_path}`);
  const mismatches: string[] = [];
  if (object.key !== keyFor(entry)) mismatches.push("key");
  if (object.size !== entry.size_bytes) mismatches.push("size");
  if (normalizeETag(object.etag) !== normalizeETag(entry.etag)) mismatches.push("etag");
  if (entry.version_id && object.version !== entry.version_id) mismatches.push("version");
  if (mismatches.length) throw new Error(`R2 identity mismatch during ${stage} for ${entry.relative_path}: ${mismatches.join(", ")}`);
}

function normalizeETag(etag: string): string {
  return etag.startsWith('"') && etag.endsWith('"') ? etag.slice(1, -1) : etag;
}

function keyFor(entry: JoinedFile): string {
  return `joined/${entry.batch_id}/objects/${entry.sha256}.mp4`;
}

function streamZip(bucket: JoinedBucket, entries: readonly ArchiveEntry[]): JoinedZip {
  const channel = new TransformStream<Uint8Array, Uint8Array>();
  const writer = channel.writable.getWriter();
  let activeReader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  const completed = (async () => {
    let offset = 0n;
    const central: CentralEntry[] = [];
    try {
      const crc32 = new CRC32();
      for (const entry of entries) {
        let body: ReadableStream<Uint8Array>;
        if (entry.inline) {
          const inline = entry.inline;
          body = new ReadableStream({ start(controller) { controller.enqueue(inline); controller.close(); } });
        } else {
          const source = entry.source;
          if (!source) throw new Error("archive source is missing");
          const object = await bucket.get(keyFor(source), { onlyIf: { etagMatches: source.etag } });
          try {
            assertIdentity(object, source, "download");
          } catch (error) {
            await object?.body?.cancel(error).catch(() => undefined);
            throw error;
          }
          if (!object.body) throw new Error(`R2 conditional get failed for ${source.relative_path}`);
          body = object.body;
        }
        const pathBytes = encoder.encode(entry.path);
        const localOffset = offset;
        const local = localHeader(pathBytes, entry.size);
        await writer.write(local);
        offset += BigInt(local.length);

        activeReader = body.getReader();
        crc32.init();
        let actual = 0n;
        while (true) {
          const result = await activeReader.read();
          if (result.done) break;
          crc32.update(result.value);
          actual += BigInt(result.value.byteLength);
          if (actual > entry.size) throw new Error(`R2 body exceeds manifest size for ${entry.path}`);
          await writer.write(result.value);
          offset += BigInt(result.value.byteLength);
        }
        activeReader = undefined;
        if (actual !== entry.size) throw new Error(`R2 body size mismatch for ${entry.path}`);
        const crc = crc32.digest();
        const descriptor = dataDescriptor(crc, actual);
        await writer.write(descriptor);
        offset += BigInt(descriptor.length);
        central.push({ path: pathBytes, crc, size: actual, offset: localOffset });
      }

      const centralOffset = offset;
      for (const entry of central) {
        const record = centralHeader(entry);
        await writer.write(record);
        offset += BigInt(record.length);
      }
      const end = endRecords(BigInt(central.length), offset - centralOffset, centralOffset, offset);
      await writer.write(end);
      await writer.close();
    } catch (error) {
      if (activeReader) await activeReader.cancel(error).catch(() => undefined);
      await writer.abort(error).catch(() => undefined);
      throw error;
    }
  })();
  return { body: channel.readable as ReadableStream<Uint8Array>, completed };
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
  const output = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}
