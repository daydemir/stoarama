import crc32Module from "./crc32.wasm";

const MAX_HEAP = 16 * 1024;

interface CRC32Exports extends WebAssembly.Exports {
  memory: WebAssembly.Memory;
  Hash_GetBuffer(): number;
  Hash_Init(polynomial: number): void;
  Hash_Update(length: number): void;
  Hash_Final(padding: number): void;
}

export class CRC32 {
  private readonly exports: CRC32Exports;
  private readonly memory: Uint8Array;

  constructor() {
    const instance = new WebAssembly.Instance(crc32Module);
    this.exports = instance.exports as CRC32Exports;
    this.memory = new Uint8Array(this.exports.memory.buffer, this.exports.Hash_GetBuffer(), MAX_HEAP);
  }

  init(): void {
    this.exports.Hash_Init(0xedb88320);
  }

  update(data: Uint8Array): void {
    for (let offset = 0; offset < data.byteLength; offset += MAX_HEAP) {
      const chunk = data.subarray(offset, offset + MAX_HEAP);
      this.memory.set(chunk);
      this.exports.Hash_Update(chunk.byteLength);
    }
  }

  digest(): number {
    this.exports.Hash_Final(0);
    return new DataView(this.exports.memory.buffer, this.exports.Hash_GetBuffer(), 4).getUint32(0, false);
  }
}
