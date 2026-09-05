import { readFileSync } from "node:fs";

export default new WebAssembly.Module(readFileSync(new URL("../src/crc32.wasm", import.meta.url)));
