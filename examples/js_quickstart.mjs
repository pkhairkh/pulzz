// pulzZ JS quickstart.
//
// Cross-language example demonstrating the minimal Node.js client lifecycle.
// Requires the WASM package to be built first:
//   wasm-pack build --target nodejs bindings/wasm
//
// Run: node examples/js_quickstart.mjs

import { PulzzWasmClient, pulzz_version } from '../bindings/wasm/pkg/pulzz_wasm.js';

console.log('pulzZ JS quickstart');
console.log('  version =', pulzz_version());

const client = new PulzzWasmClient({
  carrier: 'websocket',
  security: 'pq_simple_v1',
  batchSize: 50,
  zstdLevel: 0,
  timeoutMs: 1000,
});
console.log('  client constructed');

try {
  await client.connect('ws://127.0.0.1:1');
} catch (e) {
  console.log('  connect (to closed port) failed as expected:', e?.message ?? e);
}

try {
  await client.sendBatch([
    { itemId: 1, payload: new Uint8Array([1, 2, 3]) },
    { itemId: 2, payload: new Uint8Array([4, 5, 6]) },
  ]);
} catch (e) {
  console.log('  sendBatch (before connect) failed as expected:', e?.message ?? e);
}

await client.close();
console.log('pulzZ JS quickstart complete');
