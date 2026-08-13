// pulzZ WASM/JS binding — Node.js example.
//
// Build the WASM first (requires clang + wasm32 target):
//   wasm-pack build --target nodejs bindings/wasm
//
// Run:
//   node bindings/wasm/examples/node_example.mjs

import { PulzzWasmClient, pulzz_version } from '../pkg/pulzz_wasm.js';

console.log('pulzZ WASM/JS example (Node.js)');
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
  await client.send(1n, new Uint8Array([1, 2, 3]));
} catch (e) {
  console.log('  send (before connect) failed as expected:', e?.message ?? e);
}

try {
  await client.send_batch([
    { itemId: 1n, payload: new Uint8Array([1]) },
    { itemId: 2n, payload: new Uint8Array([2]) },
  ]);
} catch (e) {
  console.log('  send_batch (before connect) failed as expected:', e?.message ?? e);
}

await client.close();
console.log('pulzZ WASM/JS example complete');
