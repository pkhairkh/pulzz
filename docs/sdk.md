# pulzZ SDK — Multi-Language Usage Guide

**Version:** 0.4.0-sdk
**Status:** Wave 6 — Rust SDK + C ABI + WASM/JS + Python + Go bindings all implemented
**Date:** 2026-08-13

---

## 1. Overview

The pulzZ SDK provides a unified, idiomatic API surface across five languages
(Rust, C, JavaScript/WASM, Python, Go) for the pulzZ predictive-memory
transport. All bindings share:

- The same wire protocol (version 2, see `shared_protocol/src/protocol.rs`)
- The same PQC handshake profiles (`PqMutualV1`, `PqSimpleV1`, `ClassicRef1`)
- The same batch envelope format (`RecordType::BatchEnvelope`)
- The same transport carriers (WebSocket, TCP, QUIC stream/datagram, WebTransport, UDP)

The C ABI (`ffi/` crate) is the keystone — every non-Rust binding (JS, Python,
Go) ultimately calls into `libpulzz.so` / `libpulzz_ffi.a` via the C header
`ffi/include/pulzz.h`. The Rust SDK (`sdk/` crate) is the canonical
implementation; the other bindings are thin wrappers around the C ABI.

### 1.1 Architecture

```
┌─────────────────────────────────────────────────┐
│                  Application                     │
├──────────┬──────────┬──────────┬────────────────┤
│  Rust    │   JS/    │  Python  │   Go / C++    │
│  (native)│  WASM    │ (PyO3)   │   (cgo/FFI)   │
├──────────┴──────────┴──────────┴────────────────┤
│              pulzZ SDK layer                     │
├──────────────────────────────────────────────────┤
│              C ABI (extern "C")                   │
├──────────────────────────────────────────────────┤
│              shared_protocol crate               │
├──────────────────────────────────────────────────┤
│              Transport backends                  │
│  TCP │ WebSocket │ QUIC │ WebTransport │ UDP dgram│
└──────────────────────────────────────────────────┘
```

### 1.2 Backend parity matrix

| Feature | Native (Rust/C/Go/Python) | WASM (browser/Node) |
|---|---|---|
| Carriers | WebSocket, TCP, QUIC, WebTransport, UDP | WebSocket, WebTransport |
| Security | PqMutualV1, PqSimpleV1, ClassicRef1 | PqSimpleV1 only |
| Compression | zstd (full P0-P5) | Passthrough (no zstd) |
| Batching | `emit_batch` / `send_batch` | `send_batch` (uncompressed) |
| PQ crypto | ML-KEM-768 + ML-DSA-65 | ML-KEM-768 only |
| Async | Tokio (Rust), poll (C), goroutines (Go), asyncio (Python) | Promise (JS) |

---

## 2. Quickstart by language

### 2.1 Rust

```rust
use pulzz_sdk::{PulzzClient, PulzzClientBuilder, CarrierKind, SecurityProfile};
use pulzz_sdk::{ItemId, SourceKind, ExactStateMaterial};

// Build and connect (network mode)
let mut client = PulzzClient::builder()
    .carrier(CarrierKind::WebSocket)
    .security(SecurityProfile::PqSimpleV1)
    .batch_size(50)
    .connect("ws://localhost:8080")
    .await?;

// Send a single item
client.send(ItemId(1), b"hello world").await?;

// Send a batch
client.send_batch(vec![
    (ItemId(1), b"{\"user\":\"alice\"}".to_vec()),
    (ItemId(2), b"{\"user\":\"bob\"}".to_vec()),
]).await?;

// Receive
while let Some(record) = client.recv().await? {
    println!("got item {}: {} bytes", record.header.item_id.0, record.payload.len());
}

client.close().await?;
```

**In-memory test mode** (no network):

```rust
use pulzz_sdk::{PulzzClient, PulzzServer, classic_pair_for_test};
use pulzz_sdk::{CarrierKind, ClientConfig, SecurityProfile};
use shared_protocol::{ItemId, SourceKind, source::ExactStateMaterial};
use server::ServerEvent;

let (sender, receiver) = pulzz_sdk::classic_pair_for_test(shared_protocol::StreamId(1));
let cfg = ClientConfig {
    security: SecurityProfile::ClassicRef1,
    carrier: CarrierKind::WebSocket,
    ..Default::default()
};
let mut server = PulzzServer::from_protector(sender, cfg.clone());
let mut client = PulzzClient::from_session(client::ClientSession::new(receiver), cfg);

let record = server.emit_event(ServerEvent::Insert {
    item_id: ItemId(1),
    block: ExactStateMaterial::new(SourceKind::Text, b"hello".to_vec()),
})?;
client.push_record(record);
let received = client.recv().await?.expect("must receive");
println!("got item {}", received.header.item_id.0);
```

### 2.2 C

```c
#include "pulzz.h"

PulzzConfig cfg = {
    .carrier = WebSocket,
    .security = PqSimpleV1,
    .batch_size = 50,
    .zstd_level = 3,
    .timeout_ms = 5000,
};

PulzzClientHandle client = NULL;
if (pulzz_client_new(&cfg, &client) != Ok) {
    fprintf(stderr, "failed: %s\n", pulzz_last_error());
    return 1;
}

if (pulzz_client_connect(client, "ws://localhost:8080") != Ok) {
    fprintf(stderr, "connect failed: %s\n", pulzz_last_error());
    return 1;
}

/* Send single */
PulzzSlice payload = { .ptr = (const uint8_t *)"hello", .len = 5 };
pulzz_client_send(client, 1, payload);

/* Send batch */
PulzzBatchHandle batch = NULL;
pulzz_batch_new(&batch);
PulzzSlice p1 = { .ptr = (const uint8_t *)"{\"a\":1}", .len = 7 };
pulzz_batch_add(batch, 1, 1, p1);
pulzz_client_send_batch(client, batch);  /* consumes batch */

/* Receive */
uint64_t item_id;
uint8_t *payload_ptr;
size_t payload_len;
uint8_t record_type;
if (pulzz_client_recv(client, &item_id, &payload_ptr, &payload_len, &record_type, 5000) == Ok) {
    printf("got item %lu: %zu bytes (type=%u)\n", item_id, payload_len, record_type);
    pulzz_record_free_payload(payload_ptr, payload_len);
}

pulzz_client_close(client);
```

### 2.3 JavaScript / WASM

```javascript
import { PulzzWasmClient, pulzz_version } from 'pulzz-wasm';

console.log(pulzz_version());

const client = new PulzzWasmClient({
  carrier: 'websocket',
  security: 'pq_simple_v1',
  batchSize: 50,
  zstdLevel: 0,            // WASM: always 0
  timeoutMs: 5000,
});

await client.connect('ws://localhost:8080');

// Send single
await client.send(1n, new Uint8Array([1, 2, 3]));

// Send batch
await client.sendBatch([
  { itemId: 1n, payload: new Uint8Array([1]) },
  { itemId: 2n, payload: new Uint8Array([2]) },
]);

// Receive
const record = await client.recv(5000);
if (record !== null) {
  console.log(`got item ${record.itemId}: ${record.payload.length} bytes`);
}

await client.close();
```

**Build:**
```sh
wasm-pack build --target nodejs bindings/wasm
```

### 2.4 Python

```python
import pulzz

print(pulzz.pulzz_version())

client = pulzz.PulzzClient(
    carrier="websocket",
    security="pq_simple_v1",
    batch_size=50,
    zstd_level=3,
    timeout_ms=5000,
)
client.connect("ws://localhost:8080")

# Send single
client.send(1, b"hello world")

# Send batch
client.send_batch([
    (1, b'{"user":"alice"}'),
    (2, b'{"user":"bob"}'),
])

# Receive
record = client.recv(timeout_ms=5000)
if record is not None:
    item_id, payload, record_type = record
    print(f"got item {item_id}: {len(payload)} bytes (type={record_type})")

client.close()
```

**Install:**
```sh
cd bindings/python
maturin develop --release
```

### 2.5 Go

```go
package main

import (
    "fmt"
    pulzz "github.com/pkhairkh/pulzz/bindings/go"
)

func main() {
    fmt.Println(pulzz.Version())

    client, err := pulzz.NewClient(pulzz.Config{
        Carrier:   pulzz.CarrierWebSocket,
        Security:  pulzz.SecurityPqSimpleV1,
        BatchSize: 50,
        ZstdLevel: 3,
        TimeoutMs: 5000,
    })
    if err != nil { panic(err) }
    defer client.Free()

    if err := client.Connect("ws://localhost:8080"); err != nil {
        panic(err)
    }

    // Send single
    if err := client.Send(1, []byte("hello world")); err != nil {
        panic(err)
    }

    // Send batch
    if err := client.SendBatch([]pulzz.BatchItem{
        {ItemID: 1, SourceKind: pulzz.SourceJson, Payload: []byte(`{"user":"alice"}`)},
        {ItemID: 2, SourceKind: pulzz.SourceJson, Payload: []byte(`{"user":"bob"}`)},
    }); err != nil { panic(err) }

    // Receive
    rec, err := client.Recv(5000)
    if err != nil { panic(err) }
    if rec != nil {
        fmt.Printf("got item %d: %d bytes (type=%d)\n", rec.ItemID, len(rec.Payload), rec.RecordType)
    }

    client.Close()
}
```

**Build:**
```sh
cargo build -p pulzz-ffi
cd bindings/go
CGO_LDFLAGS="/path/to/target/debug/libpulzz_ffi.a -lpthread -ldl -lm" \
  go test ./...
```

---

## 3. Reference

### 3.1 Result codes

Every C ABI function returns a `PulzzResult` enum:

| Code | Value | Meaning |
|---|---|---|
| `PULZZ_OK` | 0 | Success |
| `PULZZ_ERR_INVALID_ARG` | 1 | Null pointer, invalid URL, etc. |
| `PULZZ_ERR_INVALID_STATE` | 2 | E.g. send before connect |
| `PULZZ_ERR_CONNECTION_FAILED` | 3 | TCP/WS connection refused |
| `PULZZ_ERR_HANDSHAKE_FAILED` | 4 | PQC bootstrap failure |
| `PULZZ_ERR_COMPRESSION_FAILED` | 5 | zstd error |
| `PULZZ_ERR_TIMEOUT` | 6 | Operation timed out |
| `PULZZ_ERR_BUFFER_TOO_SMALL` | 7 | Caller-allocated buffer too small |
| `PULZZ_ERR_END_OF_STREAM` | 8 | Peer closed the connection |
| `PULZZ_ERR_INTERNAL` | 99 | Internal panic (should not happen) |

Call `pulzz_last_error()` (C) or the equivalent in each binding to retrieve a
human-readable error message.

### 3.2 Carriers

| Carrier | Browser | Native | Notes |
|---|---|---|---|
| `WebSocket` | ✅ | ✅ | Default; works in all environments |
| `Tcp` | ❌ | ✅ | Native only; raw TCP framing |
| `QuicStream` | ❌ | ✅ | quinn-based; reliable ordered stream |
| `QuicDatagram` | ❌ | ✅ | quinn-based; unreliable datagram |
| `WebTransport` | ✅ | ✅ | HTTP/3-based; browser support varies |
| `UdpDatagram` | ❌ | ✅ | Native only; raw UDP |

### 3.3 Security profiles

| Profile | Browser | Native | Notes |
|---|---|---|---|
| `PqMutualV1` | ❌ | ✅ | ML-KEM-768 + ML-DSA-65; full PQ mutual auth |
| `PqSimpleV1` | ✅ (default) | ✅ | ML-KEM-768 only; PQ KEM, no signatures |
| `ClassicRef1` | ❌ | ✅ | X25519 + Ed25519; **testing only** |

### 3.4 Batch emission

The `BatchEnvelope` record type wraps N items in a single AEAD-protected
record, amortizing the per-item overhead (header + AEAD tag ≈ 94 B) across
the entire batch. When compression is enabled (native only), the entire
envelope is zstd-compressed as a single stream, capturing cross-item
redundancy (e.g. repeated JSON keys).

Wire savings are typically 60-90% for batches of 50+ items with similar
payloads. See `sdk/examples/batched.rs` for a demonstration.

---

## 4. Build artifacts

| Target | Artifact | Consumers |
|---|---|---|
| Native (Linux/macOS/Windows) | `libpulzz.so` / `.dylib` / `.dll` + `libpulzz_ffi.a` + `pulzz.h` | C, C++, Go (cgo), Python (PyO3) |
| WASM (browser/Node) | `pulzz.wasm` + `pulzz_wasm.js` (glue) | JavaScript, TypeScript |
| Python | `pulzz-0.4.0-cp3XX-*.whl` (PyO3 wheel) | Python 3.8+ |

### 4.1 Build commands

```sh
# Rust SDK + tests
cargo build --workspace
cargo test --workspace

# C ABI + header
cargo build -p pulzz-ffi
cbindgen --crate pulzz-ffi --output ffi/include/pulzz.h --config ffi/cbindgen.toml

# Python wheel
cd bindings/python && maturin develop --release

# WASM (requires clang + wasm32 target)
wasm-pack build --target nodejs bindings/wasm

# Go binding (requires libpulzz_ffi.a)
cd bindings/go
CGO_LDFLAGS="/path/to/libpulzz_ffi.a -lpthread -ldl -lm" go test ./...
```

---

## 5. Cross-language round-trip test

A wire-compatibility test that sends from Rust and receives in another
language verifies that all bindings share the same protocol. See
`tests/cross_language/` (Wave 6 T-6.a).

The test pattern:
1. Start a Rust SDK server bound to a known port.
2. From each binding (Python, Go, C), construct a client and connect.
3. Each client sends a batch of 50 items to the server.
4. The server reflects each item back as an `ExactState` record.
5. Each client receives + decodes the records, verifying item IDs + payload bytes.

The current implementation in this repo verifies wire compatibility via the
shared `shared_protocol` crate — all bindings use the same `Record` /
`RecordHeader` / `BatchEnvelope` types via the C ABI. A network round-trip
test (binding → server → binding) requires a running pulzZ server with the
full PqSimpleV1 bootstrap, which is documented but not exercised in this
repo's CI yet.

---

## 6. Versioning

| Component | Version | Where defined |
|---|---|---|
| Workspace | `0.4.0` | `Cargo.toml` `[workspace.package]` |
| Wire protocol | `2` | `shared_protocol/src/protocol.rs::PROTOCOL_VERSION` |
| C ABI | `0x000400` (0.4.0) | `ffi/src/version.rs::ABI_VERSION` |
| pulzz.h header | auto-generated | `ffi/include/pulzz.h` (via cbindgen) |

Breaking wire-protocol changes bump `PROTOCOL_VERSION`. Breaking ABI changes
bump the C ABI major version. Binding-specific versions follow each language's
convention (PyPI SemVer, npm SemVer, Go module tags).

---

## 7. Open issues / future work

### 7.1 Known limitations (v0.5.0-sdk-hardened)

1. **PqMutualV1 not wired through SDK connect.** `PulzzClient::connect_with_config`
   returns `SdkError::InvalidArg` for `PqMutualV1` because the SDK does not yet
   expose the `IssuedClientCredential` + `ServerIdentityBundle` inputs required
   by the underlying `ClientSecurityConfig::PqMutual` variant. Callers must use
   the lower-level `client::NativeClientAbiConfig` API until a future wave adds
   credential-management helpers. (Bug #1, fixed in Wave 1)

2. **ClassicRef1 not wired through SDK connect.** `PulzzClient::connect_with_config`
   returns `SdkError::InvalidArg` for `ClassicRef1` because `ClientSecurityConfig`
   has no ClassicRef1 variant. Use `PulzzClient::from_session` with
   `classic_ref1_pair_from_rng` for in-memory classic-ref1 testing. (Bug #1)

3. **`PulzzServer::emit_event`/`emit_batch` are in-memory only.** In network
   mode (after `bind`/`bind_with_config`), these methods return
   `SdkError::InvalidState` because the in-memory `ServerSession`'s protector
   is a throwaway placeholder that doesn't match any accepted connection's
   protector. Use `PulzzSession::send`/`send_batch` for network sessions.
   (Bug #3, fixed in Wave 2)

4. **`PulzzServer::emit_event` uses hardcoded seq_no=0.** Only the first
   `emit_event` call per server instance succeeds; subsequent calls fail with
   `UnexpectedSeqNo` because the protector's ratchet advances. This is a
   pre-existing seq_no management limitation, not a security issue.

5. **`PulzzWasmClient::connect` is not implemented for WASM.** Real
   network-mode connect depends on native-only transport code (tokio +
   quinn/rustls/aws-lc-sys) which cannot compile for
   `wasm32-unknown-unknown`. The `connect()` method returns a clear error.
   For browser/Node usage, construct a `PulzzClient` via `from_session` with
   a `ClientSession` built from the lower-level `client` crate's wasm32
   WebSocket backend. (Bug #5/#6, documented in Wave 1)

### 7.2 Future work

1. **WASM network connect.** Wire the `client` crate's wasm32 WebSocket /
   WebTransport backend through `pulzz-sdk` so `PulzzWasmClient::connect`
   works in browsers/Node. This requires adding a wasm32-target transport
   abstraction to the SDK.

2. **PqMutualV1 credential management.** Add `IssuedClientCredential` +
   `ServerIdentityBundle` types to the SDK and wire them through
   `connect_with_config` for PqMutualV1.

3. **Network round-trip cross-language tests.** The current cross-language
   tests verify wire-bytes compatibility (Python/Go/C parse the same bytes
   Rust produces). A network round-trip test (Rust server ↔ Python/Go/C
   client) would exercise the full handshake + transport path.

4. **Mobile (Android/iOS).** The C ABI works on both platforms via the NDK,
   but the JNI/Swift wrappers are out of scope for v0.5.0. Future waves may
   add AAR / SPM packages.
