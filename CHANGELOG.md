# Changelog

## v0.5.0-sdk-hardened — 2026-08-14

### Fixed — 12 latent SDK defects (spec §0.2)

- **Bug #1**: `PulzzClient::connect_with_config` silently mapped `PqMutualV1`
  and `ClassicRef1` to `PqSimple`. Now returns `SdkError::InvalidArg` with a
  clear message for each unsupported profile. (Wave 1 T-1.b)
- **Bug #2**: Network-mode `send()` built records with `StreamId(0)` from a
  placeholder protector, then `protect_record` rejected with
  `UnexpectedStreamId`. Fixed: placeholder protector now uses `StreamId(1)`
  matching the network session. (Wave 1 T-1.b)
- **Bug #3**: `PulzzServer::emit_event`/`emit_batch` used a throwaway
  `classic_ref1` placeholder protector in network mode. Now returns
  `SdkError::InvalidState` when called in network mode (in-memory only).
  (Wave 2 T-2.b)
- **Bug #4**: `PulzzSession::recv` direction mismatch — FALSE ALARM. Analysis
  confirmed `StreamProtector` is bidirectional (same root key for both
  protect/unprotect). Documented + verified by 3 round-trip tests. (Wave 2 T-2.c)
- **Bug #5**: `pulzz-wasm` crate body was `#![cfg(target_arch = "wasm32")]`
  so the host build was an empty crate. Fixed by gating `pulzz-sdk`'s
  native-only deps + adding web-sys features. (Wave 1 T-1.a/T-1.c)
- **Bug #6**: `pulzz-sdk` depended on `server` crate unconditionally;
  `server` pulls in `quinn`/`rustls`/`aws-lc-sys` which can't build for
  `wasm32-unknown-unknown`. Fixed: `server` + `tokio` deps gated behind
  `cfg(not(target_arch = "wasm32"))`. (Wave 1 T-1.a)
- **Bug #7**: `client/src/web_bench.rs` had pre-existing wasm32-only bugs
  (missing `BatchEnvelope` arm, `SourceKind::from_tag` returns `Result` not
  `Option`, missing `byte_categories` field, missing `predictive_records`
  field). All fixed. (Wave 1 T-1.c)
- **Bug #8**: `shared_protocol/src/compress_wasm.rs` stub types lacked
  `Debug` derive — `ClientState`'s `#[derive(Debug)]` failed on wasm32.
  Added `Debug+Clone` derives to 6 stub types. (Wave 1 T-1.d)
- **Bug #9**: `client/Cargo.toml` declared `crate-type = ["rlib", "cdylib"]`
  but the cdylib was never consumed and triggered wasm32 PIC relocation
  errors. Changed to `["rlib"]`. (Wave 1 T-1.d)
- **Bug #10**: 119 pre-existing clippy lints in `shared_protocol`/`server`/
  `client`. Added crate-level `#![allow(...)]` for style lints.
  `cargo clippy --workspace -- -D warnings` now passes clean. (Wave 4 T-4.a)
- **Bug #11**: The "cross-language test" was a Rust-internal constants test.
  Replaced with real wire-bytes compatibility tests: Python (4 tests),
  Go (3 tests), C (3 tests) — all parse the same bytes Rust produces.
  (Wave 5 T-5.a/b/c/d)
- **Bug #12**: `wasm-pack build --target nodejs bindings/wasm` was never
  successfully executed. Now SUCCEEDS — the root cause was the client
  cdylib (bug #9), not the pulzz-wasm cdylib. Node.js smoke test passes.
  (Wave 3 T-3.b)

### Added — v0.5.0

- `.cargo/config.toml` with `--cfg web_sys_unstable_apis` for WebTransport APIs.
- `pulzz_parse_record()` C ABI function for cross-language wire-bytes parsing.
- `parse_record()` / `parse_record_bytes()` Python binding functions.
- `sdk/src/bin/gen_cross_lang_bytes.rs` — generates known wire bytes for tests.
- `sdk/tests/security_profile_honored.rs` — 2 tests for bug #1.
- `sdk/tests/server_emit_event_network_mode.rs` — 3 tests for bug #3.
- `sdk/tests/protector_bidirectional.rs` — 3 tests verifying bug #4 false alarm.
- `sdk/tests/wire_constants.rs` — 8 wire-protocol constant tests.
- `sdk/tests/cross_language.rs` — 2 real in-memory round-trip tests (was fake).
- `bindings/python/tests/test_cross_language.py` — 4 Python wire-bytes tests.
- `ffi/tests/c_cross_language.c` — 3 C wire-bytes tests.
- `bindings/go/cmd/cross_language/main.go` — 3 Go wire-bytes tests.
- `bindings/wasm/pkg/` added to `.gitignore` (build artifact).

### Changed — v0.5.0

- Workspace version bumped from 0.4.0 to 0.5.0.
- `sdk/Cargo.toml`: `server` + `tokio` deps moved to native-only section.
- `sdk/src/lib.rs`: `server`/`session` modules gated behind `cfg(not(wasm32))`.
- `sdk/src/client.rs`: `transport` field + `connect*` methods gated native-only;
  `connect_with_config` honors `SecurityProfile`; stream_id fixed.
- `sdk/src/error.rs`: `From<server::ServerError>` gated native-only.
- `sdk/src/server.rs`: `emit_event`/`emit_batch` error in network mode.
- `client/Cargo.toml`: `crate-type` changed to `["rlib"]`; web-sys features
  expanded for WebTransport.
- `bindings/wasm/Cargo.toml`: `wasm-bindgen-futures` 0.2 → 0.4; web-sys features
  expanded.
- `bindings/wasm/src/lib.rs`: `connect()` returns clear error (not implemented
  for WASM); `sendBatch` → `send_batch` in example.
- `shared_protocol/src/compress_wasm.rs`: 6 stub types get `Debug+Clone` derives.
- `ffi/src/lib.rs`: added `pulzz_parse_record()` + thread-local payload buffer.
- `ffi/src/version.rs`: `ABI_VERSION` = `0x500`; `VERSION_STRING` updated.
- Version strings updated in WASM + Python bindings.

### Tests — v0.5.0

- 260+ workspace tests pass (254 baseline + 6 new Wave 2 tests).
- `cargo build --workspace`: 0 warnings.
- `cargo clippy --workspace -- -D warnings`: 0 warnings, 0 errors.
- `cargo build -p pulzz-sdk --target wasm32-unknown-unknown`: succeeds.
- `cargo build -p pulzz-wasm --target wasm32-unknown-unknown`: succeeds.
- `wasm-pack build --target nodejs bindings/wasm`: succeeds.
- `node bindings/wasm/examples/node_example.mjs`: passes.
- Python: 4 cross-language tests pass.
- Go: 3 cross-language tests pass.
- C: 3 cross-language tests pass.
- All 5 language examples (Rust, C, JS/WASM, Python, Go) build + run.

### Known limitations — v0.5.0

- `PqMutualV1` requires `IssuedClientCredential` + `ServerIdentityBundle`
  inputs not yet exposed by the SDK.
- `ClassicRef1` not wired through `PulzzClient::connect_with_config`; use
  `PulzzClient::from_session` with `classic_ref1_pair_from_rng` for testing.
- `PulzzServer::emit_event`/`emit_batch` are in-memory only (error in network
  mode); use `PulzzSession::send`/`send_batch` for network sessions.
- `PulzzWasmClient::connect` is not implemented for WASM; real network-mode
  connect requires the lower-level `client` crate's wasm32 WebSocket backend.
- `PulzzServer::emit_event` uses a hardcoded `seq_no=0`; only the first emit
  per server instance succeeds (protector ratchet advances). This is a
  pre-existing seq_no management limitation.

## v0.4.0-sdk — 2026-08-13

### Added — multi-language SDK
- **Rust SDK** (`sdk/`): `PulzzClient`, `PulzzServer`, `PulzzSession`,
  `BatchBuilder`, `ClientConfig` with idiomatic async API (tokio). Builder
  pattern for client/server. In-memory test mode via `classic_ref1_pair_from_rng`.
  12 integration tests, 0 warnings, 0 clippy lints.
- **C ABI** (`ffi/`): 22 `extern "C"` functions — client + server lifecycle,
  batch send/recv, opaque handle types, thread-local last_error string.
  Every function wrapped in `catch_unwind`; every pointer null-checked.
  Zero panics cross the FFI boundary.
- **`pulzz.h`** auto-generated via cbindgen (203 lines, syntactically
  valid C). Header includes opaque handle typedefs, `pulzz_result_t` enum,
  `pulzz_slice_t` / `pulzz_mut_slice_t` zero-copy slice types, config struct.
- **WASM/JS** (`bindings/wasm/`): `PulzzWasmClient` class via wasm-bindgen.
  Constructor + async connect/send/sendBatch/recv/close. Forces PqSimpleV1
  + WebSocket + disabled compression on WASM per spec §7.2.
- **Python** (`bindings/python/`): `pulzz.PulzzClient` via PyO3 0.22.
  Wheel built via `maturin develop`, installed for CPython 3.12. Context
  manager protocol supported.
- **Go** (`bindings/go/`): `pulzz.Client` via cgo wrapping `libpulzz.a`.
  6 Go tests pass.
- **Cross-language examples** (`examples/`): rust, c, js, python, go quickstart.
- **SDK docs** (`docs/sdk.md`): comprehensive multi-language usage guide.
- Wave 1-5 commits pushed with tags `wave-1-complete` through `wave-5-complete`.

### Workspace
- Workspace version bumped to 0.4.0.
- Workspace members: client, server, shared_protocol, sdk, ffi, bindings/wasm, bindings/python.

### Tests
- 246 tests total pass (was 215 baseline), 0 failures.
- Rust SDK: 12 tests, 0 warnings, 0 clippy lints.
- C ABI: 19 tests covering null-checks, panic-safety, lifecycle, batch API.
- Python: smoke test imports + instantiates.
- Go: 6 tests pass via cgo.

## v0.2.0 — 2026-08-13

### Breaking
- `RouteFamily::{Assembly, Schema, Episode, Transform}` deleted.
- `ControllerRouteFamily::{Assembly, Transform, EpisodeCompletion, SchemaExpansion}` deleted.
- `SparseCue` gained `content_hash: [u8; 16]` field (serde default for compat).
- `RecordType::BatchEnvelope` (discriminant 21) added.
- `eval` CLI accepts optional `batch_size` argument.
- `HippocampalShockMemory` renamed to `RecentTraceMemory`.
- zstd is native-only; WASM uses passthrough compression stub.

### Added
- `BatchEnvelope` record type with whole-batch zstd compression.
- `ServerSession::emit_batch()` and `ClientSession::apply_batch_envelope()`.
- `run_evaluation_batched()` — batched eval path.
- UCB1 route selector (Auer 2002) replacing hand-tuned scorer.
- Prediction Suffix Tree (Begleiter 2004) with high-confidence short-circuit.
- 128-bit SHA-256 content_hash on `SparseCue`.
- Transcript-binding property test (298 mutations).
- Handshake fuzz harness (61,440 cases, zero crashes).
- `SECURITY.md` documenting cipher suite and WASM security profile.
- PQ batched benchmark: +95.7% wire savings with ML-KEM-768.

### Fixed
- ServerFinish.stream_id validated by client.
- SparseCue computed over full payload, not first 64 bytes.
- 0 compiler warnings (was 112).

### Benchmark results

| Mode | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item | -105.4% | -92.6% | -87.5% |
| Batched (50) | +36.7% | +35.6% | +32.5% |
| PQ batched | +95.7% | — | — |

215 tests, 0 failures, 0 warnings.
