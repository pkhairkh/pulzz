# Changelog

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
