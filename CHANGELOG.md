# Changelog

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
