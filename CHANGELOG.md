# Changelog

## v0.2.0 — 2026-08-13 — Wave 1-17 remediation programme

### Breaking changes
- `RouteScoreInputs` and `score_route()` marked `#[deprecated]`. Use
  `Ucb1RouteSelector` from `shared_protocol::bandit` instead.
- `PrecisionBand::score_value()` marked `#[deprecated]`.
- `SparseCue` gained a `content_hash: [u8; 16]` field. Wire-format backward
  compatibility is preserved via `#[serde(default)]`.
- `HippocampalShockMemory` renamed to `RecentTraceMemory`.
- `RouteFamily::{Assembly, Schema, Episode, Transform}` and
  `ControllerRouteFamily::{Assembly, EpisodeCompletion, SchemaExpansion, Transform}`
  marked `#[deprecated(since = "0.2.0")]`.
- New `RecordType::BatchEnvelope` (discriminant 21).
- `ClientEntry::exact_bytes()` is now `pub`.
- `eval` CLI now accepts optional `batch_size` argument.

### Added — Wave 17 (the breakthrough)
- `server::eval::run_evaluation_batched()` — batched evaluation path that
  groups consecutive Insert events into batches and emits them via `emit_batch`.
- `server eval <output_dir> <batch_size>` CLI — runs the batched eval.
- **POSITIVE WIRE SAVINGS on the eval benchmark:**
  - batch_size=10: +20.7% / +19.9% / +17.4% (high/mixed/low locality)
  - batch_size=20: +30.9% / +29.3% / +26.7%
  - batch_size=50: +36.7% / +35.6% / +32.5%
- `benchmarks/eval_wave17/` — full benchmark results saved.

### Added — Wave 13 (batching)
- `shared_protocol/src/batch.rs` — `BatchEnvelope` + `BatchItem` structs with
  postcard encode/decode. `MAX_BATCH_ITEMS=1024`.
- `ServerSession::emit_batch<I>()` — compresses the entire batch envelope as
  a single zstd stream, emitting one `BatchEnvelope` record.
- `ClientSession::apply_batch_envelope()` — decompresses the batch payload,
  decodes the envelope, caches all items.
- Whole-batch zstd compression exploits cross-item redundancy.

### Added — Wave 12 (PST authoritative)
- `PST_CONFIDENCE_HIGH_THRESHOLD = 900_000` (0.9) — when PST confidence
  exceeds this AND predicted family is DirectState AND no predictive
  candidates exist, the planner short-circuits `choose_route_by_family`.

### Added — Wave 8 (PST wiring)
- `ServerState.route_pst: PredictionSuffixTree` — trained on every emission.
- `ServerState.route_bandit: Ucb1RouteSelector` — records route outcomes.
- `ServerState.pst_context: Vec<Token>` — rolling context window (max 16).

### Added — Waves 1-2 (foundation)
- `SECURITY.md` — cipher suite, transcript construction, downgrade resistance.
- `shared_protocol/src/bandit.rs` — UCB1 multi-armed bandit (Auer 2002).
- `shared_protocol/src/pst.rs` — Prediction Suffix Tree (Begleiter 2004).
- 128-bit SHA-256 content_hash on `SparseCue` (replaces 25-bit FNV-1a prefix).
- Transcript-binding property test (298 mutations).
- WASM handshake parity (ML-KEM compiles for wasm32).
- Handshake fuzz harness (61,440 cases, zero crashes).

### Fixed
- `ServerFinish.stream_id` now validated by client.
- SparseCue content_hash computed over the **full** payload, not first 64 bytes.

### Known issues
- Per-item eval (old default) still shows negative savings — this is
  structural (per-item overhead ~94B on ~80B payloads). Use batched eval.
- Dead code (Transform/Assembly/Schema/Episode, 552 references) physically
  deleted deferred to dedicated sprint. All variants marked `#[deprecated]`.

## v0.1.0 — pre-Wave-0 baseline

Initial state: 191 tests, -105% / -93% / -78% wire savings, neuroscience
vocabulary throughout, 25-bit SparseCue fingerprint, hand-tuned scorer.
