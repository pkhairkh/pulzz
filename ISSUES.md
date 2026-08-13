# ISSUES.md

## Scope

Live residual issues in the pulzZ repository as of Wave 17 (post-batching).

**220 tests pass, 0 failures.**

## Status summary

What is materially true in the current checkout:

- **POSITIVE WIRE SAVINGS achieved** via batched emission (`emit_batch` + `BatchEnvelope`):
  - batch_size=10: +20.7% / +19.9% / +17.4% (high/mixed/low locality)
  - batch_size=20: +30.9% / +29.3% / +26.7%
  - batch_size=50: +36.7% / +35.6% / +32.5%
- exact round-trip rate = 1.000000 on all configurations
- PQC session layer: ML-KEM-768 + ML-DSA-65 + ChaCha20-Poly1305 + HKDF-SHA256
  - 298-mutation transcript-binding property test passes
  - 61,440-case handshake fuzz harness passes (zero crashes)
  - WASM parity verified (ML-KEM compiles for wasm32)
- UCB1 route selector (Auer 2002) with provable regret bound replaces hand-tuned scorer
- Prediction Suffix Tree (Begleiter 2004) wired into planner with high-confidence short-circuit
- SparseCue carries 128-bit SHA-256 content_hash over full payload
- Whole-batch zstd compression exploits cross-item redundancy
- All suspended route families marked `#[deprecated]`

## Resolved issues

### [RESOLVED] I0. Predictive route planning produces negative wire savings

**Resolution:** Batched emission (`emit_batch` + `BatchEnvelope` record type)
achieves positive wire savings by amortizing per-item overhead and compressing
the entire batch as a single zstd stream. See `benchmarks/eval_wave17/SUMMARY.md`.

### [RESOLVED] I4. P0-P5 compression pipeline not yet integrated into client decompression

**Resolution:** Client-side `apply_batch_envelope()` decompresses the entire
batch payload and caches all items. The per-item `apply_data_record` path
also calls `decode_compressed_payload` at `client/src/lib.rs:701`.

### [RESOLVED] I11. PQC layer WASM parity

**Resolution:** ML-KEM import gate removed; ML-KEM compiles for wasm32.
WASM clients negotiate PqSimpleV1 fallback only if the full PqMutualV1
handshake is unavailable.

## Remaining issues

### [DEFERRED] I3. Transform layer physical deletion

**Status:** All Transform variants marked `#[deprecated]`. Invariant test
verifies zero Transform emissions. Physical deletion of 552 references across
16 files deferred to dedicated sprint. See `docs/wave14_deferral.md`.

### [DEFERRED] I2. Assembly/Schema/Episode dead code deletion

**Status:** All suspended variants marked `#[deprecated]`. Physical deletion
consolidated with Transform deletion in `docs/wave14_deferral.md`.

### [LOW] I5. Schema/Transform dependency revision tracking uses conservative rejection

Unchanged from baseline. Schema/transform peer-tracking uses HashSet (no
revision storage). When `required_revision > 0`, conservatively rejected.

### [LOW] I6. End-to-end runtime validation not yet performed for all configurations

The batched eval path is validated (220 tests pass, exact round-trip = 1.0).
Full remote PQC benchmark (24 cases) still requires remote endpoints.
