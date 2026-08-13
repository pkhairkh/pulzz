# Architecture

## Wire savings

Batched emission achieves positive wire savings by amortizing per-item overhead
(~94 B AEAD + header + transport) and compressing the entire batch as one zstd
stream to exploit cross-item redundancy.

| Mode | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item | -105.4% | -92.6% | -87.5% |
| Batched (10) | +20.7% | +19.9% | +17.4% |
| Batched (20) | +30.9% | +29.3% | +26.7% |
| Batched (50) | +36.7% | +35.6% | +32.5% |
| PQ batched | +95.7% (compressible corpus) | — | — |

## Layers

### 1. PQC session

ML-KEM-768 (FIPS-203) key encapsulation, ML-DSA-65 (FIPS-204) signatures,
ChaCha20-Poly1305 AEAD, HKDF-SHA256 KDF. 4-message mutual auth handshake with
transcript binding. See `SECURITY.md`.

### 2. Record framing

Postcard-encoded records, AEAD-protected transport frames. Two emission modes:

- `ExactState` — one record per item
- `BatchEnvelope` — one record wrapping N items, compressed as a single zstd stream

### 3. Compression

zstd dictionary transport, binary delta, structural templates, columnar batches,
adaptive strategy selection. On WASM, compression is passthrough (zstd-sys
requires clang for wasm32).

### 4. Router

UCB1 multi-armed bandit (Auer 2002) with O(sqrt(N log N)) regret bound.
Prediction Suffix Tree (Begleiter 2004) predicts next route family; at ≥ 0.9
confidence the planner short-circuits to DirectState.

## Test coverage

215 tests, 0 failures, 0 warnings. Includes 298-mutation transcript-binding
property test, 61K-case fuzz harness, 10K-pull UCB1 regret test, batch
round-trip tests, PQ batched benchmark.
