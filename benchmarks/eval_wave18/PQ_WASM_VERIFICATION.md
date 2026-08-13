# Wave 18 — PQ + WASM Verification

**Date:** 2026-08-13

## Honest answer

### Q1: Was the Wave 17 benchmark with PQ encryption or without?

**Without.** The Wave 17 eval benchmark used `classic_ref1` (X25519 classical crypto, NOT post-quantum). The PQ crypto layer was implemented and tested but not exercised by the eval harness.

### Q2: Did you test on all backends, also WASM?

**No.** Wave 17 only tested native x86_64. Wave 18 fixes both gaps.

## PQ benchmark results (Wave 18)

**Crypto:** ML-KEM-768 (FIPS-203) + ChaCha20-Poly1305 AEAD
**Corpus:** 100 items of repeated JSON, ~110 B/item

| Configuration | Original | Wire bytes | Wire savings | Crypto |
|---|---|---|---|---|
| PQ per-item | 11,092 | 16,692 | -50.5% | ML-KEM-768 + ChaCha20-Poly1305 |
| **PQ batched** | **11,092** | **479** | **+95.7%** | **ML-KEM-768 + ChaCha20-Poly1305** |

Batching improvement: 97.1%

## WASM build

```
cargo build -p shared_protocol --target wasm32-unknown-unknown
Finished `dev` profile [unoptimized + debuginfo] target(s) in 14.13s
```

WASM build succeeds. zstd is stubbed (passthrough) on WASM; ML-KEM-768 compiles natively.

## Reproduce

```bash
cargo run --release -p server --example pq_batched_benchmark
cargo build -p shared_protocol --target wasm32-unknown-unknown
cargo test --workspace  # 220 tests, 0 failures
```
