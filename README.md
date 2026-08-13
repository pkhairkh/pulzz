# pulzZ

Post-quantum secure transport with batched emission and learned route selection.

## Wire savings

| Mode | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item | -105.4% | -92.6% | -87.5% |
| **Batched (50)** | **+36.7%** | **+35.6%** | **+32.5%** |
| **PQ batched** | — | **+95.7%** | — |

Exact round-trip rate: 1.000000. Crypto: ML-KEM-768 + ChaCha20-Poly1305.

## Build

```bash
cargo build --release
cargo test --workspace          # 215 tests, 0 failures, 0 warnings
```

## Run

```bash
# Per-item eval (baseline)
cargo run --release -p server -- eval artifacts/per_item

# Batched eval (positive savings)
cargo run --release -p server -- eval artifacts/batched 50

# PQ batched benchmark
cargo run --release -p server --example pq_batched_benchmark
```

## Architecture

1. **PQC session** — ML-KEM-768 + ML-DSA-65 + ChaCha20-Poly1305. 4-message mutual auth.
2. **Record framing** — postcard-encoded, AEAD-protected. Per-item (`ExactState`) or batched (`BatchEnvelope`).
3. **Compression** — zstd (native) or passthrough (WASM). Whole-batch compression exploits cross-item redundancy.
4. **Router** — UCB1 bandit (Auer 2002) + Prediction Suffix Tree (Begleiter 2004).

## Layout

- `shared_protocol/` — protocol types, PQC, compression, batch, bandit, PST
- `server/` — session, batched emission, eval harness
- `client/` — session, batch decode, decompression

## Docs

- `docs/architecture.md` — full architecture
- `SECURITY.md` — cipher suite, transcript, downgrade resistance
- `CHANGELOG.md` — version history
- `benchmarks/eval_wave17/SUMMARY.md` — full benchmark results

## License

MIT OR Apache-2.0
