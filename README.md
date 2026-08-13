# pulzZ

**Post-quantum secure compression-aware transport with batched emission and learned route selection.**

pulzZ is a Rust implementation of a mutually-authenticated post-quantum
session layer (ML-KEM-768 + ML-DSA-65 + ChaCha20-Poly1305) over which it
transports exact-state payloads with adaptive compression, item batching,
and UCB1-based route selection.

## Wire savings — POSITIVE on batched workloads

| Configuration | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item (old default) | -105.4% | -92.6% | -87.5% |
| **Batched (batch_size=10)** | **+20.7%** | **+19.9%** | **+17.4%** |
| **Batched (batch_size=20)** | **+30.9%** | **+29.3%** | **+26.7%** |
| **Batched (batch_size=50)** | **+36.7%** | **+35.6%** | **+32.5%** |

Exact round-trip rate: 1.000000 (all configurations).

<!-- source: benchmarks/eval_wave17/SUMMARY.md -->

## Architecture

Four layers:

1. **PQC session layer** (`shared_protocol/src/bootstrap.rs`,
   `shared_protocol/src/protection.rs`) — 4-message mutual auth with
   ML-KEM-768 + ML-DSA-65 + ChaCha20-Poly1305 + HKDF-SHA256. See `SECURITY.md`.

2. **Record framing** (`shared_protocol/src/protocol.rs`,
   `shared_protocol/src/transport.rs`) — postcard-encoded records with
   AEAD-protected transport frames. Supports per-item (`ExactState`) and
   batched (`BatchEnvelope`) emission.

3. **Compression pipeline** (`shared_protocol/src/compress.rs`) — zstd
   dictionary transport, binary delta, structural templates, columnar batches,
   adaptive strategy selection. The batched path compresses the entire batch
   envelope as a single zstd stream for cross-item redundancy exploitation.

4. **Predictive router** (`shared_protocol/src/bandit.rs`,
   `shared_protocol/src/pst.rs`, `shared_protocol/src/chpmt.rs`) — UCB1
   multi-armed bandit (Auer 2002) with `O(sqrt(N log N))` regret bound.
   Prediction Suffix Tree (Begleiter 2004) for next-route prediction with
   high-confidence short-circuit at ≥ 0.9 confidence.

## Quick start

```bash
# Build
cargo build --release

# Run per-item eval (baseline)
cargo run --release -p server -- eval artifacts/eval_per_item

# Run batched eval (the path with positive savings)
cargo run --release -p server -- eval artifacts/eval_batched_50 50

# Run tests (220 tests, 0 failures)
cargo test --workspace
```

## Workspace layout

- `shared_protocol/` — protocol types, PQC layer, compression, bandit, PST, batch
- `server/` — server session, emission, batched emission, eval harness, benchmarks
- `client/` — client session, record application, batch decode, decompression

## Documentation

- `SECURITY.md` — cipher suite, transcript construction, downgrade resistance
- `docs/architecture.md` — current architecture
- `benchmarks/eval_wave17/SUMMARY.md` — full benchmark results
- `CHANGELOG.md` — version history
- `extend.md` — historical CHPMT specification (superseded; retained for reference)

## Key references

- FIPS-203 (ML-KEM), FIPS-204 (ML-DSA), RFC 8446 (TLS 1.3)
- Auer, Cesa-Bianchi & Fischer 2002 (UCB1 bandit)
- Begleiter, El-Yaniv & Yona 2004 (PST, arxiv 1107.0051)
- Shannon 1948 (content-addressable codes)
- Tridgell 1999 (rsync), Percival 2003 (bsdiff)
- IETF Compression Dictionary Transport

## License

MIT OR Apache-2.0
