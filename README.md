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
- `sdk/` — Rust SDK (idiomatic async API; wraps client + server)
- `ffi/` — C ABI (`extern "C"` + `pulzz.h` via cbindgen)
- `bindings/wasm/` — WASM/JS binding via wasm-bindgen
- `bindings/python/` — Python binding via PyO3
- `bindings/go/` — Go binding via cgo
- `examples/` — cross-language quickstart examples

## SDK

The pulzZ SDK exposes a unified, idiomatic API across five languages:
Rust (native), C, JavaScript/WASM, Python, and Go. All bindings share
the same wire protocol, PQC handshake, and batch envelope format.

**Quickstart by language:**

- **Rust:** `cargo run --example quickstart -p pulzz-sdk`
- **C:** `cc examples/c_quickstart.c -I ffi/include target/debug/libpulzz_ffi.a -lpthread -o c_quickstart && ./c_quickstart`
- **Python:** `cd bindings/python && maturin develop && python3 examples/python_quickstart.py`
- **Go:** `cd bindings/go && CGO_LDFLAGS="../target/debug/libpulzz_ffi.a -lpthread" go test ./...`
- **JS/WASM:** `wasm-pack build --target nodejs bindings/wasm && node examples/js_quickstart.mjs`

Full multi-language usage guide: [`docs/sdk.md`](docs/sdk.md).

## Docs

- `docs/sdk.md` — full multi-language SDK usage guide
- `docs/architecture.md` — full architecture
- `docs/SDK_PROPOSAL.md` — original SDK design proposal
- `SECURITY.md` — cipher suite, transcript, downgrade resistance
- `CHANGELOG.md` — version history
- `benchmarks/eval_wave17/SUMMARY.md` — full benchmark results

## License

CCL-X-1.2
