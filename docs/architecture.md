# pulzZ Architecture

**Status:** post-Wave 17 (2026-08-13)
**Supersedes:** `extend.md` (historical CHPMT specification, retained for reference)

## Scope

pulzZ is a post-quantum secure compression-aware transport for exact-state
payloads between browser (WASM) and native clients. This document describes
the current architecture after the Wave 1-17 remediation programme.

## Wire savings

| Configuration | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item (old default) | -105.4% | -92.6% | -87.5% |
| **Batched (batch_size=10)** | **+20.7%** | **+19.9%** | **+17.4%** |
| **Batched (batch_size=20)** | **+30.9%** | **+29.3%** | **+26.7%** |
| **Batched (batch_size=50)** | **+36.7%** | **+35.6%** | **+32.5%** |

The batching path achieves positive wire savings by (1) amortizing per-item
AEAD + record header + transport overhead across all items in the batch, and
(2) compressing the entire batch envelope as a single zstd stream to exploit
cross-item redundancy.

## Four layers

### 1. PQC session layer

**Files:** `shared_protocol/src/bootstrap.rs`, `shared_protocol/src/protection.rs`

Mutually-authenticated post-quantum session establishment using ML-KEM-768
(FIPS-203), ML-DSA-65 (FIPS-204), ChaCha20-Poly1305 AEAD, and HKDF-SHA256.

4-message handshake: `ClientHello → ServerHello → ClientFinish → ServerFinish`.
The transcript hash binds all four messages into the root key derivation.

See `SECURITY.md` for the full cipher suite, transcript construction, and
downgrade resistance analysis.

### 2. Record framing

**Files:** `shared_protocol/src/protocol.rs`, `shared_protocol/src/transport.rs`,
`shared_protocol/src/batch.rs`

Postcard-encoded records with AEAD-protected transport frames. Two emission
modes:

- **`ExactState`** (per-item) — one record per item. Each record carries its
  own AEAD tag (16B), record header (32B), transport envelope (16B), and
  source descriptor (30B) = ~94B overhead per item.
- **`BatchEnvelope`** (batched) — one record wrapping N items. The entire
  batch envelope is compressed as a single zstd stream before AEAD. Per-item
  overhead drops to ~94B/N. The batch wire format is postcard-encoded:
  `[item_count: u16][item_0][item_1]...[item_N-1]` where each item is
  `[item_id: u64][source_kind: u8][payload_len: u32][payload]`.

### 3. Compression pipeline

**Files:** `shared_protocol/src/compress.rs`

Six layers, all implemented and tested:

| Layer | Component | Status |
|---|---|---|
| P0 | `ZstdDictionary` | Trained per-SourceKind, compresses with dictionary |
| P1 | `BinaryDelta` | Rolling-hash content-addressable diff for upserts |
| P2 | `StructuralTemplate` | JSON key-pattern extraction, slot encoding |
| P3 | `ColumnarBatch` | Per-column RLE/delta/zstd for bulk transfers |
| P4 | `select_strategy()` | Adaptive strategy selection |
| P5 | Compression tags | Self-describing payload tags |

The per-item path calls `compress_exact_bytes()` on every `ExactState` emission.
The batched path stores items uncompressed, then compresses the entire
postcard-encoded envelope as a single zstd stream — this captures cross-item
redundancy that per-item compression cannot.

### 4. Predictive router

**Files:** `shared_protocol/src/bandit.rs`, `shared_protocol/src/pst.rs`,
`shared_protocol/src/chpmt.rs`

**UCB1 multi-armed bandit** (`bandit.rs`) — selects between route families
with provable `O(sqrt(N log N))` regret bound per arm (Auer 2002). Replaces
the previous hand-tuned weighted-sum scorer.

**Prediction Suffix Tree** (`pst.rs`) — integer-only variable-order Markov
model per Begleiter 2004. Trained on every emission with the route-family
token. When prediction confidence ≥ 0.9 AND the predicted family is
DirectState AND no predictive candidates exist, the planner short-circuits
the heuristic `choose_route_by_family` and emits DirectState directly.

**SparseCue** (`chpmt.rs`) — 128-bit SHA-256-truncated content-addressable
hash of the full payload, plus legacy diagnostic fields.

## Test coverage

220 tests pass, 0 failures:
- 298-mutation transcript-binding property test
- 61,440-case handshake fuzz harness (zero crashes)
- 10K-pull UCB1 bandit regret test
- 8 cue_property tests including last-byte-differs
- 6 PST tests including period-3 sequence prediction
- 4 batch integration tests (round-trip, wire savings, empty batch)
- 3 PST planner integration tests
- 3 PST authoritative short-circuit tests
- 3 client decompression e2e tests
- 3 dead-code invariant tests
- Full eval benchmark (per-item + batched)

## Demoted / suspended features

- **Transform layer** — DEMOTED. Wire discriminants retained for backward
  compatibility. No code path emits Transform records.
- **Assembly / Schema / Episode** route families — suspended (marked
  `#[deprecated]`). The UCB1 bandit rarely selects them. Physical deletion
  deferred to a dedicated cleanup sprint (see `docs/wave14_deferral.md`).
