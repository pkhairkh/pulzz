# pulzZ — Design Paper

**Version:** 0.2.0 (post-Wave 17)
**Date:** 2026-08-13

## Abstract

pulzZ is a post-quantum secure compression-aware transport for exact-state
payloads between browser (WASM) and native clients. It combines a
mutually-authenticated post-quantum session layer (ML-KEM-768 + ML-DSA-65 +
ChaCha20-Poly1305) with item batching and whole-batch zstd compression to
achieve positive wire savings on realistic workloads. The system achieves
**+36.7% wire savings** on high-locality workloads at batch_size=50, with
exact round-trip rate of 1.000000.

## 1. Introduction

The central challenge in payload transport is that per-item cryptographic
protection overhead (AEAD tags, record headers, transport framing) dominates
wire bytes for small payloads. At ~80 bytes per item (the average for
wikitext_103_raw), per-item overhead of ~94 bytes produces **negative wire
savings** (-105% on the high-locality workload).

pulzZ solves this with two complementary techniques:
1. **Item batching** — N items share one AEAD tag and record header,
   amortizing overhead to ~94B/N per item.
2. **Whole-batch zstd compression** — the entire batch envelope is compressed
   as a single zstd stream, exploiting cross-item redundancy (repeated JSON
   keys, identical field names, similar values) that per-item compression
   cannot capture.

## 2. PQC session layer

The session layer provides mutually-authenticated post-quantum key exchange:

- **KEM:** ML-KEM-768 (FIPS-203, formerly CRYSTALS-Kyber)
- **Signature:** ML-DSA-65 (FIPS-204, formerly CRYSTALS-Dilithium)
- **AEAD:** ChaCha20-Poly1305
- **KDF:** HKDF-SHA256

4-message handshake: `ClientHello → ServerHello → ClientFinish → ServerFinish`.
The transcript hash binds all four messages into the root key derivation,
verified by a 298-mutation property test.

## 3. Compression pipeline

The P0-P5 compression module provides six layers:

- **P0:** Zstd dictionary transport (per-SourceKind trained dictionaries)
- **P1:** Binary delta encoding for updates (rolling-hash content-addressable diff)
- **P2:** Schema-aware structural templates (JSON key-pattern extraction)
- **P3:** Format-aware columnar encoding (per-column RLE/delta/zstd)
- **P4:** Adaptive strategy selection (size/format/update-aware routing)
- **P5:** CPU optimization (self-describing compression tags)

## 4. Batched emission

The `BatchEnvelope` record type (wire discriminant 21) wraps N items in a
single AEAD-protected transport frame. The wire format is postcard-encoded:

```
[item_count: u16]
[item_0: BatchItem]
[item_1: BatchItem]
...
[item_N-1: BatchItem]
```

where each `BatchItem` is:

```
[item_id: u64]
[source_kind: u8]
[payload_len: u32]
[payload: [u8; payload_len]]
```

The entire envelope is then compressed as a single zstd stream before AEAD
protection. On decode, the client decompresses the batch payload, decodes
the envelope, and caches each item.

## 5. Predictive router

The planner uses a UCB1 multi-armed bandit (Auer 2002) to select between
route families with provable `O(sqrt(N log N))` regret bound per arm. A
Prediction Suffix Tree (Begleiter 2004) predicts the next route family;
when confidence ≥ 0.9 and the predicted family is DirectState, the planner
short-circuits the heuristic candidate generation.

## 6. Benchmark results

| Configuration | high_locality | mixed_locality | low_locality |
|---|---|---|---|
| Per-item (baseline) | -105.4% | -92.6% | -87.5% |
| Batched (batch_size=10) | +20.7% | +19.9% | +17.4% |
| Batched (batch_size=20) | +30.9% | +29.3% | +26.7% |
| Batched (batch_size=50) | +36.7% | +35.6% | +32.5% |

All configurations achieve exact round-trip rate of 1.000000.

## 7. Related work

- FIPS-203 (ML-KEM), FIPS-204 (ML-DSA), RFC 8446 (TLS 1.3)
- Auer, Cesa-Bianchi & Fischer 2002, "Finite-time Analysis of the Multiarmed
  Bandit Problem", Machine Learning 47(2-3):235-256.
- Begleiter, El-Yaniv & Yona 2004, "On Prediction Using Variable Order
  Markov Models", arxiv 1107.0051.
- Shannon 1948, "A Mathematical Theory of Communication", BSTJ 27:379-423.
- Tridgell 1999, "Efficient Algorithms for Sorting and Synchronization"
  (rsync thesis).
- Percival 2003, "Matching with Mismatches and Wildcards" (bsdiff).
- IETF `draft-ietf-httpbis-compression-dictionary-19` (Compression Dictionary
  Transport).
