# pulzZ

pulzZ is an exact-reconstruction predictive transport system. The runtime center is deterministic exact reconstruction with predictive routing.

## Current repository state

- active runtime paths are exact-reconstruction with confirmed-only peer-state admissibility
- active route families: DirectState, ExactAtom, Hybrid, Dictionary, Assembly, Schema, Episode
- transform routes are DEMOTED — candidate generation retained but no transform routes are emitted
- retired wire types (StateDef, StatePatch, BlockCatalogSync) are removed from the protocol surface
- object identity uses one canonical grammar enforced on both sender and receiver
- dependency closures are mechanically derived for exact-atom and hybrid routes
- payload encoding uses postcard (compact binary serde) for wire records
- **P0-P5 compression module** (`compress.rs`) provides zstd dictionary transport, delta encoding, structural templates, columnar encoding, adaptive strategy selection, and CPU optimization helpers — scaffolding is implemented and tested; integration into the live wire pipeline is in progress
- the server's `ServerState` includes `DictionaryManager`, `TemplateRegistry`, and `previous_versions` for P0-P5 compression pipeline state
- evaluation harness includes both predictive-memory eval and standalone compression pipeline eval (`CompressEvalMetrics`)
- benchmark results are internal-debug quality — no external performance claim should be made without separating harness overhead from protocol work

## Wire savings status

The current predictive route planning produces **negative wire savings** (-45%/-27%/-13% across workloads) because every assembly definition is unique (items have different structural hashes due to unique step numbers). The definitions are never amortized. The P0-P5 compression pipeline is the intended fix — it replaces the inflation-prone inline assembly approach with zstd dictionary compression, delta encoding for updates, and template-based structural encoding.

The standalone compression pipeline evaluation (independent of CHPMT route planning) shows positive compression ratios, confirming that the compression primitives work. The remaining work is wiring them into the dispatch/encode/decode paths.

## Route family maturity

| Family | Maturity |
|--------|----------|
| DirectState | Exact, validated |
| ExactAtom | Exact, mechanically derived |
| Hybrid | Exact, mechanically derived |
| Dictionary | Exact, confirmed-only |
| Assembly | Heuristic candidate generation — currently produces negative savings due to unique inline definitions |
| Schema | Heuristic, conservative dependency for revision > 0 |
| Episode | Heuristic, approximate temporal prediction |
| Transform | DEMOTED — not emitted |

## P0-P5 Compression Pipeline

The `compress` module implements six layers identified from arxiv research:

| Layer | Description | Status |
|-------|-------------|--------|
| P0 | Zstandard Dictionary Transport | Implemented — `ZstdDictionary` with training, compress, decompress |
| P1 | Delta Encoding for Updates | Implemented — `BinaryDelta` with rolling-hash content-addressable diff |
| P2 | Schema-Aware Structural Templates | Implemented — `StructuralTemplate` with key-pattern extraction and slot encoding |
| P3 | Format-Aware Columnar Encoding | Implemented — `ColumnarBatch` with per-column RLE/delta/zstd |
| P4 | Adaptive Strategy Selection | Implemented — `select_strategy()` with size/format/update-aware routing |
| P5 | CPU Optimization | Partial — self-describing compression tags, batch helpers; batch AEAD and cached closures pending |

Integration status: The server's `compress_exact_bytes()` method wires P0-P4 into the emission path. The standalone eval harness (`evaluate_compress_pipeline`) validates compression round-trips. The client-side decompression path needs integration.

## Crate structure

- `shared_protocol` — protocol types, record model, CHPMT routing, compress module, codec, protection
- `server` — server session, event processing, emission, eval harness, benchmark
- `client` — client session, record application, decompression

## Documentation map

- `docs/chpmt_architecture.md` — active architecture and feature maturity
- `docs/chpmt_migration_map.md` — migration and purge history
- `extend.md` — CHPMT replacement specification (design document)
- `paper.md` — design paper
- `TASKS.md` — remaining work
- `ISSUES.md` — live residual issues
