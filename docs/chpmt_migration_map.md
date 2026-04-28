# CHPMT migration map

## Retired surfaces

The following have been removed from the active architecture:

- vector-profile-driven predictive semantics
- block-first predictive runtime carriers
- dual record semantics presented as first-class runtime architecture
- benchmark/report framing centered on old profile lanes
- documentation that described retired adapter paths as supported architecture
- JSON payload encoding for PredictiveRouteDispatchPayload (replaced by postcard compact binary serde)

## Purged wire types

Wire discriminants 6 (StateDef), 7 (StatePatch), 8 (BlockCatalogSync) have been removed from the RecordType enum. They return `WireError::RetiredDiscriminant` on deserialization. No code path constructs these types. All match arms, validators, and benchmark references have been removed.

## Demoted features

Transform routing has been demoted from active architecture. The planner no longer selects transform routes. Candidate generation code is retained for potential reactivation. All transform-route paths fall through to direct-state with `FallbackReason::TransformDemoted`.

## Native exact payload plane

Exact payload emission and decode operate directly on native exact-state material. Predictive routing, cache identity, state execution, and client authority are CHPMT-native.

## Sprint history

### Sprint 1-3: Compatibility purge / DDD / feature-coherence remediation

- Retired StateDef, StatePatch, BlockCatalogSync wire types
- Installed confirmed-only admissibility with pending/confirmed split and MemoryAck-based promotion
- Implemented transactional inline definition application on receiver
- Derived mechanical dependency closures for exact-atom and hybrid routes
- Enforced canonical object-ID grammar on both sender and receiver
- Removed zero-valued eval metrics (residual_burden, completion_hit_rate, schema_activation_share, transform_reuse_share)
- Collapsed duplicate type aliases and deprecated helper functions
- Removed transform install/apply record aliases; canonical names used throughout

### Sprint 4: Postcard migration

- Replaced serde_json serialization with postcard for PredictiveRouteDispatchPayload
- Wire records now use compact binary serde instead of JSON
- Reduced per-record encoding overhead significantly

### Sprint 5: MemoryAck feedback, eval fixes, compress scaffolding

- **MemoryAck feedback loop in eval**: Client now emits pending memory acks → server applies them → promotes pending peer-state to confirmed. Without this, the server never learns that the client installed inline definitions, preventing amortization. 79 acks now flow per eval session.
- **Eval classification bug fix**: PredictiveConfirm records with `CodecMode::None` were incorrectly classified as "control" instead of "predicted_exact". Fixed by using `record_type` for classification. Predicted_exact count went from 0 to 43/25/11 across workloads.
- **Amortization-aware inflation check**: Added `estimate_definition_investment_bytes()` to measure the byte cost of inline definitions (assembly bodies, schema slots, dictionary entries, dependency closures). The inflation check now allows some inflation if the definition investment is small enough to amortize over `AMORTIZATION_BREAK_EVEN_REUSE=3` future uses. Constants: `INFLATION_CHECK_MIN_CONTENT_BYTES=256`.
- **Stable assembly ID**: Excluded `canonical_length_min/max` from the `stable_assembly_id` hash to reduce unnecessary uniqueness in assembly definitions. Items with the same structural content but different length ranges now produce the same assembly ID.
- **P0-P5 compress module scaffolding** (1698 lines in `compress.rs`):
  - P0: `ZstdDictionary` — train per-format dictionaries from representative samples, compress/decompress with dictionary, IETF-recommended 100 KB max dictionary size
  - P1: `BinaryDelta` — rolling-hash content-addressable diff for UpsertObject events, O(n) instead of O(n log n) for bsdiff
  - P2: `StructuralTemplate` — key-pattern extraction for JSON data, template ID + slot values instead of inline assembly, reduces per-item overhead from ~130 bytes to ~20 bytes
  - P3: `ColumnarBatch` — per-column encoding (RLE for repeats, delta varint for lengths, zstd for payloads), based on OpenZL (arXiv:2510.03203)
  - P4: `select_strategy()` — adaptive strategy selection based on data size, format, update pattern, available dictionaries and templates
  - P5: Self-describing compression tags (`encode_compressed_payload`/`decode_compressed_payload`), batch helpers
- **ServerState compression pipeline**: Added `dict_manager`, `template_registry`, `previous_versions` fields. Added `compress_exact_bytes()` method that feeds samples, trains dictionaries, selects strategy, and wraps with compression tags.
- **Compress eval metrics**: Added `CompressEvalMetrics` to eval harness with per-strategy counts, compression ratios, and timing. Added `EvaluationPreset::CompressPipeline` for standalone compression pipeline evaluation.

## Zero-valued eval metric purge

The following eval metrics were always zero and have been removed from `PredictiveEvalMetrics` (eval.rs): residual_burden, completion_hit_rate, schema_activation_share, transform_reuse_share. The metric `transform_reuse_share` has also been removed from `PredictiveMemoryMetrics` (bench.rs) as it is always zero when transform routes are never emitted.

## Outstanding migration work

The P0-P5 compression module is implemented as scaffolding and validated in standalone eval. The remaining migration work is:

1. **Wire protocol integration**: Dictionary/template/delta definition transport (sending definitions to the client so it can decompress)
2. **Client-side decompression**: Integrate `decode_compressed_payload()` into the client's record application flow
3. **End-to-end validation**: Run the full client-server path with compression enabled
4. **Batch columnar encoding**: Design batch emission mode for bulk transfers

Once P0-P5 wire integration is complete, the compress module should replace the current inflation-prone inline assembly approach as the primary compression strategy, potentially making the CHPMT predictive route planning a secondary optimization rather than the primary compression mechanism.
