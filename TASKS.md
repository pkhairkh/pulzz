# TASKS.md

## Status

Remaining work after Sprint 5 (MemoryAck feedback loop, eval classification fix, P0-P5 compress module scaffolding, amortization-aware inflation check, stable assembly ID).

All 146 tests pass (78 shared_protocol + 53 server + 15 client). Code is compiled and pushed.

## Working rules

- Do not mark a task done without runtime validation.
- Do not reintroduce retired types or demoted features as active architecture.
- When a task changes sender/receiver semantics, update both sides.
- Keep trackers short and residual — code should carry the architecture, not trackers.

## Completed work

### S1-S3: Compatibility purge, DDD, feature-coherence remediation ✅

- Retired StateDef, StatePatch, BlockCatalogSync from RecordType
- Confirmed-only admissibility with pending/confirmed split
- Transactional inline definitions on receiver
- Mechanical dependency closure derivation

### S4: Sprint 4 — Postcard migration ✅

- Replaced serde_json with postcard for PredictiveRouteDispatchPayload encoding
- Wire records now use compact binary serde instead of JSON

### S5: Sprint 5 — MemoryAck feedback and eval fixes ✅

- MemoryAck feedback loop in eval: client emits acks → server applies them → promotes pending to confirmed
- Eval classification bug: use `record_type` not `codec_mode` for PredictiveConfirm
- Amortization-aware inflation check: `estimate_definition_investment_bytes()`, `AMORTIZATION_BREAK_EVEN_REUSE=3`
- Stable assembly ID: excluded `canonical_length_min/max` from hash to reduce uniqueness
- P0-P5 compress module scaffolding (1698 lines): ZstdDictionary, BinaryDelta, StructuralTemplate, ColumnarBatch, select_strategy, DictionaryManager, TemplateRegistry
- Compression pipeline integration into ServerState: `compress_exact_bytes()`, `dict_manager`, `template_registry`, `previous_versions`

## Remaining work

### P0: Zstandard Dictionary Transport — Wire Integration

The `ZstdDictionary` type and `DictionaryManager` are implemented. Dictionary training works. The server's `compress_exact_bytes()` uses it for compression. What remains is the wire protocol integration:

**Steps:**
- [ ] Design a `DictionaryDef` record type for transmitting trained dictionaries to the client
- [ ] Implement dictionary installation on the client side upon receipt
- [ ] Add dictionary reference in PredictiveConfirm/PredictiveCorrect payloads (dict_id field)
- [ ] Implement client-side decompression using the installed dictionary
- [ ] Add MemoryAck promotion for dictionary installation
- [ ] Evaluate wire savings with dictionary transport enabled end-to-end

### P1: Delta Encoding for Updates — Wire Integration

`BinaryDelta` is implemented and tested. The server's `compress_exact_bytes()` uses it for UpsertObject events. What remains is the wire protocol integration:

**Steps:**
- [ ] Design a wire envelope for delta-encoded payloads (base_item_id + delta_bytes)
- [ ] Implement client-side delta application (reconstruct from previous version + delta)
- [ ] Add base-item availability check in admissibility
- [ ] Handle the case where the client doesn't have the base item (fallback to zstd raw)
- [ ] Evaluate delta encoding savings for upsert-heavy workloads

### P2: Schema-Aware Structural Templates — Wire Integration

`StructuralTemplate` is implemented with key-pattern extraction and slot encoding. The server's `compress_exact_bytes()` uses it for JSON data. What remains is wire protocol integration:

**Steps:**
- [ ] Design a `TemplateDef` record type for transmitting templates to the client
- [ ] Implement template installation on the client side upon receipt
- [ ] Add template reference in payload encoding (template_id + slot_values)
- [ ] Implement client-side reconstruction from template + slot values
- [ ] Add MemoryAck promotion for template installation
- [ ] Evaluate template savings for JSON workloads with repeated key structures

### P3: Format-Aware Columnar Encoding — Batch Integration

`ColumnarBatch` is implemented with per-column RLE/delta/zstd encoding. Currently unused in the live pipeline because columnar encoding requires batching multiple items. What remains:

**Steps:**
- [ ] Design a batch emission mode in the server (accumulate items, emit as a columnar batch)
- [ ] Implement batch boundaries (time-based or count-based)
- [ ] Add columnar batch wire format (batch header + columns)
- [ ] Implement client-side columnar batch decode and individual item extraction
- [ ] Evaluate columnar encoding savings for bulk transfer workloads

### P4: Adaptive Strategy Selection — Integration Validation

`select_strategy()` is implemented with size/format/update-aware routing. The server's `compress_exact_bytes()` calls it. What remains:

**Steps:**
- [ ] Validate that strategy selection produces correct decisions across all workload types
- [ ] Add per-strategy wire savings tracking to eval metrics
- [ ] Tune decision thresholds (min size for compression, update detection, etc.)
- [ ] Ensure fallback paths (strategy → passthrough) are correct and measured

### P5: CPU Optimization

Self-describing compression tags are implemented. What remains:

**Steps:**
- [ ] Implement batch AEAD (encrypt multiple items in one ChaCha20-Poly1305 operation)
- [ ] Cache dependency closures for repeated route patterns
- [ ] Eliminate structuralization overhead by using templates directly instead of inline assembly definitions
- [ ] Profile compress/decompress hot paths and optimize critical loops

### R2: End-to-end validation of confirmed-only admissibility

**Steps:**
- [ ] Run a session where pending state exists but no MemoryAck has been received
- [ ] Verify that routes requiring those objects fall through to direct-state or carry inline definitions
- [ ] Verify that MemoryAck receipt enables subsequent route reuse
- [ ] Verify that resync clears pending state correctly

### R3: End-to-end validation of transactional inline definitions

**Steps:**
- [ ] Construct a scenario where predictive dispatch fails after inline defs are staged
- [ ] Verify that durable stores are unchanged after failure
- [ ] Verify that MemoryAck is not emitted for failed dispatches
- [ ] Verify that successful dispatch commits inline defs and emits MemoryAck

### R4: Benchmark harness isolation audit

**Steps:**
- [ ] Measure the fraction of wall-clock time spent in SourceCache I/O during a 100MB benchmark
- [ ] Design a write-buffer or deferred-write architecture for SourceCache
- [ ] Implement and verify that measured paths contain only protocol work
- [ ] Update benchmark summaries to separate protocol work from harness overhead

### R7: Schema/Transform revision tracking

**Steps:**
- [ ] Replace `peer_schema_ids: HashSet<SchemaId>` with `HashMap<SchemaId, ObjectVersion>`
- [ ] Replace `peer_transform_ids: HashSet<TransformId>` with `HashMap<TransformId, ObjectVersion>`
- [ ] Update admissibility to use full revision comparison
- [ ] Update client dependency checks to match

### R8: Memory cliff mitigation

**Steps:**
- [ ] Evaluate MAX_SERVER_ENTRIES=16384 cap effectiveness
- [ ] Add LRU eviction for entries and predictors maps
- [ ] Add memory usage telemetry to eval metrics
- [ ] Test with high-item-count workloads

### R9: Transform reactivation (if desired)

Transform is currently demoted. Reactivation requires end-to-end synchronization.

**Steps:**
- [ ] Implement confirmed TransformDef installation flow
- [ ] Add pending/confirmed transform class tracking with MemoryAck promotion
- [ ] Enable transform route selection in planner
- [ ] Enable transform emission in sender
- [ ] Add transform-specific benchmark cases
- [ ] Update docs to reclassify transform as ACTIVE
