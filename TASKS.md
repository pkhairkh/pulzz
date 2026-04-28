# TASKS.md

## Status

Remaining work after the compatibility-purge / DDD / feature-coherence remediation pass.

No local build or test claim is made — no Rust toolchain is available in this environment.

## Working rules

- Do not mark a task done without runtime validation.
- Do not reintroduce retired types or demoted features as active architecture.
- When a task changes sender/receiver semantics, update both sides.
- Keep trackers short and residual — code should carry the architecture, not trackers.

## Remaining work

### R1. Compile-check and fix all changes

The remediation pass made structural edits across shared_protocol, server, and client crates. These need compile verification and any resulting fixes.

**Steps:**
- [ ] Run `cargo check --workspace`
- [ ] Fix any compilation errors from removed types, renamed fields, or exhausted match arms
- [ ] Run `cargo test --workspace`
- [ ] Fix any test failures

### R2. End-to-end validation of confirmed-only admissibility

Admissibility helpers now read confirmed peer state only. This needs runtime verification.

**Steps:**
- [ ] Run a session where pending state exists but no MemoryAck has been received
- [ ] Verify that routes requiring those objects fall through to direct-state or carry inline definitions
- [ ] Verify that MemoryAck receipt enables subsequent route reuse
- [ ] Verify that resync clears pending state correctly

### R3. End-to-end validation of transactional inline definitions

Client inline definition staging and rollback need runtime verification.

**Steps:**
- [ ] Construct a scenario where predictive dispatch fails after inline defs are staged
- [ ] Verify that durable stores are unchanged after failure
- [ ] Verify that MemoryAck is not emitted for failed dispatches
- [ ] Verify that successful dispatch commits inline defs and emits MemoryAck

### R4. Benchmark harness isolation audit

SourceCache disk writes during measured execution are the primary benchmark contamination source.

**Steps:**
- [ ] Measure the fraction of wall-clock time spent in SourceCache I/O during a 100MB benchmark
- [ ] Design a write-buffer or deferred-write architecture for SourceCache
- [ ] Implement and verify that measured paths contain only protocol work
- [ ] Update benchmark summaries to separate protocol work from harness overhead

### R5. Transform reactivation (if desired)

Transform is currently demoted. Reactivation requires end-to-end synchronization.

**Steps:**
- [ ] Implement confirmed TransformDef installation flow
- [ ] Add pending/confirmed transform class tracking with MemoryAck promotion
- [ ] Enable transform route selection in planner
- [ ] Enable transform emission in sender
- [ ] Add transform-specific benchmark cases
- [ ] Update docs to reclassify transform as ACTIVE

### R6. Binary encoding for PredictiveRouteDispatchPayload

JSON encoding causes payload inflation. Binary encoding would reduce wire cost.

**Steps:**
- [ ] Design a compact binary serialization for PredictiveRouteDispatchPayload
- [ ] Implement with version field for wire compatibility
- [ ] Benchmark JSON vs binary encoding overhead
- [ ] Verify exact reconstruction with both encodings

### R7. Schema/Transform revision tracking

Current HashSet-based tracking for schema and transform peer state cannot store revision.

**Steps:**
- [ ] Replace `peer_schema_ids: HashSet<SchemaId>` with `HashMap<SchemaId, ObjectVersion>`
- [ ] Replace `peer_transform_ids: HashSet<TransformId>` with `HashMap<TransformId, ObjectVersion>`
- [ ] Update admissibility to use full revision comparison
- [ ] Update client dependency checks to match
