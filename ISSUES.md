# ISSUES.md

## Scope

Live residual issues in the pulzZ repository after the compatibility-purge / DDD / feature-coherence remediation pass.

No local build or test claim is made — the current environment does not provide a Rust toolchain. Code changes are structurally verified via search but not compile-tested.

## Status summary

What is materially true in the current checkout:

- exact-state / direct-state path is structurally usable
- retired flat-state/catalog runtime lanes (StateDef, StatePatch, BlockCatalogSync) are **removed** from the RecordType enum; wire discriminants 6, 7, 8 return `WireError::RetiredDiscriminant`
- peer-state semantics use confirmed-only admissibility with pending/confirmed split and MemoryAck-based promotion
- inline definition application is transactional on the receiver
- dependency closure for exact-atom and hybrid routes is mechanically derived
- object-ID grammar is canonical and enforced strictly on both sender and receiver
- dependency checks are revision-aware on both sender and receiver
- resync semantics are symmetric across client and server
- transform is **DEMOTED** from active architecture — candidate generation retained for future reactivation but no transform routes are emitted
- benchmark payload metrics use explicit byte categories
- zero-valued eval metrics (residual_burden, completion_hit_rate, schema_activation_share, transform_reuse_share) have been removed from eval.rs; transform_reuse_share removed from bench.rs
- duplicate type aliases and deprecated helper functions have been collapsed
- transform install/apply record aliases removed; canonical names used throughout

## Open issues

### [MEDIUM] I1. SourceCache disk writes during measured benchmark execution

**Area:** benchmark operational hygiene
**Primary files:** `server/src/bench.rs`, `server/src/source_cache.rs`

SourceCache.resolve_source() disk writes during measured execution remain the primary performance bottleneck for 100MB benchmarks (~450K file writes + ~360K directory creates for cold cache). Deferring all cache writes until after measured execution would require a write-buffer architecture change. The execution timeout prevents indefinite hangs but does not eliminate the latency.

**Effect:** Benchmark summaries for large corpora reflect harness I/O overhead mixed with protocol work. No external wire-efficiency claim should rest on current 100MB benchmark numbers without separating these costs.

---

### [MEDIUM] I2. Assembly/Schema/Episode candidate generation is heuristic

**Area:** predictive routing maturity
**Primary files:** `shared_protocol/src/chpmt.rs`, `shared_protocol/src/state_policy.rs`, `server/src/lib.rs`

Assembly, schema, and episode route families are live and structurally complete, but their candidate generation relies on heuristic pattern-matching rather than exact structural induction. This means:

- candidate quality varies with input distribution
- no formal guarantee that the best candidate is always found
- promotion decisions depend on observed success/failure counts

These families are correctly classified as ACTIVE-HEURISTIC. Their wire contracts and dependency closures are mechanically correct. The heuristic gap is in candidate quality, not in execution correctness.

**Effect:** Wire savings from these route families may be suboptimal for some workloads. Exact reconstruction is still guaranteed — heuristic quality affects compression ratio, not correctness.

---

### [MEDIUM] I3. Transform reactivation prerequisites not yet implemented

**Area:** transform routing (DEMOTED)
**Primary files:** `shared_protocol/src/chpmt.rs`, `shared_protocol/src/state_policy.rs`

Transform has been demoted from active architecture because the confirmed transform-class synchronization flow is not implemented. Reactivation would require:

- explicit TransformDef installation flow on the wire
- pending/confirmed transform class tracking with MemoryAck promotion
- sender admissibility checking confirmed transform class availability
- transform instance revision discipline

The wire types (TransformDef, TransformCorrect) and candidate generation code are retained but the planner does not select transform routes. All transform-route paths fall through to direct-state with `FallbackReason::TransformDemoted`.

**Effect:** Transform routes are never emitted. The transform_demoted_fallback_count metric tracks how often transform candidates would have been selected.

---

### [LOW] I4. Schema/Transform dependency revision tracking uses conservative rejection

**Area:** dependency validation
**Primary files:** `server/src/lib.rs`, `client/src/lib.rs`

Schema and transform peer-tracking uses HashSet (no revision storage). When `required_revision > 0`, the server conservatively rejects these dependencies and the client checks only presence. Assembly and dictionary tracking stores revision and performs full comparison.

**Effect:** Routes that could be admissible with same-version schema/transform dependencies are rejected when `required_revision > 0`. This is a correctness-over-efficiency trade-off.

---

### [LOW] I5. PredictiveRouteDispatchPayload uses JSON encoding

**Area:** wire efficiency
**Primary files:** `shared_protocol/src/state.rs`

Predictive route dispatch payloads are JSON-encoded, which contributes to payload inflation. When encoded predictive payload exceeds logical content length, the inflation is tracked and the route falls back to direct state.

**Effect:** Some predictive routes that would save wire bytes under binary encoding instead fall back to direct state due to JSON inflation. The inflation detection and fallback mechanism is honest about this.

---

### [LOW] I6. End-to-end runtime validation not yet performed

**Area:** validation
**Primary files:** all

Several correctness improvements (confirmed-only admissibility, transactional inline defs, dependency closure derivation, substrate promotion) are structurally verified in code but have not been runtime-tested with the full system. No Rust toolchain is available in the current environment.

**Effect:** Code logic appears correct but may contain integration bugs discoverable only by running the system.
