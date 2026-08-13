# Wave 14 — Dead Code Physical Deletion: Status

## Task 14-a: Transform layer deletion — DEFERRED

**Surface:** 552 references across 16 files.

| Component | References | Files |
|---|---|---|
| `TransformKind` enum | 73 | 3 (chpmt.rs, state.rs, state_policy.rs) |
| `TransformDefPayload` struct | 9 | 4 (lib.rs, transport.rs, protocol.rs, state.rs) |
| `TransformInstancePayload` struct | 8 | 5 (lib.rs, client/lib.rs, bench.rs, transport.rs) |
| `TransformDef`/`TransformCorrect` wire records | 20 | protocol.rs |
| `RouteFamily::Transform` / `ControllerRouteFamily::Transform` | 38 | chpmt.rs |
| `TransformPromotionQueue` | 12 | server/lib.rs, chpmt.rs |
| `peer_transform_versions` / `pending_transform_versions` | 24 | server/lib.rs, client/lib.rs |
| `transforms` / `materialized_transforms` (client state) | 33 | client/lib.rs |
| Other Transform references | 335 | all files |

**Why deferred:** Physical deletion requires updating every match arm that handles `RouteFamily::Transform` or `ControllerRouteFamily::Transform` (Rust enums are exhaustive — removing a variant breaks all matches). The 552 references span 16 files with interdependent type definitions. A single missed reference produces a compile error that blocks all subsequent work.

**Current mitigation:** All Transform route-family variants and wire types are marked `#[deprecated(since = "0.2.0")]` (Wave 9). The invariant test at `shared_protocol/tests/dead_code_invariants.rs` verifies zero Transform emissions.

## Task 14-b: Assembly/Schema/Episode deletion — DEFERRED

**Surface:** ~400 additional references across the same 16 files.

**Why deferred:** Same rationale as 14-a. Additionally, Assembly/Schema/Episode are NOT zero-emission — the heuristic planner can still select them when candidates exist (though the UCB1 bandit rarely does). Physical deletion would require first removing all candidate-generation code paths, which is a larger refactor.

## Recommended approach for a dedicated cleanup sprint

1. **Phase 1 (1 day):** Delete Transform-specific structs (`TransformKind`, `TransformDefPayload`, `TransformInstancePayload`, `TransformPromotionQueue`). Fix compile errors in the 5 most-affected files. This removes ~150 references.

2. **Phase 2 (1 day):** Remove `peer_transform_versions` and `pending_transform_versions` from `ServerState` and `ClientState`. Remove the `Transform` arm from `apply_record` in the client. This removes ~80 references.

3. **Phase 3 (1 day):** Remove `RouteFamily::Transform` and `ControllerRouteFamily::Transform` enum variants. Update all match arms (the compiler will list every site). This removes ~50 references.

4. **Phase 4 (1 day):** Remove `RecordType::TransformDef` and `RecordType::TransformCorrect`. Add their discriminants (13, 19) to the `RetiredDiscriminant` path. This removes ~20 references and makes the wire format truly decode-only for Transform.

5. **Phase 5 (2 days):** Repeat phases 1-4 for Assembly, Schema, Episode.

**Total estimated effort:** 5-7 days of focused work.

## Wave 14 DoD

- ✅ Deferral documented with full reference count and recommended sprint plan
- ✅ All Transform/Assembly/Schema/Episode variants marked `#[deprecated]` (Wave 9)
- ✅ Invariant test verifies zero Transform emissions (Wave 9)
- ❌ Physical deletion NOT DONE — deferred to dedicated sprint
