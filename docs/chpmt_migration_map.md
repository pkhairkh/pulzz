# CHPMT migration map

## Retired surfaces

The following have been removed from the active architecture:

- vector-profile-driven predictive semantics
- block-first predictive runtime carriers
- dual record semantics presented as first-class runtime architecture
- benchmark/report framing centered on old profile lanes
- documentation that described retired adapter paths as supported architecture

## Purged wire types

Wire discriminants 6 (StateDef), 7 (StatePatch), 8 (BlockCatalogSync) have been removed from the RecordType enum. They return `WireError::RetiredDiscriminant` on deserialization. No code path constructs these types. All match arms, validators, and benchmark references have been removed.

## Demoted features

Transform routing has been demoted from active architecture. The planner no longer selects transform routes. Candidate generation code is retained for potential reactivation. All transform-route paths fall through to direct-state with `FallbackReason::TransformDemoted`.

## Native exact payload plane

Exact payload emission and decode operate directly on native exact-state material. Predictive routing, cache identity, state execution, and client authority are CHPMT-native.

## Zero-valued eval metric purge

The following eval metrics were always zero and have been removed from `PredictiveEvalMetrics` (eval.rs): residual_burden, completion_hit_rate, schema_activation_share, transform_reuse_share. The metric `transform_reuse_share` has also been removed from `PredictiveMemoryMetrics` (bench.rs) as it is always zero when transform routes are never emitted.
