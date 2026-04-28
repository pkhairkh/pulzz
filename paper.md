# pulzZ native exact-state architecture

This document describes the active pulzZ architecture after the compatibility-purge, DDD, and feature-coherence remediation pass.

## Scope

The repository exposes one payload model only:

- native exact-state material as authoritative payload state
- CHPMT route planning over direct-state, exact-atom, assembly, schema, episode, and hybrid families
- transform is DEMOTED — candidate generation retained but no transform routes are emitted
- protected-frame transport independent of carrier family
- cue/object-native cache identity and predictor state
- deterministic exact reconstruction from native state, substrate references, or predictive route graphs

## Record model

Active payload-bearing record classes:

- `ExactState`
- `PredictiveConfirm`
- `PredictiveCorrect`
- `AssemblyDef`
- `TransformDef` (DEMOTED — wire type preserved, not emitted by live server)
- `SchemaDef`
- `EpisodeHint`
- `ReplayHint`
- `MemoryRetire`
- `TransformCorrect` (DEMOTED — wire type preserved, not emitted by live server)

Control records:

- `Rekey`
- `Resync`
- `Close`
- `SourceMeta`
- `Repair`

Retired wire discriminants (6, 7, 8 for StateDef, StatePatch, BlockCatalogSync) return `WireError::RetiredDiscriminant` on deserialization. They are not part of the active architecture.

## Payload semantics

`ExactState` is the direct exact-state emission path. It carries native exact-state material encoded with the active direct/predictive exact-state data-plane modes.

`PredictiveConfirm` and `PredictiveCorrect` carry route-graph dispatch payloads for exact-atom reuse, assembly activation, schema expansion, hybrid reconstruction, and episode-conditioned continuation. Transform application is a demoted dispatch target — it is not selected by the planner.

## Routing center of gravity

Route planning is exact-reconstruction with predictive routing:

- `DirectState` is the bounded direct exact-state route
- `ExactAtom` reuses substrate references when peer basis is confirmed present
- `Assembly`, `SchemaExpansion`, and `EpisodeCompletion` operate on heuristic candidate generation with exact wire contracts
- `Transform` is DEMOTED — not emitted; candidate generation retained for future reactivation
- `Hybrid` combines substrate reuse with contradiction bytes under the same native dispatch model

## Cache and authority

Both client and server treat cue/object-native structures as authoritative runtime state:

- source descriptors bind source kind and cues
- runtime objects are native CHPMT objects
- predictor entries store native object identity
- route feedback and governor state are keyed by active route families only
- peer-state advances only on confirmed possession (MemoryAck-based promotion)

## Carrier independence

Carrier choice does not alter payload semantics. Stream, datagram, and grouped transport all reuse the same protected record framing and the same native exact-state / predictive dispatch surfaces.

## Explicit exclusions

The repository does not present any of the following as active architecture:

- retired wire types (StateDef, StatePatch, BlockCatalogSync)
- transform routes as a live emission path
- benchmark results as externally-defensible performance evidence
- dual old/new transport semantics
- cache identity derived from retired payload wrappers
