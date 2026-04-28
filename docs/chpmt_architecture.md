# CHPMT architecture

## Active center of gravity

The repository implements an exact-reconstruction predictive transport system. The runtime center is deterministic exact reconstruction with predictive routing — not lossy completion or soft inference.

### Active route families

| Family | Status | Maturity |
|--------|--------|----------|
| DirectState | ACTIVE | Exact, validated |
| ExactAtom | ACTIVE | Exact, mechanically derived dependencies |
| Hybrid | ACTIVE | Exact, mechanically derived dependencies |
| Dictionary | ACTIVE | Exact, confirmed-only peer state |
| Assembly | ACTIVE-HEURISTIC | Candidate generation is heuristic; wire contract is exact |
| Schema | ACTIVE-HEURISTIC | Candidate generation is heuristic; dependency closure is conservative for revision > 0 |
| Episode | ACTIVE-HEURISTIC | Candidate generation is heuristic; temporal prediction is approximate |

### Demoted route families

| Family | Status | Reason |
|--------|--------|--------|
| Transform | DEMOTED | No confirmed transform-class synchronization flow. Candidate generation retained but planner does not select transform routes. All transform paths fall through to direct-state with `FallbackReason::TransformDemoted`. |

### Removed wire history

Wire discriminants 6 (StateDef), 7 (StatePatch), 8 (BlockCatalogSync) are retired. They return `WireError::RetiredDiscriminant` on deserialization. These are not part of the active architecture.

## Admissibility model

One confirmed-only admissibility model is used across planner and sender:
- Admissibility reads confirmed peer state only
- Pending state is not consulted for routing decisions
- Same-batch inline definitions are supported via explicit inline parameter sets
- Receiver has its own `check_dependency_available` with `DependencyAvailability` enum for staged inline revision checks
- Schema/transform dependencies with `required_revision > 0` are conservatively rejected (no revision storage in peer tracking)

## Peer-state discipline

Peer-visible state advances only on confirmed possession:
- Emission writes to pending trackers
- MemoryAck receipt promotes pending to confirmed
- Failed or lost records cannot influence admissibility
- Resync clears both pending and confirmed state

## Object identity grammar

One canonical grammar enforced on both sender and receiver:
- `block:<id>`, `bundle:<id>`, `assembly:<id>`, `schema:<id>`, `transform:<id>`, `dictionary:<id>`
- Strict `strip_prefix` parsing — non-canonical forms rejected

## Benchmark maturity

Benchmark results are internal-debug quality only. No external performance claim should be made without:
- Separating protocol work from SourceCache I/O overhead
- Accounting for JSON encoding inflation in predictive payloads
- Distinguishing harness overhead from wire efficiency

## Explicit exclusions

The repository does not treat the following as active architecture:
- dual old/new runtime semantics
- block-first predictive runtime carriers
- retained adapter scaffolding as sanctioned edge support
- transform routes as a live emission path
- benchmark results as externally-defensible performance evidence
