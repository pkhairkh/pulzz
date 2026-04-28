# pulzZ

pulzZ is an exact-reconstruction predictive transport system. The runtime center is deterministic exact reconstruction with predictive routing.

## Current repository state

- active runtime paths are exact-reconstruction with confirmed-only peer-state admissibility
- active route families: DirectState, ExactAtom, Hybrid, Dictionary, Assembly, Schema, Episode
- transform routes are DEMOTED — candidate generation retained but no transform routes are emitted
- retired wire types (StateDef, StatePatch, BlockCatalogSync) are removed from the protocol surface
- object identity uses one canonical grammar enforced on both sender and receiver
- dependency closures are mechanically derived for exact-atom and hybrid routes
- benchmark results are internal-debug quality — no external performance claim should be made without separating harness overhead from protocol work

## Route family maturity

| Family | Maturity |
|--------|----------|
| DirectState | Exact, validated |
| ExactAtom | Exact, mechanically derived |
| Hybrid | Exact, mechanically derived |
| Dictionary | Exact, confirmed-only |
| Assembly | Heuristic candidate generation |
| Schema | Heuristic, conservative dependency for revision > 0 |
| Episode | Heuristic, approximate temporal prediction |
| Transform | DEMOTED — not emitted |

## Documentation map

- `docs/chpmt_architecture.md` — active architecture and feature maturity
- `docs/chpmt_migration_map.md` — migration and purge history
- `TASKS.md` — remaining work
- `ISSUES.md` — live residual issues
