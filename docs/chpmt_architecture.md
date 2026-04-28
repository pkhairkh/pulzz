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
| Assembly | ACTIVE-HEURISTIC | Candidate generation is heuristic; wire contract is exact; currently produces negative savings due to unique inline definitions |
| Schema | ACTIVE-HEURISTIC | Candidate generation is heuristic; dependency closure is conservative for revision > 0 |
| Episode | ACTIVE-HEURISTIC | Candidate generation is heuristic; temporal prediction is approximate |

### Demoted route families

| Family | Status | Reason |
|--------|--------|--------|
| Transform | DEMOTED | No confirmed transform-class synchronization flow. Candidate generation retained but planner does not select transform routes. All transform paths fall through to direct-state with `FallbackReason::TransformDemoted`. |

### Removed wire history

Wire discriminants 6 (StateDef), 7 (StatePatch), 8 (BlockCatalogSync) are retired. They return `WireError::RetiredDiscriminant` on deserialization. These are not part of the active architecture.

## Payload encoding

Payload encoding uses postcard (compact binary serde) for wire records. The earlier JSON encoding path has been replaced.

The P0-P5 compression module (`compress.rs`) provides an additional compression layer on top of postcard serialization. The server's `compress_exact_bytes()` method selects the best compression strategy (zstd dictionary, zstd raw, delta, template, or passthrough) and wraps the result with a self-describing compression tag. The client-side decompression path needs integration.

## Compression pipeline (P0-P5)

The `compress` module implements six layers identified from arxiv research:

| Layer | Component | Description |
|-------|-----------|-------------|
| P0 | `ZstdDictionary` | Train per-format dictionaries from representative samples; compress/decompress with dictionary |
| P1 | `BinaryDelta` | Rolling-hash content-addressable diff for UpsertObject events |
| P2 | `StructuralTemplate` | Key-pattern extraction for JSON; template ID + slot values instead of inline assembly |
| P3 | `ColumnarBatch` | Per-column encoding (RLE for repeats, delta for IDs, zstd for payloads) |
| P4 | `select_strategy()` | Adaptive strategy selection based on data size, format, update pattern |
| P5 | Compression tags | Self-describing payload tags for CPU-efficient decode routing |

### Pipeline state in ServerState

- `dict_manager: DictionaryManager` — collects samples, trains zstd dictionaries per SourceKind, compresses with dictionary
- `template_registry: TemplateRegistry` — registers structural templates for JSON data, matches items to templates
- `previous_versions: HashMap<u64, Vec<u8>>` — stores previous item versions for delta encoding

### Pipeline integration status

- Server-side: `compress_exact_bytes()` is called during emission; standalone eval validates compression round-trips
- Client-side: `decode_compressed_payload()` exists but needs integration into the record application flow
- Wire protocol: Dictionary/template/delta definition transport (sending definitions to the client) is not yet implemented

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

## Wire savings reality

Current predictive route planning produces negative wire savings (-45%/-27%/-13%) because every assembly definition is unique and inline. The definitions are never amortized. The amortization-aware inflation check (`estimate_definition_investment_bytes()` with `AMORTIZATION_BREAK_EVEN_REUSE=3`) rejects most inflated routes, causing fallback to direct-state which carries no compression. The P0-P5 compression pipeline is the intended fix.

## Benchmark maturity

Benchmark results are internal-debug quality only. No external performance claim should be made without:
- Separating protocol work from SourceCache I/O overhead
- Accounting for compression pipeline integration completeness
- Distinguishing harness overhead from wire efficiency

## Explicit exclusions

The repository does not treat the following as active architecture:
- dual old/new runtime semantics
- block-first predictive runtime carriers
- retained adapter scaffolding as sanctioned edge support
- transform routes as a live emission path
- benchmark results as externally-defensible performance evidence
- JSON payload encoding (replaced by postcard)
