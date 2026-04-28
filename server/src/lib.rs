// Server execution surface for CHPMT predictive-memory transport.
// Active runtime routing uses cue/object identity and native exact-state material throughout.

pub mod abi;
pub mod bench;
pub mod datagram_transport;
pub mod eval;
pub mod scenario;
pub mod source_cache;
pub mod transport;

use std::collections::{BTreeSet, HashMap, HashSet};

pub use abi::{
    NativeServerAbiConfig, NativeServerAcceptor, NativeServerCarrierKind, NativeServerSession,
    accept_native_session, serve_native_session, serve_native_session_once,
};
pub use datagram_transport::{
    AuthenticatedQuicDatagramSession, AuthenticatedUdpSession,
    AuthenticatedWebTransportDatagramSession, BoundWebTransportDatagramServer,
    accept_quic_datagram_session, accept_udp_session, accept_webtransport_datagram_session,
    bind_udp_socket, bind_webtransport_datagram_server, serve_quic_datagram_session_at,
    serve_udp_session_at, serve_webtransport_datagram_session_at,
};
pub use shared_protocol::{TransportConfig, TransportMode};

use rand_core::{CryptoRng, RngCore};
use shared_protocol::{
    Assembly, AssemblyDefPayload, AssemblyId, AssemblyPromotionQueue, AssemblyPromotionQueueEntry,
    BlockCatalogVersion, BlockId, BundleId, BundleMember, CacheOp, ChpmtObject, CodecMode,
    ConsolidationJob, ConsolidationJobKind, ConsolidationQueue, ContextHash, ContextTreeGovernor,
    ContextTreeOutcome, ContextTreeSymbol, ControllerRouteFamily, DataPlaneCodecPreference,
    DictionaryEntry, DictionaryId, DynamicFamilyAssignment, DynamicFamilyId, DynamicSubfamilyId,
    EpisodeActivationEvent, EpisodeCandidatePolicy, EpisodeCompletionCandidate, EpisodeMemory,
    EpisodeObjectRef, EpisodeReplayQueue, EpisodeReplayQueueEntry, EpochId, ExactStateMaterial,
    ItemId, LagBucket, MemoryAckPayload, MemoryPlane, MemoryRetirePayload, ObjectDependency,
    ObjectKind, ObjectVersion, OntologyOperation, OntologyOperationKind, OntologyState,
    PredictiveRouteDispatchPayload, PredictorEntryMeta, PromotionLevel, Record,
    RecordFlags, RecordHeader, RecordType, RekeyPayload, RepairPayload, RepairRequest,
    RouteAdmissibilityPolicy, RouteSelectionContext, RouteStatistics, SchemaCandidatePolicy,
    SchemaDefPayload, SchemaGraph, SchemaId, SchemaPromotionQueue, SchemaPromotionQueueEntry,
    SchemaRouteCandidate, SeqNo, SharedBlockCatalog, SharedDictionary, SharedDictionaryDefPayload,
    SourceDescriptor, SourceMetaPayload, SourceOptimizationConfig, StreamId,
    StreamProtector, TransformClass, TransformDefPayload, TransformId,
    TransformInstancePayload, TransformPromotionQueue, TransformPromotionQueueEntry,
    TransportSessionConfig, ValidationError,
    choose_route_by_family, derive_context_hash,
    encode_best_runtime_object_payload_with_preference, encode_episode_hint_record,
    encode_replay_hint_record, episode_hint_dependencies,
    extract_catalog_assembly_candidates, generate_episode_route_candidates,
    generate_schema_route_candidates, generate_transform_candidates,
    route_context_symbol_for_plan,
};

use crate::source_cache::{
    SourceCache, SourceCacheConfig, SourceCacheError, SourceIngestRequest, SourceResolveResult,
};

#[derive(Debug, Clone)]
pub struct ServerEntry {
    pub object: ChpmtObject,
}

impl ServerEntry {
    fn exact_bytes(&self) -> &[u8] {
        &self.object.exact_bytes
    }
}

/// S5.2: Single canonical helper for constructing ExactStateMaterial from source bytes.
/// Covers both transient/catalog-insertion and runtime/emission usage.
/// Previously there were two identical helpers (`transient_exact_material` and
/// `runtime_exact_material`); they have been collapsed into one.
fn transient_exact_material(
    source_kind: shared_protocol::SourceKind,
    exact_bytes: &[u8],
) -> ExactStateMaterial {
    ExactStateMaterial::copy_exact(source_kind, exact_bytes)
}

fn derived_dispatch_route_graph(
    route_family: ControllerRouteFamily,
    route_kind: shared_protocol::RouteFamily,
    dependency_closure: &[ObjectDependency],
    sync_risk: u32,
    contradiction_bytes: &[u8],
    literal_bytes: &[u8],
    assembly_ref: Option<&shared_protocol::AssemblyRef>,
    prg: Option<&shared_protocol::PredictiveReconstructionGraph>,
    hybrid_route: Option<&shared_protocol::HybridRoute>,
) -> shared_protocol::RouteGraphContract {
    PredictiveRouteDispatchPayload::derive_route_graph_contract_for(
        route_family,
        route_kind,
        dependency_closure,
        sync_risk,
        contradiction_bytes,
        literal_bytes,
        assembly_ref,
        prg,
        hybrid_route,
    )
}

fn is_structural_delimiter(byte: u8) -> bool {
    byte.is_ascii_whitespace()
        || matches!(
            byte,
            b',' | b';' | b':' | b'|' | b'/' | b'\\' | b'{' | b'}' | b'[' | b']' | b'(' | b')'
        )
}

fn structural_dictionary_segments(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut in_delimiter = bytes
        .first()
        .map(|byte| is_structural_delimiter(*byte))
        .unwrap_or(false);
    for (index, byte) in bytes.iter().enumerate() {
        let delimiter = is_structural_delimiter(*byte);
        if index == 0 {
            in_delimiter = delimiter;
            continue;
        }
        if delimiter != in_delimiter {
            out.push(bytes[start..index].to_vec());
            start = index;
            in_delimiter = delimiter;
        }
    }
    if start < bytes.len() {
        out.push(bytes[start..].to_vec());
    }
    out
}

fn dictionary_id_for_segments(
    source_kind: shared_protocol::SourceKind,
    segments: &[Vec<u8>],
) -> DictionaryId {
    let mut hash = shared_protocol::gf2_fingerprint(&[source_kind as u8]);
    for segment in segments {
        hash ^= shared_protocol::gf2_fingerprint(segment).rotate_left(7);
    }
    DictionaryId(hash)
}

#[derive(Debug, Clone, Default)]
pub struct SourceEmitMetrics {
    pub source_meta_emitted: bool,
    pub source_meta_baseline_bytes: u64,
    pub source_meta_payload_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SourceEmitResult {
    pub records: Vec<Record>,
    pub resolved: SourceResolveResult,
    pub metrics: SourceEmitMetrics,
}

/// S3.4: Explicit fallback reasons for route downgrades. Each variant
/// identifies a distinct cause for a predictive route falling through to
/// direct-state/exact-state fallback. This makes fallback causes observable
/// rather than silently masking planner defects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FallbackReason {
    /// Not a fallback — normal direct-state emission with no predictive route attempted.
    NotAFallback,
    /// The transform route is demoted (Branch B: I3).
    TransformDemoted,
    /// A dependency required by the planned route was not available on the peer
    /// (admissibility check failed after Sprint 1-3 changes).
    DependencyUnavailable,
    /// Substrate reference did not match any peer catalog entry.
    SubstrateMiss,
    /// Assembly body structuralization failed.
    AssemblyStructuralizationFailed,
    /// Schema route candidate had inadmissible dependencies.
    SchemaDependencyInadmissible,
    /// Dictionary route was rejected (e.g., wire cost exceeded literal).
    DictionaryRouteRejected,
    /// No predictive route candidate was viable for this item.
    NoViablePredictiveRoute,
    /// S4.7: Predictive route payload was larger than the original data
    /// (inline defs, dependency closures, PRG graphs inflated beyond raw).
    PredictiveRouteInflation,
}

impl std::fmt::Display for FallbackReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FallbackReason::NotAFallback => write!(f, "not_a_fallback"),
            FallbackReason::TransformDemoted => write!(f, "transform_demoted"),
            FallbackReason::DependencyUnavailable => write!(f, "dependency_unavailable"),
            FallbackReason::SubstrateMiss => write!(f, "substrate_miss"),
            FallbackReason::AssemblyStructuralizationFailed => {
                write!(f, "assembly_structuralization_failed")
            }
            FallbackReason::SchemaDependencyInadmissible => {
                write!(f, "schema_dependency_inadmissible")
            }
            FallbackReason::DictionaryRouteRejected => write!(f, "dictionary_route_rejected"),
            FallbackReason::NoViablePredictiveRoute => write!(f, "no_viable_predictive_route"),
            FallbackReason::PredictiveRouteInflation => write!(f, "predictive_route_inflation"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RouteFallbackMetrics {
    pub direct_state_downgrades: HashMap<String, u64>,
    /// S2.5.b3: Count of downgrades where the planned family was Transform
    /// but the transform route was demoted, falling through to direct-state.
    pub transform_demoted_downgrades: u64,
    /// S3.4: Explicit fallback reason counts, keyed by (route_family, reason).
    /// This makes fallback causes observable and auditable without relying on
    /// flag bits alone.
    pub fallback_reasons: HashMap<String, u64>,
}

/// Maximum number of entries in the server's `entries` and `predictors` maps.
/// When this cap is exceeded, the oldest entries are evicted to bound memory.
/// This prevents the "10 MB memory cliff" where RSS grows to 657+ MB because
/// every inserted item is stored indefinitely in both maps.
const MAX_SERVER_ENTRIES: usize = 16384;

/// Maximum number of pending replay hints before backpressure is applied.
const MAX_PENDING_REPLAY_HINTS: usize = 256;

/// P0: Compression metrics tracked on the server side, before AEAD protection.
#[derive(Debug, Clone, Default)]
pub struct CompressionMetrics {
    /// Number of records where zstd compression was applied.
    pub records_compressed: usize,
    /// Number of records where compression was skipped (too small / incompressible).
    pub records_skipped: usize,
    /// Total payload bytes BEFORE compression.
    pub pre_compress_bytes: usize,
    /// Total payload bytes AFTER compression.
    pub post_compress_bytes: usize,
    /// Total bytes saved by compression.
    pub savings_bytes: usize,
}

impl CompressionMetrics {
    pub fn savings_pct(&self) -> f64 {
        if self.pre_compress_bytes == 0 {
            return 0.0;
        }
        100.0 * self.savings_bytes as f64 / self.pre_compress_bytes as f64
    }
}

/// Maximum number of pending memory retire payloads.
const MAX_PENDING_MEMORY_RETIRES: usize = 256;

/// Maximum number of received memory acks retained for processing.
const MAX_RECEIVED_MEMORY_ACKS: usize = 1024;

#[derive(Debug)]
pub struct ServerState {
    entries: HashMap<ItemId, ServerEntry>,
    predictors: HashMap<ItemId, ServerEntry>,
    source_bindings: HashMap<ItemId, SourceDescriptor>,
    local_catalog: SharedBlockCatalog,
    peer_catalog: SharedBlockCatalog,
    next_catalog_version: u64,
    assemblies: HashMap<AssemblyId, Assembly>,
    assembly_cache: Option<SourceCache>,
    // CONFIRMED peer-visible state: only mutated upon receipt of MemoryAck
    // from the client, never at emission time.
    // INVARIANT: admissibility decisions read confirmed state ONLY.
    // Same-batch inline definitions are satisfied via the inline_*_ids
    // parameters passed to predictive_dependency_is_admissible_with_inline.
    // Pending (emitted but unacknowledged) state is NOT consulted for
    // admissibility — if a dependency was emitted in a prior record but
    // not yet confirmed by the peer, the route must either carry the
    // definition inline again or fall back to direct state.
    assembly_sync_versions: HashMap<AssemblyId, shared_protocol::AssemblyReuseSignature>,
    // PENDING peer-visible state: objects emitted but not yet confirmed installed.
    // Promoted to confirmed trackers upon MemoryAck receipt.
    // Cleared on resync/reset/disconnect.
    pending_assembly_sync_versions: HashMap<AssemblyId, shared_protocol::AssemblyReuseSignature>,
    assembly_promotion_queue: AssemblyPromotionQueue,
    transform_promotion_queue: TransformPromotionQueue,
    episode_memory: EpisodeMemory,
    replay_queue: EpisodeReplayQueue,
    pending_replay_hints: Vec<(ItemId, EpisodeReplayQueueEntry)>,
    pending_memory_retires: Vec<MemoryRetirePayload>,
    received_memory_acks: Vec<MemoryAckPayload>,
    // S5.3: schema_defs is the AUTHORITATIVE schema store. The `schemas` Vec
    // is a derived convenience view for ordered iteration; it must always be
    // kept in sync with schema_defs. Whenever schema_defs is updated, the
    // corresponding entry must be updated in schemas (or the Vec must be
    // rebuilt). Use install_schema_def() as the primary write path.
    // INVARIANT: for every schema_id in schema_defs, there is exactly one
    // matching entry in schemas (and vice versa).
    schemas: Vec<SchemaGraph>,
    schema_defs: HashMap<SchemaId, SchemaDefPayload>,
    dictionaries: HashMap<DictionaryId, SharedDictionary>,
    // CONFIRMED: peer has acknowledged installation
    peer_dictionary_versions: HashMap<DictionaryId, u64>,
    peer_schema_versions: HashMap<SchemaId, u32>,
    peer_transform_versions: HashMap<TransformId, u32>,
    // PENDING: emitted but not yet confirmed by the peer
    pending_dictionary_versions: HashMap<DictionaryId, u64>,
    pending_schema_versions: HashMap<SchemaId, u32>,
    pending_transform_versions: HashMap<TransformId, u32>,
    schema_promotion_queue: SchemaPromotionQueue,
    route_statistics: HashMap<String, RouteStatistics>,
    fallback_metrics: RouteFallbackMetrics,
    consolidation_queue: ConsolidationQueue,
    ontology_state: OntologyState,
    context_governor: ContextTreeGovernor,
    // P0-P5 compression pipeline state
    dict_manager: shared_protocol::DictionaryManager,
    template_registry: shared_protocol::TemplateRegistry,
    previous_versions: HashMap<u64, Vec<u8>>,
    // P0: Compression metrics tracked before AEAD protection.
    compression_metrics: CompressionMetrics,
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            predictors: HashMap::new(),
            source_bindings: HashMap::new(),
            local_catalog: SharedBlockCatalog::default(),
            peer_catalog: SharedBlockCatalog::default(),
            next_catalog_version: 0,
            assemblies: HashMap::new(),
            assembly_cache: SourceCache::new(SourceCacheConfig::default()).ok(),
            assembly_sync_versions: HashMap::new(),
            pending_assembly_sync_versions: HashMap::new(),
            assembly_promotion_queue: AssemblyPromotionQueue::default(),
            transform_promotion_queue: TransformPromotionQueue::default(),
            episode_memory: EpisodeMemory::default(),
            replay_queue: EpisodeReplayQueue::default(),
            pending_replay_hints: Vec::new(),
            pending_memory_retires: Vec::new(),
            received_memory_acks: Vec::new(),
            schemas: Vec::new(),
            schema_defs: HashMap::new(),
            dictionaries: HashMap::new(),
            peer_dictionary_versions: HashMap::new(),
            peer_schema_versions: HashMap::new(),
            peer_transform_versions: HashMap::new(),
            pending_dictionary_versions: HashMap::new(),
            pending_schema_versions: HashMap::new(),
            pending_transform_versions: HashMap::new(),
            schema_promotion_queue: SchemaPromotionQueue::default(),
            route_statistics: HashMap::new(),
            fallback_metrics: RouteFallbackMetrics::default(),
            consolidation_queue: ConsolidationQueue::default(),
            ontology_state: OntologyState::default(),
            context_governor: ContextTreeGovernor::default(),
            dict_manager: shared_protocol::DictionaryManager::default(),
            template_registry: shared_protocol::TemplateRegistry::default(),
            previous_versions: HashMap::new(),
            compression_metrics: CompressionMetrics::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ServerEvent {
    Insert {
        item_id: ItemId,
        block: ExactStateMaterial,
    },
    UpsertObject {
        item_id: ItemId,
        block: ExactStateMaterial,
    },
    // S5.1: StateDef and StatePatch variants REMOVED from ServerEvent.
    // These were retired flat-state runtime lanes. The wire discriminant
    // values (6, 7) are retired in RecordType; try_from_u8 returns
    // WireError::RetiredDiscriminant for those values.
    Evict {
        item_id: ItemId,
    },
    Invalidate {
        item_id: ItemId,
    },
}

#[derive(Debug)]
pub struct ServerSession {
    state: ServerState,
    protector: StreamProtector,
    optimizations: SourceOptimizationConfig,
}

#[derive(Debug)]
pub struct PlainServerSession {
    state: ServerState,
    stream_id: StreamId,
    epoch_id: EpochId,
    next_seq_no: SeqNo,
    pending_post_rekey_confirm: bool,
    optimizations: SourceOptimizationConfig,
}

#[derive(Debug, Clone, Copy)]
struct HeaderContext {
    stream_id: StreamId,
    epoch_id: EpochId,
    seq_no: SeqNo,
    post_rekey_confirm: bool,
}

impl HeaderContext {
    #[cfg(test)]
    fn new(stream_id: StreamId, epoch_id: EpochId, seq_no: SeqNo) -> Self {
        Self {
            stream_id,
            epoch_id,
            seq_no,
            post_rekey_confirm: false,
        }
    }

    fn flags(self, mut flags: RecordFlags) -> RecordFlags {
        if self.post_rekey_confirm {
            flags.insert(RecordFlags::POST_REKEY_CONFIRM);
        }
        flags
    }
}

fn metadata_baseline_bytes(descriptor: &SourceDescriptor) -> u64 {
    (shared_protocol::SOURCE_HASH_LEN + 1 + 8 + 2 + 2) as u64
        + descriptor
            .mime
            .as_ref()
            .map(|value| value.len() as u64)
            .unwrap_or(0)
        + descriptor
            .label
            .as_ref()
            .map(|value| value.len() as u64)
            .unwrap_or(0)
}

fn build_source_meta_payload(descriptor: &SourceDescriptor) -> SourceMetaPayload {
    SourceMetaPayload {
        source_hash: descriptor.source_hash,
        source_kind: descriptor.kind,
        source_len: descriptor.byte_len as u64,
        mime: descriptor.mime.clone(),
        label: descriptor.label.clone(),
    }
}

fn control_plane_object_kind(plane: MemoryPlane, object_id: &str) -> ObjectKind {
    match plane {
        MemoryPlane::Assembly => ObjectKind::Assembly,
        MemoryPlane::Transform => ObjectKind::Transform,
        MemoryPlane::Schema => ObjectKind::Schema,
        MemoryPlane::Episode => {
            if object_id.starts_with("replay:") {
                ObjectKind::ReplayHint
            } else {
                ObjectKind::EpisodeHint
            }
        }
        MemoryPlane::AtomSubstrate => ObjectKind::ExactState,
        MemoryPlane::Controller => ObjectKind::PredictiveObject,
    }
}

fn normalize_dependencies(mut dependencies: Vec<ObjectDependency>) -> Vec<ObjectDependency> {
    dependencies.sort_by(|left, right| {
        (left.object_kind as u8)
            .cmp(&(right.object_kind as u8))
            .then_with(|| left.object_id.cmp(&right.object_id))
            .then_with(|| left.required_revision.cmp(&right.required_revision))
    });
    dependencies.dedup();
    dependencies
}

fn dedup_schema_payloads(mut payloads: Vec<SchemaDefPayload>) -> Vec<SchemaDefPayload> {
    payloads.sort_by_key(|payload| payload.schema.schema_id.0);
    payloads.dedup_by_key(|payload| payload.schema.schema_id);
    payloads
}

fn dependency_from_substrate_ref(reference: &shared_protocol::SubstrateRef) -> ObjectDependency {
    ObjectDependency {
        object_kind: reference.object_kind,
        object_id: reference.object_id.to_string(),
        required_revision: reference.version.object_revision,
    }
}

fn dependency_from_assembly_ref(reference: &shared_protocol::AssemblyRef) -> ObjectDependency {
    ObjectDependency {
        object_kind: ObjectKind::Assembly,
        object_id: format!("assembly:{}", reference.assembly_id.0),
        required_revision: reference.version.object_revision,
    }
}

fn dependency_from_transform_id(transform_id: shared_protocol::TransformId) -> ObjectDependency {
    ObjectDependency {
        object_kind: ObjectKind::Transform,
        object_id: format!("transform:{}", transform_id.0),
        required_revision: 0,
    }
}

fn dependency_from_schema_id(schema_id: SchemaId) -> ObjectDependency {
    ObjectDependency {
        object_kind: ObjectKind::Schema,
        object_id: format!("schema:{}", schema_id.0),
        required_revision: 0,
    }
}

fn collect_prg_dependencies(graph: &shared_protocol::PredictiveReconstructionGraph) -> Vec<ObjectDependency> {
    let mut dependencies = Vec::new();
    for node in &graph.nodes {
        if let Some(reference) = &node.substrate_ref {
            dependencies.push(dependency_from_substrate_ref(reference));
        }
        if let Some(reference) = &node.assembly_ref {
            dependencies.push(dependency_from_assembly_ref(reference));
        }
        if let Some(transform_id) = node.transform_ref {
            dependencies.push(dependency_from_transform_id(transform_id));
        }
        if let Some(reference) = &node.episode_ref {
            dependencies.push(ObjectDependency {
                object_kind: reference.object_kind,
                object_id: reference.object_id.clone(),
                required_revision: 0,
            });
        }
        if let Some(schema_id) = node.schema_ref {
            dependencies.push(dependency_from_schema_id(schema_id));
        }
    }
    // S2.3.d: normalize_dependencies sorts by (kind, id, revision) then dedups,
    // ensuring deterministic deduplication of closure entries.
    normalize_dependencies(dependencies)
}

fn collect_hybrid_route_dependencies(route: &shared_protocol::HybridRoute) -> Vec<ObjectDependency> {
    let mut dependencies = route.dependency_closure.clone();
    for component in &route.components {
        match component {
            shared_protocol::HybridRouteComponent::Substrate(reference) => {
                dependencies.push(dependency_from_substrate_ref(reference));
            }
            shared_protocol::HybridRouteComponent::SubstrateGraph(graph) => {
                dependencies.extend(graph.dependency_closure.clone());
            }
            shared_protocol::HybridRouteComponent::Assembly(reference) => {
                dependencies.push(dependency_from_assembly_ref(reference));
            }
            shared_protocol::HybridRouteComponent::Transform(transform_id) => {
                dependencies.push(dependency_from_transform_id(*transform_id));
            }
            shared_protocol::HybridRouteComponent::DictionaryTokens { dictionary_id, .. } => {
                dependencies.push(ObjectDependency {
                    object_kind: ObjectKind::Dictionary,
                    object_id: format!("dictionary:{}", dictionary_id.0),
                    required_revision: 0,
                });
            }
            shared_protocol::HybridRouteComponent::Schema(graph) => {
                dependencies.extend(collect_prg_dependencies(graph));
            }
            shared_protocol::HybridRouteComponent::Literal(_)
            | shared_protocol::HybridRouteComponent::ResidualPatch { .. } => {}
        }
    }
    // S2.3.d: normalize_dependencies sorts by (kind, id, revision) then dedups,
    // ensuring deterministic deduplication of closure entries.
    normalize_dependencies(dependencies)
}

/// S4.8: Estimate the byte cost of inline definitions carried by a
/// PredictiveRouteDispatchPayload. These definitions are the "investment"
/// that causes inflation in the current record but will pay off in future
/// records that can reference them via compact IDs instead of re-sending
/// the full definition data.
///
/// The estimate is based on the number and size of inline definition
/// payloads. We use a quick heuristic rather than re-encoding each
/// definition separately, since the exact cost depends on bincode encoding
/// details that may vary. The heuristic counts:
///   - Each assembly definition: ~estimated size based on body node count
///   - Each schema definition: ~estimated size based on schema field count
///   - Each dictionary definition: ~estimated size based on entry count
///   - Each episode hint: ~fixed overhead per hint
///   - Dependency closure entries: ~fixed overhead per dependency
fn estimate_definition_investment_bytes(payload: &PredictiveRouteDispatchPayload) -> usize {
    let mut estimate = 0usize;

    // Inline assembly definitions — these carry the full assembly body
    // including all nodes, which is the largest definition investment.
    for assembly_def in &payload.inline_assembly_defs {
        // Each body node contributes ~64-256 bytes in bincode encoding
        // (node kind tag + content fields + metadata).
        let node_estimate = assembly_def.assembly.body.nodes.len() * 128;
        // Assembly header + metadata overhead.
        estimate += 64 + node_estimate;
    }

    // Inline schema definitions — carry schema slots, nodes, edges, and
    // dependency closures. Variable size depending on schema complexity.
    for schema_def in &payload.inline_schema_defs {
        // Schema slots contribute ~32 bytes each in bincode.
        let slot_estimate = schema_def.schema.slots.len() * 32;
        // Schema nodes contribute ~64 bytes each.
        let node_estimate = schema_def.schema.nodes.len() * 64;
        // Schema edges contribute ~32 bytes each.
        let edge_estimate = schema_def.schema.edges.len() * 32;
        // Schema header + metadata overhead.
        estimate += 64 + slot_estimate + node_estimate + edge_estimate;
    }

    // Inline dictionary definitions — carry dictionary entries.
    for dict_def in &payload.inline_dictionaries {
        // Each dictionary entry contributes ~16-64 bytes.
        let entry_estimate = dict_def.dictionary.entries.len() * 32;
        estimate += 32 + entry_estimate;
    }

    // Inline episode hints — relatively small per-hint overhead.
    for hint in &payload.inline_episode_hints {
        // Each hint carries context_hash, lag_bucket, precision_band,
        // dependencies, and candidates. Estimate based on candidate count.
        let candidate_estimate = hint.candidates.len() * 32;
        let dep_estimate = hint.dependencies.len() * 48;
        estimate += 32 + candidate_estimate + dep_estimate;
    }

    // Dependency closure entries — each carries object_kind, object_id,
    // and required_revision. These are typically ~40-80 bytes in bincode.
    estimate += payload.dependency_closure.len() * 48;

    // The PRG graph itself (if present) is a definition investment — it
    // establishes reconstruction logic that can be referenced by future
    // routes. Include its estimated size.
    if let Some(prg) = &payload.prg {
        estimate += 32 + prg.nodes.len() * 64;
    }

    // Hybrid route components include their own definitions.
    if let Some(hybrid) = &payload.hybrid_route {
        for component in &hybrid.components {
            match component {
                shared_protocol::HybridRouteComponent::SubstrateGraph(graph) => {
                    estimate += 32 + graph.nodes.len() * 64;
                }
                shared_protocol::HybridRouteComponent::Schema(graph) => {
                    estimate += 32 + graph.nodes.len() * 64;
                }
                _ => {}
            }
        }
    }

    // Ensure at least a minimum estimate so the amortization budget is
    // never zero (which would reject all inflation).
    estimate.max(64)
}

impl ServerState {
    /// P0-P5: Compress exact_bytes using the adaptive compression pipeline.
    /// Feeds samples to the dictionary manager, trains dictionaries if possible,
    /// selects the best strategy, and returns the compressed bytes wrapped with
    /// a self-describing compression tag.
    fn compress_exact_bytes(
        &mut self,
        source_kind: shared_protocol::SourceKind,
        item_id: ItemId,
        exact_bytes: &[u8],
    ) -> Vec<u8> {
        // Feed sample to dictionary manager for training.
        self.dict_manager.add_sample(source_kind, exact_bytes);

        // Try to register a template (JSON only).
        if source_kind == shared_protocol::SourceKind::Json {
            self.template_registry.try_register(source_kind, exact_bytes);
        }

        // Try to train a dictionary periodically.
        self.dict_manager.maybe_train(source_kind);

        // Select the best compression strategy.
        let available_dict = self.dict_manager.get_dictionary(source_kind)
            .map(|d| d.dict_id);
        let matching_template = if source_kind == shared_protocol::SourceKind::Json {
            self.template_registry.find_match(source_kind, exact_bytes)
                .map(|(id, _)| id)
        } else {
            None
        };
        let is_update = self.previous_versions.contains_key(&item_id.0);
        let previous_item_id = if is_update { Some(item_id.0) } else { None };

        let strategy = shared_protocol::select_strategy(
            source_kind,
            exact_bytes,
            is_update,
            is_update,
            available_dict,
            matching_template,
            previous_item_id,
        );

        // Apply the compression strategy.
        // Returns (actual_strategy, compressed_data) so the tag matches
        // the actual encoding used (strategy may be downgraded on fallback).
        let (actual_strategy, compressed_data) = match strategy {
            shared_protocol::CompressionStrategy::Passthrough => {
                (strategy, exact_bytes.to_vec())
            }
            shared_protocol::CompressionStrategy::ZstdDict { .. } => {
                match self.dict_manager.compress_with_dict(source_kind, exact_bytes) {
                    Some(Ok(compressed)) => (strategy, compressed),
                    _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                }
            }
            shared_protocol::CompressionStrategy::ZstdRaw => {
                match shared_protocol::zstd_compress_raw(exact_bytes) {
                    Ok(compressed) if compressed.len() < exact_bytes.len() => (strategy, compressed),
                    _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                }
            }
            shared_protocol::CompressionStrategy::Delta { base_item_id } => {
                if base_item_id == item_id.0 {
                    if let Some(base) = self.previous_versions.get(&item_id.0) {
                        let delta = shared_protocol::BinaryDelta::compute(base, exact_bytes);
                        let delta_bytes = delta.encode_to_bytes();
                        if delta_bytes.len() < exact_bytes.len() {
                            (strategy, delta_bytes)
                        } else {
                            // Delta is larger — fall back to Zstd raw.
                            match shared_protocol::zstd_compress_raw(exact_bytes) {
                                Ok(compressed) if compressed.len() < exact_bytes.len() => {
                                    (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                                }
                                _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                            }
                        }
                    } else {
                        match shared_protocol::zstd_compress_raw(exact_bytes) {
                            Ok(compressed) if compressed.len() < exact_bytes.len() => {
                                (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                            }
                            _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                        }
                    }
                } else {
                    match shared_protocol::zstd_compress_raw(exact_bytes) {
                        Ok(compressed) if compressed.len() < exact_bytes.len() => {
                            (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                        }
                        _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                    }
                }
            }
            shared_protocol::CompressionStrategy::Template { template_id } => {
                if let Some(template) = self.template_registry.get_template(template_id) {
                    if let Some(values) = template.extract_values(exact_bytes) {
                        let encoded = shared_protocol::StructuralTemplate::encode_slot_values(&values);
                        if encoded.len() < exact_bytes.len() {
                            (strategy, encoded)
                        } else {
                            match shared_protocol::zstd_compress_raw(exact_bytes) {
                                Ok(compressed) if compressed.len() < exact_bytes.len() => {
                                    (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                                }
                                _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                            }
                        }
                    } else {
                        match shared_protocol::zstd_compress_raw(exact_bytes) {
                            Ok(compressed) if compressed.len() < exact_bytes.len() => {
                                (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                            }
                            _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                        }
                    }
                } else {
                    match shared_protocol::zstd_compress_raw(exact_bytes) {
                        Ok(compressed) if compressed.len() < exact_bytes.len() => {
                            (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                        }
                        _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                    }
                }
            }
            shared_protocol::CompressionStrategy::Columnar => {
                match shared_protocol::zstd_compress_raw(exact_bytes) {
                    Ok(compressed) if compressed.len() < exact_bytes.len() => {
                        (shared_protocol::CompressionStrategy::ZstdRaw, compressed)
                    }
                    _ => (shared_protocol::CompressionStrategy::Passthrough, exact_bytes.to_vec()),
                }
            }
        };

        // Store previous version for delta encoding.
        self.previous_versions.insert(item_id.0, exact_bytes.to_vec());

        // Wrap with self-describing compression tag using the actual strategy.
        shared_protocol::encode_compressed_payload(actual_strategy, &compressed_data)
    }

    pub fn cache_entry(&self, item_id: ItemId) -> Option<&ServerEntry> {
        self.entries.get(&item_id)
    }

    pub fn cache_len(&self) -> usize {
        self.entries.len()
    }

    pub fn predictor_entry(&self, item_id: ItemId) -> Option<&ServerEntry> {
        self.predictors.get(&item_id)
    }

    pub fn predictor_len(&self) -> usize {
        self.predictors.len()
    }

    pub fn enqueue_assembly_promotion(&mut self, entry: AssemblyPromotionQueueEntry) {
        self.assembly_promotion_queue.push(entry);
    }

    pub fn promote_ready_assemblies(
        &mut self,
        ambiguity_cap: u32,
        min_support: u32,
    ) -> Vec<AssemblyPromotionQueueEntry> {
        self.assembly_promotion_queue
            .promote_ready(ambiguity_cap, min_support)
    }

    pub fn enqueue_transform_promotion(&mut self, entry: TransformPromotionQueueEntry) {
        self.transform_promotion_queue.push(entry);
    }

    pub fn append_episode_activation(&mut self, event: EpisodeActivationEvent) {
        self.append_episode_activation_for_item(ItemId(0), event);
    }

    pub fn append_episode_activation_for_item(
        &mut self,
        item_id: ItemId,
        event: EpisodeActivationEvent,
    ) {
        self.episode_memory.append_activation(event.clone());
        let predictions = generate_episode_route_candidates(
            &self.episode_memory,
            EpisodeCandidatePolicy::bounded_default(),
        );
        let top = predictions.first();
        let surprise = if let Some(top) = top {
            if top.object_ref == event.object_ref {
                0
            } else {
                top.transition_count.0.saturating_add(1)
            }
        } else {
            1
        };
        let ambiguity = predictions.len() as u32;
        let reuse_potential = event.cue.overlap_score(event.cue);
        let replay_entry = EpisodeReplayQueueEntry {
            event,
            surprise,
            reuse_potential,
            ambiguity,
        };
        self.replay_queue.push(replay_entry.clone());
        // Cap pending replay hints to bound memory; drop oldest if over capacity.
        if self.pending_replay_hints.len() >= MAX_PENDING_REPLAY_HINTS {
            self.pending_replay_hints.remove(0);
        }
        self.pending_replay_hints.push((item_id, replay_entry));
    }

    pub fn episode_memory(&self) -> &EpisodeMemory {
        &self.episode_memory
    }

    pub fn bounded_context_predictions(&self) -> Vec<EpisodeCompletionCandidate> {
        self.filter_episode_candidates(generate_episode_route_candidates(
            &self.episode_memory,
            EpisodeCandidatePolicy::bounded_default(),
        ))
    }

    pub fn episode_route_candidates(
        &self,
        policy: EpisodeCandidatePolicy,
    ) -> Vec<EpisodeCompletionCandidate> {
        self.filter_episode_candidates(generate_episode_route_candidates(
            &self.episode_memory,
            policy,
        ))
    }

    fn filter_episode_candidates(
        &self,
        candidates: Vec<EpisodeCompletionCandidate>,
    ) -> Vec<EpisodeCompletionCandidate> {
        candidates
            .into_iter()
            .filter(|candidate| self.predictive_object_ref_is_admissible(&candidate.object_ref))
            .collect()
    }

    // S1.6.d: Answers "can we route to this object in a predictive dispatch?"
    // Checks peer-visible availability for episode/assembly routing.
    // S1.1.v3 CONFIRMED-ONLY: reads confirmed peer state only, not pending.
    fn predictive_object_ref_is_admissible(&self, object_ref: &EpisodeObjectRef) -> bool {
        match object_ref.object_kind {
            ObjectKind::Assembly => object_ref
                .object_id
                .strip_prefix("assembly:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(AssemblyId)
                .map(|id| self.assembly_sync_versions.contains_key(&id))
                .unwrap_or(false),
            ObjectKind::Schema => object_ref
                .object_id
                .strip_prefix("schema:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(SchemaId)
                .map(|id| self.peer_schema_versions.contains_key(&id))
                .unwrap_or(false),
            ObjectKind::Dictionary => object_ref
                .object_id
                .strip_prefix("dictionary:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(DictionaryId)
                .map(|id| self.peer_dictionary_versions.contains_key(&id))
                .unwrap_or(false),
            ObjectKind::Transform => object_ref
                .object_id
                .strip_prefix("transform:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(TransformId)
                .map(|id| self.peer_transform_versions.contains_key(&id))
                .unwrap_or(false),
            ObjectKind::ExactBlock => object_ref
                .object_id
                .strip_prefix("block:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(BlockId)
                .map(|id| self.peer_catalog.contains_block(id))
                .unwrap_or(false),
            ObjectKind::ExactBundle => object_ref
                .object_id
                .strip_prefix("bundle:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(BundleId)
                .map(|id| self.peer_catalog.contains_bundle(id))
                .unwrap_or(false),
            ObjectKind::ExactRange => object_ref
                .object_id
                .strip_prefix("range:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(BlockId)
                .map(|id| self.peer_catalog.contains_block(id))
                .unwrap_or(false),
            ObjectKind::ExactState
            | ObjectKind::AtomFragment
            | ObjectKind::PredictiveObject
            | ObjectKind::SourceDescriptor
            | ObjectKind::SparseCue
            | ObjectKind::EpisodeHint
            | ObjectKind::ReplayHint
            | ObjectKind::ResidualBuffer => false,
        }
    }

    // S1.6.d: Answers "can we emit a reference to this object now?"
    // Returns true if the peer either has the object confirmed installed,
    // or we've emitted it in this session (pending) and it may arrive
    // before the referencing route. Does NOT answer "have we ever sent
    // something like this?" — that would be the union of confirmed + pending
    // + any historical emission records, which is not tracked.
    fn predictive_dependency_is_admissible(&self, dependency: &ObjectDependency) -> bool {
        let inline_assembly_ids = HashSet::new();
        let inline_schema_ids = HashSet::new();
        let inline_dictionary_ids = HashSet::new();
        let inline_transform_ids = HashSet::new();
        self.predictive_dependency_is_admissible_with_inline(
            dependency,
            &inline_assembly_ids,
            &inline_schema_ids,
            &inline_dictionary_ids,
            &inline_transform_ids,
        )
    }

    fn predictive_dependency_is_admissible_with_inline(
        &self,
        dependency: &ObjectDependency,
        inline_assembly_ids: &HashSet<AssemblyId>,
        inline_schema_ids: &HashSet<SchemaId>,
        inline_dictionary_ids: &HashSet<DictionaryId>,
        inline_transform_ids: &HashSet<TransformId>,
    ) -> bool {
        match dependency.object_kind {
            ObjectKind::Assembly => dependency
                .object_id
                .strip_prefix("assembly:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(AssemblyId)
                .map(|id| {
                    // S1.1.v3 CONFIRMED-ONLY: admissibility reads confirmed state only.
                    // Same-batch inline definitions are satisfied via inline_assembly_ids.
                    // Pending (emitted but unacknowledged) state is NOT consulted, because
                    // the peer may not have received or successfully installed the definition.
                    // If a dependency was emitted in a prior record but not yet confirmed,
                    // the route must either carry it inline again or fall back to direct state.
                    // S1.4.e: Full revision tracking for assembly dependencies.
                    inline_assembly_ids.contains(&id)
                        || self.assembly_sync_versions.get(&id)
                            .map(|sig| sig.version.object_revision >= dependency.required_revision)
                            .unwrap_or(false)
                })
                .unwrap_or(false),
            ObjectKind::Schema => dependency
                .object_id
                .strip_prefix("schema:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(SchemaId)
                .map(|id| {
                    // S1.1.v3 CONFIRMED-ONLY: no pending_schema_ids check.
                    // S1.4.e: Full revision tracking for schema dependencies.
                    inline_schema_ids.contains(&id)
                        || (self.peer_schema_versions.get(&id).copied().unwrap_or(0) >= dependency.required_revision)
                })
                .unwrap_or(false),
            ObjectKind::Dictionary => dependency
                .object_id
                .strip_prefix("dictionary:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(DictionaryId)
                .map(|id| {
                    // S1.1.v3 CONFIRMED-ONLY: no pending_dictionary_versions check.
                    inline_dictionary_ids.contains(&id)
                        || self
                            .peer_dictionary_versions
                            .get(&id)
                            .copied()
                            .unwrap_or(0)
                            >= u64::from(dependency.required_revision)
                })
                .unwrap_or(false),
            ObjectKind::Transform => dependency
                .object_id
                .strip_prefix("transform:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(TransformId)
                .map(|id| {
                    // S1.1.v3 CONFIRMED-ONLY: no pending_transform_ids check.
                    // S1.4.e: Full revision tracking for transform dependencies.
                    // Same-batch inline transform definitions are satisfied via
                    // inline_transform_ids, enabling the 7th route family.
                    inline_transform_ids.contains(&id)
                        || self.peer_transform_versions.get(&id).copied().unwrap_or(0) >= dependency.required_revision
                })
                .unwrap_or(false),
            ObjectKind::ExactBlock => dependency
                .object_id
                .strip_prefix("block:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(BlockId)
                .map(|id| self.peer_catalog.contains_block(id))
                .unwrap_or(false),
            ObjectKind::ExactBundle => dependency
                .object_id
                .strip_prefix("bundle:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(BundleId)
                .map(|id| self.peer_catalog.contains_bundle(id))
                .unwrap_or(false),
            ObjectKind::ExactRange => dependency
                .object_id
                .strip_prefix("range:")
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(BlockId)
                .map(|id| self.peer_catalog.contains_block(id))
                .unwrap_or(false),
            ObjectKind::ExactState
            | ObjectKind::AtomFragment
            | ObjectKind::PredictiveObject
            | ObjectKind::SourceDescriptor
            | ObjectKind::SparseCue
            | ObjectKind::EpisodeHint
            | ObjectKind::ReplayHint
            | ObjectKind::ResidualBuffer => false,
        }
    }

    fn filter_schema_candidates(
        &self,
        candidates: Vec<SchemaRouteCandidate>,
    ) -> Vec<SchemaRouteCandidate> {
        candidates
            .into_iter()
            .filter(|candidate| {
                candidate
                    .dependency_closure
                    .dependencies
                    .iter()
                    .all(|dependency| self.predictive_dependency_is_admissible(dependency))
            })
            .collect()
    }

    pub fn pending_replay_queue(&self) -> &[EpisodeReplayQueueEntry] {
        &self.replay_queue.pending
    }

    pub fn received_memory_acks(&self) -> &[MemoryAckPayload] {
        &self.received_memory_acks
    }

    pub fn record_route_outcome(
        &mut self,
        route_family: ControllerRouteFamily,
        source_kind: Option<shared_protocol::SourceKind>,
        context_hash: Option<ContextHash>,
        tick: u64,
        success: bool,
    ) {
        self.record_route_outcome_detail(
            route_family,
            source_kind,
            context_hash,
            tick,
            success,
            None,
            ContextTreeOutcome {
                success,
                ..ContextTreeOutcome::default()
            },
        );
    }

    pub fn record_route_outcome_detail(
        &mut self,
        route_family: ControllerRouteFamily,
        source_kind: Option<shared_protocol::SourceKind>,
        context_hash: Option<ContextHash>,
        tick: u64,
        success: bool,
        symbol: Option<ContextTreeSymbol>,
        outcome: ContextTreeOutcome,
    ) {
        let key = format!(
            "{}:{:?}:{:?}",
            route_family as u8,
            source_kind,
            context_hash.map(|v| v.0)
        );
        let stats = self.route_statistics.entry(key).or_insert(RouteStatistics {
            route_family,
            source_kind,
            context_hash,
            success_count: 0,
            failure_count: 0,
            last_seen_tick: 0,
        });
        stats.record(tick, success);
        // Prune stale route statistics to bound memory. Remove entries whose
        // last_seen_tick is more than 1024 ticks behind the current tick.
        // This prevents unbounded growth of route_statistics over long sessions.
        if self.route_statistics.len() > 256 && tick % 64 == 0 {
            self.route_statistics
                .retain(|_, stats| tick.saturating_sub(stats.last_seen_tick) < 1024);
        }
        if let Some(symbol) = symbol {
            self.context_governor.observe(symbol, outcome);
        }
        self.episode_memory.record_route_outcome(
            route_family,
            source_kind,
            context_hash,
            tick,
            success,
        );
    }

    pub fn route_statistics(&self) -> impl Iterator<Item = &RouteStatistics> {
        self.route_statistics.values()
    }

    pub fn fallback_metrics(&self) -> &RouteFallbackMetrics {
        &self.fallback_metrics
    }

    /// P0: Returns compression metrics tracked on the server side.
    pub fn compression_metrics(&self) -> &CompressionMetrics {
        &self.compression_metrics
    }

    fn record_direct_state_downgrade(
        &mut self,
        planned_family: ControllerRouteFamily,
        source_kind: shared_protocol::SourceKind,
        context_hash: Option<ContextHash>,
        tick: u64,
        reason: FallbackReason,
    ) {
        self.record_route_outcome(planned_family, Some(source_kind), context_hash, tick, false);
        let key = format!("{}", planned_family as u8);
        *self
            .fallback_metrics
            .direct_state_downgrades
            .entry(key)
            .or_insert(0) += 1;
        // S3.4: Track explicit fallback reason for observability.
        let reason_key = format!("{}:{}", planned_family as u8, reason);
        *self
            .fallback_metrics
            .fallback_reasons
            .entry(reason_key)
            .or_insert(0) += 1;
        // S2.5.b3: Track transform-path downgrades explicitly so demoted
        // transform routes are not silently counted as generic direct-state downgrades.
        if planned_family == ControllerRouteFamily::Transform {
            self.fallback_metrics.transform_demoted_downgrades += 1;
        }
    }

    fn emit_direct_state_fallback_data(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        block: ExactStateMaterial,
        planned_family: ControllerRouteFamily,
        source_kind: shared_protocol::SourceKind,
        context_hash: Option<ContextHash>,
        reason: FallbackReason,
    ) -> Result<Record, ServerError> {
        self.record_direct_state_downgrade(planned_family, source_kind, context_hash, ctx.seq_no.0, reason);
        self.record_route_outcome(
            ControllerRouteFamily::DirectState,
            Some(source_kind),
            context_hash,
            ctx.seq_no.0,
            true,
        );
        self.emit_plain_exact_state_data(
            ctx,
            item_id,
            block,
            true,
            planned_family == ControllerRouteFamily::ExactAtom,
            planned_family == ControllerRouteFamily::Transform,
            reason,
        )
    }

    /// S5.3: Primary write path for schema installation with full payload.
    /// Maintains the invariant that schemas Vec and schema_defs HashMap
    /// are always in sync.
    pub fn install_schema_def(&mut self, payload: SchemaDefPayload) {
        self.schemas
            .retain(|schema| schema.schema_id != payload.schema.schema_id);
        self.schemas.push(payload.schema.clone());
        self.schema_defs.insert(payload.schema.schema_id, payload);
    }

    fn dictionary_route_payload_for_bytes(
        &mut self,
        source_kind: shared_protocol::SourceKind,
        exact_bytes: &[u8],
    ) -> Option<PredictiveRouteDispatchPayload> {
        if !matches!(
            source_kind,
            shared_protocol::SourceKind::Text | shared_protocol::SourceKind::Json
        ) {
            return None;
        }
        let segments = structural_dictionary_segments(exact_bytes);
        if segments.len() < 4 {
            return None;
        }
        let mut counts = HashMap::<Vec<u8>, u32>::new();
        for segment in &segments {
            if segment.len() >= 3 && !segment.iter().all(|byte| byte.is_ascii_whitespace()) {
                *counts.entry(segment.clone()).or_insert(0) += 1;
            }
        }
        let mut entries = counts
            .into_iter()
            .filter(|(_, count)| *count > 1)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            right
                .1
                .cmp(&left.1)
                .then_with(|| right.0.len().cmp(&left.0.len()))
        });
        entries.truncate(16);
        if entries.len() < 2 {
            return None;
        }
        let dictionary_segments = entries
            .into_iter()
            .map(|(segment, _)| segment)
            .collect::<Vec<_>>();
        let dictionary_id = dictionary_id_for_segments(source_kind, &dictionary_segments);
        let token_map = dictionary_segments
            .iter()
            .enumerate()
            .map(|(index, segment)| (segment.clone(), index as u16))
            .collect::<HashMap<_, _>>();
        let mut components = Vec::new();
        let mut pending_tokens = Vec::new();
        for segment in &segments {
            if let Some(token_id) = token_map.get(segment) {
                pending_tokens.push(*token_id);
                continue;
            }
            if !pending_tokens.is_empty() {
                components.push(shared_protocol::HybridRouteComponent::DictionaryTokens {
                    dictionary_id,
                    token_ids: std::mem::take(&mut pending_tokens),
                });
            }
            components.push(shared_protocol::HybridRouteComponent::Literal(
                segment.clone(),
            ));
        }
        if !pending_tokens.is_empty() {
            components.push(shared_protocol::HybridRouteComponent::DictionaryTokens {
                dictionary_id,
                token_ids: pending_tokens,
            });
        }
        // S1.1.v3 CONFIRMED-ONLY: Only skip inline dictionary if CONFIRMED installed.
        // If only pending (emitted but not yet acked), we must carry the inline def
        // to ensure the peer can decode, because admissibility no longer reads
        // pending state — the dependency would fail the confirmed-only check
        // without the inline provision.
        let inline_dictionary = if self.peer_dictionary_versions.contains_key(&dictionary_id) {
            Vec::new()
        } else {
            let dictionary = SharedDictionary {
                dictionary_id,
                source_kind,
                version: shared_protocol::ObjectVersion::default(),
                entries: dictionary_segments
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, material)| DictionaryEntry {
                        token_id: index as u16,
                        material,
                        support_count: 1,
                    })
                    .collect(),
                keyed_permutation_seed: dictionary_id.0,
                max_token_len: dictionary_segments
                    .iter()
                    .map(|segment| segment.len())
                    .max()
                    .unwrap_or_default()
                    .min(u16::MAX as usize) as u16,
                lifecycle: shared_protocol::ObjectLifecycleMeta::default(),
            };
            // S1.5.c: Dictionary is inserted into the local store at planning time.
            // If the route is later rejected (e.g., wire cost exceeds literal), the
            // dictionary remains in local state. This is acceptable because local
            // dictionaries are not peer-visible and their existence does not affect
            // admissibility decisions. Only pending_dictionary_versions is peer-visible
            // and that is correctly deferred until route acceptance (S1.5.d).
            self.dictionaries.insert(dictionary_id, dictionary.clone());
            vec![SharedDictionaryDefPayload::new(dictionary)]
        };
        // S2.3.e: The dictionary dependency in closure is satisfied by the inline
        // dictionary definition carried in the same payload. The receiver will
        // install the inline def before attempting to decode the hybrid route,
        // so this is not a substrate reference to external state.
        let dependency_closure = vec![ObjectDependency {
            object_kind: ObjectKind::Dictionary,
            object_id: format!("dictionary:{}", dictionary_id.0),
            required_revision: 0,
        }];
        let estimated_route_bytes = components.iter().fold(0usize, |total, component| {
            total
                + match component {
                    shared_protocol::HybridRouteComponent::Literal(bytes) => bytes.len(),
                    shared_protocol::HybridRouteComponent::DictionaryTokens {
                        token_ids, ..
                    } => token_ids.len() * 2,
                    _ => 0,
                }
        }) + inline_dictionary
            .iter()
            .map(|payload| {
                payload
                    .dictionary
                    .entries
                    .iter()
                    .map(|entry| entry.material.len())
                    .sum::<usize>()
            })
            .sum::<usize>()
            + components.len() * 2;
        if estimated_route_bytes >= exact_bytes.len() {
            return None;
        }
        // S2.4: Reject degenerate hybrid routes where all components are literal.
        // A hybrid route with only Literal/ResidualPatch components provides no
        // actual substrate reuse — it ships full literal data under a misleading
        // "hybrid" label. Fall through to direct-state instead.
        let has_reuse_component = components.iter().any(|component| {
            matches!(
                component,
                shared_protocol::HybridRouteComponent::Substrate(_)
                    | shared_protocol::HybridRouteComponent::SubstrateGraph(_)
                    | shared_protocol::HybridRouteComponent::Assembly(_)
                    | shared_protocol::HybridRouteComponent::Transform(_)
                    | shared_protocol::HybridRouteComponent::DictionaryTokens { .. }
                    | shared_protocol::HybridRouteComponent::Schema(_)
            )
        });
        if !has_reuse_component {
            return None;
        }
        let route = shared_protocol::HybridRoute {
            route_family: ControllerRouteFamily::Hybrid,
            precision_band: shared_protocol::PrecisionBand::Exact,
            assembly_mode: None,
            output_len: exact_bytes.len().min(u32::MAX as usize) as u32,
            dependency_closure: dependency_closure.clone(),
            components,
        };
        // Note: pending_dictionary_versions insert is NOT done here at planning time.
        // The sole writer is emit_predictive_route_record, which inserts pending
        // state only after the record is actually emitted. This prevents phantom
        // pending state if the route is planned but emission fails, and avoids
        // version mismatches between planning-time and emission-time inserts.
        // The route-acceptance semantic from S1.5 is preserved because
        // emit_predictive_route_record is only called for accepted routes.
        Some(
            PredictiveRouteDispatchPayload {
                version: PredictiveRouteDispatchPayload::VERSION,
                route_family: ControllerRouteFamily::Hybrid,
                route_kind: ControllerRouteFamily::Hybrid.route_family(),
                route_source_kind: Some(source_kind),
                assembly_mode: None,
                precision_band: shared_protocol::PrecisionBand::Exact,
                dependency_closure: dependency_closure.clone(),
                sync_risk: 0,
                literal_bytes: Vec::new(),
                assembly_ref: None,
                inline_assembly_defs: Vec::new(),
                inline_schema_defs: Vec::new(),
                inline_dictionaries: inline_dictionary,
                inline_episode_hints: Vec::new(),
                route_graph: derived_dispatch_route_graph(
                    ControllerRouteFamily::Hybrid,
                    ControllerRouteFamily::Hybrid.route_family(),
                    &dependency_closure,
                    0,
                    &[],
                    &[],
                    None,
                    None,
                    Some(&route),
                ),
                contradiction_bytes: Vec::new(),
                prg: None,
                hybrid_route: Some(route),
            }
            .with_derived_route_graph(),
        )
    }

    pub fn enqueue_schema_promotion(&mut self, entry: SchemaPromotionQueueEntry) {
        self.schema_promotion_queue.push(entry);
    }

    pub fn promote_ready_schemas(
        &mut self,
        min_support: u32,
        min_topology_stability: u32,
    ) -> Vec<SchemaPromotionQueueEntry> {
        self.schema_promotion_queue
            .promote_ready(min_support, min_topology_stability)
    }

    pub fn schema_route_candidates(
        &self,
        cue: shared_protocol::SparseCue,
        source_kind: shared_protocol::SourceKind,
        context_hash: Option<ContextHash>,
        policy: SchemaCandidatePolicy,
    ) -> Vec<SchemaRouteCandidate> {
        self.filter_schema_candidates(generate_schema_route_candidates(
            &self.schemas,
            cue,
            source_kind,
            context_hash,
            policy,
        ))
    }

    pub fn enqueue_consolidation_job(&mut self, job: ConsolidationJob) {
        self.consolidation_queue.push(job);
    }

    pub fn drain_ready_consolidation_jobs(
        &mut self,
        min_support: u32,
        max_ambiguity: u32,
    ) -> Vec<ConsolidationJob> {
        self.consolidation_queue
            .drain_ready(min_support, max_ambiguity)
    }

    pub fn schedule_consolidation_from_replay(&mut self) {
        let tick = self
            .replay_queue
            .pending
            .last()
            .map(|entry| entry.event.tick)
            .unwrap_or_default();
        let _ = self.run_learning_cycle(tick);
    }

    pub fn apply_ontology_operation(&mut self, op: OntologyOperation) {
        self.ontology_state.apply(op);
    }

    pub fn assign_dynamic_family(&mut self, assignment: DynamicFamilyAssignment) {
        self.ontology_state
            .families
            .insert(assignment.family_id, assignment);
    }

    pub fn ontology_state(&self) -> &OntologyState {
        &self.ontology_state
    }

    fn family_id_for_plane(
        plane: MemoryPlane,
        source_kind: Option<shared_protocol::SourceKind>,
        object_id: &str,
    ) -> DynamicFamilyId {
        let mut hash = (plane as u32).wrapping_mul(1_048_573);
        if let Some(source_kind) = source_kind {
            hash ^= (source_kind as u32).wrapping_mul(65_537);
        }
        for byte in object_id.as_bytes().iter().copied().take(24) {
            hash = hash.rotate_left(5) ^ byte as u32;
        }
        DynamicFamilyId(hash.max(1))
    }

    fn register_learning_family(
        &mut self,
        plane: MemoryPlane,
        source_kind: Option<shared_protocol::SourceKind>,
        object_kind: shared_protocol::ObjectKind,
        object_id: &str,
        lifecycle: shared_protocol::ObjectLifecycleMeta,
    ) -> (DynamicFamilyId, Option<DynamicSubfamilyId>) {
        let family_id = Self::family_id_for_plane(plane, source_kind, object_id);
        let subfamily_id = Some(DynamicSubfamilyId((object_kind as u32).max(1)));
        let assignment = self
            .ontology_state
            .families
            .entry(family_id)
            .or_insert_with(|| DynamicFamilyAssignment {
                family_id,
                subfamily_id,
                label: format!("{:?}-{:?}", plane, object_kind),
                source_kind,
                plane,
                object_kinds: vec![object_kind],
                lifecycle,
            });
        assignment.lifecycle = lifecycle
            .assign_dynamic_family(family_id, subfamily_id)
            .with_promotion_level(lifecycle.promotion_level);
        assignment.source_kind = source_kind.or(assignment.source_kind);
        if !assignment.object_kinds.contains(&object_kind) {
            assignment.object_kinds.push(object_kind);
        }
        (family_id, subfamily_id)
    }

    fn persist_assembly(&mut self, assembly: Assembly) -> Result<(), ServerError> {
        if let Some(cache) = &self.assembly_cache {
            cache.store_assembly(&assembly)?;
        }
        self.assemblies.insert(assembly.assembly_id, assembly);
        Ok(())
    }

    fn persist_transform_class(&mut self, class: TransformClass) -> Result<(), ServerError> {
        if let Some(cache) = &self.assembly_cache {
            cache.store_transform_class(&class)?;
        }
        Ok(())
    }

    fn schema_payload_graph(payload: &SchemaDefPayload) -> shared_protocol::PredictiveReconstructionGraph {
        let mut nodes = payload.node_table.clone();
        for (from, to) in &payload.edge_table {
            if let Some(node) = nodes.iter_mut().find(|node| node.node_id == *from) {
                if !node.child_ids.contains(to) {
                    node.child_ids.push(*to);
                }
            }
        }
        let child_node_ids = payload
            .edge_table
            .iter()
            .map(|(_, to)| *to)
            .collect::<BTreeSet<_>>();
        let root_node_id = nodes
            .iter()
            .find(|node| !child_node_ids.contains(&node.node_id))
            .map(|node| node.node_id)
            .unwrap_or_else(|| nodes.first().map(|node| node.node_id).unwrap_or_default());
        shared_protocol::PredictiveReconstructionGraph::new(
            root_node_id,
            payload.decode_max_depth,
            payload.schema.output_len,
            nodes,
            payload
                .dependency_closure
                .dependencies
                .iter()
                .map(|dependency| dependency.object_id.clone())
                .collect(),
        )
    }

    fn collect_schema_dependencies_recursive(
        &self,
        schema_id: SchemaId,
        dependencies: &mut Vec<ObjectDependency>,
        inline_schema_defs: &mut Vec<SchemaDefPayload>,
        seen: &mut BTreeSet<SchemaId>,
    ) {
        if !seen.insert(schema_id) {
            return;
        }
        let Some(payload) = self.schema_defs.get(&schema_id).cloned() else {
            return;
        };
        dependencies.push(dependency_from_schema_id(schema_id));
        dependencies.extend(payload.dependency_closure.dependencies.clone());
        inline_schema_defs.push(payload.clone());
        for node in &payload.node_table {
            if let Some(nested_schema_id) = node.schema_ref {
                self.collect_schema_dependencies_recursive(
                    nested_schema_id,
                    dependencies,
                    inline_schema_defs,
                    seen,
                );
            }
        }
    }

    fn collect_prg_dependencies_recursive(
        &self,
        graph: &shared_protocol::PredictiveReconstructionGraph,
    ) -> (Vec<ObjectDependency>, Vec<SchemaDefPayload>) {
        let mut dependencies = collect_prg_dependencies(graph);
        let mut inline_schema_defs = Vec::new();
        let mut seen = BTreeSet::new();
        for node in &graph.nodes {
            if let Some(schema_id) = node.schema_ref {
                self.collect_schema_dependencies_recursive(
                    schema_id,
                    &mut dependencies,
                    &mut inline_schema_defs,
                    &mut seen,
                );
            }
        }
        (
            normalize_dependencies(dependencies),
            dedup_schema_payloads(inline_schema_defs),
        )
    }

    fn collect_hybrid_route_dependencies_recursive(
        &self,
        route: &shared_protocol::HybridRoute,
    ) -> (Vec<ObjectDependency>, Vec<SchemaDefPayload>) {
        let mut dependencies = collect_hybrid_route_dependencies(route);
        let mut inline_schema_defs = Vec::new();
        let mut seen = BTreeSet::new();
        for component in &route.components {
            if let shared_protocol::HybridRouteComponent::Schema(graph) = component {
                let (graph_dependencies, graph_schema_defs) =
                    self.collect_prg_dependencies_recursive(graph);
                dependencies.extend(graph_dependencies);
                for payload in graph_schema_defs {
                    if seen.insert(payload.schema.schema_id) {
                        inline_schema_defs.push(payload);
                    }
                }
            }
        }
        (
            normalize_dependencies(dependencies),
            dedup_schema_payloads(inline_schema_defs),
        )
    }

    /// S5.3: Update a schema graph and its corresponding payload entry.
    /// Both schemas Vec and schema_defs HashMap are updated together,
    /// maintaining the invariant that they are always in sync.
    fn update_schema(&mut self, schema: SchemaGraph) {
        self.schemas
            .retain(|existing| existing.schema_id != schema.schema_id);
        // S5.3: Always update schema_defs, not just when the key exists.
        // This fixes the previous inconsistency where a schema could be in
        // the Vec but not in schema_defs (or vice versa).
        let graph = shared_protocol::reconstruct_graph_from_schema(&schema);
        self.schema_defs
            .insert(schema.schema_id, SchemaDefPayload::new(schema.clone(), &graph));
        self.schemas.push(schema);
    }

    fn promote_level(level: PromotionLevel) -> PromotionLevel {
        match level {
            PromotionLevel::Cold => PromotionLevel::Warm,
            PromotionLevel::Warm => PromotionLevel::Stable,
            PromotionLevel::Stable => PromotionLevel::Promoted,
            PromotionLevel::Promoted => PromotionLevel::Canonical,
            PromotionLevel::Canonical => PromotionLevel::Canonical,
        }
    }

    fn maybe_enqueue_assembly_promotion(
        &mut self,
        assembly: &Assembly,
        estimated_wire_savings: u32,
        ambiguity_score: u32,
    ) {
        self.enqueue_assembly_promotion(AssemblyPromotionQueueEntry {
            assembly_id: assembly.assembly_id,
            source_kind: assembly.source_kind,
            ambiguity_score,
            estimated_wire_savings,
            support_count: assembly.lifecycle.support_count,
            lifecycle: assembly.lifecycle,
            cue: assembly.cue,
        });
    }

    fn maybe_enqueue_transform_promotion(&mut self, class: &TransformClass) {
        self.enqueue_transform_promotion(TransformPromotionQueueEntry {
            transform_id: class.transform_id,
            source_kind: class.source_kind,
            estimated_wire_savings: class.reuse_savings,
            stability_score: class.stability_score,
            failure_score: class.failure_score,
            lifecycle: class.lifecycle,
            cue: class.cue,
        });
    }

    fn maybe_enqueue_schema_promotion(&mut self, schema: &SchemaGraph) {
        self.enqueue_schema_promotion(SchemaPromotionQueueEntry {
            schema_id: schema.schema_id,
            source_kind: schema.source_kind,
            cross_episode_support: schema
                .cross_episode_support
                .max(schema.lifecycle.support_count),
            topology_stability: schema.edges.len() as u32 + schema.nodes.len() as u32,
            contradiction_burden: schema.contradiction_burden,
            branch_consistency: schema.branch_consistency.max(schema.edges.len() as u32),
            decode_burden: schema
                .mean_decode_burden
                .max(schema.decode_max_depth as u32),
            retire_after_failures: schema.retire_after_failures.max(4),
            lifecycle: schema.lifecycle,
            cue: schema.cue,
        });
    }

    fn ingest_replay_into_learning_loop(&mut self, tick: u64) -> Result<(), ServerError> {
        let replay_entries = std::mem::take(&mut self.replay_queue.pending);
        for entry in replay_entries {
            let plane = shared_protocol::plane_for_object_kind(entry.event.object_ref.object_kind);
            let kind = match entry.event.object_ref.object_kind {
                shared_protocol::ObjectKind::Assembly => {
                    if let Some(id) = entry
                        .event
                        .object_ref
                        .object_id
                        .strip_prefix("assembly:")
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .map(AssemblyId)
                    {
                        if let Some(assembly) = self.assemblies.get(&id).cloned() {
                            self.maybe_enqueue_assembly_promotion(
                                &assembly,
                                entry.reuse_potential.max(1),
                                entry.ambiguity,
                            );
                        }
                    }
                    if entry.ambiguity > 4 {
                        ConsolidationJobKind::AssemblySplit
                    } else {
                        ConsolidationJobKind::AssemblyMerge
                    }
                }
                shared_protocol::ObjectKind::Transform => {
                    if let Some(id) = entry
                        .event
                        .object_ref
                        .object_id
                        .strip_prefix("transform:")
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .map(shared_protocol::TransformId)
                    {
                        if let Some(class) = self
                            .assembly_cache
                            .as_ref()
                            .cloned()
                            .map(|cache| cache.lookup_transform_class(id))
                            .transpose()?
                            .flatten()
                        {
                            self.maybe_enqueue_transform_promotion(&class);
                        }
                    }
                    ConsolidationJobKind::TransformPromotion
                }
                shared_protocol::ObjectKind::Schema => {
                    if let Some(id) = entry
                        .event
                        .object_ref
                        .object_id
                        .strip_prefix("schema:")
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .map(SchemaId)
                    {
                        if let Some(schema) = self
                            .schemas
                            .iter()
                            .find(|schema| schema.schema_id == id)
                            .cloned()
                        {
                            self.maybe_enqueue_schema_promotion(&schema);
                        }
                    }
                    ConsolidationJobKind::SchemaPromotion
                }
                _ => {
                    if entry.reuse_potential > 0 {
                        ConsolidationJobKind::EpisodeMacroExtraction
                    } else {
                        ConsolidationJobKind::ObjectRetirement
                    }
                }
            };
            self.enqueue_consolidation_job(ConsolidationJob {
                job_id: tick ^ ((self.consolidation_queue.pending.len() as u64) + 1),
                kind,
                plane,
                source_kind: Some(entry.event.source_kind),
                primary_object_id: entry.event.object_ref.object_id.clone(),
                related_object_ids: Vec::new(),
                lifecycle: shared_protocol::ObjectLifecycleMeta::default()
                    .record_seen(tick, true)
                    .record_consolidated(tick),
                support: entry
                    .reuse_potential
                    .max(entry.event.cue.overlap_score(entry.event.cue)),
                ambiguity: entry.ambiguity,
                savings: entry.reuse_potential.max(entry.surprise),
            });
        }
        Ok(())
    }

    fn drain_learning_queues(&mut self, tick: u64) -> Result<(), ServerError> {
        for entry in self.promote_ready_assemblies(8, 1) {
            if let Some(mut assembly) = self.assemblies.get(&entry.assembly_id).cloned() {
                let lifecycle = assembly
                    .lifecycle
                    .record_seen(tick, true)
                    .with_promotion_level(Self::promote_level(assembly.lifecycle.promotion_level));
                let (family_id, subfamily_id) = self.register_learning_family(
                    MemoryPlane::Assembly,
                    Some(assembly.source_kind),
                    shared_protocol::ObjectKind::Assembly,
                    &format!("assembly:{}", assembly.assembly_id.0),
                    lifecycle,
                );
                assembly.lifecycle = lifecycle.assign_dynamic_family(family_id, subfamily_id);
                self.apply_ontology_operation(OntologyOperation {
                    kind: OntologyOperationKind::Promote,
                    plane: MemoryPlane::Assembly,
                    target_family: family_id,
                    related_families: Vec::new(),
                    relabel_to: None,
                    promote_to: Some(assembly.lifecycle.promotion_level),
                    retire: false,
                });
                self.persist_assembly(assembly)?;
            }
        }
        for entry in self.promote_ready_schemas(1, 1) {
            if let Some(mut schema) = self
                .schemas
                .iter()
                .find(|schema| schema.schema_id == entry.schema_id)
                .cloned()
            {
                let lifecycle = schema
                    .lifecycle
                    .record_seen(tick, true)
                    .with_promotion_level(Self::promote_level(schema.lifecycle.promotion_level));
                let (family_id, subfamily_id) = self.register_learning_family(
                    MemoryPlane::Schema,
                    Some(schema.source_kind),
                    shared_protocol::ObjectKind::Schema,
                    &format!("schema:{}", schema.schema_id.0),
                    lifecycle,
                );
                schema.lifecycle = lifecycle.assign_dynamic_family(family_id, subfamily_id);
                self.apply_ontology_operation(OntologyOperation {
                    kind: OntologyOperationKind::Promote,
                    plane: MemoryPlane::Schema,
                    target_family: family_id,
                    related_families: Vec::new(),
                    relabel_to: None,
                    promote_to: Some(schema.lifecycle.promotion_level),
                    retire: false,
                });
                self.update_schema(schema);
            }
        }
        for job in self.drain_ready_consolidation_jobs(1, 16) {
            match job.kind {
                ConsolidationJobKind::AssemblyMerge | ConsolidationJobKind::AssemblySplit => {
                    if let Some(id) = job
                        .primary_object_id
                        .strip_prefix("assembly:")
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .map(AssemblyId)
                    {
                        if let Some(mut assembly) = self.assemblies.get(&id).cloned() {
                            let lifecycle = assembly.lifecycle.record_consolidated(tick);
                            let (family_id, subfamily_id) = self.register_learning_family(
                                job.plane,
                                job.source_kind,
                                shared_protocol::ObjectKind::Assembly,
                                &job.primary_object_id,
                                lifecycle,
                            );
                            assembly.lifecycle =
                                lifecycle.assign_dynamic_family(family_id, subfamily_id);
                            if matches!(job.kind, ConsolidationJobKind::AssemblyMerge) {
                                self.apply_ontology_operation(OntologyOperation {
                                    kind: OntologyOperationKind::Relabel,
                                    plane: MemoryPlane::Assembly,
                                    target_family: family_id,
                                    related_families: Vec::new(),
                                    relabel_to: Some("assembly_consolidated".to_string()),
                                    promote_to: None,
                                    retire: false,
                                });
                            }
                            self.persist_assembly(assembly)?;
                        }
                    }
                }
                ConsolidationJobKind::TransformPromotion => {
                    if let Some(id) = job
                        .primary_object_id
                        .strip_prefix("transform:")
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .map(shared_protocol::TransformId)
                    {
                        if let Some(mut class) = self
                            .assembly_cache
                            .as_ref()
                            .cloned()
                            .map(|cache| cache.lookup_transform_class(id))
                            .transpose()?
                            .flatten()
                        {
                            let lifecycle = class
                                .lifecycle
                                .record_consolidated(tick)
                                .with_promotion_level(Self::promote_level(
                                    class.lifecycle.promotion_level,
                                ));
                            let (family_id, subfamily_id) = self.register_learning_family(
                                job.plane,
                                job.source_kind,
                                shared_protocol::ObjectKind::Transform,
                                &job.primary_object_id,
                                lifecycle,
                            );
                            class.lifecycle =
                                lifecycle.assign_dynamic_family(family_id, subfamily_id);
                            class.reuse_savings = class.reuse_savings.saturating_add(job.savings);
                            self.persist_transform_class(class)?;
                        }
                    }
                }
                ConsolidationJobKind::SchemaPromotion
                | ConsolidationJobKind::EpisodeMacroExtraction => {
                    if let Some(id) = job
                        .primary_object_id
                        .strip_prefix("schema:")
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .map(SchemaId)
                    {
                        if let Some(mut schema) = self
                            .schemas
                            .iter()
                            .find(|schema| schema.schema_id == id)
                            .cloned()
                        {
                            let lifecycle = schema
                                .lifecycle
                                .record_consolidated(tick)
                                .with_promotion_level(Self::promote_level(
                                    schema.lifecycle.promotion_level,
                                ));
                            let (family_id, subfamily_id) = self.register_learning_family(
                                MemoryPlane::Schema,
                                job.source_kind,
                                shared_protocol::ObjectKind::Schema,
                                &job.primary_object_id,
                                lifecycle,
                            );
                            schema.lifecycle =
                                lifecycle.assign_dynamic_family(family_id, subfamily_id);
                            self.update_schema(schema);
                        }
                    }
                }
                ConsolidationJobKind::ObjectRetirement => {
                    let family_id = Self::family_id_for_plane(
                        job.plane,
                        job.source_kind,
                        &job.primary_object_id,
                    );
                    self.apply_ontology_operation(OntologyOperation {
                        kind: OntologyOperationKind::Retire,
                        plane: job.plane,
                        target_family: family_id,
                        related_families: Vec::new(),
                        relabel_to: None,
                        promote_to: None,
                        retire: true,
                    });
                    // Cap pending memory retires to bound memory.
                    if self.pending_memory_retires.len() < MAX_PENDING_MEMORY_RETIRES {
                        self.pending_memory_retires.push(MemoryRetirePayload {
                            version: MemoryRetirePayload::VERSION,
                            plane: job.plane,
                            object_kind: control_plane_object_kind(job.plane, &job.primary_object_id),
                            object_id: job.primary_object_id.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn run_learning_cycle(&mut self, tick: u64) -> Result<(), ServerError> {
        self.ingest_replay_into_learning_loop(tick)?;
        self.drain_learning_queues(tick)
    }

    pub fn emit_memory_ack_record(
        &self,
        payload: MemoryAckPayload,
    ) -> Result<Record, ValidationError> {
        let payload_bytes = payload
            .encode()
            .map_err(|_| ValidationError::InvalidPredictiveHeader)?;
        Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(0),
                epoch_id: EpochId(0),
                seq_no: SeqNo(0),
                record_type: RecordType::MemoryAck,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: ItemId(1),
                payload_len: payload_bytes.len() as u32,
            },
            payload: payload_bytes,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
    }

    pub fn apply_memory_ack_record(&mut self, record: Record) -> Result<(), ServerError> {
        let payload =
            MemoryAckPayload::decode(&record.payload).map_err(ServerError::ValidationPayload)?;
        // S1.2: Upon MemoryAck receipt, promote pending peer-state to confirmed.
        self.promote_pending_to_confirmed(&payload);
        // Cap received acks to bound memory; drop oldest if over capacity.
        if self.received_memory_acks.len() >= MAX_RECEIVED_MEMORY_ACKS {
            self.received_memory_acks.remove(0);
        }
        self.received_memory_acks.push(payload);
        Ok(())
    }

    /// Promote a pending peer-state entry to confirmed upon receipt of
    /// a valid MemoryAck from the client. This is the sole pathway
    /// from pending to confirmed for all object families.
    fn promote_pending_to_confirmed(&mut self, ack: &MemoryAckPayload) {
        match ack.object_kind {
            ObjectKind::Assembly => {
                debug_assert!(ack.object_id.starts_with("assembly:"), "S1.6.c: Assembly ack with non-canonical ID: {}", ack.object_id);
                if let Some(id) = ack.object_id.strip_prefix("assembly:") {
                    if let Ok(raw) = id.parse::<u64>() {
                        let assembly_id = AssemblyId(raw);
                        if let Some(sig) = self.pending_assembly_sync_versions.remove(&assembly_id) {
                            self.assembly_sync_versions.insert(assembly_id, sig);
                        }
                    }
                }
            }
            ObjectKind::Schema => {
                debug_assert!(ack.object_id.starts_with("schema:"), "S1.6.c: Schema ack with non-canonical ID: {}", ack.object_id);
                if let Some(id) = ack.object_id.strip_prefix("schema:") {
                    if let Ok(raw) = id.parse::<u64>() {
                        let schema_id = SchemaId(raw);
                        if let Some(revision) = self.pending_schema_versions.remove(&schema_id) {
                            self.peer_schema_versions.insert(schema_id, revision);
                        }
                    }
                }
            }
            ObjectKind::Dictionary => {
                debug_assert!(ack.object_id.starts_with("dictionary:"), "S1.6.c: Dictionary ack with non-canonical ID: {}", ack.object_id);
                if let Some(id) = ack.object_id.strip_prefix("dictionary:") {
                    if let Ok(raw) = id.parse::<u64>() {
                        let dict_id = DictionaryId(raw);
                        if let Some(version) = self.pending_dictionary_versions.remove(&dict_id) {
                            self.peer_dictionary_versions.insert(dict_id, version);
                        }
                    }
                }
            }
            ObjectKind::Transform => {
                debug_assert!(ack.object_id.starts_with("transform:"), "S1.6.c: Transform ack with non-canonical ID: {}", ack.object_id);
                if let Some(id) = ack.object_id.strip_prefix("transform:") {
                    if let Ok(raw) = id.parse::<u64>() {
                        let transform_id = TransformId(raw);
                        if let Some(revision) = self.pending_transform_versions.remove(&transform_id) {
                            self.peer_transform_versions.insert(transform_id, revision);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// S1.2.f: Failure policy for abandoned pending entries.
    /// On resync/reset/disconnect, all pending peer-state entries are discarded
    /// because the peer will also have reset its state. These pending entries
    /// represent optimistic "emitted but unconfirmed" state that can never be
    /// confirmed after a session reset. They must not leak into confirmed state
    /// or be used for subsequent admissibility decisions.
    /// Confirmed state is also cleared on full resync since the peer has reset.
    // S1.6.d: Clears "optimistically emitted but unconfirmed" state.
    // After clearing, predictive_dependency_is_admissible will return false
    // for any object that was only in pending state, correctly reflecting
    // that we can no longer assume the peer has it.
    fn clear_pending_peer_state(&mut self) {
        self.pending_assembly_sync_versions.clear();
        self.pending_schema_versions.clear();
        self.pending_dictionary_versions.clear();
        self.pending_transform_versions.clear();
    }

    fn emit_pending_control_plane_records(
        &mut self,
        ctx: HeaderContext,
        max_records: usize,
    ) -> Result<Vec<Record>, ServerError> {
        let mut out = Vec::new();
        if max_records == 0 {
            return Ok(out);
        }

        while out.len() < max_records {
            if let Some((item_id, entry)) = if self.pending_replay_hints.is_empty() {
                None
            } else {
                Some(self.pending_replay_hints.remove(0))
            } {
                let candidate = EpisodeCompletionCandidate {
                    object_ref: entry.event.object_ref.clone(),
                    transition_count: shared_protocol::TransitionCount(
                        entry.reuse_potential.max(1),
                    ),
                    branch_rank: shared_protocol::BranchRank(0),
                    precision_band: shared_protocol::PrecisionBand::Balanced,
                    cue_overlap: entry.event.cue.overlap_score(entry.event.cue),
                    recency_score: 1,
                    route_support: entry.reuse_potential.max(1),
                    transition_match: entry.reuse_potential.max(1),
                    lag_bucket: LagBucket(0),
                    // I14: Check actual peer visibility instead of hardcoding true.
                    // The referenced object must be in confirmed peer state.
                    admissible: self.predictive_object_ref_is_admissible(&entry.event.object_ref),
                };
                let payload = shared_protocol::EpisodeHintPayload::new(
                    entry.event.context_hash,
                    candidate.lag_bucket,
                    candidate.precision_band,
                    episode_hint_dependencies(std::slice::from_ref(&candidate)),
                    vec![candidate],
                );
                let seq_no = SeqNo(ctx.seq_no.0 + out.len() as u64);
                out.push(self.emit_replay_hint_record(
                    HeaderContext { seq_no, ..ctx },
                    item_id,
                    payload,
                )?);
                continue;
            }

            if let Some(payload) = if self.pending_memory_retires.is_empty() {
                None
            } else {
                Some(self.pending_memory_retires.remove(0))
            } {
                let payload_bytes = payload.encode().map_err(ServerError::ValidationPayload)?;
                let seq_no = SeqNo(ctx.seq_no.0 + out.len() as u64);
                let record = Record {
                    header: RecordHeader {
                        version: shared_protocol::PROTOCOL_VERSION,
                        stream_id: ctx.stream_id,
                        epoch_id: ctx.epoch_id,
                        seq_no,
                        record_type: RecordType::MemoryRetire,
                        codec_mode: CodecMode::None,
                        flags: ctx.flags(RecordFlags::empty()),
                        item_id: ItemId(1),
                        payload_len: payload_bytes.len() as u32,
                    },
                    payload: payload_bytes,
                    auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
                }
                .validate()
                .map_err(ServerError::Validation)?;
                out.push(record);
                continue;
            }

            break;
        }

        Ok(out)
    }

    pub fn source_binding(&self, item_id: ItemId) -> Option<&SourceDescriptor> {
        self.source_bindings.get(&item_id)
    }

    pub fn local_catalog(&self) -> &SharedBlockCatalog {
        &self.local_catalog
    }

    pub fn peer_catalog(&self) -> &SharedBlockCatalog {
        &self.peer_catalog
    }

    pub fn catalog_block_id_for_item(&self, item_id: ItemId) -> Option<BlockId> {
        self.entries
            .get(&item_id)
            .and_then(|entry| {
                let block = transient_exact_material(entry.object.source_kind, entry.exact_bytes());
                shared_protocol::BlockCatalogEntry::from_bytes(
                    block.source_kind,
                    block.exact_bytes.clone(),
                )
                .ok()
            })
            .map(|entry| entry.block_id)
    }

    pub fn define_catalog_bundle(
        &mut self,
        members: Vec<BundleMember>,
    ) -> Result<BundleId, shared_protocol::CatalogError> {
        self.local_catalog.define_bundle(members)
    }

    pub fn entries_iter(&self) -> impl Iterator<Item = (ItemId, ExactStateMaterial)> + '_ {
        self.entries.iter().map(|(item_id, entry)| {
            (
                *item_id,
                transient_exact_material(entry.object.source_kind, entry.exact_bytes()),
            )
        })
    }

    fn source_binding_changed(&self, item_id: ItemId, descriptor: &SourceDescriptor) -> bool {
        self.source_bindings.get(&item_id) != Some(descriptor)
    }

    fn chpmt_object_for_item(
        &self,
        item_id: ItemId,
        exact_bytes: &[u8],
        object_kind: shared_protocol::ObjectKind,
    ) -> ChpmtObject {
        let descriptor = self
            .source_bindings
            .get(&item_id)
            .cloned()
            .unwrap_or(SourceDescriptor {
                kind: shared_protocol::SourceKind::Binary,
                source_hash: shared_protocol::compute_source_hash(
                    shared_protocol::SourceKind::Binary,
                    None,
                    exact_bytes,
                ),
                byte_len: exact_bytes.len(),
                mime: None,
                label: None,
            });
        let cue = descriptor.structural_cue_summary(exact_bytes).cue;
        let object_key = descriptor.runtime_object_key_from_bytes(exact_bytes);
        ChpmtObject::from_exact_bytes(
            descriptor.clone(),
            object_key,
            descriptor.kind,
            object_kind,
            cue,
            exact_bytes.to_vec(),
        )
    }

    fn store_runtime_object_for_item(
        &mut self,
        item_id: ItemId,
        exact_bytes: &[u8],
        object_kind: shared_protocol::ObjectKind,
    ) -> Result<(), ServerError> {
        let entry = ServerEntry {
            object: self.chpmt_object_for_item(item_id, exact_bytes, object_kind),
        };
        let catalog_block = transient_exact_material(entry.object.source_kind, entry.exact_bytes());
        self.local_catalog
            .insert_exact_material(&catalog_block)
            .map_err(ServerError::Catalog)?;
        self.maybe_evict_entries_for_insert(item_id);
        self.entries.insert(item_id, entry.clone());
        self.predictors.insert(item_id, entry);
        Ok(())
    }

    /// Evicts the oldest entry from `entries`, `predictors`, and
    /// `source_bindings` when the maps exceed `MAX_SERVER_ENTRIES`.
    /// Uses a simple FIFO strategy: the first key found by iteration is removed.
    /// This bounds the in-memory working set to prevent the "10 MB memory cliff"
    /// where RSS grows to 657+ MB under sustained insert load.
    fn maybe_evict_entries_for_insert(&mut self, incoming_id: ItemId) {
        if self.entries.len() >= MAX_SERVER_ENTRIES {
            // Find the oldest entry to evict (arbitrary but deterministic).
            // Prefer evicting an entry that is NOT the one being inserted.
            let evict_id = self
                .entries
                .keys()
                .find(|&&id| id != incoming_id)
                .copied();
            if let Some(id) = evict_id {
                self.entries.remove(&id);
                self.predictors.remove(&id);
                self.source_bindings.remove(&id);
            }
        }
    }

    fn emit_plain_event(
        &mut self,
        ctx: HeaderContext,
        event: ServerEvent,
        optimizations: SourceOptimizationConfig,
    ) -> Result<Record, ServerError> {
        match event {
            ServerEvent::Insert { item_id, block }
            | ServerEvent::UpsertObject { item_id, block } => {
                self.emit_plain_runtime_data(ctx, item_id, block, optimizations)
            }
            // S5.1: StateDef/StatePatch match arms removed — variants removed from ServerEvent.
            ServerEvent::Evict { item_id } => {
                self.emit_plain_control_data(ctx, item_id, CacheOp::Evict)
            }
            ServerEvent::Invalidate { item_id } => {
                self.emit_plain_control_data(ctx, item_id, CacheOp::Invalidate)
            }
        }
    }

    fn emit_plain_runtime_data(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        block: ExactStateMaterial,
        _optimizations: SourceOptimizationConfig,
    ) -> Result<Record, ServerError> {
        let source_kind = self
            .source_bindings
            .get(&item_id)
            .map(|descriptor| descriptor.kind)
            .unwrap_or(shared_protocol::SourceKind::Binary);
        let runtime_material = transient_exact_material(source_kind, &block.exact_bytes);
        let cue = self
            .source_bindings
            .get(&item_id)
            .map(|descriptor| {
                descriptor
                    .structural_cue_summary(&runtime_material.exact_bytes)
                    .cue
            })
            .unwrap_or_default();
        let context_hash = Some(derive_context_hash(
            cue,
            &EpisodeObjectRef {
                object_kind: shared_protocol::ObjectKind::ExactState,
                object_id: format!("item:{}", item_id.0),
            },
        ));
        if self
            .predictors
            .get(&item_id)
            .map(|entry| entry.exact_bytes() == runtime_material.exact_bytes.as_slice())
            .unwrap_or(false)
        {
            self.record_route_outcome(
                ControllerRouteFamily::DirectState,
                Some(source_kind),
                context_hash,
                ctx.seq_no.0,
                true,
            );
            return self.emit_plain_exact_state_data(ctx, item_id, block, false, false, false, FallbackReason::NotAFallback);
        }
        let has_predictive_context = self.predictors.contains_key(&item_id)
            || self.source_bindings.contains_key(&item_id)
            || self.peer_catalog.blocks_iter().next().is_some()
            || self.peer_catalog.bundles_iter().next().is_some();
        if !has_predictive_context {
            self.record_route_outcome(
                ControllerRouteFamily::DirectState,
                Some(source_kind),
                context_hash,
                ctx.seq_no.0,
                true,
            );
            return self.emit_plain_exact_state_data(ctx, item_id, block, false, false, false, FallbackReason::NotAFallback);
        }
        let assembly_candidates = extract_catalog_assembly_candidates(
            source_kind,
            &runtime_material.exact_bytes,
            shared_protocol::AssemblyExtractionConfig::default(),
        );
        let transform_candidates = generate_transform_candidates(
            &runtime_material,
            &self.peer_catalog,
        );
        let episode_candidates = self.filter_episode_candidates(generate_episode_route_candidates(
            &self.episode_memory,
            EpisodeCandidatePolicy::bounded_default(),
        ));
        let schema_candidates = self.filter_schema_candidates(generate_schema_route_candidates(
            &self.schemas,
            cue,
            source_kind,
            context_hash,
            SchemaCandidatePolicy::bounded_default(),
        ));
        let route_feedback: Vec<RouteStatistics> = self.route_statistics().cloned().collect();
        let selection = RouteSelectionContext {
            cue,
            source_kind,
            context_hash,
            tick: ctx.seq_no.0,
            governor: self.context_governor.clone(),
            prior_schema_kind: schema_candidates
                .first()
                .map(|candidate| candidate.schema_kind),
            prior_transform_family: None,
            lag_bucket: episode_candidates
                .first()
                .map(|candidate| candidate.lag_bucket)
                .unwrap_or(LagBucket::default()),
        };
        let chosen_route = choose_route_by_family(
            &runtime_material,
            &self.peer_catalog,
            &assembly_candidates,
            &transform_candidates,
            &episode_candidates,
            &schema_candidates,
            &route_feedback,
            selection.clone(),
            RouteAdmissibilityPolicy::bounded_default(),
        );
        let chosen_symbol = route_context_symbol_for_plan(&chosen_route, &selection);
        return match chosen_route {
            shared_protocol::ControllerRoutePlan::Transform {
                class, instance, ..
            } => {
                self.store_runtime_object_for_item(
                    item_id,
                    &runtime_material.exact_bytes,
                    shared_protocol::ObjectKind::PredictiveObject,
                )?;
                // Check if peer has the transform class; if not, include the definition inline.
                let peer_has_class = self.peer_transform_versions.contains_key(&instance.class_id);
                let inline_transform_defs = if peer_has_class {
                    Vec::new()
                } else {
                    vec![TransformDefPayload::new(class.clone())]
                };
                let transform_instance_payload = TransformInstancePayload::new(&class, instance);
                let instance_bytes = transform_instance_payload.encode()
                    .map_err(|e| ServerError::ValidationPayload(shared_protocol::WireError::InvalidPayload(e.to_string())))?;
                // Collect dependencies from the transform instance
                let mut dep_list = Vec::new();
                for object_id in &transform_instance_payload.dependency_closure.substrate_object_ids {
                    dep_list.push(ObjectDependency {
                        object_kind: ObjectKind::ExactBlock,
                        object_id: object_id.clone(),
                        required_revision: 0,
                    });
                }
                for assembly_id in &transform_instance_payload.dependency_closure.assembly_ids {
                    dep_list.push(ObjectDependency {
                        object_kind: ObjectKind::Assembly,
                        object_id: format!("assembly:{}", assembly_id.0),
                        required_revision: 0,
                    });
                }
                for transform_id in &transform_instance_payload.dependency_closure.transform_ids {
                    dep_list.push(ObjectDependency {
                        object_kind: ObjectKind::Transform,
                        object_id: format!("transform:{}", transform_id.0),
                        required_revision: 0,
                    });
                }
                for schema_id in &transform_instance_payload.dependency_closure.schema_ids {
                    dep_list.push(ObjectDependency {
                        object_kind: ObjectKind::Schema,
                        object_id: format!("schema:{}", schema_id.0),
                        required_revision: 0,
                    });
                }
                let dependency_closure = normalize_dependencies(dep_list);
                let inline_transform_ids: HashSet<TransformId> = inline_transform_defs
                    .iter()
                    .map(|def| def.class.transform_id)
                    .collect();
                if dependency_closure.iter().any(|dependency| {
                    !self.predictive_dependency_is_admissible_with_inline(
                        dependency,
                        &HashSet::new(),
                        &HashSet::new(),
                        &HashSet::new(),
                        &inline_transform_ids,
                    )
                }) {
                    // Transform dependency unavailable — fall back to direct state.
                    self.record_direct_state_downgrade(
                        ControllerRouteFamily::Transform,
                        source_kind,
                        context_hash,
                        ctx.seq_no.0,
                        FallbackReason::DependencyUnavailable,
                    );
                    return self.emit_direct_state_fallback_data(
                        ctx,
                        item_id,
                        block,
                        ControllerRouteFamily::Transform,
                        source_kind,
                        context_hash,
                        FallbackReason::DependencyUnavailable,
                    );
                }
                let route_graph = derived_dispatch_route_graph(
                    ControllerRouteFamily::Transform,
                    ControllerRouteFamily::Transform.route_family(),
                    &dependency_closure,
                    0,
                    &[],
                    &[],
                    None,
                    None,
                    None,
                );
                let payload = PredictiveRouteDispatchPayload {
                    version: PredictiveRouteDispatchPayload::VERSION,
                    route_family: ControllerRouteFamily::Transform,
                    route_kind: ControllerRouteFamily::Transform.route_family(),
                    route_source_kind: Some(source_kind),
                    assembly_mode: None,
                    precision_band: shared_protocol::PrecisionBand::default(),
                    dependency_closure,
                    sync_risk: 0,
                    literal_bytes: instance_bytes,
                    assembly_ref: None,
                    inline_assembly_defs: Vec::new(),
                    inline_schema_defs: Vec::new(),
                    inline_dictionaries: Vec::new(),
                    inline_episode_hints: Vec::new(),
                    route_graph,
                    contradiction_bytes: Vec::new(),
                    prg: None,
                    hybrid_route: None,
                }
                .with_derived_route_graph();
                self.record_route_outcome_detail(
                    ControllerRouteFamily::Transform,
                    Some(source_kind),
                    context_hash,
                    ctx.seq_no.0,
                    true,
                    Some(chosen_symbol),
                    ContextTreeOutcome {
                        success: true,
                        ..ContextTreeOutcome::default()
                    },
                );
                self.emit_predictive_route_or_inflation_fallback(
                    ctx, item_id, RecordType::PredictiveCorrect, payload,
                    &runtime_material.exact_bytes,
                    ControllerRouteFamily::Transform,
                    source_kind, context_hash,
                )
            }
            shared_protocol::ControllerRoutePlan::SchemaExpansion { candidate, .. } => {
                if let Some(mut schema) = self
                    .schemas
                    .iter()
                    .find(|schema| schema.schema_id == candidate.schema_id)
                    .cloned()
                {
                    self.store_runtime_object_for_item(
                        item_id,
                        &block.exact_bytes,
                        shared_protocol::ObjectKind::PredictiveObject,
                    )?;
                    schema.lifecycle = schema.lifecycle.record_seen(ctx.seq_no.0, true);
                    self.maybe_enqueue_schema_promotion(&schema);
                    self.update_schema(schema.clone());
                    let schema_payload = self
                        .schema_defs
                        .get(&candidate.schema_id)
                        .cloned()
                        .unwrap_or_else(|| {
                            let prg = shared_protocol::reconstruct_graph_from_schema(&schema);
                            SchemaDefPayload::new(schema.clone(), &prg)
                        });
                    let prg = Self::schema_payload_graph(&schema_payload);
                    let (prg_dependencies, mut inline_schema_defs) =
                        self.collect_prg_dependencies_recursive(&prg);
                    let dependency_closure = normalize_dependencies({
                        let mut dependencies = candidate.dependency_closure.dependencies;
                        dependencies.extend(prg_dependencies);
                        dependencies
                    });
                    inline_schema_defs.insert(0, schema_payload.clone());
                    let inline_schema_defs = dedup_schema_payloads(inline_schema_defs);
                    let inline_schema_ids = inline_schema_defs
                        .iter()
                        .map(|payload| payload.schema.schema_id)
                        .collect::<HashSet<_>>();
                    if dependency_closure.iter().any(|dependency| {
                        !self.predictive_dependency_is_admissible_with_inline(
                            dependency,
                            &HashSet::new(),
                            &inline_schema_ids,
                            &HashSet::new(),
                            &HashSet::new(),
                        )
                    }) {
                        // S3.4: Schema dependency inadmissible after Sprint 1-3 changes.
                        return self.emit_direct_state_fallback_data(
                            ctx,
                            item_id,
                            block.clone(),
                            ControllerRouteFamily::SchemaExpansion,
                            source_kind,
                            context_hash,
                            FallbackReason::SchemaDependencyInadmissible,
                        );
                    }
                    let route_graph = derived_dispatch_route_graph(
                        ControllerRouteFamily::SchemaExpansion,
                        ControllerRouteFamily::SchemaExpansion.route_family(),
                        &dependency_closure,
                        0,
                        &[],
                        &[],
                        None,
                        Some(&prg),
                        None,
                    );
                    let payload = PredictiveRouteDispatchPayload {
                        version: PredictiveRouteDispatchPayload::VERSION,
                        route_family: ControllerRouteFamily::SchemaExpansion,
                        route_kind: ControllerRouteFamily::SchemaExpansion.route_family(),
                        route_source_kind: Some(source_kind),
                        assembly_mode: None,
                        precision_band: candidate.precision_band,
                        dependency_closure,
                        sync_risk: 0,
                        literal_bytes: Vec::new(),
                        assembly_ref: None,
                        inline_assembly_defs: Vec::new(),
                        inline_schema_defs,
                        inline_dictionaries: Vec::new(),
                        inline_episode_hints: Vec::new(),
                        route_graph,
                        contradiction_bytes: Vec::new(),
                        prg: Some(prg),
                        hybrid_route: None,
                    }
                    .with_derived_route_graph();
                    self.record_route_outcome_detail(
                        ControllerRouteFamily::SchemaExpansion,
                        Some(source_kind),
                        context_hash,
                        ctx.seq_no.0,
                        true,
                        Some(chosen_symbol),
                        ContextTreeOutcome {
                            success: true,
                            ..ContextTreeOutcome::default()
                        },
                    );
                    self.emit_predictive_route_or_inflation_fallback(
                        ctx, item_id, RecordType::PredictiveCorrect, payload,
                        &runtime_material.exact_bytes,
                        ControllerRouteFamily::SchemaExpansion,
                        source_kind, context_hash,
                    )
                } else {
                    // S3.4: Schema expansion had no viable predictive route.
                    self.emit_direct_state_fallback_data(
                        ctx,
                        item_id,
                        block.clone(),
                        ControllerRouteFamily::SchemaExpansion,
                        source_kind,
                        context_hash,
                        FallbackReason::NoViablePredictiveRoute,
                    )
                }
            }
            shared_protocol::ControllerRoutePlan::Hybrid { route, .. } => {
                self.store_runtime_object_for_item(
                    item_id,
                    &runtime_material.exact_bytes,
                    shared_protocol::ObjectKind::PredictiveObject,
                )?;
                let (dependency_closure, inline_schema_defs) =
                    self.collect_hybrid_route_dependencies_recursive(&route);
                let inline_schema_ids = inline_schema_defs
                    .iter()
                    .map(|payload| payload.schema.schema_id)
                    .collect::<HashSet<_>>();
                if dependency_closure.iter().any(|dependency| {
                    !self.predictive_dependency_is_admissible_with_inline(
                        dependency,
                        &HashSet::new(),
                        &inline_schema_ids,
                        &HashSet::new(),
                        &HashSet::new(),
                    )
                }) {
                    // S3.4: Hybrid route dependency unavailable.
                    return self.emit_direct_state_fallback_data(
                        ctx,
                        item_id,
                        block.clone(),
                        ControllerRouteFamily::Hybrid,
                        source_kind,
                        context_hash,
                        FallbackReason::DependencyUnavailable,
                    );
                }
                let route_graph = derived_dispatch_route_graph(
                    ControllerRouteFamily::Hybrid,
                    ControllerRouteFamily::Hybrid.route_family(),
                    &dependency_closure,
                    0,
                    &[],
                    &[],
                    None,
                    None,
                    Some(&route),
                );
                let payload = PredictiveRouteDispatchPayload {
                    version: PredictiveRouteDispatchPayload::VERSION,
                    route_family: ControllerRouteFamily::Hybrid,
                    route_kind: ControllerRouteFamily::Hybrid.route_family(),
                    route_source_kind: Some(source_kind),
                    assembly_mode: None,
                    precision_band: route.precision_band,
                    dependency_closure,
                    sync_risk: 0,
                    literal_bytes: Vec::new(),
                    assembly_ref: None,
                    inline_assembly_defs: Vec::new(),
                    inline_schema_defs,
                    inline_dictionaries: Vec::new(),
                    inline_episode_hints: Vec::new(),
                    route_graph,
                    contradiction_bytes: Vec::new(),
                    prg: None,
                    hybrid_route: Some(route),
                }
                .with_derived_route_graph();
                self.record_route_outcome_detail(
                    ControllerRouteFamily::Hybrid,
                    Some(source_kind),
                    context_hash,
                    ctx.seq_no.0,
                    true,
                    Some(chosen_symbol),
                    ContextTreeOutcome {
                        success: true,
                        ..ContextTreeOutcome::default()
                    },
                );
                self.emit_predictive_route_or_inflation_fallback(
                    ctx, item_id, RecordType::PredictiveCorrect, payload,
                    &runtime_material.exact_bytes,
                    ControllerRouteFamily::Hybrid,
                    source_kind, context_hash,
                )
            }
            shared_protocol::ControllerRoutePlan::Assembly { candidate, .. } => {
                self.store_runtime_object_for_item(
                    item_id,
                    &runtime_material.exact_bytes,
                    shared_protocol::ObjectKind::PredictiveObject,
                )?;
                self.record_route_outcome_detail(
                    ControllerRouteFamily::Assembly,
                    Some(source_kind),
                    context_hash,
                    ctx.seq_no.0,
                    true,
                    Some(chosen_symbol),
                    ContextTreeOutcome {
                        success: true,
                        ..ContextTreeOutcome::default()
                    },
                );
                self.emit_assembly_route(
                    ctx,
                    item_id,
                    source_kind,
                    candidate,
                    &runtime_material.exact_bytes,
                )
            }
            shared_protocol::ControllerRoutePlan::EpisodeCompletion { candidate, .. } => {
                self.store_runtime_object_for_item(
                    item_id,
                    &runtime_material.exact_bytes,
                    shared_protocol::ObjectKind::PredictiveObject,
                )?;
                self.record_route_outcome_detail(
                    ControllerRouteFamily::EpisodeCompletion,
                    Some(source_kind),
                    context_hash,
                    ctx.seq_no.0,
                    true,
                    Some(chosen_symbol),
                    ContextTreeOutcome {
                        success: true,
                        ..ContextTreeOutcome::default()
                    },
                );
                self.emit_episode_completion_route(
                    ctx,
                    item_id,
                    source_kind,
                    cue,
                    context_hash.unwrap_or_default(),
                    runtime_material.exact_bytes.len() as u32,
                    candidate,
                )
            }
            shared_protocol::ControllerRoutePlan::DirectState { .. } => {
                self.record_route_outcome_detail(
                    ControllerRouteFamily::DirectState,
                    Some(source_kind),
                    context_hash,
                    ctx.seq_no.0,
                    true,
                    Some(chosen_symbol),
                    ContextTreeOutcome {
                        success: true,
                        ..ContextTreeOutcome::default()
                    },
                );
                self.emit_plain_exact_state_data(ctx, item_id, block.clone(), false, false, false, FallbackReason::NotAFallback)
            }
            shared_protocol::ControllerRoutePlan::ExactAtom { plan, .. } => {
                self.store_runtime_object_for_item(
                    item_id,
                    &runtime_material.exact_bytes,
                    shared_protocol::ObjectKind::PredictiveObject,
                )?;
                self.emit_exact_atom_route(ctx, item_id, source_kind, context_hash, plan)
            }
        };
    }

    fn assembly_object_version(assembly: &Assembly) -> ObjectVersion {
        let signature = shared_protocol::assembly_reuse_signature(assembly);
        ObjectVersion {
            schema_version: 1,
            object_revision: (signature.body_shape_hash
                ^ signature.dependency_fingerprint
                ^ signature.output_len as u64) as u32,
        }
    }

    fn stable_assembly_id(
        source_kind: shared_protocol::SourceKind,
        candidate: &shared_protocol::AssemblyExtractionCandidate,
    ) -> AssemblyId {
        // S4.8: Assembly ID must be stable across items that share the same
        // structural pattern, regardless of their specific byte lengths.
        // The structural_hash captures the shape (literal islands + slot
        // structure), the role_signature captures slot/delimiter roles, and
        // source_kind distinguishes text from json from binary.
        // Including canonical_length_min/max in the ID prevents reuse:
        // two items with identical structural patterns but different byte
        // lengths (e.g., "item 4 revision 3..." vs "item 7 revision 6...")
        // would get different assembly_ids and never share definitions.
        // Instead, we use the structural_hash as the primary determinant,
        // with role_signature for disambiguation when two different
        // structures happen to hash to the same value.
        let mut value = candidate.structural_hash
            ^ ((source_kind as u64) << 56);
        value ^= candidate.role_signature.role_bits.rotate_left(7);
        value ^= candidate.role_signature.delimiter_role_bits.rotate_left(13);
        value ^= candidate.role_signature.slot_role_bits.rotate_left(29);
        if value == 0 {
            value = 1;
        }
        AssemblyId(value)
    }

    fn best_material_match(material: &[u8], bytes: &[u8], offset: usize) -> Option<(u32, usize)> {
        const MIN_COMPONENT_LEN: usize = 4;
        if offset >= bytes.len() || material.len() < MIN_COMPONENT_LEN {
            return None;
        }
        let remaining = &bytes[offset..];
        let mut best: Option<(u32, usize)> = None;
        for start in 0..material.len() {
            let max_len = remaining.len().min(material.len().saturating_sub(start));
            if max_len < MIN_COMPONENT_LEN {
                continue;
            }
            let mut len = 0usize;
            while len < max_len && material[start + len] == remaining[len] {
                len += 1;
            }
            if len >= MIN_COMPONENT_LEN
                && best
                    .as_ref()
                    .map(|(_, current)| len > *current)
                    .unwrap_or(true)
            {
                best = Some((start as u32, len));
            }
        }
        best
    }

    fn best_catalog_component(
        &self,
        bytes: &[u8],
        offset: usize,
    ) -> Option<(
        shared_protocol::AssemblyComponentRef,
        usize,
        ObjectDependency,
    )> {
        let mut best: Option<(
            shared_protocol::AssemblyComponentRef,
            usize,
            ObjectDependency,
        )> = None;
        for block in self.peer_catalog.blocks_iter() {
            let material = &block.material;
            let Some((start, matched_len)) = Self::best_material_match(material, bytes, offset)
            else {
                continue;
            };
            let dep = ObjectDependency {
                object_kind: ObjectKind::ExactBlock,
                object_id: block.block_id.0.to_string(),
                required_revision: 0,
            };
            let candidate = (
                shared_protocol::AssemblyComponentRef::ExactBlock {
                    object_id: block.block_id.0.to_string(),
                    start,
                    len: matched_len as u32,
                },
                matched_len,
                dep,
            );
            if best.as_ref().map(|(_, len, _)| *len).unwrap_or(0) < matched_len {
                best = Some(candidate);
            }
        }
        for bundle in self.peer_catalog.bundles_iter() {
            let Ok(material) = self.peer_catalog.materialize_bundle(bundle.bundle_id) else {
                continue;
            };
            let Some((start, matched_len)) =
                Self::best_material_match(material.as_slice(), bytes, offset)
            else {
                continue;
            };
            let dep = ObjectDependency {
                object_kind: ObjectKind::ExactBundle,
                object_id: bundle.bundle_id.0.to_string(),
                required_revision: 0,
            };
            let candidate = (
                shared_protocol::AssemblyComponentRef::ExactBundle {
                    object_id: bundle.bundle_id.0.to_string(),
                    start,
                    len: matched_len as u32,
                },
                matched_len,
                dep,
            );
            if best.as_ref().map(|(_, len, _)| *len).unwrap_or(0) < matched_len {
                best = Some(candidate);
            }
        }
        best
    }

    fn assembly_literal_bytes(body: &shared_protocol::AssemblyBody) -> usize {
        body.literal_len() as usize
    }

    fn assembly_route_mode(
        _body: &shared_protocol::AssemblyBody,
        inline: bool,
    ) -> shared_protocol::AssemblyRouteMode {
        if inline {
            shared_protocol::AssemblyRouteMode::DefineAndActivate
        } else {
            shared_protocol::AssemblyRouteMode::ReuseReference
        }
    }

    fn is_structural_delimiter(byte: u8) -> bool {
        matches!(
            byte,
            b'\n'
                | b'\r'
                | b'\t'
                | b','
                | b';'
                | b':'
                | b'|'
                | b'{'
                | b'}'
                | b'['
                | b']'
                | b'('
                | b')'
                | b'\"'
                | b'\''
        )
    }

    fn push_bytes_as_body_nodes(nodes: &mut Vec<shared_protocol::AssemblyBodyNode>, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        if bytes
            .iter()
            .all(|byte| Self::is_structural_delimiter(*byte))
        {
            nodes.push(shared_protocol::AssemblyBodyNode::DelimiterAnchor {
                bytes: bytes.to_vec(),
            });
        } else {
            nodes.push(shared_protocol::AssemblyBodyNode::LiteralIsland {
                bytes: bytes.to_vec(),
            });
        }
    }

    fn push_dependency_from_component(
        dependencies: &mut Vec<ObjectDependency>,
        component: &shared_protocol::AssemblyComponentRef,
    ) {
        let dependency = match component {
            shared_protocol::AssemblyComponentRef::ExactBlock { object_id, .. } => {
                Some(ObjectDependency {
                    object_kind: ObjectKind::ExactBlock,
                    object_id: object_id.clone(),
                    required_revision: 0,
                })
            }
            shared_protocol::AssemblyComponentRef::ExactBundle { object_id, .. } => {
                Some(ObjectDependency {
                    object_kind: ObjectKind::ExactBundle,
                    object_id: object_id.clone(),
                    required_revision: 0,
                })
            }
            shared_protocol::AssemblyComponentRef::ExactRange { object_id, .. } => {
                Some(ObjectDependency {
                    object_kind: ObjectKind::ExactRange,
                    object_id: object_id.clone(),
                    required_revision: 0,
                })
            }
            shared_protocol::AssemblyComponentRef::AtomFragment { object_id, .. } => {
                Some(ObjectDependency {
                    object_kind: ObjectKind::AtomFragment,
                    object_id: object_id.clone(),
                    required_revision: 0,
                })
            }
            shared_protocol::AssemblyComponentRef::ResidualBuffer { .. } => None,
        };
        if let Some(dependency) = dependency {
            dependencies.push(dependency);
        }
    }

    fn structuralize_literal_island(
        &self,
        bytes: &[u8],
        nodes: &mut Vec<shared_protocol::AssemblyBodyNode>,
        dependencies: &mut Vec<ObjectDependency>,
    ) {
        let mut cursor = 0usize;
        let mut literal_start = 0usize;
        let mut matched_any = false;
        while cursor < bytes.len() {
            if let Some((component, consumed, dependency)) =
                self.best_catalog_component(bytes, cursor)
            {
                Self::push_bytes_as_body_nodes(nodes, &bytes[literal_start..cursor]);
                nodes.push(shared_protocol::AssemblyBodyNode::SubstrateSpan {
                    reference: component.clone(),
                });
                dependencies.push(dependency);
                matched_any = true;
                cursor += consumed;
                literal_start = cursor;
                continue;
            }
            cursor += 1;
        }
        if !matched_any {
            Self::push_bytes_as_body_nodes(nodes, bytes);
        } else {
            Self::push_bytes_as_body_nodes(nodes, &bytes[literal_start..]);
        }
    }

    fn structuralize_assembly_body(
        &self,
        candidate: &shared_protocol::AssemblyExtractionCandidate,
        _bytes: &[u8],
    ) -> Option<(shared_protocol::AssemblyBody, Vec<ObjectDependency>)> {
        let mut nodes = Vec::new();
        let mut dependencies = candidate.dependency_closure.dependencies.clone();

        for node in &candidate.body.nodes {
            match node {
                shared_protocol::AssemblyBodyNode::LiteralIsland { bytes }
                | shared_protocol::AssemblyBodyNode::DelimiterAnchor { bytes } => {
                    self.structuralize_literal_island(bytes, &mut nodes, &mut dependencies);
                }
                shared_protocol::AssemblyBodyNode::SubstrateSpan { reference } => {
                    Self::push_dependency_from_component(&mut dependencies, reference);
                    nodes.push(shared_protocol::AssemblyBodyNode::SubstrateSpan {
                        reference: reference.clone(),
                    });
                }
                shared_protocol::AssemblyBodyNode::SlotPlaceholder {
                    slot_index,
                    role_bits,
                    bytes,
                } => nodes.push(shared_protocol::AssemblyBodyNode::SlotPlaceholder {
                    slot_index: *slot_index,
                    role_bits: *role_bits,
                    bytes: bytes.clone(),
                }),
                shared_protocol::AssemblyBodyNode::MotifLink { motif_hash, bytes } => {
                    nodes.push(shared_protocol::AssemblyBodyNode::MotifLink {
                        motif_hash: *motif_hash,
                        bytes: bytes.clone(),
                    });
                }
                shared_protocol::AssemblyBodyNode::TypedBoundary { label, bytes } => {
                    nodes.push(shared_protocol::AssemblyBodyNode::TypedBoundary {
                        label: label.clone(),
                        bytes: bytes.clone(),
                    });
                }
            }
        }

        dependencies.sort_by(|l, r| {
            (l.object_kind as u8)
                .cmp(&(r.object_kind as u8))
                .then_with(|| l.object_id.cmp(&r.object_id))
        });
        dependencies.dedup();
        let body = shared_protocol::AssemblyBody { nodes };
        if body.structural_component_count() == 0 {
            return None;
        }
        let residual_bytes = Self::assembly_literal_bytes(&body) as u32;
        let predicted_bytes = body.output_len();
        // S1.4: Use real peer-availability checks for assembly dependency admissibility
        // instead of vacuous string-non-empty checks.
        // S1.4.f: This is not a vacuous "string exists" check — predictive_dependency_is_admissible
        // consults actual confirmed+pending peer state with revision discipline.
        let dependencies_available = dependencies.is_empty()
            || dependencies
                .iter()
                .all(|dependency| self.predictive_dependency_is_admissible(dependency));
        if shared_protocol::check_assembly_route_admissibility(
            shared_protocol::AssemblyAdmissibilityPolicy::bounded_default(),
            shared_protocol::AssemblyAdmissibilityInput {
                ambiguity_score: candidate.ambiguity_score(),
                dependencies_available,
                expansion_depth: body.structural_component_count().min(u8::MAX as usize) as u8,
                residual_bytes,
                predicted_bytes,
                contract_min_len: candidate.canonical_length_min,
                contract_max_len: candidate.canonical_length_max,
            },
        )
        .is_err()
        {
            return None;
        }
        Some((body, dependencies))
    }

    fn materialize_or_lookup_assembly(
        &mut self,
        mut assembly: Assembly,
    ) -> Result<Assembly, ServerError> {
        assembly.dependency_closure.version = Self::assembly_object_version(&assembly);
        if let Some(existing) = self.assemblies.get(&assembly.assembly_id).cloned() {
            return Ok(existing);
        }
        if let Some(cache) = &self.assembly_cache {
            if let Some(existing) = cache.lookup_assembly(assembly.assembly_id)? {
                self.assemblies
                    .insert(existing.assembly_id, existing.clone());
                return Ok(existing);
            }
            cache.store_assembly(&assembly)?;
        }
        self.assemblies
            .insert(assembly.assembly_id, assembly.clone());
        Ok(assembly)
    }

    fn emit_assembly_route(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        source_kind: shared_protocol::SourceKind,
        candidate: shared_protocol::AssemblyExtractionCandidate,
        bytes: &[u8],
    ) -> Result<Record, ServerError> {
        let Some((body, mut dependencies)) = self.structuralize_assembly_body(&candidate, bytes)
        else {
            // S3.4: Assembly body structuralization failed.
            return self.emit_direct_state_fallback_data(
                ctx,
                item_id,
                transient_exact_material(source_kind, bytes),
                ControllerRouteFamily::Assembly,
                source_kind,
                None,
                FallbackReason::AssemblyStructuralizationFailed,
            );
        };
        let assembly_id = Self::stable_assembly_id(source_kind, &candidate);
        let estimated_route_wire_cost = candidate.estimated_route_wire_cost();
        let ambiguity_score = candidate.ambiguity_score();
        let dependency_sync_risk = candidate.dependency_sync_risk();
        let assembly_kind = candidate.assembly_kind;
        let role_signature = candidate.role_signature;
        let slots = candidate.slots.clone();
        let cue = candidate.cue;
        let canonical_length_min = candidate.canonical_length_min;
        let canonical_length_max = candidate.canonical_length_max;
        dependencies.extend(candidate.dependency_closure.dependencies);
        dependencies.sort_by(|l, r| {
            (l.object_kind as u8)
                .cmp(&(r.object_kind as u8))
                .then_with(|| l.object_id.cmp(&r.object_id))
        });
        dependencies.dedup();
        let mut assembly = self.materialize_or_lookup_assembly(Assembly {
            assembly_id,
            source_kind,
            assembly_kind,
            role_signature,
            slots,
            body,
            dependency_closure: shared_protocol::DependencyClosure {
                version: ObjectVersion::default(),
                dependencies,
            },
            cue,
            lifecycle: shared_protocol::ObjectLifecycleMeta::default(),
            canonical_length_min,
            canonical_length_max,
        })?;
        assembly.lifecycle = assembly.lifecycle.record_seen(ctx.seq_no.0, true);
        assembly.dependency_closure.version = Self::assembly_object_version(&assembly);
        self.maybe_enqueue_assembly_promotion(
            &assembly,
            (bytes.len() as u32)
                .saturating_sub(estimated_route_wire_cost)
                .max(1),
            ambiguity_score,
        );
        self.persist_assembly(assembly.clone())?;
        let def_payload = AssemblyDefPayload::new(assembly.clone());
        let assembly_ref = shared_protocol::assembly_ref_from_payload(&def_payload);
        // S4.8: Verify the assembly ref's output_len matches the input.
        if assembly_ref.output_len as usize != bytes.len() {
            // The assembly body's output_len doesn't match the original input.
            // This means the structuralization is buggy — fall back to direct state.
            return self.emit_direct_state_fallback_data(
                ctx,
                item_id,
                transient_exact_material(source_kind, bytes),
                ControllerRouteFamily::Assembly,
                source_kind,
                None,
                FallbackReason::AssemblyStructuralizationFailed,
            );
        }
        let current_signature = shared_protocol::assembly_reuse_signature(&assembly);
        // S1.1.v3 CONFIRMED-ONLY: Only consider confirmed state for "already synced".
        // If only pending (emitted but not acked), we must carry the inline def
        // to ensure the peer can decode, because admissibility no longer reads
        // pending state.
        let already_synced = self
            .assembly_sync_versions
            .get(&assembly.assembly_id)
            .map(|signature| signature == &current_signature)
            .unwrap_or(false);
        let inline_assembly_defs = if already_synced {
            Vec::new()
        } else {
            vec![def_payload.clone()]
        };
        // Note: pending_assembly_sync_versions insert is NOT done here at planning time.
        // The sole writer is emit_predictive_route_record, which inserts pending
        // state only after the record is actually emitted. This prevents phantom
        // pending state if the route is planned but emission fails.
        // S1.1.v3: Same-batch flows carry inline definitions. Admissibility reads
        // confirmed state only; pending state is not consulted. If a dependency
        // was emitted in a prior record but not yet confirmed, the current route
        // must carry the definition inline or fall back.
        let assembly_mode = Self::assembly_route_mode(&assembly.body, !already_synced);
        let dependency_closure = assembly.dependency_closure.dependencies.clone();
        let dispatch_sync_risk = if already_synced {
            0
        } else {
            dependency_sync_risk.max(1)
        };
        let dispatch_payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::Assembly,
            route_kind: ControllerRouteFamily::Assembly.route_family(),
            route_source_kind: Some(source_kind),
            assembly_mode: Some(assembly_mode),
            precision_band: shared_protocol::PrecisionBand::Exact,
            dependency_closure: dependency_closure.clone(),
            sync_risk: dispatch_sync_risk,
            literal_bytes: Vec::new(),
            assembly_ref: Some(assembly_ref.clone()),
            inline_assembly_defs,
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: Vec::new(),
            route_graph: derived_dispatch_route_graph(
                ControllerRouteFamily::Assembly,
                ControllerRouteFamily::Assembly.route_family(),
                &dependency_closure,
                dispatch_sync_risk,
                &[],
                &[],
                Some(&assembly_ref),
                None,
                None,
            ),
            contradiction_bytes: Vec::new(),
            prg: None,
            hybrid_route: None,
        }
        .with_derived_route_graph();
        self.emit_predictive_route_or_inflation_fallback(
            ctx, item_id, RecordType::PredictiveConfirm, dispatch_payload,
            bytes,
            ControllerRouteFamily::Assembly,
            source_kind,
            None,
        )
    }

    fn emit_exact_atom_route(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        source_kind: shared_protocol::SourceKind,
        context_hash: Option<ContextHash>,
        plan: shared_protocol::ExactAtomPlan,
    ) -> Result<Record, ServerError> {
        if plan.substrate_graph.nodes.is_empty() {
            let block = self
                .entries
                .get(&item_id)
                .map(|entry| {
                    transient_exact_material(entry.object.source_kind, entry.exact_bytes())
                })
                .ok_or_else(|| ServerError::Validation(ValidationError::InvalidPredictiveHeader))?;
            return self.emit_direct_state_fallback_data(
                ctx,
                item_id,
                block,
                ControllerRouteFamily::ExactAtom,
                source_kind,
                context_hash,
                FallbackReason::SubstrateMiss,
            );
        }
        self.record_route_outcome(
            ControllerRouteFamily::ExactAtom,
            Some(source_kind),
            context_hash,
            ctx.seq_no.0,
            true,
        );
        let substrate_graph = plan.substrate_graph;
        let dependency_closure = normalize_dependencies(substrate_graph.dependency_closure.clone());
        if dependency_closure
            .iter()
            .any(|dependency| !self.predictive_dependency_is_admissible(dependency))
        {
            let block = self
                .entries
                .get(&item_id)
                .map(|entry| transient_exact_material(entry.object.source_kind, entry.exact_bytes()))
                .ok_or_else(|| ServerError::Validation(ValidationError::InvalidPredictiveHeader))?;
            return self.emit_direct_state_fallback_data(
                ctx,
                item_id,
                block,
                ControllerRouteFamily::ExactAtom,
                source_kind,
                context_hash,
                FallbackReason::DependencyUnavailable,
            );
        }
        let route = shared_protocol::HybridRoute {
            route_family: ControllerRouteFamily::ExactAtom,
            precision_band: shared_protocol::PrecisionBand::Exact,
            assembly_mode: None,
            output_len: plan.output_len,
            dependency_closure: dependency_closure.clone(),
            components: vec![shared_protocol::HybridRouteComponent::SubstrateGraph(
                substrate_graph,
            )],
        };
        let payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::ExactAtom,
            route_kind: ControllerRouteFamily::ExactAtom.route_family(),
            route_source_kind: Some(source_kind),
            assembly_mode: None,
            precision_band: shared_protocol::PrecisionBand::Exact,
            dependency_closure,
            sync_risk: 0,
            literal_bytes: Vec::new(),
            assembly_ref: None,
            inline_assembly_defs: Vec::new(),
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: Vec::new(),
            route_graph: derived_dispatch_route_graph(
                ControllerRouteFamily::ExactAtom,
                ControllerRouteFamily::ExactAtom.route_family(),
                &route.dependency_closure,
                0,
                &[],
                &[],
                None,
                None,
                Some(&route),
            ),
            contradiction_bytes: Vec::new(),
            prg: None,
            hybrid_route: Some(route),
        }
        .with_derived_route_graph();
        // S4.7: Use inflation fallback for exact-atom routes.
        let exact_bytes = self
            .entries
            .get(&item_id)
            .map(|entry| entry.exact_bytes().to_vec())
            .unwrap_or_default();
        self.emit_predictive_route_or_inflation_fallback(
            ctx, item_id, RecordType::PredictiveConfirm, payload,
            &exact_bytes,
            ControllerRouteFamily::ExactAtom,
            source_kind,
            context_hash,
        )
    }

    fn emit_episode_hint_record(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        payload: shared_protocol::EpisodeHintPayload,
    ) -> Result<Record, ServerError> {
        let header = RecordHeader {
            version: shared_protocol::PROTOCOL_VERSION,
            stream_id: ctx.stream_id,
            epoch_id: ctx.epoch_id,
            seq_no: ctx.seq_no,
            record_type: RecordType::EpisodeHint,
            codec_mode: CodecMode::None,
            flags: ctx.flags(RecordFlags::empty()),
            item_id,
            payload_len: 0,
        };
        encode_episode_hint_record(header, &payload)
            .map_err(ServerError::StateProgram)?
            .validate()
            .map_err(ServerError::Validation)
    }

    fn emit_replay_hint_record(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        payload: shared_protocol::EpisodeHintPayload,
    ) -> Result<Record, ServerError> {
        let header = RecordHeader {
            version: shared_protocol::PROTOCOL_VERSION,
            stream_id: ctx.stream_id,
            epoch_id: ctx.epoch_id,
            seq_no: ctx.seq_no,
            record_type: RecordType::ReplayHint,
            codec_mode: CodecMode::None,
            flags: ctx.flags(RecordFlags::empty()),
            item_id,
            payload_len: 0,
        };
        encode_replay_hint_record(header, &payload)
            .map_err(ServerError::StateProgram)?
            .validate()
            .map_err(ServerError::Validation)
    }

    fn emit_episode_completion_route(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        source_kind: shared_protocol::SourceKind,
        cue: shared_protocol::SparseCue,
        context_hash: ContextHash,
        output_len: u32,
        candidate: EpisodeCompletionCandidate,
    ) -> Result<Record, ServerError> {
        let hint_payload = shared_protocol::EpisodeHintPayload::new(
            context_hash,
            LagBucket(0),
            candidate.precision_band,
            episode_hint_dependencies(std::slice::from_ref(&candidate)),
            vec![candidate.clone()],
        );
        let graph = shared_protocol::PredictiveReconstructionGraph::new(
            1,
            2,
            output_len,
            vec![shared_protocol::PrgNode {
                node_id: 1,
                kind: shared_protocol::PrgNodeKind::EpisodeRef,
                output_len,
                dependency_contract: shared_protocol::PrgDependencyContract::default(),
                literal_bytes: Vec::new(),
                substrate_ref: None,
                assembly_ref: None,
                transform_ref: None,
                episode_ref: Some(candidate.object_ref.clone()),
                schema_ref: None,
                child_ids: Vec::new(),
                permutation: Vec::new(),
                repeat_count: None,
                select_index: None,
                slot_id: None,
                patch_offset: None,
                patch_remove_len: None,
                basis_node_id: None,
                guard_precision: None,
                branch_target: None,
            }],
            vec![candidate.object_ref.object_id.clone()],
        );
        let dependency_closure = episode_hint_dependencies(std::slice::from_ref(&candidate));
        let payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::EpisodeCompletion,
            route_kind: ControllerRouteFamily::EpisodeCompletion.route_family(),
            route_source_kind: Some(source_kind),
            assembly_mode: None,
            precision_band: candidate.precision_band,
            dependency_closure: dependency_closure.clone(),
            sync_risk: 0,
            literal_bytes: Vec::new(),
            assembly_ref: None,
            inline_assembly_defs: Vec::new(),
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: vec![hint_payload],
            route_graph: derived_dispatch_route_graph(
                ControllerRouteFamily::EpisodeCompletion,
                ControllerRouteFamily::EpisodeCompletion.route_family(),
                &dependency_closure,
                0,
                &[],
                &[],
                None,
                Some(&graph),
                None,
            ),
            contradiction_bytes: Vec::new(),
            prg: Some(graph),
            hybrid_route: None,
        }
        .with_derived_route_graph();
        let object_ref = candidate.object_ref;
        let episode_context_hash = derive_context_hash(cue, &object_ref);
        self.append_episode_activation_for_item(
            item_id,
            EpisodeActivationEvent {
                source_kind,
                object_ref,
                cue,
                context_hash: episode_context_hash,
                tick: ctx.seq_no.0,
                success: true,
            },
        );
        // S4.7: Use inflation fallback for episode completion routes.
        let exact_bytes = self
            .entries
            .get(&item_id)
            .map(|entry| entry.exact_bytes().to_vec())
            .unwrap_or_default();
        self.emit_predictive_route_or_inflation_fallback(
            ctx, item_id, RecordType::PredictiveConfirm, payload,
            &exact_bytes,
            ControllerRouteFamily::EpisodeCompletion,
            source_kind,
            Some(episode_context_hash),
        )
    }

    fn emit_plain_exact_state_data(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        block: ExactStateMaterial,
        direct_state_fallback: bool,
        substrate_fallback: bool,
        transform_demoted_fallback: bool,
        fallback_reason: FallbackReason,
    ) -> Result<Record, ServerError> {
        let source_kind = self
            .source_bindings
            .get(&item_id)
            .map(|descriptor| descriptor.kind)
            .unwrap_or(shared_protocol::SourceKind::Binary);
        let runtime_material = transient_exact_material(source_kind, &block.exact_bytes);
        if !direct_state_fallback && !substrate_fallback {
            if let Some(payload) =
                self.dictionary_route_payload_for_bytes(source_kind, &runtime_material.exact_bytes)
            {
                self.store_runtime_object_for_item(
                    item_id,
                    &runtime_material.exact_bytes,
                    shared_protocol::ObjectKind::PredictiveObject,
                )?;
                self.record_route_outcome(
                    ControllerRouteFamily::Hybrid,
                    Some(source_kind),
                    None,
                    ctx.seq_no.0,
                    true,
                );
                return self.emit_predictive_route_or_inflation_fallback(
                    ctx, item_id, RecordType::PredictiveCorrect, payload,
                    &runtime_material.exact_bytes,
                    ControllerRouteFamily::Hybrid,
                    source_kind,
                    None,
                );
            }
        }
        let predictor = self.predictors.get(&item_id);
        let predictor_state = predictor
            .map(|entry| {
                shared_protocol::PredictorState::Ready(PredictorEntryMeta {
                    item_id,
                    source_kind: self
                        .source_bindings
                        .get(&item_id)
                        .map(|descriptor| descriptor.kind)
                        .unwrap_or(shared_protocol::SourceKind::Binary),
                    object_kind: shared_protocol::ObjectKind::ExactState,
                    cue: self
                        .source_bindings
                        .get(&item_id)
                        .map(|descriptor| {
                            descriptor.structural_cue_summary(&entry.exact_bytes()).cue
                        })
                        .unwrap_or_default(),
                })
            })
            .unwrap_or(shared_protocol::PredictorState::Empty);
        let runtime_object = self.chpmt_object_for_item(
            item_id,
            &runtime_material.exact_bytes,
            shared_protocol::ObjectKind::ExactState,
        );
        // P0-P5: Compress the exact_bytes before encoding into the record payload.
        // The server-side state (runtime_object, entries, predictors) keeps
        // uncompressed data for correct route planning and predictor comparison.
        // Only the wire payload is compressed.
        let compressed_bytes = self.compress_exact_bytes(source_kind, item_id, &runtime_material.exact_bytes);
        let encoded = encode_best_runtime_object_payload_with_preference(
            runtime_object.source_kind,
            runtime_object.object_kind,
            &compressed_bytes,
            // Use Empty predictor state for compressed payload encoding since
            // the predictor stores uncompressed bytes and residual computation
            // against compressed data would be incorrect.
            shared_protocol::PredictorState::Empty,
            None,
            DataPlaneCodecPreference::DirectExactOnly,
        )
        .map_err(ServerError::Codec)?;
        let mut flags = RecordFlags::empty();
        if direct_state_fallback {
            flags.insert(RecordFlags::DIRECT_STATE_FALLBACK);
        }
        if substrate_fallback {
            flags.insert(RecordFlags::SUBSTRATE_FALLBACK);
        }
        if transform_demoted_fallback {
            flags.insert(RecordFlags::TRANSFORM_DEMOTED_FALLBACK);
        }
        // S3.4: Set reason-specific fallback flag bits for observability.
        match fallback_reason {
            FallbackReason::TransformDemoted => {
                flags.insert(RecordFlags::TRANSFORM_DEMOTED_FALLBACK);
            }
            FallbackReason::DependencyUnavailable => {
                flags.insert(RecordFlags::DEPENDENCY_UNAVAILABLE_FALLBACK);
            }
            FallbackReason::SubstrateMiss => {
                flags.insert(RecordFlags::SUBSTRATE_FALLBACK);
            }
            FallbackReason::AssemblyStructuralizationFailed => {
                flags.insert(RecordFlags::ASSEMBLY_STRUCTURALIZATION_FALLBACK);
            }
            FallbackReason::SchemaDependencyInadmissible => {
                flags.insert(RecordFlags::SCHEMA_DEPENDENCY_INADMISSIBLE_FALLBACK);
            }
            FallbackReason::DictionaryRouteRejected
            | FallbackReason::NoViablePredictiveRoute
            | FallbackReason::PredictiveRouteInflation => {
                // These reasons are tracked in fallback_reasons metrics only,
                // no dedicated flag bit — they're subsumed under DIRECT_STATE_FALLBACK.
            }
            FallbackReason::NotAFallback => {
                // Not a fallback — normal direct-state emission with no predictive
                // route attempted. No fallback flag bits to set.
            }
        }
        let record = Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::ExactState,
                codec_mode: encoded.mode,
                flags: ctx.flags(flags),
                item_id,
                payload_len: encoded.payload.len() as u32,
            },
            payload: encoded.payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?;
        // P0-P5: Override the encoded runtime_object with the uncompressed version.
        // The encoded payload contains compressed bytes, but the server's internal
        // state (entries, predictors) must store uncompressed data for correct
        // route planning, cue computation, and predictor comparison.
        let entry = ServerEntry {
            object: runtime_object,
        };
        let cue = self
            .source_bindings
            .get(&item_id)
            .map(|descriptor| descriptor.structural_cue_summary(&entry.exact_bytes()).cue)
            .unwrap_or_default();
        let object_ref = EpisodeObjectRef {
            object_kind: shared_protocol::ObjectKind::ExactState,
            object_id: format!("item:{}", item_id.0),
        };
        let context_hash = derive_context_hash(cue, &object_ref);
        self.append_episode_activation_for_item(
            item_id,
            EpisodeActivationEvent {
                source_kind: self
                    .source_bindings
                    .get(&item_id)
                    .map(|descriptor| descriptor.kind)
                    .unwrap_or(shared_protocol::SourceKind::Binary),
                object_ref,
                cue,
                context_hash,
                tick: ctx.seq_no.0,
                success: true,
            },
        );
        let catalog_block = transient_exact_material(entry.object.source_kind, entry.exact_bytes());
        self.local_catalog
            .insert_exact_material(&catalog_block)
            .map_err(ServerError::Catalog)?;
        self.entries.insert(item_id, entry.clone());
        self.predictors.insert(item_id, entry);
        Ok(record)
    }

    /// Internal: emit a predictive route record using pre-encoded bytes.
    /// Called by emit_predictive_route_or_inflation_fallback after inflation
    /// check passes.
    fn emit_predictive_route_record_with_bytes(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        record_type: RecordType,
        payload: PredictiveRouteDispatchPayload,
        payload_bytes: Vec<u8>,
    ) -> Result<Record, ServerError> {
        // S1.1.v1: Debug assertion — confirmed peer-state trackers must NOT be
        // mutated during emission. Only pending trackers may be updated here.
        // If this assertion fires, it means confirmed state is being written
        // at emission time, which violates the pending/confirmed split.
        #[cfg(debug_assertions)]
        let pre_assembly_sync_len = self.assembly_sync_versions.len();
        #[cfg(debug_assertions)]
        let pre_schema_ids_len = self.peer_schema_versions.len();
        #[cfg(debug_assertions)]
        let pre_dict_versions_len = self.peer_dictionary_versions.len();
        #[cfg(debug_assertions)]
        let pre_transform_ids_len = self.peer_transform_versions.len();

        // S1.1/S1.2: Insert into PENDING trackers only at emission time.
        // Promotion to confirmed happens upon MemoryAck receipt.
        for assembly_def in &payload.inline_assembly_defs {
            self.pending_assembly_sync_versions.insert(
                assembly_def.assembly.assembly_id,
                shared_protocol::assembly_reuse_signature(&assembly_def.assembly),
            );
        }
        for schema_def in &payload.inline_schema_defs {
            self.pending_schema_versions.insert(schema_def.schema.schema_id, schema_def.schema.dependency_closure.version.object_revision);
        }
        for dictionary_def in &payload.inline_dictionaries {
            self.pending_dictionary_versions.insert(
                dictionary_def.dictionary.dictionary_id,
                u64::from(dictionary_def.dictionary.version.object_revision),
            );
        }
        // Build the record using the pre-encoded payload_bytes.
        // Protocol constraint: PredictiveConfirm/PredictiveCorrect records
        // use CodecMode::None per the wire validation rules. The record_type
        // itself distinguishes predictive from direct-state routes.
        //
        // P0: Compress the payload with Zstd. Predictive route payloads
        // contain inline assembly definitions, dependency closures, and
        // route graphs — all highly compressible structured data with
        // repeated keys and patterns. Zstd level 3 typically achieves
        // 50-70% compression on these payloads.
        let pre_compress_len = payload_bytes.len();
        let record = Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type,
                codec_mode: CodecMode::None,
                flags: ctx.flags(RecordFlags::empty()),
                item_id,
                payload_len: payload_bytes.len() as u32,
            },
            payload: payload_bytes,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .maybe_compress_zstd()
        .validate()
        .map_err(ServerError::Validation)?;

        // P0: Track compression metrics (before AEAD protection).
        let post_compress_len = record.payload.len();
        if record.header.flags.contains(RecordFlags::PAYLOAD_ZSTD)
            || record.header.flags.contains(RecordFlags::PAYLOAD_ZSTD_DICT)
        {
            self.compression_metrics.records_compressed += 1;
            self.compression_metrics.pre_compress_bytes += pre_compress_len;
            self.compression_metrics.post_compress_bytes += post_compress_len;
            self.compression_metrics.savings_bytes += pre_compress_len.saturating_sub(post_compress_len);
        } else {
            self.compression_metrics.records_skipped += 1;
        }

        // S1.1.v1: Post-emission assertion — confirmed trackers must not have grown.
        // This catches any code path that accidentally writes to confirmed state
        // during emission instead of pending state.
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(self.assembly_sync_versions.len(), pre_assembly_sync_len,
                "S1.1.v1: assembly_sync_versions mutated during emission — use pending_assembly_sync_versions");
            debug_assert_eq!(self.peer_schema_versions.len(), pre_schema_ids_len,
                "S1.1.v1: peer_schema_versions mutated during emission — use pending_schema_versions");
            debug_assert_eq!(self.peer_dictionary_versions.len(), pre_dict_versions_len,
                "S1.1.v1: peer_dictionary_versions mutated during emission — use pending_dictionary_versions");
            debug_assert_eq!(self.peer_transform_versions.len(), pre_transform_ids_len,
                "S1.1.v1: peer_transform_versions mutated during emission — use pending_transform_versions");
        }

        Ok(record)
    }

    /// S4.7 → S4.8: Emit a predictive route with amortization-aware inflation
    /// fallback. The previous binary check (payload > logical_content) prevented
    /// definitions from ever being installed, creating a chicken-and-egg problem:
    /// routes were rejected for inflation, but the definitions they carried
    /// (which caused the inflation) would have enabled future compact references.
    ///
    /// The amortization-aware check computes an **inflation budget** that scales
    /// with observed reuse patterns from route statistics:
    ///
    ///   budget = definition_investment_bytes * amortization_multiplier
    ///
    /// Where:
    ///   - definition_investment_bytes = estimated byte cost of inline definitions
    ///     (assembly_defs + schema_defs + dictionaries + episode_hints)
    ///   - amortization_multiplier = max(1.0, observed_successes / BREAK_EVEN_REUSE)
    ///     This scales the allowed inflation proportional to how many times the
    ///     route family has been observed to succeed (i.e., its definitions are
    ///     being reused). A family that has never succeeded gets multiplier 1.0
    ///     (allow inflation up to the definition cost itself — a single reuse
    ///     would break even). A family with 10 successes gets multiplier 3.3,
    ///     allowing more generous inflation since the definitions are clearly
    ///     being amortized across many records.
    ///
    /// The inflation is allowed only when:
    ///   actual_inflation <= definition_investment_bytes * amortization_multiplier
    ///
    /// For small payloads (< 256 bytes logical content), the inflation check is
    /// skipped entirely since the absolute overhead is bounded and definitions
    /// enable future prediction reuse regardless.
    fn emit_predictive_route_or_inflation_fallback(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        record_type: RecordType,
        payload: PredictiveRouteDispatchPayload,
        exact_bytes: &[u8],
        planned_family: ControllerRouteFamily,
        source_kind: shared_protocol::SourceKind,
        context_hash: Option<ContextHash>,
    ) -> Result<Record, ServerError> {
        // Pre-encode to check inflation before any state mutation.
        let payload_bytes = payload.encode().map_err(ServerError::StateProgram)?;
        let logical_content_len = payload.route_graph.output_len as usize;

        // Small payloads are always allowed — the absolute overhead is bounded
        // and the route establishes definitions for future reuse.
        const INFLATION_CHECK_MIN_CONTENT_BYTES: usize = 256;
        if logical_content_len >= INFLATION_CHECK_MIN_CONTENT_BYTES
            && payload_bytes.len() > logical_content_len
        {
            let actual_inflation = payload_bytes.len() - logical_content_len;

            // Estimate the byte cost of inline definitions (the "investment"
            // that will pay off in future records via compact references).
            let definition_investment_bytes = estimate_definition_investment_bytes(&payload);

            // Compute amortization multiplier from route statistics.
            // Routes that have succeeded in the past have installed definitions
            // that are being reused, so their inflation is amortized.
            let amortization_multiplier = self.compute_amortization_multiplier(
                planned_family,
                source_kind,
                context_hash,
            );

            let inflation_budget =
                (definition_investment_bytes as f64 * amortization_multiplier) as usize;

            if actual_inflation <= inflation_budget {
                // S4.8: Amortization-allowed inflation — the definition investment
                // is justified by observed reuse. Track this for observability.
                let key = format!(
                    "{:?}/amortization_allowed_inflation",
                    payload.route_family.route_family()
                );
                *self.fallback_metrics.fallback_reasons.entry(key).or_insert(0) += 1;
                // Fall through to emit the predictive route.
            } else {
                // Inflation exceeds even the amortization budget — genuinely
                // wasteful. Fall back to direct state.
                let key = format!(
                    "{:?}/predictive_route_inflation",
                    payload.route_family.route_family()
                );
                *self.fallback_metrics.fallback_reasons.entry(key).or_insert(0) += 1;
                // Record the downgrade for route statistics
                self.record_direct_state_downgrade(
                    planned_family,
                    source_kind,
                    context_hash,
                    ctx.seq_no.0,
                    FallbackReason::PredictiveRouteInflation,
                );
                // Emit direct state using the raw literal bytes
                let block = ExactStateMaterial {
                    source_kind,
                    exact_bytes: exact_bytes.to_vec(),
                };
                return self.emit_direct_state_fallback_data(
                    ctx,
                    item_id,
                    block,
                    planned_family,
                    source_kind,
                    context_hash,
                    FallbackReason::PredictiveRouteInflation,
                );
            }
        }
        // No inflation or amortization-allowed inflation — proceed with
        // normal predictive route emission. Pass already-encoded bytes.
        self.emit_predictive_route_record_with_bytes(
            ctx, item_id, record_type, payload, payload_bytes,
        )
    }

    /// Compute the amortization multiplier for a given route family based on
    /// observed success patterns in route statistics. The multiplier scales the
    /// allowed inflation budget proportional to evidence of reuse.
    ///
    /// Logic:
    ///   - Gather all route statistics for the same route_family (regardless of
    ///     source_kind/context_hash) to get a family-level reuse signal.
    ///   - multiplier = 1.0 + (total_successes / BREAK_EVEN_REUSE)
    ///   - Clamped to [1.0, MAX_AMORTIZATION_MULTIPLIER]
    ///
    /// A multiplier of 1.0 means "allow inflation up to the definition cost
    /// itself" (break-even after a single reuse). Higher multipliers mean
    /// the definitions are clearly being amortized across many records.
    fn compute_amortization_multiplier(
        &self,
        route_family: ControllerRouteFamily,
        source_kind: shared_protocol::SourceKind,
        context_hash: Option<ContextHash>,
    ) -> f64 {
        /// Number of observed successes required to increase the multiplier by 1.
        /// With BREAK_EVEN_REUSE = 3, 3 successes → multiplier 2.0, 6 → 3.0, etc.
        const BREAK_EVEN_REUSE: f64 = 3.0;
        /// Upper bound on the multiplier to prevent unbounded inflation.
        const MAX_AMORTIZATION_MULTIPLIER: f64 = 8.0;

        // First, look for an exact match (family + source_kind + context_hash).
        let exact_key = format!(
            "{}:{:?}:{:?}",
            route_family as u8,
            source_kind,
            context_hash.map(|v| v.0)
        );
        if let Some(stats) = self.route_statistics.get(&exact_key) {
            let successes = stats.success_count as f64;
            let multiplier = 1.0 + (successes / BREAK_EVEN_REUSE);
            return multiplier.min(MAX_AMORTIZATION_MULTIPLIER);
        }

        // No exact match — aggregate across all entries for this route family.
        let total_successes: f64 = self
            .route_statistics
            .values()
            .filter(|stats| stats.route_family == route_family)
            .map(|stats| stats.success_count as f64)
            .sum();

        let multiplier = 1.0 + (total_successes / BREAK_EVEN_REUSE);
        multiplier.min(MAX_AMORTIZATION_MULTIPLIER)
    }

    fn emit_plain_repair(
        &mut self,
        ctx: HeaderContext,
        payload: RepairPayload,
    ) -> Result<Record, ServerError> {
        let encoded = payload.encode().map_err(ServerError::Catalog)?;
        let record = Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::Repair,
                codec_mode: CodecMode::None,
                flags: ctx.flags(RecordFlags::empty()),
                item_id: ItemId(0),
                payload_len: encoded.len() as u32,
            },
            payload: encoded,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?;

        if let RepairPayload::Response { sync, .. } = payload {
            self.peer_catalog
                .apply_sync(sync)
                .map_err(ServerError::Catalog)?;
        }
        Ok(record)
    }

    fn apply_repair_record(
        &mut self,
        record: Record,
    ) -> Result<Option<RepairPayload>, ServerError> {
        match RepairPayload::decode(&record.payload).map_err(ServerError::Catalog)? {
            RepairPayload::Request { request, .. } => {
                let version = self.allocate_catalog_version();
                Ok(Some(
                    self.local_catalog
                        .repair_response(&request, version)
                        .map_err(ServerError::Catalog)?,
                ))
            }
            RepairPayload::Response { sync, .. } => {
                self.local_catalog
                    .apply_sync(sync)
                    .map_err(ServerError::Catalog)?;
                Ok(None)
            }
        }
    }

    fn allocate_catalog_version(&mut self) -> BlockCatalogVersion {
        self.next_catalog_version = self.next_catalog_version.saturating_add(1);
        BlockCatalogVersion(self.next_catalog_version)
    }

    fn emit_plain_control_data(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        op: CacheOp,
    ) -> Result<Record, ServerError> {
        let mut flags = RecordFlags::empty();
        match op {
            CacheOp::Evict => {
                flags.insert(RecordFlags::IS_EVICT);
                self.entries.remove(&item_id);
                self.predictors.remove(&item_id);
                self.source_bindings.remove(&item_id);
            }
            CacheOp::Invalidate => {
                flags.insert(RecordFlags::IS_INVALIDATE);
                self.entries.remove(&item_id);
                self.predictors.remove(&item_id);
                self.source_bindings.remove(&item_id);
            }
            CacheOp::Insert | CacheOp::UpsertObject => {
                unreachable!("vector ops are handled separately")
            }
        }

        Ok(Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::ExactState,
                codec_mode: CodecMode::None,
                flags: ctx.flags(flags),
                item_id,
                payload_len: 0,
            },
            payload: Vec::new(),
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?)
    }

    fn emit_plain_rekey(
        &mut self,
        ctx: HeaderContext,
        payload: RekeyPayload,
    ) -> Result<Record, ServerError> {
        Ok(Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::Rekey,
                codec_mode: CodecMode::None,
                flags: ctx.flags(RecordFlags::empty()),
                item_id: ItemId(0),
                payload_len: shared_protocol::REKEY_PAYLOAD_LEN as u32,
            },
            payload: payload.encode().to_vec(),
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?)
    }

    fn emit_plain_source_meta(
        &mut self,
        ctx: HeaderContext,
        item_id: ItemId,
        descriptor: SourceDescriptor,
        payload: SourceMetaPayload,
    ) -> Result<Record, ServerError> {
        self.source_bindings.insert(item_id, descriptor);
        let payload = payload.encode().map_err(ServerError::Source)?;
        Ok(Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::SourceMeta,
                codec_mode: CodecMode::None,
                flags: ctx.flags(RecordFlags::empty()),
                item_id,
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?)
    }

    fn emit_plain_resync(&mut self, ctx: HeaderContext) -> Result<Record, ServerError> {
        self.predictors.clear();
        self.peer_catalog = SharedBlockCatalog::default();
        self.assembly_sync_versions.clear();
        self.peer_dictionary_versions.clear();
        self.peer_schema_versions.clear();
        self.peer_transform_versions.clear();
        // S1.2/S3.3: Clear pending peer state on resync too
        self.clear_pending_peer_state();
        Ok(Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::Resync,
                codec_mode: CodecMode::None,
                flags: ctx.flags(RecordFlags::empty()),
                item_id: ItemId(0),
                payload_len: 0,
            },
            payload: Vec::new(),
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?)
    }

    fn emit_plain_close(
        &mut self,
        ctx: HeaderContext,
        close_code: Option<u16>,
    ) -> Result<Record, ServerError> {
        let payload = close_code
            .map(|code| code.to_le_bytes().to_vec())
            .unwrap_or_default();
        Ok(Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: ctx.stream_id,
                epoch_id: ctx.epoch_id,
                seq_no: ctx.seq_no,
                record_type: RecordType::Close,
                codec_mode: CodecMode::None,
                flags: ctx.flags(RecordFlags::empty()),
                item_id: ItemId(0),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ServerError::Validation)?)
    }
}

impl ServerSession {
    pub fn new(protector: StreamProtector) -> Self {
        Self::new_with_configs(protector, SourceOptimizationConfig::default())
    }

    pub fn new_with_transport_session(
        protector: StreamProtector,
        session: TransportSessionConfig,
    ) -> Self {
        Self::new_with_transport_and_optimizations(
            protector,
            session,
            SourceOptimizationConfig::default(),
        )
    }

    pub fn new_with_transport_and_optimizations(
        protector: StreamProtector,
        session: TransportSessionConfig,
        mut optimizations: SourceOptimizationConfig,
    ) -> Self {
        optimizations.data_plane_codec = session.data_plane_codec;
        Self::new_with_configs(protector, optimizations)
    }

    pub fn new_with_configs(
        protector: StreamProtector,
        optimizations: SourceOptimizationConfig,
    ) -> Self {
        Self {
            state: ServerState::default(),
            protector,
            optimizations,
        }
    }

    pub fn state(&self) -> &ServerState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut ServerState {
        &mut self.state
    }

    pub fn protector(&self) -> &StreamProtector {
        &self.protector
    }

    pub fn emit_event(&mut self, event: ServerEvent) -> Result<Record, ServerError> {
        let plain =
            self.state
                .emit_plain_event(self.header_context(), event, self.optimizations)?;
        self.protector
            .protect_record(plain)
            .map_err(ServerError::Protection)
    }

    pub fn emit_trace<I>(&mut self, events: I) -> Result<Vec<Record>, ServerError>
    where
        I: IntoIterator<Item = ServerEvent>,
    {
        events
            .into_iter()
            .map(|event| self.emit_event(event))
            .collect()
    }

    pub fn define_catalog_bundle(
        &mut self,
        members: Vec<BundleMember>,
    ) -> Result<BundleId, ServerError> {
        self.state
            .define_catalog_bundle(members)
            .map_err(ServerError::Catalog)
    }

    pub fn emit_repair_response(&mut self, request: RepairRequest) -> Result<Record, ServerError> {
        let version = self.state.allocate_catalog_version();
        let payload = self
            .state
            .local_catalog
            .repair_response(&request, version)
            .map_err(ServerError::Catalog)?;
        let plain = self
            .state
            .emit_plain_repair(self.header_context(), payload)?;
        self.protector
            .protect_record(plain)
            .map_err(ServerError::Protection)
    }

    pub fn apply_peer_record(&mut self, record: Record) -> Result<Option<Record>, ServerError> {
        let record = self
            .protector
            .unprotect_record(record)
            .map_err(ServerError::Protection)?;
        match record.header.record_type {
            RecordType::Repair => {
                let response = self.state.apply_repair_record(record)?;
                match response {
                    Some(payload) => {
                        let plain = self
                            .state
                            .emit_plain_repair(self.header_context(), payload)?;
                        self.protector
                            .protect_record(plain)
                            .map(Some)
                            .map_err(ServerError::Protection)
                    }
                    None => Ok(None),
                }
            }
            RecordType::MemoryAck => {
                self.state.apply_memory_ack_record(record)?;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub fn emit_source_insert(
        &mut self,
        request: SourceIngestRequest<'_>,
        cache: &SourceCache,
    ) -> Result<SourceEmitResult, ServerError> {
        let resolved = cache
            .resolve_source(request.source)
            .map_err(ServerError::SourceCache)?;
        self.emit_resolved_source_insert(request.item_id, resolved)
    }

    pub fn emit_resolved_source_insert(
        &mut self,
        item_id: ItemId,
        resolved: SourceResolveResult,
    ) -> Result<SourceEmitResult, ServerError> {
        self.emit_resolved_source_event(item_id, resolved, true)
    }

    pub fn emit_resolved_source_upsert(
        &mut self,
        item_id: ItemId,
        resolved: SourceResolveResult,
    ) -> Result<SourceEmitResult, ServerError> {
        self.emit_resolved_source_event(item_id, resolved, false)
    }

    pub fn emit_rekey<R>(&mut self, rng: &mut R) -> Result<(Record, RekeyPayload), ServerError>
    where
        R: CryptoRng + RngCore + ?Sized,
    {
        let payload = self.protector.make_rekey_payload(rng);
        let plain = self
            .state
            .emit_plain_rekey(self.header_context(), payload)?;
        let protected = self
            .protector
            .protect_record(plain)
            .map_err(ServerError::Protection)?;
        Ok((protected, payload))
    }

    pub fn emit_resync(&mut self) -> Result<Record, ServerError> {
        let plain = self.state.emit_plain_resync(self.header_context())?;
        self.protector
            .protect_record(plain)
            .map_err(ServerError::Protection)
    }

    pub fn emit_pending_control_plane_records(
        &mut self,
        max_records: usize,
    ) -> Result<Vec<Record>, ServerError> {
        let plain = self
            .state
            .emit_pending_control_plane_records(self.header_context(), max_records)?;
        plain
            .into_iter()
            .map(|record| {
                self.protector
                    .protect_record(record)
                    .map_err(ServerError::Protection)
            })
            .collect()
    }

    pub fn emit_close(&mut self, close_code: Option<u16>) -> Result<Record, ServerError> {
        let plain = self
            .state
            .emit_plain_close(self.header_context(), close_code)?;
        self.protector
            .protect_record(plain)
            .map_err(ServerError::Protection)
    }

    fn header_context(&self) -> HeaderContext {
        HeaderContext {
            stream_id: self.protector.stream_id(),
            epoch_id: self.protector.expected_epoch_id(),
            seq_no: self.protector.expected_seq_no(),
            post_rekey_confirm: self.protector.post_rekey_confirm_required(),
        }
    }

    fn emit_resolved_source_event(
        &mut self,
        item_id: ItemId,
        resolved: SourceResolveResult,
        is_insert: bool,
    ) -> Result<SourceEmitResult, ServerError> {
        let mut records = Vec::new();
        let mut metrics = SourceEmitMetrics {
            source_meta_baseline_bytes: metadata_baseline_bytes(&resolved.descriptor),
            ..SourceEmitMetrics::default()
        };

        if self.optimizations.inline_source_meta_enabled
            && self
                .state
                .source_binding_changed(item_id, &resolved.descriptor)
        {
            let payload = build_source_meta_payload(&resolved.descriptor);
            let plain = self.state.emit_plain_source_meta(
                self.header_context(),
                item_id,
                resolved.descriptor.clone(),
                payload,
            )?;
            let protected = self
                .protector
                .protect_record(plain)
                .map_err(ServerError::Protection)?;
            metrics.source_meta_emitted = true;
            metrics.source_meta_payload_bytes += protected.payload.len() as u64;
            records.push(protected);
        }

        let candidates = self.state.bounded_context_predictions();
        if !candidates.is_empty() {
            let cue = resolved
                .descriptor
                .structural_cue_summary(&resolved.object.exact_bytes)
                .cue;
            let context_hash = derive_context_hash(
                cue,
                &EpisodeObjectRef {
                    object_kind: shared_protocol::ObjectKind::ExactState,
                    object_id: format!("item:{}", item_id.0),
                },
            );
            let payload = shared_protocol::EpisodeHintPayload::new(
                context_hash,
                LagBucket(0),
                shared_protocol::PrecisionBand::Balanced,
                episode_hint_dependencies(&candidates),
                candidates,
            );
            let plain_hint =
                self.state
                    .emit_episode_hint_record(self.header_context(), item_id, payload)?;
            let protected_hint = self
                .protector
                .protect_record(plain_hint)
                .map_err(ServerError::Protection)?;
            records.push(protected_hint);
        }

        let event = if is_insert {
            ServerEvent::Insert {
                item_id,
                block: transient_exact_material(
                    resolved.object.source_kind,
                    &resolved.object.exact_bytes,
                ),
            }
        } else {
            ServerEvent::UpsertObject {
                item_id,
                block: transient_exact_material(
                    resolved.object.source_kind,
                    &resolved.object.exact_bytes,
                ),
            }
        };
        records.push(self.emit_event(event)?);
        Ok(SourceEmitResult {
            records,
            resolved,
            metrics,
        })
    }
}

impl PlainServerSession {
    pub fn new(stream_id: StreamId) -> Self {
        Self::new_with_configs(stream_id, SourceOptimizationConfig::default())
    }

    pub fn new_with_transport_session(
        stream_id: StreamId,
        session: TransportSessionConfig,
    ) -> Self {
        Self::new_with_transport_and_optimizations(
            stream_id,
            session,
            SourceOptimizationConfig::default(),
        )
    }

    pub fn new_with_transport_and_optimizations(
        stream_id: StreamId,
        session: TransportSessionConfig,
        mut optimizations: SourceOptimizationConfig,
    ) -> Self {
        optimizations.data_plane_codec = session.data_plane_codec;
        Self::new_with_configs(stream_id, optimizations)
    }

    pub fn new_with_configs(stream_id: StreamId, optimizations: SourceOptimizationConfig) -> Self {
        Self {
            state: ServerState::default(),
            stream_id,
            epoch_id: EpochId(0),
            next_seq_no: SeqNo(0),
            pending_post_rekey_confirm: false,
            optimizations,
        }
    }

    pub fn state(&self) -> &ServerState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut ServerState {
        &mut self.state
    }

    pub fn emit_event(&mut self, event: ServerEvent) -> Result<Record, ServerError> {
        let record =
            self.state
                .emit_plain_event(self.header_context(), event, self.optimizations)?;
        self.advance_after_emit(&record);
        Ok(record)
    }

    pub fn emit_trace<I>(&mut self, events: I) -> Result<Vec<Record>, ServerError>
    where
        I: IntoIterator<Item = ServerEvent>,
    {
        events
            .into_iter()
            .map(|event| self.emit_event(event))
            .collect()
    }

    pub fn define_catalog_bundle(
        &mut self,
        members: Vec<BundleMember>,
    ) -> Result<BundleId, ServerError> {
        self.state
            .define_catalog_bundle(members)
            .map_err(ServerError::Catalog)
    }

    pub fn emit_repair_response(&mut self, request: RepairRequest) -> Result<Record, ServerError> {
        let version = self.state.allocate_catalog_version();
        let payload = self
            .state
            .local_catalog
            .repair_response(&request, version)
            .map_err(ServerError::Catalog)?;
        let record = self
            .state
            .emit_plain_repair(self.header_context(), payload)?;
        self.advance_after_emit(&record);
        Ok(record)
    }

    pub fn apply_peer_record(&mut self, record: Record) -> Result<Option<Record>, ServerError> {
        match record.header.record_type {
            RecordType::Repair => {
                let response = self.state.apply_repair_record(record)?;
                match response {
                    Some(payload) => {
                        let record = self
                            .state
                            .emit_plain_repair(self.header_context(), payload)?;
                        self.advance_after_emit(&record);
                        Ok(Some(record))
                    }
                    None => Ok(None),
                }
            }
            RecordType::MemoryAck => {
                self.state.apply_memory_ack_record(record)?;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub fn emit_source_insert(
        &mut self,
        request: SourceIngestRequest<'_>,
        cache: &SourceCache,
    ) -> Result<SourceEmitResult, ServerError> {
        let resolved = cache
            .resolve_source(request.source)
            .map_err(ServerError::SourceCache)?;
        self.emit_resolved_source_insert(request.item_id, resolved)
    }

    pub fn emit_resolved_source_insert(
        &mut self,
        item_id: ItemId,
        resolved: SourceResolveResult,
    ) -> Result<SourceEmitResult, ServerError> {
        self.emit_resolved_source_event(item_id, resolved, true)
    }

    pub fn emit_resolved_source_upsert(
        &mut self,
        item_id: ItemId,
        resolved: SourceResolveResult,
    ) -> Result<SourceEmitResult, ServerError> {
        self.emit_resolved_source_event(item_id, resolved, false)
    }

    pub fn emit_rekey<R>(&mut self, rng: &mut R) -> Result<(Record, RekeyPayload), ServerError>
    where
        R: CryptoRng + RngCore + ?Sized,
    {
        let mut rekey_seed = [0_u8; shared_protocol::REKEY_SEED_LEN];
        rng.fill_bytes(&mut rekey_seed);
        let payload = RekeyPayload {
            next_epoch_id: EpochId(self.epoch_id.0 + 1),
            rekey_seed,
        };
        let record = self
            .state
            .emit_plain_rekey(self.header_context(), payload)?;
        self.next_seq_no = SeqNo(self.next_seq_no.0 + 1);
        self.epoch_id = payload.next_epoch_id;
        self.next_seq_no = SeqNo(0);
        self.pending_post_rekey_confirm = true;
        Ok((record, payload))
    }

    pub fn emit_resync(&mut self) -> Result<Record, ServerError> {
        let record = self.state.emit_plain_resync(self.header_context())?;
        self.advance_after_emit(&record);
        Ok(record)
    }

    pub fn emit_pending_control_plane_records(
        &mut self,
        max_records: usize,
    ) -> Result<Vec<Record>, ServerError> {
        let records = self
            .state
            .emit_pending_control_plane_records(self.header_context(), max_records)?;
        for record in &records {
            self.advance_after_emit(record);
        }
        Ok(records)
    }

    pub fn emit_close(&mut self, close_code: Option<u16>) -> Result<Record, ServerError> {
        let record = self
            .state
            .emit_plain_close(self.header_context(), close_code)?;
        self.advance_after_emit(&record);
        Ok(record)
    }

    fn advance_after_emit(&mut self, _record: &Record) {
        self.next_seq_no = SeqNo(self.next_seq_no.0 + 1);
        if self.pending_post_rekey_confirm {
            self.pending_post_rekey_confirm = false;
        }
    }

    fn header_context(&self) -> HeaderContext {
        HeaderContext {
            stream_id: self.stream_id,
            epoch_id: self.epoch_id,
            seq_no: self.next_seq_no,
            post_rekey_confirm: self.pending_post_rekey_confirm,
        }
    }

    fn emit_resolved_source_event(
        &mut self,
        item_id: ItemId,
        resolved: SourceResolveResult,
        is_insert: bool,
    ) -> Result<SourceEmitResult, ServerError> {
        let mut records = Vec::new();
        let mut metrics = SourceEmitMetrics {
            source_meta_baseline_bytes: metadata_baseline_bytes(&resolved.descriptor),
            ..SourceEmitMetrics::default()
        };

        if self.optimizations.inline_source_meta_enabled
            && self
                .state
                .source_binding_changed(item_id, &resolved.descriptor)
        {
            let payload = build_source_meta_payload(&resolved.descriptor);
            let record = self.state.emit_plain_source_meta(
                self.header_context(),
                item_id,
                resolved.descriptor.clone(),
                payload,
            )?;
            self.advance_after_emit(&record);
            metrics.source_meta_emitted = true;
            metrics.source_meta_payload_bytes += record.payload.len() as u64;
            records.push(record);
        }

        let candidates = self.state.bounded_context_predictions();
        if !candidates.is_empty() {
            let cue = resolved
                .descriptor
                .structural_cue_summary(&resolved.object.exact_bytes)
                .cue;
            let context_hash = derive_context_hash(
                cue,
                &EpisodeObjectRef {
                    object_kind: shared_protocol::ObjectKind::ExactState,
                    object_id: format!("item:{}", item_id.0),
                },
            );
            let payload = shared_protocol::EpisodeHintPayload::new(
                context_hash,
                LagBucket(0),
                shared_protocol::PrecisionBand::Balanced,
                episode_hint_dependencies(&candidates),
                candidates,
            );
            let record =
                self.state
                    .emit_episode_hint_record(self.header_context(), item_id, payload)?;
            self.advance_after_emit(&record);
            records.push(record);
        }

        let event = if is_insert {
            ServerEvent::Insert {
                item_id,
                block: transient_exact_material(
                    resolved.object.source_kind,
                    &resolved.object.exact_bytes,
                ),
            }
        } else {
            ServerEvent::UpsertObject {
                item_id,
                block: transient_exact_material(
                    resolved.object.source_kind,
                    &resolved.object.exact_bytes,
                ),
            }
        };
        records.push(self.emit_event(event)?);
        Ok(SourceEmitResult {
            records,
            resolved,
            metrics,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error(transparent)]
    Validation(ValidationError),
    #[error(transparent)]
    Codec(shared_protocol::CodecError),
    #[error(transparent)]
    StateProgram(shared_protocol::StateProgramError),
    #[error(transparent)]
    Catalog(shared_protocol::CatalogError),
    #[error("remote peer is missing state basis: {0:?}")]
    RemoteBasisMissing(RepairRequest),
    #[error(transparent)]
    Source(shared_protocol::SourceError),
    #[error(transparent)]
    SourceCache(#[from] SourceCacheError),
    #[error(transparent)]
    Protection(shared_protocol::ProtectionError),
    #[error(transparent)]
    ValidationPayload(shared_protocol::WireError),
    #[error("entry not found for item {0:?}")]
    EntryNotFound(ItemId),
    #[error("predictive route payload inflation: encoded {encoded_bytes} bytes exceeds logical content {logical_content_bytes} bytes for family {route_family:?}")]
    PredictiveRouteInflation {
        encoded_bytes: usize,
        logical_content_bytes: usize,
        route_family: shared_protocol::RouteFamily,
    },
}

#[cfg(test)]
mod tests {
    use client::{ClientApplyError, ClientSession};
    use rand::{SeedableRng, rngs::StdRng};
    use shared_protocol::{
        CodecMode, Record, SourceKind, StreamDirection, StreamState, classic_ref1_pair_from_rng,
    };
    use tokio::net::TcpListener;

    use super::*;
    use crate::source_cache::{SourceCache, SourceCacheConfig, prepare_text_ingest_source};

    fn sample_block(seed: f32) -> ExactStateMaterial {
        let mut bytes = Vec::with_capacity(256);
        for index in 0..256 {
            let value = (((index as f32 + 1.0) * seed).cos() * 127.0).round() as i16;
            bytes.push((value & 0xff) as u8);
        }
        ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, &bytes)
    }

    async fn round_trip_records(records: Vec<Record>) -> Vec<Record> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task =
            tokio::spawn(async move { transport::serve_records_once(listener, records).await });
        let url = format!("ws://{}", addr);
        let received = transport::read_records_once(&url).await.unwrap();
        server_task.await.unwrap().unwrap();
        received
    }

    async fn round_trip_frames(frames: Vec<Vec<u8>>) -> Vec<Record> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task =
            tokio::spawn(
                async move { transport::serve_binary_frames_once(listener, frames).await },
            );
        let url = format!("ws://{}", addr);
        let received = transport::read_records_once(&url).await.unwrap();
        server_task.await.unwrap().unwrap();
        received
    }

    #[test]
    fn episode_completion_route_emits_predictive_confirm() {
        let mut state = ServerState::default();
        let record = state
            .emit_episode_completion_route(
                HeaderContext {
                    stream_id: StreamId(1),
                    epoch_id: EpochId(0),
                    seq_no: SeqNo(3),
                    post_rekey_confirm: false,
                },
                ItemId(9),
                shared_protocol::SourceKind::Text,
                shared_protocol::derive_sparse_cue(
                    shared_protocol::SourceKind::Text,
                    b"episode-route",
                ),
                ContextHash(99),
                b"episode-route".len() as u32,
                shared_protocol::EpisodeCompletionCandidate {
                    object_ref: EpisodeObjectRef {
                        object_kind: shared_protocol::ObjectKind::ExactState,
                        object_id: "item:9".to_string(),
                    },
                    transition_count: shared_protocol::TransitionCount(5),
                    branch_rank: shared_protocol::BranchRank(0),
                    precision_band: shared_protocol::PrecisionBand::Tight,
                    cue_overlap: 12,
                    recency_score: 32,
                    route_support: 16,
                    transition_match: 24,
                    lag_bucket: shared_protocol::LagBucket(0),
                    admissible: true,
                },
            )
            .unwrap();
        assert_eq!(record.header.record_type, RecordType::PredictiveConfirm);
        let payload = PredictiveRouteDispatchPayload::decode(
            &record.maybe_decompress_zstd().unwrap().payload
        ).unwrap();
        assert_eq!(
            payload.route_family,
            ControllerRouteFamily::EpisodeCompletion
        );
        assert_eq!(payload.inline_episode_hints.len(), 1);
        assert!(payload.prg.is_some());
    }

    #[test]
    fn episode_activation_queues_replay_until_explicit_learning_pass() {
        let mut state = ServerState::default();
        state.append_episode_activation(EpisodeActivationEvent {
            source_kind: shared_protocol::SourceKind::Text,
            object_ref: EpisodeObjectRef {
                object_kind: shared_protocol::ObjectKind::ExactState,
                object_id: "item:1".to_string(),
            },
            cue: shared_protocol::derive_sparse_cue(shared_protocol::SourceKind::Text, b"seed"),
            context_hash: ContextHash(7),
            tick: 1,
            success: true,
        });
        assert_eq!(state.pending_replay_queue().len(), 1);
        state.schedule_consolidation_from_replay();
        assert!(state.pending_replay_queue().is_empty());
    }

    #[test]
    fn source_insert_emits_episode_hint_when_predictions_exist() {
        let mut state = ServerState::default();
        // Set up a peer catalog with a known block so that an episode
        // candidate referencing that block passes the admissibility filter.
        let shared_block =
            ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, b"episode-block");
        let block_id = state
            .peer_catalog
            .insert_exact_material(&shared_block)
            .unwrap();
        state.append_episode_activation(EpisodeActivationEvent {
            source_kind: shared_protocol::SourceKind::Text,
            object_ref: EpisodeObjectRef {
                object_kind: shared_protocol::ObjectKind::ExactBlock,
                object_id: format!("block:{}", block_id.0),
            },
            cue: shared_protocol::derive_sparse_cue(shared_protocol::SourceKind::Text, b"seed"),
            context_hash: ContextHash(7),
            tick: 1,
            success: true,
        });
        let mut rng = StdRng::seed_from_u64(33);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(7), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        session.state = state;
        let cache = SourceCache::new(SourceCacheConfig::default()).unwrap();
        let resolved = cache
            .resolve_source(&prepare_text_ingest_source(
                "episode hint transport",
                Some("episode.txt".to_string()),
            ))
            .unwrap();
        let emitted = session
            .emit_resolved_source_insert(ItemId(2), resolved)
            .unwrap();
        assert!(
            emitted
                .records
                .iter()
                .any(|record| record.header.record_type == RecordType::EpisodeHint)
        );
    }

    #[test]
    fn first_seen_items_emit_non_delta() {
        let mut rng = StdRng::seed_from_u64(21);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(7), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        let record = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: sample_block(0.01),
            })
            .unwrap();
        match record.header.record_type {
            RecordType::ExactState => {
                assert!(matches!(
                    record.header.codec_mode,
                    CodecMode::PackedExact | CodecMode::DirectExact
                ));
            }
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                assert_eq!(record.header.codec_mode, CodecMode::None);
            }
            other => panic!("unexpected first-seen record type: {:?}", other),
        }
    }

    #[test]
    fn repeated_items_can_emit_latent_delta() {
        let mut rng = StdRng::seed_from_u64(22);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(7), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: sample_block(0.01),
            })
            .unwrap();
        let record = session
            .emit_event(ServerEvent::UpsertObject {
                item_id: ItemId(1),
                block: sample_block(0.0101),
            })
            .unwrap();
        match record.header.record_type {
            RecordType::ExactState => {
                assert!(matches!(
                    record.header.codec_mode,
                    CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
                ));
            }
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                assert_eq!(record.header.codec_mode, CodecMode::None);
            }
            other => panic!("unexpected repeated-item record type: {:?}", other),
        }
    }

    #[test]
    fn direct_exact_only_mode_disables_latent_emission() {
        let mut rng = StdRng::seed_from_u64(2201);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(7), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new_with_configs(
            sender,
            SourceOptimizationConfig {
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::DirectExactOnly,
                ..SourceOptimizationConfig::default()
            },
        );
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: sample_block(0.01),
            })
            .unwrap();
        let record = session
            .emit_event(ServerEvent::UpsertObject {
                item_id: ItemId(1),
                block: sample_block(0.0101),
            })
            .unwrap();
        match record.header.record_type {
            RecordType::ExactState => {
                assert_eq!(record.header.codec_mode, CodecMode::DirectExact);
            }
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                assert_eq!(record.header.codec_mode, CodecMode::None);
            }
            other => panic!("unexpected copy-exact-only record type: {:?}", other),
        }
    }

    #[test]
    fn state_first_policy_chooses_bundle_over_exact_copy_when_peer_has_basis() {
        let mut session = PlainServerSession::new(StreamId(7));
        let left_bytes = b"left basis left basis left basis left basis ".to_vec();
        let right_bytes = b"right basis right basis right basis right basis".to_vec();
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: ExactStateMaterial::copy_exact(
                    shared_protocol::SourceKind::Text,
                    &left_bytes,
                ),
            })
            .unwrap();
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(2),
                block: ExactStateMaterial::copy_exact(
                    shared_protocol::SourceKind::Text,
                    &right_bytes,
                ),
            })
            .unwrap();
        let left_id = session
            .state()
            .catalog_block_id_for_item(ItemId(1))
            .unwrap();
        let right_id = session
            .state()
            .catalog_block_id_for_item(ItemId(2))
            .unwrap();
        let bundle = session
            .define_catalog_bundle(vec![
                BundleMember::block(left_id, 0, left_bytes.len() as u32),
                BundleMember::block(right_id, 0, right_bytes.len() as u32),
            ])
            .unwrap();
        let sync = session
            .state()
            .local_catalog()
            .sync_payload(BlockCatalogVersion(1), &[], &[bundle])
            .unwrap();
        session.state_mut().peer_catalog.apply_sync(sync).unwrap();

        let mut exact = left_bytes;
        exact.extend_from_slice(&right_bytes);
        let record = session
            .emit_event(ServerEvent::UpsertObject {
                item_id: ItemId(3),
                block: ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, &exact),
            })
            .unwrap();

        assert!(matches!(
            record.header.record_type,
            RecordType::PredictiveConfirm
        ));
        assert_eq!(record.header.codec_mode, CodecMode::None);
        assert_eq!(
            session
                .state()
                .cache_entry(ItemId(3))
                .unwrap()
                .object
                .exact_bytes,
            exact
        );
    }

    #[test]
    fn server_repair_response_provides_requested_block_material() {
        let mut session = PlainServerSession::new(StreamId(7));
        let basis = ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, b"basis");
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: basis,
            })
            .unwrap();
        let block_id = session
            .state()
            .catalog_block_id_for_item(ItemId(1))
            .unwrap();
        let request =
            RepairRequest::missing_basis(BlockCatalogVersion(0), vec![block_id], Vec::new());
        let repair = session.emit_repair_response(request).unwrap();
        assert_eq!(repair.header.record_type, RecordType::Repair);

        match RepairPayload::decode(&repair.payload).unwrap() {
            RepairPayload::Response { sync, .. } => {
                assert_eq!(sync.blocks.len(), 1);
                assert_eq!(sync.blocks[0].block_id, block_id);
                assert_eq!(sync.blocks[0].material, b"basis");
            }
            RepairPayload::Request { .. } => panic!("expected repair response"),
        }
    }

    #[test]
    fn assembly_route_structuralizes_over_peer_catalog_and_then_reuses_sync() {
        let mut state = ServerState::default();
        let shared_block =
            ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, b"shared-block");
        state
            .peer_catalog
            .insert_exact_material(&shared_block)
            .unwrap();
        let item_id = ItemId(7);
        let source_kind = shared_protocol::SourceKind::Text;
        state.source_bindings.insert(
            item_id,
            SourceDescriptor {
                kind: source_kind,
                source_hash: shared_protocol::compute_source_hash(
                    source_kind,
                    None,
                    b"prefix shared-block suffix",
                ),
                byte_len: b"prefix shared-block suffix".len(),
                mime: None,
                label: None,
            },
        );
        let candidate = shared_protocol::extract_catalog_assembly_candidates(
            source_kind,
            b"prefix shared-block suffix",
            shared_protocol::AssemblyExtractionConfig::bounded_default(),
        )
        .into_iter()
        .next()
        .unwrap();
        let ctx = HeaderContext::new(StreamId(1), EpochId(0), SeqNo(0));
        let record = state
            .emit_assembly_route(
                ctx,
                item_id,
                source_kind,
                candidate.clone(),
                b"prefix shared-block suffix",
            )
            .unwrap();
        let payload =
            shared_protocol::PredictiveRouteDispatchPayload::decode(
                &record.maybe_decompress_zstd().unwrap().payload
            ).unwrap();
        assert_eq!(payload.route_family, ControllerRouteFamily::Assembly);
        assert!(payload.installs_assembly_defs());
        assert_eq!(
            payload.assembly_mode,
            Some(shared_protocol::AssemblyRouteMode::DefineAndActivate)
        );
        let installed = payload.inline_assembly_defs.first().unwrap();
        assert!(installed.assembly.body.is_structural());
        assert!(installed.assembly.body.nodes.iter().any(|node| matches!(
            node,
            shared_protocol::AssemblyBodyNode::SubstrateSpan {
                reference: shared_protocol::AssemblyComponentRef::ExactBlock { .. }
            } | shared_protocol::AssemblyBodyNode::SubstrateSpan {
                reference: shared_protocol::AssemblyComponentRef::ExactBundle { .. }
            } | shared_protocol::AssemblyBodyNode::MotifLink { .. }
                | shared_protocol::AssemblyBodyNode::SlotPlaceholder { .. }
        )));

        // Simulate MemoryAck receipt to promote pending → confirmed,
        // so the second emission recognizes the assembly as already synced.
        let assembly_id = installed.assembly.assembly_id;
        state.promote_pending_to_confirmed(&shared_protocol::MemoryAckPayload {
            version: shared_protocol::MemoryAckPayload::VERSION,
            plane: shared_protocol::MemoryPlane::Assembly,
            object_kind: shared_protocol::ObjectKind::Assembly,
            object_id: format!("assembly:{}", assembly_id.0),
            acked_record_type: shared_protocol::RecordType::PredictiveConfirm,
            acked_seq_no: shared_protocol::SeqNo(0),
        });
        let ctx = HeaderContext::new(StreamId(1), EpochId(0), SeqNo(1));
        let record = state
            .emit_assembly_route(
                ctx,
                item_id,
                source_kind,
                candidate,
                b"prefix shared-block suffix",
            )
            .unwrap();
        let payload =
            shared_protocol::PredictiveRouteDispatchPayload::decode(
                &record.maybe_decompress_zstd().unwrap().payload
            ).unwrap();
        assert!(payload.reuses_synchronized_assembly());
        assert!(payload.inline_assembly_defs.is_empty());
        assert_eq!(
            payload.assembly_mode,
            Some(shared_protocol::AssemblyRouteMode::ReuseReference)
        );
        assert!(payload.hybrid_route.is_none());
    }

    #[test]
    fn assembly_route_uses_partial_catalog_ranges_for_structural_components() {
        let mut state = ServerState::default();
        let shared_block =
            ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, b"xxHEADERyyBODYzz");
        state
            .peer_catalog
            .insert_exact_material(&shared_block)
            .unwrap();
        let item_id = ItemId(17);
        let source_kind = shared_protocol::SourceKind::Text;
        let bytes = b"HEADER--BODY";
        state.source_bindings.insert(
            item_id,
            SourceDescriptor {
                kind: source_kind,
                source_hash: shared_protocol::compute_source_hash(source_kind, None, bytes),
                byte_len: bytes.len(),
                mime: None,
                label: None,
            },
        );
        let candidate = shared_protocol::extract_catalog_assembly_candidates(
            source_kind,
            bytes,
            shared_protocol::AssemblyExtractionConfig::bounded_default(),
        )
        .into_iter()
        .next()
        .unwrap();
        let record = state
            .emit_assembly_route(
                HeaderContext::new(StreamId(2), EpochId(0), SeqNo(0)),
                item_id,
                source_kind,
                candidate,
                bytes,
            )
            .unwrap();
        let payload =
            shared_protocol::PredictiveRouteDispatchPayload::decode(
                &record.maybe_decompress_zstd().unwrap().payload
            ).unwrap();
        let installed = payload.inline_assembly_defs.first().unwrap();
        assert_eq!(
            payload.assembly_mode,
            Some(shared_protocol::AssemblyRouteMode::DefineAndActivate)
        );
        assert!(installed.assembly.body.structural_component_count() >= 1);
        assert!(installed.assembly.body.nodes.iter().all(|node| matches!(node, shared_protocol::AssemblyBodyNode::SubstrateSpan { reference: shared_protocol::AssemblyComponentRef::ExactBlock { len, .. } } if *len >= 4) || matches!(node, shared_protocol::AssemblyBodyNode::DelimiterAnchor { .. }) || matches!(node, shared_protocol::AssemblyBodyNode::LiteralIsland { .. }) || matches!(node, shared_protocol::AssemblyBodyNode::SlotPlaceholder { .. }) || matches!(node, shared_protocol::AssemblyBodyNode::MotifLink { .. }) || matches!(node, shared_protocol::AssemblyBodyNode::TypedBoundary { .. })));
        assert!(installed.assembly.body.literal_len() < installed.assembly.body.output_len());
    }

    #[test]
    fn evict_records_clear_cache_and_predictor_state() {
        let mut rng = StdRng::seed_from_u64(23);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(7), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: sample_block(0.01),
            })
            .unwrap();
        let record = session
            .emit_event(ServerEvent::Evict { item_id: ItemId(1) })
            .unwrap();
        assert!(record.header.flags.contains(RecordFlags::IS_EVICT));
        assert!(session.state().cache_entry(ItemId(1)).is_none());
        assert!(session.state().predictor_entry(ItemId(1)).is_none());
    }

    #[test]
    fn resync_clears_predictors_but_keeps_cache_entries() {
        let mut rng = StdRng::seed_from_u64(24);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(7), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: sample_block(0.01),
            })
            .unwrap();
        let record = session.emit_resync().unwrap();
        assert_eq!(record.header.record_type, RecordType::Resync);
        assert!(session.state().cache_entry(ItemId(1)).is_some());
        assert!(session.state().predictor_entry(ItemId(1)).is_none());
    }

    #[test]
    fn rekey_record_puts_sender_into_pending_epoch() {
        let mut rng = StdRng::seed_from_u64(25);
        let (sender, mut receiver) =
            classic_ref1_pair_from_rng(StreamId(9), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        let (rekey_record, payload) = session.emit_rekey(&mut rng).unwrap();
        let recovered = receiver.unprotect_record(rekey_record).unwrap();
        assert_eq!(RekeyPayload::decode(&recovered.payload).unwrap(), payload);
        assert_eq!(
            session.protector().stream_state(),
            StreamState::RekeyPending {
                next_epoch_id: EpochId(1)
            }
        );

        let first_new_epoch = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(1),
                block: sample_block(0.02),
            })
            .unwrap();
        assert_eq!(first_new_epoch.header.epoch_id, EpochId(1));
        assert_eq!(first_new_epoch.header.seq_no, SeqNo(0));
        assert!(
            first_new_epoch
                .header
                .flags
                .contains(RecordFlags::POST_REKEY_CONFIRM)
        );
    }

    #[test]
    fn deterministic_traces_can_be_emitted_locally() {
        let mut rng = StdRng::seed_from_u64(26);
        let (sender, _) =
            classic_ref1_pair_from_rng(StreamId(10), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new(sender);
        let records = session
            .emit_trace(vec![
                ServerEvent::Insert {
                    item_id: ItemId(1),
                    block: sample_block(0.01),
                },
                ServerEvent::UpsertObject {
                    item_id: ItemId(1),
                    block: sample_block(0.0103),
                },
                ServerEvent::Invalidate { item_id: ItemId(1) },
            ])
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].header.seq_no, SeqNo(0));
        assert_eq!(records[1].header.seq_no, SeqNo(1));
        assert_eq!(records[2].header.seq_no, SeqNo(2));
    }

    #[tokio::test]
    async fn websocket_loopback_handles_happy_path_and_delta_mode() {
        let mut rng = StdRng::seed_from_u64(31);
        let (sender, receiver) =
            classic_ref1_pair_from_rng(StreamId(20), StreamDirection::ServerToClient, &mut rng);
        let mut server_session = ServerSession::new(sender);
        let first = sample_block(0.02);
        let second = first.clone();
        let records = vec![
            server_session
                .emit_event(ServerEvent::Insert {
                    item_id: ItemId(1),
                    block: first.clone(),
                })
                .unwrap(),
            server_session
                .emit_event(ServerEvent::UpsertObject {
                    item_id: ItemId(1),
                    block: second.clone(),
                })
                .unwrap(),
        ];
        match records[0].header.record_type {
            RecordType::ExactState => {
                assert!(matches!(
                    records[0].header.codec_mode,
                    CodecMode::PackedExact | CodecMode::DirectExact
                ));
            }
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                assert_eq!(records[0].header.codec_mode, CodecMode::None);
            }
            other => panic!("unexpected first loopback record type: {:?}", other),
        }
        match records[1].header.record_type {
            RecordType::ExactState => {
                assert!(matches!(
                    records[1].header.codec_mode,
                    CodecMode::PredictedExact | CodecMode::DirectExact
                ));
            }
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                assert_eq!(records[1].header.codec_mode, CodecMode::None);
            }
            other => panic!("unexpected second loopback record type: {:?}", other),
        }

        let received = round_trip_records(records).await;
        let mut client_session = ClientSession::new(receiver);
        client_session.apply_protected_trace(received).unwrap();
        let received = &client_session
            .state()
            .cache_entry(ItemId(1))
            .unwrap()
            .object;
        assert_eq!(received.exact_bytes, second.exact_bytes);
    }

    #[tokio::test]
    async fn websocket_loopback_resets_predictors_after_evict_and_resync() {
        let mut rng = StdRng::seed_from_u64(32);
        let (sender, receiver) =
            classic_ref1_pair_from_rng(StreamId(21), StreamDirection::ServerToClient, &mut rng);
        let mut server_session = ServerSession::new(sender);
        let block = sample_block(0.021);
        let records = vec![
            server_session
                .emit_event(ServerEvent::Insert {
                    item_id: ItemId(2),
                    block: block.clone(),
                })
                .unwrap(),
            server_session
                .emit_event(ServerEvent::Evict { item_id: ItemId(2) })
                .unwrap(),
            server_session
                .emit_event(ServerEvent::Insert {
                    item_id: ItemId(2),
                    block: block.clone(),
                })
                .unwrap(),
            server_session.emit_resync().unwrap(),
            server_session
                .emit_event(ServerEvent::UpsertObject {
                    item_id: ItemId(2),
                    block: block.clone(),
                })
                .unwrap(),
        ];

        for index in [0usize, 2, 4] {
            match records[index].header.record_type {
                RecordType::ExactState => {
                    assert!(matches!(
                        records[index].header.codec_mode,
                        CodecMode::PackedExact | CodecMode::DirectExact
                    ));
                }
                RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                    assert_eq!(records[index].header.codec_mode, CodecMode::None);
                }
                other => panic!(
                    "unexpected loopback reset record type at {}: {:?}",
                    index, other
                ),
            }
        }

        let received = round_trip_records(records).await;
        let mut client_session = ClientSession::new(receiver);
        client_session.apply_protected_trace(received).unwrap();
        assert!(client_session.state().predictor_meta(ItemId(2)).is_some());
        assert!(client_session.state().cache_entry(ItemId(2)).is_some());
    }

    #[tokio::test]
    async fn websocket_loopback_handles_rekey_resync_and_close() {
        let mut rng = StdRng::seed_from_u64(33);
        let (sender, receiver) =
            classic_ref1_pair_from_rng(StreamId(22), StreamDirection::ServerToClient, &mut rng);
        let mut server_session = ServerSession::new(sender);
        let initial = sample_block(0.03);
        let refreshed = sample_block(0.031);
        let mut records = vec![
            server_session
                .emit_event(ServerEvent::Insert {
                    item_id: ItemId(3),
                    block: initial,
                })
                .unwrap(),
        ];
        let (rekey_record, _) = server_session.emit_rekey(&mut rng).unwrap();
        records.push(rekey_record);
        records.push(
            server_session
                .emit_event(ServerEvent::UpsertObject {
                    item_id: ItemId(3),
                    block: refreshed.clone(),
                })
                .unwrap(),
        );
        records.push(server_session.emit_resync().unwrap());
        records.push(
            server_session
                .emit_event(ServerEvent::UpsertObject {
                    item_id: ItemId(3),
                    block: refreshed.clone(),
                })
                .unwrap(),
        );
        records.push(server_session.emit_close(None).unwrap());

        assert_eq!(records[2].header.epoch_id, EpochId(1));
        assert!(
            records[2]
                .header
                .flags
                .contains(RecordFlags::POST_REKEY_CONFIRM)
        );
        match records[4].header.record_type {
            RecordType::ExactState => {
                assert!(matches!(
                    records[4].header.codec_mode,
                    CodecMode::PackedExact | CodecMode::DirectExact
                ));
            }
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                assert_eq!(records[4].header.codec_mode, CodecMode::None);
            }
            other => panic!("unexpected post-resync record type: {:?}", other),
        }

        let received = round_trip_records(records).await;
        let mut client_session = ClientSession::new(receiver);
        client_session.apply_protected_trace(received).unwrap();
        assert_eq!(
            client_session.protector().stream_state(),
            StreamState::Closed
        );
        assert_eq!(client_session.protector().current_epoch_id(), EpochId(1));
        let cached: ExactStateMaterial = (&client_session
            .state()
            .cache_entry(ItemId(3))
            .unwrap()
            .object)
            .into();
        assert_eq!(cached.exact_bytes, refreshed.exact_bytes);
    }

    #[tokio::test]
    async fn websocket_loopback_rejects_unexpected_sequence() {
        let mut rng = StdRng::seed_from_u64(34);
        let (sender, receiver) =
            classic_ref1_pair_from_rng(StreamId(23), StreamDirection::ServerToClient, &mut rng);
        let mut server_session = ServerSession::new(sender);
        let mut record = server_session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(4),
                block: sample_block(0.04),
            })
            .unwrap();
        record.header.seq_no = SeqNo(9);

        let received = round_trip_records(vec![record]).await;
        let mut client_session = ClientSession::new(receiver);
        let error = client_session.apply_protected_trace(received).unwrap_err();
        assert!(matches!(
            error,
            ClientApplyError::Protection(shared_protocol::ProtectionError::UnexpectedSeqNo { .. })
        ));
        assert_eq!(
            client_session.protector().stream_state(),
            StreamState::Closed
        );
    }

    #[tokio::test]
    async fn websocket_loopback_rejects_authentication_failure() {
        let mut rng = StdRng::seed_from_u64(35);
        let (sender, receiver) =
            classic_ref1_pair_from_rng(StreamId(24), StreamDirection::ServerToClient, &mut rng);
        let mut server_session = ServerSession::new(sender);
        let mut record = server_session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(5),
                block: sample_block(0.05),
            })
            .unwrap()
            .to_bytes();
        let last = record.len() - 1;
        record[last] ^= 0xAA;

        let received = round_trip_frames(vec![record]).await;
        let mut client_session = ClientSession::new(receiver);
        let error = client_session.apply_protected_trace(received).unwrap_err();
        assert!(matches!(
            error,
            ClientApplyError::Protection(shared_protocol::ProtectionError::AuthenticationFailure)
        ));
        assert_eq!(
            client_session.protector().stream_state(),
            StreamState::Closed
        );
    }

    #[tokio::test]
    async fn source_aware_insert_round_trips_source_meta() {
        let mut rng = StdRng::seed_from_u64(36);
        let (sender, receiver) =
            classic_ref1_pair_from_rng(StreamId(25), StreamDirection::ServerToClient, &mut rng);
        let mut session = ServerSession::new_with_configs(
            sender,
            SourceOptimizationConfig {
                inline_source_meta_enabled: true,
                ..SourceOptimizationConfig::default()
            },
        );
        let cache_root = std::env::temp_dir().join("pulzz_source_roundtrip_cache");
        let _ = std::fs::remove_dir_all(&cache_root);
        let cache = SourceCache::new(SourceCacheConfig {
            root_dir: cache_root.clone(),
            max_object_material_bytes: 16 * 1024 * 1024,
            ..SourceCacheConfig::default()
        })
        .unwrap();
        let prepared = prepare_text_ingest_source("hello\r\nworld", Some("doc.txt".to_string()));
        let emitted = session
            .emit_source_insert(
                SourceIngestRequest {
                    item_id: ItemId(7),
                    source: &prepared,
                },
                &cache,
            )
            .unwrap();
        assert!(
            emitted
                .records
                .iter()
                .any(|record| record.header.record_type == RecordType::SourceMeta)
        );

        let received = round_trip_records(emitted.records).await;
        let mut client_session = ClientSession::new(receiver);
        client_session.apply_protected_trace(received).unwrap();

        let binding = client_session.state().source_binding(ItemId(7)).unwrap();
        assert_eq!(binding.source_hash, prepared.descriptor.source_hash);
        assert_eq!(binding.label.as_deref(), Some("doc.txt"));
        assert_eq!(binding.mime.as_deref(), Some("text/plain; charset=utf-8"));
        let _ = std::fs::remove_dir_all(cache_root);
    }
    #[test]
    fn hybrid_routes_emit_predictive_correct_records() {
        let mut state = ServerState::default();
        let hybrid_route = shared_protocol::HybridRoute {
            route_family: ControllerRouteFamily::Hybrid,
            precision_band: shared_protocol::PrecisionBand::Balanced,
            assembly_mode: None,
            output_len: 4,
            dependency_closure: Vec::new(),
            components: vec![shared_protocol::HybridRouteComponent::Literal(
                b"test".to_vec(),
            )],
        };
        let payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::Hybrid,
            route_kind: ControllerRouteFamily::Hybrid.route_family(),
            route_source_kind: Some(shared_protocol::SourceKind::Text),
            assembly_mode: None,
            precision_band: shared_protocol::PrecisionBand::Balanced,
            dependency_closure: Vec::new(),
            sync_risk: 0,
            literal_bytes: Vec::new(),
            assembly_ref: None,
            inline_assembly_defs: Vec::new(),
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: Vec::new(),
            route_graph: derived_dispatch_route_graph(
                ControllerRouteFamily::Hybrid,
                ControllerRouteFamily::Hybrid.route_family(),
                &[],
                0,
                &[],
                &[],
                None,
                None,
                Some(&hybrid_route),
            ),
            contradiction_bytes: Vec::new(),
            prg: None,
            hybrid_route: Some(hybrid_route),
        }
        .with_derived_route_graph();
        let payload_bytes = payload.encode().unwrap();
        let record = state
            .emit_predictive_route_record_with_bytes(
                HeaderContext {
                    stream_id: StreamId(1),
                    epoch_id: EpochId(0),
                    seq_no: SeqNo(1),
                    post_rekey_confirm: false,
                },
                ItemId(5),
                RecordType::PredictiveCorrect,
                payload,
                payload_bytes,
            )
            .unwrap();
        assert_eq!(record.header.record_type, RecordType::PredictiveCorrect);
    }
    #[test]
    fn operational_control_plane_records_round_trip_with_memory_ack() {
        let mut server_session =
            PlainServerSession::new_with_configs(StreamId(41), SourceOptimizationConfig::default());

        server_session.state.append_episode_activation_for_item(
            ItemId(7),
            EpisodeActivationEvent {
                source_kind: SourceKind::Text,
                object_ref: EpisodeObjectRef {
                    object_kind: shared_protocol::ObjectKind::ExactState,
                    object_id: "item:7".to_string(),
                },
                cue: shared_protocol::SparseCue::default(),
                context_hash: ContextHash(77),
                tick: 1,
                success: true,
            },
        );
        server_session
            .state
            .enqueue_consolidation_job(ConsolidationJob {
                job_id: 1,
                kind: ConsolidationJobKind::ObjectRetirement,
                plane: MemoryPlane::Transform,
                source_kind: Some(SourceKind::Text),
                primary_object_id: "transform:5".to_string(),
                related_object_ids: Vec::new(),
                lifecycle: shared_protocol::ObjectLifecycleMeta::default(),
                support: 1,
                ambiguity: 0,
                savings: 0,
            });
        server_session.state.schedule_consolidation_from_replay();

        let records = server_session
            .emit_pending_control_plane_records(8)
            .unwrap();
        assert!(
            records
                .iter()
                .any(|record| record.header.record_type == RecordType::ReplayHint)
        );
        assert!(
            records
                .iter()
                .any(|record| record.header.record_type == RecordType::MemoryRetire)
        );

        let mut client_state = client::ClientState::default();
        client_state.apply_trace(records).unwrap();
        let acks = client_state.take_pending_memory_acks();
        assert_eq!(acks.len(), 2);
        for payload in acks {
            let ack = server_session
                .state
                .emit_memory_ack_record(payload)
                .unwrap();
            assert!(matches!(ack.header.record_type, RecordType::MemoryAck));
            server_session.apply_peer_record(ack).unwrap();
        }
        assert_eq!(server_session.state.received_memory_acks().len(), 2);
    }
}
