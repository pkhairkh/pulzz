// Client execution surface for CHPMT predictive-memory transport.
// Active runtime routing uses cue/object identity and native exact-state material throughout.

#[cfg(not(target_arch = "wasm32"))]
pub mod abi;
#[cfg(not(target_arch = "wasm32"))]
mod datagram;
pub mod source_cache;

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

#[cfg(not(target_arch = "wasm32"))]
use std::{
    net::{IpAddr, SocketAddr},
    sync::{Arc, OnceLock},
};

#[cfg(not(target_arch = "wasm32"))]
pub use abi::{
    NativeClientAbiConfig, NativeClientCarrierKind, NativeClientSession, connect_native_session,
};
#[cfg(not(target_arch = "wasm32"))]
pub use datagram::{
    ConnectedQuicDatagramSession, ConnectedUdpSession, ConnectedWebTransportDatagramSession,
    connect_quic_datagram_session, connect_udp_session, connect_webtransport_datagram_session,
};
pub use shared_protocol::{
    ClientSecurityConfig, TransportConfig, TransportMode, TransportSessionConfig,
};
pub use source_cache::{WebSourceCache, WebSourceCacheConfig, WebSourceLookupResult};

use shared_protocol::{
    Assembly, AssemblyComponentRef, AssemblyDefPayload, AssemblyId, BlockId, BootstrapClientConfig,
    BundleId, ChpmtObject, CodecMode, ContextTreeGovernor, ContextTreeOutcome, ContextTreeSymbol,
    ControllerRouteFamily, DelimiterClass, DictionaryId, EpisodeActivationEvent,
    EpisodeHintPayload, EpisodeMemory, EpisodeObjectRef, HybridRouteComponent,
    IssuedClientCredential, ItemId, LagBucket, MemoryAckPayload, MemoryRetirePayload,
    PlaneResyncPayload, PredictiveRouteDispatchPayload, PredictorEntryMeta, PredictorState, Record,
    RecordFlags, RecordHeader, RecordType, RepairCause, RepairPayload, RepairRequest,
    ResidualSizeBucket, RouteStatistics, SchemaDefPayload, SchemaGraph, SchemaId,
    SharedBlockCatalog, SharedDictionary, SlotShapeBucket, SourceDescriptor, SourceMetaPayload,
    SparseCue, StreamDirection, StreamId, StreamProtector, SyncStateBucket, TransformBasisRef,
    TransformClass, TransformId, TransformInstancePayload, ValidationError,
    apply_exact_contradiction_mask_to_bytes, apply_transform_instance, decode_assembly_def_record,
    derive_catalog_block_id,
    decode_data_payload_to_runtime_object, decode_episode_hint_record, decode_replay_hint_record,
    decode_schema_def_record, decode_transform_def_record, decode_transform_instance_record,
    decode_transport_records, derive_context_hash, execute_hybrid_route, execute_prg,
    expand_assembly_body,
};

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;

#[cfg(not(target_arch = "wasm32"))]
use futures_util::{SinkExt, StreamExt};

#[cfg(not(target_arch = "wasm32"))]
use rand::{RngCore, rngs::OsRng};

#[cfg(not(target_arch = "wasm32"))]
use tokio::{
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
    time::{Duration, sleep, timeout},
};

#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{self, Message, protocol::WebSocketConfig},
};

#[cfg(not(target_arch = "wasm32"))]
use shared_protocol::{
    BootstrapCompleted, BootstrapMessage, ClientBootstrapState, ProtectionProfileKind,
    carrier::reliable::{ReliableCarrier, read_length_prefixed_frame, write_length_prefixed_frame},
};

#[cfg(not(target_arch = "wasm32"))]
use quinn::{
    Connection as QuicConnection, Endpoint as QuicEndpoint, IdleTimeout as QuicIdleTimeout,
    ReadError as QuicReadError, ReadExactError as QuicReadExactError, RecvStream, SendStream,
    TransportConfig as QuicTransportConfig, VarInt as QuicVarInt,
};

#[cfg(not(target_arch = "wasm32"))]
const WEBSOCKET_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

#[cfg(not(target_arch = "wasm32"))]
fn websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(WEBSOCKET_MAX_MESSAGE_BYTES);
    config.max_frame_size = Some(WEBSOCKET_MAX_MESSAGE_BYTES);
    config
}

#[cfg(not(target_arch = "wasm32"))]
use rustls::{
    ClientConfig as RustlsClientConfig,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};

#[cfg(target_arch = "wasm32")]
mod web_bench;

#[derive(Debug, Clone)]
pub struct ClientEntry {
    pub object: ChpmtObject,
}

impl ClientEntry {
    pub fn exact_bytes(&self) -> &[u8] {
        &self.object.exact_bytes
    }
}
// S3.2: Client store → invalidation plane dependency map
//
// | Store                          | Depends On          | Authoritative? | Rebuildable? | Cleared on Substrate Invalidation |
// |--------------------------------|---------------------|----------------|--------------|-----------------------------------|
// | cache                          | AtomSubstrate       | yes            | no           | yes                               |
// | predictors                     | AtomSubstrate       | derived        | yes          | yes                               |
// | source_bindings                | AtomSubstrate       | yes            | no           | yes                               |
// | catalog                        | AtomSubstrate       | yes            | no           | yes                               |
// | assemblies                     | Assembly + Substrate| derived        | yes          | yes                               |
// | installed_assembly_defs        | Assembly + Substrate| derived        | yes          | yes                               |
// | assembly_index                 | Assembly + Substrate| derived        | yes          | yes                               |
// | assembly_sync_versions         | Assembly            | derived        | yes          | yes                               |
// | transforms                     | Transform + Substrate| derived       | yes          | yes                               |
// | materialized_transforms        | Transform + Substrate| derived       | yes          | yes                               |
// | schemas                        | Schema + Substrate  | derived        | yes          | yes                               |
// | schema_payloads                | Schema + Substrate  | derived        | yes          | yes                               |
// | dictionaries                   | AtomSubstrate       | derived        | yes          | yes                               |
// | repair_requests                | AtomSubstrate       | derived        | yes          | yes                               |
// | pending_memory_acks            | all planes          | derived        | yes          | yes (on resync)                   |
// | episode_memory                 | Episode             | derived        | yes          | no (unless Episode resync)        |
// | installed_episode_hints        | Episode + Substrate | derived        | yes          | yes                               |
// | installed_schema_defs          | Schema + Substrate  | derived        | yes          | yes                               |
// | route_statistics               | Controller          | derived        | yes          | no (unless Controller resync)     |
// | context_governor               | Controller          | derived        | yes          | no (unless Controller resync)     |
//
// S3.2 sub-items completed:
//   [x] S3.2.a — All client stores enumerated in table above (cache, catalog, schemas,
//       schema_payloads, installed_schema_defs, dictionaries, transforms,
//       materialized_transforms, assemblies, installed_assembly_defs, assembly_index,
//       assembly_sync_versions, source_bindings, predictors, repair_requests,
//       pending_memory_acks, episode_memory, installed_episode_hints,
//       route_statistics, context_governor).
//   [x] S3.2.b — Dependency plane recorded in "Depends On" column for each store.
//   [x] S3.2.c — Authoritative vs derived status recorded in "Authoritative?" column.
//   [x] S3.2.d — Rebuildability and substrate-invalidation behavior recorded in
//       "Rebuildable?" and "Cleared on Substrate Invalidation" columns; cross-referenced
//       via S3.2 inline comments in resync_plane().
#[derive(Debug)]
pub struct ClientState {
    cache: HashMap<ItemId, ClientEntry>,
    predictors: HashMap<ItemId, PredictorEntryMeta>,
    source_bindings: HashMap<ItemId, SourceDescriptor>,
    catalog: SharedBlockCatalog,
    assemblies: HashMap<AssemblyId, Assembly>,
    installed_assembly_defs: Vec<AssemblyDefPayload>,
    assembly_index: HashMap<String, AssemblyId>,
    assembly_sync_versions: HashMap<AssemblyId, shared_protocol::AssemblyReuseSignature>,
    assembly_cache: WebSourceCache,
    transforms: HashMap<TransformId, TransformClass>,
    materialized_transforms: HashMap<TransformId, Vec<u8>>,
    schemas: HashMap<SchemaId, SchemaGraph>,
    schema_payloads: HashMap<SchemaId, SchemaDefPayload>,
    dictionaries: HashMap<DictionaryId, SharedDictionary>,
    repair_requests: Vec<RepairRequest>,
    pending_memory_acks: Vec<MemoryAckPayload>,
    episode_memory: EpisodeMemory,
    installed_episode_hints: Vec<EpisodeHintPayload>,
    installed_schema_defs: Vec<SchemaDefPayload>,
    route_statistics: HashMap<String, RouteStatistics>,
    context_governor: ContextTreeGovernor,
    // P0-P5 compression pipeline state
    dict_manager: shared_protocol::DictionaryManager,
    template_registry: shared_protocol::TemplateRegistry,
    previous_versions: HashMap<u64, Vec<u8>>,
    // P5: Compressor cache for Zstd context reuse.
    compressor_cache: shared_protocol::CompressorCache,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
            predictors: HashMap::new(),
            source_bindings: HashMap::new(),
            catalog: SharedBlockCatalog::default(),
            assemblies: HashMap::new(),
            installed_assembly_defs: Vec::new(),
            assembly_index: HashMap::new(),
            assembly_sync_versions: HashMap::new(),
            assembly_cache: WebSourceCache::new(WebSourceCacheConfig::default()),
            transforms: HashMap::new(),
            materialized_transforms: HashMap::new(),
            schemas: HashMap::new(),
            schema_payloads: HashMap::new(),
            dictionaries: HashMap::new(),
            repair_requests: Vec::new(),
            pending_memory_acks: Vec::new(),
            episode_memory: EpisodeMemory::default(),
            installed_episode_hints: Vec::new(),
            installed_schema_defs: Vec::new(),
            route_statistics: HashMap::new(),
            context_governor: ContextTreeGovernor::default(),
            dict_manager: shared_protocol::DictionaryManager::default(),
            template_registry: shared_protocol::TemplateRegistry::default(),
            previous_versions: HashMap::new(),
            compressor_cache: shared_protocol::CompressorCache::default(),
        }
    }
}

#[derive(Debug)]
pub struct ClientSession {
    state: ClientState,
    protector: StreamProtector,
}

#[derive(Debug, Clone)]
pub struct CredentialProvider {
    issued_credential: IssuedClientCredential,
}

#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub max_attempts: usize,
}

#[derive(Debug, Clone)]
pub struct ClientConnectConfig {
    pub url: String,
    pub stream_id: StreamId,
    pub direction: StreamDirection,
    pub session: TransportSessionConfig,
    pub security: ClientSecurityConfig,
    pub reconnect_policy: ReconnectPolicy,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct ConnectedWebSocketSession {
    session: ClientSession,
    websocket: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    transport_config: TransportConfig,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct ConnectedTcpSession {
    session: ClientSession,
    stream_reader: ReadHalf<TcpStream>,
    stream_writer: WriteHalf<TcpStream>,
    transport_config: TransportConfig,
    max_transport_frame_bytes: usize,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct ConnectedQuicSession {
    session: ClientSession,
    _endpoint: QuicEndpoint,
    _connection: QuicConnection,
    send: SendStream,
    recv: RecvStream,
    transport_config: TransportConfig,
    max_transport_frame_bytes: usize,
}

impl ClientState {
    pub fn cache_entry(&self, item_id: ItemId) -> Option<&ClientEntry> {
        self.cache.get(&item_id)
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub fn predictor_meta(&self, item_id: ItemId) -> Option<PredictorEntryMeta> {
        self.predictors.get(&item_id).copied()
    }

    pub fn predictor_len(&self) -> usize {
        self.predictors.len()
    }

    pub fn append_episode_activation(&mut self, event: EpisodeActivationEvent) {
        self.episode_memory.append_activation(event);
    }

    pub fn episode_memory(&self) -> &EpisodeMemory {
        &self.episode_memory
    }

    pub fn installed_episode_hints(&self) -> &[EpisodeHintPayload] {
        &self.installed_episode_hints
    }

    pub fn installed_assembly_defs(&self) -> &[AssemblyDefPayload] {
        &self.installed_assembly_defs
    }

    pub fn installed_schema_defs(&self) -> &[SchemaDefPayload] {
        &self.installed_schema_defs
    }

    pub fn source_binding(&self, item_id: ItemId) -> Option<&SourceDescriptor> {
        self.source_bindings.get(&item_id)
    }

    pub fn catalog(&self) -> &SharedBlockCatalog {
        &self.catalog
    }

    /// S3.1.a2: Promote decoded/materialized bytes into the shared substrate catalog.
    /// Model A: successful decode promotes reusable substrate. This makes the
    /// client's substrate catalog grow symmetrically with the server's assumptions,
    /// since the server already promotes local data to its local_catalog on decode.
    ///
    /// S3.1.a4: Promoted substrate carries canonical block IDs and versions usable
    /// by route planning (block:<id> format per S2.6), ensuring sender and receiver
    /// share one substrate-growth story.
    ///
    /// S3.1.a3: Prevents uncontrolled duplication by checking catalog.contains_block()
    /// before insertion. The content-derived block ID is deterministic, so identical
    /// bytes are never inserted twice.
    fn promote_to_substrate_catalog(
        &mut self,
        source_kind: shared_protocol::SourceKind,
        exact_bytes: &[u8],
    ) {
        let block_id = derive_catalog_block_id(source_kind, exact_bytes);
        if self.catalog.contains_block(block_id) {
            return;
        }
        // Insert the material as a catalog block. The block_id is derived from
        // the content hash, which is canonical per S2.6. Promotion is best-effort;
        // a failure here does not invalidate the successful decode that triggered it.
        let _ = self.catalog.insert_block(source_kind, exact_bytes.to_vec());
    }

    pub fn pending_repair_requests(&self) -> &[RepairRequest] {
        &self.repair_requests
    }

    pub fn take_pending_repair_requests(&mut self) -> Vec<RepairRequest> {
        std::mem::take(&mut self.repair_requests)
    }

    pub fn pop_next_repair_request(&mut self) -> Option<RepairRequest> {
        if self.repair_requests.is_empty() {
            None
        } else {
            Some(self.repair_requests.remove(0))
        }
    }

    pub fn clear_pending_repair_requests(&mut self) {
        self.repair_requests.clear();
    }

    pub fn pending_memory_acks(&self) -> &[MemoryAckPayload] {
        &self.pending_memory_acks
    }

    pub fn take_pending_memory_acks(&mut self) -> Vec<MemoryAckPayload> {
        std::mem::take(&mut self.pending_memory_acks)
    }

    pub fn pop_next_memory_ack(&mut self) -> Option<MemoryAckPayload> {
        if self.pending_memory_acks.is_empty() {
            None
        } else {
            Some(self.pending_memory_acks.remove(0))
        }
    }

    pub fn clear_pending_memory_acks(&mut self) {
        self.pending_memory_acks.clear();
    }

    pub fn apply_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let record = record.validate().map_err(ClientApplyError::Validation)?;

        match record.header.record_type {
            RecordType::ExactState | RecordType::BatchEnvelope => self.apply_data_record(record),
            RecordType::Rekey | RecordType::Close => Ok(()),
            RecordType::Resync => self.apply_resync_record(record),
            RecordType::SourceMeta => self.apply_source_meta_record(record),
            RecordType::Repair => self.apply_repair_record(record),
            RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
                self.apply_predictive_dispatch_record(record)
            }
            RecordType::TransformCorrect => self.apply_transform_instance_record(record),
            RecordType::AssemblyDef => self.apply_assembly_def_record(record),
            RecordType::TransformDef => self.apply_transform_def_record(record),
            RecordType::EpisodeHint => self.apply_episode_hint_record(record),
            RecordType::SchemaDef => self.apply_schema_def_record(record),
            RecordType::MemoryRetire => self.apply_memory_retire_record(record),
            RecordType::ReplayHint => self.apply_replay_hint_record(record),
            RecordType::MemoryAck => Ok(()),
        }
    }

    /// Wave 13 T-13-c: Apply a BatchEnvelope record by decoding the envelope,
    /// then decompressing and caching each item.
    ///
    /// Wave 13 T-13-b update: the entire batch payload is now zstd-compressed
    /// as a single stream (not per-item). This method decompresses the batch
    /// payload first, then decodes the envelope, then caches each item.
    fn apply_batch_envelope(
        &mut self,
        header: RecordHeader,
        payload: Vec<u8>,
    ) -> Result<(), ClientApplyError> {
        // Wave 13 T-13-b: decompress the entire batch payload first.
        // The server compresses the postcard-encoded BatchEnvelope as a
        // single zstd stream. We decompress it here, then decode the envelope.
        let envelope_bytes = if payload.len() > 64 {
            match shared_protocol::compress::zstd_decompress_raw(&payload, 2 * 1024 * 1024) {
                Ok(decompressed) => decompressed,
                Err(_) => payload, // fallback: treat as uncompressed
            }
        } else {
            payload
        };

        let envelope = shared_protocol::batch::BatchEnvelope::decode(&envelope_bytes)
            .map_err(|e| ClientApplyError::PredictiveDispatch(e.to_string()))?;
        for item in envelope.items {
            // Each item's payload is already uncompressed (the server stored
            // uncompressed items and compressed the whole envelope).
            let decompressed = item.payload;
            // Feed the decompressed sample to the dict_manager for future training.
            self.dict_manager.add_sample(item.source_kind, &decompressed);
            if item.source_kind == shared_protocol::SourceKind::Json {
                self.template_registry
                    .try_register(item.source_kind, &decompressed);
            }
            self.dict_manager.maybe_train(item.source_kind);
            // Store the decompressed bytes in the client cache.
            let descriptor = shared_protocol::source::SourceDescriptor {
                kind: item.source_kind,
                source_hash: shared_protocol::compute_source_hash(
                    item.source_kind,
                    None,
                    &decompressed,
                ),
                byte_len: decompressed.len(),
                mime: None,
                label: None,
            };
            let object_key = descriptor.runtime_object_key_from_bytes(&decompressed);
            let object = shared_protocol::source::ChpmtObject::from_exact_bytes(
                descriptor,
                object_key,
                item.source_kind,
                shared_protocol::ObjectKind::ExactState,
                shared_protocol::chpmt::derive_sparse_cue_from_bytes(item.source_kind, &decompressed),
                decompressed,
            );
            let entry = ClientEntry { object };
            self.cache.insert(item.item_id, entry);
        }
        let _ = header; // header validated by caller
        Ok(())
    }

    fn apply_resync_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        if record.payload.is_empty() {
            // S3.3: Empty resync means full reset — clear all planes
            // symmetrically, just like the server does.
            self.cache.clear();
            self.source_bindings.clear();
            self.catalog = SharedBlockCatalog::default();
            self.predictors.clear();
            self.dictionaries.clear();
            self.clear_assembly_plane();
            self.transforms.clear();
            self.materialized_transforms.clear();
            self.schemas.clear();
            self.schema_payloads.clear();
            self.installed_schema_defs.clear();
            self.installed_episode_hints.clear();
            self.assembly_sync_versions.clear();
            // S1.2.f: On resync, discard any pending memory acks — the server
            // has also reset its state and will not recognize stale acks.
            self.pending_memory_acks.clear();
            return Ok(());
        }
        let payload = PlaneResyncPayload::decode(&record.payload)
            .map_err(|error| ClientApplyError::PredictiveDispatch(error.to_string()))?;
        self.resync_plane(payload);
        Ok(())
    }

    fn apply_memory_retire_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let payload = MemoryRetirePayload::decode(&record.payload)
            .map_err(|error| ClientApplyError::PredictiveDispatch(error.to_string()))?;
        self.enqueue_memory_ack(MemoryAckPayload {
            version: MemoryAckPayload::VERSION,
            plane: payload.plane,
            object_kind: payload.object_kind,
            object_id: payload.object_id.clone(),
            acked_record_type: RecordType::MemoryRetire,
            acked_seq_no: record.header.seq_no,
        });
        self.retire_memory_object(payload);
        Ok(())
    }

    fn enqueue_memory_ack(&mut self, payload: MemoryAckPayload) {
        if self.pending_memory_acks.iter().any(|existing| {
            existing.plane == payload.plane
                && existing.object_kind == payload.object_kind
                && existing.object_id == payload.object_id
                && existing.acked_record_type == payload.acked_record_type
                && existing.acked_seq_no == payload.acked_seq_no
        }) {
            return;
        }
        self.pending_memory_acks.push(payload);
    }

    fn retire_memory_object(&mut self, payload: MemoryRetirePayload) {
        match payload.plane {
            shared_protocol::MemoryPlane::AtomSubstrate => {
                if let Some(item_id) = Self::parse_item_id(payload.object_id.as_str()) {
                    self.cache.remove(&item_id);
                    self.predictors.remove(&item_id);
                    self.source_bindings.remove(&item_id);
                } else {
                    let retired_items = self
                        .source_bindings
                        .iter()
                        .filter_map(|(item_id, descriptor)| {
                            (descriptor.source_hash.to_hex() == payload.object_id).then_some(*item_id)
                        })
                        .collect::<Vec<_>>();
                    for item_id in retired_items {
                        self.cache.remove(&item_id);
                        self.predictors.remove(&item_id);
                        self.source_bindings.remove(&item_id);
                    }
                }
                self.prune_resolved_repair_requests();
            }
            shared_protocol::MemoryPlane::Assembly => {
                self.retire_assembly_object(payload.object_id.as_str())
            }
            shared_protocol::MemoryPlane::Transform => {
                self.transforms
                    .retain(|id, _| id.0.to_string() != payload.object_id);
                self.materialized_transforms
                    .retain(|id, _| id.0.to_string() != payload.object_id);
            }
            shared_protocol::MemoryPlane::Episode => {
                self.installed_episode_hints
                    .retain(|hint| hint.context_hash.0.to_string() != payload.object_id);
            }
            shared_protocol::MemoryPlane::Schema => {
                self.schemas
                    .retain(|id, _| id.0.to_string() != payload.object_id);
                self.schema_payloads
                    .retain(|id, _| id.0.to_string() != payload.object_id);
                self.installed_schema_defs
                    .retain(|def| def.schema.schema_id.0.to_string() != payload.object_id);
            }
            shared_protocol::MemoryPlane::Controller => {
                self.route_statistics.remove(&payload.object_id);
                self.context_governor = ContextTreeGovernor::default();
            }
        }
    }

    // S3.2: Each arm of this match corresponds to a row in the store → invalidation
    // plane dependency map (see table above ClientState). The clears below are
    // the materialization of the "Cleared on Substrate Invalidation" column.
    fn resync_plane(&mut self, payload: PlaneResyncPayload) {
        match payload.plane {
            // S3.2: See store dependency map — AtomSubstrate invalidation clears cache,
            // source_bindings, catalog (authoritative), plus dictionaries, assemblies,
            // schemas, transforms, episode_hints (all depend on substrate).
            shared_protocol::MemoryPlane::AtomSubstrate => {
                self.cache.clear();
                self.source_bindings.clear();
                self.catalog = SharedBlockCatalog::default();
                // S3.3: AtomSubstrate is the foundation for all derived planes.
                // When it is invalidated, derived planes that reference substrate
                // must also be cleared to prevent dangling references.
                // Dictionaries, assemblies, schemas, and transforms all reference
                // substrate blocks/bundles/ranges, so they are vacated here.
                self.dictionaries.clear();
                self.clear_assembly_plane();
                self.transforms.clear();
                self.materialized_transforms.clear();
                self.schemas.clear();
                self.schema_payloads.clear();
                self.installed_schema_defs.clear();
                self.installed_episode_hints.clear();
                self.assembly_sync_versions.clear();
            }
            // S3.2: Assembly plane invalidation clears assemblies, installed_assembly_defs,
            // assembly_index (all Assembly + Substrate dependent).
            shared_protocol::MemoryPlane::Assembly => self.clear_assembly_plane(),
            // S3.2: Transform plane invalidation clears transforms, materialized_transforms
            // (both depend on Transform + Substrate).
            shared_protocol::MemoryPlane::Transform => {
                self.transforms.clear();
                self.materialized_transforms.clear();
            }
            // S3.2: Episode plane invalidation clears episode_memory (Episode-only dep)
            // and installed_episode_hints (Episode + Substrate dep).
            shared_protocol::MemoryPlane::Episode => {
                self.episode_memory = EpisodeMemory::default();
                self.installed_episode_hints.clear();
            }
            // S3.2: Schema plane invalidation clears schemas, schema_payloads,
            // installed_schema_defs (all Schema + Substrate dependent).
            shared_protocol::MemoryPlane::Schema => {
                self.schemas.clear();
                self.schema_payloads.clear();
                self.installed_schema_defs.clear();
            }
            // S3.2: Controller plane invalidation clears route_statistics and
            // context_governor (both Controller-only deps, not cleared on substrate).
            shared_protocol::MemoryPlane::Controller => {
                self.route_statistics.clear();
                self.context_governor = ContextTreeGovernor::default();
            }
        }
        if payload.reset_predictors {
            self.predictors.clear();
        }
        // S1.2.f: Discard pending acks on plane resync as server state may have changed.
        // S3.2: pending_memory_acks depends on all planes, cleared on any resync.
        self.pending_memory_acks.clear();
    }

    pub fn apply_trace<I>(&mut self, records: I) -> Result<(), ClientApplyError>
    where
        I: IntoIterator<Item = Record>,
    {
        for record in records {
            self.apply_record(record)?;
        }
        Ok(())
    }

    pub fn apply_transport_frame(&mut self, frame: &[u8]) -> Result<usize, ClientApplyError> {
        let records = decode_transport_records(frame).map_err(ClientApplyError::TransportWire)?;
        let count = records.len();
        self.apply_trace(records)?;
        Ok(count)
    }

    pub fn apply_transport_frames<I, B>(&mut self, frames: I) -> Result<usize, ClientApplyError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut applied = 0_usize;
        for frame in frames {
            applied += self.apply_transport_frame(frame.as_ref())?;
        }
        Ok(applied)
    }

    pub fn predictor_state_for(&self, item_id: ItemId) -> PredictorState {
        shared_protocol::predictor_state_for(item_id, self.predictors.get(&item_id).copied())
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

    fn apply_data_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let header = record.header;
        if header.flags.contains(RecordFlags::IS_EVICT)
            || header.flags.contains(RecordFlags::IS_INVALIDATE)
        {
            self.cache.remove(&header.item_id);
            self.predictors.remove(&header.item_id);
            self.source_bindings.remove(&header.item_id);
            self.prune_resolved_repair_requests();
            return Ok(());
        }

        // Wave 13 T-13-c: BatchEnvelope records contain multiple items in a
        // single AEAD-protected payload. Decode the envelope, then apply each
        // item's compressed payload through the existing decompression path.
        if header.record_type == RecordType::BatchEnvelope {
            return self.apply_batch_envelope(header, record.payload);
        }

        let predictor_state = self.predictor_state_for(header.item_id);
        let predicted_exact_bytes = match predictor_state {
            PredictorState::Ready(_) => self
                .cache
                .get(&header.item_id)
                .map(|entry| entry.exact_bytes()),
            PredictorState::Empty => None,
        };
        let source_descriptor = self.source_bindings.get(&header.item_id).cloned();
        let object = decode_data_payload_to_runtime_object(
            &record.payload,
            header.codec_mode,
            predicted_exact_bytes,
            source_descriptor,
            shared_protocol::SourceKind::Binary,
            shared_protocol::ObjectKind::ExactState,
        )
        .map_err(|error| match error {
            shared_protocol::CodecError::MissingPredictor => {
                ClientApplyError::MissingPredictor(header.item_id)
            }
            other => ClientApplyError::DecodePayload(other),
        })?;

        if !matches!(
            header.codec_mode,
            CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
        ) {
            return Err(ClientApplyError::UnsupportedDataShape);
        }

        let source_kind = object.source_kind;
        // P0-P5: Check if the decoded exact_bytes are compressed (start with a
        // compression tag). If so, decompress them and create a new object with
        // the uncompressed data. The compression is transparent to the record
        // layer — the client just sees the decompressed result.
        let (decompressed_object, source_kind) = if shared_protocol::starts_with_compression_tag(&object.exact_bytes) {
            let decompressed_result = shared_protocol::decode_compressed_payload(
                &object.exact_bytes,
                &self.dict_manager,
                &self.template_registry,
                &self.previous_versions,
                source_kind,
            );
            let decompressed = match &decompressed_result {
                Ok(d) => d.clone(),
                Err(e) => {
                    // Decompression failed — this can happen if the client
                    // doesn't have the required base version for delta encoding
                    // (e.g., after a resync). Fall back to treating the payload
                    // as raw bytes without the two-byte compression prefix.
                    // Skip the [0xC0][tag] prefix and use the rest.
                    if object.exact_bytes.len() > 2 {
                        object.exact_bytes[2..].to_vec()
                    } else {
                        object.exact_bytes.clone()
                    }
                }
            };
            // Feed the decompressed sample to the client's dict_manager
            // so it can potentially train dictionaries for future use.
            self.dict_manager.add_sample(source_kind, &decompressed);
            // Try to register a template (JSON only).
            if source_kind == shared_protocol::SourceKind::Json {
                self.template_registry.try_register(source_kind, &decompressed);
            }
            // Try to train a dictionary.
            self.dict_manager.maybe_train(source_kind);
            // Reconstruct the object with decompressed bytes and correct metadata.
            let descriptor = self.source_bindings.get(&header.item_id).cloned()
                .unwrap_or(SourceDescriptor {
                    kind: source_kind,
                    source_hash: shared_protocol::compute_source_hash(
                        source_kind, None, &decompressed,
                    ),
                    byte_len: decompressed.len(),
                    mime: None,
                    label: None,
                });
            let cue = descriptor.structural_cue_summary(&decompressed).cue;
            let object_key = descriptor.runtime_object_key_from_bytes(&decompressed);
            let sk = descriptor.kind;
            let new_object = ChpmtObject::from_exact_bytes(
                descriptor, object_key, sk,
                shared_protocol::ObjectKind::ExactState, cue, decompressed,
            );
            // Store previous version for delta decoding.
            self.previous_versions.insert(header.item_id.0, new_object.exact_bytes.clone());
            (new_object, sk)
        } else {
            // Not compressed — store previous version for future delta decoding.
            self.previous_versions.insert(header.item_id.0, object.exact_bytes.clone());
            (object, source_kind)
        };
        let object = decompressed_object;
        let cue = object.cue;
        self.cache.insert(header.item_id, ClientEntry { object });
        // S3.1.a2: Model A — successful ExactState decode promotes reusable substrate.
        // The decoded exact bytes become available for future route planning
        // via the shared substrate catalog, making sender/receiver substrate
        // assumptions symmetric.
        // NOTE: Clone bytes before promoting to avoid borrowing self immutably
        // (cache.get) while calling self.promote_to_substrate_catalog (requires &mut self).
        {
            let exact_bytes = self.cache.get(&header.item_id)
                .map(|e| e.exact_bytes().to_vec());
            if let Some(bytes) = exact_bytes {
                self.promote_to_substrate_catalog(source_kind, &bytes);
            }
        }
        self.predictors.insert(
            header.item_id,
            PredictorEntryMeta {
                item_id: header.item_id,
                source_kind,
                object_kind: shared_protocol::ObjectKind::ExactState,
                cue,
            },
        );
        let object_ref = EpisodeObjectRef {
            object_kind: shared_protocol::ObjectKind::ExactState,
            object_id: format!("item:{}", header.item_id.0),
        };
        let context_hash = derive_context_hash(cue, &object_ref);
        self.append_episode_activation(EpisodeActivationEvent {
            source_kind,
            object_ref,
            cue,
            context_hash,
            tick: header.seq_no.0,
            success: true,
        });
        if header.record_type == RecordType::ExactState {
            self.record_route_outcome(ControllerRouteFamily::DirectState, header.seq_no.0, true);
        }
        self.prune_resolved_repair_requests();
        Ok(())
    }

    fn apply_source_meta_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let header = record.header;
        let payload =
            SourceMetaPayload::decode(&record.payload).map_err(ClientApplyError::SourceWire)?;
        self.source_bindings.insert(
            header.item_id,
            SourceDescriptor {
                kind: payload.source_kind,
                source_hash: payload.source_hash,
                byte_len: payload.source_len as usize,
                mime: payload.mime,
                label: payload.label,
            },
        );
        Ok(())
    }

    fn record_route_outcome(
        &mut self,
        route_family: ControllerRouteFamily,
        tick: u64,
        success: bool,
    ) {
        self.record_route_outcome_detail(
            route_family,
            tick,
            success,
            None,
            ContextTreeOutcome {
                success,
                ..ContextTreeOutcome::default()
            },
        );
    }

    fn record_route_outcome_detail(
        &mut self,
        route_family: ControllerRouteFamily,
        tick: u64,
        success: bool,
        symbol: Option<ContextTreeSymbol>,
        outcome: ContextTreeOutcome,
    ) {
        let key = format!("{}", route_family as u8);
        let stats = self.route_statistics.entry(key).or_insert(RouteStatistics {
            route_family,
            source_kind: None,
            context_hash: None,
            success_count: 0,
            failure_count: 0,
            last_seen_tick: 0,
        });
        stats.record(tick, success);
        if let Some(symbol) = symbol {
            self.context_governor.observe(symbol, outcome);
        }
        self.episode_memory
            .record_route_outcome(route_family, None, None, tick, success);
    }

    fn context_symbol_from_payload(
        &self,
        payload: &PredictiveRouteDispatchPayload,
        cue: SparseCue,
    ) -> ContextTreeSymbol {
        let slot_shape = payload
            .inline_schema_defs
            .first()
            .map(|def| SlotShapeBucket::from_slot_count(def.schema.slots.len()))
            .or_else(|| {
                payload
                    .inline_assembly_defs
                    .first()
                    .map(|def| SlotShapeBucket::from_slot_count(def.assembly.slots.len()))
            })
            .unwrap_or(SlotShapeBucket::None);
        let prior_schema_kind = payload
            .inline_schema_defs
            .first()
            .map(|def| def.schema.schema_kind);
        ContextTreeSymbol {
            route_family: payload.route_family,
            delimiter_class: DelimiterClass::from_cue(cue),
            slot_shape,
            prior_schema_kind,
            prior_transform_family: None,
            lag_bucket: LagBucket::default(),
            residual_bucket: ResidualSizeBucket::from_bytes(
                payload.contradiction_bytes.len().min(u32::MAX as usize) as u32,
            ),
            sync_state: SyncStateBucket::from_sync_risk(payload.sync_risk),
        }
    }

    /// S2.6.v2: Strict canonical prefix parsing — rejects legacy/ambiguous alternates.
    /// Only the canonical "schema:<id>" form is accepted. Permissive rsplit(':')
    /// parsing is no longer used because the sender now emits only canonical IDs.
    fn parse_schema_id(object_id: &str) -> Option<SchemaId> {
        object_id
            .strip_prefix("schema:")
            .and_then(|id| id.parse::<u64>().ok())
            .map(SchemaId)
    }

    /// S2.6.v2: Strict canonical prefix parsing — rejects legacy/ambiguous alternates.
    /// Only the canonical "transform:<id>" form is accepted.
    fn parse_transform_id(object_id: &str) -> Option<TransformId> {
        object_id
            .strip_prefix("transform:")
            .and_then(|id| id.parse::<u64>().ok())
            .map(TransformId)
    }

    /// S2.6.v2: Strict canonical prefix parsing for generic item references.
    /// Accepts canonical "block:<id>", "bundle:<id>", "assembly:<id>",
    /// "schema:<id>", "transform:<id>", "dictionary:<id>" forms.
    /// Legacy bare-numeric or non-canonical prefixed forms are rejected.
    fn parse_item_id(object_id: &str) -> Option<ItemId> {
        // Try each canonical prefix; the ID must follow the prefix as a decimal u64.
        const CANONICAL_PREFIXES: &[&str] = &[
            "block:", "bundle:", "assembly:", "schema:", "transform:", "dictionary:",
        ];
        for prefix in CANONICAL_PREFIXES {
            if let Some(id) = object_id.strip_prefix(prefix) {
                if let Ok(raw) = id.parse::<u64>() {
                    return Some(ItemId(raw));
                }
                return None; // prefix matched but id wasn't a valid u64 — reject
            }
        }
        None // no canonical prefix matched — reject
    }

    fn dependency_available(&self, dependency: &shared_protocol::ObjectDependency) -> bool {
        self.check_dependency_available(dependency).is_available()
    }

    /// S2.7.c: Returns distinct failure reasons for dependency availability checks.
    fn check_dependency_available(&self, dependency: &shared_protocol::ObjectDependency) -> DependencyAvailability {
        match dependency.object_kind {
            shared_protocol::ObjectKind::Assembly =>
                dependency
                .object_id
                .strip_prefix("assembly:")
                .and_then(|id| id.parse::<u64>().ok())
                .map(AssemblyId)
                .map(|id| {
                    if dependency.required_revision == 0 {
                        if self.assemblies.contains_key(&id) {
                            DependencyAvailability::Available
                        } else {
                            DependencyAvailability::MissingObject
                        }
                    } else {
                        match self.assembly_sync_versions.get(&id) {
                            Some(sig) if sig.version.object_revision >= dependency.required_revision => DependencyAvailability::Available,
                            Some(sig) => DependencyAvailability::WrongRevision { actual: sig.version.object_revision, required: dependency.required_revision },
                            None => DependencyAvailability::MissingObject,
                        }
                    }
                })
                .unwrap_or(DependencyAvailability::MalformedIdentity),
            shared_protocol::ObjectKind::Schema => Self::parse_schema_id(&dependency.object_id)
                .map(|id| {
                    if dependency.required_revision == 0 {
                        if self.schema_payloads.contains_key(&id) {
                            DependencyAvailability::Available
                        } else {
                            DependencyAvailability::MissingObject
                        }
                    } else {
                        match self.schema_payloads.get(&id) {
                            Some(payload) if payload.schema.dependency_closure.version.object_revision >= dependency.required_revision => DependencyAvailability::Available,
                            Some(payload) => DependencyAvailability::WrongRevision { actual: payload.schema.dependency_closure.version.object_revision, required: dependency.required_revision },
                            None => DependencyAvailability::MissingObject,
                        }
                    }
                })
                .unwrap_or(DependencyAvailability::MalformedIdentity),
            shared_protocol::ObjectKind::Transform => Self::parse_transform_id(&dependency.object_id)
                .map(|id| {
                    if dependency.required_revision == 0 {
                        if self.materialized_transforms.contains_key(&id) {
                            DependencyAvailability::Available
                        } else {
                            DependencyAvailability::MissingObject
                        }
                    } else {
                        // S2.7.c: TransformClass does not carry ObjectVersion revision info;
                        // fall back to presence-only check. If the class exists, assume available.
                        match self.transforms.get(&id) {
                            Some(_class) => DependencyAvailability::Available,
                            None => DependencyAvailability::MissingObject,
                        }
                    }
                })
                .unwrap_or(DependencyAvailability::MalformedIdentity),
            shared_protocol::ObjectKind::Dictionary =>
                dependency
                .object_id
                .strip_prefix("dictionary:")
                .and_then(|id| id.parse::<u64>().ok())
                .map(DictionaryId)
                .map(|id| {
                    if dependency.required_revision == 0 {
                        if self.dictionaries.contains_key(&id) {
                            DependencyAvailability::Available
                        } else {
                            DependencyAvailability::MissingObject
                        }
                    } else {
                        match self.dictionaries.get(&id) {
                            Some(dict) if dict.version.object_revision >= dependency.required_revision => DependencyAvailability::Available,
                            Some(dict) => DependencyAvailability::WrongRevision { actual: dict.version.object_revision, required: dependency.required_revision },
                            None => DependencyAvailability::MissingObject,
                        }
                    }
                })
                .unwrap_or(DependencyAvailability::MalformedIdentity),
            // For remaining kinds, delegate to existing bool check
            _ => {
                if self.dependency_available_bool(dependency) {
                    DependencyAvailability::Available
                } else {
                    DependencyAvailability::MissingObject
                }
            }
        }
    }

    /// S2.7.d: Revision-aware dependency check that first consults staged inline
    /// definitions, then falls back to the durable store. This makes the inline
    /// revision satisfaction path explicit rather than relying on the implicit
    /// side effect of temporarily installing staged defs into durable stores.
    fn check_dependency_available_with_staged(
        &self,
        dependency: &shared_protocol::ObjectDependency,
        staged: &StagedInlineRevisions,
    ) -> DependencyAvailability {
        match dependency.object_kind {
            shared_protocol::ObjectKind::Assembly => {
                dependency.object_id.strip_prefix("assembly:")
                    .and_then(|id| id.parse::<u64>().ok())
                    .map(AssemblyId)
                    .map(|id| {
                        // S2.7.d: Check staged inline revision first
                        if let Some(&revision) = staged.assembly_revisions.get(&id) {
                            if revision >= dependency.required_revision {
                                return DependencyAvailability::Available;
                            } else {
                                return DependencyAvailability::WrongRevision {
                                    actual: revision,
                                    required: dependency.required_revision,
                                };
                            }
                        }
                        // Fall back to durable store check
                        self.check_dependency_available(dependency)
                    })
                    .unwrap_or(DependencyAvailability::MalformedIdentity)
            }
            shared_protocol::ObjectKind::Schema => {
                Self::parse_schema_id(&dependency.object_id)
                    .map(|id| {
                        if let Some(&revision) = staged.schema_revisions.get(&id) {
                            if revision >= dependency.required_revision {
                                return DependencyAvailability::Available;
                            } else {
                                return DependencyAvailability::WrongRevision {
                                    actual: revision,
                                    required: dependency.required_revision,
                                };
                            }
                        }
                        self.check_dependency_available(dependency)
                    })
                    .unwrap_or(DependencyAvailability::MalformedIdentity)
            }
            shared_protocol::ObjectKind::Dictionary => {
                dependency.object_id.strip_prefix("dictionary:")
                    .and_then(|id| id.parse::<u64>().ok())
                    .map(DictionaryId)
                    .map(|id| {
                        if let Some(&revision) = staged.dictionary_revisions.get(&id) {
                            if revision >= dependency.required_revision {
                                return DependencyAvailability::Available;
                            } else {
                                return DependencyAvailability::WrongRevision {
                                    actual: revision,
                                    required: dependency.required_revision,
                                };
                            }
                        }
                        self.check_dependency_available(dependency)
                    })
                    .unwrap_or(DependencyAvailability::MalformedIdentity)
            }
            // For other kinds, delegate to the existing durable-store check
            _ => self.check_dependency_available(dependency),
        }
    }

    /// Original bool-based dependency check, retained for fallback in check_dependency_available.
    fn dependency_available_bool(&self, dependency: &shared_protocol::ObjectDependency) -> bool {
        match dependency.object_kind {
            shared_protocol::ObjectKind::ExactState
            | shared_protocol::ObjectKind::AtomFragment
            | shared_protocol::ObjectKind::PredictiveObject
            | shared_protocol::ObjectKind::SourceDescriptor
            | shared_protocol::ObjectKind::ResidualBuffer => {
                // S2.6: Use canonical prefix-based parsing for item IDs
                dependency
                    .object_id
                    .strip_prefix("item:")
                    .and_then(|id| id.parse::<u64>().ok())
                    .map(ItemId)
                    .map(|id| self.cache.contains_key(&id))
                    .unwrap_or(false)
            },
            shared_protocol::ObjectKind::ExactBlock => dependency
                .object_id
                .strip_prefix("block:")
                .and_then(|id| id.parse::<u64>().ok())
                .map(BlockId)
                .map(|id| self.catalog.contains_block(id))
                .unwrap_or(false),
            shared_protocol::ObjectKind::ExactBundle => dependency
                .object_id
                .strip_prefix("bundle:")
                .and_then(|id| id.parse::<u64>().ok())
                .map(shared_protocol::BundleId)
                .map(|id| self.catalog.contains_bundle(id))
                .unwrap_or(false),
            shared_protocol::ObjectKind::ExactRange => dependency
                .object_id
                .strip_prefix("range:")
                .and_then(|id| id.parse::<u64>().ok())
                .map(BlockId)
                .map(|id| self.catalog.contains_block(id))
                .unwrap_or(false),
            shared_protocol::ObjectKind::EpisodeHint | shared_protocol::ObjectKind::ReplayHint => self
                .installed_episode_hints
                .iter()
                .any(|hint| hint.context_hash.0.to_string() == dependency.object_id),
            shared_protocol::ObjectKind::SparseCue => Self::parse_item_id(&dependency.object_id)
                .map(|id| {
                    self.predictors.contains_key(&id)
                        || self.source_bindings.contains_key(&id)
                        || self.cache.contains_key(&id)
                })
                .unwrap_or_else(|| {
                    self.installed_episode_hints
                        .iter()
                        .any(|hint| hint.context_hash.0.to_string() == dependency.object_id)
                }),
            // Assembly, Schema, Transform, Dictionary are handled by check_dependency_available
            _ => false,
        }
    }

    fn derive_predictive_source_kind(
        &self,
        item_id: ItemId,
        payload: &PredictiveRouteDispatchPayload,
    ) -> shared_protocol::SourceKind {
        self.source_bindings
            .get(&item_id)
            .map(|descriptor| descriptor.kind)
            .or_else(|| payload.route_source_kind)
            .or_else(|| {
                payload
                    .inline_assembly_defs
                    .first()
                    .map(|def| def.assembly.source_kind)
            })
            .or_else(|| {
                payload
                    .inline_schema_defs
                    .first()
                    .map(|def| def.schema.source_kind)
            })
            .or_else(|| {
                if payload.route_family == ControllerRouteFamily::Assembly {
                    payload.assembly_ref.as_ref().and_then(|reference| {
                        self.assemblies
                            .get(&reference.assembly_id)
                            .map(|assembly| assembly.source_kind)
                    })
                } else {
                    None
                }
            })
            .unwrap_or(shared_protocol::SourceKind::Binary)
    }

    fn apply_predictive_dispatch_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        // P0: Decompress the payload if it's Zstd-compressed. The server
        // compresses PredictiveRouteDispatchPayload.encode() output with Zstd
        // before putting it into the record. We decompress here before
        // passing it to the bincode decoder.
        let record = record.maybe_decompress_zstd()
            .map_err(|e| ClientApplyError::StateWire(
                shared_protocol::StateProgramError::PredictiveDispatchSerialization(
                    format!("zstd decompression failed: {e}")
                )
            ))?;
        let header = record.header;
        let payload = match PredictiveRouteDispatchPayload::decode(&record.payload) {
            Ok(payload) => payload,
            Err(error) => {
                self.record_route_outcome(
                    ControllerRouteFamily::DirectState,
                    header.seq_no.0,
                    false,
                );
                return Err(ClientApplyError::StateWire(error));
            }
        };
        if header.record_type != payload.route_family.dispatch_record_type() {
            self.record_route_outcome(ControllerRouteFamily::DirectState, header.seq_no.0, false);
            return Err(ClientApplyError::PredictiveRouteRecordTypeMismatch {
                record_type: header.record_type,
                route_family: payload.route_family,
            });
        }
        let mut applied_cue = None;
        // S1.3: Stage inline definitions before mutating durable stores.
        // Inline defs are validated and held in staging structures, then
        // committed only after dispatch succeeds. On failure, staged defs
        // are discarded without leaving residue in durable stores.
        let staged_assemblies: Vec<_> = payload.inline_assembly_defs.clone();
        let staged_schemas: Vec<_> = payload.inline_schema_defs.clone();
        let staged_dictionaries: Vec<_> = payload.inline_dictionaries.clone();
        let staged_episode_hints: Vec<_> = payload.inline_episode_hints.clone();

        // S2.7.d: Build explicit staged revision tracker for dependency checking.
        // This makes revision-aware checks for inline-provided objects explicit
        // rather than relying on the implicit side effect of temporary installation
        // into durable stores.
        let mut staged_revisions = StagedInlineRevisions::default();
        for assembly_def in &staged_assemblies {
            staged_revisions.assembly_revisions.insert(
                assembly_def.assembly.assembly_id,
                assembly_def.assembly.dependency_closure.version.object_revision,
            );
        }
        for schema_def in &staged_schemas {
            staged_revisions.schema_revisions.insert(
                schema_def.schema.schema_id,
                schema_def.schema.dependency_closure.version.object_revision,
            );
        }
        for dict_def in &staged_dictionaries {
            staged_revisions.dictionary_revisions.insert(
                dict_def.dictionary.dictionary_id,
                dict_def.dictionary.version.object_revision,
            );
        }

        // Make staged defs visible to dependency resolution during dispatch
        // by temporarily installing them. If dispatch fails, we roll back.
        // Track what was newly installed so we can undo on failure.
        let mut installed_assembly_ids = Vec::new();
        let mut installed_schema_ids = Vec::new();
        let mut installed_dictionary_ids = Vec::new();
        let mut installed_episode_count = 0usize;

        let outcome = (|| {
            for assembly_def in &staged_assemblies {
                let id = assembly_def.assembly.assembly_id;
                self.install_assembly_payload(assembly_def.clone())?;
                installed_assembly_ids.push(id);
            }
            for schema_def in &staged_schemas {
                let id = schema_def.schema.schema_id;
                self.schemas.insert(id, schema_def.schema.clone());
                self.schema_payloads.insert(id, schema_def.clone());
                self.installed_schema_defs.push(schema_def.clone());
                installed_schema_ids.push(id);
            }
            for dictionary_def in &staged_dictionaries {
                let id = dictionary_def.dictionary.dictionary_id;
                self.dictionaries.insert(id, dictionary_def.dictionary.clone());
                installed_dictionary_ids.push(id);
            }
            for episode_hint in &staged_episode_hints {
                self.install_episode_hint_payload(
                    header.item_id,
                    header.seq_no.0,
                    episode_hint.clone(),
                    false,
                );
                installed_episode_count += 1;
            }
            let bytes = self.dispatch_predictive_payload(&payload, &staged_revisions)?;
            let source_kind = self.derive_predictive_source_kind(header.item_id, &payload);
            let cue = self
                .source_bindings
                .get(&header.item_id)
                .map(|descriptor| descriptor.structural_cue_summary(&bytes).cue)
                .unwrap_or_else(|| shared_protocol::derive_sparse_cue(source_kind, &bytes));
            applied_cue = Some(cue);
            let object = self.chpmt_object_for_item(
                header.item_id,
                &bytes,
                shared_protocol::ObjectKind::PredictiveObject,
            );
            self.cache.insert(header.item_id, ClientEntry { object });
            // S3.1.a2: Model A — successful predictive dispatch promotes reusable
            // substrate. The materialized bytes from dispatch become available for
            // future route planning via the shared substrate catalog.
            self.promote_to_substrate_catalog(source_kind, &bytes);
            self.predictors.insert(
                header.item_id,
                PredictorEntryMeta {
                    item_id: header.item_id,
                    source_kind,
                    object_kind: shared_protocol::ObjectKind::PredictiveObject,
                    cue,
                },
            );
            self.update_episode_memory_from_predictive_dispatch(
                header.item_id,
                header.seq_no.0,
                &payload,
                cue,
            );
            Ok(())
        })();

        if outcome.is_err() {
            // S1.2.v2: Debug assertion — failed dispatch MUST NOT produce MemoryAcks.
            // If this fires, it means the success path emitted acks before the
            // dispatch outcome was determined, which would cause the server to
            // promote pending state to confirmed for a failed decode.
            #[cfg(debug_assertions)]
            debug_assert!(self.pending_memory_acks.is_empty()
                || !self.pending_memory_acks.iter().any(|ack|
                    ack.acked_record_type == header.record_type
                    && ack.acked_seq_no == header.seq_no),
                "S1.2.v2: MemoryAck emitted for failed predictive dispatch — \
                 this would cause phantom confirmed state on the server");

            // S1.3: Roll back staged inline definitions on dispatch failure.
            // This prevents orphaned definitions from polluting durable state.
            for id in installed_assembly_ids {
                self.assemblies.remove(&id);
                self.installed_assembly_defs.retain(|def| def.assembly.assembly_id != id);
                self.assembly_index.retain(|_, aid| *aid != id);
                self.assembly_sync_versions.remove(&id);
            }
            for id in installed_schema_ids {
                self.schemas.remove(&id);
                self.schema_payloads.remove(&id);
                self.installed_schema_defs.retain(|def| def.schema.schema_id != id);
            }
            for id in installed_dictionary_ids {
                self.dictionaries.remove(&id);
            }
            // Roll back episode hints by removing the last N installed
            if installed_episode_count > 0 && self.installed_episode_hints.len() >= installed_episode_count {
                let new_len = self.installed_episode_hints.len() - installed_episode_count;
                self.installed_episode_hints.truncate(new_len);
            }
        } else {
            // S1.2: Emit MemoryAck for each successfully installed inline definition.
            // This confirms to the sender that the peer has the object installed,
            // enabling promotion from pending to confirmed on the server side.
            for id in &installed_assembly_ids {
                self.enqueue_memory_ack(MemoryAckPayload {
                    version: MemoryAckPayload::VERSION,
                    plane: shared_protocol::MemoryPlane::Assembly,
                    object_kind: shared_protocol::ObjectKind::Assembly,
                    object_id: format!("assembly:{}", id.0),
                    acked_record_type: record.header.record_type,
                    acked_seq_no: record.header.seq_no,
                });
            }
            for id in &installed_schema_ids {
                self.enqueue_memory_ack(MemoryAckPayload {
                    version: MemoryAckPayload::VERSION,
                    plane: shared_protocol::MemoryPlane::Schema,
                    object_kind: shared_protocol::ObjectKind::Schema,
                    object_id: format!("schema:{}", id.0),
                    acked_record_type: record.header.record_type,
                    acked_seq_no: record.header.seq_no,
                });
            }
            for id in &installed_dictionary_ids {
                self.enqueue_memory_ack(MemoryAckPayload {
                    version: MemoryAckPayload::VERSION,
                    plane: shared_protocol::MemoryPlane::AtomSubstrate,
                    object_kind: shared_protocol::ObjectKind::Dictionary,
                    object_id: format!("dictionary:{}", id.0),
                    acked_record_type: record.header.record_type,
                    acked_seq_no: record.header.seq_no,
                });
            }
        }

        let symbol = self.context_symbol_from_payload(&payload, applied_cue.unwrap_or_default());
        self.record_route_outcome_detail(
            payload.route_family,
            header.seq_no.0,
            outcome.is_ok(),
            Some(symbol),
            ContextTreeOutcome {
                success: outcome.is_ok(),
                fallback: false,
                sync_failure: matches!(
                    &outcome,
                    Err(ClientApplyError::MissingPredictiveDependency(_))
                ),
                contradiction_mass: payload.contradiction_bytes.len().min(u32::MAX as usize) as u32,
            },
        );
        outcome
    }

    /// S2.7.d: Accepts optional staged inline revisions so that inline-provided
    /// objects can satisfy revision-aware dependency checks explicitly, rather
    /// than relying on the implicit side effect of temporary durable-store installation.
    fn missing_predictive_dependencies(
        &self,
        payload: &PredictiveRouteDispatchPayload,
        staged: Option<&StagedInlineRevisions>,
    ) -> Vec<String> {
        payload
            .dependency_closure
            .iter()
            .filter(|dependency| {
                if let Some(staged) = staged {
                    !self.check_dependency_available_with_staged(dependency, staged).is_available()
                } else {
                    !self.dependency_available(dependency)
                }
            })
            .map(|dependency| {
                format!(
                    "{:?}:{}@{}",
                    dependency.object_kind,
                    dependency.object_id,
                    dependency.required_revision
                )
            })
            .collect()
    }

    fn dispatch_predictive_payload(
        &mut self,
        payload: &PredictiveRouteDispatchPayload,
        staged: &StagedInlineRevisions,
    ) -> Result<Vec<u8>, ClientApplyError> {
        if payload.route_family == ControllerRouteFamily::Assembly {
            match payload
                .assembly_mode
                .or_else(|| payload.derived_assembly_mode())
            {
                Some(shared_protocol::AssemblyRouteMode::DefineAndActivate)
                | Some(shared_protocol::AssemblyRouteMode::ReuseReference) => {
                    if let Some(reference) = &payload.assembly_ref {
                        if let Some(bytes) = self.resolve_assembly_reference(reference) {
                            return Ok(bytes);
                        }
                    }
                }
                _ => {}
            }
        }
        // S2.7.d: Use staged inline revisions for explicit revision-aware dependency checks
        let missing_dependencies = self.missing_predictive_dependencies(payload, Some(staged));
        if !missing_dependencies.is_empty() {
            return Err(ClientApplyError::MissingPredictiveDependency(
                missing_dependencies.join(", "),
            ));
        }
        // S2.7.d: The available closure also uses staged inline revisions so that
        // hybrid route validation can see inline-provided objects explicitly.
        let available =
            |kind: shared_protocol::ObjectKind, object_id: &str, revision: u32| -> bool {
                self.check_dependency_available_with_staged(
                    &shared_protocol::ObjectDependency {
                        object_kind: kind,
                        object_id: object_id.to_string(),
                        required_revision: revision,
                    },
                    staged,
                ).is_available()
            };
        if let Some(route) = &payload.hybrid_route {
            if !route.validate_dependencies(available) {
                return Err(ClientApplyError::MissingPredictiveDependency(format!(
                    "hybrid route validation failed for {:?} with closure [{}]",
                    payload.route_family,
                    payload
                        .dependency_closure
                        .iter()
                        .map(|dependency| format!(
                            "{:?}:{}@{}",
                            dependency.object_kind,
                            dependency.object_id,
                            dependency.required_revision
                        ))
                        .collect::<Vec<_>>()
                        .join(", "),
                )));
            }
            let bytes = execute_hybrid_route(
                route,
                |reference| self.resolve_substrate_reference(reference),
                |reference| self.resolve_assembly_reference(reference),
                |transform_id| self.resolve_transform_output(transform_id),
                |dictionary_id, token_ids| self.resolve_dictionary_tokens(dictionary_id, token_ids),
                |episode_ref| self.resolve_episode_reference(episode_ref),
                |schema_id| self.resolve_schema_output(schema_id),
            )
            .map_err(ClientApplyError::StateWire)?;
            return apply_exact_contradiction_mask_to_bytes(bytes, &payload.contradiction_bytes)
                .map_err(ClientApplyError::StateWire);
        }
        if let Some(prg) = &payload.prg {
            let bytes = execute_prg(
                prg,
                |reference| self.resolve_substrate_reference(reference),
                |reference| self.resolve_assembly_reference(reference),
                |transform_id| self.resolve_transform_output(transform_id),
                |episode_ref| self.resolve_episode_reference(episode_ref),
                |schema_id| self.resolve_schema_output(schema_id),
            )
            .map_err(ClientApplyError::StateWire)?;
            return apply_exact_contradiction_mask_to_bytes(bytes, &payload.contradiction_bytes)
                .map_err(ClientApplyError::StateWire);
        }
        if !payload.literal_bytes.is_empty() {
            return apply_exact_contradiction_mask_to_bytes(
                payload.literal_bytes.clone(),
                &payload.contradiction_bytes,
            )
            .map_err(ClientApplyError::StateWire);
        }
        Err(ClientApplyError::MissingPredictiveDependency(format!(
            "route family {:?} had no decodable dispatch body",
            payload.route_family,
        )))
    }

    fn update_episode_memory_from_predictive_dispatch(
        &mut self,
        item_id: ItemId,
        tick: u64,
        payload: &PredictiveRouteDispatchPayload,
        cue: shared_protocol::SparseCue,
    ) {
        let source_kind = self
            .source_bindings
            .get(&item_id)
            .map(|descriptor| descriptor.kind)
            .unwrap_or(shared_protocol::SourceKind::Binary);
        let object_ref =
            self.primary_episode_object_ref(payload)
                .unwrap_or_else(|| EpisodeObjectRef {
                    object_kind: shared_protocol::ObjectKind::PredictiveObject,
                    object_id: format!("state:{}", item_id.0),
                });
        let context_hash = derive_context_hash(cue, &object_ref);
        self.append_episode_activation(EpisodeActivationEvent {
            source_kind,
            object_ref,
            cue,
            context_hash,
            tick,
            success: true,
        });
    }

    fn primary_episode_object_ref(
        &self,
        payload: &PredictiveRouteDispatchPayload,
    ) -> Option<EpisodeObjectRef> {
        if let Some(prg) = &payload.prg {
            for node in &prg.nodes {
                if let Some(reference) = &node.episode_ref {
                    return Some(reference.clone());
                }
            }
        }
        if let Some(route) = &payload.hybrid_route {
            for component in &route.components {
                if let HybridRouteComponent::Schema(graph) = component {
                    for node in &graph.nodes {
                        if let Some(reference) = &node.episode_ref {
                            return Some(reference.clone());
                        }
                    }
                }
            }
        }
        payload.inline_episode_hints.iter().find_map(|hint| {
            hint.candidates
                .first()
                .map(|candidate| candidate.object_ref.clone())
        })
    }

    fn resolve_substrate_reference(
        &self,
        reference: &shared_protocol::SubstrateRef,
    ) -> Option<Vec<u8>> {
        match reference.object_kind {
            shared_protocol::ObjectKind::AtomFragment | shared_protocol::ObjectKind::ExactState => {
                self.cache
                    .get(&ItemId(reference.object_id))
                    .map(|entry| entry.exact_bytes().to_vec())
                    .and_then(|bytes| {
                        if reference.start == 0 && reference.len == 0 {
                            Some(bytes)
                        } else {
                            slice_component_bytes(&bytes, reference.start, reference.len)
                        }
                    })
            }
            shared_protocol::ObjectKind::ExactBlock => self
                .catalog
                .block(BlockId(reference.object_id))
                .and_then(|entry| {
                    if reference.start == 0
                        && (reference.len == 0 || reference.len as usize == entry.material.len())
                    {
                        Some(entry.material.clone())
                    } else {
                        slice_component_bytes(&entry.material, reference.start, reference.len)
                    }
                }),
            shared_protocol::ObjectKind::ExactRange => self
                .catalog
                .materialize_block_range(
                    BlockId(reference.object_id),
                    reference.start,
                    if reference.len == 0 {
                        u32::MAX
                    } else {
                        reference.len
                    },
                )
                .ok(),
            shared_protocol::ObjectKind::ExactBundle => self
                .catalog
                .materialize_bundle(shared_protocol::BundleId(reference.object_id))
                .ok()
                .and_then(|material| {
                    if reference.start == 0
                        && (reference.len == 0 || reference.len as usize == material.len())
                    {
                        Some(material)
                    } else {
                        slice_component_bytes(&material, reference.start, reference.len)
                    }
                }),
            shared_protocol::ObjectKind::ResidualBuffer => self
                .cache
                .get(&ItemId(reference.object_id))
                .map(|entry| entry.exact_bytes().to_vec())
                .and_then(|bytes| {
                    if reference.start == 0 && reference.len == 0 {
                        Some(bytes)
                    } else {
                        slice_component_bytes(&bytes, reference.start, reference.len)
                    }
                }),
            _ => None,
        }
    }

    fn resolve_episode_reference(&self, object_ref: &EpisodeObjectRef) -> Option<Vec<u8>> {
        match object_ref.object_kind {
            shared_protocol::ObjectKind::Assembly => object_ref
                .object_id
                .rsplit(':')
                .next()
                .and_then(|id| id.parse::<u64>().ok())
                .map(AssemblyId)
                .and_then(|assembly_id| {
                    let assembly = self.assemblies.get(&assembly_id)?;
                    expand_assembly_body(&assembly.body, |component| {
                        self.resolve_assembly_component(component)
                    })
                    .ok()
                }),
            shared_protocol::ObjectKind::Transform => {
                Self::parse_transform_id(&object_ref.object_id)
                    .and_then(|id| self.resolve_transform_output(id))
            }
            shared_protocol::ObjectKind::Schema => Self::parse_schema_id(&object_ref.object_id)
                .and_then(|id| self.resolve_schema_output(id)),
            shared_protocol::ObjectKind::ExactState
            | shared_protocol::ObjectKind::PredictiveObject => object_ref
                .object_id
                .rsplit(':')
                .next()
                .and_then(|id| id.parse::<u64>().ok())
                .map(ItemId)
                .and_then(|item_id| {
                    self.cache
                        .get(&item_id)
                        .map(|entry| entry.exact_bytes().to_vec())
                }),
            shared_protocol::ObjectKind::EpisodeHint | shared_protocol::ObjectKind::ReplayHint => {
                self.installed_episode_hints.iter().rev().find_map(|hint| {
                    hint.candidates
                        .iter()
                        .find(|candidate| candidate.object_ref.object_id == object_ref.object_id)
                        .and_then(|candidate| self.resolve_episode_reference(&candidate.object_ref))
                })
            }
            _ => None,
        }
    }

    fn apply_assembly_def_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let payload = decode_assembly_def_record(&record).map_err(ClientApplyError::StateWire)?;
        self.install_assembly_payload(payload)?;
        Ok(())
    }

    fn install_assembly_payload(
        &mut self,
        payload: AssemblyDefPayload,
    ) -> Result<(), ClientApplyError> {
        let install_plan = payload.install_plan();
        if install_plan.output_len != payload.assembly.body.output_len() {
            return Err(ClientApplyError::StateWire(
                shared_protocol::StateProgramError::PrgNodeOutputLenMismatch {
                    node_id: 0,
                    expected: install_plan.output_len,
                    actual: payload.assembly.body.output_len(),
                },
            ));
        }
        let assembly = payload.assembly.clone();
        self.assembly_index.insert(
            format!("assembly:{}", assembly.assembly_id.0),
            assembly.assembly_id,
        );
        self.assembly_sync_versions.insert(
            assembly.assembly_id,
            shared_protocol::assembly_reuse_signature(&assembly),
        );
        self.assemblies
            .insert(assembly.assembly_id, assembly.clone());
        poll_immediate(self.assembly_cache.store_assembly(&assembly))
            .map_err(|error| ClientApplyError::AssemblyCache(error.to_string()))?;
        self.installed_assembly_defs.push(payload);
        Ok(())
    }


    fn resolve_assembly_reference(
        &self,
        reference: &shared_protocol::AssemblyRef,
    ) -> Option<Vec<u8>> {
        let synced = self.assembly_sync_versions.get(&reference.assembly_id)?;
        if synced.version.object_revision < reference.version.object_revision {
            return None;
        }
        if synced.body_shape_hash != reference.body_shape_hash
            || synced.dependency_fingerprint != reference.dependency_fingerprint
            || synced.output_len != reference.output_len
        {
            return None;
        }
        let assembly = self.assemblies.get(&reference.assembly_id)?;
        let materialized_signature = shared_protocol::assembly_reuse_signature(assembly);
        if materialized_signature.body_shape_hash != reference.body_shape_hash
            || materialized_signature.dependency_fingerprint != reference.dependency_fingerprint
            || materialized_signature.output_len != reference.output_len
            || materialized_signature.version.object_revision < reference.version.object_revision
        {
            return None;
        }
        expand_assembly_body(&assembly.body, |component| {
            self.resolve_assembly_component(component)
        })
        .ok()
    }

    fn resolve_assembly_component(&self, component: &AssemblyComponentRef) -> Option<Vec<u8>> {
        match component {
            AssemblyComponentRef::AtomFragment {
                object_id,
                start,
                len,
            }
            | AssemblyComponentRef::ExactBlock {
                object_id,
                start,
                len,
            }
            | AssemblyComponentRef::ExactRange {
                object_id,
                start,
                len,
            }
            | AssemblyComponentRef::ResidualBuffer {
                object_id,
                start,
                len,
            } => self.resolve_component_object(object_id, *start, *len),
            AssemblyComponentRef::ExactBundle {
                object_id,
                start,
                len,
            } => self.resolve_component_bundle(object_id, *start, *len),
        }
    }

    fn resolve_component_object(&self, object_id: &str, start: u32, len: u32) -> Option<Vec<u8>> {
        let item_id = object_id
            .rsplit(':')
            .next()
            .and_then(|id| id.parse::<u64>().ok())
            .map(ItemId);
        let bytes = item_id
            .and_then(|item_id| {
                self.cache
                    .get(&item_id)
                    .map(|entry| entry.exact_bytes().to_vec())
            })
            .or_else(|| {
                object_id
                    .parse::<u64>()
                    .ok()
                    .map(BlockId)
                    .and_then(|block_id| {
                        self.catalog
                            .blocks_iter()
                            .find(|entry| entry.block_id == block_id)
                            .map(|entry| entry.material.clone())
                    })
            })?;
        slice_component_bytes(&bytes, start, len)
    }

    fn resolve_component_bundle(&self, object_id: &str, start: u32, len: u32) -> Option<Vec<u8>> {
        let bundle_id = object_id
            .rsplit(':')
            .next()
            .and_then(|id| id.parse::<u64>().ok())
            .map(BundleId)
            .or_else(|| object_id.parse::<u64>().ok().map(BundleId))?;
        let bytes = self.catalog.materialize_bundle(bundle_id).ok()?;
        slice_component_bytes(&bytes, start, len)
    }

    fn retire_assembly_object(&mut self, object_id: &str) {
        let Some(assembly_id) = object_id
            .rsplit(':')
            .next()
            .and_then(|id| id.parse::<u64>().ok())
            .map(AssemblyId)
        else {
            return;
        };
        self.assemblies.remove(&assembly_id);
        self.assembly_index
            .remove(&format!("assembly:{}", assembly_id.0));
        self.assembly_sync_versions.remove(&assembly_id);
        self.installed_assembly_defs
            .retain(|payload| payload.assembly.assembly_id != assembly_id);
    }

    fn clear_assembly_plane(&mut self) {
        self.assemblies.clear();
        self.assembly_index.clear();
        self.assembly_sync_versions.clear();
        self.installed_assembly_defs.clear();
        self.assembly_cache = WebSourceCache::new(WebSourceCacheConfig::default());
    }

    fn apply_transform_def_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let payload = decode_transform_def_record(&record).map_err(ClientApplyError::StateWire)?;
        self.transforms
            .insert(payload.class.transform_id, payload.class);
        Ok(())
    }

    fn apply_transform_instance_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let header = record.header;
        let payload: TransformInstancePayload = match decode_transform_instance_record(&record) {
            Ok(payload) => payload,
            Err(error) => {
                self.record_route_outcome(ControllerRouteFamily::Transform, header.seq_no.0, false);
                return Err(ClientApplyError::StateWire(error));
            }
        };
        let outcome = (|| {
            let class = self
                .transforms
                .get(&payload.class_id)
                .ok_or(ClientApplyError::MissingTransformClass(payload.class_id))?;
            let bytes = apply_transform_instance(class, &payload.instance(), |basis| {
                self.resolve_transform_basis(basis)
            })
            .map_err(ClientApplyError::StateWire)?;
            self.materialized_transforms
                .insert(payload.class_id, bytes.clone());
            let source_kind = self
                .source_bindings
                .get(&header.item_id)
                .map(|descriptor| descriptor.kind)
                .unwrap_or(shared_protocol::SourceKind::Binary);
            self.cache.insert(
                header.item_id,
                ClientEntry {
                    object: self.chpmt_object_for_item(
                        header.item_id,
                        &bytes,
                        shared_protocol::ObjectKind::PredictiveObject,
                    ),
                },
            );
            self.predictors.insert(
                header.item_id,
                PredictorEntryMeta {
                    item_id: header.item_id,
                    source_kind,
                    object_kind: shared_protocol::ObjectKind::PredictiveObject,
                    cue: self
                        .source_bindings
                        .get(&header.item_id)
                        .map(|descriptor| descriptor.structural_cue_summary(&bytes).cue)
                        .unwrap_or_default(),
                },
            );
            let cue = class.cue;
            let object_ref = EpisodeObjectRef::transform(class.transform_id);
            let context_hash = derive_context_hash(cue, &object_ref);
            self.append_episode_activation(EpisodeActivationEvent {
                source_kind: class.source_kind,
                object_ref,
                cue,
                context_hash,
                tick: header.seq_no.0,
                success: true,
            });
            Ok(())
        })();
        self.record_route_outcome(
            ControllerRouteFamily::Transform,
            header.seq_no.0,
            outcome.is_ok(),
        );
        outcome
    }

    fn apply_episode_hint_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let payload = decode_episode_hint_record(&record).map_err(ClientApplyError::StateWire)?;
        self.install_episode_hint_payload(
            record.header.item_id,
            record.header.seq_no.0,
            payload,
            false,
        );
        Ok(())
    }

    fn apply_replay_hint_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let payload = decode_replay_hint_record(&record).map_err(ClientApplyError::StateWire)?;
        self.enqueue_memory_ack(MemoryAckPayload {
            version: MemoryAckPayload::VERSION,
            plane: shared_protocol::MemoryPlane::Episode,
            object_kind: shared_protocol::ObjectKind::ReplayHint,
            object_id: format!("replay:{}", payload.context_hash.0),
            acked_record_type: RecordType::ReplayHint,
            acked_seq_no: record.header.seq_no,
        });
        self.install_episode_hint_payload(
            record.header.item_id,
            record.header.seq_no.0,
            payload,
            true,
        );
        Ok(())
    }

    fn install_episode_hint_payload(
        &mut self,
        item_id: ItemId,
        tick: u64,
        payload: EpisodeHintPayload,
        replay_hint: bool,
    ) {
        self.installed_episode_hints.push(payload.clone());
        let default_source_kind = self
            .source_bindings
            .get(&item_id)
            .map(|descriptor| descriptor.kind)
            .unwrap_or(shared_protocol::SourceKind::Binary);
        for candidate in &payload.candidates {
            let object_ref = if replay_hint {
                EpisodeObjectRef {
                    object_kind: shared_protocol::ObjectKind::ReplayHint,
                    object_id: format!(
                        "replay:{}:{}",
                        payload.context_hash.0, candidate.object_ref.object_id
                    ),
                }
            } else {
                candidate.object_ref.clone()
            };
            let source_kind =
                self.infer_episode_source_kind(&candidate.object_ref, default_source_kind);
            let cue = self.derive_episode_cue(&candidate.object_ref, source_kind);
            self.append_episode_activation(EpisodeActivationEvent {
                source_kind,
                context_hash: payload.context_hash,
                cue,
                object_ref,
                tick,
                success: true,
            });
        }
    }

    fn infer_episode_source_kind(
        &self,
        object_ref: &EpisodeObjectRef,
        fallback: shared_protocol::SourceKind,
    ) -> shared_protocol::SourceKind {
        match object_ref.object_kind {
            shared_protocol::ObjectKind::Assembly => object_ref
                .object_id
                .rsplit(':')
                .next()
                .and_then(|id| id.parse::<u64>().ok())
                .map(AssemblyId)
                .and_then(|id| {
                    self.assemblies
                        .get(&id)
                        .map(|assembly| assembly.source_kind)
                })
                .unwrap_or(fallback),
            shared_protocol::ObjectKind::Transform => {
                Self::parse_transform_id(&object_ref.object_id)
                    .and_then(|id| self.transforms.get(&id).map(|class| class.source_kind))
                    .unwrap_or(fallback)
            }
            shared_protocol::ObjectKind::Schema => Self::parse_schema_id(&object_ref.object_id)
                .and_then(|id| {
                    self.schema_payloads
                        .get(&id)
                        .map(|payload| payload.schema.source_kind)
                })
                .unwrap_or(fallback),
            shared_protocol::ObjectKind::ExactState
            | shared_protocol::ObjectKind::PredictiveObject => object_ref
                .object_id
                .rsplit(':')
                .next()
                .and_then(|id| id.parse::<u64>().ok())
                .map(ItemId)
                .and_then(|item_id| {
                    self.source_bindings
                        .get(&item_id)
                        .map(|descriptor| descriptor.kind)
                })
                .unwrap_or(fallback),
            _ => fallback,
        }
    }

    fn derive_episode_cue(
        &self,
        object_ref: &EpisodeObjectRef,
        source_kind: shared_protocol::SourceKind,
    ) -> shared_protocol::SparseCue {
        self.resolve_episode_reference(object_ref)
            .map(|bytes| shared_protocol::derive_sparse_cue(source_kind, &bytes))
            .unwrap_or_else(|| {
                shared_protocol::derive_sparse_cue(source_kind, object_ref.object_id.as_bytes())
            })
    }

    fn apply_schema_def_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let payload = decode_schema_def_record(&record).map_err(ClientApplyError::StateWire)?;
        self.schemas
            .insert(payload.schema.schema_id, payload.schema.clone());
        self.schema_payloads
            .insert(payload.schema.schema_id, payload.clone());
        self.installed_schema_defs.push(payload.clone());
        let cue = payload.schema.cue;
        let object_ref = EpisodeObjectRef::schema(format!("schema:{}", payload.schema.schema_id.0));
        let context_hash = derive_context_hash(cue, &object_ref);
        self.append_episode_activation(EpisodeActivationEvent {
            source_kind: payload.schema.source_kind,
            object_ref,
            cue,
            context_hash,
            tick: record.header.seq_no.0,
            success: true,
        });
        Ok(())
    }

    fn apply_repair_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        match RepairPayload::decode(&record.payload).map_err(ClientApplyError::Catalog)? {
            RepairPayload::Request { request, .. } => {
                self.enqueue_repair_request(request);
                Ok(())
            }
            RepairPayload::Response { sync, .. } => {
                self.catalog
                    .apply_sync(sync)
                    .map_err(ClientApplyError::Catalog)?;
                self.prune_resolved_repair_requests();
                Ok(())
            }
        }
    }

    fn resolve_transform_basis(&self, basis: &TransformBasisRef) -> Option<Vec<u8>> {
        match basis.object_kind {
            shared_protocol::ObjectKind::ExactBlock => {
                let block_id = basis
                    .object_id
                    .strip_prefix("block:")?
                    .parse::<u64>()
                    .ok()?;
                self.catalog
                    .block(BlockId(block_id))
                    .map(|entry| entry.material.clone())
            }
            shared_protocol::ObjectKind::ExactBundle => {
                let bundle_id = basis
                    .object_id
                    .strip_prefix("bundle:")?
                    .parse::<u64>()
                    .ok()?;
                self.catalog.materialize_bundle(BundleId(bundle_id)).ok()
            }
            _ => None,
        }
        .map(|bytes| {
            let start = basis.start as usize;
            let end = start.saturating_add(basis.len as usize).min(bytes.len());
            bytes.get(start..end).map(|s| s.to_vec()).unwrap_or(bytes)
        })
    }

    fn resolve_transform_output(&self, transform_id: TransformId) -> Option<Vec<u8>> {
        self.materialized_transforms.get(&transform_id).cloned()
    }

    fn resolve_dictionary_tokens(
        &self,
        dictionary_id: DictionaryId,
        token_ids: &[u16],
    ) -> Option<Vec<u8>> {
        self.dictionaries
            .get(&dictionary_id)
            .and_then(|dictionary| dictionary.decode_tokens(token_ids))
    }

    fn resolve_schema_output(&self, schema_id: SchemaId) -> Option<Vec<u8>> {
        let mut active = std::collections::BTreeSet::new();
        self.resolve_schema_output_inner(schema_id, &mut active)
            .ok()
    }

    fn resolve_schema_output_inner(
        &self,
        schema_id: SchemaId,
        active: &mut std::collections::BTreeSet<SchemaId>,
    ) -> Result<Vec<u8>, shared_protocol::StateProgramError> {
        if !active.insert(schema_id) {
            return Err(shared_protocol::StateProgramError::PrgCycleDetected(
                schema_id.0 as u32,
            ));
        }
        let payload = self
            .schema_payloads
            .get(&schema_id)
            .ok_or(ClientApplyError::MissingSchema(schema_id))
            .map_err(|err| match err {
                ClientApplyError::MissingSchema(id) => {
                    shared_protocol::StateProgramError::PrgMissingDependency(id.0.to_string())
                }
                _ => unreachable!(),
            })?;
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
            .collect::<std::collections::BTreeSet<_>>();
        let root_node_id = nodes
            .iter()
            .find(|node| !child_node_ids.contains(&node.node_id))
            .map(|node| node.node_id)
            .unwrap_or_else(|| nodes.first().map(|node| node.node_id).unwrap_or_default());
        let graph = shared_protocol::PredictiveReconstructionGraph::new(
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
        );
        let result = execute_prg(
            &graph,
            |reference| self.resolve_substrate_reference(reference),
            |reference| self.resolve_assembly_reference(reference),
            |transform_id| self.resolve_transform_output(transform_id),
            |episode_ref| self.resolve_episode_reference(episode_ref),
            |nested_schema_id| {
                self.resolve_schema_output_inner(nested_schema_id, active)
                    .ok()
            },
        );
        active.remove(&schema_id);
        result
    }

    fn enqueue_repair_request(&mut self, request: RepairRequest) {
        if !self
            .repair_requests
            .iter()
            .any(|existing| existing == &request)
        {
            self.repair_requests.push(request);
        }
    }

    fn prune_resolved_repair_requests(&mut self) {
        let catalog = &self.catalog;
        let cache = &self.cache;
        self.repair_requests
            .retain(|request| !repair_request_is_resolved_against(catalog, cache, request));
    }
}

fn repair_request_is_resolved_against(
    catalog: &SharedBlockCatalog,
    cache: &HashMap<ItemId, ClientEntry>,
    request: &RepairRequest,
) -> bool {
    match &request.cause {
        RepairCause::MissingBasis | RepairCause::CatalogVersionMismatch { .. } => {
            request
                .missing_blocks
                .iter()
                .all(|block_id| catalog.contains_block(*block_id))
                && request
                    .missing_bundles
                    .iter()
                    .all(|bundle_id| catalog.contains_bundle(*bundle_id))
        }
        RepairCause::UnsupportedStateReference { detail } => !detail.split(", ").any(|part| {
            part.strip_prefix("missing prior state ")
                .and_then(|value| value.parse::<u64>().ok())
                .map(|item_id| !cache.contains_key(&ItemId(item_id)))
                .unwrap_or(false)
        }),
    }
}

impl ClientSession {
    fn header_for_outbound_control_record(
        &self,
        record_type: RecordType,
        item_id: ItemId,
        payload_len: usize,
    ) -> RecordHeader {
        RecordHeader {
            version: shared_protocol::PROTOCOL_VERSION,
            stream_id: self.protector.stream_id(),
            epoch_id: self.protector.expected_epoch_id(),
            seq_no: self.protector.expected_seq_no(),
            record_type,
            codec_mode: CodecMode::None,
            flags: {
                let mut flags = RecordFlags::empty();
                if self.protector.post_rekey_confirm_required() {
                    flags.insert(RecordFlags::POST_REKEY_CONFIRM);
                }
                flags
            },
            item_id,
            payload_len: payload_len as u32,
        }
    }

    pub fn new(protector: StreamProtector) -> Self {
        Self {
            state: ClientState::default(),
            protector,
        }
    }

    pub fn state(&self) -> &ClientState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut ClientState {
        &mut self.state
    }

    pub fn protector(&self) -> &StreamProtector {
        &self.protector
    }

    pub fn protector_mut(&mut self) -> &mut StreamProtector {
        &mut self.protector
    }

    pub fn apply_protected_record(&mut self, record: Record) -> Result<(), ClientApplyError> {
        let record = self
            .protector
            .unprotect_record(record)
            .map_err(ClientApplyError::Protection)?;
        self.state.apply_record(record)
    }

    pub fn unprotect_transport_frame(
        &mut self,
        frame: &[u8],
    ) -> Result<Vec<Record>, ClientApplyError> {
        self.protector
            .unprotect_transport_frame(frame)
            .map_err(ClientApplyError::Protection)
    }

    pub fn apply_unprotected_trace<I>(&mut self, records: I) -> Result<(), ClientApplyError>
    where
        I: IntoIterator<Item = Record>,
    {
        self.state.apply_trace(records)
    }

    pub fn apply_protected_trace<I>(&mut self, records: I) -> Result<(), ClientApplyError>
    where
        I: IntoIterator<Item = Record>,
    {
        for record in records {
            self.apply_protected_record(record)?;
        }
        Ok(())
    }

    pub fn apply_protected_transport_frame(
        &mut self,
        frame: &[u8],
    ) -> Result<usize, ClientApplyError> {
        let records = self.unprotect_transport_frame(frame)?;
        let count = records.len();
        self.apply_unprotected_trace(records)?;
        Ok(count)
    }

    pub fn apply_protected_transport_frames<I, B>(
        &mut self,
        frames: I,
    ) -> Result<usize, ClientApplyError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut applied = 0_usize;
        for frame in frames {
            applied += self.apply_protected_transport_frame(frame.as_ref())?;
        }
        Ok(applied)
    }

    pub fn protect_record(&mut self, record: Record) -> Result<Record, ClientApplyError> {
        self.protector
            .protect_record(record)
            .map_err(ClientApplyError::Protection)
    }

    pub fn emit_next_memory_ack(&mut self) -> Result<Option<Record>, ClientApplyError> {
        let Some(payload) = self.state.pop_next_memory_ack() else {
            return Ok(None);
        };
        let payload_bytes = payload
            .encode()
            .map_err(|error| ClientApplyError::PredictiveDispatch(error.to_string()))?;
        let record = Record {
            header: self.header_for_outbound_control_record(
                RecordType::MemoryAck,
                ItemId(1),
                payload_bytes.len(),
            ),
            payload: payload_bytes,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
        .validate()
        .map_err(ClientApplyError::Validation)?;
        self.protect_record(record).map(Some)
    }

    pub fn emit_pending_memory_acks(&mut self) -> Result<Vec<Record>, ClientApplyError> {
        let mut out = Vec::new();
        while let Some(record) = self.emit_next_memory_ack()? {
            out.push(record);
        }
        Ok(out)
    }

    pub fn protect_trace<I>(&mut self, records: I) -> Result<Vec<Record>, ClientApplyError>
    where
        I: IntoIterator<Item = Record>,
    {
        records
            .into_iter()
            .map(|record| self.protect_record(record))
            .collect()
    }

    pub fn pack_protected_trace<I>(
        &mut self,
        records: I,
        config: TransportConfig,
    ) -> Result<Vec<Vec<u8>>, ClientApplyError>
    where
        I: IntoIterator<Item = Record>,
    {
        shared_protocol::pack_record_groups(records, config)
            .into_iter()
            .map(|group| {
                self.protector
                    .protect_transport_records(group)
                    .map_err(ClientApplyError::Protection)
            })
            .collect()
    }
}

impl CredentialProvider {
    pub fn memory(issued_credential: IssuedClientCredential) -> Self {
        Self { issued_credential }
    }

    pub fn issued_credential(&self) -> &IssuedClientCredential {
        &self.issued_credential
    }
}

impl ReconnectPolicy {
    pub fn disabled() -> Self {
        Self {
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            max_attempts: 1,
        }
    }
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 250,
            max_backoff_ms: 4_000,
            max_attempts: 4,
        }
    }
}

impl ClientConnectConfig {
    pub fn bootstrap_client_config(&self) -> BootstrapClientConfig {
        BootstrapClientConfig {
            stream_id: self.stream_id,
            direction: self.direction,
            bootstrap: self.session.bootstrap,
            security: self.security.clone(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConnectedWebSocketSession {
    pub fn session(&self) -> &ClientSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        &mut self.session
    }

    pub fn websocket_mut(&mut self) -> &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>> {
        &mut self.websocket
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.transport_config
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        self.websocket.send(Message::Binary(frame.into())).await?;
        Ok(())
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for frame in frames {
            self.websocket
                .send(Message::Binary(frame.as_ref().to_vec().into()))
                .await?;
        }
        Ok(())
    }

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = self
            .session
            .pack_protected_trace(records, self.transport_config)
            .map_err(ClientConnectError::Apply)?;
        self.send_transport_frames(frames).await
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        let mut applied = 0_usize;
        while let Some(frame) = self.read_transport_frame().await? {
            applied += self
                .session
                .apply_protected_transport_frame(&frame)
                .map_err(ClientConnectError::Apply)?;
        }
        Ok(applied)
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        loop {
            match self.websocket.next().await {
                Some(Ok(Message::Binary(frame))) => return Ok(Some(frame.to_vec())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => return Err(ClientConnectError::WebSocket(error)),
                None => return Ok(None),
            }
        }
    }

    pub async fn close(mut self) -> Result<(), ClientConnectError> {
        self.websocket.close(None).await?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConnectedTcpSession {
    pub fn session(&self) -> &ClientSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        &mut self.session
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.transport_config
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        write_length_prefixed_frame(&mut self.stream_writer, &frame).await?;
        Ok(())
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for frame in frames {
            write_length_prefixed_frame(&mut self.stream_writer, frame.as_ref()).await?;
        }
        Ok(())
    }

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = self
            .session
            .pack_protected_trace(records, self.transport_config)
            .map_err(ClientConnectError::Apply)?;
        self.send_transport_frames(frames).await
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        let mut applied = 0_usize;
        while let Some(frame) = self.read_transport_frame().await? {
            applied += self
                .session
                .apply_protected_transport_frame(&frame)
                .map_err(ClientConnectError::Apply)?;
        }
        Ok(applied)
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        Ok(
            read_length_prefixed_frame(&mut self.stream_reader, self.max_transport_frame_bytes)
                .await?,
        )
    }

    pub async fn close(mut self) -> Result<(), ClientConnectError> {
        self.stream_writer.shutdown().await?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl ConnectedQuicSession {
    pub fn session(&self) -> &ClientSession {
        &self.session
    }

    pub fn session_mut(&mut self) -> &mut ClientSession {
        &mut self.session
    }

    pub fn connection(&self) -> &QuicConnection {
        &self._connection
    }

    pub fn endpoint(&self) -> &QuicEndpoint {
        &self._endpoint
    }

    pub fn transport_config(&self) -> TransportConfig {
        self.transport_config
    }

    pub async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), ClientConnectError> {
        write_length_prefixed_frame(&mut self.send, &frame)
            .await
            .map_err(|error| ClientConnectError::Quic(error.to_string()))
    }

    pub async fn send_transport_frames<I, B>(&mut self, frames: I) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        for frame in frames {
            write_length_prefixed_frame(&mut self.send, frame.as_ref()).await?;
        }
        Ok(())
    }

    pub async fn send_plain_records_packed<I>(
        &mut self,
        records: I,
    ) -> Result<(), ClientConnectError>
    where
        I: IntoIterator<Item = Record>,
    {
        let frames = self
            .session
            .pack_protected_trace(records, self.transport_config)
            .map_err(ClientConnectError::Apply)?;
        self.send_transport_frames(frames).await
    }

    pub async fn receive_until_close(&mut self) -> Result<usize, ClientConnectError> {
        let mut applied = 0_usize;
        while let Some(frame) = self.read_transport_frame().await? {
            applied += self
                .session
                .apply_protected_transport_frame(&frame)
                .map_err(ClientConnectError::Apply)?;
        }
        Ok(applied)
    }

    pub async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, ClientConnectError> {
        read_quic_length_prefixed_frame(&mut self.recv, self.max_transport_frame_bytes)
            .await
            .map_err(ClientConnectError::Quic)
    }

    pub async fn close(mut self) -> Result<(), ClientConnectError> {
        self.send
            .finish()
            .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
        let _ = self.send.stopped().await;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn connect_websocket_session(
    config: &ClientConnectConfig,
) -> Result<ConnectedWebSocketSession, ClientConnectError> {
    let attempts = config.reconnect_policy.max_attempts.max(1);
    let mut backoff_ms = config.reconnect_policy.initial_backoff_ms.max(1);
    let mut last_error = None;

    for attempt in 0..attempts {
        match connect_websocket_session_once(config).await {
            Ok(session) => return Ok(session),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < attempts {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2))
                        .min(config.reconnect_policy.max_backoff_ms.max(backoff_ms));
                }
            }
        }
    }

    Err(last_error.expect("at least one connect attempt was executed"))
}

#[cfg(not(target_arch = "wasm32"))]
async fn connect_websocket_session_once(
    config: &ClientConnectConfig,
) -> Result<ConnectedWebSocketSession, ClientConnectError> {
    let (websocket, _) =
        connect_async_with_config(&config.url, Some(websocket_config()), false).await?;
    let mut carrier = WebSocketClientCarrier { websocket };
    let session = bootstrap_client_over_carrier(&mut carrier, config).await?;
    Ok(ConnectedWebSocketSession {
        session,
        websocket: carrier.websocket,
        transport_config: config.session.transport,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn connect_tcp_session(
    config: &ClientConnectConfig,
) -> Result<ConnectedTcpSession, ClientConnectError> {
    let attempts = config.reconnect_policy.max_attempts.max(1);
    let mut backoff_ms = config.reconnect_policy.initial_backoff_ms.max(1);
    let mut last_error = None;

    for attempt in 0..attempts {
        match connect_tcp_session_once(config).await {
            Ok(session) => return Ok(session),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < attempts {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2))
                        .min(config.reconnect_policy.max_backoff_ms.max(backoff_ms));
                }
            }
        }
    }

    Err(last_error.expect("at least one connect attempt was executed"))
}

#[cfg(not(target_arch = "wasm32"))]
async fn connect_tcp_session_once(
    config: &ClientConnectConfig,
) -> Result<ConnectedTcpSession, ClientConnectError> {
    let endpoint = normalize_endpoint(&config.url, "tcp://");
    let stream = TcpStream::connect(endpoint).await?;
    let (stream_reader, stream_writer) = tokio::io::split(stream);
    let mut carrier = TcpClientCarrier {
        stream_reader,
        stream_writer,
    };
    let session = bootstrap_client_over_carrier(&mut carrier, config).await?;
    Ok(ConnectedTcpSession {
        session,
        stream_reader: carrier.stream_reader,
        stream_writer: carrier.stream_writer,
        transport_config: config.session.transport,
        max_transport_frame_bytes: config.session.runtime_limits.max_transport_frame_bytes,
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn connect_quic_session(
    config: &ClientConnectConfig,
) -> Result<ConnectedQuicSession, ClientConnectError> {
    let attempts = config.reconnect_policy.max_attempts.max(1);
    let mut backoff_ms = config.reconnect_policy.initial_backoff_ms.max(1);
    let mut last_error = None;

    for attempt in 0..attempts {
        match connect_quic_session_once(config).await {
            Ok(session) => return Ok(session),
            Err(error) => {
                last_error = Some(error);
                if attempt + 1 < attempts {
                    sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2))
                        .min(config.reconnect_policy.max_backoff_ms.max(backoff_ms));
                }
            }
        }
    }

    Err(last_error.expect("at least one connect attempt was executed"))
}

#[cfg(not(target_arch = "wasm32"))]
async fn connect_quic_session_once(
    config: &ClientConnectConfig,
) -> Result<ConnectedQuicSession, ClientConnectError> {
    ensure_insecure_quic_host_allowed(&config.url)?;
    let bind_addr = "[::]:0".parse().map_err(|error| {
        ClientConnectError::Quic(format!("invalid QUIC client bind addr: {error}"))
    })?;
    let mut endpoint = QuicEndpoint::client(bind_addr)?;
    endpoint.set_default_client_config(make_benchmark_only_insecure_quic_client_config(
        config.session.runtime_limits.max_transport_frame_bytes,
    )?);
    let address = tokio::net::lookup_host(normalize_endpoint(&config.url, "quic://"))
        .await?
        .next()
        .ok_or_else(|| ClientConnectError::Quic("no QUIC remote address resolved".to_string()))?;
    let server_name = quic_server_name(&config.url);
    let connection = endpoint
        .connect(address, &server_name)
        .map_err(|error| ClientConnectError::Quic(error.to_string()))?
        .await
        .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
    let (send, recv) = connection
        .open_bi()
        .await
        .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
    let mut carrier = QuicClientCarrier {
        endpoint,
        connection,
        send,
        recv,
    };
    let session = bootstrap_client_over_carrier(&mut carrier, config).await?;
    Ok(ConnectedQuicSession {
        session,
        _endpoint: carrier.endpoint,
        _connection: carrier.connection,
        send: carrier.send,
        recv: carrier.recv,
        transport_config: config.session.transport,
        max_transport_frame_bytes: config.session.runtime_limits.max_transport_frame_bytes,
    })
}

fn slice_component_bytes(bytes: &[u8], start: u32, len: u32) -> Option<Vec<u8>> {
    let start = start as usize;
    let len = len as usize;
    let end = start.checked_add(len)?;
    bytes.get(start..end).map(|slice| slice.to_vec())
}

fn poll_immediate<F>(future: F) -> F::Output
where
    F: Future,
{
    fn noop_raw_waker() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            noop_raw_waker()
        }
        fn wake(_: *const ()) {}
        fn wake_by_ref(_: *const ()) {}
        fn drop(_: *const ()) {}
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
        )
    }
    let waker = unsafe { Waker::from_raw(noop_raw_waker()) };
    let mut context = Context::from_waker(&waker);
    let mut future = Pin::from(Box::new(future));
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("client assembly cache future unexpectedly pending"),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientApplyError {
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error(transparent)]
    TransportWire(shared_protocol::WireError),
    #[error(transparent)]
    Protection(shared_protocol::ProtectionError),
    #[error("payload decode failed: {0}")]
    DecodePayload(shared_protocol::CodecError),
    #[error("state payload decode failed: {0}")]
    StateWire(shared_protocol::StateProgramError),
    #[error("catalog payload failed: {0}")]
    Catalog(shared_protocol::CatalogError),
    #[error("state basis repair required: {0:?}")]
    RepairRequired(RepairRequest),
    #[error("missing predictor state for item {0:?}")]
    MissingPredictor(ItemId),
    #[error(transparent)]
    SourceWire(shared_protocol::SourceError),
    #[error("unsupported DATA record shape")]
    UnsupportedDataShape,
    #[error("missing transform class {0:?}")]
    MissingTransformClass(TransformId),
    #[error("missing schema {0:?}")]
    MissingSchema(SchemaId),
    #[error("missing predictive dependency for dispatched route: {0}")]
    MissingPredictiveDependency(String),
    #[error(
        "predictive route record type mismatch: record={record_type:?}, family={route_family:?}"
    )]
    PredictiveRouteRecordTypeMismatch {
        record_type: RecordType,
        route_family: ControllerRouteFamily,
    },
    #[error("predictive dispatch failed: {0}")]
    PredictiveDispatch(String),
    #[error("assembly cache failed: {0}")]
    AssemblyCache(String),
}

/// S2.7.c: Distinct dependency availability failure reasons
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyAvailability {
    Available,
    MissingObject,
    WrongRevision { actual: u32, required: u32 },
    MalformedIdentity,
}

impl DependencyAvailability {
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }
}

/// S2.7.d: Tracks revision information for staged inline definitions during
/// predictive dispatch validation, making revision-aware dependency checks
/// explicit for inline-provided objects. Rather than relying on the implicit
/// side effect of temporarily installing inline defs into durable stores,
/// this struct captures revision metadata upfront so that dependency checks
/// can explicitly consult staged revisions before falling back to durable state.
#[derive(Debug, Clone, Default)]
struct StagedInlineRevisions {
    assembly_revisions: HashMap<AssemblyId, u32>,
    schema_revisions: HashMap<SchemaId, u32>,
    dictionary_revisions: HashMap<DictionaryId, u32>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
pub enum ClientConnectError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),
    #[error(transparent)]
    Bootstrap(#[from] shared_protocol::BootstrapError),
    #[error(transparent)]
    Apply(#[from] ClientApplyError),
    #[error(transparent)]
    Datagram(#[from] shared_protocol::DatagramSessionError),
    #[error("bootstrap timed out after {0} ms")]
    HandshakeTimeout(u64),
    #[error("security profile mismatch: session={session:?}, security={security:?}")]
    SecurityProfileMismatch {
        session: ProtectionProfileKind,
        security: ProtectionProfileKind,
    },
    #[error("quic error: {0}")]
    Quic(String),
    #[error("webtransport error: {0}")]
    WebTransport(String),
    #[error("refusing insecure QUIC client configuration for public host `{0}`")]
    InsecureQuicHostRejected(String),
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ReliableCarrier for WebSocketClientCarrier {
    type Error = ClientConnectError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        self.websocket
            .send(Message::Binary(frame.to_vec().into()))
            .await?;
        Ok(())
    }

    async fn recv_frame(&mut self, _max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.websocket.next().await {
                Some(Ok(Message::Binary(frame))) => return Ok(Some(frame.to_vec())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => return Err(ClientConnectError::WebSocket(error)),
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.websocket.close(None).await?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ReliableCarrier for TcpClientCarrier {
    type Error = ClientConnectError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        write_length_prefixed_frame(&mut self.stream_writer, frame).await?;
        Ok(())
    }

    async fn recv_frame(&mut self, max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(read_length_prefixed_frame(&mut self.stream_reader, max_frame_len).await?)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.stream_writer.shutdown().await?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl ReliableCarrier for QuicClientCarrier {
    type Error = ClientConnectError;

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Self::Error> {
        write_length_prefixed_frame(&mut self.send, frame)
            .await
            .map_err(|error| ClientConnectError::Quic(error.to_string()))
    }

    async fn recv_frame(&mut self, max_frame_len: usize) -> Result<Option<Vec<u8>>, Self::Error> {
        read_quic_length_prefixed_frame(&mut self.recv, max_frame_len)
            .await
            .map_err(ClientConnectError::Quic)
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.send
            .finish()
            .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
        let _ = self.send.stopped().await;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct WebSocketClientCarrier {
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct TcpClientCarrier {
    stream_reader: ReadHalf<TcpStream>,
    stream_writer: WriteHalf<TcpStream>,
}

#[cfg(not(target_arch = "wasm32"))]
struct QuicClientCarrier {
    endpoint: QuicEndpoint,
    connection: QuicConnection,
    send: SendStream,
    recv: RecvStream,
}

#[cfg(not(target_arch = "wasm32"))]
async fn bootstrap_client_over_carrier<C>(
    carrier: &mut C,
    config: &ClientConnectConfig,
) -> Result<ClientSession, ClientConnectError>
where
    C: ReliableCarrier<Error = ClientConnectError>,
{
    let security_profile = match &config.security {
        ClientSecurityConfig::PqMutual { .. } => ProtectionProfileKind::PqMutualV1,
        ClientSecurityConfig::PqSimple => ProtectionProfileKind::PqSimpleV1,
    };
    if config.session.protection_profile != security_profile {
        return Err(ClientConnectError::SecurityProfileMismatch {
            session: config.session.protection_profile,
            security: security_profile,
        });
    }

    let mut client_nonce = [0_u8; shared_protocol::BOOTSTRAP_NONCE_LEN];
    let mut client_kem_seed = [0_u8; shared_protocol::BOOTSTRAP_KEM_SEED_LEN];
    OsRng.fill_bytes(&mut client_nonce);
    OsRng.fill_bytes(&mut client_kem_seed);

    let (mut bootstrap_state, client_hello) = ClientBootstrapState::start(
        config.bootstrap_client_config(),
        client_nonce,
        client_kem_seed,
    )?;
    send_bootstrap_message(carrier, &client_hello, config.session.bootstrap).await?;
    let server_hello = receive_bootstrap_message(
        carrier,
        config.session.bootstrap,
        first_server_message_kind(config.session.protection_profile),
    )
    .await?;
    let progress = bootstrap_state.handle_server_hello(server_hello, unix_time_secs())?;
    let completed = if let Some(outbound) = progress.outbound {
        send_bootstrap_message(carrier, &outbound, config.session.bootstrap).await?;
        let server_finish = receive_bootstrap_message(
            carrier,
            config.session.bootstrap,
            shared_protocol::BootstrapMessageKind::ServerFinish,
        )
        .await?;
        bootstrap_state.handle_server_finish(server_finish)?
    } else {
        progress.completed.ok_or_else(|| {
            ClientConnectError::Bootstrap(shared_protocol::BootstrapError::UnexpectedMessageKind {
                expected: first_server_message_kind(config.session.protection_profile),
                actual: first_server_message_kind(config.session.protection_profile),
            })
        })?
    };
    Ok(ClientSession::new(protector_from_completed(&completed)))
}

#[cfg(not(target_arch = "wasm32"))]
async fn send_bootstrap_message<C>(
    carrier: &mut C,
    message: &BootstrapMessage,
    config: shared_protocol::BootstrapConfig,
) -> Result<(), ClientConnectError>
where
    C: ReliableCarrier<Error = ClientConnectError>,
{
    carrier.send_frame(&message.to_frame(&config)?).await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn receive_bootstrap_message<C>(
    carrier: &mut C,
    config: shared_protocol::BootstrapConfig,
    expected_kind: shared_protocol::BootstrapMessageKind,
) -> Result<BootstrapMessage, ClientConnectError>
where
    C: ReliableCarrier<Error = ClientConnectError>,
{
    timeout(Duration::from_millis(config.handshake_timeout_ms), async {
        match carrier.recv_frame(config.max_bootstrap_frame_bytes).await? {
            Some(frame) => {
                BootstrapMessage::from_frame(&frame, &config).map_err(ClientConnectError::Bootstrap)
            }
            None => Err(ClientConnectError::Bootstrap(
                shared_protocol::BootstrapError::UnexpectedMessageKind {
                    expected: expected_kind,
                    actual: expected_kind,
                },
            )),
        }
    })
    .await
    .map_err(|_| ClientConnectError::HandshakeTimeout(config.handshake_timeout_ms))?
}

#[cfg(not(target_arch = "wasm32"))]
fn first_server_message_kind(
    protection_profile: ProtectionProfileKind,
) -> shared_protocol::BootstrapMessageKind {
    match protection_profile.canonical_stream_profile() {
        ProtectionProfileKind::PqMutualV1 => shared_protocol::BootstrapMessageKind::ServerHello,
        ProtectionProfileKind::PqSimpleV1 => {
            shared_protocol::BootstrapMessageKind::SimpleServerHello
        }
        ProtectionProfileKind::ClassicRef1 => shared_protocol::BootstrapMessageKind::ServerHello,
        ProtectionProfileKind::PqSimpleDgramV1 | ProtectionProfileKind::PqMutualDgramV1 => {
            unreachable!("canonical_stream_profile removes datagram variants")
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn normalize_endpoint<'a>(value: &'a str, scheme_prefix: &str) -> &'a str {
    value.strip_prefix(scheme_prefix).unwrap_or(value)
}

#[cfg(not(target_arch = "wasm32"))]
fn quic_server_name(value: &str) -> String {
    match endpoint_host(value) {
        Some(host) if !host.is_empty() => host,
        _ => "localhost".to_string(),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn endpoint_host(value: &str) -> Option<String> {
    let trimmed = value
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(value);
    let authority = trimmed.split('/').next().unwrap_or(trimmed);
    if authority.is_empty() {
        return None;
    }

    if let Some(stripped) = authority.strip_prefix('[') {
        let (host, _) = stripped.split_once(']')?;
        return Some(host.to_string());
    }

    if let Ok(socket_addr) = authority.parse::<SocketAddr>() {
        return Some(socket_addr.ip().to_string());
    }

    authority
        .rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .or_else(|| Some(authority.to_string()))
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_insecure_quic_host_allowed(value: &str) -> Result<(), ClientConnectError> {
    let Some(host) = endpoint_host(value) else {
        return Err(ClientConnectError::Quic(
            "missing QUIC host for insecure client configuration".to_string(),
        ));
    };

    if quic_host_is_private_or_loopback(&host) {
        return Ok(());
    }

    Err(ClientConnectError::InsecureQuicHostRejected(host))
}

#[cfg(not(target_arch = "wasm32"))]
fn quic_host_is_private_or_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }

    let Ok(ip) = host.parse::<IpAddr>() else {
        return false;
    };

    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback() || ip.is_private() || ip.is_link_local() || ip.is_unspecified()
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Builds a QUIC client config that intentionally disables certificate verification.
///
/// This is restricted to benchmark/development traffic by `ensure_insecure_quic_host_allowed`,
/// and must not be repurposed as a general transport security configuration.
pub(crate) fn make_benchmark_only_insecure_quic_client_config(
    max_transport_frame_bytes: usize,
) -> Result<quinn::ClientConfig, ClientConnectError> {
    ensure_rustls_crypto_provider()?;
    let crypto = RustlsClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|error| ClientConnectError::Quic(error.to_string()))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(build_quic_transport_config(
        max_transport_frame_bytes,
    )?));
    Ok(config)
}

#[cfg(not(target_arch = "wasm32"))]
fn build_quic_transport_config(
    max_transport_frame_bytes: usize,
) -> Result<QuicTransportConfig, ClientConnectError> {
    let stream_window = max_transport_frame_bytes
        .saturating_mul(4)
        .max(8 * 1024 * 1024);
    let connection_window = stream_window.saturating_mul(4);
    let datagram_window = max_transport_frame_bytes.saturating_mul(4).max(64 * 1024);
    let mut transport = QuicTransportConfig::default();
    transport
        .max_idle_timeout(Some(
            QuicIdleTimeout::try_from(Duration::from_secs(120))
                .map_err(|error| ClientConnectError::Quic(error.to_string()))?,
        ))
        .keep_alive_interval(Some(Duration::from_secs(5)))
        .stream_receive_window(
            QuicVarInt::try_from(stream_window)
                .map_err(|error| ClientConnectError::Quic(error.to_string()))?,
        )
        .receive_window(
            QuicVarInt::try_from(connection_window)
                .map_err(|error| ClientConnectError::Quic(error.to_string()))?,
        )
        .send_window(connection_window as u64);
    transport
        .datagram_receive_buffer_size(Some(datagram_window))
        .datagram_send_buffer_size(datagram_window);
    Ok(transport)
}

#[cfg(not(target_arch = "wasm32"))]
fn invalid_quic_frame_length(frame_len: usize, max_frame_len: usize) -> String {
    if frame_len == 0 {
        "frame length 0 is invalid".to_string()
    } else {
        format!("frame length {frame_len} exceeds max {max_frame_len}")
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn read_quic_length_prefixed_frame(
    recv: &mut RecvStream,
    max_frame_len: usize,
) -> Result<Option<Vec<u8>>, String> {
    let mut len_bytes = [0_u8; 4];
    match recv.read_exact(&mut len_bytes).await {
        Ok(()) => {}
        Err(QuicReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(QuicReadExactError::ReadError(QuicReadError::ConnectionLost(_)))
        | Err(QuicReadExactError::ReadError(QuicReadError::ClosedStream)) => return Ok(None),
        Err(error) => return Err(format!("{error:?}")),
    }

    let frame_len = u32::from_le_bytes(len_bytes) as usize;
    if frame_len == 0 || frame_len > max_frame_len {
        return Err(invalid_quic_frame_length(frame_len, max_frame_len));
    }

    let mut frame = vec![0_u8; frame_len];
    recv.read_exact(&mut frame)
        .await
        .map_err(|error| format!("{error:?}"))?;
    Ok(Some(frame))
}

#[cfg(not(target_arch = "wasm32"))]
fn ensure_rustls_crypto_provider() -> Result<(), ClientConnectError> {
    static PROVIDER: OnceLock<Result<(), String>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                rustls::crypto::aws_lc_rs::default_provider()
                    .install_default()
                    .map_err(|error| format!("{error:?}"))?;
            }
            Ok(())
        })
        .as_ref()
        .map_err(|error| ClientConnectError::Quic(error.clone()))?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct SkipServerVerification;

#[cfg(not(target_arch = "wasm32"))]
impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn protector_from_completed(completed: &BootstrapCompleted) -> StreamProtector {
    StreamProtector::from_bootstrap_root(
        completed.protection_profile,
        completed.stream_id,
        completed.direction,
        completed.root,
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn unix_time_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_protocol::{
        BlockCatalogEntry, BlockCatalogVersion, BundleMember, CatalogSyncPayload, EpochId,
        ExactStateMaterial, RecordHeader, RepairPayload, SeqNo, SourceKind,
        StreamDirection, StreamId, TransportConfig, TransportMode, assembly_ref_from_payload,
        classic_ref1_pair_from_static_secrets, encode_delta_material, encode_delta_payload,
        encode_exact_payload, pack_records,
    };

    fn sample_block(seed: f32) -> ExactStateMaterial {
        let mut bytes = Vec::with_capacity(256);
        for index in 0..256 {
            let value = (((index as f32 + 1.0) * seed).sin() * 127.0).round() as i16;
            bytes.push((value & 0xff) as u8);
        }
        ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, &bytes)
    }

    fn raw_record(
        stream_id: StreamId,
        seq_no: SeqNo,
        item_id: ItemId,
        block: &ExactStateMaterial,
    ) -> Record {
        let payload = encode_exact_payload(block);
        Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id,
                epoch_id: EpochId(0),
                seq_no,
                record_type: RecordType::ExactState,
                codec_mode: CodecMode::DirectExact,
                flags: RecordFlags::empty(),
                item_id,
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
    }

    fn resync_record(seq_no: SeqNo) -> Record {
        Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no,
                record_type: RecordType::Resync,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: ItemId(0),
                payload_len: 0,
            },
            payload: Vec::new(),
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
    }

    fn replay_hint_record(seq_no: SeqNo, item_id: ItemId, payload: &EpisodeHintPayload) -> Record {
        shared_protocol::encode_replay_hint_record(
            RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no,
                record_type: RecordType::ReplayHint,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id,
                payload_len: 0,
            },
            payload,
        )
        .unwrap()
    }

    fn repair_record(seq_no: SeqNo, payload: RepairPayload) -> Record {
        let payload = payload.encode().unwrap();
        Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no,
                record_type: RecordType::Repair,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id: ItemId(0),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
    }

    fn assembly_def_record(seq_no: SeqNo, item_id: ItemId, assembly: Assembly) -> Record {
        let payload = AssemblyDefPayload::new(assembly).encode().unwrap();
        Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no,
                record_type: RecordType::AssemblyDef,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id,
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
    }

    fn predictive_confirm_record(
        seq_no: SeqNo,
        item_id: ItemId,
        payload: PredictiveRouteDispatchPayload,
    ) -> Record {
        let payload = payload.encode().unwrap();
        Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no,
                record_type: RecordType::PredictiveConfirm,
                codec_mode: CodecMode::None,
                flags: RecordFlags::empty(),
                item_id,
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        }
    }

    #[test]
    fn client_installs_assembly_defs_and_executes_hybrid_assembly_routes() {
        let mut state = ClientState::default();
        let assembly = Assembly {
            assembly_id: AssemblyId(77),
            source_kind: shared_protocol::SourceKind::Text,
            assembly_kind: shared_protocol::AssemblyKind::ContiguousMotif,
            role_signature: shared_protocol::RoleSignature::default(),
            slots: Vec::new(),
            body: shared_protocol::AssemblyBody::from_literal(b"assembly-plane".to_vec()),
            dependency_closure: shared_protocol::DependencyClosure::default(),
            cue: shared_protocol::derive_sparse_cue(
                shared_protocol::SourceKind::Text,
                b"assembly-plane",
            ),
            lifecycle: shared_protocol::ObjectLifecycleMeta::default(),
            canonical_length_min: 14,
            canonical_length_max: 14,
        };
        state
            .apply_record(assembly_def_record(SeqNo(0), ItemId(5), assembly.clone()))
            .unwrap();
        assert_eq!(state.installed_assembly_defs().len(), 1);

        let assembly_ref = assembly_ref_from_payload(&AssemblyDefPayload::new(assembly.clone()));
        let payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::Assembly,
            route_kind: shared_protocol::RouteFamily::Assembly,
            route_source_kind: Some(shared_protocol::SourceKind::Text),
            assembly_mode: None,
            precision_band: shared_protocol::PrecisionBand::Exact,
            dependency_closure: vec![shared_protocol::ObjectDependency {
                object_kind: shared_protocol::ObjectKind::Assembly,
                object_id: format!("assembly:{}", assembly.assembly_id.0),
                required_revision: 0,
            }],
            sync_risk: 0,
            literal_bytes: Vec::new(),
            assembly_ref: Some(assembly_ref.clone()),
            inline_assembly_defs: Vec::new(),
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: Vec::new(),
            route_graph: shared_protocol::RouteGraphContract::default(),
            contradiction_bytes: Vec::new(),
            prg: None,
            hybrid_route: Some(shared_protocol::HybridRoute {
                route_family: ControllerRouteFamily::Assembly,
                precision_band: shared_protocol::PrecisionBand::Exact,
                assembly_mode: None,
                output_len: assembly.body.output_len(),
                dependency_closure: vec![shared_protocol::ObjectDependency {
                    object_kind: shared_protocol::ObjectKind::Assembly,
                    object_id: format!("assembly:{}", assembly.assembly_id.0),
                    required_revision: 0,
                }],
                components: vec![HybridRouteComponent::Assembly(assembly_ref)],
            }),
        }
        .with_derived_route_graph();
        state
            .apply_record(predictive_confirm_record(SeqNo(1), ItemId(5), payload))
            .unwrap();
        assert_eq!(
            state.cache_entry(ItemId(5)).unwrap().object.exact_bytes,
            b"assembly-plane"
        );
    }

    #[test]
    fn client_reloads_assembly_from_cache_and_resync_clears_plane() {
        let mut state = ClientState::default();
        let assembly = Assembly {
            assembly_id: AssemblyId(88),
            source_kind: shared_protocol::SourceKind::Text,
            assembly_kind: shared_protocol::AssemblyKind::ContiguousMotif,
            role_signature: shared_protocol::RoleSignature::default(),
            slots: Vec::new(),
            body: shared_protocol::AssemblyBody::from_literal(b"cache-backed".to_vec()),
            dependency_closure: shared_protocol::DependencyClosure::default(),
            cue: shared_protocol::derive_sparse_cue(
                shared_protocol::SourceKind::Text,
                b"cache-backed",
            ),
            lifecycle: shared_protocol::ObjectLifecycleMeta::default(),
            canonical_length_min: 12,
            canonical_length_max: 12,
        };
        state
            .apply_record(assembly_def_record(SeqNo(0), ItemId(9), assembly.clone()))
            .unwrap();
        state.assemblies.clear();

        let assembly_def_payload = AssemblyDefPayload::new(assembly.clone());
        let assembly_ref = assembly_ref_from_payload(&assembly_def_payload);
        let dependency = shared_protocol::ObjectDependency {
            object_kind: shared_protocol::ObjectKind::Assembly,
            object_id: format!("assembly:{}", assembly.assembly_id.0),
            required_revision: 0,
        };
        let payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::Assembly,
            route_kind: shared_protocol::RouteFamily::Assembly,
            route_source_kind: Some(shared_protocol::SourceKind::Text),
            assembly_mode: None,
            precision_band: shared_protocol::PrecisionBand::Exact,
            dependency_closure: vec![dependency.clone()],
            sync_risk: 0,
            literal_bytes: Vec::new(),
            assembly_ref: Some(assembly_ref.clone()),
            inline_assembly_defs: vec![assembly_def_payload],
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: Vec::new(),
            route_graph: shared_protocol::RouteGraphContract::default(),
            contradiction_bytes: Vec::new(),
            prg: Some(shared_protocol::PredictiveReconstructionGraph::new(
                1,
                4,
                assembly.body.output_len(),
                vec![shared_protocol::PrgNode {
                    node_id: 1,
                    kind: shared_protocol::PrgNodeKind::AssemblyRef,
                    output_len: assembly.body.output_len(),
                    dependency_contract: shared_protocol::PrgDependencyContract::default(),
                    literal_bytes: Vec::new(),
                    substrate_ref: None,
                    assembly_ref: Some(assembly_ref),
                    transform_ref: None,
                    episode_ref: None,
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
                vec![format!("assembly:{}", assembly.assembly_id.0)],
            )),
            hybrid_route: None,
        }
        .with_derived_route_graph();
        state
            .apply_record(predictive_confirm_record(SeqNo(1), ItemId(9), payload))
            .unwrap();
        assert_eq!(
            state.cache_entry(ItemId(9)).unwrap().object.exact_bytes,
            b"cache-backed"
        );

        state.resync_plane(PlaneResyncPayload {
            version: PlaneResyncPayload::VERSION,
            plane: shared_protocol::MemoryPlane::Assembly,
            object_kinds: vec![shared_protocol::ObjectKind::Assembly],
            reset_predictors: false,
        });
        assert!(state.installed_assembly_defs().is_empty());
    }

    #[test]
    fn client_ingests_replay_hints_with_structural_cues() {
        let mut state = ClientState::default();
        let block = ExactStateMaterial::copy_exact(
            shared_protocol::SourceKind::Text,
            b"episode-replay-signal",
        );
        let prepared = shared_protocol::prepare_text_source(
            "episode-replay-signal",
            Some("episode".to_string()),
        );
        state
            .source_bindings
            .insert(ItemId(7), prepared.descriptor.clone());
        state.cache.insert(
            ItemId(7),
            ClientEntry {
                object: state.chpmt_object_for_item(
                    ItemId(7),
                    &block.exact_bytes,
                    shared_protocol::ObjectKind::ExactState,
                ),
            },
        );
        let payload = EpisodeHintPayload::new(
            shared_protocol::ContextHash(42),
            shared_protocol::LagBucket(0),
            shared_protocol::PrecisionBand::Balanced,
            vec![shared_protocol::ObjectDependency {
                object_kind: shared_protocol::ObjectKind::ExactState,
                object_id: "item:7".to_string(),
                required_revision: 0,
            }],
            vec![shared_protocol::EpisodeCompletionCandidate {
                object_ref: EpisodeObjectRef {
                    object_kind: shared_protocol::ObjectKind::ExactState,
                    object_id: "item:7".to_string(),
                },
                transition_count: shared_protocol::TransitionCount(3),
                branch_rank: shared_protocol::BranchRank(0),
                precision_band: shared_protocol::PrecisionBand::Balanced,
                cue_overlap: 8,
                recency_score: 24,
                route_support: 8,
                transition_match: 16,
                lag_bucket: shared_protocol::LagBucket(0),
                admissible: true,
            }],
        );
        state
            .apply_record(replay_hint_record(SeqNo(2), ItemId(7), &payload))
            .unwrap();
        assert_eq!(state.installed_episode_hints().len(), 1);
        let event = state.episode_memory().working_trace.events.last().unwrap();
        assert_eq!(event.context_hash, shared_protocol::ContextHash(42));
        assert_ne!(event.cue, shared_protocol::SparseCue::default());
        assert_eq!(
            event.object_ref.object_kind,
            shared_protocol::ObjectKind::ReplayHint
        );
    }

    #[test]
    fn client_applies_raw_record() {
        let mut state = ClientState::default();
        let block = sample_block(0.01);
        state
            .apply_record(raw_record(StreamId(1), SeqNo(0), ItemId(9), &block))
            .unwrap();
        assert!(state.cache_entry(ItemId(9)).is_some());
    }

    #[test]
    fn client_applies_delta_record() {
        let mut state = ClientState::default();
        let previous = sample_block(0.01);
        let current = sample_block(0.0103);
        state
            .apply_record(raw_record(StreamId(1), SeqNo(0), ItemId(9), &previous))
            .unwrap();

        let encoded = encode_delta_material(&current, &previous);
        let payload = encode_delta_payload(&encoded);
        let record = Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: SeqNo(1),
                record_type: RecordType::ExactState,
                codec_mode: CodecMode::PredictedExact,
                flags: RecordFlags::empty(),
                item_id: ItemId(9),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        };

        state.apply_record(record).unwrap();
        assert_eq!(
            state.cache_entry(ItemId(9)).unwrap().exact_bytes(),
            current.exact_bytes.as_slice()
        );
    }

    #[test]
    fn resync_clears_predictors_and_blocks_delta() {
        let mut state = ClientState::default();
        let previous = sample_block(0.01);
        let current = sample_block(0.0103);
        state
            .apply_record(raw_record(StreamId(1), SeqNo(0), ItemId(9), &previous))
            .unwrap();
        state.apply_record(resync_record(SeqNo(1))).unwrap();

        let encoded = encode_delta_material(&current, &previous);
        let payload = encode_delta_payload(&encoded);
        let record = Record {
            header: RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: EpochId(0),
                seq_no: SeqNo(2),
                record_type: RecordType::ExactState,
                codec_mode: CodecMode::PredictedExact,
                flags: RecordFlags::empty(),
                item_id: ItemId(9),
                payload_len: payload.len() as u32,
            },
            payload,
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        };

        let error = state.apply_record(record).unwrap_err();
        assert!(matches!(
            error,
            ClientApplyError::MissingPredictor(ItemId(9))
        ));
    }

    #[test]
    fn catalog_sync_updates_client_catalog_and_rejects_stale_versions() {
        let mut state = ClientState::default();
        let block = BlockCatalogEntry::from_bytes(SourceKind::Text, b"catalog".to_vec()).unwrap();
        state
            .apply_record(repair_record(
                SeqNo(0),
                RepairPayload::response(CatalogSyncPayload::new(
                    BlockCatalogVersion(2),
                    vec![block.clone()],
                    Vec::new(),
                )),
            ))
            .unwrap();
        assert!(state.catalog().contains_block(block.block_id));

        let stale = state
            .apply_record(repair_record(
                SeqNo(1),
                RepairPayload::response(CatalogSyncPayload::new(
                    BlockCatalogVersion(1),
                    Vec::new(),
                    Vec::new(),
                )),
            ))
            .unwrap_err();
        assert!(matches!(stale, ClientApplyError::Catalog(_)));
    }

    #[test]
    fn repair_response_restores_missing_block_and_catalog_sync_succeeds() {
        let block =
            BlockCatalogEntry::from_bytes(SourceKind::Text, b"repair basis".to_vec()).unwrap();
        let mut state = ClientState::default();
        let request = RepairRequest {
            catalog_version: BlockCatalogVersion(0),
            missing_blocks: vec![block.block_id],
            missing_bundles: Vec::new(),
            cause: RepairCause::MissingBasis,
        };

        state
            .apply_record(repair_record(SeqNo(0), RepairPayload::request(request)))
            .unwrap();
        assert_eq!(state.pending_repair_requests().len(), 1);

        let repair = RepairPayload::response(CatalogSyncPayload::new(
            BlockCatalogVersion(1),
            vec![block.clone()],
            Vec::new(),
        ));
        state.apply_record(repair_record(SeqNo(1), repair)).unwrap();
        assert!(state.pending_repair_requests().is_empty());
        assert!(state.catalog().contains_block(block.block_id));
    }

    #[test]
    fn repair_queue_is_deduplicated_and_cleared_after_catalog_arrives() {
        let block = BlockCatalogEntry::from_bytes(SourceKind::Text, b"repair me".to_vec()).unwrap();
        let request = RepairRequest {
            catalog_version: BlockCatalogVersion(0),
            missing_blocks: vec![block.block_id],
            missing_bundles: Vec::new(),
            cause: RepairCause::MissingBasis,
        };

        let mut state = ClientState::default();
        state
            .apply_record(repair_record(
                SeqNo(0),
                RepairPayload::request(request.clone()),
            ))
            .unwrap();
        state
            .apply_record(repair_record(SeqNo(1), RepairPayload::request(request)))
            .unwrap();
        assert_eq!(state.pending_repair_requests().len(), 1);

        let repair = RepairPayload::response(CatalogSyncPayload::new(
            BlockCatalogVersion(1),
            vec![block],
            Vec::new(),
        ));
        state.apply_record(repair_record(SeqNo(1), repair)).unwrap();
        assert!(state.pending_repair_requests().is_empty());
    }

    #[test]
    fn protected_session_applies_encrypted_records() {
        let client_secret = [7_u8; 32];
        let server_secret = [9_u8; 32];
        let (mut sender, receiver) = classic_ref1_pair_from_static_secrets(
            StreamId(2),
            StreamDirection::ServerToClient,
            client_secret,
            server_secret,
        );
        let block = sample_block(0.02);
        let protected = sender
            .protect_record(raw_record(StreamId(2), SeqNo(0), ItemId(3), &block))
            .unwrap();
        let mut session = ClientSession::new(receiver);
        session.apply_protected_record(protected).unwrap();
        assert!(session.state().cache_entry(ItemId(3)).is_some());
    }

    #[test]
    fn state_applies_packed_transport_frame() {
        let mut state = ClientState::default();
        let first = sample_block(0.01);
        let second = sample_block(0.0103);
        let packed = pack_records(
            vec![
                raw_record(StreamId(3), SeqNo(0), ItemId(1), &first),
                raw_record(StreamId(3), SeqNo(1), ItemId(2), &second),
            ],
            TransportConfig {
                mode: TransportMode::BurstSmall,
            },
        );
        let applied = state.apply_transport_frames(&packed).unwrap();
        assert_eq!(applied, 2);
        assert!(state.cache_entry(ItemId(1)).is_some());
        assert!(state.cache_entry(ItemId(2)).is_some());
    }

    #[test]
    fn protected_session_applies_packed_transport_frame() {
        let client_secret = [3_u8; 32];
        let server_secret = [5_u8; 32];
        let (sender, receiver) = classic_ref1_pair_from_static_secrets(
            StreamId(4),
            StreamDirection::ServerToClient,
            client_secret,
            server_secret,
        );
        let first = sample_block(0.02);
        let second = sample_block(0.0205);
        let mut sender_session = ClientSession::new(sender);
        let packed = sender_session
            .pack_protected_trace(
                vec![
                    raw_record(StreamId(4), SeqNo(0), ItemId(1), &first),
                    raw_record(StreamId(4), SeqNo(1), ItemId(2), &second),
                ],
                TransportConfig {
                    mode: TransportMode::BurstSmall,
                },
            )
            .unwrap();
        let mut session = ClientSession::new(receiver);
        let applied = session.apply_protected_transport_frames(&packed).unwrap();
        assert_eq!(applied, 2);
        assert!(session.state().cache_entry(ItemId(1)).is_some());
        assert!(session.state().cache_entry(ItemId(2)).is_some());
    }
    #[test]
    fn client_rejects_predictive_record_type_mismatch() {
        let mut state = ClientState::default();
        let payload = PredictiveRouteDispatchPayload {
            version: PredictiveRouteDispatchPayload::VERSION,
            route_family: ControllerRouteFamily::Hybrid,
            route_kind: shared_protocol::RouteFamily::PredictiveCorrect,
            route_source_kind: Some(shared_protocol::SourceKind::Text),
            assembly_mode: None,
            precision_band: shared_protocol::PrecisionBand::Exact,
            dependency_closure: Vec::new(),
            sync_risk: 0,
            literal_bytes: b"mismatch".to_vec(),
            assembly_ref: None,
            inline_assembly_defs: Vec::new(),
            inline_schema_defs: Vec::new(),
            inline_dictionaries: Vec::new(),
            inline_episode_hints: Vec::new(),
            route_graph: shared_protocol::RouteGraphContract::default(),
            contradiction_bytes: Vec::new(),
            prg: None,
            hybrid_route: None,
        }
        .with_derived_route_graph();
        let record = predictive_confirm_record(SeqNo(1), ItemId(42), payload);
        let err = state.apply_record(record).unwrap_err();
        assert!(matches!(
            err,
            ClientApplyError::PredictiveRouteRecordTypeMismatch { .. }
        ));
    }
}
