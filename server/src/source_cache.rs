use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use shared_protocol::{
    Assembly, AssemblyId, CachedPredictiveObject, ChpmtObject, CompletionCandidate,
    CompletionQuery, ObjectKind, ObjectLifecycleMeta, PredictiveObjectKey,
    PredictiveObjectStorePath, PreparedSource, SourceDescriptor, SourceError,
    SourceOptimizationConfig, SparseIndexEntry, SparseIndexKey, SparseIndexTable, TransformClass,
    TransformId,
    prepare_text_source,
};
use thiserror::Error;

const DEFAULT_CACHE_ROOT_RELATIVE: &str = ".cache/pulzz/source_cache_v2";
const CONTENT_DIR: &str = "content";
const PREDICTIVE_OBJECT_DIR: &str = "predictive_objects";
const OBJECT_DIR: &str = "objects";
const INDEX_DIR: &str = "index";

/// Default maximum number of entries in each hot cache (descriptors, objects,
/// assemblies, transforms). Bounds memory usage by evicting the oldest entries
/// (FIFO/insertion-order) when the cap is exceeded.
pub const DEFAULT_MAX_HOT_ENTRIES: usize = 4096;

#[derive(Debug, Clone)]
pub struct SourceCacheConfig {
    pub root_dir: PathBuf,
    pub optimizations: SourceOptimizationConfig,
    pub max_object_material_bytes: u64,
    /// Maximum number of entries in each hot cache. When exceeded, the oldest
    /// entries (by insertion order) are evicted. This bounds the in-memory
    /// working set independently of the on-disk byte budget.
    pub max_hot_entries: usize,
    pub cleanup_on_drop: bool,
    /// When true, all disk I/O is skipped. Objects are stored only in the
    /// in-memory hot caches, and lookups only check hot caches (no disk
    /// fallback). This isolates benchmark measurements from filesystem
    /// latency, preventing ~450K file writes from contaminating throughput
    /// and latency measurements.
    pub in_memory_only: bool,
}

impl Default for SourceCacheConfig {
    fn default() -> Self {
        Self {
            root_dir: default_cache_root(),
            optimizations: SourceOptimizationConfig::default(),
            max_object_material_bytes: 256 * 1024 * 1024,
            max_hot_entries: DEFAULT_MAX_HOT_ENTRIES,
            cleanup_on_drop: false,
            in_memory_only: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceCache {
    config: SourceCacheConfig,
    // IndexMap preserves insertion order for FIFO eviction when the hot cache
    // exceeds max_hot_entries. This bounds the in-memory working set.
    hot_descriptors: Arc<Mutex<IndexMap<String, SourceDescriptor>>>,
    hot_objects: Arc<Mutex<IndexMap<String, CachedPredictiveObject>>>,
    hot_assemblies: Arc<Mutex<IndexMap<String, Assembly>>>,
    hot_transforms: Arc<Mutex<IndexMap<String, TransformClass>>>,
    sparse_index: Arc<Mutex<SparseIndexTable>>,
    object_material_bytes: Arc<Mutex<u64>>,
}

#[derive(Debug, Clone)]
pub struct SourceIngestRequest<'a> {
    pub item_id: shared_protocol::ItemId,
    pub source: &'a PreparedSource,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceCacheMetrics {
    pub lookups: u64,
    pub hits: u64,
    pub misses: u64,
    #[serde(alias = "embeddings_skipped")]
    pub object_materializations_skipped: u64,
    pub cache_read_ns: u64,
    pub cache_write_ns: u64,
}

#[derive(Debug, Clone)]
pub struct SourceResolveResult {
    pub descriptor: SourceDescriptor,
    pub object: ChpmtObject,
    pub object_key: PredictiveObjectKey,
    pub cache_hit: bool,
    pub metrics: SourceCacheMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedSourceDescriptor {
    descriptor: SourceDescriptor,
    last_access_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedObjectMaterialEntry {
    cached: CachedPredictiveObject,
    stored_unix_secs: u64,
    last_access_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredObjectMaterial {
    pub object_key: PredictiveObjectKey,
    pub path: PredictiveObjectStorePath,
    pub cue: shared_protocol::SparseCue,
    pub exact_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSparseIndexMetadata {
    pub object_key: PredictiveObjectKey,
    pub source_kind: shared_protocol::SourceKind,
    pub object_kind: ObjectKind,
    pub cue: shared_protocol::SparseCue,
    pub lifecycle: ObjectLifecycleMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAssemblyObject {
    pub assembly: Assembly,
    pub stored_unix_secs: u64,
    pub last_access_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTransformClass {
    pub class: TransformClass,
    pub stored_unix_secs: u64,
    pub last_access_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPredictiveObject {
    pub descriptor: SourceDescriptor,
    pub material: StoredObjectMaterial,
    pub metadata: StoredSparseIndexMetadata,
    pub stored_unix_secs: u64,
    pub last_access_unix_secs: u64,
}

impl SourceCache {
    pub fn new(config: SourceCacheConfig) -> Result<Self, SourceCacheError> {
        // In memory-only mode, skip all directory creation and disk I/O setup.
        if !config.in_memory_only {
            fs::create_dir_all(config.root_dir.join(CONTENT_DIR))?;
            fs::create_dir_all(config.root_dir.join(PREDICTIVE_OBJECT_DIR))?;
            fs::create_dir_all(config.root_dir.join(OBJECT_DIR))?;
            fs::create_dir_all(config.root_dir.join(INDEX_DIR))?;
        }
        Ok(Self {
            config,
            hot_descriptors: Arc::new(Mutex::new(IndexMap::new())),
            hot_objects: Arc::new(Mutex::new(IndexMap::new())),
            hot_assemblies: Arc::new(Mutex::new(IndexMap::new())),
            hot_transforms: Arc::new(Mutex::new(IndexMap::new())),
            sparse_index: Arc::new(Mutex::new(SparseIndexTable::default())),
            object_material_bytes: Arc::new(Mutex::new(0)),
        })
    }

    pub fn config(&self) -> &SourceCacheConfig {
        &self.config
    }

    /// Evict the oldest entries (by insertion order) from an IndexMap-based
    /// hot cache until it is at or below `max_entries`. This bounds memory by
    /// discarding the least-recently-inserted entries first, which approximates
    /// FIFO eviction. Entries that were accessed more recently tend to survive
    /// longer because re-insertion (on cache hit promotion) moves them to the
    /// end of the insertion order.
    fn evict_if_over_capacity<K, V>(cache: &mut IndexMap<K, V>, max_entries: usize)
    where
        K: std::hash::Hash + Eq + Clone,
    {
        while cache.len() > max_entries {
            cache.shift_remove_index(0);
        }
    }

    fn cached_chpmt_object(
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: Vec<u8>,
    ) -> CachedPredictiveObject {
        let cue = descriptor.structural_cue_summary(&exact_bytes).cue;
        CachedPredictiveObject {
            descriptor: descriptor.clone(),
            object_key: object_key.clone(),
            cue,
            object: ChpmtObject::from_exact_bytes(
                descriptor.clone(),
                object_key.clone(),
                descriptor.kind,
                object_key.object_kind,
                cue,
                exact_bytes,
            ),
        }
    }

    pub fn lookup_chpmt_object(
        &self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, SourceCacheError> {
        self.lookup_object_material(object_key)
    }

    pub fn store_chpmt_object(
        &self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), SourceCacheError> {
        self.store_source(descriptor)?;
        let now = unix_time_secs();
        let cached = Self::cached_chpmt_object(descriptor, object_key, exact_bytes.to_vec());
        if let Ok(mut cache) = self.hot_objects.lock() {
            cache.insert(object_key.storage_key(), cached.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        // Skip all disk I/O in memory-only mode (benchmark I/O isolation).
        if self.config.in_memory_only {
            return Ok(());
        }
        let entry = CachedObjectMaterialEntry {
            cached: cached.clone(),
            stored_unix_secs: now,
            last_access_unix_secs: now,
        };
        let path = self.predictive_object_material_path(object_key);
        let previous_len = path.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        let serialized = bincode::serde::encode_to_vec(&entry, bincode::config::standard())?;
        fs::write(&path, &serialized)?;
        self.store_predictive_object_record(&cached, now)?;
        self.insert_sparse_index_entry_for_cached_object(&cached, default_lifecycle_meta(cached.cue, now))?;
        if let Ok(mut total_bytes) = self.object_material_bytes.lock() {
            *total_bytes = total_bytes
                .saturating_sub(previous_len)
                .saturating_add(serialized.len() as u64);
        }
        self.prune_predictive_object_material_if_needed()?;
        Ok(())
    }

    pub fn lookup_object_material_descriptor(
        &self,
        descriptor: &SourceDescriptor,
    ) -> Result<Option<SourceDescriptor>, SourceCacheError> {
        if let Ok(cache) = self.hot_descriptors.lock() {
            if let Some(cached) = cache.get(&descriptor.source_hash.to_string()) {
                return Ok(Some(cached.clone()));
            }
        }
        // In memory-only mode, only check hot caches (no disk fallback).
        if self.config.in_memory_only {
            return Ok(None);
        }
        let path = self.content_path(descriptor.source_hash);
        if !path.exists() {
            return Ok(None);
        }
        let (entry, _): (CachedSourceDescriptor, usize) = bincode::serde::decode_from_slice(&fs::read(&path)?, bincode::config::standard())?;
        if let Ok(mut cache) = self.hot_descriptors.lock() {
            cache.insert(descriptor.source_hash.to_string(), entry.descriptor.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        Ok(Some(entry.descriptor))
    }

    pub fn store_source(&self, descriptor: &SourceDescriptor) -> Result<(), SourceCacheError> {
        if let Ok(mut cache) = self.hot_descriptors.lock() {
            cache.insert(descriptor.source_hash.to_string(), descriptor.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        // Skip disk write in memory-only mode.
        if self.config.in_memory_only {
            return Ok(());
        }
        let entry = CachedSourceDescriptor {
            descriptor: descriptor.clone(),
            last_access_unix_secs: unix_time_secs(),
        };
        fs::write(
            self.content_path(descriptor.source_hash),
            bincode::serde::encode_to_vec(&entry, bincode::config::standard())?,
        )?;
        Ok(())
    }

    pub fn lookup_object_material(
        &self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, SourceCacheError> {
        self.lookup_predictive_material(object_key)
    }

    pub fn lookup_predictive_material(
        &self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, SourceCacheError> {
        let storage_key = object_key.storage_key();
        if let Ok(cache) = self.hot_objects.lock() {
            if let Some(cached) = cache.get(&storage_key) {
                return Ok(Some(cached.clone()));
            }
        }
        // In memory-only mode, only check hot caches (no disk fallback).
        if self.config.in_memory_only {
            return Ok(None);
        }
        let path = self.predictive_object_material_path(object_key);
        if !path.exists() {
            return Ok(None);
        }
        let (entry, _): (CachedObjectMaterialEntry, usize) = bincode::serde::decode_from_slice(&fs::read(&path)?, bincode::config::standard())?;
        if let Ok(mut cache) = self.hot_objects.lock() {
            cache.insert(storage_key, entry.cached.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        if let Ok(mut cache) = self.hot_descriptors.lock() {
            cache.insert(
                entry.cached.descriptor.source_hash.to_string(),
                entry.cached.descriptor.clone(),
            );
        }
        Ok(Some(entry.cached))
    }

    pub fn store_object_material(
        &self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), SourceCacheError> {
        self.store_chpmt_object(descriptor, object_key, exact_bytes)
    }

    pub fn lookup_source(
        &self,
        descriptor: &SourceDescriptor,
    ) -> Result<Option<SourceDescriptor>, SourceCacheError> {
        self.lookup_object_material_descriptor(descriptor)
    }

    pub fn lookup_predictive_object(
        &self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, SourceCacheError> {
        self.lookup_object_material(object_key)
    }

    pub fn store_predictive_object(
        &self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), SourceCacheError> {
        self.store_chpmt_object(descriptor, object_key, exact_bytes)
    }

    pub fn store_predictive_material(
        &self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), SourceCacheError> {
        self.store_object_material(descriptor, object_key, exact_bytes)
    }

    fn store_predictive_object_record(
        &self,
        cached: &CachedPredictiveObject,
        now: u64,
    ) -> Result<(), SourceCacheError> {
        let path_shape = PredictiveObjectStorePath::from_key(&cached.object_key, cached.cue);
        let persisted = StoredPredictiveObject {
            descriptor: cached.descriptor.clone(),
            material: StoredObjectMaterial {
                object_key: cached.object_key.clone(),
                path: path_shape.clone(),
                cue: cached.cue,
                exact_bytes: cached.object.exact_bytes.clone(),
            },
            metadata: StoredSparseIndexMetadata {
                object_key: cached.object_key.clone(),
                source_kind: cached.descriptor.kind,
                object_kind: cached.object_key.object_kind,
                cue: cached.cue,
                lifecycle: default_lifecycle_meta(cached.cue, now),
            },
            stored_unix_secs: now,
            last_access_unix_secs: now,
        };
        let object_root = self
            .config
            .root_dir
            .join(OBJECT_DIR)
            .join(&path_shape.family_partition)
            .join(&path_shape.object_class)
            .join(&path_shape.cue_partition);
        fs::create_dir_all(&object_root)?;
        fs::write(
            object_root.join(format!("{}.json", path_shape.storage_key)),
            bincode::serde::encode_to_vec(&persisted, bincode::config::standard())?,
        )?;
        Ok(())
    }

    pub fn lookup_completion_candidates(
        &self,
        query: &CompletionQuery,
    ) -> Result<Vec<CompletionCandidate>, SourceCacheError> {
        let index = self
            .sparse_index
            .lock()
            .map_err(|_| SourceCacheError::Invariant("sparse index lock poisoned".to_string()))?;
        Ok(index.query(query))
    }

    pub fn resolve_or_materialize_predictive_object<F>(
        &self,
        prepared: &PreparedSource,
        materialize: F,
    ) -> Result<SourceResolveResult, SourceCacheError>
    where
        F: FnOnce(&SourceDescriptor, &[u8]) -> Result<Vec<u8>, SourceCacheError>,
    {
        self.lookup_or_materialize(prepared, materialize)
    }

    pub fn lookup_or_materialize<F>(
        &self,
        prepared: &PreparedSource,
        materialize: F,
    ) -> Result<SourceResolveResult, SourceCacheError>
    where
        F: FnOnce(&SourceDescriptor, &[u8]) -> Result<Vec<u8>, SourceCacheError>,
    {
        let mut metrics = SourceCacheMetrics::default();
        let object_key = prepared
            .descriptor
            .object_cache_key_from_bytes(&prepared.canonical_bytes);

        if self.config.optimizations.dedup_enabled {
            metrics.lookups = 1;
            let lookup_start = std::time::Instant::now();
            if let Some(cached) = self.lookup_object_material(&object_key)? {
                metrics.cache_read_ns = lookup_start.elapsed().as_nanos() as u64;
                metrics.hits = 1;
                metrics.object_materializations_skipped = 1;
                return Ok(SourceResolveResult {
                    descriptor: cached.descriptor,
                    object: cached.object,
                    object_key,
                    cache_hit: true,
                    metrics,
                });
            }
            metrics.cache_read_ns = lookup_start.elapsed().as_nanos() as u64;
            metrics.misses = 1;
        }

        let exact_bytes = materialize(&prepared.descriptor, &prepared.canonical_bytes)?;
        let object =
            Self::cached_chpmt_object(&prepared.descriptor, &object_key, exact_bytes).object;
        if self.config.optimizations.dedup_enabled {
            let write_start = std::time::Instant::now();
            self.store_chpmt_object(&prepared.descriptor, &object_key, &object.exact_bytes)?;
            metrics.cache_write_ns += write_start.elapsed().as_nanos() as u64;
        }
        Ok(SourceResolveResult {
            descriptor: prepared.descriptor.clone(),
            object,
            object_key,
            cache_hit: false,
            metrics,
        })
    }

    pub fn resolve_source(
        &self,
        prepared: &PreparedSource,
    ) -> Result<SourceResolveResult, SourceCacheError> {
        self.lookup_or_materialize(prepared, |_descriptor, canonical_bytes| {
            Ok(canonical_bytes.to_vec())
        })
    }

    fn prune_predictive_object_material_if_needed(&self) -> Result<(), SourceCacheError> {
        let mut current_total = self
            .object_material_bytes
            .lock()
            .map_err(|_| {
                SourceCacheError::Invariant(
                    "predictive object material byte lock poisoned".to_string(),
                )
            })?
            .to_owned();
        if current_total <= self.config.max_object_material_bytes {
            return Ok(());
        }

        let object_dir = self.config.root_dir.join(PREDICTIVE_OBJECT_DIR);
        let mut entries = fs::read_dir(&object_dir)?
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|entry| {
                let path = entry.path();
                let metadata = entry.metadata().ok()?;
                let json = fs::read(&path).ok()?;
                let cached: CachedObjectMaterialEntry = bincode::serde::decode_from_slice(&json, bincode::config::standard()).map(|(v, _)| v).ok()?;
                Some((path, metadata.len(), cached.last_access_unix_secs))
            })
            .collect::<Vec<_>>();

        entries.sort_by_key(|(_, _, last_access)| *last_access);
        for (path, len, _) in entries {
            if current_total <= self.config.max_object_material_bytes {
                break;
            }
            if let Some(file_stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                if let Ok(mut cache) = self.hot_objects.lock() {
                    cache.shift_remove(file_stem);
                }
            }
            fs::remove_file(&path)?;
            current_total = current_total.saturating_sub(len);
        }
        if let Ok(mut total_bytes) = self.object_material_bytes.lock() {
            *total_bytes = current_total;
        }
        Ok(())
    }

    fn content_path(&self, source_hash: shared_protocol::SourceHash) -> PathBuf {
        self.config
            .root_dir
            .join(CONTENT_DIR)
            .join(format!("{source_hash}.json"))
    }

    fn predictive_object_material_path(&self, object_key: &PredictiveObjectKey) -> PathBuf {
        self.config
            .root_dir
            .join(PREDICTIVE_OBJECT_DIR)
            .join(format!("{}.json", object_key.storage_key()))
    }
}

impl Drop for SourceCache {
    fn drop(&mut self) {
        if self.config.cleanup_on_drop {
            let _ = fs::remove_dir_all(&self.config.root_dir);
        }
    }
}

pub fn prepare_text_ingest_source(text: &str, label: Option<String>) -> PreparedSource {
    prepare_text_source(text, label)
}

fn default_cache_root() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(DEFAULT_CACHE_ROOT_RELATIVE)
}

fn default_lifecycle_meta(cue: shared_protocol::SparseCue, tick: u64) -> ObjectLifecycleMeta {
    ObjectLifecycleMeta {
        support_count: 1,
        success_count: 0,
        failure_count: 0,
        salience: cue.family_bits.count_ones()
            + cue.role_bits.count_ones()
            + cue.delimiter_bits.count_ones()
            + cue.length_bucket_bits.count_ones()
            + cue.temporal_bits.count_ones(),
        creation_tick: tick,
        last_seen_tick: tick,
        promotion_level: shared_protocol::PromotionLevel::Cold,
        consolidation_count: 0,
        last_consolidated_tick: 0,
        ontology_family_id: None,
        ontology_subfamily_id: None,
    }
}

fn unix_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Error)]
pub enum SourceCacheError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Decode(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    Encode(#[from] bincode::error::EncodeError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("source cache invariant failed: {0}")]
    Invariant(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cache_root(name: &str) -> PathBuf {
        let mut root = std::env::temp_dir();
        root.push(format!(
            "pulzz_source_cache_test_{name}_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        root
    }

    #[test]
    fn disk_cache_reuses_predictive_object_after_restart() {
        let root = test_cache_root("persist");
        let config = SourceCacheConfig {
            root_dir: root.clone(),
            optimizations: SourceOptimizationConfig {
                dedup_enabled: true,
                inline_source_meta_enabled: false,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::Adaptive,
                reversible_preprocessing_enabled: true,
                canonicalization_profile: shared_protocol::CanonicalizationProfile::Structural,
            },
            max_object_material_bytes: 16 * 1024 * 1024,
            max_hot_entries: DEFAULT_MAX_HOT_ENTRIES,
            cleanup_on_drop: false,
            in_memory_only: false,
        };
        let prepared = prepare_text_ingest_source("alpha\r\nbeta", Some("note.txt".to_string()));
        let cache = SourceCache::new(config.clone()).unwrap();
        let first = cache
            .lookup_or_materialize(&prepared, |_descriptor, canonical_bytes| {
                Ok(canonical_bytes.to_vec())
            })
            .unwrap();
        assert!(!first.cache_hit);

        let reopened = SourceCache::new(config).unwrap();
        let second = reopened
            .lookup_or_materialize(&prepared, |_descriptor, canonical_bytes| {
                Ok(canonical_bytes.to_vec())
            })
            .unwrap();
        assert!(second.cache_hit);
        assert_eq!(first.object.exact_bytes, second.object.exact_bytes);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn disabled_dedup_forces_reembedding() {
        let root = test_cache_root("no_dedup");
        let config = SourceCacheConfig {
            root_dir: root.clone(),
            optimizations: SourceOptimizationConfig {
                dedup_enabled: false,
                inline_source_meta_enabled: false,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::Adaptive,
                reversible_preprocessing_enabled: true,
                canonicalization_profile: shared_protocol::CanonicalizationProfile::Structural,
            },
            max_object_material_bytes: 16 * 1024 * 1024,
            max_hot_entries: DEFAULT_MAX_HOT_ENTRIES,
            cleanup_on_drop: false,
            in_memory_only: false,
        };
        let prepared = prepare_text_ingest_source("gamma", Some("gamma.txt".to_string()));
        let cache = SourceCache::new(config).unwrap();

        let first = cache
            .lookup_or_materialize(&prepared, |_descriptor, canonical_bytes| {
                Ok(canonical_bytes.to_vec())
            })
            .unwrap();
        let second = cache
            .lookup_or_materialize(&prepared, |_descriptor, canonical_bytes| {
                Ok(canonical_bytes.to_vec())
            })
            .unwrap();
        assert!(!first.cache_hit);
        assert!(!second.cache_hit);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn hot_cache_evicts_oldest_entries_when_over_capacity() {
        let root = test_cache_root("eviction");
        let config = SourceCacheConfig {
            root_dir: root.clone(),
            optimizations: SourceOptimizationConfig {
                dedup_enabled: true,
                inline_source_meta_enabled: false,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::Adaptive,
                reversible_preprocessing_enabled: true,
                canonicalization_profile: shared_protocol::CanonicalizationProfile::Structural,
            },
            max_object_material_bytes: 16 * 1024 * 1024,
            max_hot_entries: 3,
            cleanup_on_drop: true,
            in_memory_only: true,
        };
        let cache = SourceCache::new(config).unwrap();

        // Insert 5 distinct items; only the last 3 should remain in the hot cache.
        for i in 0..5u8 {
            let text = format!("eviction-test-item-{i}");
            let prepared = prepare_text_ingest_source(&text, Some(format!("item{i}.txt")));
            let _ = cache
                .lookup_or_materialize(&prepared, |_descriptor, canonical_bytes| {
                    Ok(canonical_bytes.to_vec())
                })
                .unwrap();
        }

        let hot_objects = cache.hot_objects.lock().unwrap();
        assert!(hot_objects.len() <= 3, "hot cache should be bounded by max_hot_entries, got {}", hot_objects.len());

        let _ = fs::remove_dir_all(root);
    }
}

impl SourceCache {
    pub fn store_assembly(&self, assembly: &Assembly) -> Result<(), SourceCacheError> {
        let now = unix_time_secs();
        let key = format!("assembly:{}", assembly.assembly_id.0);
        if let Ok(mut cache) = self.hot_assemblies.lock() {
            cache.insert(key.clone(), assembly.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        if let Ok(mut sparse_index) = self.sparse_index.lock() {
            sparse_index.insert(
                SparseIndexKey {
                    source_kind: assembly.source_kind,
                    family: assembly.source_kind.cue_family(),
                    cue: assembly.cue,
                },
                SparseIndexEntry {
                    object_id: key.clone(),
                    source_kind: assembly.source_kind,
                    object_kind: ObjectKind::Assembly,
                    cue: assembly.cue,
                    lifecycle: assembly.lifecycle,
                },
            );
        }
        // Skip disk write in memory-only mode.
        if self.config.in_memory_only {
            return Ok(());
        }
        let stored = StoredAssemblyObject {
            assembly: assembly.clone(),
            stored_unix_secs: now,
            last_access_unix_secs: now,
        };
        let path = self
            .config
            .root_dir
            .join(OBJECT_DIR)
            .join(format!("{}.json", key.replace(":", "_")));
        fs::write(path, bincode::serde::encode_to_vec(&stored, bincode::config::standard())?)?;
        Ok(())
    }

    pub fn lookup_assembly(
        &self,
        assembly_id: AssemblyId,
    ) -> Result<Option<Assembly>, SourceCacheError> {
        let key = format!("assembly:{}", assembly_id.0);
        if let Ok(cache) = self.hot_assemblies.lock() {
            if let Some(assembly) = cache.get(&key) {
                return Ok(Some(assembly.clone()));
            }
        }
        // In memory-only mode, only check hot caches (no disk fallback).
        if self.config.in_memory_only {
            return Ok(None);
        }
        let path = self
            .config
            .root_dir
            .join(OBJECT_DIR)
            .join(format!("{}.json", key.replace(":", "_")));
        if !path.exists() {
            return Ok(None);
        }
        let (stored, _): (StoredAssemblyObject, usize) = bincode::serde::decode_from_slice(&fs::read(&path)?, bincode::config::standard())?;
        if let Ok(mut cache) = self.hot_assemblies.lock() {
            cache.insert(key, stored.assembly.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        Ok(Some(stored.assembly))
    }

    pub fn lookup_assembly_candidates(&self, query: &CompletionQuery) -> Vec<CompletionCandidate> {
        let mut query = query.clone();
        if !query
            .admissible_object_kinds
            .contains(&ObjectKind::Assembly)
        {
            query.admissible_object_kinds.push(ObjectKind::Assembly);
        }
        self.lookup_completion_candidates(&query)
            .unwrap_or_default()
    }

    pub fn store_transform_class(&self, class: &TransformClass) -> Result<(), SourceCacheError> {
        let key = format!("transform:{}", class.transform_id.0);
        let now = unix_time_secs();
        if let Ok(mut cache) = self.hot_transforms.lock() {
            cache.insert(key.clone(), class.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        self.insert_sparse_index_entry_for_transform(class, &key)?;
        // Skip disk write in memory-only mode.
        if self.config.in_memory_only {
            return Ok(());
        }
        let stored = StoredTransformClass {
            class: class.clone(),
            stored_unix_secs: now,
            last_access_unix_secs: now,
        };
        let path = self
            .config
            .root_dir
            .join(OBJECT_DIR)
            .join(format!("{}.json", key.replace(":", "_")));
        fs::write(&path, bincode::serde::encode_to_vec(&stored, bincode::config::standard())?)?;
        Ok(())
    }

    fn insert_sparse_index_entry_for_cached_object(
        &self,
        cached: &CachedPredictiveObject,
        lifecycle: ObjectLifecycleMeta,
    ) -> Result<(), SourceCacheError> {
        let entry = SparseIndexEntry {
            object_id: cached.object_key.storage_key(),
            source_kind: cached.descriptor.kind,
            object_kind: cached.object_key.object_kind,
            cue: cached.cue,
            lifecycle,
        };
        let index_key = SparseIndexKey {
            source_kind: cached.descriptor.kind,
            family: cached.descriptor.kind.cue_family(),
            cue: cached.cue,
        };
        {
            let mut index = self.sparse_index.lock().map_err(|_| {
                SourceCacheError::Invariant("sparse index lock poisoned".to_string())
            })?;
            index.insert(index_key, entry.clone());
        }
        // Skip disk write in memory-only mode.
        if self.config.in_memory_only {
            return Ok(());
        }
        let index_root = self
            .config
            .root_dir
            .join(INDEX_DIR)
            .join(cached.descriptor.kind.slug())
            .join(cached.object_key.object_kind.storage_slug());
        fs::create_dir_all(&index_root)?;
        let key = cached.object_key.storage_key();
        fs::write(
            index_root.join(format!("{}.bin", key.replace(":", "_"))),
            bincode::serde::encode_to_vec(&entry, bincode::config::standard())
                .map_err(|e| SourceCacheError::Invariant(e.to_string()))?,
        )?;
        Ok(())
    }

    fn insert_sparse_index_entry_for_transform(
        &self,
        class: &TransformClass,
        key: &str,
    ) -> Result<(), SourceCacheError> {
        let entry = SparseIndexEntry {
            object_id: key.to_string(),
            source_kind: class.source_kind,
            object_kind: ObjectKind::Transform,
            cue: class.cue,
            lifecycle: class.lifecycle,
        };
        let index_key = SparseIndexKey {
            source_kind: class.source_kind,
            family: class.source_kind.cue_family(),
            cue: class.cue,
        };
        {
            let mut index = self.sparse_index.lock().map_err(|_| {
                SourceCacheError::Invariant("sparse index lock poisoned".to_string())
            })?;
            index.insert(index_key, entry.clone());
        }
        // Skip disk write in memory-only mode.
        if self.config.in_memory_only {
            return Ok(());
        }
        let index_root = self
            .config
            .root_dir
            .join(INDEX_DIR)
            .join(class.source_kind.slug())
            .join(ObjectKind::Transform.storage_slug());
        fs::create_dir_all(&index_root)?;
        fs::write(
            index_root.join(format!("{}.json", key.replace(":", "_"))),
            bincode::serde::encode_to_vec(&entry, bincode::config::standard())?,
        )?;
        Ok(())
    }

    pub fn lookup_transform_class(
        &self,
        transform_id: TransformId,
    ) -> Result<Option<TransformClass>, SourceCacheError> {
        let key = format!("transform:{}", transform_id.0);
        if let Ok(cache) = self.hot_transforms.lock() {
            if let Some(class) = cache.get(&key) {
                return Ok(Some(class.clone()));
            }
        }
        // In memory-only mode, only check hot caches (no disk fallback).
        if self.config.in_memory_only {
            return Ok(None);
        }
        let path = self
            .config
            .root_dir
            .join(OBJECT_DIR)
            .join(format!("{}.json", key.replace(":", "_")));
        if !path.exists() {
            return Ok(None);
        }
        let (stored, _): (StoredTransformClass, usize) = bincode::serde::decode_from_slice(&fs::read(&path)?, bincode::config::standard())?;
        if let Ok(mut cache) = self.hot_transforms.lock() {
            cache.insert(key, stored.class.clone());
            Self::evict_if_over_capacity(&mut cache, self.config.max_hot_entries);
        }
        Ok(Some(stored.class))
    }

    pub fn lookup_transform_candidates(
        &self,
        query: &CompletionQuery,
    ) -> Result<Vec<CompletionCandidate>, SourceCacheError> {
        let mut query = query.clone();
        if !query
            .admissible_object_kinds
            .contains(&ObjectKind::Transform)
        {
            query.admissible_object_kinds.push(ObjectKind::Transform);
        }
        self.lookup_completion_candidates(&query)
    }
}
