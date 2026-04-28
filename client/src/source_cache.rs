use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use shared_protocol::{
    Assembly, AssemblyId, CachedPredictiveObject, ChpmtObject, CompletionCandidate,
    CompletionQuery, ObjectKind, ObjectLifecycleMeta, PredictiveObjectKey,
    PredictiveObjectStorePath, PreparedSource, SourceDescriptor, SourceError,
    SourceOptimizationConfig, SparseIndexEntry, SparseIndexKey, SparseIndexTable, TransformClass,
    TransformId,
};

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct WebSourceCacheConfig {
    pub database_name: String,
    pub memory_cache_entries: usize,
    pub optimizations: SourceOptimizationConfig,
}

impl Default for WebSourceCacheConfig {
    fn default() -> Self {
        Self {
            database_name: "pulzz_source_cache_v2".to_string(),
            memory_cache_entries: 256,
            optimizations: SourceOptimizationConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebSourceCache {
    config: WebSourceCacheConfig,
    memory_objects: HashMap<String, CachedPredictiveObject>,
    memory_assemblies: HashMap<String, Assembly>,
    memory_transforms: HashMap<String, TransformClass>,
    sparse_index: SparseIndexTable,
}

#[derive(Debug, Clone)]
pub struct WebSourceLookupResult {
    pub cached: CachedPredictiveObject,
    pub cache_hit: bool,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSourceDescriptor {
    descriptor: SourceDescriptor,
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredObjectEntry {
    cached: CachedPredictiveObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredObjectMaterial {
    object_key: PredictiveObjectKey,
    path: PredictiveObjectStorePath,
    cue: shared_protocol::SparseCue,
    exact_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSparseIndexMetadata {
    object_key: PredictiveObjectKey,
    source_kind: shared_protocol::SourceKind,
    object_kind: ObjectKind,
    cue: shared_protocol::SparseCue,
    lifecycle: ObjectLifecycleMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPredictiveObject {
    descriptor: SourceDescriptor,
    material: StoredObjectMaterial,
    metadata: StoredSparseIndexMetadata,
}

impl WebSourceCache {
    pub fn new(config: WebSourceCacheConfig) -> Self {
        Self {
            config,
            memory_objects: HashMap::new(),
            memory_assemblies: HashMap::new(),
            memory_transforms: HashMap::new(),
            sparse_index: SparseIndexTable::default(),
        }
    }

    pub fn config(&self) -> &WebSourceCacheConfig {
        &self.config
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

    pub async fn lookup_chpmt_object(
        &mut self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, WebSourceCacheError> {
        self.lookup_object_material(object_key).await
    }

    pub async fn store_chpmt_object(
        &mut self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), WebSourceCacheError> {
        let cached = Self::cached_chpmt_object(descriptor, object_key, exact_bytes.to_vec());
        platform_store_descriptor(&self.config, descriptor).await?;
        platform_store_object_material(&self.config, &cached).await?;
        let cue = cached.cue;
        let _persisted = StoredPredictiveObject {
            descriptor: descriptor.clone(),
            material: StoredObjectMaterial {
                object_key: object_key.clone(),
                path: PredictiveObjectStorePath::from_key(object_key, cue),
                cue,
                exact_bytes: exact_bytes.to_vec(),
            },
            metadata: StoredSparseIndexMetadata {
                object_key: object_key.clone(),
                source_kind: descriptor.kind,
                object_kind: object_key.object_kind,
                cue,
                lifecycle: default_lifecycle_meta(cue),
            },
        };
        self.insert_sparse_index_entry(&cached);
        self.insert_object_memory(object_key.storage_key(), cached);
        Ok(())
    }

    pub async fn lookup_object_material(
        &mut self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, WebSourceCacheError> {
        let key = object_key.storage_key();
        if let Some(cached) = self.memory_objects.get(&key).cloned() {
            return Ok(Some(cached));
        }
        let cached = platform_lookup_object_material(&self.config, &key).await?;
        if let Some(cached) = &cached {
            self.insert_object_memory(key, cached.clone());
        }
        Ok(cached)
    }

    pub async fn store_object_material(
        &mut self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), WebSourceCacheError> {
        self.store_chpmt_object(descriptor, object_key, exact_bytes)
            .await
    }

    pub async fn lookup_source(
        &mut self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, WebSourceCacheError> {
        self.lookup_object_material(object_key).await
    }

    pub async fn lookup_predictive_object(
        &mut self,
        object_key: &PredictiveObjectKey,
    ) -> Result<Option<CachedPredictiveObject>, WebSourceCacheError> {
        self.lookup_object_material(object_key).await
    }

    pub async fn store_predictive_object(
        &mut self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), WebSourceCacheError> {
        self.store_chpmt_object(descriptor, object_key, exact_bytes)
            .await
    }

    pub async fn store_predictive_material(
        &mut self,
        descriptor: &SourceDescriptor,
        object_key: &PredictiveObjectKey,
        exact_bytes: &[u8],
    ) -> Result<(), WebSourceCacheError> {
        self.store_object_material(descriptor, object_key, exact_bytes)
            .await
    }

    fn insert_sparse_index_entry(&mut self, cached: &CachedPredictiveObject) {
        let key = SparseIndexKey {
            source_kind: cached.descriptor.kind,
            family: cached.descriptor.kind.cue_family(),
            cue: cached.cue,
        };
        let entry = SparseIndexEntry {
            object_id: cached.object_key.storage_key(),
            source_kind: cached.descriptor.kind,
            object_kind: cached.object_key.object_kind,
            cue: cached.cue,
            lifecycle: default_lifecycle_meta(cached.cue),
        };
        self.sparse_index.insert(key, entry);
    }

    pub fn lookup_completion_candidates(
        &self,
        query: &CompletionQuery,
    ) -> Vec<CompletionCandidate> {
        self.sparse_index.query(query)
    }

    pub async fn resolve_or_materialize_predictive_object<F, Fut>(
        &mut self,
        prepared: &PreparedSource,
        materialize: F,
    ) -> Result<WebSourceLookupResult, WebSourceCacheError>
    where
        F: FnOnce(SourceDescriptor, Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, WebSourceCacheError>>,
    {
        self.lookup_or_materialize(prepared, materialize).await
    }

    pub async fn lookup_or_materialize<F, Fut>(
        &mut self,
        prepared: &PreparedSource,
        materialize: F,
    ) -> Result<WebSourceLookupResult, WebSourceCacheError>
    where
        F: FnOnce(SourceDescriptor, Vec<u8>) -> Fut,
        Fut: std::future::Future<Output = Result<Vec<u8>, WebSourceCacheError>>,
    {
        let object_key = prepared
            .descriptor
            .object_cache_key_from_bytes(&prepared.canonical_bytes);
        if self.config.optimizations.dedup_enabled {
            if let Some(cached) = self.lookup_object_material(&object_key).await? {
                return Ok(WebSourceLookupResult {
                    cached,
                    cache_hit: true,
                });
            }
        }

        let exact_bytes = materialize(
            prepared.descriptor.clone(),
            prepared.canonical_bytes.clone(),
        )
        .await?;
        if self.config.optimizations.dedup_enabled {
            self.store_chpmt_object(&prepared.descriptor, &object_key, &exact_bytes)
                .await?;
        }
        Ok(WebSourceLookupResult {
            cached: Self::cached_chpmt_object(&prepared.descriptor, &object_key, exact_bytes),
            cache_hit: false,
        })
    }

    fn insert_object_memory(&mut self, key: String, cached: CachedPredictiveObject) {
        if self.memory_objects.len() >= self.config.memory_cache_entries {
            if let Some(first_key) = self.memory_objects.keys().next().cloned() {
                self.memory_objects.remove(&first_key);
            }
        }
        self.memory_objects.insert(key, cached);
    }
}

fn default_lifecycle_meta(cue: shared_protocol::SparseCue) -> ObjectLifecycleMeta {
    ObjectLifecycleMeta {
        support_count: 1,
        success_count: 0,
        failure_count: 0,
        salience: cue.family_bits.count_ones()
            + cue.role_bits.count_ones()
            + cue.delimiter_bits.count_ones()
            + cue.length_bucket_bits.count_ones()
            + cue.temporal_bits.count_ones(),
        creation_tick: 0,
        last_seen_tick: 0,
        promotion_level: shared_protocol::PromotionLevel::Cold,
        consolidation_count: 0,
        last_consolidated_tick: 0,
        ontology_family_id: None,
        ontology_subfamily_id: None,
    }
}

#[derive(Debug, Error)]
pub enum WebSourceCacheError {
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("browser storage is unavailable: {0}")]
    Browser(String),
}

#[cfg(not(target_arch = "wasm32"))]
async fn platform_lookup_object_material(
    _config: &WebSourceCacheConfig,
    _key: &str,
) -> Result<Option<CachedPredictiveObject>, WebSourceCacheError> {
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
async fn platform_store_descriptor(
    _config: &WebSourceCacheConfig,
    _descriptor: &SourceDescriptor,
) -> Result<(), WebSourceCacheError> {
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn platform_store_object_material(
    _config: &WebSourceCacheConfig,
    _cached: &CachedPredictiveObject,
) -> Result<(), WebSourceCacheError> {
    Ok(())
}

#[cfg(target_arch = "wasm32")]
const IDB_DB_VERSION: u32 = 4;
#[cfg(target_arch = "wasm32")]
const IDB_SOURCE_STORE: &str = "sources";
#[cfg(target_arch = "wasm32")]
const IDB_OBJECT_STORE: &str = "objects";

#[cfg(target_arch = "wasm32")]
async fn platform_lookup_object_material(
    config: &WebSourceCacheConfig,
    key: &str,
) -> Result<Option<CachedPredictiveObject>, WebSourceCacheError> {
    let db = open_database(&config.database_name).await?;
    let tx = db
        .transaction_with_str_and_mode(IDB_OBJECT_STORE, web_sys::IdbTransactionMode::Readonly)
        .map_err(js_error)?;
    let store = tx.object_store(IDB_OBJECT_STORE).map_err(js_error)?;
    let request = store
        .get(&wasm_bindgen::JsValue::from_str(key))
        .map_err(js_error)?;
    let value = wait_for_request(&request).await?;
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }
    let json = value.as_string().ok_or_else(|| {
        WebSourceCacheError::Browser(
            "indexeddb returned non-string predictive object entry".to_string(),
        )
    })?;
    let stored: StoredObjectEntry = serde_json::from_str(&json)?;
    Ok(Some(stored.cached))
}

#[cfg(target_arch = "wasm32")]
async fn platform_store_descriptor(
    config: &WebSourceCacheConfig,
    descriptor: &SourceDescriptor,
) -> Result<(), WebSourceCacheError> {
    let db = open_database(&config.database_name).await?;
    let tx = db
        .transaction_with_str_and_mode(IDB_SOURCE_STORE, web_sys::IdbTransactionMode::Readwrite)
        .map_err(js_error)?;
    let store = tx.object_store(IDB_SOURCE_STORE).map_err(js_error)?;
    let payload = serde_json::to_string(&StoredSourceDescriptor {
        descriptor: descriptor.clone(),
    })?;
    let request = store
        .put_with_key(
            &wasm_bindgen::JsValue::from_str(&payload),
            &wasm_bindgen::JsValue::from_str(&descriptor.source_hash.to_string()),
        )
        .map_err(js_error)?;
    let _ = wait_for_request(&request).await?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn platform_store_object_material(
    config: &WebSourceCacheConfig,
    cached: &CachedPredictiveObject,
) -> Result<(), WebSourceCacheError> {
    let db = open_database(&config.database_name).await?;
    let tx = db
        .transaction_with_str_and_mode(IDB_OBJECT_STORE, web_sys::IdbTransactionMode::Readwrite)
        .map_err(js_error)?;
    let store = tx.object_store(IDB_OBJECT_STORE).map_err(js_error)?;
    let payload = serde_json::to_string(&StoredObjectEntry {
        cached: cached.clone(),
    })?;
    let request = store
        .put_with_key(
            &wasm_bindgen::JsValue::from_str(&payload),
            &wasm_bindgen::JsValue::from_str(&cached.object_key.storage_key()),
        )
        .map_err(js_error)?;
    let _ = wait_for_request(&request).await?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
async fn open_database(database_name: &str) -> Result<web_sys::IdbDatabase, WebSourceCacheError> {
    use futures_channel::oneshot;
    use wasm_bindgen::{JsCast, closure::Closure};

    let window = web_sys::window()
        .ok_or_else(|| WebSourceCacheError::Browser("window is unavailable".to_string()))?;
    let factory = window
        .indexed_db()
        .map_err(js_error)?
        .ok_or_else(|| WebSourceCacheError::Browser("indexeddb is unavailable".to_string()))?;
    let request = factory
        .open_with_u32(database_name, IDB_DB_VERSION)
        .map_err(js_error)?;

    let upgrade_request = request.clone();
    let on_upgrade = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        if let Ok(result) = upgrade_request.result() {
            if let Ok(database) = result.dyn_into::<web_sys::IdbDatabase>() {
                let names = database.object_store_names();
                if !names.contains(IDB_SOURCE_STORE) {
                    let _ = database.create_object_store(IDB_SOURCE_STORE);
                }
                if !names.contains(IDB_OBJECT_STORE) {
                    let _ = database.create_object_store(IDB_OBJECT_STORE);
                }
            }
        }
    }) as Box<dyn FnMut(_)>);
    request.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));

    let (sender, receiver) = oneshot::channel::<Result<web_sys::IdbDatabase, String>>();
    let sender = std::rc::Rc::new(std::cell::RefCell::new(Some(sender)));

    let success_request = request.clone();
    let success_sender = sender.clone();
    let on_success = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        if let Some(sender) = success_sender.borrow_mut().take() {
            let result = success_request
                .result()
                .map_err(js_error)
                .and_then(|value| {
                    value.dyn_into::<web_sys::IdbDatabase>().map_err(|_| {
                        WebSourceCacheError::Browser(
                            "failed to open indexeddb database".to_string(),
                        )
                    })
                })
                .map_err(|error| error.to_string());
            let _ = sender.send(result);
        }
    }) as Box<dyn FnMut(_)>);
    request.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));

    let error_sender = sender.clone();
    let on_error = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        if let Some(sender) = error_sender.borrow_mut().take() {
            let _ = sender.send(Err("indexeddb request failed".to_string()));
        }
    }) as Box<dyn FnMut(_)>);
    request.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    let database = receiver
        .await
        .map_err(|_| WebSourceCacheError::Browser("indexeddb request channel closed".to_string()))?
        .map_err(WebSourceCacheError::Browser)?;

    request.set_onupgradeneeded(None);
    request.set_onsuccess(None);
    request.set_onerror(None);
    drop(on_upgrade);
    drop(on_success);
    drop(on_error);

    Ok(database)
}

#[cfg(target_arch = "wasm32")]
async fn wait_for_request(
    request: &web_sys::IdbRequest,
) -> Result<wasm_bindgen::JsValue, WebSourceCacheError> {
    use futures_channel::oneshot;
    use wasm_bindgen::{JsCast, closure::Closure};

    let request = request.clone();
    let (sender, receiver) = oneshot::channel::<Result<wasm_bindgen::JsValue, String>>();
    let sender = std::rc::Rc::new(std::cell::RefCell::new(Some(sender)));

    let success_request = request.clone();
    let success_sender = sender.clone();
    let on_success = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        if let Some(sender) = success_sender.borrow_mut().take() {
            let result = success_request
                .result()
                .map_err(js_error)
                .map_err(|error| error.to_string());
            let _ = sender.send(result);
        }
    }) as Box<dyn FnMut(_)>);
    request.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));

    let error_sender = sender.clone();
    let on_error = Closure::wrap(Box::new(move |_event: web_sys::Event| {
        if let Some(sender) = error_sender.borrow_mut().take() {
            let _ = sender.send(Err("indexeddb request failed".to_string()));
        }
    }) as Box<dyn FnMut(_)>);
    request.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    let value = receiver
        .await
        .map_err(|_| WebSourceCacheError::Browser("indexeddb request channel closed".to_string()))?
        .map_err(WebSourceCacheError::Browser)?;

    request.set_onsuccess(None);
    request.set_onerror(None);
    drop(on_success);
    drop(on_error);
    Ok(value)
}

#[cfg(target_arch = "wasm32")]
fn js_error(error: wasm_bindgen::JsValue) -> WebSourceCacheError {
    WebSourceCacheError::Browser(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_protocol::prepare_text_source;

    #[tokio::test]
    async fn native_lookup_or_materialize_reuses_memory_entry() {
        let mut cache = WebSourceCache::new(WebSourceCacheConfig::default());
        let prepared = prepare_text_source("alpha", Some("a.txt".to_string()));
        let first = cache
            .lookup_or_materialize(&prepared, |_descriptor, _canonical_bytes| async move {
                Ok(b"cached".to_vec())
            })
            .await
            .unwrap();
        assert!(!first.cache_hit);
        let second = cache
            .lookup_object_material(&first.cached.object_key)
            .await
            .unwrap();
        assert!(second.is_some());
    }
}

impl WebSourceCache {
    pub async fn store_assembly(&mut self, assembly: &Assembly) -> Result<(), WebSourceCacheError> {
        let key = format!("assembly:{}", assembly.assembly_id.0);
        self.memory_assemblies.insert(key.clone(), assembly.clone());
        self.sparse_index.insert(
            SparseIndexKey {
                source_kind: assembly.source_kind,
                family: assembly.source_kind.cue_family(),
                cue: assembly.cue,
            },
            SparseIndexEntry {
                object_id: key,
                source_kind: assembly.source_kind,
                object_kind: ObjectKind::Assembly,
                cue: assembly.cue,
                lifecycle: assembly.lifecycle,
            },
        );
        Ok(())
    }

    pub async fn lookup_assembly(
        &mut self,
        assembly_id: AssemblyId,
    ) -> Result<Option<Assembly>, WebSourceCacheError> {
        Ok(self
            .memory_assemblies
            .get(&format!("assembly:{}", assembly_id.0))
            .cloned())
    }

    pub fn lookup_assembly_candidates(&self, query: &CompletionQuery) -> Vec<CompletionCandidate> {
        let mut query = query.clone();
        if !query
            .admissible_object_kinds
            .contains(&ObjectKind::Assembly)
        {
            query.admissible_object_kinds.push(ObjectKind::Assembly);
        }
        self.sparse_index.query(&query)
    }

    pub async fn store_transform_class(
        &mut self,
        class: &TransformClass,
    ) -> Result<(), WebSourceCacheError> {
        let key = format!("transform:{}", class.transform_id.0);
        self.memory_transforms.insert(key.clone(), class.clone());
        self.sparse_index.insert(
            SparseIndexKey {
                source_kind: class.source_kind,
                family: class.source_kind.cue_family(),
                cue: class.cue,
            },
            SparseIndexEntry {
                object_id: key,
                source_kind: class.source_kind,
                object_kind: ObjectKind::Transform,
                cue: class.cue,
                lifecycle: class.lifecycle,
            },
        );
        Ok(())
    }

    pub async fn lookup_transform_class(
        &mut self,
        transform_id: TransformId,
    ) -> Result<Option<TransformClass>, WebSourceCacheError> {
        Ok(self
            .memory_transforms
            .get(&format!("transform:{}", transform_id.0))
            .cloned())
    }

    pub fn lookup_transform_candidates(&self, query: &CompletionQuery) -> Vec<CompletionCandidate> {
        let mut query = query.clone();
        if !query
            .admissible_object_kinds
            .contains(&ObjectKind::Transform)
        {
            query.admissible_object_kinds.push(ObjectKind::Transform);
        }
        self.sparse_index.query(&query)
    }
}
