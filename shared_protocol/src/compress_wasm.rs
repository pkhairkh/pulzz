//! WASM stub for the compression module.
//!
//! zstd-sys requires clang for wasm32 builds. On WASM, compression is
//! disabled — payloads are transmitted uncompressed. The batch envelope
//! format is still valid; only whole-batch zstd compression is skipped.

use crate::SourceKind;

pub fn zstd_compress_raw(data: &[u8]) -> Result<Vec<u8>, String> {
    Ok(data.to_vec())
}

pub fn zstd_decompress_raw(data: &[u8], _max: usize) -> Result<Vec<u8>, String> {
    Ok(data.to_vec())
}

pub fn starts_with_compression_tag(_data: &[u8]) -> bool {
    false
}

#[derive(Default, Debug, Clone)]
pub struct DictionaryManager;
impl DictionaryManager {
    pub fn add_sample(&mut self, _kind: SourceKind, _data: &[u8]) {}
    pub fn maybe_train(&mut self, _kind: SourceKind) {}
    pub fn get_dictionary(&self, _kind: SourceKind) -> Option<&()> {
        None
    }
}

#[derive(Default, Debug, Clone)]
pub struct TemplateRegistry;
impl TemplateRegistry {
    pub fn try_register(&mut self, _kind: SourceKind, _data: &[u8]) {}
    pub fn find_match(&self, _kind: SourceKind, _data: &[u8]) -> Option<(u64, ())> {
        None
    }
}

#[derive(Default, Debug, Clone)]
pub struct CompressorCache;
impl CompressorCache {
    pub fn record_compress(&mut self, _dict_id: Option<u64>) {}
}

#[derive(Default, Debug, Clone)]
pub struct CompressionBudget;
impl CompressionBudget {
    pub fn within_budget_for_size(&self, _size: usize) -> bool {
        true
    }
}

#[derive(Default, Debug, Clone)]
pub struct ColumnarBatchAccumulator;

#[derive(Default, Debug, Clone)]
pub struct StrategyPerformanceTracker;
impl StrategyPerformanceTracker {
    pub fn select_strategy_with_feedback(
        &self,
        _kind: SourceKind,
        _data: &[u8],
        _is_update: bool,
        _is_update2: bool,
        _dict: Option<u64>,
        _template: Option<u64>,
        _prev: Option<u64>,
        _col: usize,
    ) -> CompressionStrategy {
        CompressionStrategy::Passthrough
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionStrategy {
    Passthrough,
    ZstdDict,
    ZstdRaw,
    Delta,
    Template,
}

#[derive(Debug, Clone)]
pub struct ZstdDictionary {
    pub dict_id: crate::DictionaryId,
    pub dict_bytes: Vec<u8>,
    pub source_kind: crate::SourceKind,
    pub version: u64,
}
impl ZstdDictionary {
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Ok(data.to_vec())
    }
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        Ok(data.to_vec())
    }
}

pub fn select_strategy(
    _kind: SourceKind,
    _data: &[u8],
    _is_update: bool,
    _is_update2: bool,
    _dict: Option<u64>,
    _template: Option<u64>,
    _prev: Option<u64>,
) -> CompressionStrategy {
    CompressionStrategy::Passthrough
}

pub fn decode_compressed_payload(
    data: &[u8],
    _dict: &DictionaryManager,
    _tmpl: &TemplateRegistry,
    _prev: &std::collections::HashMap<u64, Vec<u8>>,
    _kind: SourceKind,
) -> Result<Vec<u8>, String> {
    Ok(data.to_vec())
}
