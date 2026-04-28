use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ChpmtCapabilityDescriptor, CueFamily, DataPlaneCodecPreference, MemoryPlane, ObjectKind,
    PrecisionBand, RouteFamily, SparseCue, StructuralCueSummary, keyed_permute_indices,
};

pub const SOURCE_HASH_LEN: usize = 32;
pub const SOURCE_HASH_VERSION: u8 = 1;
const OPTIONAL_STRING_NONE_LEN: u16 = u16::MAX;
const SOURCE_HASH_DOMAIN: &[u8] = b"pulzz/source-hash-v1";
const PREDICTIVE_OBJECT_KEY_DOMAIN: &[u8] = b"pulzz/predictive-object-key-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct SourceHash(pub [u8; SOURCE_HASH_LEN]);

impl SourceHash {
    pub fn to_hex(self) -> String {
        hex_encode(&self.0)
    }
}

impl fmt::Display for SourceHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    #[default]
    Text = 1,
    Json = 2,
    Binary = 3,
    Image = 4,
}

impl SourceKind {
    pub fn from_tag(tag: u8) -> Result<Self, SourceError> {
        match tag {
            1 => Ok(Self::Text),
            2 => Ok(Self::Json),
            3 => Ok(Self::Binary),
            4 => Ok(Self::Image),
            value => Err(SourceError::UnknownSourceKind(value)),
        }
    }

    pub const fn tag(self) -> u8 {
        self as u8
    }

    pub const fn slug(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
            Self::Binary => "binary",
            Self::Image => "image",
        }
    }

    pub const fn compact_asset_kind(self) -> Self {
        match self {
            Self::Text => Self::Text,
            Self::Json => Self::Json,
            Self::Binary | Self::Image => Self::Binary,
        }
    }

    pub const fn asset_namespace(self) -> &'static str {
        match self.compact_asset_kind() {
            Self::Text => "text_v2",
            Self::Json => "json_v2",
            Self::Binary | Self::Image => "binary_v2",
        }
    }

    pub const fn memory_plane(self) -> MemoryPlane {
        MemoryPlane::AtomSubstrate
    }

    pub const fn route_family(self) -> RouteFamily {
        RouteFamily::DirectState
    }

    pub const fn object_kind(self) -> ObjectKind {
        ObjectKind::ExactState
    }

    pub const fn precision_band(self) -> PrecisionBand {
        PrecisionBand::Exact
    }

    pub const fn cue_family(self) -> CueFamily {
        CueFamily {
            plane: self.memory_plane(),
            route_family: self.route_family(),
            object_kind: self.object_kind(),
            precision_band: self.precision_band(),
        }
    }

    pub const fn family_bits(self) -> u64 {
        match self {
            Self::Text => 1 << 0,
            Self::Json => 1 << 1,
            Self::Binary => 1 << 2,
            Self::Image => 1 << 3,
        }
    }

    pub const fn role_bits(self) -> u64 {
        match self {
            Self::Text => (1 << 0) | (1 << 8),
            Self::Json => (1 << 1) | (1 << 9),
            Self::Binary => (1 << 2) | (1 << 10),
            Self::Image => (1 << 3) | (1 << 11),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOptimizationConfig {
    pub dedup_enabled: bool,
    pub inline_source_meta_enabled: bool,
    pub data_plane_codec: DataPlaneCodecPreference,
    pub reversible_preprocessing_enabled: bool,
    pub canonicalization_profile: CanonicalizationProfile,
}

impl Default for SourceOptimizationConfig {
    fn default() -> Self {
        Self {
            dedup_enabled: true,
            inline_source_meta_enabled: false,
            data_plane_codec: DataPlaneCodecPreference::DirectExactOnly,
            reversible_preprocessing_enabled: true,
            canonicalization_profile: CanonicalizationProfile::Structural,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalizationProfile {
    Minimal = 1,
    #[default]
    Structural = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ReversiblePreprocessKind {
    #[default]
    Identity = 1,
    NormalizeLineEndings = 2,
}

impl ReversiblePreprocessKind {
    pub const fn description(self) -> &'static str {
        match self {
            Self::Identity => "identity preprocessing",
            Self::NormalizeLineEndings => "newline canonicalization",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AppliedPreprocessStep {
    pub kind: ReversiblePreprocessKind,
    pub input_len: u32,
    pub output_len: u32,
    pub metadata_bytes: u16,
    pub net_gain_estimate: i32,
}

impl AppliedPreprocessStep {
    pub fn new(kind: ReversiblePreprocessKind, input_len: usize, output_len: usize) -> Self {
        let input_len = input_len.min(u32::MAX as usize) as u32;
        let output_len = output_len.min(u32::MAX as usize) as u32;
        let metadata_bytes = 4_u16;
        let structural_gain = input_len.saturating_sub(output_len) as i32;
        Self {
            kind,
            input_len,
            output_len,
            metadata_bytes,
            net_gain_estimate: structural_gain - metadata_bytes as i32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDescriptor {
    pub kind: SourceKind,
    pub source_hash: SourceHash,
    pub byte_len: usize,
    pub mime: Option<String>,
    pub label: Option<String>,
}

impl SourceDescriptor {
    pub fn runtime_object_key_from_cue(&self, cue: SparseCue) -> PredictiveObjectKey {
        PredictiveObjectKey {
            source_hash: self.source_hash,
            source_kind: self.kind,
            object_kind: self.kind.object_kind(),
            cue,
        }
    }

    pub fn runtime_object_key_from_bytes(&self, exact_bytes: &[u8]) -> PredictiveObjectKey {
        self.runtime_object_key_from_cue(self.structural_cue_summary(exact_bytes).cue)
    }

    pub fn chpmt_object_key_from_cue(&self, cue: SparseCue) -> PredictiveObjectKey {
        self.runtime_object_key_from_cue(cue)
    }

    pub fn chpmt_cache_key_from_bytes(&self, exact_bytes: &[u8]) -> PredictiveObjectKey {
        self.runtime_object_key_from_bytes(exact_bytes)
    }

    pub fn object_cache_key_from_bytes(&self, exact_bytes: &[u8]) -> PredictiveObjectKey {
        self.runtime_object_key_from_bytes(exact_bytes)
    }

    pub fn predictive_object_key_from_cue(&self, cue: SparseCue) -> PredictiveObjectKey {
        self.runtime_object_key_from_cue(cue)
    }

    pub fn substrate_object_key_from_cue(
        &self,
        cue: SparseCue,
        object_kind: ObjectKind,
    ) -> PredictiveObjectKey {
        PredictiveObjectKey {
            source_hash: self.source_hash,
            source_kind: self.kind,
            object_kind,
            cue,
        }
    }

    pub fn substrate_object_key_from_bytes(
        &self,
        object_kind: ObjectKind,
        exact_bytes: &[u8],
    ) -> PredictiveObjectKey {
        self.substrate_object_key_from_cue(
            self.structural_cue_summary(exact_bytes).cue,
            object_kind,
        )
    }

    pub fn structural_cue_summary(&self, exact_bytes: &[u8]) -> StructuralCueSummary {
        StructuralCueSummary {
            cue: derive_sparse_cue(self.kind, exact_bytes),
            source_kind: self.kind,
            plane: self.kind.memory_plane(),
            route_family: self.kind.route_family(),
            object_kind: self.kind.object_kind(),
            precision_band: self.kind.precision_band(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedSource {
    pub descriptor: SourceDescriptor,
    pub canonical_bytes: Vec<u8>,
    #[serde(default)]
    pub preprocess_steps: Vec<AppliedPreprocessStep>,
}

impl PreparedSource {
    pub fn source_hash(&self) -> SourceHash {
        self.descriptor.source_hash
    }

    pub fn canonical_len(&self) -> usize {
        self.canonical_bytes.len()
    }

    pub fn cue_family(&self) -> CueFamily {
        self.descriptor.kind.cue_family()
    }

    pub fn family_cues(&self) -> SparseCue {
        derive_sparse_cue(self.descriptor.kind, &self.canonical_bytes)
    }

    pub fn structural_cues(&self) -> StructuralCueSummary {
        self.descriptor
            .structural_cue_summary(&self.canonical_bytes)
    }

    pub fn preprocessing_net_gain(&self) -> i32 {
        self.preprocess_steps
            .iter()
            .map(|step| step.net_gain_estimate)
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictiveObjectKey {
    pub source_hash: SourceHash,
    pub source_kind: SourceKind,
    pub object_kind: ObjectKind,
    pub cue: SparseCue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeTraceLookupKey {
    pub source_kind: SourceKind,
    pub object_kind: ObjectKind,
    pub cue: SparseCue,
    pub context_hash: crate::ContextHash,
}

impl EpisodeTraceLookupKey {
    pub fn storage_key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"pulzz/episode-trace-key-v1");
        hasher.update([self.source_kind.tag()]);
        hasher.update([self.object_kind as u8]);
        hasher.update(self.cue.family_bits.to_le_bytes());
        hasher.update(self.cue.role_bits.to_le_bytes());
        hasher.update(self.cue.delimiter_bits.to_le_bytes());
        hasher.update(self.cue.length_bucket_bits.to_le_bytes());
        hasher.update(self.cue.temporal_bits.to_le_bytes());
        hasher.update(self.context_hash.0.to_le_bytes());
        hex_encode(&hasher.finalize())
    }
}

impl PredictiveObjectKey {
    pub fn storage_key(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(PREDICTIVE_OBJECT_KEY_DOMAIN);
        hasher.update(self.source_hash.0);
        hasher.update([self.source_kind.tag()]);
        hasher.update([self.object_kind as u8]);
        hasher.update(self.cue.family_bits.to_le_bytes());
        hasher.update(self.cue.role_bits.to_le_bytes());
        hasher.update(self.cue.delimiter_bits.to_le_bytes());
        hasher.update(self.cue.length_bucket_bits.to_le_bytes());
        hasher.update(self.cue.temporal_bits.to_le_bytes());
        hex_encode(&hasher.finalize())
    }

    pub fn object_store_family(&self) -> &'static str {
        self.source_kind.slug()
    }

    pub fn object_class_slug(&self) -> &'static str {
        self.object_kind.storage_slug()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredictiveObjectStorePath {
    pub family_partition: String,
    pub object_class: String,
    pub cue_partition: String,
    pub storage_key: String,
}

impl PredictiveObjectStorePath {
    pub fn from_key(key: &PredictiveObjectKey, cue: SparseCue) -> Self {
        let source_seed = u64::from_le_bytes(key.source_hash.0[..8].try_into().unwrap_or([0; 8]));
        let shard = keyed_permute_indices(16, source_seed)
            .first()
            .copied()
            .unwrap_or_default();
        Self {
            family_partition: key.object_store_family().to_string(),
            object_class: key.object_class_slug().to_string(),
            cue_partition: format!(
                "sh{:02x}-lb{:02x}-fb{:016x}-db{:016x}",
                shard,
                cue.length_bucket_bits.trailing_zeros(),
                cue.family_bits,
                cue.delimiter_bits
            ),
            storage_key: key.storage_key(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChpmtObject {
    pub descriptor: SourceDescriptor,
    pub object_key: PredictiveObjectKey,
    pub source_kind: SourceKind,
    pub object_kind: ObjectKind,
    pub cue: SparseCue,
    pub exact_bytes: Vec<u8>,
}

impl ChpmtObject {
    pub fn from_exact_bytes(
        descriptor: SourceDescriptor,
        object_key: PredictiveObjectKey,
        source_kind: SourceKind,
        object_kind: ObjectKind,
        cue: SparseCue,
        exact_bytes: Vec<u8>,
    ) -> Self {
        Self {
            descriptor,
            object_key,
            source_kind,
            object_kind,
            cue,
            exact_bytes,
        }
    }

    pub fn runtime_exact_bytes(&self) -> &[u8] {
        &self.exact_bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactStateMaterial {
    pub source_kind: SourceKind,
    pub exact_bytes: Vec<u8>,
}

impl ExactStateMaterial {
    pub fn new(source_kind: SourceKind, exact_bytes: Vec<u8>) -> Self {
        Self {
            source_kind,
            exact_bytes,
        }
    }

    pub fn copy_exact(source_kind: SourceKind, exact_bytes: &[u8]) -> Self {
        Self::new(source_kind, exact_bytes.to_vec())
    }

    pub fn runtime_exact_bytes(&self) -> &[u8] {
        &self.exact_bytes
    }

    pub fn byte_len(&self) -> usize {
        self.exact_bytes.len()
    }
}

impl From<&ChpmtObject> for ExactStateMaterial {
    fn from(object: &ChpmtObject) -> Self {
        Self::new(object.source_kind, object.exact_bytes.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedPredictiveObject {
    pub descriptor: SourceDescriptor,
    pub object_key: PredictiveObjectKey,
    pub cue: SparseCue,
    pub object: ChpmtObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetaPayload {
    pub source_hash: SourceHash,
    pub source_kind: SourceKind,
    pub source_len: u64,
    pub mime: Option<String>,
    pub label: Option<String>,
}

impl SourceMetaPayload {
    pub fn encode(&self) -> Result<Vec<u8>, SourceError> {
        let mime_len = optional_string_len(self.mime.as_deref())?;
        let label_len = optional_string_len(self.label.as_deref())?;
        let mut out = Vec::with_capacity(
            SOURCE_HASH_LEN
                + 1
                + 8
                + 2
                + self.mime.as_ref().map(|value| value.len()).unwrap_or(0)
                + 2
                + self.label.as_ref().map(|value| value.len()).unwrap_or(0),
        );
        out.extend_from_slice(&self.source_hash.0);
        out.push(self.source_kind.tag());
        out.extend_from_slice(&self.source_len.to_le_bytes());
        encode_optional_string(&mut out, mime_len, self.mime.as_deref());
        encode_optional_string(&mut out, label_len, self.label.as_deref());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SourceError> {
        if bytes.len() < SOURCE_HASH_LEN + 1 + 8 + 2 + 2 {
            return Err(SourceError::InvalidSourceMetaPayload(
                "source meta payload is truncated".to_string(),
            ));
        }
        let mut source_hash = [0_u8; SOURCE_HASH_LEN];
        source_hash.copy_from_slice(&bytes[..SOURCE_HASH_LEN]);
        let source_kind = SourceKind::from_tag(bytes[SOURCE_HASH_LEN])?;
        let source_len = u64::from_le_bytes(
            bytes[SOURCE_HASH_LEN + 1..SOURCE_HASH_LEN + 9]
                .try_into()
                .unwrap(),
        );
        let mut cursor = SOURCE_HASH_LEN + 9;
        let mime = decode_optional_string(bytes, &mut cursor)?;
        let label = decode_optional_string(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(SourceError::InvalidSourceMetaPayload(
                "source meta payload has trailing bytes".to_string(),
            ));
        }
        Ok(Self {
            source_hash: SourceHash(source_hash),
            source_kind,
            source_len,
            mime,
            label,
        })
    }
}

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("invalid utf-8 in prepared source")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("unknown source kind tag: {0}")]
    UnknownSourceKind(u8),
    #[error("invalid source meta payload: {0}")]
    InvalidSourceMetaPayload(String),
    #[error("source metadata string is too large: {len} bytes")]
    SourceMetaStringTooLarge { len: usize },
}

pub fn prepare_text_source(text: &str, label: Option<String>) -> PreparedSource {
    let original = text.as_bytes();
    let canonical_bytes = canonicalize_text_bytes(text);
    let preprocess_steps = text_preprocess_steps(original, &canonical_bytes);
    let descriptor = SourceDescriptor {
        kind: SourceKind::Text,
        source_hash: compute_source_hash(SourceKind::Text, None, &canonical_bytes),
        byte_len: canonical_bytes.len(),
        mime: Some("text/plain; charset=utf-8".to_string()),
        label,
    };
    PreparedSource {
        descriptor,
        canonical_bytes,
        preprocess_steps,
    }
}

pub fn prepare_json_source(json: &str, label: Option<String>) -> PreparedSource {
    let canonical_bytes = json.as_bytes().to_vec();
    let canonical_len = canonical_bytes.len();
    let descriptor = SourceDescriptor {
        kind: SourceKind::Json,
        source_hash: compute_source_hash(
            SourceKind::Json,
            Some("application/json"),
            &canonical_bytes,
        ),
        byte_len: canonical_bytes.len(),
        mime: Some("application/json".to_string()),
        label,
    };
    PreparedSource {
        descriptor,
        canonical_bytes,
        preprocess_steps: vec![AppliedPreprocessStep::new(
            ReversiblePreprocessKind::Identity,
            json.len(),
            canonical_len,
        )],
    }
}

pub fn prepare_binary_source(
    label: Option<String>,
    mime: Option<&str>,
    bytes: &[u8],
) -> PreparedSource {
    let canonical_bytes = bytes.to_vec();
    let canonical_len = canonical_bytes.len();
    let descriptor = SourceDescriptor {
        kind: SourceKind::Binary,
        source_hash: compute_source_hash(SourceKind::Binary, mime, &canonical_bytes),
        byte_len: canonical_bytes.len(),
        mime: mime.map(str::to_string),
        label,
    };
    PreparedSource {
        descriptor,
        canonical_bytes,
        preprocess_steps: vec![AppliedPreprocessStep::new(
            ReversiblePreprocessKind::Identity,
            bytes.len(),
            canonical_len,
        )],
    }
}

pub fn prepare_image_source(name: Option<String>, mime: &str, bytes: &[u8]) -> PreparedSource {
    let canonical_bytes = bytes.to_vec();
    let canonical_len = canonical_bytes.len();
    let descriptor = SourceDescriptor {
        kind: SourceKind::Image,
        source_hash: compute_source_hash(SourceKind::Image, Some(mime), &canonical_bytes),
        byte_len: canonical_bytes.len(),
        mime: Some(mime.to_string()),
        label: name,
    };
    PreparedSource {
        descriptor,
        canonical_bytes,
        preprocess_steps: vec![AppliedPreprocessStep::new(
            ReversiblePreprocessKind::Identity,
            bytes.len(),
            canonical_len,
        )],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ByteCueComponents {
    pub delimiter_bits: u64,
    pub role_bits: u64,
    pub length_bucket_bits: u64,
    pub temporal_bits: u64,
    pub local_hash_bits: u64,
    pub prefix_suffix_bits: u64,
}

pub fn derive_sparse_cue(source_kind: SourceKind, bytes: &[u8]) -> SparseCue {
    let components = derive_byte_cue_components(bytes);
    SparseCue::new(
        source_kind.family_bits(),
        source_kind.role_bits() | components.role_bits | components.local_hash_bits,
        components.delimiter_bits | components.prefix_suffix_bits,
        components.length_bucket_bits,
        components.temporal_bits,
    )
}

pub fn derive_byte_cue_components(bytes: &[u8]) -> ByteCueComponents {
    ByteCueComponents {
        delimiter_bits: delimiter_bits_from_bytes(bytes),
        role_bits: role_bits_from_bytes(bytes),
        length_bucket_bits: length_bucket_bits(bytes.len()),
        temporal_bits: temporal_bits_from_bytes(bytes),
        local_hash_bits: local_hash_bits_from_bytes(bytes),
        prefix_suffix_bits: prefix_suffix_bits_from_bytes(bytes),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CorpusSupportProfile {
    pub source_kind: SourceKind,
    pub required_planes: Vec<MemoryPlane>,
    pub required_object_kinds: Vec<ObjectKind>,
    pub minimum_length_bucket: u8,
    pub cue: SparseCue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CorpusSupportValidation {
    pub source_kind: SourceKind,
    pub family_supported: bool,
    pub cue_supported: bool,
    pub object_support_complete: bool,
}

impl PreparedSource {
    pub fn corpus_support_profile(&self) -> CorpusSupportProfile {
        CorpusSupportProfile {
            source_kind: self.descriptor.kind,
            required_planes: vec![
                MemoryPlane::AtomSubstrate,
                MemoryPlane::Assembly,
                MemoryPlane::Transform,
            ],
            required_object_kinds: vec![ObjectKind::ExactState, ObjectKind::SparseCue],
            minimum_length_bucket: self.family_cues().length_bucket_tag(),
            cue: self.family_cues(),
        }
    }
}

pub fn validate_corpus_support(
    prepared: &PreparedSource,
    supported_planes: &[MemoryPlane],
    supported_object_kinds: &[ObjectKind],
) -> CorpusSupportValidation {
    let profile = prepared.corpus_support_profile();
    let family_supported = supported_planes
        .iter()
        .any(|plane| *plane == prepared.descriptor.kind.memory_plane());
    let cue_supported = !profile.cue.is_empty();
    let object_support_complete = profile
        .required_object_kinds
        .iter()
        .all(|kind| supported_object_kinds.contains(kind));
    CorpusSupportValidation {
        source_kind: prepared.descriptor.kind,
        family_supported,
        cue_supported,
        object_support_complete,
    }
}

pub fn validate_capability_support(
    prepared: &PreparedSource,
    capability: &ChpmtCapabilityDescriptor,
) -> CorpusSupportValidation {
    let profile = prepared.corpus_support_profile();
    let family_supported = capability.supports_source_kind(prepared.descriptor.kind)
        && capability
            .supported_planes
            .iter()
            .any(|plane| *plane == prepared.descriptor.kind.memory_plane());
    let cue_supported = !profile.cue.is_empty();
    let object_support_complete = profile
        .required_object_kinds
        .iter()
        .all(|kind| capability.supported_object_kinds.contains(kind));
    CorpusSupportValidation {
        source_kind: prepared.descriptor.kind,
        family_supported,
        cue_supported,
        object_support_complete,
    }
}

fn role_bits_from_bytes(bytes: &[u8]) -> u64 {
    let mut bits = 0_u64;
    if bytes.is_ascii() {
        bits |= 1 << 12;
    }
    if bytes.iter().any(|byte| byte.is_ascii_uppercase()) {
        bits |= 1 << 13;
    }
    if bytes.iter().any(|byte| byte.is_ascii_digit()) {
        bits |= 1 << 14;
    }
    if kernel_contains_window(bytes, b"\r\n") {
        bits |= 1 << 15;
    }
    if kernel_contains_window(bytes, b"::") {
        bits |= 1 << 16;
    }
    bits
}

fn delimiter_bits_from_bytes(bytes: &[u8]) -> u64 {
    let mut bits = 0_u64;
    for &byte in kernel_byte_iter(bytes) {
        bits |= match byte {
            b'\n' => 1 << 0,
            b'\t' => 1 << 1,
            b' ' => 1 << 2,
            b',' => 1 << 3,
            b':' => 1 << 4,
            b';' => 1 << 5,
            b'{' | b'}' => 1 << 6,
            b'[' | b']' => 1 << 7,
            b'(' | b')' => 1 << 8,
            b'<' | b'>' => 1 << 9,
            b'/' | b'\\' => 1 << 10,
            b'=' => 1 << 11,
            b'"' | b'\'' => 1 << 12,
            _ => 0,
        };
    }
    bits
}

fn prefix_suffix_bits_from_bytes(bytes: &[u8]) -> u64 {
    let mut bits = 0_u64;
    if let Some(first) = bytes.first() {
        bits |= 1_u64 << ((first & 0x0f) as u64);
    }
    if let Some(last) = bytes.last() {
        bits |= 1_u64 << (16 + ((last & 0x0f) as u64));
    }
    if bytes.len() >= 2 {
        let prefix = u16::from_le_bytes([bytes[0], bytes[1]]);
        bits |= 1_u64 << (32 + (prefix as u64 & 0x0f));
        let suffix = u16::from_le_bytes([bytes[bytes.len() - 2], bytes[bytes.len() - 1]]);
        bits |= 1_u64 << (48 + (suffix as u64 & 0x0f));
    }
    bits
}

fn local_hash_bits_from_bytes(bytes: &[u8]) -> u64 {
    if bytes.is_empty() {
        return 0;
    }
    kernel_rolling_hash_window_bits(bytes, 4)
}

fn kernel_byte_iter(bytes: &[u8]) -> std::slice::Iter<'_, u8> {
    bytes.iter()
}

fn kernel_contains_window(bytes: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    bytes.windows(needle.len()).any(|window| window == needle)
}

fn kernel_rolling_hash_window_bits(bytes: &[u8], window: usize) -> u64 {
    if bytes.is_empty() || window == 0 {
        return 0;
    }
    let mut bits = 0_u64;
    let mut acc = 0_u32;
    for (idx, &byte) in bytes.iter().enumerate() {
        acc = acc.rotate_left(5) ^ ((byte as u32) + idx as u32 * 131);
        if idx + 1 >= window {
            bits |= 1_u64 << ((acc as u64) & 63);
        }
    }
    bits
}

fn kernel_adjacent_repeat(bytes: &[u8]) -> bool {
    bytes.windows(2).any(|window| window[0] == window[1])
}

fn kernel_strided_repeat(bytes: &[u8], stride: usize) -> bool {
    let width = stride.saturating_mul(2);
    width != 0
        && bytes
            .windows(width)
            .any(|window| window[0..stride] == window[stride..width])
}

fn length_bucket_bits(len: usize) -> u64 {
    match len {
        0 => 1 << 0,
        1..=32 => 1 << 1,
        33..=128 => 1 << 2,
        129..=512 => 1 << 3,
        513..=2048 => 1 << 4,
        2049..=8192 => 1 << 5,
        _ => 1 << 6,
    }
}

fn temporal_bits_from_bytes(bytes: &[u8]) -> u64 {
    let mut bits = 0_u64;
    if bytes.len() >= 64 {
        bits |= 1 << 0;
    }
    if kernel_adjacent_repeat(bytes) {
        bits |= 1 << 1;
    }
    if kernel_strided_repeat(bytes, 2) {
        bits |= 1 << 2;
    }
    let newline_count = bytes.iter().filter(|&&byte| byte == b'\n').count();
    if newline_count >= 2 {
        bits |= 1 << 3;
    }
    if bytes.len() >= 8 {
        let prefix = &bytes[..4];
        let suffix = &bytes[bytes.len() - 4..];
        if prefix == suffix {
            bits |= 1 << 4;
        }
    }
    bits
}

fn canonicalize_text_bytes(text: &str) -> Vec<u8> {
    text.replace("\r\n", "\n").replace('\r', "\n").into_bytes()
}

fn text_preprocess_steps(input: &[u8], output: &[u8]) -> Vec<AppliedPreprocessStep> {
    if input == output {
        vec![AppliedPreprocessStep::new(
            ReversiblePreprocessKind::Identity,
            input.len(),
            output.len(),
        )]
    } else {
        vec![AppliedPreprocessStep::new(
            ReversiblePreprocessKind::NormalizeLineEndings,
            input.len(),
            output.len(),
        )]
    }
}

pub fn compute_source_hash(kind: SourceKind, mime: Option<&str>, bytes: &[u8]) -> SourceHash {
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_HASH_DOMAIN);
    hasher.update([SOURCE_HASH_VERSION]);
    hasher.update([kind.tag()]);
    if let Some(mime) = mime {
        hasher.update((mime.len() as u32).to_le_bytes());
        hasher.update(mime.as_bytes());
    } else {
        hasher.update(0_u32.to_le_bytes());
    }
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut hash = [0_u8; SOURCE_HASH_LEN];
    hash.copy_from_slice(&digest);
    SourceHash(hash)
}

fn optional_string_len(value: Option<&str>) -> Result<u16, SourceError> {
    let Some(value) = value else {
        return Ok(OPTIONAL_STRING_NONE_LEN);
    };
    let len = value.len();
    if len >= OPTIONAL_STRING_NONE_LEN as usize {
        return Err(SourceError::SourceMetaStringTooLarge { len });
    }
    Ok(len as u16)
}

fn encode_optional_string(out: &mut Vec<u8>, len: u16, value: Option<&str>) {
    out.extend_from_slice(&len.to_le_bytes());
    if let Some(value) = value {
        out.extend_from_slice(value.as_bytes());
    }
}

fn decode_optional_string(bytes: &[u8], cursor: &mut usize) -> Result<Option<String>, SourceError> {
    if *cursor + 2 > bytes.len() {
        return Err(SourceError::InvalidSourceMetaPayload(
            "source meta payload is truncated".to_string(),
        ));
    }
    let len = u16::from_le_bytes(bytes[*cursor..*cursor + 2].try_into().unwrap());
    *cursor += 2;
    if len == OPTIONAL_STRING_NONE_LEN {
        return Ok(None);
    }
    let len = len as usize;
    let end = *cursor + len;
    if end > bytes.len() {
        return Err(SourceError::InvalidSourceMetaPayload(
            "source meta string overruns payload".to_string(),
        ));
    }
    let value = String::from_utf8(bytes[*cursor..end].to_vec())?;
    *cursor = end;
    Ok(Some(value))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectKind;

    #[test]
    fn text_hash_normalizes_line_endings() {
        let lf = prepare_text_source("a\nb\n", Some("lf".to_string()));
        let crlf = prepare_text_source("a\r\nb\r\n", Some("crlf".to_string()));
        assert_eq!(lf.source_hash(), crlf.source_hash());
        assert_eq!(lf.canonical_bytes, crlf.canonical_bytes);
    }

    #[test]
    fn image_hash_includes_mime() {
        let png = prepare_image_source(Some("x".to_string()), "image/png", &[1, 2, 3]);
        let webp = prepare_image_source(Some("x".to_string()), "image/webp", &[1, 2, 3]);
        assert_ne!(png.source_hash(), webp.source_hash());
    }

    #[test]
    fn source_meta_payload_round_trip() {
        let payload = SourceMetaPayload {
            source_hash: SourceHash([7; SOURCE_HASH_LEN]),
            source_kind: SourceKind::Text,
            source_len: 123,
            mime: Some("text/plain".to_string()),
            label: Some("demo.txt".to_string()),
        };
        assert_eq!(
            SourceMetaPayload::decode(&payload.encode().unwrap()).unwrap(),
            payload
        );
    }

    #[test]
    fn chpmt_cache_key_is_stable_for_same_material() {
        let source = prepare_text_source("hello", None);
        let first = source
            .descriptor
            .object_cache_key_from_bytes(&source.canonical_bytes);
        let second = source
            .descriptor
            .object_cache_key_from_bytes(&source.canonical_bytes);
        assert_eq!(first.storage_key(), second.storage_key());
        assert_eq!(first.object_kind, ObjectKind::ExactState);
    }

    #[test]
    fn sparse_cues_capture_family_and_delimiters() {
        let source = prepare_json_source("{\"items\":[1,2,3]}", None);
        let cue = source.family_cues();
        assert_ne!(cue.family_bits, 0);
        assert_ne!(cue.delimiter_bits & (1 << 6), 0);
        assert_ne!(cue.delimiter_bits & (1 << 7), 0);
    }
}
