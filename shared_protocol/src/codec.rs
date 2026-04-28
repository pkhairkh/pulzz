use std::sync::OnceLock;

use constriction::stream::{
    Decode, model::DefaultContiguousCategoricalEntropyModel, stack::DefaultAnsCoder,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::protocol::{CodecMode, ItemId, PredictorEntryMeta, PredictorState};
use crate::{
    ChpmtObject, CueFamily, ExactStateMaterial, ObjectKind, PreparedSource, SourceDescriptor,
    SourceKind, StructuralCueSummary, source::compute_source_hash,
};

const DIRECT_EXACT_HEADER_LEN: usize = 1;
const PACKED_PAYLOAD_VERSION: u8 = 2;
const PACKED_PAYLOAD_HEADER_LEN: usize = 1 + 1 + 2 + 4 + 2 + 4 + 4;
const BLOCK_LEN: usize = 256;
const PACKED_PACK16_ID: u16 = 0x0010;
const PACKED_PACK32_ID: u16 = 0x0020;
const PACKED_PACK64_ID: u16 = 0x0040;
const PACKED_PACK128_ID: u16 = 0x0080;
const PREDICTED_PACK_FALLBACK: u16 = PACKED_PACK32_ID;
const SMALL_PACKED_WIN_BYTES: usize = 8;

type EntropyModel = DefaultContiguousCategoricalEntropyModel;
type PackId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DataPlaneCodecPreference {
    Adaptive,
    #[default]
    DirectExactOnly,
}

impl DataPlaneCodecPreference {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Adaptive => "adaptive",
            Self::DirectExactOnly => "direct_exact_only",
        }
    }

    pub fn forces_direct_exact(self) -> bool {
        matches!(self, Self::DirectExactOnly)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CodecDecision {
    DirectExact,
    PackedExact,
    PredictedExact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidualCodingMode {
    None,
    AllZero,
    SmallSignedRans,
    SparsePositions,
    LiteralRaw,
}

pub fn derive_prepared_source_cues(prepared: &PreparedSource) -> StructuralCueSummary {
    prepared.structural_cues()
}

pub fn derive_prepared_source_kind(prepared: &PreparedSource) -> CueFamily {
    prepared.cue_family()
}

fn packed_asset_source_kind(source_kind: SourceKind) -> SourceKind {
    source_kind.compact_asset_kind()
}

fn exact_material_from_source_kind(
    source_kind: SourceKind,
    exact_bytes: Vec<u8>,
) -> ExactStateMaterial {
    ExactStateMaterial::new(source_kind, exact_bytes)
}

fn runtime_object_from_exact_bytes(
    source_kind: SourceKind,
    object_kind: ObjectKind,
    exact_bytes: &[u8],
) -> ChpmtObject {
    let descriptor = SourceDescriptor {
        kind: source_kind,
        source_hash: compute_source_hash(source_kind, None, exact_bytes),
        byte_len: exact_bytes.len(),
        mime: None,
        label: None,
    };
    let cue = descriptor.structural_cue_summary(exact_bytes).cue;
    let object_key = descriptor.runtime_object_key_from_bytes(exact_bytes);
    ChpmtObject::from_exact_bytes(
        descriptor,
        object_key,
        source_kind,
        object_kind,
        cue,
        exact_bytes.to_vec(),
    )
}

fn runtime_object_from_material(material: &ExactStateMaterial) -> ChpmtObject {
    runtime_object_from_exact_bytes(
        material.source_kind,
        ObjectKind::ExactState,
        &material.exact_bytes,
    )
}

fn exact_material(source_kind: SourceKind, exact_bytes: &[u8]) -> ExactStateMaterial {
    ExactStateMaterial::copy_exact(source_kind, exact_bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadInspection {
    pub source_kind: SourceKind,
    pub original_len: usize,
    pub residual_mode: ResidualCodingMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaEncodedMaterial {
    pub source_kind: SourceKind,
    pub original_len: u32,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedPayload {
    pub mode: CodecMode,
    pub payload: Vec<u8>,
    pub runtime_object: ChpmtObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    #[error("codec payload is truncated: expected at least {minimum}, actual {actual}")]
    TruncatedPayload { minimum: usize, actual: usize },
    #[error("payload length mismatch: expected {expected}, actual {actual}")]
    PayloadLenMismatch { expected: usize, actual: usize },
    #[error("delta decode requires a predictor")]
    MissingPredictor,
    #[error("unsupported control codec mode")]
    UnsupportedCodecMode,
    #[error("unsupported packed pack id: {0:?}")]
    UnsupportedPackId(PackId),
    #[error("unknown source kind tag: {0}")]
    UnknownSourceKind(u8),
    #[error("invalid packed payload version: {0}")]
    InvalidPackedVersion(u8),
    #[error("packed payload pack mismatch: header={header:?} payload={payload:?}")]
    PayloadPackMismatch { header: PackId, payload: PackId },
    #[error("invalid block count in packed payload")]
    InvalidBlockCount,
    #[error("invalid code count in packed payload: expected {expected}, actual {actual}")]
    InvalidCodeCount { expected: usize, actual: usize },
    #[error("invalid residual tag: {0}")]
    InvalidResidualTag(u8),
    #[error("invalid layer specification")]
    InvalidLayerSpec,
    #[error("packed assets failed to load: {0}")]
    AssetLoad(String),
    #[error("entropy coding error: {0}")]
    EntropyCoding(String),
}

#[derive(Debug, Clone, Deserialize)]
struct RawLinearLayerAsset {
    in_dim: usize,
    out_dim: usize,
    input_zero_point: i16,
    output_zero_point: i16,
    divisor: i32,
    apply_relu: bool,
    clamp_min: i16,
    clamp_max: i16,
    weights: Vec<i8>,
    bias: Vec<i32>,
}

#[derive(Debug, Clone)]
struct LinearLayerAsset {
    in_dim: usize,
    out_dim: usize,
    input_zero_point: i16,
    output_zero_point: i16,
    divisor: i32,
    apply_relu: bool,
    clamp_min: i16,
    clamp_max: i16,
    weights: Box<[i8]>,
    bias: Box<[i32]>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawProfileAsset {
    #[serde(rename = "profile_name")]
    _profile_name: String,
    lane_profile_id: u16,
    latent_dim: usize,
    fsq_levels: Vec<i16>,
    latent_probabilities: Vec<f64>,
    analysis_head: RawLinearLayerAsset,
    synthesis_head: RawLinearLayerAsset,
}

#[derive(Debug, Clone)]
struct ProfileAsset {
    lane_profile_id: u16,
    latent_dim: usize,
    fsq_levels: Vec<i16>,
    latent_model: EntropyModel,
    analysis_head: LinearLayerAsset,
    synthesis_head: LinearLayerAsset,
}

#[derive(Debug, Clone, Deserialize)]
struct RawResidualAsset {
    all_zero_tag: u8,
    small_signed_rans_tag: u8,
    sparse_positions_tag: u8,
    literal_raw_tag: u8,
    small_range: i16,
    sparse_threshold_percent: f32,
    small_signed_probabilities: Vec<f64>,
}

#[derive(Debug, Clone)]
struct ResidualAsset {
    all_zero_tag: u8,
    small_signed_rans_tag: u8,
    sparse_positions_tag: u8,
    literal_raw_tag: u8,
    small_range: i16,
    sparse_threshold_percent: f32,
    small_signed_model: EntropyModel,
}

#[derive(Debug, Clone, Deserialize)]
struct RawFamilyAsset {
    format_version: u8,
    #[serde(rename = "family_name")]
    _family_name: String,
    block_len: usize,
    residual: RawResidualAsset,
    analysis_trunk: Vec<RawLinearLayerAsset>,
    synthesis_trunk: Vec<RawLinearLayerAsset>,
    profiles: Vec<RawProfileAsset>,
}

#[derive(Debug, Clone)]
struct FamilyAsset {
    residual: ResidualAsset,
    analysis_trunk: Vec<LinearLayerAsset>,
    synthesis_trunk: Vec<LinearLayerAsset>,
    profiles: Vec<ProfileAsset>,
}

#[derive(Debug, Clone)]
struct EmbeddedAssets {
    text: FamilyAsset,
    json: FamilyAsset,
    binary: FamilyAsset,
}

fn embedded_assets() -> Result<&'static EmbeddedAssets, CodecError> {
    static CELL: OnceLock<Result<EmbeddedAssets, CodecError>> = OnceLock::new();
    CELL.get_or_init(|| {
        Ok(EmbeddedAssets {
            text: parse_family_asset(include_str!("../assets/byte_latent/text_v2.json"))?,
            json: parse_family_asset(include_str!("../assets/byte_latent/json_v2.json"))?,
            binary: parse_family_asset(include_str!("../assets/byte_latent/binary_v2.json"))?,
        })
    })
    .as_ref()
    .map_err(Clone::clone)
}

fn parse_family_asset(json: &str) -> Result<FamilyAsset, CodecError> {
    let raw: RawFamilyAsset =
        serde_json::from_str(json).map_err(|error| CodecError::AssetLoad(error.to_string()))?;
    if raw.format_version != PACKED_PAYLOAD_VERSION || raw.block_len != BLOCK_LEN {
        return Err(CodecError::AssetLoad(format!(
            "unexpected asset shape: version={} block_len={}",
            raw.format_version, raw.block_len
        )));
    }

    let analysis_trunk = raw
        .analysis_trunk
        .iter()
        .map(LinearLayerAsset::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let synthesis_trunk = raw
        .synthesis_trunk
        .iter()
        .map(LinearLayerAsset::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let residual = ResidualAsset::try_from(raw.residual)?;
    let profiles = raw
        .profiles
        .into_iter()
        .map(ProfileAsset::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(FamilyAsset {
        residual,
        analysis_trunk,
        synthesis_trunk,
        profiles,
    })
}

impl TryFrom<&RawLinearLayerAsset> for LinearLayerAsset {
    type Error = CodecError;

    fn try_from(raw: &RawLinearLayerAsset) -> Result<Self, Self::Error> {
        if raw.divisor <= 0
            || raw.weights.len() != raw.in_dim * raw.out_dim
            || raw.bias.len() != raw.out_dim
        {
            return Err(CodecError::InvalidLayerSpec);
        }
        Ok(Self {
            in_dim: raw.in_dim,
            out_dim: raw.out_dim,
            input_zero_point: raw.input_zero_point,
            output_zero_point: raw.output_zero_point,
            divisor: raw.divisor,
            apply_relu: raw.apply_relu,
            clamp_min: raw.clamp_min,
            clamp_max: raw.clamp_max,
            weights: raw.weights.clone().into_boxed_slice(),
            bias: raw.bias.clone().into_boxed_slice(),
        })
    }
}

impl TryFrom<RawResidualAsset> for ResidualAsset {
    type Error = CodecError;

    fn try_from(raw: RawResidualAsset) -> Result<Self, Self::Error> {
        if raw.small_range <= 0 || raw.small_signed_probabilities.is_empty() {
            return Err(CodecError::AssetLoad(
                "invalid residual probabilities".to_string(),
            ));
        }
        Ok(Self {
            all_zero_tag: raw.all_zero_tag,
            small_signed_rans_tag: raw.small_signed_rans_tag,
            sparse_positions_tag: raw.sparse_positions_tag,
            literal_raw_tag: raw.literal_raw_tag,
            small_range: raw.small_range,
            sparse_threshold_percent: raw.sparse_threshold_percent,
            small_signed_model: build_entropy_model(&raw.small_signed_probabilities)?,
        })
    }
}

impl TryFrom<RawProfileAsset> for ProfileAsset {
    type Error = CodecError;

    fn try_from(raw: RawProfileAsset) -> Result<Self, Self::Error> {
        if raw.fsq_levels.len() != 16 || raw.latent_probabilities.len() != 16 {
            return Err(CodecError::AssetLoad(
                "invalid fsq asset probabilities".to_string(),
            ));
        }
        Ok(Self {
            lane_profile_id: raw.lane_profile_id,
            latent_dim: raw.latent_dim,
            fsq_levels: raw.fsq_levels,
            latent_model: build_entropy_model(&raw.latent_probabilities)?,
            analysis_head: LinearLayerAsset::try_from(&raw.analysis_head)?,
            synthesis_head: LinearLayerAsset::try_from(&raw.synthesis_head)?,
        })
    }
}

fn family_asset(source_kind: SourceKind) -> Result<&'static FamilyAsset, CodecError> {
    let assets = embedded_assets()?;
    Ok(match packed_asset_source_kind(source_kind) {
        SourceKind::Text => &assets.text,
        SourceKind::Json => &assets.json,
        SourceKind::Binary | SourceKind::Image => &assets.binary,
    })
}

fn profile_asset(
    source_kind: SourceKind,
    profile: PackId,
) -> Result<&'static ProfileAsset, CodecError> {
    family_asset(source_kind)?
        .profiles
        .iter()
        .find(|candidate| candidate.lane_profile_id == profile)
        .ok_or(CodecError::UnsupportedPackId(profile))
}

fn build_entropy_model(probabilities: &[f64]) -> Result<EntropyModel, CodecError> {
    if probabilities.is_empty() {
        return Err(CodecError::AssetLoad("empty probability table".to_string()));
    }
    DefaultContiguousCategoricalEntropyModel::from_floating_point_probabilities_fast(
        probabilities,
        None,
    )
    .map_err(|error| CodecError::AssetLoad(format!("invalid entropy table: {error:?}")))
}

pub fn choose_codec_mode(
    current: &ExactStateMaterial,
    predictor_state: PredictorState,
    predicted: Option<&ExactStateMaterial>,
) -> CodecDecision {
    choose_codec_mode_with_preference(
        current,
        predictor_state,
        predicted,
        DataPlaneCodecPreference::DirectExactOnly,
    )
}

pub fn choose_codec_mode_with_preference(
    current: &ExactStateMaterial,
    predictor_state: PredictorState,
    predicted: Option<&ExactStateMaterial>,
    preference: DataPlaneCodecPreference,
) -> CodecDecision {
    encode_best_exact_payload_with_preference(current, predictor_state, predicted, preference)
        .map(|encoded| match encoded.mode {
            CodecMode::DirectExact => CodecDecision::DirectExact,
            CodecMode::PackedExact => CodecDecision::PackedExact,
            CodecMode::PredictedExact => CodecDecision::PredictedExact,
            CodecMode::None => CodecDecision::DirectExact,
        })
        .unwrap_or(CodecDecision::DirectExact)
}

pub fn predictor_state_for(
    current_item_id: ItemId,
    predictor_meta: Option<PredictorEntryMeta>,
) -> PredictorState {
    match predictor_meta {
        Some(meta) if meta.item_id == current_item_id => PredictorState::Ready(meta),
        _ => PredictorState::Empty,
    }
}

pub fn encode_best_runtime_object_payload(
    source_kind: SourceKind,
    object_kind: ObjectKind,
    exact_bytes: &[u8],
    predictor_state: PredictorState,
    predicted_exact_bytes: Option<&[u8]>,
) -> Result<EncodedPayload, CodecError> {
    encode_best_runtime_object_payload_with_preference(
        source_kind,
        object_kind,
        exact_bytes,
        predictor_state,
        predicted_exact_bytes,
        DataPlaneCodecPreference::DirectExactOnly,
    )
}

pub fn encode_best_runtime_object_payload_with_preference(
    source_kind: SourceKind,
    object_kind: ObjectKind,
    exact_bytes: &[u8],
    predictor_state: PredictorState,
    predicted_exact_bytes: Option<&[u8]>,
    preference: DataPlaneCodecPreference,
) -> Result<EncodedPayload, CodecError> {
    let current = exact_material(source_kind, exact_bytes);
    let predicted = predicted_exact_bytes.map(|bytes| exact_material(source_kind, bytes));
    let mut encoded = encode_best_exact_payload_with_preference(
        &current,
        predictor_state,
        predicted.as_ref(),
        preference,
    )?;
    encoded.runtime_object = runtime_object_from_exact_bytes(source_kind, object_kind, exact_bytes);
    Ok(encoded)
}

pub fn decode_data_payload_to_runtime_object(
    payload: &[u8],
    mode: CodecMode,
    predicted_exact_bytes: Option<&[u8]>,
    descriptor: Option<SourceDescriptor>,
    fallback_source_kind: SourceKind,
    object_kind: ObjectKind,
) -> Result<ChpmtObject, CodecError> {
    let predictor_source_kind = descriptor
        .as_ref()
        .map(|descriptor| descriptor.kind)
        .unwrap_or(fallback_source_kind);
    let predicted = predicted_exact_bytes.map(|bytes| exact_material(predictor_source_kind, bytes));
    let block = decode_data_payload(payload, mode, predicted.as_ref())?;
    let source_kind = descriptor
        .as_ref()
        .map(|descriptor| descriptor.kind)
        .unwrap_or(block.source_kind);
    let descriptor = descriptor.unwrap_or_else(|| SourceDescriptor {
        kind: source_kind,
        source_hash: compute_source_hash(source_kind, None, &block.exact_bytes),
        byte_len: block.exact_bytes.len(),
        mime: None,
        label: None,
    });
    let cue = descriptor.structural_cue_summary(&block.exact_bytes).cue;
    let object_key = descriptor.runtime_object_key_from_bytes(&block.exact_bytes);
    Ok(ChpmtObject::from_exact_bytes(
        descriptor,
        object_key,
        source_kind,
        object_kind,
        cue,
        block.exact_bytes,
    ))
}

pub fn encode_best_exact_payload(
    current: &ExactStateMaterial,
    predictor_state: PredictorState,
    predicted: Option<&ExactStateMaterial>,
) -> Result<EncodedPayload, CodecError> {
    encode_best_exact_payload_with_preference(
        current,
        predictor_state,
        predicted,
        DataPlaneCodecPreference::DirectExactOnly,
    )
}

pub fn encode_best_exact_payload_with_preference(
    current: &ExactStateMaterial,
    predictor_state: PredictorState,
    predicted: Option<&ExactStateMaterial>,
    preference: DataPlaneCodecPreference,
) -> Result<EncodedPayload, CodecError> {
    let copy_payload = encode_direct_exact_payload(current);
    let copy_len = copy_payload.len();
    let minimum_win = minimum_packed_win(copy_len, current.exact_bytes.len());
    let runtime_object = runtime_object_from_material(current);
    let mut best = EncodedPayload {
        mode: CodecMode::DirectExact,
        payload: copy_payload,
        runtime_object: runtime_object.clone(),
    };

    if preference.forces_direct_exact() {
        return Ok(best);
    }

    for profile in candidate_profiles(current.exact_bytes.len()) {
        let raw = encode_packed_payload(current.source_kind, *profile, &current.exact_bytes)?;
        if packed_candidate_wins(raw.len(), copy_len, minimum_win)
            && (best.mode == CodecMode::DirectExact || raw.len() < best.payload.len())
        {
            best = EncodedPayload {
                mode: CodecMode::PackedExact,
                payload: raw,
                runtime_object: runtime_object.clone(),
            };
        }
    }

    if !predictor_state.requires_raw() {
        if let Some(predicted) = predicted {
            let delta = wrapping_delta_bytes(&current.exact_bytes, &predicted.exact_bytes);
            for profile in candidate_profiles(current.exact_bytes.len()) {
                let encoded = encode_packed_payload(current.source_kind, *profile, &delta)?;
                if packed_candidate_wins(encoded.len(), copy_len, minimum_win)
                    && (best.mode == CodecMode::DirectExact || encoded.len() < best.payload.len())
                {
                    best = EncodedPayload {
                        mode: CodecMode::PredictedExact,
                        payload: encoded,
                        runtime_object: runtime_object.clone(),
                    };
                }
            }
        }
    }

    Ok(best)
}

fn packed_candidate_wins(candidate_len: usize, copy_len: usize, minimum_win: usize) -> bool {
    candidate_len.saturating_add(minimum_win) <= copy_len
}

fn minimum_packed_win(copy_len: usize, exact_len: usize) -> usize {
    if exact_len <= 1024 {
        SMALL_PACKED_WIN_BYTES
    } else {
        copy_len.div_ceil(100).max(1)
    }
}

pub fn encode_exact_payload(block: &ExactStateMaterial) -> Vec<u8> {
    encode_direct_exact_payload(block)
}

pub fn decode_exact_payload(payload: &[u8]) -> Result<ExactStateMaterial, CodecError> {
    decode_direct_exact_payload(payload)
}

pub fn encode_delta_material(
    current: &ExactStateMaterial,
    predicted: &ExactStateMaterial,
) -> DeltaEncodedMaterial {
    let profile = PREDICTED_PACK_FALLBACK;
    let payload = encode_packed_payload(
        current.source_kind,
        profile,
        &wrapping_delta_bytes(&current.exact_bytes, &predicted.exact_bytes),
    )
    .expect("supported profile");
    DeltaEncodedMaterial {
        source_kind: current.source_kind,
        original_len: current.exact_bytes.len() as u32,
        bytes: payload,
    }
}

pub fn encode_delta_payload(encoded: &DeltaEncodedMaterial) -> Vec<u8> {
    encoded.bytes.clone()
}

pub fn decode_delta_payload(payload: &[u8]) -> Result<DeltaEncodedMaterial, CodecError> {
    let (family, _pack_id, original_len, _, _, _) = parse_packed_payload_header(payload)?;
    Ok(DeltaEncodedMaterial {
        source_kind: family,
        original_len,
        bytes: payload.to_vec(),
    })
}

pub fn decode_delta_material(
    encoded: &DeltaEncodedMaterial,
    predicted: &ExactStateMaterial,
) -> Result<ExactStateMaterial, CodecError> {
    let delta = decode_packed_payload(&encoded.bytes)?;
    Ok(exact_material_from_source_kind(
        encoded.source_kind,
        apply_wrapping_delta(&delta.exact_bytes, &predicted.exact_bytes),
    ))
}

pub fn decode_data_payload(
    payload: &[u8],
    mode: CodecMode,
    predicted: Option<&ExactStateMaterial>,
) -> Result<ExactStateMaterial, CodecError> {
    match mode {
        CodecMode::DirectExact => decode_direct_exact_payload(payload),
        CodecMode::PackedExact => decode_packed_payload(payload),
        CodecMode::PredictedExact => {
            let predicted = predicted.ok_or(CodecError::MissingPredictor)?;
            let delta = decode_packed_payload(payload)?;
            Ok(exact_material_from_source_kind(
                delta.source_kind,
                apply_wrapping_delta(&delta.exact_bytes, &predicted.exact_bytes),
            ))
        }
        CodecMode::None => {
            // No codec applied — payload IS the raw exact bytes with no
            // encoding header. Source kind is unknown (no tag byte), so
            // default to Binary. This handles ExactState records that carry
            // no codec encoding (e.g., eviction/invalidation markers, or
            // records where no encoding was applied).
            Ok(exact_material_from_source_kind(
                SourceKind::Binary,
                payload.to_vec(),
            ))
        }
    }
}

pub fn inspect_data_payload(
    payload: &[u8],
    mode: CodecMode,
) -> Result<PayloadInspection, CodecError> {
    match mode {
        CodecMode::DirectExact => {
            if payload.len() < DIRECT_EXACT_HEADER_LEN {
                return Err(CodecError::TruncatedPayload {
                    minimum: DIRECT_EXACT_HEADER_LEN,
                    actual: payload.len(),
                });
            }
            let family = SourceKind::from_tag(payload[0])
                .map_err(|_| CodecError::UnknownSourceKind(payload[0]))?;
            Ok(PayloadInspection {
                source_kind: family,
                original_len: payload.len().saturating_sub(DIRECT_EXACT_HEADER_LEN),
                residual_mode: ResidualCodingMode::None,
            })
        }
        CodecMode::PackedExact | CodecMode::PredictedExact => {
            let (family, _pack_id, original_len, _, code_len, residual_len) =
                parse_packed_payload_header(payload)?;
            let code_start = PACKED_PAYLOAD_HEADER_LEN;
            let code_end = code_start + code_len as usize;
            let residual_end = code_end + residual_len as usize;
            if payload.len() != residual_end {
                return Err(CodecError::PayloadLenMismatch {
                    expected: residual_end,
                    actual: payload.len(),
                });
            }
            let residual =
                payload
                    .get(code_end..residual_end)
                    .ok_or(CodecError::PayloadLenMismatch {
                        expected: residual_end,
                        actual: payload.len(),
                    })?;
            let spec = &family_asset(family)?.residual;
            let residual_mode = classify_residual_payload_tag(
                *residual.first().ok_or(CodecError::TruncatedPayload {
                    minimum: code_end + 1,
                    actual: payload.len(),
                })?,
                spec,
            )?;
            Ok(PayloadInspection {
                source_kind: family,
                original_len: original_len as usize,
                residual_mode,
            })
        }
        CodecMode::None => {
            // No codec applied — payload is raw/uncompressed. The original
            // content length equals the payload length. Source kind is unknown
            // (no header byte to inspect), so we default to Binary. Residual
            // mode is None because there is no residual coding layer.
            //
            // Previously this returned UnsupportedCodecMode, which caused
            // massive WARNING spam during benchmark runs when ExactState
            // records with CodecMode::None (eviction/invalidation markers,
            // or records where no encoding was applied) were inspected for
            // metrics via benchmark_original_payload_len / classify_payload_bytes.
            Ok(PayloadInspection {
                source_kind: SourceKind::Binary,
                original_len: payload.len(),
                residual_mode: ResidualCodingMode::None,
            })
        }
    }
}

fn encode_direct_exact_payload(block: &ExactStateMaterial) -> Vec<u8> {
    let mut payload = Vec::with_capacity(DIRECT_EXACT_HEADER_LEN + block.exact_bytes.len());
    payload.push(block.source_kind.tag());
    payload.extend_from_slice(&block.exact_bytes);
    payload
}

fn decode_direct_exact_payload(payload: &[u8]) -> Result<ExactStateMaterial, CodecError> {
    if payload.len() < DIRECT_EXACT_HEADER_LEN {
        return Err(CodecError::TruncatedPayload {
            minimum: DIRECT_EXACT_HEADER_LEN,
            actual: payload.len(),
        });
    }
    let family =
        SourceKind::from_tag(payload[0]).map_err(|_| CodecError::UnknownSourceKind(payload[0]))?;
    Ok(exact_material_from_source_kind(
        family,
        payload[1..].to_vec(),
    ))
}

fn classify_residual_payload_tag(
    tag: u8,
    spec: &ResidualAsset,
) -> Result<ResidualCodingMode, CodecError> {
    match tag {
        tag if tag == spec.all_zero_tag => Ok(ResidualCodingMode::AllZero),
        tag if tag == spec.small_signed_rans_tag => Ok(ResidualCodingMode::SmallSignedRans),
        tag if tag == spec.sparse_positions_tag => Ok(ResidualCodingMode::SparsePositions),
        tag if tag == spec.literal_raw_tag => Ok(ResidualCodingMode::LiteralRaw),
        other => Err(CodecError::InvalidResidualTag(other)),
    }
}

fn encode_packed_payload(
    family: SourceKind,
    profile: PackId,
    bytes: &[u8],
) -> Result<Vec<u8>, CodecError> {
    let family_asset = family_asset(family)?;
    let profile_asset = profile_asset(family, profile)?;
    let block_count = bytes.len().div_ceil(BLOCK_LEN);
    if block_count == 0 {
        return Ok(build_packed_payload(
            family,
            profile,
            0,
            0,
            &[],
            &[family_asset.residual.all_zero_tag],
        ));
    }

    let mut encoded_symbols = Vec::with_capacity(block_count * profile_asset.latent_dim);
    let mut predicted = Vec::with_capacity(block_count * BLOCK_LEN);
    for block_bytes in bytes.chunks(BLOCK_LEN) {
        let padded = pad_block(block_bytes);
        let code_symbols = encode_block_codes(family_asset, profile_asset, &padded)?;
        let reconstructed = decode_block_codes(family_asset, profile_asset, &code_symbols)?;
        encoded_symbols.extend(code_symbols.into_iter().map(|symbol| symbol as usize));
        predicted.extend_from_slice(&reconstructed);
    }
    predicted.truncate(bytes.len());
    let residual = centered_signed_residuals(bytes, &predicted);
    let code_payload = encode_ans_symbols(&encoded_symbols, &profile_asset.latent_model)?;
    let residual_payload = encode_residual_payload(&residual, &family_asset.residual)?;
    Ok(build_packed_payload(
        family,
        profile,
        bytes.len() as u32,
        block_count as u16,
        &code_payload,
        &residual_payload,
    ))
}

fn build_packed_payload(
    family: SourceKind,
    profile: PackId,
    original_len: u32,
    block_count: u16,
    code_bytes: &[u8],
    residual_bytes: &[u8],
) -> Vec<u8> {
    let mut payload =
        Vec::with_capacity(PACKED_PAYLOAD_HEADER_LEN + code_bytes.len() + residual_bytes.len());
    payload.push(family.tag());
    payload.push(PACKED_PAYLOAD_VERSION);
    payload.extend_from_slice(&profile.to_le_bytes());
    payload.extend_from_slice(&original_len.to_le_bytes());
    payload.extend_from_slice(&block_count.to_le_bytes());
    payload.extend_from_slice(&(code_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(residual_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(code_bytes);
    payload.extend_from_slice(residual_bytes);
    payload
}

fn decode_packed_payload(payload: &[u8]) -> Result<ExactStateMaterial, CodecError> {
    let (family, payload_profile, original_len, block_count, code_len, residual_len) =
        parse_packed_payload_header(payload)?;
    let family_asset = family_asset(family)?;
    let profile_asset = profile_asset(family, payload_profile)?;

    let code_start = PACKED_PAYLOAD_HEADER_LEN;
    let code_end = code_start + code_len as usize;
    let residual_end = code_end + residual_len as usize;
    if payload.len() != residual_end {
        return Err(CodecError::PayloadLenMismatch {
            expected: residual_end,
            actual: payload.len(),
        });
    }

    let code_symbol_count = block_count as usize * profile_asset.latent_dim;
    let code_symbols = decode_ans_symbols(
        &payload[code_start..code_end],
        code_symbol_count,
        &profile_asset.latent_model,
    )?;
    if code_symbols.len() != code_symbol_count {
        return Err(CodecError::InvalidCodeCount {
            expected: code_symbol_count,
            actual: code_symbols.len(),
        });
    }

    let mut predicted = Vec::with_capacity(block_count as usize * BLOCK_LEN);
    for chunk in code_symbols.chunks(profile_asset.latent_dim) {
        predicted.extend_from_slice(&decode_block_codes(
            family_asset,
            profile_asset,
            &chunk.iter().map(|symbol| *symbol as u8).collect::<Vec<_>>(),
        )?);
    }
    predicted.truncate(original_len as usize);
    let residual = decode_residual_payload(
        &payload[code_end..residual_end],
        original_len as usize,
        &family_asset.residual,
    )?;
    let exact_bytes = apply_centered_signed_residuals(&predicted, &residual);
    Ok(exact_material_from_source_kind(family, exact_bytes))
}

fn parse_packed_payload_header(
    payload: &[u8],
) -> Result<(SourceKind, PackId, u32, u16, u32, u32), CodecError> {
    if payload.len() < PACKED_PAYLOAD_HEADER_LEN {
        return Err(CodecError::TruncatedPayload {
            minimum: PACKED_PAYLOAD_HEADER_LEN,
            actual: payload.len(),
        });
    }
    let family =
        SourceKind::from_tag(payload[0]).map_err(|_| CodecError::UnknownSourceKind(payload[0]))?;
    let version = payload[1];
    if version != PACKED_PAYLOAD_VERSION {
        return Err(CodecError::InvalidPackedVersion(version));
    }
    let profile = u16::from_le_bytes(payload[2..4].try_into().unwrap());
    let original_len = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let block_count = u16::from_le_bytes(payload[8..10].try_into().unwrap());
    if (original_len > 0 && block_count == 0) || block_count as usize > u16::MAX as usize {
        return Err(CodecError::InvalidBlockCount);
    }
    let code_len = u32::from_le_bytes(payload[10..14].try_into().unwrap());
    let residual_len = u32::from_le_bytes(payload[14..18].try_into().unwrap());
    Ok((
        family,
        profile,
        original_len,
        block_count,
        code_len,
        residual_len,
    ))
}

fn pad_block(bytes: &[u8]) -> [u8; BLOCK_LEN] {
    let mut out = [0_u8; BLOCK_LEN];
    out[..bytes.len()].copy_from_slice(bytes);
    out
}

fn encode_block_codes(
    family: &FamilyAsset,
    profile: &ProfileAsset,
    block: &[u8; BLOCK_LEN],
) -> Result<Vec<u8>, CodecError> {
    let inputs = block.iter().map(|byte| *byte as i16).collect::<Vec<_>>();
    let trunk = apply_layers(&family.analysis_trunk, &inputs)?;
    let latent = apply_layer(&profile.analysis_head, &trunk)?;
    Ok(latent
        .iter()
        .map(|value| nearest_fsq_code(*value, &profile.fsq_levels))
        .collect())
}

fn decode_block_codes(
    family: &FamilyAsset,
    profile: &ProfileAsset,
    codes: &[u8],
) -> Result<Vec<u8>, CodecError> {
    if codes.len() != profile.latent_dim {
        return Err(CodecError::InvalidCodeCount {
            expected: profile.latent_dim,
            actual: codes.len(),
        });
    }
    let latent = codes
        .iter()
        .map(|code| profile.fsq_levels[*code as usize])
        .collect::<Vec<_>>();
    let head = apply_layer(&profile.synthesis_head, &latent)?;
    let reconstructed = apply_layers(&family.synthesis_trunk, &head)?;
    Ok(reconstructed
        .into_iter()
        .map(|value| value.clamp(0, 255) as u8)
        .collect())
}

fn apply_layers(layers: &[LinearLayerAsset], inputs: &[i16]) -> Result<Vec<i16>, CodecError> {
    let mut current = inputs.to_vec();
    for layer in layers {
        current = apply_layer(layer, &current)?;
    }
    Ok(current)
}

fn apply_layer(layer: &LinearLayerAsset, inputs: &[i16]) -> Result<Vec<i16>, CodecError> {
    if inputs.len() != layer.in_dim {
        return Err(CodecError::InvalidLayerSpec);
    }
    let mut out = Vec::with_capacity(layer.out_dim);
    for out_index in 0..layer.out_dim {
        let mut acc = layer.bias[out_index] as i64;
        let row = &layer.weights[out_index * layer.in_dim..(out_index + 1) * layer.in_dim];
        for (input, weight) in inputs.iter().zip(row.iter()) {
            acc += (*input as i64 - layer.input_zero_point as i64) * *weight as i64;
        }
        let mut value = div_round_i64(acc, layer.divisor as i64) + layer.output_zero_point as i64;
        if layer.apply_relu && value < 0 {
            value = 0;
        }
        value = value.clamp(layer.clamp_min as i64, layer.clamp_max as i64);
        out.push(value as i16);
    }
    Ok(out)
}

fn div_round_i64(value: i64, divisor: i64) -> i64 {
    if value >= 0 {
        (value + divisor / 2) / divisor
    } else {
        (value - divisor / 2) / divisor
    }
}

fn nearest_fsq_code(value: i16, levels: &[i16]) -> u8 {
    let mut best_index = 0usize;
    let mut best_distance = i16::MAX as i32;
    for (index, level) in levels.iter().enumerate() {
        let distance = (*level as i32 - value as i32).abs();
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }
    best_index as u8
}

fn encode_residual_payload(residual: &[i8], spec: &ResidualAsset) -> Result<Vec<u8>, CodecError> {
    if residual.iter().all(|value| *value == 0) {
        return Ok(vec![spec.all_zero_tag]);
    }
    let nonzero_count = residual.iter().filter(|value| **value != 0).count();
    let nonzero_percent = (nonzero_count as f32 * 100.0) / residual.len().max(1) as f32;
    if residual
        .iter()
        .all(|value| (*value as i16).abs() <= spec.small_range)
    {
        let symbols = residual
            .iter()
            .map(|value| (*value as i16 + spec.small_range) as usize)
            .collect::<Vec<_>>();
        let encoded = encode_ans_symbols(&symbols, &spec.small_signed_model)?;
        let mut out = Vec::with_capacity(1 + encoded.len());
        out.push(spec.small_signed_rans_tag);
        out.extend_from_slice(&encoded);
        return Ok(out);
    }
    if nonzero_percent <= spec.sparse_threshold_percent {
        return Ok(encode_sparse_residual(residual, spec.sparse_positions_tag));
    }
    Ok(encode_literal_residual(residual, spec.literal_raw_tag))
}

fn encode_literal_residual(residual: &[i8], tag: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + residual.len());
    out.push(tag);
    out.extend(residual.iter().map(|value| *value as u8));
    out
}

fn encode_sparse_residual(residual: &[i8], tag: u8) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(tag);
    let nonzero = residual.iter().filter(|value| **value != 0).count() as u32;
    out.extend_from_slice(&nonzero.to_le_bytes());
    let mut previous_index = 0usize;
    let mut first = true;
    for (index, value) in residual.iter().enumerate() {
        if *value == 0 {
            continue;
        }
        let delta = if first {
            index as u32
        } else {
            (index - previous_index - 1) as u32
        };
        encode_varint(delta, &mut out);
        out.push(*value as u8);
        previous_index = index;
        first = false;
    }
    out
}

fn decode_residual_payload(
    payload: &[u8],
    expected_len: usize,
    spec: &ResidualAsset,
) -> Result<Vec<i8>, CodecError> {
    if payload.is_empty() {
        return Err(CodecError::TruncatedPayload {
            minimum: 1,
            actual: 0,
        });
    }
    match payload[0] {
        tag if tag == spec.all_zero_tag => Ok(vec![0; expected_len]),
        tag if tag == spec.small_signed_rans_tag => {
            let symbols =
                decode_ans_symbols(&payload[1..], expected_len, &spec.small_signed_model)?;
            Ok(symbols
                .into_iter()
                .map(|symbol| symbol as i16 - spec.small_range)
                .map(|value| value as i8)
                .collect())
        }
        tag if tag == spec.sparse_positions_tag => {
            decode_sparse_residual(&payload[1..], expected_len)
        }
        tag if tag == spec.literal_raw_tag => {
            let residual = payload[1..]
                .iter()
                .copied()
                .map(|value| value as i8)
                .collect::<Vec<_>>();
            if residual.len() != expected_len {
                return Err(CodecError::PayloadLenMismatch {
                    expected: expected_len,
                    actual: residual.len(),
                });
            }
            Ok(residual)
        }
        other => Err(CodecError::InvalidResidualTag(other)),
    }
}

fn decode_sparse_residual(payload: &[u8], expected_len: usize) -> Result<Vec<i8>, CodecError> {
    if payload.len() < 4 {
        return Err(CodecError::TruncatedPayload {
            minimum: 4,
            actual: payload.len(),
        });
    }
    let nonzero = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    let mut residual = vec![0_i8; expected_len];
    let mut cursor = 4usize;
    let mut index = 0usize;
    for entry in 0..nonzero {
        let delta = decode_varint(payload, &mut cursor)? as usize;
        index = if entry == 0 { delta } else { index + delta + 1 };
        if index >= expected_len || cursor >= payload.len() {
            return Err(CodecError::PayloadLenMismatch {
                expected: expected_len,
                actual: payload.len(),
            });
        }
        residual[index] = payload[cursor] as i8;
        cursor += 1;
    }
    if cursor != payload.len() {
        return Err(CodecError::PayloadLenMismatch {
            expected: cursor,
            actual: payload.len(),
        });
    }
    Ok(residual)
}

fn encode_ans_symbols(symbols: &[usize], model: &EntropyModel) -> Result<Vec<u8>, CodecError> {
    if symbols.is_empty() {
        return Ok(Vec::new());
    }
    let mut coder = DefaultAnsCoder::new();
    coder
        .encode_iid_symbols_reverse(symbols.iter().copied(), model)
        .map_err(|error| CodecError::EntropyCoding(format!("{error:?}")))?;
    let compressed = coder
        .into_compressed()
        .map_err(|error| CodecError::EntropyCoding(format!("{error:?}")))?;
    let mut out = Vec::with_capacity(compressed.len() * 4);
    for word in compressed {
        out.extend_from_slice(&word.to_le_bytes());
    }
    Ok(out)
}

fn decode_ans_symbols(
    payload: &[u8],
    symbol_count: usize,
    model: &EntropyModel,
) -> Result<Vec<usize>, CodecError> {
    if symbol_count == 0 {
        if payload.is_empty() {
            return Ok(Vec::new());
        }
        return Err(CodecError::PayloadLenMismatch {
            expected: 0,
            actual: payload.len(),
        });
    }
    if payload.len() % 4 != 0 {
        return Err(CodecError::PayloadLenMismatch {
            expected: payload.len().div_ceil(4) * 4,
            actual: payload.len(),
        });
    }
    let words = payload
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    let mut coder = DefaultAnsCoder::from_compressed(words)
        .map_err(|_| CodecError::EntropyCoding("invalid ans payload".to_string()))?;
    coder
        .decode_iid_symbols(symbol_count, model)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| CodecError::EntropyCoding(format!("{error:?}")))
}

fn centered_signed_residuals(actual: &[u8], predicted: &[u8]) -> Vec<i8> {
    actual
        .iter()
        .enumerate()
        .map(|(index, actual_byte)| {
            actual_byte.wrapping_sub(predicted.get(index).copied().unwrap_or(0)) as i8
        })
        .collect()
}

fn apply_centered_signed_residuals(predicted: &[u8], residual: &[i8]) -> Vec<u8> {
    predicted
        .iter()
        .enumerate()
        .map(|(index, predicted_byte)| {
            predicted_byte.wrapping_add(residual.get(index).copied().unwrap_or(0) as u8)
        })
        .collect()
}

fn wrapping_delta_bytes(current: &[u8], predicted: &[u8]) -> Vec<u8> {
    current
        .iter()
        .enumerate()
        .map(|(index, byte)| byte.wrapping_sub(predicted.get(index).copied().unwrap_or(0)))
        .collect()
}

fn apply_wrapping_delta(delta: &[u8], predicted: &[u8]) -> Vec<u8> {
    delta
        .iter()
        .enumerate()
        .map(|(index, byte)| byte.wrapping_add(predicted.get(index).copied().unwrap_or(0)))
        .collect()
}

fn encode_varint(mut value: u32, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> Result<u32, CodecError> {
    let mut shift = 0u32;
    let mut value = 0u32;
    loop {
        if *cursor >= bytes.len() {
            return Err(CodecError::TruncatedPayload {
                minimum: *cursor + 1,
                actual: bytes.len(),
            });
        }
        let byte = bytes[*cursor];
        *cursor += 1;
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 32 {
            return Err(CodecError::PayloadLenMismatch {
                expected: 5,
                actual: *cursor,
            });
        }
    }
}

fn candidate_profiles(len: usize) -> &'static [PackId] {
    if len <= 128 {
        &[]
    } else if len <= 1024 {
        &[PACKED_PACK16_ID, PACKED_PACK32_ID]
    } else if len <= 8 * 1024 {
        &[PACKED_PACK32_ID, PACKED_PACK64_ID]
    } else {
        &[PACKED_PACK64_ID, PACKED_PACK128_ID]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(family: SourceKind, bytes: &[u8]) -> ExactStateMaterial {
        ExactStateMaterial::copy_exact(family, bytes)
    }

    #[test]
    fn copy_exact_round_trips() {
        let current = block(SourceKind::Json, b"{\"ok\":true}");
        let payload = encode_exact_payload(&current);
        let decoded = decode_data_payload(&payload, CodecMode::DirectExact, None).unwrap();
        assert_eq!(decoded.exact_bytes, current.exact_bytes);
        assert_eq!(decoded.source_kind, SourceKind::Json);
    }

    #[test]
    fn latent_raw_round_trips() {
        let current = block(
            SourceKind::Text,
            b"hello hello hello hello hello hello hello hello hello hello",
        );
        let encoded = encode_best_exact_payload(&current, PredictorState::Empty, None).unwrap();
        let decoded = decode_data_payload(&encoded.payload, encoded.mode, None).unwrap();
        assert_eq!(decoded.exact_bytes, current.exact_bytes);
        assert_eq!(decoded.source_kind, current.source_kind);
    }

    #[test]
    fn latent_delta_round_trips() {
        let previous = block(SourceKind::Json, b"{\"count\":41,\"ok\":true}");
        let current = block(SourceKind::Json, b"{\"count\":42,\"ok\":true}");
        let encoded = encode_best_exact_payload(
            &current,
            PredictorState::Ready(PredictorEntryMeta {
                item_id: ItemId(1),
                source_kind: crate::SourceKind::Binary,
                object_kind: crate::ObjectKind::ExactState,
                cue: Default::default(),
            }),
            Some(&previous),
        )
        .unwrap();
        let decoded = decode_data_payload(&encoded.payload, encoded.mode, Some(&previous)).unwrap();
        assert_eq!(decoded.exact_bytes, current.exact_bytes);
        assert_eq!(decoded.source_kind, current.source_kind);
    }

    #[test]
    fn tiny_payloads_stay_copy_exact() {
        let current = block(SourceKind::Json, b"{\"x\":1}");
        let encoded = encode_best_exact_payload(&current, PredictorState::Empty, None).unwrap();
        assert_eq!(encoded.mode, CodecMode::DirectExact);
    }

    #[test]
    fn direct_exact_only_preference_disables_latent_modes() {
        let previous = block(SourceKind::Json, b"{\"count\":41,\"ok\":true}");
        let current = block(SourceKind::Json, b"{\"count\":42,\"ok\":true}");
        let encoded = encode_best_exact_payload_with_preference(
            &current,
            PredictorState::Ready(PredictorEntryMeta {
                item_id: ItemId(1),
                source_kind: crate::SourceKind::Binary,
                object_kind: crate::ObjectKind::ExactState,
                cue: Default::default(),
            }),
            Some(&previous),
            DataPlaneCodecPreference::DirectExactOnly,
        )
        .unwrap();
        assert_eq!(encoded.mode, CodecMode::DirectExact);
    }

    #[test]
    fn repeated_text_can_choose_latent() {
        let current = block(
            SourceKind::Text,
            b"The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.",
        );
        let encoded = encode_best_exact_payload(&current, PredictorState::Empty, None).unwrap();
        assert!(matches!(
            encoded.mode,
            CodecMode::PackedExact | CodecMode::DirectExact
        ));
        let decoded = decode_data_payload(&encoded.payload, encoded.mode, None).unwrap();
        assert_eq!(decoded.exact_bytes, current.exact_bytes);
    }
}
