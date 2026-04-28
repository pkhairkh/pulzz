use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{SourceKind, gf2_fingerprint};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPlane {
    #[default]
    AtomSubstrate = 1,
    Assembly = 2,
    Transform = 3,
    Episode = 4,
    Schema = 5,
    Controller = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum RouteFamily {
    #[default]
    DirectState = 1,
    PredictiveConfirm = 2,
    PredictiveCorrect = 3,
    Assembly = 4,
    /// DEMOTED from active architecture. Transform routes are not emitted by the
    /// live server. Retained for wire compatibility and potential future reactivation.
    Transform = 5,
    Schema = 6,
    Episode = 7,
    Replay = 8,
    ExactAtom = 9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ControllerRouteFamily {
    #[default]
    DirectState = 1,
    ExactAtom = 2,
    Assembly = 3,
    /// DEMOTED from active architecture. Transform routes are not emitted by the
    /// live server. Retained for wire compatibility and potential future reactivation.
    Transform = 4,
    EpisodeCompletion = 5,
    SchemaExpansion = 6,
    Hybrid = 7,
}

impl ControllerRouteFamily {
    pub const fn route_family(self) -> RouteFamily {
        match self {
            Self::DirectState => RouteFamily::DirectState,
            Self::ExactAtom => RouteFamily::ExactAtom,
            Self::Assembly => RouteFamily::Assembly,
            Self::Transform => RouteFamily::Transform,
            Self::EpisodeCompletion => RouteFamily::PredictiveConfirm,
            Self::SchemaExpansion => RouteFamily::Schema,
            Self::Hybrid => RouteFamily::PredictiveCorrect,
        }
    }

    pub const fn dispatch_record_type(self) -> crate::RecordType {
        match self {
            Self::DirectState => crate::RecordType::ExactState,
            Self::ExactAtom | Self::Assembly | Self::EpisodeCompletion => {
                crate::RecordType::PredictiveConfirm
            }
            Self::SchemaExpansion | Self::Hybrid => crate::RecordType::PredictiveCorrect,
            Self::Transform => crate::RecordType::TransformCorrect,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyRouteMode {
    #[default]
    DefineAndActivate = 1,
    ReuseReference = 2,
    StructuralHybrid = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    AtomFragment = 1,
    ExactBlock = 2,
    ExactBundle = 3,
    ExactRange = 4,
    ResidualBuffer = 5,
    ExactState = 6,
    SourceDescriptor = 7,
    SparseCue = 8,
    Assembly = 9,
    Transform = 10,
    Schema = 11,
    EpisodeHint = 12,
    ReplayHint = 13,
    Dictionary = 14,
    #[default]
    PredictiveObject = 15,
}

impl ObjectKind {
    pub const fn storage_slug(self) -> &'static str {
        match self {
            Self::AtomFragment => "atom_fragment",
            Self::ExactBlock => "exact_block",
            Self::ExactBundle => "exact_bundle",
            Self::ExactRange => "exact_range",
            Self::ResidualBuffer => "residual_buffer",
            Self::ExactState => "exact_state",
            Self::SourceDescriptor => "source_descriptor",
            Self::SparseCue => "sparse_cue",
            Self::Assembly => "assembly",
            Self::Transform => "transform",
            Self::Schema => "schema",
            Self::EpisodeHint => "episode_hint",
            Self::ReplayHint => "replay_hint",
            Self::Dictionary => "dictionary",
            Self::PredictiveObject => "predictive_object",
        }
    }

    pub const fn is_substrate(self) -> bool {
        matches!(
            self,
            Self::AtomFragment
                | Self::ExactBlock
                | Self::ExactBundle
                | Self::ExactRange
                | Self::ResidualBuffer
        )
    }

    pub const fn substrate_capability(self) -> Option<SubstrateCapability> {
        match self {
            Self::AtomFragment => Some(SubstrateCapability::AtomFragment),
            Self::ExactBlock => Some(SubstrateCapability::ExactBlock),
            Self::ExactBundle => Some(SubstrateCapability::ExactBundle),
            Self::ExactRange => Some(SubstrateCapability::ExactRange),
            Self::ResidualBuffer => Some(SubstrateCapability::ResidualBuffer),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SubstrateCapability {
    AtomFragment = 1,
    ExactBlock = 2,
    ExactBundle = 3,
    ExactRange = 4,
    ResidualBuffer = 5,
    #[default]
    CompositeGraph = 6,
}

impl SubstrateCapability {
    pub const fn bit(self) -> u16 {
        1_u16 << ((self as u8).saturating_sub(1) as u16)
    }

    pub fn supports_kind(self, kind: ObjectKind) -> bool {
        match self {
            Self::CompositeGraph => kind.is_substrate(),
            _ => {
                matches!(kind.substrate_capability(), Some(capability) if capability as u8 == self as u8)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct SubstrateCapabilityMask(pub u16);

impl SubstrateCapabilityMask {
    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn insert(&mut self, capability: SubstrateCapability) {
        self.0 |= capability.bit();
    }

    pub const fn contains(self, capability: SubstrateCapability) -> bool {
        self.0 & capability.bit() != 0
    }

    pub fn supports(self, kind: ObjectKind) -> bool {
        match kind.substrate_capability() {
            Some(capability) => {
                self.contains(capability) || self.contains(SubstrateCapability::CompositeGraph)
            }
            None => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum PrecisionBand {
    Exact = 1,
    Tight = 2,
    #[default]
    Balanced = 3,
    Broad = 4,
    Background = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum PromotionLevel {
    #[default]
    Cold = 1,
    Warm = 2,
    Stable = 3,
    Promoted = 4,
    Canonical = 5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Transform is DEMOTED from the active architecture. The planner does not select
/// transform routes for emission. Retained for wire compatibility and potential future
/// reactivation.
pub struct ChpmtCapabilityDescriptor {
    pub capability_slug: &'static str,
    pub supported_planes: Vec<MemoryPlane>,
    pub supported_object_kinds: Vec<ObjectKind>,
    pub supported_source_kind_bits: u64,
}

impl ChpmtCapabilityDescriptor {
    pub fn supports_source_kind(&self, source_kind: crate::SourceKind) -> bool {
        self.supported_source_kind_bits & source_kind.family_bits() != 0
    }
}

pub fn architecture_text_family_capability_descriptor() -> ChpmtCapabilityDescriptor {
    ChpmtCapabilityDescriptor {
        capability_slug: "text_family_cue_object",
        supported_planes: vec![
            MemoryPlane::AtomSubstrate,
            MemoryPlane::Assembly,
            MemoryPlane::Transform,
            MemoryPlane::Episode,
            MemoryPlane::Schema,
            MemoryPlane::Controller,
        ],
        supported_object_kinds: vec![
            ObjectKind::ExactState,
            ObjectKind::SparseCue,
            ObjectKind::Assembly,
            ObjectKind::Transform,
            ObjectKind::Schema,
            ObjectKind::Dictionary,
            ObjectKind::PredictiveObject,
        ],
        supported_source_kind_bits: crate::SourceKind::Text.family_bits()
            | crate::SourceKind::Json.family_bits()
            | crate::SourceKind::Binary.family_bits(),
    }
}

pub fn architecture_image_family_capability_descriptor() -> ChpmtCapabilityDescriptor {
    ChpmtCapabilityDescriptor {
        capability_slug: "image_family_cue_object",
        supported_planes: vec![
            MemoryPlane::AtomSubstrate,
            MemoryPlane::Assembly,
            MemoryPlane::Transform,
            MemoryPlane::Episode,
            MemoryPlane::Schema,
            MemoryPlane::Controller,
        ],
        supported_object_kinds: vec![
            ObjectKind::ExactState,
            ObjectKind::SparseCue,
            ObjectKind::Assembly,
            ObjectKind::Transform,
            ObjectKind::Schema,
            ObjectKind::PredictiveObject,
        ],
        supported_source_kind_bits: crate::SourceKind::Image.family_bits(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct SparseCue {
    pub family_bits: u64,
    pub role_bits: u64,
    pub delimiter_bits: u64,
    pub length_bucket_bits: u64,
    pub temporal_bits: u64,
}

impl SparseCue {
    pub const fn new(
        family_bits: u64,
        role_bits: u64,
        delimiter_bits: u64,
        length_bucket_bits: u64,
        temporal_bits: u64,
    ) -> Self {
        Self {
            family_bits,
            role_bits,
            delimiter_bits,
            length_bucket_bits,
            temporal_bits,
        }
    }

    pub const fn is_empty(self) -> bool {
        self.family_bits == 0
            && self.role_bits == 0
            && self.delimiter_bits == 0
            && self.length_bucket_bits == 0
            && self.temporal_bits == 0
    }

    pub const fn overlap_score(self, other: Self) -> u32 {
        (self.family_bits & other.family_bits).count_ones()
            + (self.role_bits & other.role_bits).count_ones()
            + (self.delimiter_bits & other.delimiter_bits).count_ones()
            + (self.length_bucket_bits & other.length_bucket_bits).count_ones()
            + (self.temporal_bits & other.temporal_bits).count_ones()
    }

    pub const fn length_bucket_tag(self) -> u8 {
        let bits = self.length_bucket_bits;
        if bits == 0 {
            0
        } else {
            bits.trailing_zeros() as u8
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct PrecisionBandValue(pub u16);

impl PrecisionBand {
    pub const fn score_value(self) -> PrecisionBandValue {
        PrecisionBandValue(match self {
            Self::Exact => 1000,
            Self::Tight => 850,
            Self::Balanced => 650,
            Self::Broad => 400,
            Self::Background => 150,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteScoreInputs {
    pub wire_cost: u32,
    pub decode_cost: u32,
    pub residual_cost: u32,
    pub sync_risk: u32,
    pub ambiguity: u32,
    pub novelty: u32,
    pub support_gain: u32,
    pub predictive_match_gain: u32,
    pub temporal_continuation_gain: u32,
    pub schema_reuse_gain: u32,
    pub adaptive_prior_gain: u32,
    pub adaptive_failure_penalty: u32,
    pub adaptive_promotion_gain: u32,
    pub adaptive_suppression_penalty: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteScoreBreakdown {
    pub base_gains: u32,
    pub base_penalties: u32,
    pub adaptive_prior_gain: u32,
    pub adaptive_failure_penalty: u32,
    pub adaptive_promotion_gain: u32,
    pub adaptive_suppression_penalty: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteScore {
    pub total: i32,
    pub precision_band: PrecisionBand,
    pub breakdown: RouteScoreBreakdown,
}

pub fn score_route(inputs: RouteScoreInputs, precision_band: PrecisionBand) -> RouteScore {
    let base_gains = inputs
        .support_gain
        .saturating_add(inputs.predictive_match_gain)
        .saturating_add(inputs.temporal_continuation_gain)
        .saturating_add(inputs.schema_reuse_gain)
        .saturating_add(precision_band.score_value().0 as u32);
    let base_penalties = inputs
        .wire_cost
        .saturating_add(inputs.decode_cost)
        .saturating_add(inputs.residual_cost)
        .saturating_add(inputs.sync_risk)
        .saturating_add(inputs.ambiguity)
        .saturating_add(inputs.novelty);
    let adaptive_gains = inputs
        .adaptive_prior_gain
        .saturating_add(inputs.adaptive_promotion_gain);
    let adaptive_penalties = inputs
        .adaptive_failure_penalty
        .saturating_add(inputs.adaptive_suppression_penalty);
    RouteScore {
        total: base_gains as i32 + adaptive_gains as i32
            - base_penalties as i32
            - adaptive_penalties as i32,
        precision_band,
        breakdown: RouteScoreBreakdown {
            base_gains,
            base_penalties,
            adaptive_prior_gain: inputs.adaptive_prior_gain,
            adaptive_failure_penalty: inputs.adaptive_failure_penalty,
            adaptive_promotion_gain: inputs.adaptive_promotion_gain,
            adaptive_suppression_penalty: inputs.adaptive_suppression_penalty,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteFeedbackSummary {
    pub route_family: ControllerRouteFamily,
    pub total_successes: u32,
    pub total_failures: u32,
    pub source_successes: u32,
    pub source_failures: u32,
    pub context_successes: u32,
    pub context_failures: u32,
    pub recent_successes: u32,
    pub recent_failures: u32,
}

impl RouteFeedbackSummary {
    pub const RECENT_TICK_WINDOW: u64 = 256;

    pub fn planner_prior_gain(&self) -> u32 {
        self.total_successes
            .saturating_div(2)
            .saturating_add(self.source_successes)
            .saturating_add(self.context_successes.saturating_mul(2))
            .min(96)
    }

    pub fn planner_failure_penalty(&self) -> u32 {
        self.total_failures
            .saturating_div(2)
            .saturating_add(self.source_failures)
            .saturating_add(self.context_failures.saturating_mul(2))
            .min(128)
    }

    pub fn success_promotion_gain(&self) -> u32 {
        if self.recent_successes >= 3 && self.recent_successes > self.recent_failures {
            self.recent_successes.saturating_mul(6).min(48)
        } else {
            0
        }
    }

    pub fn suppression_penalty(&self) -> u32 {
        if self.recent_failures >= 3 && self.recent_failures > self.recent_successes {
            self.recent_failures.saturating_mul(12).min(160)
        } else {
            0
        }
    }

    pub fn suppressed(&self) -> bool {
        self.suppression_penalty() >= 72
    }
}

pub fn summarize_route_feedback<'a>(
    route_family: ControllerRouteFamily,
    source_kind: SourceKind,
    context_hash: Option<ContextHash>,
    tick: u64,
    statistics: impl IntoIterator<Item = &'a RouteStatistics>,
) -> RouteFeedbackSummary {
    let mut summary = RouteFeedbackSummary {
        route_family,
        ..RouteFeedbackSummary::default()
    };
    for stat in statistics {
        if stat.route_family != route_family {
            continue;
        }
        summary.total_successes = summary.total_successes.saturating_add(stat.success_count);
        summary.total_failures = summary.total_failures.saturating_add(stat.failure_count);
        if stat.source_kind == Some(source_kind) {
            summary.source_successes = summary.source_successes.saturating_add(stat.success_count);
            summary.source_failures = summary.source_failures.saturating_add(stat.failure_count);
        }
        if context_hash.is_some() && stat.context_hash == context_hash {
            summary.context_successes =
                summary.context_successes.saturating_add(stat.success_count);
            summary.context_failures = summary.context_failures.saturating_add(stat.failure_count);
        }
        if tick.saturating_sub(stat.last_seen_tick) <= RouteFeedbackSummary::RECENT_TICK_WINDOW {
            summary.recent_successes = summary.recent_successes.saturating_add(stat.success_count);
            summary.recent_failures = summary.recent_failures.saturating_add(stat.failure_count);
        }
    }
    summary
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteStatistics {
    pub route_family: ControllerRouteFamily,
    pub source_kind: Option<SourceKind>,
    pub context_hash: Option<ContextHash>,
    pub success_count: u32,
    pub failure_count: u32,
    pub last_seen_tick: u64,
}

impl RouteStatistics {
    pub fn record(&mut self, tick: u64, success: bool) {
        self.last_seen_tick = tick;
        if success {
            self.success_count = self.success_count.saturating_add(1);
        } else {
            self.failure_count = self.failure_count.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum DelimiterClass {
    #[default]
    None = 1,
    Sparse = 2,
    Structured = 3,
    Dense = 4,
}

impl DelimiterClass {
    pub fn from_cue(cue: SparseCue) -> Self {
        let active = cue.delimiter_bits.count_ones();
        match active {
            0 => Self::None,
            1..=2 => Self::Sparse,
            3..=5 => Self::Structured,
            _ => Self::Dense,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SlotShapeBucket {
    #[default]
    None = 1,
    Narrow = 2,
    Medium = 3,
    Wide = 4,
}

impl SlotShapeBucket {
    pub const fn from_slot_count(slot_count: usize) -> Self {
        match slot_count {
            0 => Self::None,
            1..=2 => Self::Narrow,
            3..=4 => Self::Medium,
            _ => Self::Wide,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ResidualSizeBucket {
    #[default]
    None = 1,
    Tiny = 2,
    Small = 3,
    Medium = 4,
    Large = 5,
}

impl ResidualSizeBucket {
    pub const fn from_bytes(bytes: u32) -> Self {
        match bytes {
            0 => Self::None,
            1..=16 => Self::Tiny,
            17..=64 => Self::Small,
            65..=256 => Self::Medium,
            _ => Self::Large,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SyncStateBucket {
    #[default]
    Stable = 1,
    Warm = 2,
    Risky = 3,
    Repairing = 4,
}

impl SyncStateBucket {
    pub const fn from_sync_risk(sync_risk: u32) -> Self {
        match sync_risk {
            0..=8 => Self::Stable,
            9..=24 => Self::Warm,
            25..=63 => Self::Risky,
            _ => Self::Repairing,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct ContextTreeSymbol {
    pub route_family: ControllerRouteFamily,
    pub delimiter_class: DelimiterClass,
    pub slot_shape: SlotShapeBucket,
    pub prior_schema_kind: Option<SchemaKind>,
    pub prior_transform_family: Option<TransformTransducerFamily>,
    pub lag_bucket: LagBucket,
    pub residual_bucket: ResidualSizeBucket,
    pub sync_state: SyncStateBucket,
}

impl ContextTreeSymbol {
    fn atoms(self) -> [u16; 8] {
        [
            self.route_family as u16,
            self.delimiter_class as u16,
            self.slot_shape as u16,
            self.prior_schema_kind
                .map(|value| value as u16)
                .unwrap_or(0),
            self.prior_transform_family
                .map(|value| value as u16)
                .unwrap_or(0),
            self.lag_bucket.0 as u16,
            self.residual_bucket as u16,
            self.sync_state as u16,
        ]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextTreeOutcome {
    pub success: bool,
    pub fallback: bool,
    pub sync_failure: bool,
    pub contradiction_mass: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContextTreeNodeStats {
    pub observations: u32,
    pub successes: u32,
    pub failures: u32,
    pub fallbacks: u32,
    pub sync_failures: u32,
    pub contradiction_mass: u32,
}

impl ContextTreeNodeStats {
    pub fn record(&mut self, outcome: ContextTreeOutcome) {
        self.observations = self.observations.saturating_add(1);
        if outcome.success {
            self.successes = self.successes.saturating_add(1);
        } else {
            self.failures = self.failures.saturating_add(1);
        }
        if outcome.fallback {
            self.fallbacks = self.fallbacks.saturating_add(1);
        }
        if outcome.sync_failure {
            self.sync_failures = self.sync_failures.saturating_add(1);
        }
        self.contradiction_mass = self
            .contradiction_mass
            .saturating_add(outcome.contradiction_mass);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteGovernorSignal {
    pub route_family: ControllerRouteFamily,
    pub prior_gain: u32,
    pub failure_penalty: u32,
    pub suppression_penalty: u32,
    pub residual_penalty: u32,
    pub sync_penalty: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextTreeGovernor {
    pub max_depth: u8,
    pub stats: BTreeMap<Vec<u16>, ContextTreeNodeStats>,
}

impl Default for ContextTreeGovernor {
    fn default() -> Self {
        Self {
            max_depth: 4,
            stats: BTreeMap::new(),
        }
    }
}

impl ContextTreeGovernor {
    pub fn observe(&mut self, symbol: ContextTreeSymbol, outcome: ContextTreeOutcome) {
        let atoms = symbol.atoms();
        let max_depth = usize::from(self.max_depth.max(1)).min(atoms.len());
        for depth in 1..=max_depth {
            let key = atoms[..depth].to_vec();
            self.stats.entry(key).or_default().record(outcome);
        }
    }

    pub fn signal_for(&self, symbol: ContextTreeSymbol) -> RouteGovernorSignal {
        let atoms = symbol.atoms();
        let max_depth = usize::from(self.max_depth.max(1)).min(atoms.len());
        let mut weighted_success = 0_u32;
        let mut weighted_failure = 0_u32;
        let mut weighted_fallback = 0_u32;
        let mut weighted_sync_failure = 0_u32;
        let mut weighted_contradiction = 0_u32;
        let mut weight_total = 0_u32;
        for depth in 1..=max_depth {
            if let Some(stats) = self.stats.get(&atoms[..depth].to_vec()) {
                let weight = depth as u32;
                weighted_success =
                    weighted_success.saturating_add(stats.successes.saturating_mul(weight));
                weighted_failure =
                    weighted_failure.saturating_add(stats.failures.saturating_mul(weight));
                weighted_fallback =
                    weighted_fallback.saturating_add(stats.fallbacks.saturating_mul(weight));
                weighted_sync_failure = weighted_sync_failure
                    .saturating_add(stats.sync_failures.saturating_mul(weight));
                weighted_contradiction = weighted_contradiction
                    .saturating_add(stats.contradiction_mass.saturating_mul(weight));
                weight_total = weight_total.saturating_add(weight);
            }
        }
        if weight_total == 0 {
            return RouteGovernorSignal {
                route_family: symbol.route_family,
                ..RouteGovernorSignal::default()
            };
        }
        let success_score = weighted_success / weight_total;
        let failure_score = weighted_failure / weight_total;
        let fallback_score = weighted_fallback / weight_total;
        let sync_failure_score = weighted_sync_failure / weight_total;
        let contradiction_score = weighted_contradiction / weight_total;
        let prior_gain = success_score
            .saturating_mul(10)
            .saturating_sub(failure_score.saturating_mul(2))
            .min(96);
        let failure_penalty = failure_score
            .saturating_mul(8)
            .saturating_add(fallback_score.saturating_mul(6))
            .min(128);
        let suppression_penalty = if failure_score
            .saturating_add(fallback_score)
            .saturating_add(sync_failure_score)
            > success_score
        {
            failure_score
                .saturating_add(fallback_score)
                .saturating_add(sync_failure_score)
                .saturating_mul(6)
                .min(160)
        } else {
            0
        };
        RouteGovernorSignal {
            route_family: symbol.route_family,
            prior_gain,
            failure_penalty,
            suppression_penalty,
            residual_penalty: contradiction_score.saturating_div(8).min(48),
            sync_penalty: sync_failure_score.saturating_mul(8).min(48),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DependencyClosureGate {
    pub dependencies_present: bool,
    pub version_compatible: bool,
    pub sync_risk: u32,
}

impl DependencyClosureGate {
    pub const fn admissible(&self, max_sync_risk: u32) -> bool {
        self.dependencies_present && self.version_compatible && self.sync_risk <= max_sync_risk
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct CueFamily {
    pub plane: MemoryPlane,
    pub route_family: RouteFamily,
    pub object_kind: ObjectKind,
    pub precision_band: PrecisionBand,
}

impl CueFamily {
    pub const fn exact_state() -> Self {
        Self {
            plane: MemoryPlane::AtomSubstrate,
            route_family: RouteFamily::DirectState,
            object_kind: ObjectKind::ExactState,
            precision_band: PrecisionBand::Exact,
        }
    }

    pub const fn source_descriptor() -> Self {
        Self {
            plane: MemoryPlane::AtomSubstrate,
            route_family: RouteFamily::DirectState,
            object_kind: ObjectKind::SourceDescriptor,
            precision_band: PrecisionBand::Exact,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct StructuralCueSummary {
    pub cue: SparseCue,
    pub source_kind: SourceKind,
    pub plane: MemoryPlane,
    pub route_family: RouteFamily,
    pub object_kind: ObjectKind,
    pub precision_band: PrecisionBand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct ObjectVersion {
    pub schema_version: u16,
    pub object_revision: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct ObjectDependency {
    pub object_kind: ObjectKind,
    pub object_id: String,
    pub required_revision: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct ObjectLifecycleMeta {
    pub support_count: u32,
    pub success_count: u32,
    pub failure_count: u32,
    pub salience: u32,
    pub creation_tick: u64,
    pub last_seen_tick: u64,
    pub promotion_level: PromotionLevel,
    pub consolidation_count: u32,
    pub last_consolidated_tick: u64,
    pub ontology_family_id: Option<DynamicFamilyId>,
    pub ontology_subfamily_id: Option<DynamicSubfamilyId>,
}

impl ObjectLifecycleMeta {
    pub const fn record_seen(mut self, tick: u64, success: bool) -> Self {
        self.support_count = self.support_count.saturating_add(1);
        self.last_seen_tick = tick;
        if success {
            self.success_count = self.success_count.saturating_add(1);
        } else {
            self.failure_count = self.failure_count.saturating_add(1);
        }
        self
    }

    pub const fn with_promotion_level(mut self, level: PromotionLevel) -> Self {
        self.promotion_level = level;
        self
    }

    pub const fn record_consolidated(mut self, tick: u64) -> Self {
        self.consolidation_count = self.consolidation_count.saturating_add(1);
        self.last_consolidated_tick = tick;
        self
    }

    pub const fn assign_dynamic_family(
        mut self,
        family_id: DynamicFamilyId,
        subfamily_id: Option<DynamicSubfamilyId>,
    ) -> Self {
        self.ontology_family_id = Some(family_id);
        self.ontology_subfamily_id = subfamily_id;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SparseIndexKey {
    pub source_kind: SourceKind,
    pub family: CueFamily,
    pub cue: SparseCue,
}

impl SparseIndexKey {
    pub fn partition_key(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}:{}",
            self.source_kind.slug(),
            self.family.plane as u8,
            self.family.route_family as u8,
            self.family.object_kind as u8,
            self.family.precision_band as u8,
            self.cue.family_bits,
            self.cue.length_bucket_bits,
            self.cue.delimiter_bits,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SparseIndexEntry {
    pub object_id: String,
    pub source_kind: SourceKind,
    pub object_kind: ObjectKind,
    pub cue: SparseCue,
    pub lifecycle: ObjectLifecycleMeta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SparseIndexTable {
    pub partitions: BTreeMap<String, Vec<SparseIndexEntry>>,
}

impl SparseIndexTable {
    pub fn insert(&mut self, key: SparseIndexKey, entry: SparseIndexEntry) {
        let partition = self.partitions.entry(key.partition_key()).or_default();
        if let Some(existing) = partition
            .iter_mut()
            .find(|candidate| candidate.object_id == entry.object_id)
        {
            *existing = entry;
        } else {
            partition.push(entry);
        }
    }

    pub fn query(&self, query: &CompletionQuery) -> Vec<CompletionCandidate> {
        let mut out = Vec::new();
        for entries in self.partitions.values() {
            for entry in entries {
                if let Some(filter) = query.family_filter {
                    if entry.source_kind != filter {
                        continue;
                    }
                }
                if !query.admissible_object_kinds.is_empty()
                    && !query.admissible_object_kinds.contains(&entry.object_kind)
                {
                    continue;
                }
                let overlap = entry.cue.overlap_score(query.cue);
                if overlap == 0 {
                    continue;
                }
                out.push(CompletionCandidate {
                    object_id: entry.object_id.clone(),
                    source_kind: entry.source_kind,
                    object_kind: entry.object_kind,
                    cue_overlap: overlap,
                    lifecycle: entry.lifecycle,
                    cue: entry.cue,
                });
            }
        }
        out.sort_by(|left, right| {
            right
                .cue_overlap
                .cmp(&left.cue_overlap)
                .then_with(|| right.lifecycle.salience.cmp(&left.lifecycle.salience))
                .then_with(|| {
                    right
                        .lifecycle
                        .support_count
                        .cmp(&left.lifecycle.support_count)
                })
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        out.truncate(query.max_candidates as usize);
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionQuery {
    pub cue: SparseCue,
    pub family_filter: Option<SourceKind>,
    pub max_candidates: u16,
    pub admissible_object_kinds: Vec<ObjectKind>,
}

impl CompletionQuery {
    pub fn bounded(
        cue: SparseCue,
        family_filter: Option<SourceKind>,
        max_candidates: u16,
        admissible_object_kinds: Vec<ObjectKind>,
    ) -> Self {
        Self {
            cue,
            family_filter,
            max_candidates: max_candidates.max(1),
            admissible_object_kinds,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionCandidate {
    pub object_id: String,
    pub source_kind: SourceKind,
    pub object_kind: ObjectKind,
    pub cue_overlap: u32,
    pub lifecycle: ObjectLifecycleMeta,
    pub cue: SparseCue,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct AssemblyId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyKind {
    #[default]
    ContiguousMotif = 1,
    DiscontinuousFieldGroup = 2,
    DelimiterBounded = 3,
    SlotBearing = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct RoleSignature {
    pub role_bits: u64,
    pub delimiter_role_bits: u64,
    pub slot_role_bits: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct SlotDescriptor {
    pub slot_index: u16,
    pub role_bits: u64,
    pub min_len: u32,
    pub max_len: u32,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssemblyComponentRef {
    AtomFragment {
        object_id: String,
        start: u32,
        len: u32,
    },
    ExactBlock {
        object_id: String,
        start: u32,
        len: u32,
    },
    ExactBundle {
        object_id: String,
        start: u32,
        len: u32,
    },
    ExactRange {
        object_id: String,
        start: u32,
        len: u32,
    },
    ResidualBuffer {
        object_id: String,
        start: u32,
        len: u32,
    },
}

impl AssemblyComponentRef {
    pub const fn object_kind(&self) -> ObjectKind {
        match self {
            Self::AtomFragment { .. } => ObjectKind::AtomFragment,
            Self::ExactBlock { .. } => ObjectKind::ExactBlock,
            Self::ExactBundle { .. } => ObjectKind::ExactBundle,
            Self::ExactRange { .. } => ObjectKind::ExactRange,
            Self::ResidualBuffer { .. } => ObjectKind::ResidualBuffer,
        }
    }

    pub const fn byte_len(&self) -> u32 {
        match self {
            Self::AtomFragment { len, .. }
            | Self::ExactBlock { len, .. }
            | Self::ExactBundle { len, .. }
            | Self::ExactRange { len, .. }
            | Self::ResidualBuffer { len, .. } => *len,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DependencyClosure {
    pub version: ObjectVersion,
    pub dependencies: Vec<ObjectDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "node_kind", rename_all = "snake_case")]
pub enum AssemblyBodyNode {
    DelimiterAnchor {
        bytes: Vec<u8>,
    },
    LiteralIsland {
        bytes: Vec<u8>,
    },
    SubstrateSpan {
        reference: AssemblyComponentRef,
    },
    SlotPlaceholder {
        slot_index: u16,
        role_bits: u64,
        bytes: Vec<u8>,
    },
    MotifLink {
        motif_hash: u64,
        bytes: Vec<u8>,
    },
    TypedBoundary {
        label: String,
        bytes: Vec<u8>,
    },
}

impl AssemblyBodyNode {
    pub fn output_len(&self) -> u32 {
        match self {
            Self::DelimiterAnchor { bytes }
            | Self::LiteralIsland { bytes }
            | Self::SlotPlaceholder { bytes, .. }
            | Self::MotifLink { bytes, .. }
            | Self::TypedBoundary { bytes, .. } => bytes.len().min(u32::MAX as usize) as u32,
            Self::SubstrateSpan { reference } => reference.byte_len(),
        }
    }

    pub fn inline_bytes_len(&self) -> u32 {
        match self {
            Self::SubstrateSpan { .. } => 0,
            _ => self.output_len(),
        }
    }

    pub fn is_structural(&self) -> bool {
        !matches!(self, Self::LiteralIsland { .. })
    }

    pub fn support_weight(&self) -> u32 {
        match self {
            Self::LiteralIsland { .. } => 0,
            Self::DelimiterAnchor { bytes } | Self::TypedBoundary { bytes, .. } => {
                (bytes.len().min(u32::MAX as usize) as u32).max(1)
            }
            Self::SlotPlaceholder { bytes, .. } | Self::MotifLink { bytes, .. } => {
                (bytes.len().min(u32::MAX as usize) as u32).max(4)
            }
            Self::SubstrateSpan { reference } => reference.byte_len().max(4),
        }
    }

    pub fn as_component_ref(&self) -> Option<&AssemblyComponentRef> {
        match self {
            Self::SubstrateSpan { reference } => Some(reference),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyBody {
    pub nodes: Vec<AssemblyBodyNode>,
}

impl AssemblyBody {
    pub fn from_literal(bytes: Vec<u8>) -> Self {
        Self {
            nodes: if bytes.is_empty() {
                Vec::new()
            } else {
                vec![AssemblyBodyNode::LiteralIsland { bytes }]
            },
        }
    }

    pub fn output_len(&self) -> u32 {
        self.nodes.iter().map(AssemblyBodyNode::output_len).sum()
    }

    pub fn literal_len(&self) -> u32 {
        self.nodes
            .iter()
            .filter_map(|node| match node {
                AssemblyBodyNode::LiteralIsland { bytes } => {
                    Some(bytes.len().min(u32::MAX as usize) as u32)
                }
                _ => None,
            })
            .sum()
    }

    pub fn inline_len(&self) -> u32 {
        self.nodes
            .iter()
            .map(AssemblyBodyNode::inline_bytes_len)
            .sum()
    }

    pub fn structural_component_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|node| node.is_structural())
            .count()
    }

    pub fn dependency_component_count(&self) -> usize {
        self.nodes
            .iter()
            .filter(|node| matches!(node, AssemblyBodyNode::SubstrateSpan { .. }))
            .count()
    }

    pub fn body_shape_hash(&self) -> u64 {
        let mut hash = 0x9e3779b97f4a7c15_u64;
        for node in &self.nodes {
            let discriminant = match node {
                AssemblyBodyNode::DelimiterAnchor { .. } => 1_u64,
                AssemblyBodyNode::LiteralIsland { .. } => 2_u64,
                AssemblyBodyNode::SubstrateSpan { .. } => 3_u64,
                AssemblyBodyNode::SlotPlaceholder { .. } => 4_u64,
                AssemblyBodyNode::MotifLink { .. } => 5_u64,
                AssemblyBodyNode::TypedBoundary { .. } => 6_u64,
            };
            hash ^= discriminant.wrapping_mul(0x100000001b3_u64);
            hash = hash.rotate_left(11);
            match node {
                AssemblyBodyNode::DelimiterAnchor { bytes }
                | AssemblyBodyNode::LiteralIsland { bytes }
                | AssemblyBodyNode::TypedBoundary { bytes, .. } => {
                    hash ^= rolling_assembly_hash(bytes);
                }
                AssemblyBodyNode::SubstrateSpan { reference } => {
                    hash ^= (reference.object_kind() as u64).rotate_left(7);
                    match reference {
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
                        | AssemblyComponentRef::ExactBundle {
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
                        } => {
                            hash ^= rolling_assembly_hash(object_id.as_bytes());
                            hash ^= (*start as u64).rotate_left(17);
                            hash ^= (*len as u64).rotate_left(29);
                        }
                    }
                }
                AssemblyBodyNode::SlotPlaceholder {
                    slot_index,
                    role_bits,
                    bytes,
                } => {
                    hash ^= (*slot_index as u64).rotate_left(13);
                    hash ^= role_bits.rotate_left(23);
                    hash ^= rolling_assembly_hash(bytes);
                }
                AssemblyBodyNode::MotifLink { motif_hash, bytes } => {
                    hash ^= motif_hash.rotate_left(19);
                    hash ^= rolling_assembly_hash(bytes);
                }
            }
        }
        hash
    }

    pub fn is_structural(&self) -> bool {
        self.structural_component_count() > 0 && self.literal_len() < self.output_len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblySupportVote {
    pub role_bits: u64,
    pub delimiter_bits: u64,
    pub slot_bits: u64,
    pub overlap_bytes: u32,
    pub support_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyAgreementSummary {
    pub agreement_score: u32,
    pub literal_burden: u32,
    pub ambiguity: u32,
    pub support: u32,
    pub support_count: u32,
    pub estimated_wire_cost: u32,
    pub decode_burden: u32,
    pub synchronization_cost: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyReuseSignature {
    pub version: ObjectVersion,
    pub body_shape_hash: u64,
    pub dependency_fingerprint: u64,
    pub output_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Assembly {
    pub assembly_id: AssemblyId,
    pub source_kind: SourceKind,
    pub assembly_kind: AssemblyKind,
    pub role_signature: RoleSignature,
    pub slots: Vec<SlotDescriptor>,
    pub body: AssemblyBody,
    pub dependency_closure: DependencyClosure,
    pub cue: SparseCue,
    pub lifecycle: ObjectLifecycleMeta,
    pub canonical_length_min: u32,
    pub canonical_length_max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyExtractionCandidate {
    pub assembly_kind: AssemblyKind,
    pub role_signature: RoleSignature,
    pub slots: Vec<SlotDescriptor>,
    pub body: AssemblyBody,
    pub dependency_closure: DependencyClosure,
    pub cue: SparseCue,
    pub canonical_length_min: u32,
    pub canonical_length_max: u32,
    pub structural_hash: u64,
    pub support_votes: Vec<AssemblySupportVote>,
    pub agreement: AssemblyAgreementSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyExtractionConfig {
    pub min_motif_len: u16,
    pub max_motif_len: u16,
    pub max_gap_bytes: u16,
    pub max_slots: u16,
}

impl AssemblyExtractionConfig {
    pub const fn bounded_default() -> Self {
        Self {
            min_motif_len: 4,
            max_motif_len: 32,
            max_gap_bytes: 24,
            max_slots: 4,
        }
    }
}

impl AssemblyExtractionCandidate {
    pub fn predicted_bytes(&self) -> u32 {
        self.body.output_len()
    }

    pub fn residual_bytes(&self) -> u32 {
        self.agreement.literal_burden
    }

    pub fn expansion_depth(&self) -> u8 {
        self.agreement.decode_burden.min(u8::MAX as u32) as u8
    }

    pub fn estimated_decode_cost(&self) -> u32 {
        self.agreement.decode_burden
    }

    pub fn support_gain(&self) -> u32 {
        self.agreement.support
    }

    pub fn support_count(&self) -> u32 {
        self.agreement.support_count
    }

    pub fn support_vote_count(&self) -> u32 {
        self.support_votes.len().min(u32::MAX as usize) as u32
    }

    pub fn agreement_score(&self) -> u32 {
        self.agreement.agreement_score
    }

    pub fn ambiguity_score(&self) -> u32 {
        self.agreement.ambiguity
    }

    pub fn dependency_sync_risk(&self) -> u32 {
        self.agreement.synchronization_cost
    }

    pub fn estimated_route_wire_cost(&self) -> u32 {
        self.agreement.estimated_wire_cost
    }

    pub fn admissibility_input(
        &self,
        ambiguity_score: u32,
        dependencies_available: bool,
    ) -> AssemblyAdmissibilityInput {
        AssemblyAdmissibilityInput {
            ambiguity_score,
            dependencies_available,
            expansion_depth: self.expansion_depth(),
            residual_bytes: self.residual_bytes(),
            predicted_bytes: self.predicted_bytes(),
            contract_min_len: self.canonical_length_min,
            contract_max_len: self.canonical_length_max,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyAdmissibilityPolicy {
    pub ambiguity_cap: u32,
    pub max_expansion_depth: u8,
    pub max_residual_dominance_percent: u8,
}

impl AssemblyAdmissibilityPolicy {
    pub const fn bounded_default() -> Self {
        Self {
            ambiguity_cap: 32,
            max_expansion_depth: 4,
            max_residual_dominance_percent: 70,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyAdmissibilityInput {
    pub ambiguity_score: u32,
    pub dependencies_available: bool,
    pub expansion_depth: u8,
    pub residual_bytes: u32,
    pub predicted_bytes: u32,
    pub contract_min_len: u32,
    pub contract_max_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssemblyAdmissibilityFailure {
    AmbiguityCap,
    DependencyUnavailable,
    ExpansionDepthExceeded,
    ResidualDominant,
    LengthContractInvalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyPromotionQueueEntry {
    pub assembly_id: AssemblyId,
    pub source_kind: SourceKind,
    pub ambiguity_score: u32,
    pub estimated_wire_savings: u32,
    pub support_count: u32,
    pub lifecycle: ObjectLifecycleMeta,
    pub cue: SparseCue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AssemblyPromotionQueue {
    pub pending: Vec<AssemblyPromotionQueueEntry>,
}

impl AssemblyPromotionQueue {
    pub fn push(&mut self, entry: AssemblyPromotionQueueEntry) {
        self.pending.push(entry);
        self.pending.sort_by(|left, right| {
            right
                .estimated_wire_savings
                .cmp(&left.estimated_wire_savings)
                .then_with(|| left.ambiguity_score.cmp(&right.ambiguity_score))
                .then_with(|| right.support_count.cmp(&left.support_count))
                .then_with(|| left.assembly_id.0.cmp(&right.assembly_id.0))
        });
    }

    pub fn promote_ready(
        &mut self,
        ambiguity_cap: u32,
        min_support: u32,
    ) -> Vec<AssemblyPromotionQueueEntry> {
        let mut promoted = Vec::new();
        let mut retained = Vec::new();
        for entry in self.pending.drain(..) {
            if entry.ambiguity_score <= ambiguity_cap && entry.support_count >= min_support {
                promoted.push(entry);
            } else {
                retained.push(entry);
            }
        }
        self.pending = retained;
        promoted
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct TransformId(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct DictionaryId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DictionaryEntry {
    pub token_id: u16,
    pub material: Vec<u8>,
    pub support_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SharedDictionary {
    pub dictionary_id: DictionaryId,
    pub source_kind: SourceKind,
    pub version: ObjectVersion,
    pub entries: Vec<DictionaryEntry>,
    pub keyed_permutation_seed: u64,
    pub max_token_len: u16,
    pub lifecycle: ObjectLifecycleMeta,
}

impl SharedDictionary {
    pub fn decode_tokens(&self, token_ids: &[u16]) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        for token_id in token_ids {
            let entry = self
                .entries
                .iter()
                .find(|entry| entry.token_id == *token_id)?;
            out.extend_from_slice(&entry.material);
        }
        Some(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
/// DEMOTED from active architecture. Transform kinds are retained for wire compatibility
/// and potential future reactivation, but the planner does not select transform routes.
pub enum TransformKind {
    #[default]
    PrefixInsert = 1,
    SuffixInsert = 2,
    Wrap = 3,
    BoundedInteriorInsert = 4,
    BoundedDelete = 5,
    BoundedSubstitution = 6,
    RepeatedMotifExpansion = 7,
    CopyWithGap = 8,
    SlotSubstitution = 9,
    RolePermutation = 10,
    DelimiterPreservingRewrite = 11,
    LocalMirror = 12,
    BoundedDuplication = 13,
    StridedSelection = 14,
    SpliceFromTwoBases = 15,
    SchemaSlotFill = 16,
    BoundedReorder = 17,
    MotifDuplicateCompress = 18,
    MirrorSymmetry = 19,
    SchemaConditionedExpansion = 20,
    RecursiveMacroExpansion = 21,
    TokenDictionarySubstitution = 22,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum TransformTransducerFamily {
    #[default]
    LegacyPatch = 1,
    SlotBind = 2,
    BoundedSubstitute = 3,
    DelimiterPreservingRewrite = 4,
    BoundedReorder = 5,
    MotifDuplicateCompress = 6,
    RolePermutation = 7,
    MirrorSymmetry = 8,
    SchemaConditionedExpansion = 9,
    RecursiveMacroExpansion = 10,
    TokenDictionary = 11,
}

impl TransformTransducerFamily {
    pub const fn default_for_kind(kind: TransformKind) -> Self {
        match kind {
            TransformKind::SlotSubstitution | TransformKind::SchemaSlotFill => Self::SlotBind,
            TransformKind::BoundedSubstitution => Self::BoundedSubstitute,
            TransformKind::DelimiterPreservingRewrite => Self::DelimiterPreservingRewrite,
            TransformKind::RolePermutation => Self::RolePermutation,
            TransformKind::BoundedReorder => Self::BoundedReorder,
            TransformKind::MotifDuplicateCompress
            | TransformKind::RepeatedMotifExpansion
            | TransformKind::BoundedDuplication => Self::MotifDuplicateCompress,
            TransformKind::MirrorSymmetry | TransformKind::LocalMirror => Self::MirrorSymmetry,
            TransformKind::SchemaConditionedExpansion => Self::SchemaConditionedExpansion,
            TransformKind::RecursiveMacroExpansion => Self::RecursiveMacroExpansion,
            TransformKind::TokenDictionarySubstitution => Self::TokenDictionary,
            TransformKind::PrefixInsert
            | TransformKind::SuffixInsert
            | TransformKind::Wrap
            | TransformKind::BoundedInteriorInsert
            | TransformKind::BoundedDelete
            | TransformKind::CopyWithGap
            | TransformKind::StridedSelection
            | TransformKind::SpliceFromTwoBases => Self::LegacyPatch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformParameterField {
    pub name: String,
    pub min_value: i32,
    pub max_value: i32,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformParameterSchema {
    pub parameters: Vec<TransformParameterField>,
    pub max_basis_count: u8,
    pub residual_allowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformBasisMask {
    pub allow_substrate: bool,
    pub allow_assembly: bool,
    pub allow_transform: bool,
}

impl TransformBasisMask {
    pub const fn substrate_only() -> Self {
        Self {
            allow_substrate: true,
            allow_assembly: false,
            allow_transform: false,
        }
    }

    pub const fn admits(self, object_kind: ObjectKind) -> bool {
        match object_kind {
            ObjectKind::AtomFragment
            | ObjectKind::ExactBlock
            | ObjectKind::ExactBundle
            | ObjectKind::ExactRange
            | ObjectKind::ResidualBuffer
            | ObjectKind::ExactState
            | ObjectKind::SourceDescriptor
            | ObjectKind::SparseCue
            | ObjectKind::Dictionary
            | ObjectKind::PredictiveObject => self.allow_substrate,
            ObjectKind::Assembly => self.allow_assembly,
            ObjectKind::Transform => self.allow_transform,
            ObjectKind::Schema | ObjectKind::EpisodeHint | ObjectKind::ReplayHint => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformOutputContract {
    pub min_output_len: u32,
    pub max_output_len: u32,
    pub preserves_delimiters: bool,
    pub deterministic: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformBasisRef {
    pub object_kind: ObjectKind,
    pub object_id: String,
    pub start: u32,
    pub len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformDependencyClosure {
    pub substrate_object_ids: Vec<String>,
    pub assembly_ids: Vec<AssemblyId>,
    pub transform_ids: Vec<TransformId>,
    pub schema_ids: Vec<SchemaId>,
    pub max_decode_depth: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformRoutePrior {
    pub planner_gain: u32,
    pub route_win_count: u32,
    pub route_loss_count: u32,
    pub last_route_win_tick: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformSupportMetrics {
    pub mean_residual_bytes: u32,
    pub mean_decode_steps: u32,
    pub last_residual_bytes: u32,
    pub last_decode_steps: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformClass {
    pub transform_id: TransformId,
    pub source_kind: SourceKind,
    pub transform_kind: TransformKind,
    pub transducer_family: TransformTransducerFamily,
    pub parameter_schema: TransformParameterSchema,
    pub basis_mask: TransformBasisMask,
    pub output_contract: TransformOutputContract,
    pub invertible: bool,
    pub dependency_closure: TransformDependencyClosure,
    pub support_metrics: TransformSupportMetrics,
    pub route_prior: TransformRoutePrior,
    pub lifecycle: ObjectLifecycleMeta,
    pub mean_residual_bytes: u32,
    pub mean_decode_steps: u32,
    pub reuse_savings: u32,
    pub stability_score: u32,
    pub failure_score: u32,
    pub cue: SparseCue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TransformInstance {
    pub class_id: TransformId,
    pub basis_refs: Vec<TransformBasisRef>,
    pub integer_parameters: Vec<i32>,
    pub residual_offsets: Vec<u32>,
    pub residual_bytes: Vec<Vec<u8>>,
    pub dependency_closure: TransformDependencyClosure,
    pub output_contract: TransformOutputContract,
    pub invertibility_expected: bool,
    pub output_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransformCandidate {
    pub class: TransformClass,
    pub instance: TransformInstance,
    pub estimated_wire_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransformPromotionQueueEntry {
    pub transform_id: TransformId,
    pub source_kind: SourceKind,
    pub estimated_wire_savings: u32,
    pub stability_score: u32,
    pub failure_score: u32,
    pub lifecycle: ObjectLifecycleMeta,
    pub cue: SparseCue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransformPromotionQueue {
    pub pending: Vec<TransformPromotionQueueEntry>,
}

impl TransformPromotionQueue {
    pub fn push(&mut self, entry: TransformPromotionQueueEntry) {
        self.pending.push(entry);
        self.pending.sort_by(|left, right| {
            right
                .estimated_wire_savings
                .cmp(&left.estimated_wire_savings)
                .then_with(|| right.stability_score.cmp(&left.stability_score))
                .then_with(|| left.failure_score.cmp(&right.failure_score))
                .then_with(|| left.transform_id.0.cmp(&right.transform_id.0))
        });
    }

    pub fn promote_ready(
        &mut self,
        min_support: u32,
        min_stability_score: u32,
        max_failure_score: u32,
    ) -> Vec<TransformPromotionQueueEntry> {
        let mut promoted = Vec::new();
        let mut retained = Vec::new();
        for entry in self.pending.drain(..) {
            if entry.lifecycle.support_count >= min_support
                && entry.stability_score >= min_stability_score
                && entry.failure_score <= max_failure_score
            {
                promoted.push(entry);
            } else {
                retained.push(entry);
            }
        }
        self.pending = retained;
        promoted
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct ContextHash(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct LagBucket(pub u16);

impl LagBucket {
    pub const fn from_distance(distance: u32) -> Self {
        Self(if distance <= 1 {
            0
        } else if distance <= 2 {
            1
        } else if distance <= 4 {
            2
        } else if distance <= 8 {
            3
        } else if distance <= 16 {
            4
        } else if distance <= 32 {
            5
        } else {
            6
        })
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct TransitionCount(pub u32);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct BranchRank(pub u16);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct EpisodeNodeId(pub u64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct EpisodeEdgeId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeObjectRef {
    pub object_kind: ObjectKind,
    pub object_id: String,
}

impl EpisodeObjectRef {
    pub fn assembly(id: AssemblyId) -> Self {
        Self {
            object_kind: ObjectKind::Assembly,
            object_id: format!("assembly:{}", id.0),
        }
    }
    pub fn transform(id: TransformId) -> Self {
        Self {
            object_kind: ObjectKind::Transform,
            object_id: format!("transform:{}", id.0),
        }
    }
    pub fn schema(object_id: impl Into<String>) -> Self {
        Self {
            object_kind: ObjectKind::Schema,
            object_id: object_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeNode {
    pub node_id: EpisodeNodeId,
    pub plane: MemoryPlane,
    pub source_kind: SourceKind,
    pub object_ref: EpisodeObjectRef,
    pub context_hash: ContextHash,
    pub lifecycle: ObjectLifecycleMeta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeEdge {
    pub edge_id: EpisodeEdgeId,
    pub from_node: EpisodeNodeId,
    pub to_node: EpisodeNodeId,
    pub lag_bucket: LagBucket,
    pub transition_count: TransitionCount,
    pub branch_rank: BranchRank,
    pub context_hash: ContextHash,
    pub lifecycle: ObjectLifecycleMeta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeActivationEvent {
    pub source_kind: SourceKind,
    pub object_ref: EpisodeObjectRef,
    pub cue: SparseCue,
    pub context_hash: ContextHash,
    pub tick: u64,
    pub success: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeObjectTransition {
    pub from_kind: ObjectKind,
    pub to_kind: ObjectKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeRouteWinTrace {
    pub success_by_route: BTreeMap<u8, u32>,
    pub failure_by_route: BTreeMap<u8, u32>,
    pub total_successes: u32,
    pub total_failures: u32,
    pub last_route: Option<ControllerRouteFamily>,
}

impl EpisodeRouteWinTrace {
    pub fn record(&mut self, route_family: ControllerRouteFamily, success: bool) -> bool {
        self.last_route = Some(route_family);
        let tag = route_family as u8;
        if success {
            let entry = self.success_by_route.entry(tag).or_insert(0);
            let first_win = *entry == 0;
            *entry = entry.saturating_add(1);
            self.total_successes = self.total_successes.saturating_add(1);
            first_win
        } else {
            let entry = self.failure_by_route.entry(tag).or_insert(0);
            *entry = entry.saturating_add(1);
            self.total_failures = self.total_failures.saturating_add(1);
            false
        }
    }

    pub fn support_score(&self) -> u32 {
        self.total_successes
            .saturating_mul(8)
            .saturating_sub(self.total_failures.saturating_mul(3))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SparseEpisodeTrace {
    pub trace_id: u64,
    pub lookup_key: crate::EpisodeTraceLookupKey,
    pub family_partition: u64,
    pub collision_slot: u16,
    pub source_kind: SourceKind,
    pub cue: SparseCue,
    pub cue_bits: u64,
    pub role_bits: u64,
    pub boundary_bits: u64,
    pub lag_bucket: LagBucket,
    pub transition: EpisodeObjectTransition,
    pub object_ref: EpisodeObjectRef,
    pub support_count: u32,
    pub recency_tick: u64,
    pub salience: u32,
    pub route_wins: EpisodeRouteWinTrace,
}

impl SparseEpisodeTrace {
    fn fresh(
        trace_id: u64,
        event: &EpisodeActivationEvent,
        prev: Option<&EpisodeActivationEvent>,
        family_partition: u64,
        collision_slot: u16,
    ) -> Self {
        let lag_bucket = prev
            .map(|prev| LagBucket::from_distance(event.tick.saturating_sub(prev.tick) as u32))
            .unwrap_or_default();
        let transition = EpisodeObjectTransition {
            from_kind: prev
                .map(|prev| prev.object_ref.object_kind)
                .unwrap_or(event.object_ref.object_kind),
            to_kind: event.object_ref.object_kind,
        };
        let salience = initial_trace_salience(event.cue, event.success, lag_bucket, transition);
        Self {
            trace_id,
            lookup_key: crate::EpisodeTraceLookupKey {
                source_kind: event.source_kind,
                object_kind: event.object_ref.object_kind,
                cue: event.cue,
                context_hash: event.context_hash,
            },
            family_partition,
            collision_slot,
            source_kind: event.source_kind,
            cue: event.cue,
            cue_bits: event.cue.family_bits,
            role_bits: event.cue.role_bits,
            boundary_bits: event.cue.delimiter_bits,
            lag_bucket,
            transition,
            object_ref: event.object_ref.clone(),
            support_count: 1,
            recency_tick: event.tick,
            salience,
            route_wins: EpisodeRouteWinTrace::default(),
        }
    }

    fn absorb_activation(
        &mut self,
        event: &EpisodeActivationEvent,
        prev: Option<&EpisodeActivationEvent>,
    ) {
        self.cue = event.cue;
        self.lookup_key.cue = event.cue;
        self.lookup_key.context_hash = event.context_hash;
        self.cue_bits = event.cue.family_bits;
        self.role_bits = event.cue.role_bits;
        self.boundary_bits = event.cue.delimiter_bits;
        self.lag_bucket = prev
            .map(|prev| LagBucket::from_distance(event.tick.saturating_sub(prev.tick) as u32))
            .unwrap_or(self.lag_bucket);
        self.transition = EpisodeObjectTransition {
            from_kind: prev
                .map(|prev| prev.object_ref.object_kind)
                .unwrap_or(self.transition.from_kind),
            to_kind: event.object_ref.object_kind,
        };
        self.support_count = self.support_count.saturating_add(1);
        self.recency_tick = self.recency_tick.max(event.tick);
        self.salience = self
            .salience
            .saturating_add(
                initial_trace_salience(event.cue, event.success, self.lag_bucket, self.transition)
                    / 2,
            )
            .max(self.support_count);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HippocampalShockMemory {
    pub max_traces: usize,
    pub max_collision_slots: u16,
    pub traces: Vec<SparseEpisodeTrace>,
}

impl Default for HippocampalShockMemory {
    fn default() -> Self {
        Self {
            max_traces: 256,
            max_collision_slots: 32,
            traces: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundedContextModel {
    pub max_order: u8,
    pub max_branches_per_context: u16,
    pub transition_counts: BTreeMap<Vec<u64>, BTreeMap<String, u32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContinuationPrediction {
    pub context_hash: ContextHash,
    pub predicted_kind: ObjectKind,
    pub predicted_object_id: String,
    pub transition_count: TransitionCount,
    pub branch_rank: BranchRank,
    pub precision_band: PrecisionBand,
}

impl BoundedContextModel {
    pub fn bounded(max_order: u8, max_branches_per_context: u16) -> Self {
        Self {
            max_order: max_order.max(1),
            max_branches_per_context: max_branches_per_context.max(1),
            transition_counts: BTreeMap::new(),
        }
    }

    pub fn observe(&mut self, context: &[ContextHash], next: &EpisodeObjectRef) {
        let max_order = self.max_order as usize;
        let start = context.len().saturating_sub(max_order);
        for order_start in start..context.len() {
            let key = context[order_start..]
                .iter()
                .map(|hash| hash.0)
                .collect::<Vec<_>>();
            let entry = self.transition_counts.entry(key).or_default();
            let object_key = format!("{}:{}", next.object_kind as u8, next.object_id);
            let counter = entry.entry(object_key).or_insert(0);
            *counter = counter.saturating_add(1);
        }
    }

    pub fn predict_next_kind(&self, context: &[ContextHash]) -> Vec<ContinuationPrediction> {
        let max_order = self.max_order as usize;
        let start = context.len().saturating_sub(max_order);
        let mut merged: BTreeMap<String, u32> = BTreeMap::new();
        for order_start in start..context.len() {
            let key = context[order_start..]
                .iter()
                .map(|hash| hash.0)
                .collect::<Vec<_>>();
            if let Some(counts) = self.transition_counts.get(&key) {
                for (object_key, count) in counts {
                    let entry = merged.entry(object_key.clone()).or_insert(0);
                    *entry = entry.saturating_add(*count);
                }
            }
        }
        let mut ranked = merged.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|l, r| r.1.cmp(&l.1).then_with(|| l.0.cmp(&r.0)));
        ranked.truncate(self.max_branches_per_context as usize);
        ranked
            .into_iter()
            .enumerate()
            .map(|(idx, (object_key, count))| {
                let (kind_tag, object_id) = object_key
                    .split_once(':')
                    .unwrap_or(("14", object_key.as_str()));
                let predicted_kind = match kind_tag.parse::<u8>().ok() {
                    Some(9) => ObjectKind::Assembly,
                    Some(10) => ObjectKind::Transform,
                    Some(11) => ObjectKind::Schema,
                    Some(12) => ObjectKind::EpisodeHint,
                    Some(13) => ObjectKind::ReplayHint,
                    _ => ObjectKind::PredictiveObject,
                };
                let precision_band = if count >= 8 {
                    PrecisionBand::Tight
                } else if count >= 4 {
                    PrecisionBand::Balanced
                } else {
                    PrecisionBand::Broad
                };
                ContinuationPrediction {
                    context_hash: context.last().copied().unwrap_or_default(),
                    predicted_kind,
                    predicted_object_id: object_id.to_string(),
                    transition_count: TransitionCount(count),
                    branch_rank: BranchRank(idx as u16),
                    precision_band,
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkingTraceMemory {
    pub max_events: usize,
    pub events: Vec<EpisodeActivationEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShortEpisodeMemory {
    pub max_nodes: usize,
    pub nodes: Vec<EpisodeNode>,
    pub edges: Vec<EpisodeEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusteredEpisodeMemory {
    pub max_contexts: usize,
    pub clusters: BTreeMap<ContextHash, Vec<EpisodeEdge>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeMemory {
    pub working_trace: WorkingTraceMemory,
    pub short_episode: ShortEpisodeMemory,
    pub clustered_episode: ClusteredEpisodeMemory,
    pub hippocampal_shock: HippocampalShockMemory,
    pub context_model: BoundedContextModel,
}

impl Default for BoundedContextModel {
    fn default() -> Self {
        Self::bounded(4, 8)
    }
}

impl Default for WorkingTraceMemory {
    fn default() -> Self {
        Self {
            max_events: 32,
            events: Vec::new(),
        }
    }
}

impl Default for ShortEpisodeMemory {
    fn default() -> Self {
        Self {
            max_nodes: 128,
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }
}

impl Default for ClusteredEpisodeMemory {
    fn default() -> Self {
        Self {
            max_contexts: 64,
            clusters: BTreeMap::new(),
        }
    }
}

impl Default for EpisodeMemory {
    fn default() -> Self {
        Self::bounded()
    }
}

impl EpisodeMemory {
    pub fn bounded() -> Self {
        Self {
            working_trace: WorkingTraceMemory {
                max_events: 32,
                events: Vec::new(),
            },
            short_episode: ShortEpisodeMemory {
                max_nodes: 128,
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            clustered_episode: ClusteredEpisodeMemory {
                max_contexts: 64,
                clusters: BTreeMap::new(),
            },
            hippocampal_shock: HippocampalShockMemory::default(),
            context_model: BoundedContextModel::bounded(4, 8),
        }
    }

    pub fn append_activation(&mut self, event: EpisodeActivationEvent) {
        let prev_event = self.working_trace.events.last().cloned();

        self.working_trace.events.push(event.clone());
        if self.working_trace.events.len() > self.working_trace.max_events {
            let drop_n = self.working_trace.events.len() - self.working_trace.max_events;
            self.working_trace.events.drain(0..drop_n);
        }

        let node_id = EpisodeNodeId(self.short_episode.nodes.len() as u64 + 1);
        let node = EpisodeNode {
            node_id,
            plane: plane_for_object_kind(event.object_ref.object_kind),
            source_kind: event.source_kind,
            object_ref: event.object_ref.clone(),
            context_hash: event.context_hash,
            lifecycle: ObjectLifecycleMeta::default().record_seen(event.tick, event.success),
        };
        self.short_episode.nodes.push(node.clone());
        if self.short_episode.nodes.len() > self.short_episode.max_nodes {
            let drop_n = self.short_episode.nodes.len() - self.short_episode.max_nodes;
            self.short_episode.nodes.drain(0..drop_n);
        }

        self.ingest_sparse_trace(&event, prev_event.as_ref());

        let context_window = self.recent_context_hashes();
        self.context_model
            .observe(&context_window, &event.object_ref);

        if self.short_episode.nodes.len() >= 2 {
            let prev = &self.short_episode.nodes[self.short_episode.nodes.len() - 2];
            let rank = rank_branch(prev.node_id, &node.object_ref, &self.short_episode.edges);
            let lag_bucket = prev_event
                .as_ref()
                .map(|prev_event| {
                    LagBucket::from_distance(event.tick.saturating_sub(prev_event.tick) as u32)
                })
                .unwrap_or_default();
            let edge = EpisodeEdge {
                edge_id: EpisodeEdgeId(self.short_episode.edges.len() as u64 + 1),
                from_node: prev.node_id,
                to_node: node.node_id,
                lag_bucket,
                transition_count: TransitionCount(
                    edge_transition_count(prev.node_id, node.node_id, &self.short_episode.edges)
                        .saturating_add(1),
                ),
                branch_rank: rank,
                context_hash: event.context_hash,
                lifecycle: ObjectLifecycleMeta::default().record_seen(event.tick, event.success),
            };
            self.short_episode.edges.push(edge.clone());
            self.clustered_episode
                .clusters
                .entry(event.context_hash)
                .or_default()
                .push(edge);
            if self.clustered_episode.clusters.len() > self.clustered_episode.max_contexts {
                if let Some(first) = self.clustered_episode.clusters.keys().next().copied() {
                    self.clustered_episode.clusters.remove(&first);
                }
            }
        }
    }

    pub fn record_route_outcome(
        &mut self,
        route_family: ControllerRouteFamily,
        source_kind: Option<SourceKind>,
        context_hash: Option<ContextHash>,
        tick: u64,
        success: bool,
    ) {
        let mut reinforced = false;
        for trace in self.hippocampal_shock.traces.iter_mut().rev() {
            if source_kind
                .map(|source| source == trace.source_kind)
                .unwrap_or(true)
                && context_hash
                    .map(|hash| hash == trace.lookup_key.context_hash)
                    .unwrap_or(true)
                && trace.recency_tick <= tick
            {
                let first_win = trace.route_wins.record(route_family, success);
                trace.recency_tick = trace.recency_tick.max(tick);
                trace.salience = trace.salience.saturating_add(if first_win && success {
                    24
                } else if success {
                    8
                } else {
                    2
                });
                if success {
                    trace.support_count = trace.support_count.saturating_add(1);
                }
                reinforced = true;
                break;
            }
        }
        if !reinforced && success {
            if let Some(last_event) = self.working_trace.events.last().cloned() {
                let prev_event = self.previous_working_event().cloned();
                self.ingest_sparse_trace(&last_event, prev_event.as_ref());
                if let Some(trace) = self.hippocampal_shock.traces.last_mut() {
                    let _ = trace.route_wins.record(route_family, true);
                    trace.salience = trace.salience.saturating_add(24);
                    trace.support_count = trace.support_count.saturating_add(1);
                    trace.recency_tick = tick;
                }
            }
        }
    }

    pub fn recent_context_hashes(&self) -> Vec<ContextHash> {
        self.working_trace
            .events
            .iter()
            .rev()
            .take(self.context_model.max_order as usize)
            .map(|e| e.context_hash)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    pub fn decay_working_trace(&mut self, keep_last: usize) {
        if self.working_trace.events.len() > keep_last {
            let drop_n = self.working_trace.events.len() - keep_last;
            self.working_trace.events.drain(0..drop_n);
        }
    }

    pub fn decay_short_episode(&mut self, keep_last: usize) {
        if self.short_episode.nodes.len() > keep_last {
            let drop_n = self.short_episode.nodes.len() - keep_last;
            self.short_episode.nodes.drain(0..drop_n);
        }
        if self.short_episode.edges.len() > keep_last {
            let drop_n = self.short_episode.edges.len() - keep_last;
            self.short_episode.edges.drain(0..drop_n);
        }
        if self.hippocampal_shock.traces.len() > keep_last {
            self.hippocampal_shock.traces.sort_by(|l, r| {
                r.salience
                    .cmp(&l.salience)
                    .then_with(|| r.recency_tick.cmp(&l.recency_tick))
            });
            self.hippocampal_shock.traces.truncate(keep_last);
        }
    }

    pub fn decay_clustered_episode(&mut self, keep_contexts: usize) {
        while self.clustered_episode.clusters.len() > keep_contexts {
            if let Some(first) = self.clustered_episode.clusters.keys().next().copied() {
                self.clustered_episode.clusters.remove(&first);
            } else {
                break;
            }
        }
    }

    fn previous_working_event(&self) -> Option<&EpisodeActivationEvent> {
        if self.working_trace.events.len() >= 2 {
            self.working_trace
                .events
                .get(self.working_trace.events.len().saturating_sub(2))
        } else {
            None
        }
    }

    fn ingest_sparse_trace(
        &mut self,
        event: &EpisodeActivationEvent,
        prev: Option<&EpisodeActivationEvent>,
    ) {
        let family_partition = trace_family_partition(event, prev);
        let candidate = SparseEpisodeTrace::fresh(
            self.hippocampal_shock.traces.len() as u64 + 1,
            event,
            prev,
            family_partition,
            0,
        );
        if let Some(existing) = self.hippocampal_shock.traces.iter_mut().find(|trace| {
            trace.lookup_key.object_kind == candidate.lookup_key.object_kind
                && trace.object_ref == candidate.object_ref
                && trace.lookup_key.context_hash == candidate.lookup_key.context_hash
                && trace.family_partition == family_partition
        }) {
            existing.absorb_activation(event, prev);
            return;
        }
        let collision_slot = diversified_collision_slot(
            &self.hippocampal_shock.traces,
            &candidate,
            self.hippocampal_shock.max_collision_slots,
        );
        let mut separated = candidate;
        separated.collision_slot = collision_slot;
        self.hippocampal_shock.traces.push(separated);
        if self.hippocampal_shock.traces.len() > self.hippocampal_shock.max_traces {
            self.hippocampal_shock.traces.sort_by(|l, r| {
                r.salience
                    .cmp(&l.salience)
                    .then_with(|| r.support_count.cmp(&l.support_count))
                    .then_with(|| r.recency_tick.cmp(&l.recency_tick))
            });
            self.hippocampal_shock
                .traces
                .truncate(self.hippocampal_shock.max_traces);
        }
    }
}

fn trace_family_partition(
    event: &EpisodeActivationEvent,
    prev: Option<&EpisodeActivationEvent>,
) -> u64 {
    let from_kind = prev
        .map(|prev| prev.object_ref.object_kind as u64)
        .unwrap_or(event.object_ref.object_kind as u64);
    event.cue.family_bits
        ^ event.source_kind.family_bits().rotate_left(19)
        ^ from_kind.rotate_left(43)
        ^ (event.object_ref.object_kind as u64).rotate_left(51)
}

fn trace_collision_seed(trace: &SparseEpisodeTrace) -> u16 {
    let mut seed =
        (trace.lookup_key.context_hash.0 ^ trace.cue.family_bits ^ trace.role_bits.rotate_left(7))
            .wrapping_add(trace.boundary_bits.rotate_left(13));
    seed ^= (trace.transition.from_kind as u64) << 8;
    seed ^= (trace.transition.to_kind as u64) << 16;
    (seed as u16).wrapping_mul(31)
}

fn trace_near_collision(left: &SparseEpisodeTrace, right: &SparseEpisodeTrace) -> bool {
    left.family_partition == right.family_partition
        && left.cue.overlap_score(right.cue) >= 8
        && left.object_ref != right.object_ref
}

fn diversified_collision_slot(
    traces: &[SparseEpisodeTrace],
    candidate: &SparseEpisodeTrace,
    max_collision_slots: u16,
) -> u16 {
    let limit = max_collision_slots.max(1);
    let seed = trace_collision_seed(candidate) % limit;
    for salt in 0..limit {
        let slot = seed.wrapping_add(salt) % limit;
        let occupied = traces.iter().any(|trace| {
            trace.family_partition == candidate.family_partition
                && trace.collision_slot == slot
                && trace_near_collision(trace, candidate)
        });
        if !occupied {
            return slot;
        }
    }
    seed
}

fn initial_trace_salience(
    cue: SparseCue,
    success: bool,
    lag_bucket: LagBucket,
    transition: EpisodeObjectTransition,
) -> u32 {
    let cue_mass = cue.family_bits.count_ones()
        + cue.role_bits.count_ones()
        + cue.delimiter_bits.count_ones()
        + cue.length_bucket_bits.count_ones();
    let success_mass = if success { 16 } else { 4 };
    cue_mass
        .saturating_add(success_mass)
        .saturating_add(transition.from_kind as u32)
        .saturating_add(transition.to_kind as u32)
        .saturating_sub(lag_bucket.0 as u32)
}

fn trace_recency_score(last_tick: u64, trace_tick: u64) -> u32 {
    if last_tick == 0 || trace_tick == 0 {
        return 0;
    }
    let delta = last_tick.saturating_sub(trace_tick).min(63) as u32;
    64_u32.saturating_sub(delta)
}

fn trace_transition_match(previous_kind: Option<ObjectKind>, trace: &SparseEpisodeTrace) -> u32 {
    match previous_kind {
        Some(kind) if kind == trace.transition.from_kind => 24,
        Some(kind) if kind == trace.transition.to_kind => 8,
        Some(_) => 0,
        None => 4,
    }
}

fn trace_lag_match(observed_lag: LagBucket, trace: &SparseEpisodeTrace) -> u32 {
    if observed_lag == trace.lag_bucket {
        16
    } else {
        let delta = observed_lag.0.abs_diff(trace.lag_bucket.0) as u32;
        8_u32.saturating_sub(delta.saturating_mul(2))
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SchemaId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SchemaKind {
    #[default]
    Sequence = 1,
    Template = 2,
    Alternation = 3,
    Permutation = 4,
    Expansion = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSlotType {
    #[default]
    ExactBytes = 1,
    Atom = 2,
    Assembly = 3,
    Transform = 4,
    Episode = 5,
    Schema = 6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaEntryCondition {
    pub cue: SparseCue,
    pub required_context_hashes: Vec<ContextHash>,
    pub admissible_source_kinds: Vec<SourceKind>,
    pub min_support_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaSlotDescriptor {
    pub slot_id: u16,
    pub slot_type: SchemaSlotType,
    pub required: bool,
    pub min_len: u32,
    pub max_len: u32,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SchemaNodeId(pub u32);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct SchemaEdgeId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaObjectRef {
    pub object_kind: ObjectKind,
    pub object_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaNode {
    pub node_id: SchemaNodeId,
    pub kind: ObjectKind,
    pub object_ref: Option<SchemaObjectRef>,
    pub slot_binding: Option<u16>,
    pub output_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaEdge {
    pub edge_id: SchemaEdgeId,
    pub from_node: SchemaNodeId,
    pub to_node: SchemaNodeId,
    pub guard_precision: PrecisionBand,
    pub max_fanout: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaDependencyClosure {
    pub version: ObjectVersion,
    pub dependencies: Vec<ObjectDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaGraph {
    pub schema_id: SchemaId,
    pub source_kind: SourceKind,
    pub schema_kind: SchemaKind,
    pub entry_condition: SchemaEntryCondition,
    pub slots: Vec<SchemaSlotDescriptor>,
    pub nodes: Vec<SchemaNode>,
    pub edges: Vec<SchemaEdge>,
    pub dependency_closure: SchemaDependencyClosure,
    pub decode_max_nodes: u16,
    pub decode_max_depth: u8,
    pub output_len: u32,
    pub lifecycle: ObjectLifecycleMeta,
    pub cue: SparseCue,
    pub cross_episode_support: u32,
    pub contradiction_burden: u32,
    pub branch_consistency: u32,
    pub mean_decode_burden: u32,
    pub retire_after_failures: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaRouteCandidate {
    pub schema_id: SchemaId,
    pub source_kind: SourceKind,
    pub schema_kind: SchemaKind,
    pub entry_condition: SchemaEntryCondition,
    pub dependency_closure: SchemaDependencyClosure,
    pub precision_band: PrecisionBand,
    pub fanout_cap: u16,
    pub depth_cap: u8,
    pub output_len: u32,
    pub cue: SparseCue,
    pub cue_overlap: u32,
    pub cross_episode_support: u32,
    pub contradiction_burden: u32,
    pub branch_consistency: u32,
    pub decode_burden: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaCandidatePolicy {
    pub max_candidates: u16,
    pub max_depth: u8,
    pub max_fanout: u16,
}

impl SchemaCandidatePolicy {
    pub const fn bounded_default() -> Self {
        Self {
            max_candidates: 8,
            max_depth: 8,
            max_fanout: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaPromotionQueueEntry {
    pub schema_id: SchemaId,
    pub source_kind: SourceKind,
    pub cross_episode_support: u32,
    pub topology_stability: u32,
    pub contradiction_burden: u32,
    pub branch_consistency: u32,
    pub decode_burden: u32,
    pub retire_after_failures: u32,
    pub lifecycle: ObjectLifecycleMeta,
    pub cue: SparseCue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaPromotionQueue {
    pub pending: Vec<SchemaPromotionQueueEntry>,
}

impl SchemaPromotionQueue {
    pub fn push(&mut self, entry: SchemaPromotionQueueEntry) {
        self.pending.push(entry);
        self.pending.sort_by(|l, r| {
            r.cross_episode_support
                .cmp(&l.cross_episode_support)
                .then_with(|| l.contradiction_burden.cmp(&r.contradiction_burden))
                .then_with(|| r.branch_consistency.cmp(&l.branch_consistency))
                .then_with(|| r.topology_stability.cmp(&l.topology_stability))
                .then_with(|| l.schema_id.0.cmp(&r.schema_id.0))
        });
    }

    pub fn promote_ready(
        &mut self,
        min_support: u32,
        min_topology_stability: u32,
    ) -> Vec<SchemaPromotionQueueEntry> {
        let mut promoted = Vec::new();
        let mut retained = Vec::new();
        for entry in self.pending.drain(..) {
            let contradiction_gate = entry
                .cross_episode_support
                .saturating_mul(4)
                .saturating_add(entry.branch_consistency.max(1))
                .saturating_add(16);
            let consistency_gate = entry.branch_consistency.max(1)
                >= entry.contradiction_burden.saturating_div(2).max(1);
            if entry.cross_episode_support >= min_support
                && entry.topology_stability >= min_topology_stability
                && entry.contradiction_burden <= contradiction_gate
                && consistency_gate
            {
                promoted.push(entry);
            } else {
                retained.push(entry);
            }
        }
        self.pending = retained;
        promoted
    }
}

pub fn generate_schema_route_candidates(
    schemas: &[SchemaGraph],
    cue: SparseCue,
    source_kind: SourceKind,
    context_hash: Option<ContextHash>,
    policy: SchemaCandidatePolicy,
) -> Vec<SchemaRouteCandidate> {
    let mut out = schemas
        .iter()
        .filter(|schema| schema.source_kind == source_kind)
        .filter(|schema| {
            schema.decode_max_depth <= policy.max_depth
                && schema.decode_max_nodes <= policy.max_fanout
        })
        .filter(|schema| {
            schema.entry_condition.admissible_source_kinds.is_empty()
                || schema
                    .entry_condition
                    .admissible_source_kinds
                    .contains(&source_kind)
        })
        .filter(|schema| {
            if let Some(context_hash) = context_hash {
                schema.entry_condition.required_context_hashes.is_empty()
                    || schema
                        .entry_condition
                        .required_context_hashes
                        .contains(&context_hash)
            } else {
                schema.entry_condition.required_context_hashes.is_empty()
            }
        })
        .map(|schema| SchemaRouteCandidate {
            schema_id: schema.schema_id,
            source_kind: schema.source_kind,
            schema_kind: schema.schema_kind,
            entry_condition: schema.entry_condition.clone(),
            dependency_closure: schema.dependency_closure.clone(),
            precision_band: PrecisionBand::Balanced,
            fanout_cap: schema.decode_max_nodes.min(policy.max_fanout),
            depth_cap: schema.decode_max_depth.min(policy.max_depth),
            output_len: schema.output_len,
            cue: schema.cue,
            cue_overlap: cue.overlap_score(schema.cue),
            cross_episode_support: schema
                .cross_episode_support
                .max(schema.lifecycle.support_count),
            contradiction_burden: schema.contradiction_burden,
            branch_consistency: schema
                .branch_consistency
                .max(schema.edges.len().min(u32::MAX as usize) as u32),
            decode_burden: schema
                .mean_decode_burden
                .max(schema.decode_max_depth as u32),
        })
        .collect::<Vec<_>>();
    out.sort_by(|l, r| {
        let left_quality = l
            .cue_overlap
            .saturating_add(l.cross_episode_support)
            .saturating_add(l.branch_consistency)
            .saturating_sub(l.contradiction_burden)
            .saturating_sub(l.decode_burden / 2);
        let right_quality = r
            .cue_overlap
            .saturating_add(r.cross_episode_support)
            .saturating_add(r.branch_consistency)
            .saturating_sub(r.contradiction_burden)
            .saturating_sub(r.decode_burden / 2);
        right_quality
            .cmp(&left_quality)
            .then_with(|| r.cue_overlap.cmp(&l.cue_overlap))
            .then_with(|| l.schema_id.0.cmp(&r.schema_id.0))
    });
    out.truncate(policy.max_candidates as usize);
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeCompletionCandidate {
    pub object_ref: EpisodeObjectRef,
    pub transition_count: TransitionCount,
    pub branch_rank: BranchRank,
    pub precision_band: PrecisionBand,
    pub cue_overlap: u32,
    pub recency_score: u32,
    pub route_support: u32,
    pub transition_match: u32,
    pub lag_bucket: LagBucket,
    pub admissible: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeCandidatePolicy {
    pub max_fanout: u16,
    pub min_transition_count: u32,
    pub max_branch_rank: u16,
}

impl EpisodeCandidatePolicy {
    pub const fn bounded_default() -> Self {
        Self {
            max_fanout: 8,
            min_transition_count: 1,
            max_branch_rank: 7,
        }
    }
}

pub fn derive_context_hash(cue: SparseCue, object_ref: &EpisodeObjectRef) -> ContextHash {
    let mut cue_bytes = Vec::with_capacity(40);
    cue_bytes.extend_from_slice(&cue.family_bits.to_le_bytes());
    cue_bytes.extend_from_slice(&cue.role_bits.to_le_bytes());
    cue_bytes.extend_from_slice(&cue.delimiter_bits.to_le_bytes());
    cue_bytes.extend_from_slice(&cue.length_bucket_bits.to_le_bytes());
    cue_bytes.extend_from_slice(&cue.temporal_bits.to_le_bytes());
    let cue_hash = gf2_fingerprint(&cue_bytes);
    let object_hash = gf2_fingerprint(object_ref.object_id.as_bytes());
    ContextHash(cue_hash ^ object_hash.rotate_left(7) ^ ((object_ref.object_kind as u64) << 48))
}

pub fn plane_for_object_kind(kind: ObjectKind) -> MemoryPlane {
    match kind {
        ObjectKind::Assembly => MemoryPlane::Assembly,
        ObjectKind::Transform => MemoryPlane::Transform,
        ObjectKind::Schema => MemoryPlane::Schema,
        ObjectKind::EpisodeHint | ObjectKind::ReplayHint => MemoryPlane::Episode,
        _ => MemoryPlane::AtomSubstrate,
    }
}

fn edge_transition_count(
    from_node: EpisodeNodeId,
    to_node: EpisodeNodeId,
    edges: &[EpisodeEdge],
) -> u32 {
    edges
        .iter()
        .filter(|edge| edge.from_node == from_node && edge.to_node == to_node)
        .count() as u32
}

fn rank_branch(
    from_node: EpisodeNodeId,
    next: &EpisodeObjectRef,
    edges: &[EpisodeEdge],
) -> BranchRank {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    for edge in edges.iter().filter(|edge| edge.from_node == from_node) {
        let key = edge.to_node.0.to_string();
        let entry = counts.entry(key).or_insert(0);
        *entry = entry.saturating_add(edge.transition_count.0);
    }
    let next_key = next.object_id.clone();
    let rank = counts
        .keys()
        .filter(|key| key.as_str() < next_key.as_str())
        .count() as u16;
    BranchRank(rank)
}

pub fn generate_episode_completion_candidates(
    memory: &EpisodeMemory,
    policy: EpisodeCandidatePolicy,
) -> Vec<EpisodeCompletionCandidate> {
    struct EpisodeAccumulator {
        object_ref: EpisodeObjectRef,
        transition_count: u32,
        branch_rank: u16,
        cue_overlap: u32,
        recency_score: u32,
        route_support: u32,
        transition_match: u32,
        lag_bucket: LagBucket,
        precision_score: u16,
    }

    impl Default for EpisodeAccumulator {
        fn default() -> Self {
            Self {
                object_ref: EpisodeObjectRef::default(),
                transition_count: 0,
                branch_rank: u16::MAX,
                cue_overlap: 0,
                recency_score: 0,
                route_support: 0,
                transition_match: 0,
                lag_bucket: LagBucket::default(),
                precision_score: 0,
            }
        }
    }

    let context = memory.recent_context_hashes();
    let partial_cue = memory
        .working_trace
        .events
        .last()
        .map(|event| event.cue)
        .unwrap_or_default();
    let last_tick = memory
        .working_trace
        .events
        .last()
        .map(|event| event.tick)
        .unwrap_or_default();
    let previous_kind = memory
        .working_trace
        .events
        .last()
        .map(|event| event.object_ref.object_kind);
    let observed_lag = if memory.working_trace.events.len() >= 2 {
        let last = &memory.working_trace.events[memory.working_trace.events.len() - 1];
        let prev = &memory.working_trace.events[memory.working_trace.events.len() - 2];
        LagBucket::from_distance(last.tick.saturating_sub(prev.tick) as u32)
    } else {
        LagBucket::default()
    };

    let mut merged: BTreeMap<String, EpisodeAccumulator> = BTreeMap::new();

    for prediction in memory.context_model.predict_next_kind(&context) {
        let key = format!(
            "{}:{}",
            prediction.predicted_kind as u8, prediction.predicted_object_id
        );
        let entry = merged.entry(key).or_default();
        entry.object_ref = EpisodeObjectRef {
            object_kind: prediction.predicted_kind,
            object_id: prediction.predicted_object_id.clone(),
        };
        entry.transition_count = entry
            .transition_count
            .saturating_add(prediction.transition_count.0);
        entry.branch_rank = entry.branch_rank.min(prediction.branch_rank.0);
        entry.precision_score = entry
            .precision_score
            .max(prediction.precision_band.score_value().0);
    }

    for trace in &memory.hippocampal_shock.traces {
        let cue_overlap = partial_cue.overlap_score(trace.cue);
        let transition_match = trace_transition_match(previous_kind, trace);
        let lag_match = trace_lag_match(observed_lag, trace);
        let route_support = trace.route_wins.support_score();
        let recency_score = trace_recency_score(last_tick, trace.recency_tick);
        if cue_overlap == 0 && transition_match == 0 && route_support == 0 {
            continue;
        }
        let key = format!(
            "{}:{}",
            trace.object_ref.object_kind as u8, trace.object_ref.object_id
        );
        let entry = merged.entry(key).or_default();
        if entry.object_ref.object_id.is_empty() {
            entry.object_ref = trace.object_ref.clone();
            entry.branch_rank = u16::MAX;
        }
        entry.transition_count = entry
            .transition_count
            .saturating_add(trace.support_count)
            .saturating_add(route_support / 8)
            .saturating_add(lag_match / 4);
        entry.cue_overlap = entry.cue_overlap.max(cue_overlap);
        entry.recency_score = entry.recency_score.max(recency_score);
        entry.route_support = entry.route_support.max(route_support);
        entry.transition_match = entry.transition_match.max(transition_match);
        entry.lag_bucket = trace.lag_bucket;
        entry.precision_score =
            entry
                .precision_score
                .max(if cue_overlap >= 12 || route_support >= 16 {
                    PrecisionBand::Tight.score_value().0
                } else if cue_overlap >= 6 || transition_match >= 16 {
                    PrecisionBand::Balanced.score_value().0
                } else {
                    PrecisionBand::Broad.score_value().0
                });
    }

    let mut out = merged
        .into_values()
        .filter_map(|entry| {
            if entry.object_ref.object_id.is_empty() {
                return None;
            }
            let branch_rank = if entry.branch_rank == u16::MAX {
                0
            } else {
                entry.branch_rank
            };
            let admissible = entry.transition_count >= policy.min_transition_count
                && branch_rank <= policy.max_branch_rank;
            if !admissible {
                return None;
            }
            let precision_band = if entry.precision_score >= PrecisionBand::Tight.score_value().0 {
                PrecisionBand::Tight
            } else if entry.precision_score >= PrecisionBand::Balanced.score_value().0 {
                PrecisionBand::Balanced
            } else {
                PrecisionBand::Broad
            };
            Some(EpisodeCompletionCandidate {
                object_ref: entry.object_ref,
                transition_count: TransitionCount(entry.transition_count),
                branch_rank: BranchRank(branch_rank),
                precision_band,
                cue_overlap: entry.cue_overlap,
                recency_score: entry.recency_score,
                route_support: entry.route_support,
                transition_match: entry.transition_match,
                lag_bucket: entry.lag_bucket,
                admissible,
            })
        })
        .collect::<Vec<_>>();

    fn episode_candidate_score(candidate: &EpisodeCompletionCandidate) -> u32 {
        candidate
            .transition_count
            .0
            .saturating_add(candidate.cue_overlap.saturating_mul(2))
            .saturating_add(candidate.route_support.saturating_mul(2))
            .saturating_add(candidate.recency_score)
            .saturating_add(candidate.transition_match)
    }

    out.sort_by(|l, r| {
        episode_candidate_score(r)
            .cmp(&episode_candidate_score(l))
            .then_with(|| r.route_support.cmp(&l.route_support))
            .then_with(|| r.cue_overlap.cmp(&l.cue_overlap))
            .then_with(|| r.recency_score.cmp(&l.recency_score))
            .then_with(|| r.transition_match.cmp(&l.transition_match))
            .then_with(|| r.transition_count.0.cmp(&l.transition_count.0))
            .then_with(|| l.branch_rank.0.cmp(&r.branch_rank.0))
            .then_with(|| l.object_ref.object_id.cmp(&r.object_ref.object_id))
    });
    out.truncate(policy.max_fanout as usize);
    for (idx, candidate) in out.iter_mut().enumerate() {
        candidate.branch_rank = BranchRank(idx as u16);
        candidate.admissible = candidate.transition_count.0 >= policy.min_transition_count
            && candidate.branch_rank.0 <= policy.max_branch_rank;
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeReplayQueueEntry {
    pub event: EpisodeActivationEvent,
    pub surprise: u32,
    pub reuse_potential: u32,
    pub ambiguity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EpisodeReplayQueue {
    pub pending: Vec<EpisodeReplayQueueEntry>,
}

impl EpisodeReplayQueue {
    pub fn push(&mut self, entry: EpisodeReplayQueueEntry) {
        self.pending.push(entry);
        self.pending.sort_by(|l, r| {
            r.surprise
                .cmp(&l.surprise)
                .then_with(|| r.reuse_potential.cmp(&l.reuse_potential))
                .then_with(|| l.ambiguity.cmp(&r.ambiguity))
        });
    }
}

pub fn transform_should_promote(
    class: &TransformClass,
    min_support: u32,
    min_stability_score: u32,
    min_reuse_savings: u32,
    max_failure_score: u32,
) -> bool {
    class.lifecycle.support_count >= min_support
        && class.stability_score >= min_stability_score
        && class.reuse_savings >= min_reuse_savings
        && class.failure_score <= max_failure_score
}

pub fn transform_apply_failure_demotes(class: &mut TransformClass, tick: u64) {
    class.lifecycle = class.lifecycle.record_seen(tick, false);
    class.failure_score = class.failure_score.saturating_add(1);
    class.stability_score = class.stability_score.saturating_sub(1);
    class.route_prior.route_loss_count = class.route_prior.route_loss_count.saturating_add(1);
    class.route_prior.planner_gain = class.route_prior.planner_gain.saturating_sub(1);
    class.lifecycle.promotion_level = match class.lifecycle.promotion_level {
        PromotionLevel::Canonical => PromotionLevel::Promoted,
        PromotionLevel::Promoted => PromotionLevel::Stable,
        PromotionLevel::Stable => PromotionLevel::Warm,
        PromotionLevel::Warm | PromotionLevel::Cold => PromotionLevel::Cold,
    };
}

pub fn transform_apply_success(
    class: &mut TransformClass,
    tick: u64,
    reuse_savings: u32,
    residual_bytes: u32,
    decode_steps: u32,
) {
    class.lifecycle = class.lifecycle.record_seen(tick, true);
    class.reuse_savings = class.reuse_savings.saturating_add(reuse_savings);
    class.stability_score = class.stability_score.saturating_add(1);
    class.mean_residual_bytes = running_mean_u32(
        class.mean_residual_bytes,
        class.lifecycle.success_count,
        residual_bytes,
    );
    class.mean_decode_steps = running_mean_u32(
        class.mean_decode_steps,
        class.lifecycle.success_count,
        decode_steps,
    );
    class.support_metrics.mean_residual_bytes = class.mean_residual_bytes;
    class.support_metrics.mean_decode_steps = class.mean_decode_steps;
    class.support_metrics.last_residual_bytes = residual_bytes;
    class.support_metrics.last_decode_steps = decode_steps;
    class.route_prior.route_win_count = class.route_prior.route_win_count.saturating_add(1);
    class.route_prior.last_route_win_tick = tick;
    class.route_prior.planner_gain = class
        .route_prior
        .planner_gain
        .saturating_add(route_prior_gain_delta(class));
    if transform_should_promote(class, 3, 2, 1, 4) {
        class.lifecycle.promotion_level = match class.lifecycle.promotion_level {
            PromotionLevel::Cold => PromotionLevel::Warm,
            PromotionLevel::Warm => PromotionLevel::Stable,
            PromotionLevel::Stable => PromotionLevel::Promoted,
            PromotionLevel::Promoted | PromotionLevel::Canonical => class.lifecycle.promotion_level,
        };
    }
}

fn running_mean_u32(current: u32, samples: u32, observation: u32) -> u32 {
    if samples <= 1 {
        return observation;
    }
    let retained = current.saturating_mul(samples.saturating_sub(1));
    retained
        .saturating_add(observation)
        .checked_div(samples)
        .unwrap_or(observation)
}

fn route_prior_gain_delta(class: &TransformClass) -> u32 {
    let stability = class.stability_score.min(8);
    let residual_bonus = class
        .mean_residual_bytes
        .saturating_sub(class.support_metrics.last_residual_bytes)
        .min(8);
    1_u32
        .saturating_add(stability / 2)
        .saturating_add(residual_bonus / 4)
}

pub fn assembly_route_eligibility(
    policy: AssemblyAdmissibilityPolicy,
    input: AssemblyAdmissibilityInput,
) -> Result<(), AssemblyAdmissibilityFailure> {
    if input.ambiguity_score > policy.ambiguity_cap {
        return Err(AssemblyAdmissibilityFailure::AmbiguityCap);
    }
    if !input.dependencies_available {
        return Err(AssemblyAdmissibilityFailure::DependencyUnavailable);
    }
    if input.expansion_depth > policy.max_expansion_depth {
        return Err(AssemblyAdmissibilityFailure::ExpansionDepthExceeded);
    }
    if input.contract_min_len > input.contract_max_len
        || input.predicted_bytes < input.contract_min_len
        || input.predicted_bytes > input.contract_max_len
    {
        return Err(AssemblyAdmissibilityFailure::LengthContractInvalid);
    }
    let total = input.predicted_bytes.max(1);
    let residual_percent = input.residual_bytes.saturating_mul(100) / total;
    if residual_percent > policy.max_residual_dominance_percent as u32 {
        return Err(AssemblyAdmissibilityFailure::ResidualDominant);
    }
    Ok(())
}

pub fn extract_contiguous_motif_candidates(
    source_kind: SourceKind,
    bytes: &[u8],
    config: AssemblyExtractionConfig,
) -> Vec<AssemblyExtractionCandidate> {
    let mut out = Vec::new();
    let min_len = config.min_motif_len.max(1) as usize;
    let max_len = config.max_motif_len.max(config.min_motif_len) as usize;
    let scan_cap = bytes.len().min(256);
    for start in 0..scan_cap {
        let remaining = bytes.len().saturating_sub(start);
        if remaining < min_len {
            break;
        }
        let len = remaining.min(max_len);
        let motif = &bytes[start..start + len];
        if motif.windows(2).all(|pair| pair[0] == pair[1]) {
            continue;
        }
        let cue = derive_sparse_cue_from_bytes(source_kind, motif);
        let role_signature = RoleSignature {
            role_bits: cue.role_bits,
            delimiter_role_bits: cue.delimiter_bits,
            slot_role_bits: 0,
        };
        out.push(build_assembly_candidate(
            AssemblyKind::ContiguousMotif,
            role_signature,
            Vec::new(),
            vec![AssemblyBodyNode::MotifLink {
                motif_hash: rolling_assembly_hash(motif),
                bytes: motif.to_vec(),
            }],
            DependencyClosure::default(),
            cue,
            len as u32,
            len as u32,
            rolling_assembly_hash(motif),
        ));
        if out.len() >= 8 {
            break;
        }
    }
    out
}

pub fn extract_discontinuous_field_group_candidates(
    source_kind: SourceKind,
    bytes: &[u8],
    config: AssemblyExtractionConfig,
) -> Vec<AssemblyExtractionCandidate> {
    let mut out = Vec::new();
    let max_gap = config.max_gap_bytes.max(1) as usize;
    for (left_idx, left) in bytes
        .split(|b| matches!(*b, b',' | b';' | b'|' | b'\n'))
        .enumerate()
    {
        if left.is_empty() {
            continue;
        }
        if left.len() < config.min_motif_len as usize {
            continue;
        }
        let offset = bytes
            .windows(left.len())
            .position(|w| w == left)
            .unwrap_or(0);
        let gap_start = offset + left.len();
        let gap_end = (gap_start + max_gap).min(bytes.len());
        let gap = &bytes[gap_start..gap_end];
        let right = gap
            .split(|b| matches!(*b, b',' | b';' | b'|' | b'\n'))
            .find(|part| !part.is_empty())
            .unwrap_or(&[]);
        if right.len() < config.min_motif_len as usize {
            continue;
        }
        let combined_len = left.len() + right.len();
        let cue = derive_sparse_cue_from_bytes(source_kind, &[left, right].concat());
        let left_slot = SlotDescriptor {
            slot_index: 0,
            role_bits: cue.role_bits | (1_u64 << (left_idx.min(31) as u64)),
            min_len: 1,
            max_len: left.len() as u32,
            required: true,
        };
        let right_slot = SlotDescriptor {
            slot_index: 1,
            role_bits: cue.role_bits | (1_u64 << ((left_idx + 1).min(31) as u64)),
            min_len: 1,
            max_len: right.len() as u32,
            required: true,
        };
        let mut nodes = vec![AssemblyBodyNode::SlotPlaceholder {
            slot_index: left_slot.slot_index,
            role_bits: left_slot.role_bits,
            bytes: left.to_vec(),
        }];
        let gap_literal = gap[..gap.len().saturating_sub(right.len())].to_vec();
        push_bytes_as_assembly_nodes(&mut nodes, &gap_literal);
        nodes.push(AssemblyBodyNode::SlotPlaceholder {
            slot_index: right_slot.slot_index,
            role_bits: right_slot.role_bits,
            bytes: right.to_vec(),
        });
        let role_signature = RoleSignature {
            role_bits: cue.role_bits | (1_u64 << (left_idx.min(31) as u64)),
            delimiter_role_bits: cue.delimiter_bits | delimiter_bits_for_bytes(&gap_literal),
            slot_role_bits: left_slot.role_bits | right_slot.role_bits,
        };
        out.push(build_assembly_candidate(
            AssemblyKind::DiscontinuousFieldGroup,
            role_signature,
            vec![left_slot, right_slot],
            nodes,
            DependencyClosure::default(),
            cue,
            combined_len as u32,
            (combined_len + max_gap) as u32,
            rolling_assembly_hash(left) ^ rolling_assembly_hash(right),
        ));
        if out.len() >= 8 {
            break;
        }
    }
    out
}

pub fn extract_delimiter_bounded_candidates(
    source_kind: SourceKind,
    bytes: &[u8],
    config: AssemblyExtractionConfig,
) -> Vec<AssemblyExtractionCandidate> {
    let mut out = Vec::new();
    let mut start = 0usize;
    while start < bytes.len() {
        while start < bytes.len() && !is_delimiter(bytes[start]) {
            start += 1;
        }
        if start >= bytes.len() {
            break;
        }
        let delimiter = bytes[start];
        let segment_start = start + 1;
        let mut end = segment_start;
        while end < bytes.len() && bytes[end] != delimiter {
            end += 1;
        }
        if end <= bytes.len() {
            let segment = &bytes[segment_start..end.min(bytes.len())];
            if segment.len() >= config.min_motif_len as usize {
                let cue = derive_sparse_cue_from_bytes(source_kind, segment);
                let role_signature = RoleSignature {
                    role_bits: cue.role_bits,
                    delimiter_role_bits: cue.delimiter_bits | delimiter_bit(delimiter),
                    slot_role_bits: 0,
                };
                out.push(build_assembly_candidate(
                    AssemblyKind::DelimiterBounded,
                    role_signature,
                    Vec::new(),
                    vec![
                        AssemblyBodyNode::DelimiterAnchor {
                            bytes: vec![delimiter],
                        },
                        AssemblyBodyNode::MotifLink {
                            motif_hash: rolling_assembly_hash(segment),
                            bytes: segment.to_vec(),
                        },
                        AssemblyBodyNode::DelimiterAnchor {
                            bytes: vec![delimiter],
                        },
                    ],
                    DependencyClosure::default(),
                    cue,
                    (segment.len() + 2) as u32,
                    (segment.len() + 2) as u32,
                    rolling_assembly_hash(segment) ^ ((delimiter as u64) << 48),
                ));
            }
        }
        start = end.saturating_add(1);
        if out.len() >= 8 {
            break;
        }
    }
    out
}

pub fn extract_slot_bearing_candidates(
    source_kind: SourceKind,
    bytes: &[u8],
    config: AssemblyExtractionConfig,
) -> Vec<AssemblyExtractionCandidate> {
    let mut out = Vec::new();
    for open in [b'{', b'[', b'(', b'<'] {
        let close = matching_close(open);
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            let Some(open_idx) = bytes[cursor..]
                .iter()
                .position(|b| *b == open)
                .map(|p| p + cursor)
            else {
                break;
            };
            let Some(close_idx) = bytes[open_idx + 1..]
                .iter()
                .position(|b| *b == close)
                .map(|p| p + open_idx + 1)
            else {
                break;
            };
            let inner = &bytes[open_idx + 1..close_idx];
            let parts = inner
                .split(|b| matches!(*b, b',' | b':' | b';' | b'|' | b' '))
                .filter(|part| !part.is_empty())
                .take(config.max_slots.max(1) as usize)
                .collect::<Vec<_>>();
            if parts.len() < 2 {
                cursor = close_idx + 1;
                continue;
            }
            let cue = derive_sparse_cue_from_bytes(source_kind, inner);
            let slots = parts
                .iter()
                .enumerate()
                .map(|(idx, part)| SlotDescriptor {
                    slot_index: idx as u16,
                    role_bits: cue.role_bits | (1_u64 << (idx.min(31) as u64)),
                    min_len: 1,
                    max_len: part.len() as u32,
                    required: true,
                })
                .collect::<Vec<_>>();
            let mut nodes = vec![AssemblyBodyNode::TypedBoundary {
                label: format!("open:{}", open as char),
                bytes: vec![open],
            }];
            let mut search_cursor = 0usize;
            for (idx, part) in parts.iter().enumerate() {
                let Some(relative_start) = inner[search_cursor..]
                    .windows(part.len())
                    .position(|window| window == *part)
                else {
                    continue;
                };
                let part_start = search_cursor + relative_start;
                push_bytes_as_assembly_nodes(&mut nodes, &inner[search_cursor..part_start]);
                let slot = &slots[idx];
                nodes.push(AssemblyBodyNode::SlotPlaceholder {
                    slot_index: slot.slot_index,
                    role_bits: slot.role_bits,
                    bytes: part.to_vec(),
                });
                search_cursor = part_start + part.len();
            }
            push_bytes_as_assembly_nodes(&mut nodes, &inner[search_cursor..]);
            nodes.push(AssemblyBodyNode::TypedBoundary {
                label: format!("close:{}", close as char),
                bytes: vec![close],
            });
            let role_signature = RoleSignature {
                role_bits: cue.role_bits,
                delimiter_role_bits: cue.delimiter_bits
                    | delimiter_bit(open)
                    | delimiter_bit(close),
                slot_role_bits: slots.iter().fold(0_u64, |acc, slot| acc | slot.role_bits),
            };
            out.push(build_assembly_candidate(
                AssemblyKind::SlotBearing,
                role_signature,
                slots,
                nodes,
                DependencyClosure::default(),
                cue,
                inner.len() as u32 + 2,
                inner.len() as u32 + 2,
                rolling_assembly_hash(inner) ^ ((open as u64) << 56),
            ));
            cursor = close_idx + 1;
            if out.len() >= 8 {
                break;
            }
        }
        if !out.is_empty() {
            break;
        }
    }
    out
}

fn push_bytes_as_assembly_nodes(nodes: &mut Vec<AssemblyBodyNode>, bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    if bytes.iter().all(|byte| is_delimiter(*byte)) {
        nodes.push(AssemblyBodyNode::DelimiterAnchor {
            bytes: bytes.to_vec(),
        });
    } else {
        nodes.push(AssemblyBodyNode::LiteralIsland {
            bytes: bytes.to_vec(),
        });
    }
}

fn delimiter_bits_for_bytes(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0_u64, |acc, byte| acc | delimiter_bit(*byte))
}

fn derive_assembly_support_votes(
    body: &AssemblyBody,
    role_signature: RoleSignature,
    slots: &[SlotDescriptor],
) -> Vec<AssemblySupportVote> {
    let mut votes = Vec::new();
    for node in &body.nodes {
        match node {
            AssemblyBodyNode::LiteralIsland { .. } => {}
            AssemblyBodyNode::DelimiterAnchor { bytes } => votes.push(AssemblySupportVote {
                role_bits: role_signature.role_bits,
                delimiter_bits: delimiter_bits_for_bytes(bytes),
                slot_bits: 0,
                overlap_bytes: bytes.len().min(u32::MAX as usize) as u32,
                support_bytes: bytes.len().min(u32::MAX as usize) as u32,
            }),
            AssemblyBodyNode::SubstrateSpan { reference } => votes.push(AssemblySupportVote {
                role_bits: role_signature.role_bits,
                delimiter_bits: role_signature.delimiter_role_bits,
                slot_bits: role_signature.slot_role_bits,
                overlap_bytes: reference.byte_len(),
                support_bytes: reference.byte_len(),
            }),
            AssemblyBodyNode::SlotPlaceholder {
                slot_index,
                role_bits,
                bytes,
            } => {
                let slot_bits = slots
                    .iter()
                    .find(|slot| slot.slot_index == *slot_index)
                    .map(|slot| slot.role_bits)
                    .unwrap_or(*role_bits);
                votes.push(AssemblySupportVote {
                    role_bits: *role_bits,
                    delimiter_bits: role_signature.delimiter_role_bits,
                    slot_bits,
                    overlap_bytes: bytes.len().min(u32::MAX as usize) as u32,
                    support_bytes: bytes.len().min(u32::MAX as usize) as u32,
                });
            }
            AssemblyBodyNode::MotifLink { bytes, .. } => votes.push(AssemblySupportVote {
                role_bits: role_signature.role_bits,
                delimiter_bits: role_signature.delimiter_role_bits,
                slot_bits: role_signature.slot_role_bits,
                overlap_bytes: bytes.len().min(u32::MAX as usize) as u32,
                support_bytes: bytes.len().min(u32::MAX as usize) as u32,
            }),
            AssemblyBodyNode::TypedBoundary { bytes, .. } => votes.push(AssemblySupportVote {
                role_bits: role_signature.role_bits,
                delimiter_bits: delimiter_bits_for_bytes(bytes),
                slot_bits: role_signature.slot_role_bits,
                overlap_bytes: bytes.len().min(u32::MAX as usize) as u32,
                support_bytes: bytes.len().min(u32::MAX as usize) as u32,
            }),
        }
    }
    votes
}

fn summarize_assembly_agreement(
    body: &AssemblyBody,
    slots: &[SlotDescriptor],
    dependencies: &[ObjectDependency],
    support_votes: &[AssemblySupportVote],
) -> AssemblyAgreementSummary {
    let literal_burden = body.literal_len();
    let support_count = support_votes.len().min(u32::MAX as usize) as u32;
    let support_bytes: u32 = support_votes.iter().map(|vote| vote.support_bytes).sum();
    let overlap_bytes: u32 = support_votes.iter().map(|vote| vote.overlap_bytes).sum();
    let optional_slot_penalty = slots
        .iter()
        .filter(|slot| !slot.required)
        .count()
        .min(u32::MAX as usize) as u32
        * 4;
    let motif_penalty = body
        .nodes
        .iter()
        .filter(|node| matches!(node, AssemblyBodyNode::MotifLink { .. }))
        .count()
        .saturating_sub(1)
        .min(u32::MAX as usize) as u32
        * 3;
    let residual_penalty = if body.output_len() == 0 {
        0
    } else {
        literal_burden.saturating_mul(32) / body.output_len().max(1)
    };
    let ambiguity = optional_slot_penalty
        .saturating_add(motif_penalty)
        .saturating_add(residual_penalty);
    let decode_burden = body
        .nodes
        .iter()
        .filter(|node| {
            !matches!(
                node,
                AssemblyBodyNode::LiteralIsland { .. } | AssemblyBodyNode::DelimiterAnchor { .. }
            )
        })
        .count()
        .min(u32::MAX as usize) as u32;
    let synchronization_cost = (dependencies.len().min(u32::MAX as usize) as u32)
        .saturating_mul(6)
        .saturating_add(body.dependency_component_count().min(u32::MAX as usize) as u32 * 2);
    let support = support_bytes.saturating_add(support_count.saturating_mul(4));
    let estimated_wire_cost = literal_burden
        .saturating_add(decode_burden)
        .saturating_add(synchronization_cost)
        .saturating_add(
            body.nodes
                .iter()
                .filter(|node| matches!(node, AssemblyBodyNode::DelimiterAnchor { .. }))
                .count()
                .min(u32::MAX as usize) as u32,
        );
    let agreement_score = support
        .saturating_add(overlap_bytes / 2)
        .saturating_sub(ambiguity)
        .saturating_sub(literal_burden / 2)
        .saturating_sub(synchronization_cost / 2)
        .max(1);

    AssemblyAgreementSummary {
        agreement_score,
        literal_burden,
        ambiguity,
        support,
        support_count,
        estimated_wire_cost,
        decode_burden,
        synchronization_cost,
    }
}

fn build_assembly_candidate(
    assembly_kind: AssemblyKind,
    role_signature: RoleSignature,
    slots: Vec<SlotDescriptor>,
    nodes: Vec<AssemblyBodyNode>,
    dependency_closure: DependencyClosure,
    cue: SparseCue,
    canonical_length_min: u32,
    canonical_length_max: u32,
    structural_hash: u64,
) -> AssemblyExtractionCandidate {
    let body = AssemblyBody { nodes };
    let support_votes = derive_assembly_support_votes(&body, role_signature, &slots);
    let agreement = summarize_assembly_agreement(
        &body,
        &slots,
        &dependency_closure.dependencies,
        &support_votes,
    );
    AssemblyExtractionCandidate {
        assembly_kind,
        role_signature,
        slots,
        body,
        dependency_closure,
        cue,
        canonical_length_min,
        canonical_length_max,
        structural_hash,
        support_votes,
        agreement,
    }
}

pub fn assembly_dependency_fingerprint(dependencies: &[ObjectDependency]) -> u64 {
    let mut sorted = dependencies.to_vec();
    sorted.sort_by(|left, right| {
        (left.object_kind as u8)
            .cmp(&(right.object_kind as u8))
            .then_with(|| left.object_id.cmp(&right.object_id))
            .then_with(|| left.required_revision.cmp(&right.required_revision))
    });
    let mut material = Vec::with_capacity(sorted.len() * 24);
    for dependency in sorted {
        material.push(dependency.object_kind as u8);
        material.extend_from_slice(&dependency.required_revision.to_le_bytes());
        material.extend_from_slice(dependency.object_id.as_bytes());
        material.push(0xff);
    }
    gf2_fingerprint(&material)
}

pub fn assembly_reuse_signature_from_parts(
    version: ObjectVersion,
    body: &AssemblyBody,
    dependencies: &[ObjectDependency],
) -> AssemblyReuseSignature {
    AssemblyReuseSignature {
        version,
        body_shape_hash: body.body_shape_hash(),
        dependency_fingerprint: assembly_dependency_fingerprint(dependencies),
        output_len: body.output_len(),
    }
}

pub fn assembly_reuse_signature(assembly: &Assembly) -> AssemblyReuseSignature {
    assembly_reuse_signature_from_parts(
        assembly.dependency_closure.version,
        &assembly.body,
        &assembly.dependency_closure.dependencies,
    )
}

pub fn derive_sparse_cue_from_bytes(source_kind: SourceKind, bytes: &[u8]) -> SparseCue {
    let mut family_bits = source_kind.cue_family().plane as u64;
    family_bits |= source_kind.cue_family().route_family as u64;
    let mut role_bits = 0_u64;
    let mut delimiter_bits = 0_u64;
    for &byte in bytes.iter().take(64) {
        if byte.is_ascii_alphabetic() {
            role_bits |= 1 << 0;
        }
        if byte.is_ascii_digit() {
            role_bits |= 1 << 1;
        }
        if byte.is_ascii_whitespace() {
            role_bits |= 1 << 2;
        }
        if byte == b'_' || byte == b'-' {
            role_bits |= 1 << 3;
        }
        delimiter_bits |= delimiter_bit(byte);
    }
    let len_bucket_bits = if bytes.is_empty() {
        0
    } else {
        1_u64 << length_bucket(bytes.len())
    };
    let temporal_bits = rolling_assembly_hash(bytes) & 0xffff;
    SparseCue::new(
        family_bits,
        role_bits,
        delimiter_bits,
        len_bucket_bits,
        temporal_bits,
    )
}

fn rolling_assembly_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for &byte in bytes.iter().take(256) {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3_u64);
    }
    hash
}

fn length_bucket(len: usize) -> u32 {
    match len {
        0 => 0,
        1..=8 => 1,
        9..=16 => 2,
        17..=32 => 3,
        33..=64 => 4,
        65..=128 => 5,
        _ => 6,
    }
}

fn delimiter_bit(byte: u8) -> u64 {
    match byte {
        b'\n' => 1 << 0,
        b'\r' => 1 << 1,
        b'\t' => 1 << 2,
        b',' => 1 << 3,
        b';' => 1 << 4,
        b':' => 1 << 5,
        b'|' => 1 << 6,
        b'{' | b'}' => 1 << 7,
        b'[' | b']' => 1 << 8,
        b'(' | b')' => 1 << 9,
        b'"' | b'\'' => 1 << 10,
        _ => 0,
    }
}

fn is_delimiter(byte: u8) -> bool {
    delimiter_bit(byte) != 0
}

fn matching_close(open: u8) -> u8 {
    match open {
        b'{' => b'}',
        b'[' => b']',
        b'(' => b')',
        b'<' => b'>',
        _ => open,
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct DynamicFamilyId(pub u32);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct DynamicSubfamilyId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum OntologyOperationKind {
    Merge = 1,
    Split = 2,
    Relabel = 3,
    Promote = 4,
    #[default]
    Retire = 5,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DynamicFamilyAssignment {
    pub family_id: DynamicFamilyId,
    pub subfamily_id: Option<DynamicSubfamilyId>,
    pub label: String,
    pub source_kind: Option<SourceKind>,
    pub plane: MemoryPlane,
    pub object_kinds: Vec<ObjectKind>,
    pub lifecycle: ObjectLifecycleMeta,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OntologyOperation {
    pub kind: OntologyOperationKind,
    pub plane: MemoryPlane,
    pub target_family: DynamicFamilyId,
    pub related_families: Vec<DynamicFamilyId>,
    pub relabel_to: Option<String>,
    pub promote_to: Option<PromotionLevel>,
    pub retire: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OntologyState {
    pub families: BTreeMap<DynamicFamilyId, DynamicFamilyAssignment>,
    pub history: Vec<OntologyOperation>,
}

impl OntologyState {
    pub fn apply(&mut self, op: OntologyOperation) {
        match op.kind {
            OntologyOperationKind::Merge => {
                self.merge_families(op.target_family, &op.related_families);
            }
            OntologyOperationKind::Split => {}
            OntologyOperationKind::Relabel => {
                if let Some(label) = op.relabel_to.as_ref() {
                    self.relabel_family(op.target_family, label);
                }
            }
            OntologyOperationKind::Promote => {
                if let Some(level) = op.promote_to {
                    self.promote_family(op.target_family, level);
                }
            }
            OntologyOperationKind::Retire => {
                self.retire_family(op.target_family);
            }
        }
        if op.retire && op.kind != OntologyOperationKind::Retire {
            self.retire_family(op.target_family);
        }
        self.history.push(op);
    }

    fn merge_families(
        &mut self,
        target_family: DynamicFamilyId,
        related_families: &[DynamicFamilyId],
    ) {
        let mut related_assignments = Vec::new();
        for related in related_families.iter().copied() {
            if related == target_family {
                continue;
            }
            if let Some(other) = self.families.remove(&related) {
                related_assignments.push(other);
            }
        }
        if related_assignments.is_empty() {
            return;
        }

        let mut target = self.families.remove(&target_family).unwrap_or_else(|| {
            let mut derived = related_assignments.remove(0);
            derived.family_id = target_family;
            derived
        });
        for other in related_assignments {
            absorb_family_assignment(&mut target, other);
        }
        self.families.insert(target_family, target);
    }

    fn relabel_family(&mut self, target_family: DynamicFamilyId, label: &str) {
        if let Some(target) = self.families.get_mut(&target_family) {
            target.label.clear();
            target.label.push_str(label);
        }
    }

    fn promote_family(&mut self, target_family: DynamicFamilyId, level: PromotionLevel) {
        if let Some(target) = self.families.get_mut(&target_family) {
            target.lifecycle.promotion_level = level;
        }
    }

    fn retire_family(&mut self, target_family: DynamicFamilyId) {
        self.families.remove(&target_family);
    }
}

fn absorb_family_assignment(
    target: &mut DynamicFamilyAssignment,
    mut other: DynamicFamilyAssignment,
) {
    target.lifecycle.support_count = target
        .lifecycle
        .support_count
        .saturating_add(other.lifecycle.support_count);
    target.lifecycle.success_count = target
        .lifecycle
        .success_count
        .saturating_add(other.lifecycle.success_count);
    target.lifecycle.failure_count = target
        .lifecycle
        .failure_count
        .saturating_add(other.lifecycle.failure_count);
    target.lifecycle.salience = target.lifecycle.salience.max(other.lifecycle.salience);
    target.lifecycle.last_seen_tick = target
        .lifecycle
        .last_seen_tick
        .max(other.lifecycle.last_seen_tick);
    target.lifecycle.creation_tick = if target.lifecycle.creation_tick == 0 {
        other.lifecycle.creation_tick
    } else if other.lifecycle.creation_tick == 0 {
        target.lifecycle.creation_tick
    } else {
        target
            .lifecycle
            .creation_tick
            .min(other.lifecycle.creation_tick)
    };
    if target.source_kind.is_none() {
        target.source_kind = other.source_kind.take();
    }
    for kind in other.object_kinds.drain(..) {
        if !target.object_kinds.contains(&kind) {
            target.object_kinds.push(kind);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum ConsolidationJobKind {
    AssemblyMerge = 1,
    AssemblySplit = 2,
    TransformPromotion = 3,
    EpisodeMacroExtraction = 4,
    SchemaPromotion = 5,
    #[default]
    ObjectRetirement = 6,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConsolidationJob {
    pub job_id: u64,
    pub kind: ConsolidationJobKind,
    pub plane: MemoryPlane,
    pub source_kind: Option<SourceKind>,
    pub primary_object_id: String,
    pub related_object_ids: Vec<String>,
    pub lifecycle: ObjectLifecycleMeta,
    pub support: u32,
    pub ambiguity: u32,
    pub savings: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConsolidationQueue {
    pub pending: Vec<ConsolidationJob>,
}

impl ConsolidationQueue {
    pub fn push(&mut self, job: ConsolidationJob) {
        self.pending.push(job);
        self.pending.sort_by(|l, r| {
            r.savings
                .cmp(&l.savings)
                .then_with(|| r.support.cmp(&l.support))
                .then_with(|| l.ambiguity.cmp(&r.ambiguity))
                .then_with(|| l.job_id.cmp(&r.job_id))
        });
    }

    pub fn drain_ready(&mut self, min_support: u32, max_ambiguity: u32) -> Vec<ConsolidationJob> {
        let mut ready = Vec::new();
        let mut retained = Vec::new();
        for job in self.pending.drain(..) {
            if job.support >= min_support && job.ambiguity <= max_ambiguity {
                ready.push(job);
            } else {
                retained.push(job);
            }
        }
        self.pending = retained;
        ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(
        tick: u64,
        object_kind: ObjectKind,
        object_id: &str,
        bytes: &[u8],
    ) -> EpisodeActivationEvent {
        let source_kind = SourceKind::Text;
        let cue = crate::derive_sparse_cue(source_kind, bytes);
        let object_ref = EpisodeObjectRef {
            object_kind,
            object_id: object_id.to_string(),
        };
        let context_hash = derive_context_hash(cue, &object_ref);
        EpisodeActivationEvent {
            source_kind,
            object_ref,
            cue,
            context_hash,
            tick,
            success: true,
        }
    }

    #[test]
    fn sparse_episode_traces_apply_pattern_separation() {
        let mut memory = EpisodeMemory::default();
        let left = event(1, ObjectKind::ExactState, "item:left", b"alpha:one");
        let right = event(2, ObjectKind::ExactState, "item:right", b"alpha:two");
        memory.append_activation(left);
        memory.append_activation(right);

        assert_eq!(memory.hippocampal_shock.traces.len(), 2);
        assert_eq!(
            memory.hippocampal_shock.traces[0].family_partition,
            memory.hippocampal_shock.traces[1].family_partition
        );
        assert_ne!(
            memory.hippocampal_shock.traces[0].collision_slot,
            memory.hippocampal_shock.traces[1].collision_slot
        );
    }

    #[test]
    fn episode_completion_uses_trace_overlap_route_support_and_recency() {
        let mut memory = EpisodeMemory::default();
        let seed = event(1, ObjectKind::ExactState, "item:seed", b"episode-seed");
        let target = event(4, ObjectKind::Assembly, "assembly:9", b"episode-target");
        memory.append_activation(seed);
        memory.append_activation(target.clone());
        memory.record_route_outcome(
            ControllerRouteFamily::EpisodeCompletion,
            Some(target.source_kind),
            Some(target.context_hash),
            4,
            true,
        );
        memory.append_activation(event(
            5,
            ObjectKind::ExactState,
            "item:seed-2",
            b"episode-seed",
        ));

        let candidates = generate_episode_completion_candidates(
            &memory,
            EpisodeCandidatePolicy::bounded_default(),
        );
        assert!(!candidates.is_empty());
        assert_eq!(candidates[0].object_ref.object_id, "assembly:9");
        assert!(candidates[0].cue_overlap > 0);
        assert!(candidates[0].route_support > 0);
        assert!(candidates[0].recency_score > 0);
    }
}
