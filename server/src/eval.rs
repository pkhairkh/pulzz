use std::{collections::HashMap, fs, path::Path, time::Instant};

use client::ClientSession;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use shared_protocol::{
    CodecMode, ItemId, SourceKind, StreamDirection, StreamId, classic_ref1_pair_from_rng,
    prepare_binary_source, prepare_json_source, prepare_text_source,
};
use thiserror::Error;

use crate::{ServerEvent, ServerSession};

const DEFAULT_EVENT_COUNT: usize = 160;
const DEFAULT_SECURITY_PROFILE: &str = "classic_ref1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationPreset {
    Default,
    ExactByteBaseline,
}

impl EvaluationPreset {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::ExactByteBaseline => "exact_byte_baseline",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Default => "predictive-memory exact-state evaluation",
            Self::ExactByteBaseline => "exact-state baseline preset for direct comparison",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadKind {
    HighLocality,
    MixedLocality,
    LowLocality,
}

impl WorkloadKind {
    pub fn slug(self) -> &'static str {
        match self {
            Self::HighLocality => "high_locality",
            Self::MixedLocality => "mixed_locality",
            Self::LowLocality => "low_locality",
        }
    }

    fn insert_threshold(self) -> u8 {
        match self {
            Self::HighLocality => 15,
            Self::MixedLocality => 35,
            Self::LowLocality => 55,
        }
    }

    fn upsert_threshold(self) -> u8 {
        match self {
            Self::HighLocality => 85,
            Self::MixedLocality => 80,
            Self::LowLocality => 70,
        }
    }

    fn evict_threshold(self) -> u8 {
        match self {
            Self::HighLocality => 95,
            Self::MixedLocality => 92,
            Self::LowLocality => 88,
        }
    }

    fn hot_bias(self) -> f64 {
        match self {
            Self::HighLocality => 0.90,
            Self::MixedLocality => 0.65,
            Self::LowLocality => 0.20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceOp {
    Insert,
    UpsertObject,
    Evict,
    Invalidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub step: usize,
    pub workload: WorkloadKind,
    pub item_id: u64,
    pub op: TraceOp,
    pub source_kind: Option<SourceKind>,
    pub payload_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvaluationConfig {
    pub event_count_per_workload: usize,
    pub security_profile: &'static str,
    pub preset: EvaluationPreset,
}

impl Default for EvaluationConfig {
    fn default() -> Self {
        Self {
            event_count_per_workload: DEFAULT_EVENT_COUNT,
            security_profile: DEFAULT_SECURITY_PROFILE,
            preset: EvaluationPreset::Default,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CodecModeCounts {
    pub direct_exact: usize,
    pub packed_exact: usize,
    pub predicted_exact: usize,
    pub control: usize,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SourceKindCounts {
    pub text: usize,
    pub json: usize,
    pub binary: usize,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ByteMetrics {
    pub original_payload_bytes: usize,
    pub encoded_payload_bytes: usize,
    pub protected_wire_bytes: usize,
    pub payload_savings_vs_original_pct: f64,
    pub wire_savings_vs_original_pct: f64,
    pub wire_overhead_vs_encoded_pct: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ExactnessMetrics {
    pub checked_events: usize,
    pub exact_round_trips: usize,
    pub exact_round_trip_rate: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LatencyMetrics {
    pub encode_total_ns: u128,
    pub encode_avg_ns: f64,
    pub decode_total_ns: u128,
    pub decode_avg_ns: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PredictiveEvalMetrics {
    /// Telemetry only: offline share of emitted route families. This is not a planner input.
    pub route_family_share: HashMap<String, f64>,
    /// Telemetry only: count of server-side direct-state downgrades caused by dependency or sync risk.
    pub sync_risk_fallback_count: usize,
    /// Telemetry only: count of exact-atom plans that downgraded to direct exact-state emission.
    pub exact_atom_direct_state_fallback_count: usize,
    /// S2.5.b3: count of transform-path downgrades where the transform route was demoted.
    pub transform_demoted_fallback_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadReport {
    pub workload: WorkloadKind,
    pub seed: u64,
    pub event_count: usize,
    pub payload_event_count: usize,
    pub trace_path: String,
    pub codec_modes: CodecModeCounts,
    pub source_kinds: SourceKindCounts,
    pub bytes: ByteMetrics,
    pub exactness: ExactnessMetrics,
    pub latency: LatencyMetrics,
    pub predictive_memory: PredictiveEvalMetrics,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvaluationReport {
    pub config: EvaluationConfig,
    pub workloads: Vec<WorkloadReport>,
    pub divergences: Vec<String>,
}

#[derive(Debug, Error)]
pub enum EvalError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Server(#[from] crate::ServerError),
    #[error(transparent)]
    Client(#[from] client::ClientApplyError),
    #[error("exactness check failed for item {}", .item_id.0)]
    Exactness { item_id: ItemId },
}

pub fn run_default_evaluation(output_dir: impl AsRef<Path>) -> Result<EvaluationReport, EvalError> {
    run_evaluation(&EvaluationConfig::default(), output_dir)
}

pub fn run_evaluation(
    config: &EvaluationConfig,
    output_dir: impl AsRef<Path>,
) -> Result<EvaluationReport, EvalError> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;

    let workloads = [
        WorkloadKind::HighLocality,
        WorkloadKind::MixedLocality,
        WorkloadKind::LowLocality,
    ]
    .into_iter()
    .enumerate()
    .map(|(index, workload)| {
        let seed = 0x7000_0000_0000_0000_u64 ^ (index as u64 * 0x1f1f_0101);
        evaluate_workload(config, output_dir, workload, seed)
    })
    .collect::<Result<Vec<_>, _>>()?;

    let report = EvaluationReport {
        config: config.clone(),
        workloads,
        divergences: vec![
            "evaluation presets now converge on the same exact-state lane"
                .to_string(),
            "evaluation baselines now compare original payload bytes, encoded payload bytes, and protected wire bytes; predictive-memory route metrics replaced semantic vector utility metrics".to_string(),
        ],
    };

    fs::write(
        output_dir.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    fs::write(output_dir.join("summary.md"), render_summary(&report))?;
    Ok(report)
}

fn evaluate_workload(
    config: &EvaluationConfig,
    output_dir: &Path,
    workload: WorkloadKind,
    seed: u64,
) -> Result<WorkloadReport, EvalError> {
    let trace = generate_trace(workload, config.event_count_per_workload, seed);
    let trace_path = output_dir.join(format!("{}_trace.json", workload.slug()));
    fs::write(&trace_path, serde_json::to_vec_pretty(&trace)?)?;

    let mut rng = StdRng::seed_from_u64(seed ^ 0x9090_2026);
    let (sender, receiver) =
        classic_ref1_pair_from_rng(StreamId(seed), StreamDirection::ServerToClient, &mut rng);
    let mut server = ServerSession::new(sender);
    let mut client = ClientSession::new(receiver);
    let mut expected = HashMap::<ItemId, Vec<u8>>::new();

    let mut payload_event_count = 0_usize;
    let mut codec_modes = CodecModeCounts::default();
    let mut source_kinds = SourceKindCounts::default();
    let mut original_payload_bytes = 0_usize;
    let mut encoded_payload_bytes = 0_usize;
    let mut protected_wire_bytes = 0_usize;
    let mut encode_total_ns = 0_u128;
    let mut decode_total_ns = 0_u128;
    let mut exact_checked = 0_usize;
    let mut exact_round_trips = 0_usize;

    for event in trace {
        let record = match event.op {
            TraceOp::Insert | TraceOp::UpsertObject => {
                let (kind, payload) = (
                    event.source_kind.expect("payload event kind"),
                    event.payload_bytes.expect("payload event bytes"),
                );
                let prepared = prepared_source_for(kind, &payload, event.item_id);
                let block = shared_protocol::ExactStateMaterial::copy_exact(
                    prepared.descriptor.kind,
                    &prepared.canonical_bytes,
                );
                original_payload_bytes += prepared.canonical_bytes.len();
                payload_event_count += 1;
                match kind {
                    SourceKind::Text => source_kinds.text += 1,
                    SourceKind::Json => source_kinds.json += 1,
                    SourceKind::Binary | SourceKind::Image => source_kinds.binary += 1,
                }

                let encode_start = Instant::now();
                let record = match event.op {
                    TraceOp::Insert => server.emit_event(ServerEvent::Insert {
                        item_id: ItemId(event.item_id),
                        block,
                    })?,
                    TraceOp::UpsertObject => server.emit_event(ServerEvent::UpsertObject {
                        item_id: ItemId(event.item_id),
                        block,
                    })?,
                    TraceOp::Evict | TraceOp::Invalidate => unreachable!(),
                };
                encode_total_ns += encode_start.elapsed().as_nanos();
                expected.insert(ItemId(event.item_id), prepared.canonical_bytes);
                record
            }
            TraceOp::Evict => {
                expected.remove(&ItemId(event.item_id));
                server.emit_event(ServerEvent::Evict {
                    item_id: ItemId(event.item_id),
                })?
            }
            TraceOp::Invalidate => {
                expected.remove(&ItemId(event.item_id));
                server.emit_event(ServerEvent::Invalidate {
                    item_id: ItemId(event.item_id),
                })?
            }
        };

        encoded_payload_bytes += record.payload.len();
        protected_wire_bytes += record.to_bytes().len();
        classify_mode(&mut codec_modes, record.header.codec_mode);

        let decode_start = Instant::now();
        client.apply_protected_record(record.clone())?;
        decode_total_ns += decode_start.elapsed().as_nanos();

        if matches!(event.op, TraceOp::Insert | TraceOp::UpsertObject) {
            exact_checked += 1;
            let cache_entry =
                client
                    .state()
                    .cache_entry(ItemId(event.item_id))
                    .ok_or(EvalError::Exactness {
                        item_id: ItemId(event.item_id),
                    })?;
            let expected_bytes =
                expected
                    .get(&ItemId(event.item_id))
                    .ok_or(EvalError::Exactness {
                        item_id: ItemId(event.item_id),
                    })?;
            if cache_entry.object.exact_bytes == *expected_bytes {
                exact_round_trips += 1;
            } else {
                return Err(EvalError::Exactness {
                    item_id: ItemId(event.item_id),
                });
            }
        }
    }

    let bytes = summarize_bytes(
        original_payload_bytes,
        encoded_payload_bytes,
        protected_wire_bytes,
    );
    let latency = LatencyMetrics {
        encode_total_ns,
        encode_avg_ns: average_ns(
            encode_total_ns,
            payload_event_count + codec_modes.control as usize,
        ),
        decode_total_ns,
        decode_avg_ns: average_ns(
            decode_total_ns,
            payload_event_count + codec_modes.control as usize,
        ),
    };
    let exactness = ExactnessMetrics {
        checked_events: exact_checked,
        exact_round_trips,
        exact_round_trip_rate: if exact_checked == 0 {
            1.0
        } else {
            exact_round_trips as f64 / exact_checked as f64
        },
    };
    let route_stats = server.state().route_statistics().collect::<Vec<_>>();
    let total_route_events = route_stats
        .iter()
        .map(|stat| u64::from(stat.success_count + stat.failure_count))
        .sum::<u64>()
        .max(1) as f64;
    let mut route_family_share = HashMap::new();
    for family in [
        shared_protocol::ControllerRouteFamily::DirectState,
        shared_protocol::ControllerRouteFamily::ExactAtom,
        shared_protocol::ControllerRouteFamily::Assembly,
        shared_protocol::ControllerRouteFamily::Transform,
        shared_protocol::ControllerRouteFamily::EpisodeCompletion,
        shared_protocol::ControllerRouteFamily::SchemaExpansion,
        shared_protocol::ControllerRouteFamily::Hybrid,
    ] {
        let family_events = route_stats
            .iter()
            .filter(|stat| stat.route_family == family)
            .map(|stat| u64::from(stat.success_count + stat.failure_count))
            .sum::<u64>() as f64;
        if family_events > 0.0 {
            route_family_share.insert(
                format!("{}", family as u8),
                family_events / total_route_events,
            );
        }
    }
    let fallback_metrics = server.state().fallback_metrics();
    let predictive_memory = PredictiveEvalMetrics {
        route_family_share,
        sync_risk_fallback_count: 0,
        exact_atom_direct_state_fallback_count: fallback_metrics
            .direct_state_downgrades
            .get(&format!(
                "{}",
                shared_protocol::ControllerRouteFamily::ExactAtom as u8
            ))
            .copied()
            .unwrap_or(0) as usize,
        transform_demoted_fallback_count: fallback_metrics.transform_demoted_downgrades as usize,
    };

    Ok(WorkloadReport {
        workload,
        seed,
        event_count: config.event_count_per_workload,
        payload_event_count,
        trace_path: trace_path.display().to_string(),
        codec_modes,
        source_kinds,
        bytes,
        exactness,
        latency,
        predictive_memory,
    })
}

fn summarize_bytes(
    original_payload_bytes: usize,
    encoded_payload_bytes: usize,
    protected_wire_bytes: usize,
) -> ByteMetrics {
    let original = original_payload_bytes.max(1) as f64;
    let encoded = encoded_payload_bytes.max(1) as f64;
    ByteMetrics {
        original_payload_bytes,
        encoded_payload_bytes,
        protected_wire_bytes,
        payload_savings_vs_original_pct: 100.0 * (1.0 - encoded_payload_bytes as f64 / original),
        wire_savings_vs_original_pct: 100.0 * (1.0 - protected_wire_bytes as f64 / original),
        wire_overhead_vs_encoded_pct: 100.0 * (protected_wire_bytes as f64 / encoded - 1.0),
    }
}

fn average_ns(total_ns: u128, samples: usize) -> f64 {
    if samples == 0 {
        0.0
    } else {
        total_ns as f64 / samples as f64
    }
}

fn classify_mode(counts: &mut CodecModeCounts, mode: CodecMode) {
    match mode {
        CodecMode::DirectExact => counts.direct_exact += 1,
        CodecMode::PackedExact => counts.packed_exact += 1,
        CodecMode::PredictedExact => counts.predicted_exact += 1,
        CodecMode::None => counts.control += 1,
    }
}

fn prepared_source_for(
    kind: SourceKind,
    payload: &[u8],
    item_id: u64,
) -> shared_protocol::PreparedSource {
    match kind {
        SourceKind::Text => prepare_text_source(
            std::str::from_utf8(payload).unwrap_or_default(),
            Some(format!("item-{item_id}.txt")),
        ),
        SourceKind::Json => prepare_json_source(
            std::str::from_utf8(payload).unwrap_or("{}"),
            Some(format!("item-{item_id}.json")),
        ),
        SourceKind::Binary | SourceKind::Image => prepare_binary_source(
            Some(format!("item-{item_id}.bin")),
            Some("application/octet-stream"),
            payload,
        ),
    }
}

fn generate_trace(workload: WorkloadKind, count: usize, seed: u64) -> Vec<TraceEvent> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut live = Vec::<u64>::new();
    let mut next_item_id = 1_u64;
    let mut trace = Vec::with_capacity(count);

    for step in 0..count {
        let roll = rng.gen_range(0_u8..100);
        let op = if live.is_empty() || roll < workload.insert_threshold() {
            TraceOp::Insert
        } else if roll < workload.upsert_threshold() {
            TraceOp::UpsertObject
        } else if roll < workload.evict_threshold() {
            TraceOp::Evict
        } else {
            TraceOp::Invalidate
        };

        let item_id = match op {
            TraceOp::Insert => {
                let new_id = next_item_id;
                next_item_id += 1;
                live.push(new_id);
                new_id
            }
            TraceOp::UpsertObject | TraceOp::Evict | TraceOp::Invalidate => {
                let hot_roll = rng.gen_bool(workload.hot_bias());
                if hot_roll {
                    *live.last().unwrap_or(&1)
                } else {
                    *live.get(rng.gen_range(0..live.len())).unwrap_or(&1)
                }
            }
        };

        let (source_kind, payload_bytes) = match op {
            TraceOp::Insert | TraceOp::UpsertObject => {
                let source_kind = match step % 3 {
                    0 => SourceKind::Text,
                    1 => SourceKind::Json,
                    _ => SourceKind::Binary,
                };
                let payload = build_payload(workload, source_kind, item_id, step, &mut rng);
                (Some(source_kind), Some(payload))
            }
            TraceOp::Evict | TraceOp::Invalidate => {
                live.retain(|candidate| *candidate != item_id);
                (None, None)
            }
        };

        trace.push(TraceEvent {
            step,
            workload,
            item_id,
            op,
            source_kind,
            payload_bytes,
        });
    }

    trace
}

fn build_payload(
    workload: WorkloadKind,
    source_kind: SourceKind,
    item_id: u64,
    step: usize,
    rng: &mut StdRng,
) -> Vec<u8> {
    let locality_hint = match workload {
        WorkloadKind::HighLocality => "cache-hot",
        WorkloadKind::MixedLocality => "cache-mixed",
        WorkloadKind::LowLocality => "cache-cold",
    };
    match source_kind {
        SourceKind::Text => format!(
            "{locality_hint} item {item_id} revision {step} repeated content repeated content {}",
            step % 7
        )
        .into_bytes(),
        SourceKind::Json => format!(
            "{{\"item\":{item_id},\"step\":{step},\"locality\":\"{locality_hint}\",\"bucket\":{},\"stable\":\"{}\"}}",
            step % 11,
            if step % 2 == 0 { "even" } else { "odd" }
        )
        .into_bytes(),
        SourceKind::Binary | SourceKind::Image => {
            let len = 96 + (step % 96);
            let mut out = vec![0_u8; len];
            for (index, slot) in out.iter_mut().enumerate() {
                *slot = ((item_id as usize + step + index) % 251) as u8 ^ rng.gen_range(0..4);
            }
            out
        }
    }
}

fn render_summary(report: &EvaluationReport) -> String {
    let mut out = String::new();
    out.push_str("# Exact-Byte Evaluation Summary\n\n");
    out.push_str(&format!(
        "- security profile: `{}`\n- preset: `{}`\n- description: `{}`\n- events per workload: `{}`\n\n",
        report.config.security_profile,
        report.config.preset.slug(),
        report.config.preset.description(),
        report.config.event_count_per_workload,
    ));

    for workload in &report.workloads {
        out.push_str(&format!("## {}\n\n", workload.workload.slug()));
        out.push_str(&format!(
            "- payload events: `{}`\n- source kinds: text=`{}`, json=`{}`, binary=`{}`\n- codec modes: direct_exact=`{}`, packed_exact=`{}`, predicted_exact=`{}`, control=`{}`\n",
            workload.payload_event_count,
            workload.source_kinds.text,
            workload.source_kinds.json,
            workload.source_kinds.binary,
            workload.codec_modes.direct_exact,
            workload.codec_modes.packed_exact,
            workload.codec_modes.predicted_exact,
            workload.codec_modes.control,
        ));
        out.push_str(&format!(
            "- original bytes: `{}`\n- encoded payload bytes: `{}`\n- protected wire bytes: `{}`\n",
            workload.bytes.original_payload_bytes,
            workload.bytes.encoded_payload_bytes,
            workload.bytes.protected_wire_bytes,
        ));
        out.push_str(&format!(
            "- payload savings vs original: `{:.4}%`\n- wire savings vs original: `{:.4}%`\n- wire overhead vs encoded: `{:.4}%`\n",
            workload.bytes.payload_savings_vs_original_pct,
            workload.bytes.wire_savings_vs_original_pct,
            workload.bytes.wire_overhead_vs_encoded_pct,
        ));
        out.push_str(&format!(
            "- exact round-trip rate: `{:.6}`\n- avg encode ns: `{:.2}`\n- avg decode ns: `{:.2}`\n\n",
            workload.exactness.exact_round_trip_rate,
            workload.latency.encode_avg_ns,
            workload.latency.decode_avg_ns,
        ));
    }

    out.push_str("## Divergences\n\n");
    for divergence in &report.divergences {
        out.push_str(&format!("- {divergence}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traces_are_reproducible() {
        let left = generate_trace(WorkloadKind::HighLocality, 32, 17);
        let right = generate_trace(WorkloadKind::HighLocality, 32, 17);
        assert_eq!(
            serde_json::to_string(&left).unwrap(),
            serde_json::to_string(&right).unwrap()
        );
    }

    #[test]
    fn byte_metrics_report_savings_consistently() {
        let metrics = summarize_bytes(100, 60, 72);
        assert!((metrics.payload_savings_vs_original_pct - 40.0).abs() < 1e-6);
        assert!((metrics.wire_savings_vs_original_pct - 28.0).abs() < 1e-6);
        assert!((metrics.wire_overhead_vs_encoded_pct - 20.0).abs() < 1e-6);
    }
}
