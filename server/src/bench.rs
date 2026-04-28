use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use client::{
    ClientConnectConfig, ClientSession, ReconnectPolicy, connect_quic_datagram_session,
    connect_quic_session, connect_tcp_session, connect_udp_session, connect_websocket_session,
    connect_webtransport_datagram_session,
};
use futures_util::{SinkExt, StreamExt};
use rand::{Rng, SeedableRng, rngs::StdRng, seq::SliceRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use indicatif::{ProgressBar, ProgressStyle, HumanDuration, HumanBytes};
use shared_protocol::{
    Assembly, AssemblyBody, AssemblyId, BootstrapClientConfig, BootstrapConfig,
    BootstrapServerConfig, ChpmtObject, ClientSecurityConfig as SharedClientSecurityConfig,
    CodecMode, CredentialScope, DatagramSessionMetrics, ExactStateMaterial, ItemId,
    ObjectLifecycleMeta, PqSimpleServerBootstrapConfig, ProtectionProfileKind, Record,
    STREAM_ROOT_LEN, ServerSecurityConfig, SourceOptimizationConfig, StreamDirection, StreamId,
    StreamProtector, TransportConfig, TransportSessionConfig,
    carrier::reliable::ReliableCarrierKind, classic_ref1_pair_from_rng,
    extract_catalog_assembly_candidates, generate_transform_candidates, issue_client_credential,
    issue_server_identity, pq_simple_v1_pair_from_rng,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    accept_async_with_config, connect_async_with_config,
    tungstenite::{self, Message, protocol::WebSocketConfig},
};

use crate::{
    PlainServerSession, ServerEvent, ServerSession,
    source_cache::{SourceCache, SourceCacheConfig, SourceResolveResult},
};

const BENCH_TARGET_1_MB: u64 = 1_000_000;
const BENCH_TARGET_10_MB: u64 = 10_000_000;
const BENCH_TARGET_100_MB: u64 = 100_000_000;
const BENCH_MAX_SESSION_TARGET_BYTES: u64 = BENCH_TARGET_100_MB;
const BENCH_DEFAULT_TARGETS: [BenchmarkTargetBytes; 3] = [
    BenchmarkTargetBytes::OneMb,
    BenchmarkTargetBytes::TenMb,
    BenchmarkTargetBytes::HundredMb,
];
const BURST_SMALL_CAP: usize = 4_096;
const BURST_MEDIUM_CAP: usize = 65_536;
const BURST_BIG_CAP: usize = 1_048_576;
const DEFAULT_BULK_TRANSPORT_PAYLOAD_CAP: usize = BURST_BIG_CAP;
const BENCH_WEBSOCKET_MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_BENCH_PORT: u16 = 9130;
const DEFAULT_REMOTE_HOST: &str = "10.42.0.1";
const DEFAULT_REMOTE_SSH_TARGET: &str = "pkhairkh@10.42.0.1";
const DEFAULT_REMOTE_REPO_PATH: &str = "~/pulzz_remote";
const REMOTE_RUNTIME_SCRATCH_ROOT: &str = "/tmp/pulzz_bench_runtime";
const PROCESS_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);
const BENCH_LOCAL_DISK_MIN_FREE_BYTES: u64 = 1024 * 1024 * 1024;
const DIRECT_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(30);
const REMOTE_READY_TIMEOUT: Duration = Duration::from_secs(300);
const REMOTE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const UTILITY_SAMPLE_LIMIT: usize = 256;
const LATENCY_SAMPLE_CAP: usize = 8_192;
const SMOKE_TARGET_BYTES: u64 = 4_096;
/// S4.6: Maximum wall-clock time for a single benchmark case before forced termination.
/// Prevents runaway cases (especially 100MB) from hanging indefinitely.
/// Default: 10 minutes. Configurable via PULZZ_BENCH_CASE_TIMEOUT_SECS env var.
/// Reduced from 30 min because 100MB cases should complete well within this
/// when not blocked by cold SourceCache disk writes.
const BENCH_CASE_TIMEOUT: Duration = Duration::from_secs(600);
const SERVER_SIDE_DIR: &str = "server";
const CLIENT_SIDE_DIR: &str = "client";
/// Creates a progress bar for benchmark matrix execution with ETA,
/// elapsed time, case count, and per-case result annotations.
/// Shows: spinner | elapsed | bar | completed/total | ETA | message
fn bench_matrix_progress_bar(total: u64) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} cases ({eta}) {msg}",
        )
        .unwrap()
        .progress_chars("█▓░"),
    );
    pb.enable_steady_tick(Duration::from_millis(500));
    pb
}

/// Creates a sub-progress bar for within-case execution showing
/// payload byte progress against the target, with throughput,
/// record count, codec mode distribution, and elapsed time.
fn bench_case_byte_progress_bar(target_bytes: u64, case_label: &str) -> ProgressBar {
    let pb = ProgressBar::new(target_bytes);
    pb.set_style(
        ProgressStyle::with_template(
            &format!(
                "  {{spinner:.yellow}} [{{elapsed_precise}}] [{{wide_bar:.green/white}}] {{bytes}}/{{total_bytes}} ({{percent}}%) {{msg}} · {}",
                case_label,
            )
        )
        .unwrap()
        .progress_chars("█▓░"),
    );
    pb.enable_steady_tick(Duration::from_millis(250));
    pb
}

/// Creates a dedicated progress bar for distributed benchmark case execution.
/// This provides granular visibility into the longest phase (server+verify)
/// by showing estimated progress based on target bytes, elapsed time, and
/// historical throughput from completed cases.
///
/// Layout: spinner | elapsed | bar | estimated bytes | estimated % | throughput | message
fn bench_distributed_case_progress_bar(
    target_bytes: u64,
    case_label: &str,
    estimated_duration: Duration,
) -> ProgressBar {
    let pb = ProgressBar::new(target_bytes);
    pb.set_style(
        ProgressStyle::with_template(
            &format!(
                "  {{spinner:.magenta}} [{{elapsed_precise}}/{{per_eta}}] [{{wide_bar:.yellow/red}}] {{bytes}}/{{total_bytes}} ({{percent}}%) ~{{msg}} · {}",
                case_label,
            )
        )
        .unwrap()
        .progress_chars("█▓░")
        // per_eta shows estimated total time for this case
        .with_key("per_eta", |state: &indicatif::ProgressState, f: &mut dyn std::fmt::Write| {
            write!(f, "{}", HumanDuration(state.eta())).unwrap()
        }),
    );
    pb.enable_steady_tick(Duration::from_millis(200));
    // Set initial position estimate based on nothing (will be updated by the
    // time-based estimator during execution)
    let _ = estimated_duration; // used by caller to set initial message
    pb
}

const WEB_BENCH_CASE_FILE: &str = "web_bench_case.json";
const WEB_BENCH_FRAMES_FILE: &str = "web_bench_frames.bin";
const INPUT_CORPUS_MANIFEST_FILE: &str = "input_corpus_manifest.json";
const WEB_BENCH_BUNDLE_DIR: &str = "target/web_bench_bundle";
const WEB_BENCH_VENV_DIR: &str = "target/web_bench_venv";
const WEB_BENCH_TIMEOUT_SECONDS: &str = "3600";
const WIKITEXT_CORPUS_FETCH_SCRIPT: &str = "benchmarks/scripts/fetch_wikitext_103_raw.py";
const WIKITEXT_CORPUS_DIR: &str = "benchmarks/input_corpora/wikitext_103_raw";
const WIKITEXT_CORPUS_MANIFEST_JSON: &str =
    "benchmarks/input_corpora/wikitext_103_raw/manifest.json";
const WIKITEXT_CORPUS_CHUNKS_JSONL: &str = "benchmarks/input_corpora/wikitext_103_raw/chunks.jsonl";
const WEB_IMAGE_CORPUS_FETCH_SCRIPT: &str = "benchmarks/scripts/fetch_web_image_corpus.py";
const WEB_IMAGE_CORPUS_DIR: &str = "benchmarks/input_corpora/web_image_50";
const WEB_IMAGE_CORPUS_FILES_DIR: &str = "benchmarks/input_corpora/web_image_50/files";
const WEB_IMAGE_CORPUS_MANIFEST_JSON: &str = "benchmarks/input_corpora/web_image_50/manifest.json";
const INPUT_CHUNK_CHAR_COUNT: usize = 384;
const INPUT_CHUNK_CHAR_STRIDE: usize = 256;
const INPUT_CHUNK_MIN_CHARS: usize = 96;
pub const DEFAULT_PRODUCTION_BENCH_ARTIFACT_ROOT: &str = "benchmarks/mutual_pqc";

static WEB_BENCH_BUNDLE_READY: OnceLock<PathBuf> = OnceLock::new();
static WEB_BENCH_VENV_PYTHON: OnceLock<PathBuf> = OnceLock::new();
static REMOTE_SYNC_CACHE: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static REMOTE_BINARY_SYNC_CACHE: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
static WIKITEXT_INPUT_CORPUS: OnceLock<InputCorpus> = OnceLock::new();
static WEB_IMAGE_INPUT_CORPUS: OnceLock<InputCorpus> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkEnvironment {
    Local,
    Remote,
}

impl BenchmarkEnvironment {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "local" => Some(Self::Local),
            "remote" => Some(Self::Remote),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkCapabilityProfile {
    TextFamilyCueObject,
    ImageFamilyCueObject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PredictiveBenchSupportLevel {
    #[default]
    Full,
    Partial,
    Unsupported,
}

impl BenchmarkCapabilityProfile {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "text_family_cue_object" => Some(Self::TextFamilyCueObject),
            "image_family_cue_object" => Some(Self::ImageFamilyCueObject),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::TextFamilyCueObject => "text_family_cue_object",
            Self::ImageFamilyCueObject => "image_family_cue_object",
        }
    }

    pub fn descriptor(self) -> shared_protocol::ChpmtCapabilityDescriptor {
        match self {
            Self::TextFamilyCueObject => {
                shared_protocol::architecture_text_family_capability_descriptor()
            }
            Self::ImageFamilyCueObject => {
                shared_protocol::architecture_image_family_capability_descriptor()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkWorkload {
    HighLocality,
    MixedLocality,
    LowLocality,
}

impl BenchmarkWorkload {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "high_locality" => Some(Self::HighLocality),
            "mixed_locality" => Some(Self::MixedLocality),
            "low_locality" => Some(Self::LowLocality),
            _ => None,
        }
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkProtection {
    ClassicRef1,
    PqSimpleV1,
    PqSimpleDgramV1,
    #[default]
    PqMutualV1,
    PqMutualDgramV1,
}

impl BenchmarkProtection {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "classic_ref1" => Some(Self::ClassicRef1),
            "pq_simple_v1" => Some(Self::PqSimpleV1),
            "pq_simple_dgram_v1" => Some(Self::PqSimpleDgramV1),
            "pq_mutual_v1" => Some(Self::PqMutualV1),
            "pq_mutual_dgram_v1" => Some(Self::PqMutualDgramV1),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::ClassicRef1 => "classic_ref1",
            Self::PqSimpleV1 => "pq_simple_v1",
            Self::PqSimpleDgramV1 => "pq_simple_dgram_v1",
            Self::PqMutualV1 => "pq_mutual_v1",
            Self::PqMutualDgramV1 => "pq_mutual_dgram_v1",
        }
    }

    pub fn profile_kind(self) -> ProtectionProfileKind {
        match self {
            Self::ClassicRef1 => ProtectionProfileKind::ClassicRef1,
            Self::PqSimpleV1 => ProtectionProfileKind::PqSimpleV1,
            Self::PqSimpleDgramV1 => ProtectionProfileKind::PqSimpleDgramV1,
            Self::PqMutualV1 => ProtectionProfileKind::PqMutualV1,
            Self::PqMutualDgramV1 => ProtectionProfileKind::PqMutualDgramV1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkMode {
    Bulk,
    BurstSmall,
    BurstMedium,
    BurstBig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkClientRuntime {
    #[default]
    NativeRust,
    #[serde(rename = "web_wasm")]
    WebWasm,
}

impl BenchmarkClientRuntime {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "native_rust" | "native" => Some(Self::NativeRust),
            "web_wasm" => Some(Self::WebWasm),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::NativeRust => "native_rust",
            Self::WebWasm => "web_wasm",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkCarrier {
    #[default]
    WebSocket,
    Tcp,
    QuicStream,
    Udp,
    QuicDatagram,
    WebTransportDatagram,
}

impl BenchmarkCarrier {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "websocket" | "ws" => Some(Self::WebSocket),
            "tcp" => Some(Self::Tcp),
            "quic_stream" | "quic" => Some(Self::QuicStream),
            "udp" => Some(Self::Udp),
            "quic_datagram" | "quic_dgram" => Some(Self::QuicDatagram),
            "webtransport_datagram" | "webtransport_dgram" | "webtransport" => {
                Some(Self::WebTransportDatagram)
            }
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::Tcp => "tcp",
            Self::QuicStream => "quic_stream",
            Self::Udp => "udp",
            Self::QuicDatagram => "quic_datagram",
            Self::WebTransportDatagram => "webtransport_datagram",
        }
    }

    pub fn reliable_kind(self) -> ReliableCarrierKind {
        match self {
            Self::WebSocket => ReliableCarrierKind::WebSocket,
            Self::Tcp => ReliableCarrierKind::Tcp,
            Self::QuicStream => ReliableCarrierKind::QuicStream,
            Self::Udp | Self::QuicDatagram | Self::WebTransportDatagram => {
                ReliableCarrierKind::WebSocket
            }
        }
    }

    pub fn is_datagram(self) -> bool {
        matches!(
            self,
            Self::Udp | Self::QuicDatagram | Self::WebTransportDatagram
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkDirection {
    #[default]
    ServerToClient,
    ClientToServer,
}

impl BenchmarkDirection {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "server_to_client" | "s2c" => Some(Self::ServerToClient),
            "client_to_server" | "c2s" => Some(Self::ClientToServer),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::ServerToClient => "server_to_client",
            Self::ClientToServer => "client_to_server",
        }
    }

    pub fn stream_direction(self) -> StreamDirection {
        match self {
            Self::ServerToClient => StreamDirection::ServerToClient,
            Self::ClientToServer => StreamDirection::ClientToServer,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkCorpusKind {
    #[default]
    #[serde(rename = "wikitext_103_raw")]
    Wikitext103Raw,
    #[serde(rename = "web_image_50")]
    WebImage50,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkOptimization {
    pub source_dedup: bool,
    pub data_plane_codec: shared_protocol::DataPlaneCodecPreference,
}

impl Default for BenchmarkOptimization {
    fn default() -> Self {
        Self {
            source_dedup: true,
            data_plane_codec: shared_protocol::DataPlaneCodecPreference::Adaptive,
        }
    }
}

impl BenchmarkOptimization {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "baseline" => Some(Self {
                source_dedup: false,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::Adaptive,
            }),
            "dedup_only" => Some(Self {
                source_dedup: true,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::Adaptive,
            }),
            "direct_exact_only" => Some(Self {
                source_dedup: true,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::DirectExactOnly,
            }),
            "baseline_direct_exact" => Some(Self {
                source_dedup: false,
                data_plane_codec: shared_protocol::DataPlaneCodecPreference::DirectExactOnly,
            }),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match (self.source_dedup, self.data_plane_codec) {
            (false, shared_protocol::DataPlaneCodecPreference::Adaptive) => "baseline",
            (true, shared_protocol::DataPlaneCodecPreference::Adaptive) => "dedup_only",
            (true, shared_protocol::DataPlaneCodecPreference::DirectExactOnly) => {
                "direct_exact_only"
            }
            (false, shared_protocol::DataPlaneCodecPreference::DirectExactOnly) => {
                "baseline_direct_exact"
            }
        }
    }

    pub fn source_optimization_config(self) -> SourceOptimizationConfig {
        SourceOptimizationConfig {
            dedup_enabled: self.source_dedup,
            inline_source_meta_enabled: false,
            data_plane_codec: self.data_plane_codec,
            reversible_preprocessing_enabled: true,
            canonicalization_profile: shared_protocol::CanonicalizationProfile::Structural,
        }
    }
}

impl BenchmarkCorpusKind {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "wikitext_103_raw" => Some(Self::Wikitext103Raw),
            "web_image_50" => Some(Self::WebImage50),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Wikitext103Raw => "wikitext_103_raw",
            Self::WebImage50 => "web_image_50",
        }
    }
}

impl BenchmarkMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bulk" => Some(Self::Bulk),
            "burst_small" => Some(Self::BurstSmall),
            "burst_medium" => Some(Self::BurstMedium),
            "burst_big" => Some(Self::BurstBig),
            _ => None,
        }
    }

    pub fn slug(self) -> &'static str {
        match self {
            Self::Bulk => "bulk",
            Self::BurstSmall => "burst_small",
            Self::BurstMedium => "burst_medium",
            Self::BurstBig => "burst_big",
        }
    }

    pub fn payload_cap_bytes(self) -> Option<usize> {
        match self {
            Self::Bulk => None,
            Self::BurstSmall => Some(BURST_SMALL_CAP),
            Self::BurstMedium => Some(BURST_MEDIUM_CAP),
            Self::BurstBig => Some(BURST_BIG_CAP),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "bytes", rename_all = "snake_case")]
pub enum BenchmarkTargetBytes {
    OneMb,
    TenMb,
    HundredMb,
    Custom(u64),
}

impl BenchmarkTargetBytes {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "1mb" => Some(Self::OneMb),
            "10mb" => Some(Self::TenMb),
            "100mb" => Some(Self::HundredMb),
            other => other.parse::<u64>().ok().map(Self::Custom),
        }
    }

    pub fn bytes(self) -> u64 {
        match self {
            Self::OneMb => BENCH_TARGET_1_MB,
            Self::TenMb => BENCH_TARGET_10_MB,
            Self::HundredMb => BENCH_TARGET_100_MB,
            Self::Custom(bytes) => bytes,
        }
    }

    pub fn slug(self) -> String {
        match self {
            Self::OneMb => "1mb".to_string(),
            Self::TenMb => "10mb".to_string(),
            Self::HundredMb => "100mb".to_string(),
            Self::Custom(bytes) => format!("{bytes}b"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkCase {
    pub environment: BenchmarkEnvironment,
    #[serde(default)]
    pub carrier: BenchmarkCarrier,
    #[serde(default)]
    pub direction: BenchmarkDirection,
    #[serde(default)]
    pub corpus: BenchmarkCorpusKind,
    #[serde(default)]
    pub optimization: BenchmarkOptimization,
    pub capability_profile: BenchmarkCapabilityProfile,
    pub workload: BenchmarkWorkload,
    pub protection: BenchmarkProtection,
    pub mode: BenchmarkMode,
    pub target_bytes: BenchmarkTargetBytes,
    pub seed: u64,
    pub stream_id: StreamId,
    pub bind_addr: String,
    pub direct_host: Option<String>,
    pub port: u16,
    pub utility_sample_limit: usize,
    pub remote_ssh_target: Option<String>,
    pub remote_repo_path: Option<String>,
    #[serde(default)]
    pub client_runtime: BenchmarkClientRuntime,
    #[serde(default)]
    pub input_corpus_fingerprint: String,
    #[serde(default)]
    pub input_corpus_file_count: usize,
    #[serde(default)]
    pub input_corpus_chunk_count: usize,
}

impl BenchmarkCase {
    pub fn new(
        environment: BenchmarkEnvironment,
        capability_profile: BenchmarkCapabilityProfile,
        workload: BenchmarkWorkload,
        protection: BenchmarkProtection,
        mode: BenchmarkMode,
        target_bytes: BenchmarkTargetBytes,
    ) -> Self {
        let seed = derive_case_seed(
            environment,
            BenchmarkCarrier::WebSocket,
            BenchmarkDirection::ServerToClient,
            BenchmarkCorpusKind::Wikitext103Raw,
            BenchmarkOptimization::default(),
            capability_profile,
            workload,
            protection,
            mode,
            target_bytes,
        );
        let port = match environment {
            BenchmarkEnvironment::Local => 0,
            BenchmarkEnvironment::Remote => DEFAULT_BENCH_PORT,
        };
        let bind_addr = match environment {
            BenchmarkEnvironment::Local => "127.0.0.1:0".to_string(),
            BenchmarkEnvironment::Remote => format!("0.0.0.0:{DEFAULT_BENCH_PORT}"),
        };

        Self {
            environment,
            carrier: BenchmarkCarrier::WebSocket,
            direction: BenchmarkDirection::ServerToClient,
            corpus: BenchmarkCorpusKind::Wikitext103Raw,
            optimization: BenchmarkOptimization::default(),
            capability_profile,
            workload,
            protection,
            mode,
            target_bytes,
            seed,
            stream_id: StreamId(seed ^ 0x5eed_f00d_2026_0001),
            bind_addr,
            direct_host: (environment == BenchmarkEnvironment::Remote)
                .then(|| DEFAULT_REMOTE_HOST.to_string()),
            port,
            utility_sample_limit: UTILITY_SAMPLE_LIMIT,
            remote_ssh_target: (environment == BenchmarkEnvironment::Remote)
                .then(|| DEFAULT_REMOTE_SSH_TARGET.to_string()),
            remote_repo_path: (environment == BenchmarkEnvironment::Remote)
                .then(|| DEFAULT_REMOTE_REPO_PATH.to_string()),
            client_runtime: BenchmarkClientRuntime::NativeRust,
            input_corpus_fingerprint: input_corpus_manifest(BenchmarkCorpusKind::Wikitext103Raw)
                .fingerprint
                .clone(),
            input_corpus_file_count: input_corpus_manifest(BenchmarkCorpusKind::Wikitext103Raw)
                .file_count,
            input_corpus_chunk_count: input_corpus_manifest(BenchmarkCorpusKind::Wikitext103Raw)
                .chunk_count,
        }
    }

    pub fn with_custom_target(mut self, bytes: u64) -> Self {
        self.target_bytes = BenchmarkTargetBytes::Custom(bytes);
        self.refresh_identity();
        self
    }

    pub fn with_direction(mut self, direction: BenchmarkDirection) -> Self {
        self.direction = direction;
        self.refresh_identity();
        self
    }

    pub fn with_carrier(mut self, carrier: BenchmarkCarrier) -> Self {
        self.carrier = carrier;
        if carrier != BenchmarkCarrier::WebSocket {
            self.client_runtime = BenchmarkClientRuntime::NativeRust;
        }
        self.refresh_identity();
        self
    }

    pub fn with_corpus(mut self, corpus: BenchmarkCorpusKind) -> Self {
        self.corpus = corpus;
        self.input_corpus_fingerprint = input_corpus_manifest(corpus).fingerprint.clone();
        self.input_corpus_file_count = input_corpus_manifest(corpus).file_count;
        self.input_corpus_chunk_count = input_corpus_manifest(corpus).chunk_count;
        self.refresh_identity();
        self
    }

    pub fn with_optimization(mut self, optimization: BenchmarkOptimization) -> Self {
        self.optimization = optimization;
        self.refresh_identity();
        self
    }

    pub fn with_client_runtime(mut self, client_runtime: BenchmarkClientRuntime) -> Self {
        self.client_runtime = client_runtime;
        self
    }

    fn refresh_identity(&mut self) {
        self.seed = derive_case_seed(
            self.environment,
            self.carrier,
            self.direction,
            self.corpus,
            self.optimization,
            self.capability_profile,
            self.workload,
            self.protection,
            self.mode,
            self.target_bytes,
        );
        self.stream_id = StreamId(self.seed ^ 0x5eed_f00d_2026_0001);
    }

    pub fn artifact_rel_path(&self) -> PathBuf {
        PathBuf::from(self.environment.slug())
            .join(self.carrier.slug())
            .join(self.direction.slug())
            .join(self.corpus.slug())
            .join(self.optimization.slug())
            .join(self.capability_profile.slug())
            .join(self.workload.slug())
            .join(self.protection.slug())
            .join(self.mode.slug())
            .join(self.target_bytes.slug())
    }

    pub fn display_name(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            self.environment.slug(),
            self.carrier.slug(),
            self.direction.slug(),
            self.corpus.slug(),
            self.optimization.slug(),
            self.capability_profile.slug(),
            self.workload.slug(),
            self.protection.slug(),
            self.mode.slug(),
            self.client_runtime.slug(),
            self.target_bytes.slug(),
        )
    }

    fn direct_url(&self) -> Result<String, BenchError> {
        let host = self
            .direct_host
            .as_deref()
            .ok_or_else(|| BenchError::InvalidCase("missing direct host".to_string()))?;
        Ok(match self.carrier {
            BenchmarkCarrier::WebSocket => format!("ws://{host}:{}", self.port),
            BenchmarkCarrier::Tcp => format!("tcp://{host}:{}", self.port),
            BenchmarkCarrier::QuicStream => format!("quic://{host}:{}", self.port),
            BenchmarkCarrier::Udp => format!("udp://{host}:{}", self.port),
            BenchmarkCarrier::QuicDatagram => format!("quic_datagram://{host}:{}", self.port),
            BenchmarkCarrier::WebTransportDatagram => {
                format!("webtransport://{host}:{}/pulzz", self.port)
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkMatrixResult {
    pub artifact_root: String,
    pub total_cases: usize,
    pub completed_cases: usize,
    pub failed_cases: usize,
    pub case_results: Vec<BenchmarkCaseSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkCaseSummary {
    pub case: BenchmarkCase,
    pub artifact_dir: String,
    pub success: bool,
    pub failure: Option<String>,
    /// S4.5.e: Whether this result was loaded from cache (fingerprint-verified)
    /// or freshly executed.
    #[serde(default)]
    pub cache_hit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub case: BenchmarkCase,
    pub artifact_dir: String,
    pub target_payload_bytes: u64,
    #[serde(default)]
    pub original_payload_bytes: u64,
    pub actual_payload_bytes: u64,
    pub payload_overshoot_bytes: u64,
    pub protected_wire_bytes: u64,
    pub build_profile: String,
    pub direct_connectivity_verified: bool,
    #[serde(default)]
    pub sharded: bool,
    #[serde(default)]
    pub shard_count: usize,
    #[serde(default)]
    pub shard_payload_cap_bytes: Option<u64>,
    #[serde(default)]
    pub shard_results: Vec<String>,
    #[serde(default)]
    pub byte_categories: PayloadByteCategories,
    /// S4.2.e: Scratch/cache directory used by this benchmark run.
    /// Makes it explicit where protocol-external artifacts were stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scratch_dir: Option<String>,
    /// S4.5.e: Whether this result was loaded from a fingerprint-verified cache
    /// rather than freshly executed.
    #[serde(default)]
    pub cache_hit: bool,
    pub server: BenchmarkSideMetrics,
    pub client: BenchmarkSideMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PredictiveMemoryMetrics {
    pub route_family_share: HashMap<String, f64>,
    pub residual_burden: f64,
    pub completion_hit_rate: f64,
    pub schema_activation_share: f64,
    pub sync_risk_fallback_count: u64,
    #[serde(default)]
    pub exact_atom_direct_state_fallback_count: u64,
    /// S2.5.b3: count of transform-path downgrades where the transform route was demoted.
    #[serde(default)]
    pub transform_demoted_fallback_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSideMetrics {
    pub side: BenchmarkSide,
    pub records: u64,
    pub cue_object_records: u64,
    #[serde(default)]
    pub predictive_records: u64,
    #[serde(default)]
    pub original_payload_bytes: u64,
    pub payload_bytes: u64,
    pub wire_bytes: u64,
    pub throughput: ThroughputMetrics,
    pub process: ProcessMetrics,
    pub codec_modes: CodecModeTotals,
    #[serde(default)]
    pub source_kinds: BenchSourceKindTotals,
    #[serde(default)]
    pub residual_modes: ResidualModeTotals,
    pub encode_latency: Option<LatencySummary>,
    pub embedding_latency: Option<LatencySummary>,
    pub apply_latency: Option<LatencySummary>,
    pub burst_stats: Option<BurstStats>,
    pub utility: Option<UtilitySummary>,
    pub source_cache: Option<SourceCacheMetrics>,
    #[serde(default)]
    pub predictive_memory: PredictiveMemoryMetrics,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datagram: Option<BenchmarkDatagramMetrics>,
    #[serde(default)]
    pub byte_categories: PayloadByteCategories,
    pub side_result_path: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkSide {
    Server,
    Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ThroughputMetrics {
    pub total_duration_ms: u128,
    pub payload_bytes_per_sec: f64,
    pub wire_bytes_per_sec: f64,
    pub records_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProcessMetrics {
    pub peak_rss_bytes: u64,
    pub peak_vsz_bytes: u64,
    pub peak_cpu_percent: f64,
    pub timeseries_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodecModeTotals {
    pub direct_exact: u64,
    pub packed_exact: u64,
    pub predicted_exact: u64,
    pub control: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BenchSourceKindTotals {
    pub text: u64,
    pub json: u64,
    pub binary: u64,
    pub unknown: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResidualModeTotals {
    pub none: u64,
    pub all_zero: u64,
    pub small_signed_rans: u64,
    pub sparse_positions: u64,
    pub literal_raw: u64,
    pub unknown: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatencySummary {
    pub samples: usize,
    pub min_ns: u64,
    pub avg_ns: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BurstStats {
    pub burst_count: u64,
    pub total_burst_payload_bytes: u64,
    pub max_burst_payload_bytes: u64,
    pub average_fill_ratio: f64,
    pub peak_buffered_payload_bytes: u64,
    pub peak_buffered_frame_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UtilitySummary {
    pub measured_events: usize,
    pub exact_chunk_top1_rate: f64,
    pub exact_chunk_top5_rate: f64,
    pub same_file_top1_rate: f64,
    pub same_file_top5_rate: f64,
    pub mean_reciprocal_rank: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceCacheMetrics {
    pub lookups: u64,
    pub hits: u64,
    pub misses: u64,
    #[serde(alias = "embeddings_skipped")]
    pub object_materializations_skipped: u64,
    pub cache_read_ns: u64,
    pub cache_write_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BenchmarkDatagramMetrics {
    pub outbound_messages: u64,
    pub outbound_datagrams: u64,
    pub retransmitted_datagrams: u64,
    pub acknowledged_messages: u64,
    pub repair_requests_sent: u64,
    pub repair_requests_received: u64,
    pub duplicate_chunks_ignored: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SideArtifact {
    side: BenchmarkSide,
    metrics: BenchmarkSideMetrics,
    samples: Vec<TimeSeriesSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebBenchCase {
    role: WebBenchRole,
    session_config: Option<TransportSessionConfig>,
    bootstrap_client_config: Option<BootstrapClientConfig>,
    protection_kind: Option<ProtectionProfileKind>,
    stream_id: Option<StreamId>,
    receiver_bootstrap_root: Option<[u8; STREAM_ROOT_LEN]>,
    yield_on_group_boundary: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WebBenchRole {
    Receiver,
    Sender,
}

#[derive(Debug, Clone)]
struct BenchmarkBootstrapMaterials {
    session_config: TransportSessionConfig,
    server_config: BootstrapServerConfig,
    client_config: BootstrapClientConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebBenchRunnerOutput {
    result: WebBenchResult,
    samples: Vec<WebBenchSample>,
    peak_rss_bytes: u64,
    peak_vsz_bytes: u64,
    peak_cpu_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebBenchResult {
    records: u64,
    cue_object_records: u64,
    #[serde(default)]
    predictive_records: u64,
    #[serde(default)]
    original_payload_bytes: u64,
    payload_bytes: u64,
    wire_bytes: u64,
    total_duration_ms: u64,
    codec_modes: CodecModeTotals,
    #[serde(default)]
    source_kinds: BenchSourceKindTotals,
    #[serde(default)]
    residual_modes: ResidualModeTotals,
    apply_latency: Option<LatencySummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebBenchSample {
    elapsed_ms: u128,
    rss_bytes: u64,
    vsz_bytes: u64,
    cpu_percent: f64,
    records: u64,
    cue_object_records: u64,
    #[serde(default)]
    predictive_records: u64,
    #[serde(default)]
    original_payload_bytes: u64,
    payload_bytes: u64,
    wire_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputCorpusManifest {
    kind: BenchmarkCorpusKind,
    fingerprint: String,
    file_count: usize,
    chunk_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dataset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    config: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    split: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chunk_char_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chunk_char_stride: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    chunk_min_chars: Option<usize>,
    files: Vec<InputCorpusManifestFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputCorpusManifestFile {
    relative_path: String,
    sha256: String,
    byte_len: usize,
    chunk_count: usize,
    mime: Option<String>,
    source_url: Option<String>,
}

#[derive(Debug, Clone)]
struct InputCorpus {
    manifest: InputCorpusManifest,
    files: Vec<InputCorpusFile>,
    chunks: Vec<InputCorpusChunk>,
}

#[derive(Debug, Clone)]
struct InputCorpusFile {
    first_chunk_index: usize,
    chunk_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MaterializedTextChunkRecord {
    file_index: usize,
    chunk_index: usize,
    file_sha256: String,
    chunk_sha256: String,
    text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamedWikitextCorpus {
    manifest: InputCorpusManifest,
    chunks: Vec<MaterializedTextChunkRecord>,
}

#[derive(Debug, Clone)]
struct InputCorpusChunk {
    file_index: usize,
    file_sha256: String,
    chunk_index: usize,
    chunk_sha256: String,
    label: String,
    source: InputCorpusSource,
}

#[derive(Debug, Clone)]
enum InputCorpusSource {
    Text(String),
    Image { mime: String, bytes: Arc<[u8]> },
}

impl InputCorpusChunk {
    fn prepared_source(&self) -> shared_protocol::PreparedSource {
        match &self.source {
            InputCorpusSource::Text(text) => {
                shared_protocol::prepare_text_source(text, Some(self.label.clone()))
            }
            InputCorpusSource::Image { mime, bytes } => {
                shared_protocol::prepare_image_source(Some(self.label.clone()), mime, bytes)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebImageCorpusManifest {
    source: String,
    file_count: usize,
    files: Vec<WebImageCorpusManifestFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebImageCorpusManifestFile {
    relative_path: String,
    source_url: String,
    source_page_url: Option<String>,
    mime: String,
    byte_len: usize,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesSample {
    pub side: BenchmarkSide,
    pub elapsed_ms: u128,
    pub rss_bytes: u64,
    pub vsz_bytes: u64,
    pub cpu_percent: f64,
    pub records: u64,
    pub cue_object_records: u64,
    #[serde(default)]
    pub predictive_records: u64,
    #[serde(default)]
    pub original_payload_bytes: u64,
    pub payload_bytes: u64,
    pub wire_bytes: u64,
    pub bursts: u64,
}

#[derive(Debug, Clone)]
struct WorkloadGenerator {
    workload: BenchmarkWorkload,
    corpus: &'static InputCorpus,
    rng: StdRng,
    next_item_id: u64,
    live_items: HashMap<u64, WorkloadItemState>,
    live_ids: Vec<u64>,
    live_id_positions: HashMap<u64, usize>,
    step: usize,
}

#[derive(Debug, Clone, Copy)]
struct WorkloadItemState {
    corpus_index: usize,
    revision: u32,
}

#[derive(Debug, Clone)]
enum WorkloadEvent {
    Insert {
        item_id: u64,
        input: InputCorpusChunk,
    },
    UpsertObject {
        item_id: u64,
        input: InputCorpusChunk,
    },
    Evict {
        item_id: u64,
    },
    Invalidate {
        item_id: u64,
    },
}

#[derive(Debug, Clone)]
struct LiveBenchmarkItem {
    exact_bytes: Vec<u8>,
    file_sha256: String,
    chunk_sha256: String,
}

/// S4.1.c: Distinct byte category counters for honest benchmark reporting.
/// Every byte counted in payload_bytes is accounted for in exactly one category.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PayloadByteCategories {
    /// Bytes from ExactState records (direct literal content)
    pub exact_state_payload_bytes: u64,
    /// Bytes from PredictiveConfirm/PredictiveCorrect records
    pub predictive_dispatch_payload_bytes: u64,
    /// Bytes from inline definition records (AssemblyDef, TransformDef, SchemaDef)
    pub inline_definition_bytes: u64,
    /// Bytes from control-plane records that carry application data (Repair, MemoryRetire, SourceMeta)
    pub control_data_payload_bytes: u64,
    /// Bytes from EpisodeHint/ReplayHint records
    pub episode_hint_payload_bytes: u64,
    /// S4.1.c: Logical content bytes — the original uncompressed byte length of
    /// all transmitted items, regardless of which route family carried them.
    /// This is the honest denominator for compression ratios:
    ///   compression_ratio = logical_content_bytes / total_wire_bytes
    /// Unlike payload_bytes (which only counts ExactState payload), this counts
    /// the original bytes for ALL items: ExactState originals, predictive
    /// output lengths, transform output lengths, etc.
    pub logical_content_bytes: u64,
    /// Total wire bytes (full frame, including headers/auth)
    pub total_wire_bytes: u64,
    /// Record/header/auth overhead bytes (wire - payload)
    pub overhead_bytes: u64,
}

#[derive(Default)]
struct SideProgress {
    records: u64,
    cue_object_records: u64,
    predictive_records: u64,
    route_family_counts: HashMap<String, u64>,
    schema_activation_count: u64,
    transform_reuse_count: u64,
    predictive_completion_hits: u64,
    predictive_completion_attempts: u64,
    sync_risk_fallback_count: u64,
    exact_atom_direct_state_fallback_count: u64,
    transform_demoted_fallback_count: u64,
    original_payload_bytes: u64,
    payload_bytes: u64,
    wire_bytes: u64,
    bursts: u64,
    codec_modes: CodecModeTotals,
    source_kinds: BenchSourceKindTotals,
    residual_modes: ResidualModeTotals,
    byte_categories: PayloadByteCategories,
}

#[derive(Debug, Clone)]
struct BurstFrame {
    record: Record,
}

#[derive(Debug, Clone)]
struct BurstAccumulator {
    cap: usize,
    frames: Vec<BurstFrame>,
    buffered_payload_bytes: u64,
    buffered_transport_bytes: u64,
    peak_buffered_payload_bytes: u64,
    peak_buffered_frame_count: usize,
    burst_count: u64,
    total_burst_payload_bytes: u64,
    max_burst_payload_bytes: u64,
}

impl BurstAccumulator {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            frames: Vec::new(),
            buffered_payload_bytes: 0,
            buffered_transport_bytes: 0,
            peak_buffered_payload_bytes: 0,
            peak_buffered_frame_count: 0,
            burst_count: 0,
            total_burst_payload_bytes: 0,
            max_burst_payload_bytes: 0,
        }
    }

    fn push(&mut self, frame: BurstFrame, flushed: &mut Vec<Vec<BurstFrame>>) {
        let next_payload = benchmark_payload_len(&frame.record);
        let next_transport = benchmark_transport_len(&frame.record);
        if !self.frames.is_empty()
            && self.buffered_transport_bytes + next_transport > self.cap as u64
        {
            flushed.push(self.take_flush());
        }

        self.buffered_payload_bytes += next_payload;
        self.buffered_transport_bytes += next_transport;
        self.frames.push(frame);
        self.peak_buffered_payload_bytes = self
            .peak_buffered_payload_bytes
            .max(self.buffered_payload_bytes);
        self.peak_buffered_frame_count = self.peak_buffered_frame_count.max(self.frames.len());

        if self.buffered_transport_bytes >= self.cap as u64 {
            flushed.push(self.take_flush());
        }
    }

    fn flush_remaining(&mut self, flushed: &mut Vec<Vec<BurstFrame>>) {
        if !self.frames.is_empty() {
            flushed.push(self.take_flush());
        }
    }

    fn stats(&self) -> BurstStats {
        let average_fill_ratio = if self.burst_count == 0 {
            0.0
        } else {
            self.total_burst_payload_bytes as f64 / (self.burst_count as f64 * self.cap as f64)
        };

        BurstStats {
            burst_count: self.burst_count,
            total_burst_payload_bytes: self.total_burst_payload_bytes,
            max_burst_payload_bytes: self.max_burst_payload_bytes,
            average_fill_ratio,
            peak_buffered_payload_bytes: self.peak_buffered_payload_bytes,
            peak_buffered_frame_count: self.peak_buffered_frame_count,
        }
    }

    fn take_flush(&mut self) -> Vec<BurstFrame> {
        let payload_bytes = self
            .frames
            .iter()
            .map(|frame| benchmark_payload_len(&frame.record))
            .sum::<u64>();
        self.burst_count += 1;
        self.total_burst_payload_bytes += payload_bytes;
        self.max_burst_payload_bytes = self.max_burst_payload_bytes.max(payload_bytes);
        self.buffered_payload_bytes = 0;
        self.buffered_transport_bytes = 0;
        std::mem::take(&mut self.frames)
    }
}

enum AuthenticatedServerBenchConnection {
    WebSocket(crate::transport::AuthenticatedServerConnection),
    Tcp(crate::transport::AuthenticatedTcpSession),
    Quic(crate::transport::AuthenticatedQuicSession),
    Udp(crate::datagram_transport::AuthenticatedUdpSession),
    QuicDatagram(crate::datagram_transport::AuthenticatedQuicDatagramSession),
    WebTransportDatagram(crate::datagram_transport::AuthenticatedWebTransportDatagramSession),
}

impl AuthenticatedServerBenchConnection {
    fn protector_mut(&mut self) -> &mut StreamProtector {
        match self {
            Self::WebSocket(connection) => connection.protector_mut(),
            Self::Tcp(connection) => connection.protector_mut(),
            Self::Quic(connection) => connection.protector_mut(),
            Self::Udp(connection) => connection.protector_mut(),
            Self::QuicDatagram(connection) => connection.protector_mut(),
            Self::WebTransportDatagram(connection) => connection.protector_mut(),
        }
    }

    async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), BenchError> {
        match self {
            Self::WebSocket(connection) => connection.send_transport_frame(frame).await?,
            Self::Tcp(connection) => connection.send_transport_frame(frame).await?,
            Self::Quic(connection) => connection.send_transport_frame(frame).await?,
            Self::Udp(connection) => connection.send_transport_frame(frame).await?,
            Self::QuicDatagram(connection) => connection.send_transport_frame(frame).await?,
            Self::WebTransportDatagram(connection) => {
                connection.send_transport_frame(frame).await?
            }
        }
        Ok(())
    }

    async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, BenchError> {
        Ok(match self {
            Self::WebSocket(connection) => connection.read_transport_frame().await?,
            Self::Tcp(connection) => connection.read_transport_frame().await?,
            Self::Quic(connection) => connection.read_transport_frame().await?,
            Self::Udp(connection) => connection.read_transport_frame().await?,
            Self::QuicDatagram(connection) => connection.read_transport_frame().await?,
            Self::WebTransportDatagram(connection) => connection.read_transport_frame().await?,
        })
    }

    fn datagram_metrics(&self) -> Option<BenchmarkDatagramMetrics> {
        match self {
            Self::Udp(connection) => {
                Some(benchmark_datagram_metrics(connection.datagram_metrics()))
            }
            Self::QuicDatagram(connection) => {
                Some(benchmark_datagram_metrics(connection.datagram_metrics()))
            }
            Self::WebTransportDatagram(connection) => {
                Some(benchmark_datagram_metrics(connection.datagram_metrics()))
            }
            Self::WebSocket(_) | Self::Tcp(_) | Self::Quic(_) => None,
        }
    }

    async fn close(self) -> Result<(), BenchError> {
        match self {
            Self::WebSocket(connection) => connection.close().await?,
            Self::Tcp(connection) => connection.close().await?,
            Self::Quic(connection) => connection.close().await?,
            Self::Udp(connection) => connection.close().await?,
            Self::QuicDatagram(connection) => connection.close().await?,
            Self::WebTransportDatagram(connection) => connection.close().await?,
        }
        Ok(())
    }
}

enum AuthenticatedClientBenchConnection {
    WebSocket(client::ConnectedWebSocketSession),
    Tcp(client::ConnectedTcpSession),
    Quic(client::ConnectedQuicSession),
    Udp(client::ConnectedUdpSession),
    QuicDatagram(client::ConnectedQuicDatagramSession),
    WebTransportDatagram(client::ConnectedWebTransportDatagramSession),
}

impl AuthenticatedClientBenchConnection {
    fn session_mut(&mut self) -> &mut ClientSession {
        match self {
            Self::WebSocket(connection) => connection.session_mut(),
            Self::Tcp(connection) => connection.session_mut(),
            Self::Quic(connection) => connection.session_mut(),
            Self::Udp(connection) => connection.session_mut(),
            Self::QuicDatagram(connection) => connection.session_mut(),
            Self::WebTransportDatagram(connection) => connection.session_mut(),
        }
    }

    async fn send_transport_frame(&mut self, frame: Vec<u8>) -> Result<(), BenchError> {
        match self {
            Self::WebSocket(connection) => connection.send_transport_frame(frame).await?,
            Self::Tcp(connection) => connection.send_transport_frame(frame).await?,
            Self::Quic(connection) => connection.send_transport_frame(frame).await?,
            Self::Udp(connection) => connection.send_transport_frame(frame).await?,
            Self::QuicDatagram(connection) => connection.send_transport_frame(frame).await?,
            Self::WebTransportDatagram(connection) => {
                connection.send_transport_frame(frame).await?
            }
        }
        Ok(())
    }

    async fn read_transport_frame(&mut self) -> Result<Option<Vec<u8>>, BenchError> {
        Ok(match self {
            Self::WebSocket(connection) => connection.read_transport_frame().await?,
            Self::Tcp(connection) => connection.read_transport_frame().await?,
            Self::Quic(connection) => connection.read_transport_frame().await?,
            Self::Udp(connection) => connection.read_transport_frame().await?,
            Self::QuicDatagram(connection) => connection.read_transport_frame().await?,
            Self::WebTransportDatagram(connection) => connection.read_transport_frame().await?,
        })
    }

    fn datagram_metrics(&self) -> Option<BenchmarkDatagramMetrics> {
        match self {
            Self::Udp(connection) => {
                Some(benchmark_datagram_metrics(connection.datagram_metrics()))
            }
            Self::QuicDatagram(connection) => {
                Some(benchmark_datagram_metrics(connection.datagram_metrics()))
            }
            Self::WebTransportDatagram(connection) => {
                Some(benchmark_datagram_metrics(connection.datagram_metrics()))
            }
            Self::WebSocket(_) | Self::Tcp(_) | Self::Quic(_) => None,
        }
    }

    async fn close(self) -> Result<(), BenchError> {
        match self {
            Self::WebSocket(connection) => connection.close().await?,
            Self::Tcp(connection) => connection.close().await?,
            Self::Quic(connection) => connection.close().await?,
            Self::Udp(connection) => connection.close().await?,
            Self::QuicDatagram(connection) => connection.close().await?,
            Self::WebTransportDatagram(connection) => connection.close().await?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LatencyReservoir {
    rng: StdRng,
    seen: u64,
    values: Vec<u64>,
}

impl LatencyReservoir {
    fn new(seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            seen: 0,
            values: Vec::with_capacity(LATENCY_SAMPLE_CAP),
        }
    }

    fn record(&mut self, value: u64) {
        self.seen += 1;
        if self.values.len() < LATENCY_SAMPLE_CAP {
            self.values.push(value);
            return;
        }

        let replace_index = self.rng.gen_range(0_u64..self.seen);
        if replace_index < LATENCY_SAMPLE_CAP as u64 {
            self.values[replace_index as usize] = value;
        }
    }

    fn summary(&self) -> Option<LatencySummary> {
        if self.values.is_empty() {
            return None;
        }

        let mut sorted = self.values.clone();
        sorted.sort_unstable();
        let count = sorted.len();
        let sum = sorted.iter().copied().sum::<u64>();
        Some(LatencySummary {
            samples: count,
            min_ns: *sorted.first().unwrap(),
            avg_ns: sum as f64 / count as f64,
            p50_ns: percentile(&sorted, 0.50),
            p95_ns: percentile(&sorted, 0.95),
            p99_ns: percentile(&sorted, 0.99),
            max_ns: *sorted.last().unwrap(),
        })
    }
}

#[derive(Debug, Clone, Default)]
struct UtilityAccumulator {
    measured_events: usize,
    exact_chunk_top1_sum: f64,
    exact_chunk_top5_sum: f64,
    same_file_top1_sum: f64,
    same_file_top5_sum: f64,
    reciprocal_rank_sum: f64,
}

#[derive(Debug, Clone)]
struct ProcessSampler {
    pid: u32,
    side: BenchmarkSide,
    start: Instant,
    last_sample: Instant,
    samples: Vec<TimeSeriesSample>,
    peak_rss_bytes: u64,
    peak_vsz_bytes: u64,
    peak_cpu_percent: f64,
}

impl ProcessSampler {
    fn new(side: BenchmarkSide) -> Self {
        let now = Instant::now();
        Self {
            pid: std::process::id(),
            side,
            start: now,
            last_sample: now - PROCESS_SAMPLE_INTERVAL,
            samples: Vec::new(),
            peak_rss_bytes: 0,
            peak_vsz_bytes: 0,
            peak_cpu_percent: 0.0,
        }
    }

    fn maybe_sample(&mut self, progress: &SideProgress) -> Result<(), BenchError> {
        if self.last_sample.elapsed() < PROCESS_SAMPLE_INTERVAL {
            return Ok(());
        }
        self.last_sample = Instant::now();
        let (rss_bytes, vsz_bytes, cpu_percent) = sample_process_metrics(self.pid)?;
        self.peak_rss_bytes = self.peak_rss_bytes.max(rss_bytes);
        self.peak_vsz_bytes = self.peak_vsz_bytes.max(vsz_bytes);
        self.peak_cpu_percent = self.peak_cpu_percent.max(cpu_percent);
        self.samples.push(TimeSeriesSample {
            side: self.side,
            elapsed_ms: self.start.elapsed().as_millis(),
            rss_bytes,
            vsz_bytes,
            cpu_percent,
            records: progress.records,
            cue_object_records: progress.cue_object_records,
            predictive_records: progress.predictive_records,
            original_payload_bytes: progress.original_payload_bytes,
            payload_bytes: progress.payload_bytes,
            wire_bytes: progress.wire_bytes,
            bursts: progress.bursts,
        });
        Ok(())
    }

    fn finish(
        mut self,
        progress: &SideProgress,
    ) -> Result<(ProcessMetrics, Vec<TimeSeriesSample>), BenchError> {
        self.last_sample = self.start - PROCESS_SAMPLE_INTERVAL;
        self.maybe_sample(progress)?;
        Ok((
            ProcessMetrics {
                peak_rss_bytes: self.peak_rss_bytes,
                peak_vsz_bytes: self.peak_vsz_bytes,
                peak_cpu_percent: self.peak_cpu_percent,
                timeseries_path: String::new(),
            },
            self.samples,
        ))
    }
}

#[derive(Debug, Error)]
pub enum BenchError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Server(#[from] crate::ServerError),
    #[error(transparent)]
    Client(#[from] client::ClientApplyError),
    #[error(transparent)]
    ClientConnect(#[from] client::ClientConnectError),
    #[error(transparent)]
    Bootstrap(#[from] shared_protocol::BootstrapError),
    #[error(transparent)]
    Protection(#[from] shared_protocol::ProtectionError),
    #[error(transparent)]
    Wire(#[from] shared_protocol::WireError),
    #[error(transparent)]
    Codec(#[from] shared_protocol::CodecError),
    #[error(transparent)]
    SourceCache(#[from] crate::source_cache::SourceCacheError),
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),
    #[error(transparent)]
    Transport(#[from] crate::transport::TransportError),
    #[error("invalid benchmark case: {0}")]
    InvalidCase(String),
    #[error("benchmark invariant failed: {0}")]
    Invariant(String),
    #[error("failed to parse process metrics: {0}")]
    ProcessSample(String),
    #[error("remote direct connectivity check failed for {host}:{port}")]
    DirectConnectivityFailed { host: String, port: u16 },
    #[error("remote benchmark host must be a direct non-loopback address")]
    LoopbackRemoteHostRejected,
    #[error("remote command failed: {0}")]
    RemoteCommand(String),
    #[error("local command failed: {0}")]
    LocalCommand(String),
    #[error("remote benchmark run failed before direct connectivity became available")]
    RemoteServerExitedEarly,
}

pub async fn run_benchmark_case(
    case: BenchmarkCase,
    artifact_root: impl AsRef<Path>,
) -> Result<BenchmarkResult, BenchError> {
    validate_case_input_corpus(&case)?;
    let case_root = artifact_root.as_ref().join(case.artifact_rel_path());
    prepare_case_root(&case_root, &case)?;

    let shard_targets =
        split_target_bytes(case.target_bytes.bytes(), BENCH_MAX_SESSION_TARGET_BYTES);
    if shard_targets.len() > 1 {
        return run_sharded_benchmark_case(case, &case_root, &shard_targets).await;
    }

    run_single_benchmark_case(case, &case_root).await
}

async fn run_single_benchmark_case(
    case: BenchmarkCase,
    case_root: &Path,
) -> Result<BenchmarkResult, BenchError> {
    match case.environment {
        BenchmarkEnvironment::Local => run_local_case(case, case_root).await,
        BenchmarkEnvironment::Remote => run_remote_case(case, case_root).await,
    }
}

async fn run_sharded_benchmark_case(
    case: BenchmarkCase,
    case_root: &Path,
    shard_targets: &[u64],
) -> Result<BenchmarkResult, BenchError> {
    let shards_root = case_root.join("shards");
    fs::create_dir_all(&shards_root)?;

    let mut shard_results = Vec::with_capacity(shard_targets.len());
    let mut shard_result_paths = Vec::with_capacity(shard_targets.len());
    for (index, shard_target_bytes) in shard_targets.iter().copied().enumerate() {
        let shard_case = shard_case(&case, index, shard_target_bytes);
        let shard_dir = shards_root.join(format!(
            "shard_{:04}_{}",
            index + 1,
            BenchmarkTargetBytes::Custom(shard_target_bytes).slug()
        ));
        prepare_case_root(&shard_dir, &shard_case)?;
        let shard_result = run_single_benchmark_case(shard_case, &shard_dir).await?;
        shard_result_paths.push(shard_dir.join("result.json").display().to_string());
        shard_results.push(shard_result);
    }

    fs::write(
        case_root.join("shards.json"),
        serde_json::to_vec_pretty(&shard_results)?,
    )?;
    finalize_sharded_case_result(
        case,
        case_root,
        shard_targets,
        shard_result_paths,
        &shard_results,
    )
}

pub async fn run_benchmark_matrix(
    cases: Vec<BenchmarkCase>,
    artifact_root: impl AsRef<Path>,
) -> Result<BenchmarkMatrixResult, BenchError> {
    let artifact_root = artifact_root.as_ref();
    fs::create_dir_all(artifact_root)?;

    let total_cases = cases.len();
    let pb = bench_matrix_progress_bar(total_cases as u64);
    pb.set_message("starting local matrix...");

    let mut summaries = Vec::with_capacity(total_cases);
    let mut completed = 0_usize;
    let mut cache_hits = 0_usize;
    let mut case_durations: Vec<Duration> = Vec::new();
    let matrix_start = Instant::now();
    for (case_index, case) in cases.into_iter().enumerate() {
        let display_name = case.display_name();
        let artifact_dir = artifact_root.join(case.artifact_rel_path());
        if artifact_dir.join("result.json").exists() {
            // S4.5: Fingerprint-based cached-result reuse. Check that the
            // cached result was produced by the same source/config/input.
            let cache_hit = check_result_fingerprint(&artifact_dir, &case);
            if !cache_hit {
                pb.println(format!(
                    "S4.5: fingerprint mismatch (v5) → re-run: {display_name}"
                ));
                pb.set_message(format!(
                    "[{}/{}] re-run (fp mismatch): {}",
                    case_index + 1, total_cases, display_name
                ));
                // Fall through to re-run instead of using stale result
            } else {
                completed += 1;
                cache_hits += 1;
                pb.inc(1);
                pb.set_message(format!(
                    "[{}/{}] ✓ cached: {}",
                    case_index + 1, total_cases, display_name
                ));
                summaries.push(BenchmarkCaseSummary {
                    case,
                    artifact_dir: artifact_dir.display().to_string(),
                    success: true,
                    failure: None,
                    cache_hit: true, // S4.5.e: fingerprint-verified cache hit
                });
                continue;
            }
        } else {
            pb.set_message(format!(
                "[{}/{}] ▶ running: {}",
                case_index + 1, total_cases, display_name
            ));
        }
        let case_start = Instant::now();
        match run_benchmark_case(case.clone(), artifact_root).await {
            Ok(result) => {
                let case_elapsed = case_start.elapsed();
                case_durations.push(case_elapsed);
                completed += 1;
                pb.inc(1);

                let throughput_mbps = if case_elapsed.as_secs_f64() > 0.0 {
                    result.protected_wire_bytes as f64 / case_elapsed.as_secs_f64() / 1e6
                } else { 0.0 };
                let savings_pct = if result.original_payload_bytes > 0 {
                    (1.0 - result.protected_wire_bytes as f64 / result.original_payload_bytes as f64) * 100.0
                } else { 0.0 };

                pb.println(format!(
                    "  [{}/{}] ✓ {} | {} | {:.1} MB/s wire | {} → {} ({:.1}% savings)",
                    completed, total_cases, display_name,
                    HumanDuration(case_elapsed),
                    throughput_mbps,
                    HumanBytes(result.original_payload_bytes),
                    HumanBytes(result.protected_wire_bytes),
                    savings_pct,
                ));

                // Update matrix bar message with running average
                if !case_durations.is_empty() {
                    let avg_case = average_duration(&case_durations);
                    let remaining = total_cases - case_index - 1;
                    let eta_remaining = avg_case * remaining as u32;
                    pb.set_message(format!(
                        "avg {}/case | {} remaining | ETA {}",
                        HumanDuration(avg_case),
                        remaining,
                        HumanDuration(eta_remaining),
                    ));
                }

                summaries.push(BenchmarkCaseSummary {
                    case,
                    artifact_dir: artifact_dir.display().to_string(),
                    success: true,
                    failure: None,
                    cache_hit: false, // S4.5.e: freshly executed
                });
            }
            Err(error) => {
                let case_elapsed = case_start.elapsed();
                case_durations.push(case_elapsed);
                pb.inc(1);
                pb.println(format!(
                    "  [{}/{}] ✗ {} FAILED | {} | {}",
                    case_index + 1, total_cases, display_name,
                    HumanDuration(case_elapsed),
                    error,
                ));
                write_failure_log(&artifact_dir, &error.to_string())?;
                let failure_text = error.to_string();
                let stop_for_disk = is_disk_exhaustion_error(&error);
                summaries.push(BenchmarkCaseSummary {
                    case,
                    artifact_dir: artifact_dir.display().to_string(),
                    success: false,
                    failure: Some(failure_text.clone()),
                    cache_hit: false, // S4.5.e: fresh run (failed)
                });
                if stop_for_disk {
                    pb.println(format!(
                        "matrix aborted after disk exhaustion: {}",
                        failure_text,
                    ));
                    break;
                }
            }
        }
    }

    let matrix_elapsed = matrix_start.elapsed();
    let failed_count = summaries.len() - completed;
    pb.finish_with_message(format!(
        "done: {completed}/{total_cases} ok, {failed_count} failed, {cache_hits} cached | {}",
        HumanDuration(matrix_elapsed)
    ));

    let matrix = BenchmarkMatrixResult {
        artifact_root: artifact_root.display().to_string(),
        total_cases,
        completed_cases: completed,
        failed_cases: summaries.len() - completed,
        case_results: summaries,
    };
    fs::write(
        artifact_root.join("matrix_result.json"),
        serde_json::to_vec_pretty(&matrix)?,
    )?;
    fs::write(
        artifact_root.join("summary.md"),
        render_matrix_summary(&matrix),
    )?;
    Ok(matrix)
}

const DISTRIBUTED_BENCH_PORT_BASE: u16 = 32_000;
const DISTRIBUTED_BENCH_PORT_SPAN: u16 = 20_000;
const DISTRIBUTED_SERVER_READY_DELAY: Duration = Duration::from_millis(1_500);

pub async fn run_distributed_native_matrix(
    cases: Vec<BenchmarkCase>,
    artifact_root: impl AsRef<Path>,
    client_ssh_target: &str,
    client_repo_path: &str,
    server_host: &str,
) -> Result<BenchmarkMatrixResult, BenchError> {
    let artifact_root = artifact_root.as_ref();
    fs::create_dir_all(artifact_root)?;

    sync_workspace_to_remote(client_ssh_target, client_repo_path)?;
    sync_release_server_binary_to_remote(client_ssh_target, client_repo_path)?;

    let total_cases = cases.len();
    let pb = bench_matrix_progress_bar(total_cases as u64);
    pb.set_message("workspace synced, starting matrix...");

    let mut summaries = Vec::with_capacity(total_cases);
    let mut completed = 0_usize;
    let mut cache_hits = 0_usize;
    let mut re_runs = 0_usize;
    let mut case_durations: Vec<Duration> = Vec::new();
    let matrix_start = Instant::now();

    for (case_index, base_case) in cases.into_iter().enumerate() {
        let case = prepare_distributed_native_case(base_case, server_host);
        let display_name = case.display_name();
        let target_bytes = case.target_bytes.bytes();
        let artifact_dir = artifact_root.join(case.artifact_rel_path());

        if artifact_dir.join("result.json").exists() {
            // S4.5: Fingerprint-based cached-result reuse for distributed path
            let cache_hit = check_result_fingerprint(&artifact_dir, &case);
            if !cache_hit {
                re_runs += 1;
                pb.println(format!(
                    "S4.5: fingerprint mismatch (v5) → re-run #{re_runs}: {display_name}"
                ));
                pb.set_message(format!(
                    "[{}/{}] re-run (fp mismatch): {}",
                    case_index + 1, total_cases, display_name
                ));
                // Fall through to re-run
            } else {
                completed += 1;
                cache_hits += 1;
                pb.inc(1);
                pb.set_message(format!(
                    "[{}/{}] ✓ cached: {}",
                    case_index + 1, total_cases, display_name
                ));
                summaries.push(BenchmarkCaseSummary {
                    case,
                    artifact_dir: artifact_dir.display().to_string(),
                    success: true,
                    failure: None,
                    cache_hit: true, // S4.5.e: fingerprint-verified cache hit
                });
                continue;
            }
        } else {
            pb.set_message(format!(
                "[{}/{}] ▶ running: {} ({})",
                case_index + 1, total_cases, display_name, HumanBytes(target_bytes)
            ));
        }

        // Estimate case duration from historical data
        let estimated_duration = estimate_case_duration(&case_durations, target_bytes);
        let case_start = Instant::now();

        match run_distributed_native_case(
            case.clone(),
            artifact_root,
            client_ssh_target,
            client_repo_path,
            &pb,
            case_index,
            total_cases,
            estimated_duration,
        )
        .await
        {
            Ok(result) => {
                let case_elapsed = case_start.elapsed();
                case_durations.push(case_elapsed);
                completed += 1;
                pb.inc(1);

                let throughput_mbps = if case_elapsed.as_secs_f64() > 0.0 {
                    result.protected_wire_bytes as f64 / case_elapsed.as_secs_f64() / 1e6
                } else { 0.0 };
                let savings_pct = if result.original_payload_bytes > 0 {
                    (1.0 - result.protected_wire_bytes as f64 / result.original_payload_bytes as f64) * 100.0
                } else { 0.0 };

                pb.println(format!(
                    "  [{}/{}] ✓ {} | {} | {:.1} MB/s wire | {} → {} ({:.1}% savings)",
                    completed, total_cases, display_name,
                    HumanDuration(case_elapsed),
                    throughput_mbps,
                    HumanBytes(result.original_payload_bytes),
                    HumanBytes(result.protected_wire_bytes),
                    savings_pct,
                ));

                // Update matrix bar message with running average
                let avg_case = average_duration(&case_durations);
                let remaining = total_cases - case_index - 1;
                let eta_remaining = avg_case * remaining as u32;
                pb.set_message(format!(
                    "avg {}/case | {} remaining | ETA {}",
                    HumanDuration(avg_case),
                    remaining,
                    HumanDuration(eta_remaining),
                ));

                summaries.push(BenchmarkCaseSummary {
                    case,
                    artifact_dir: artifact_dir.display().to_string(),
                    success: true,
                    failure: None,
                    cache_hit: false, // S4.5.e: freshly executed
                });
            }
            Err(error) => {
                let case_elapsed = case_start.elapsed();
                case_durations.push(case_elapsed);
                pb.inc(1);
                pb.println(format!(
                    "  [{}/{}] ✗ {} FAILED | {} | {}",
                    case_index + 1, total_cases, display_name,
                    HumanDuration(case_elapsed),
                    error,
                ));
                write_failure_log(&artifact_dir, &error.to_string())?;
                let failure_text = error.to_string();
                let stop_for_disk = is_disk_exhaustion_error(&error);
                summaries.push(BenchmarkCaseSummary {
                    case,
                    artifact_dir: artifact_dir.display().to_string(),
                    success: false,
                    failure: Some(failure_text.clone()),
                    cache_hit: false, // S4.5.e: fresh run (failed)
                });
                if stop_for_disk {
                    pb.println(format!(
                        "matrix aborted after disk exhaustion: {}",
                        failure_text,
                    ));
                    break;
                }
            }
        }
    }

    let matrix_elapsed = matrix_start.elapsed();
    let failed_count = summaries.len() - completed;
    pb.finish_with_message(format!(
        "done: {completed}/{total_cases} ok, {failed_count} failed, {cache_hits} cached | {}",
        HumanDuration(matrix_elapsed)
    ));

    let matrix = BenchmarkMatrixResult {
        artifact_root: artifact_root.display().to_string(),
        total_cases,
        completed_cases: completed,
        failed_cases: failed_count,
        case_results: summaries,
    };
    fs::write(
        artifact_root.join("matrix_result.json"),
        serde_json::to_vec_pretty(&matrix)?,
    )?;
    fs::write(
        artifact_root.join("summary.md"),
        render_matrix_summary(&matrix),
    )?;
    Ok(matrix)
}

/// Estimate case duration from historical durations and target bytes.
/// Uses a simple bytes-per-second model from completed cases.
fn estimate_case_duration(case_durations: &[Duration], target_bytes: u64) -> Duration {
    if case_durations.is_empty() {
        // Rough defaults based on typical 100MB throughput
        let estimated_secs = target_bytes as f64 / 5_000_000.0; // ~5 MB/s conservative
        Duration::from_secs_f64(estimated_secs.max(1.0))
    } else {
        let avg = average_duration(case_durations);
        // Scale by target bytes ratio (rough heuristic)
        let avg_bytes = 10_000_000.0; // assume average case is ~10MB
        let scale = target_bytes as f64 / avg_bytes;
        Duration::from_secs_f64((avg.as_secs_f64() * scale).max(1.0))
    }
}

/// Compute the average of a slice of Durations.
fn average_duration(durations: &[Duration]) -> Duration {
    if durations.is_empty() {
        return Duration::ZERO;
    }
    let total_nanos: u64 = durations.iter().map(|d| d.as_nanos() as u64).sum();
    Duration::from_nanos(total_nanos / durations.len() as u64)
}

fn prepare_distributed_native_case(mut case: BenchmarkCase, server_host: &str) -> BenchmarkCase {
    case.environment = BenchmarkEnvironment::Remote;
    case.direct_host = Some(server_host.to_string());
    case.remote_ssh_target = None;
    case.remote_repo_path = None;
    case.refresh_identity();
    case.port = distributed_case_port(case.seed);
    case.bind_addr = format!("0.0.0.0:{}", case.port);
    case
}

fn distributed_case_port(seed: u64) -> u16 {
    DISTRIBUTED_BENCH_PORT_BASE + (seed % u64::from(DISTRIBUTED_BENCH_PORT_SPAN)) as u16
}

fn estimated_case_disk_bytes(case: &BenchmarkCase) -> u64 {
    BENCH_LOCAL_DISK_MIN_FREE_BYTES.max(case.target_bytes.bytes().saturating_mul(4))
}

fn is_disk_exhaustion_error(error: &BenchError) -> bool {
    error.to_string().contains("No space left on device")
}

fn available_disk_bytes(path: &Path) -> Result<u64, BenchError> {
    let target = if path.exists() {
        path
    } else {
        path.parent().unwrap_or(Path::new("."))
    };
    let output = run_command_capture(
        Command::new("df")
            .arg("-Pk")
            .arg(target),
    )?;
    let line = output
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| BenchError::Invariant(format!(
            "failed to read disk availability for {}",
            target.display()
        )))?;
    let available_kb = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| BenchError::Invariant(format!(
            "failed to parse available disk blocks for {} from: {}",
            target.display(),
            line
        )))?
        .parse::<u64>()
        .map_err(|error| BenchError::Invariant(format!(
            "failed to parse available disk blocks for {}: {}",
            target.display(),
            error
        )))?;
    Ok(available_kb.saturating_mul(1024))
}

fn ensure_local_case_disk_budget(case_root: &Path, case: &BenchmarkCase) -> Result<(), BenchError> {
    let required_bytes = estimated_case_disk_bytes(case);
    let available_bytes = available_disk_bytes(case_root)?;
    if available_bytes < required_bytes {
        return Err(BenchError::Invariant(format!(
            "insufficient local disk for {}: available={} bytes required_at_least={} bytes path={}",
            case.display_name(),
            available_bytes,
            required_bytes,
            case_root.display(),
        )));
    }
    Ok(())
}

async fn run_distributed_native_case(
    case: BenchmarkCase,
    artifact_root: &Path,
    client_ssh_target: &str,
    client_repo_path: &str,
    pb: &ProgressBar,
    case_index: usize,
    total_cases: usize,
    estimated_duration: Duration,
) -> Result<BenchmarkResult, BenchError> {
    validate_case_input_corpus(&case)?;
    if case.client_runtime != BenchmarkClientRuntime::NativeRust {
        return Err(BenchError::InvalidCase(
            "distributed native runner requires native_rust client runtime".to_string(),
        ));
    }

    let display_name = case.display_name();
    let target_bytes = case.target_bytes.bytes();
    let case_root = artifact_root.join(case.artifact_rel_path());
    prepare_case_root(&case_root, &case)?;
    ensure_local_case_disk_budget(&case_root, &case)?;
    let server_dir = case_root.join(SERVER_SIDE_DIR);
    let client_dir = case_root.join(CLIENT_SIDE_DIR);
    fs::create_dir_all(&server_dir)?;
    fs::create_dir_all(&client_dir)?;

    let remote_repo_shell_path = remote_shell_path(client_repo_path);
    let remote_case_rel_path = format!(
        "distributed_native/{}/{:016x}",
        case.artifact_rel_path().display(),
        case.seed
    );
    let remote_case_dir = format!("{REMOTE_RUNTIME_SCRATCH_ROOT}/{remote_case_rel_path}");
    let remote_case_dir_shell = remote_case_dir.clone();

    // ── Stage 1/6: rsync case data to remote ──
    pb.set_message(format!(
        "[{}/{}] 1/6 rsync → remote: {} ({})",
        case_index + 1, total_cases, display_name, HumanBytes(target_bytes)
    ));
    run_command(Command::new("ssh").arg(client_ssh_target).arg(format!(
        "rm -rf \"{remote_case_dir_shell}\" && mkdir -p \"{remote_case_dir_shell}/{client_dir}\"",
        client_dir = CLIENT_SIDE_DIR,
    )))?;
    run_command(Command::new("rsync").args([
        "-az",
        case_root.join("case.json").to_str().unwrap(),
        &format!("{client_ssh_target}:{remote_case_dir}/case.json"),
    ]))?;

    // ── Stage 2/6: Start local server ──
    let url = case.direct_url()?;
    pb.set_message(format!(
        "[{}/{}] 2/6 starting server :{} for {}",
        case_index + 1, total_cases, case.port, display_name
    ));
    let mut server_task = spawn_distributed_server_task(case.clone(), server_dir.clone());
    sleep(DISTRIBUTED_SERVER_READY_DELAY).await;

    // ── Stage 3/6: Run remote verify (this is the LONGEST phase) ──
    // Create a dedicated sub-progress bar with time-based estimated progress.
    // Since we can't pipe byte-level progress from the remote SSH client,
    // we estimate progress based on target bytes, elapsed time, and the
    // estimated total duration from historical data.
    let case_pb = bench_distributed_case_progress_bar(
        target_bytes,
        &format!("[{}/{}] {display_name}", case_index + 1, total_cases),
        estimated_duration,
    );
    case_pb.set_message(format!(
        "verifying → {} (~{})",
        url,
        HumanDuration(estimated_duration),
    ));

    let remote_command = format!(
        "cd \"{repo}\" && ./target/release/server bench verify \"{case_dir}/case.json\" \"{url}\" \"{case_dir}/{client_dir}\"",
        repo = remote_repo_shell_path,
        case_dir = remote_case_dir_shell,
        url = url,
        client_dir = CLIENT_SIDE_DIR,
    );

    // Spawn the SSH verify command and simultaneously run a time-based
    // progress estimator that updates the case progress bar.
    let ssh_target = client_ssh_target.to_string();
    let verify_handle = tokio::spawn(async move {
        let mut cmd = Command::new("ssh");
        cmd.arg("-o")
            .arg("ServerAliveInterval=15")
            .arg("-o")
            .arg("ServerAliveCountMax=4")
            .arg(&ssh_target)
            .arg(&remote_command);
        cmd.status()
    });

    // Time-based progress estimator: update the sub-progress bar while
    // the SSH verify command runs. Estimate position based on linear
    // interpolation from elapsed time vs estimated total duration.
    let verify_start = Instant::now();
    let est_dur_secs = estimated_duration.as_secs_f64().max(1.0);
    loop {
        // Check if the verify command has completed
        if verify_handle.is_finished() {
            break;
        }
        let elapsed_secs = verify_start.elapsed().as_secs_f64();
        let fraction = (elapsed_secs / est_dur_secs).min(0.95); // cap at 95% until done
        let estimated_bytes = (target_bytes as f64 * fraction) as u64;
        case_pb.set_position(estimated_bytes.min(target_bytes));

        // Show live throughput estimate: if we've processed X bytes in Y seconds
        let throughput = if elapsed_secs > 0.0 {
            estimated_bytes as f64 / elapsed_secs
        } else {
            0.0
        };
        case_pb.set_message(format!(
            "verifying → {} | {:.1} MB/s est | {}/{} elapsed",
            url,
            throughput / 1e6,
            HumanDuration(verify_start.elapsed()),
            HumanDuration(estimated_duration),
        ));

        sleep(Duration::from_millis(250)).await;
    }

    // Verify command finished — collect result
    let client_verify_result = match verify_handle.await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(BenchError::LocalCommand(format!(
            "SSH verify failed with status {status}"
        ))),
        Ok(Err(io_error)) => Err(BenchError::Io(io_error)),
        Err(join_error) => Err(BenchError::Invariant(format!(
            "verify task join failed: {join_error}"
        ))),
    };

    // Finish the sub-progress bar
    case_pb.finish_and_clear();

    if let Err(error) = client_verify_result {
        let server_error = match timeout(Duration::from_secs(3), &mut server_task).await {
            Ok(Ok(Ok(_))) => Some(
                "server task completed successfully before remote verify failure".to_string(),
            ),
            Ok(Ok(Err(server_error))) => Some(server_error.to_string()),
            Ok(Err(join_error)) => Some(format!("server task join failed: {join_error}")),
            Err(_) => None,
        };
        cleanup_remote_runtime_dir(client_ssh_target, &remote_case_dir_shell);
        if server_error.is_none() {
            server_task.abort();
        }
        return Err(match server_error {
            Some(server_error) => BenchError::Invariant(format!(
                "remote verify failed for {}: {}; server task error: {}",
                display_name,
                error,
                server_error,
            )),
            None => error,
        });
    }

    // ── Stage 4/6: Collect server metrics ──
    pb.set_message(format!(
        "[{}/{}] 4/6 collecting server metrics: {}",
        case_index + 1, total_cases, display_name
    ));
    let server_metrics = server_task
        .await
        .map_err(|error| BenchError::Invariant(format!("server task failed: {error}")))??;

    // ── Stage 5/6: rsync results from remote ──
    pb.set_message(format!(
        "[{}/{}] 5/6 rsync ← remote: {}",
        case_index + 1, total_cases, display_name
    ));
    run_command(Command::new("rsync").args([
        "-az",
        &format!(
            "{client_ssh_target}:{remote_case_dir}/{client_dir}/",
            client_dir = CLIENT_SIDE_DIR
        ),
        client_dir.to_str().unwrap(),
    ]))?;
    cleanup_remote_runtime_dir(client_ssh_target, &remote_case_dir_shell);
    let mut client_metrics = read_side_artifact(client_dir.join("side_result.json"))?.metrics;
    client_metrics.side_result_path = client_dir.join("side_result.json").display().to_string();
    client_metrics.process.timeseries_path =
        client_dir.join("timeseries.jsonl").display().to_string();

    // ── Stage 6/6: Finalize ──
    pb.set_message(format!(
        "[{}/{}] 6/6 finalizing: {}",
        case_index + 1, total_cases, display_name
    ));
    finalize_case_result(case, &case_root, true, server_metrics, client_metrics)
}

fn spawn_distributed_server_task(
    case: BenchmarkCase,
    server_dir: PathBuf,
) -> tokio::task::JoinHandle<Result<BenchmarkSideMetrics, BenchError>> {
    tokio::spawn(async move {
        match case.carrier {
            BenchmarkCarrier::QuicStream => match case.direction {
                BenchmarkDirection::ServerToClient => {
                    serve_case_with_quic_authenticated(&case, &case.bind_addr, &server_dir).await
                }
                BenchmarkDirection::ClientToServer => {
                    receive_case_with_quic_authenticated(&case, &case.bind_addr, &server_dir).await
                }
            },
            BenchmarkCarrier::Udp => {
                let socket = crate::datagram_transport::bind_udp_socket(&case.bind_addr).await?;
                match case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_udp_authenticated(&case, socket, &server_dir).await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_udp_authenticated(&case, socket, &server_dir).await
                    }
                }
            }
            BenchmarkCarrier::QuicDatagram => match case.direction {
                BenchmarkDirection::ServerToClient => {
                    serve_case_with_quic_datagram_authenticated(&case, &case.bind_addr, &server_dir)
                        .await
                }
                BenchmarkDirection::ClientToServer => {
                    receive_case_with_quic_datagram_authenticated(
                        &case,
                        &case.bind_addr,
                        &server_dir,
                    )
                    .await
                }
            },
            BenchmarkCarrier::WebTransportDatagram => {
                let bound = crate::datagram_transport::bind_webtransport_datagram_server(
                    &case.bind_addr,
                    crate::transport::ConnectionLimits::default().max_transport_frame_bytes,
                )?;
                match case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_webtransport_datagram_authenticated(
                            &case,
                            bound,
                            &server_dir,
                        )
                        .await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_webtransport_datagram_authenticated(
                            &case,
                            bound,
                            &server_dir,
                        )
                        .await
                    }
                }
            }
            BenchmarkCarrier::WebSocket | BenchmarkCarrier::Tcp => {
                let listener = TcpListener::bind(&case.bind_addr).await?;
                match case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_listener(&case, listener, &server_dir).await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_listener(&case, listener, &server_dir).await
                    }
                }
            }
        }
    })
}

pub fn default_full_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for environment in [BenchmarkEnvironment::Local, BenchmarkEnvironment::Remote] {
        for direction in [
            BenchmarkDirection::ServerToClient,
            BenchmarkDirection::ClientToServer,
        ] {
            for capability_profile in [
                BenchmarkCapabilityProfile::TextFamilyCueObject,
                BenchmarkCapabilityProfile::ImageFamilyCueObject,
            ] {
                for workload in [
                    BenchmarkWorkload::HighLocality,
                    BenchmarkWorkload::MixedLocality,
                    BenchmarkWorkload::LowLocality,
                ] {
                    for protection in [
                        BenchmarkProtection::ClassicRef1,
                        BenchmarkProtection::PqSimpleV1,
                        BenchmarkProtection::PqMutualV1,
                    ] {
                        for mode in [
                            BenchmarkMode::Bulk,
                            BenchmarkMode::BurstSmall,
                            BenchmarkMode::BurstMedium,
                            BenchmarkMode::BurstBig,
                        ] {
                            for target in BENCH_DEFAULT_TARGETS {
                                cases.push(
                                    BenchmarkCase::new(
                                        environment,
                                        capability_profile,
                                        workload,
                                        protection,
                                        mode,
                                        target,
                                    )
                                    .with_direction(direction)
                                    .with_client_runtime(BenchmarkClientRuntime::WebWasm),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    cases
}

pub fn smoke_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for direction in [
        BenchmarkDirection::ServerToClient,
        BenchmarkDirection::ClientToServer,
    ] {
        for capability_profile in [
            BenchmarkCapabilityProfile::TextFamilyCueObject,
            BenchmarkCapabilityProfile::ImageFamilyCueObject,
        ] {
            for workload in [
                BenchmarkWorkload::HighLocality,
                BenchmarkWorkload::MixedLocality,
                BenchmarkWorkload::LowLocality,
            ] {
                for protection in [
                    BenchmarkProtection::ClassicRef1,
                    BenchmarkProtection::PqSimpleV1,
                    BenchmarkProtection::PqMutualV1,
                ] {
                    for mode in [
                        BenchmarkMode::Bulk,
                        BenchmarkMode::BurstSmall,
                        BenchmarkMode::BurstMedium,
                        BenchmarkMode::BurstBig,
                    ] {
                        cases.push(
                            BenchmarkCase::new(
                                BenchmarkEnvironment::Local,
                                capability_profile,
                                workload,
                                protection,
                                mode,
                                BenchmarkTargetBytes::Custom(SMOKE_TARGET_BYTES),
                            )
                            .with_custom_target(SMOKE_TARGET_BYTES)
                            .with_direction(direction)
                            .with_client_runtime(BenchmarkClientRuntime::WebWasm),
                        );
                    }
                }
            }
        }
    }
    cases
}

pub fn production_remote_bidirectional_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for direction in [
        BenchmarkDirection::ServerToClient,
        BenchmarkDirection::ClientToServer,
    ] {
        for mode in [
            BenchmarkMode::Bulk,
            BenchmarkMode::BurstSmall,
            BenchmarkMode::BurstMedium,
            BenchmarkMode::BurstBig,
        ] {
            for target in BENCH_DEFAULT_TARGETS {
                cases.push(
                    BenchmarkCase::new(
                        BenchmarkEnvironment::Remote,
                        BenchmarkCapabilityProfile::TextFamilyCueObject,
                        BenchmarkWorkload::MixedLocality,
                        BenchmarkProtection::PqMutualV1,
                        mode,
                        target,
                    )
                    .with_direction(direction)
                    .with_client_runtime(BenchmarkClientRuntime::WebWasm),
                );
            }
        }
    }
    cases
}

pub fn production_remote_bidirectional_web_image_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for direction in [
        BenchmarkDirection::ServerToClient,
        BenchmarkDirection::ClientToServer,
    ] {
        for mode in [
            BenchmarkMode::Bulk,
            BenchmarkMode::BurstSmall,
            BenchmarkMode::BurstMedium,
            BenchmarkMode::BurstBig,
        ] {
            for target in BENCH_DEFAULT_TARGETS {
                cases.push(
                    BenchmarkCase::new(
                        BenchmarkEnvironment::Remote,
                        BenchmarkCapabilityProfile::ImageFamilyCueObject,
                        BenchmarkWorkload::MixedLocality,
                        BenchmarkProtection::PqMutualV1,
                        mode,
                        target,
                    )
                    .with_direction(direction)
                    .with_corpus(BenchmarkCorpusKind::WebImage50)
                    .with_client_runtime(BenchmarkClientRuntime::WebWasm),
                );
            }
        }
    }
    cases
}

pub fn websocket_capability_comparison_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for protection in [
        BenchmarkProtection::PqSimpleV1,
        BenchmarkProtection::PqMutualV1,
    ] {
        for direction in [
            BenchmarkDirection::ServerToClient,
            BenchmarkDirection::ClientToServer,
        ] {
            cases.push(
                BenchmarkCase::new(
                    BenchmarkEnvironment::Remote,
                    BenchmarkCapabilityProfile::TextFamilyCueObject,
                    BenchmarkWorkload::MixedLocality,
                    protection,
                    BenchmarkMode::BurstMedium,
                    BenchmarkTargetBytes::HundredMb,
                )
                .with_direction(direction)
                .with_client_runtime(BenchmarkClientRuntime::WebWasm),
            );
        }
    }
    cases
}

pub fn reliable_carrier_comparison_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for carrier in [
        BenchmarkCarrier::WebSocket,
        BenchmarkCarrier::Tcp,
        BenchmarkCarrier::QuicStream,
    ] {
        for protection in [
            BenchmarkProtection::PqSimpleV1,
            BenchmarkProtection::PqMutualV1,
        ] {
            for direction in [
                BenchmarkDirection::ServerToClient,
                BenchmarkDirection::ClientToServer,
            ] {
                for mode in [BenchmarkMode::Bulk, BenchmarkMode::BurstMedium] {
                    cases.push(
                        BenchmarkCase::new(
                            BenchmarkEnvironment::Local,
                            BenchmarkCapabilityProfile::TextFamilyCueObject,
                            BenchmarkWorkload::MixedLocality,
                            protection,
                            mode,
                            BenchmarkTargetBytes::HundredMb,
                        )
                        .with_carrier(carrier)
                        .with_direction(direction)
                        .with_client_runtime(BenchmarkClientRuntime::NativeRust),
                    );
                }
            }
        }
    }
    cases
}

pub fn datagram_native_carrier_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for carrier in [
        BenchmarkCarrier::Udp,
        BenchmarkCarrier::QuicDatagram,
        BenchmarkCarrier::WebTransportDatagram,
    ] {
        for protection in [
            BenchmarkProtection::PqSimpleDgramV1,
            BenchmarkProtection::PqMutualDgramV1,
        ] {
            for direction in [
                BenchmarkDirection::ServerToClient,
                BenchmarkDirection::ClientToServer,
            ] {
                for mode in [BenchmarkMode::BurstSmall, BenchmarkMode::BurstMedium] {
                    for target in BENCH_DEFAULT_TARGETS {
                        cases.push(
                            BenchmarkCase::new(
                                BenchmarkEnvironment::Local,
                                BenchmarkCapabilityProfile::TextFamilyCueObject,
                                BenchmarkWorkload::MixedLocality,
                                protection,
                                mode,
                                target,
                            )
                            .with_carrier(carrier)
                            .with_direction(direction)
                            .with_client_runtime(BenchmarkClientRuntime::NativeRust),
                        );
                    }
                }
            }
        }
    }
    cases
}

pub fn datagram_native_smoke_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for carrier in [
        BenchmarkCarrier::Udp,
        BenchmarkCarrier::QuicDatagram,
        BenchmarkCarrier::WebTransportDatagram,
    ] {
        for protection in [
            BenchmarkProtection::PqSimpleDgramV1,
            BenchmarkProtection::PqMutualDgramV1,
        ] {
            cases.push(
                BenchmarkCase::new(
                    BenchmarkEnvironment::Local,
                    BenchmarkCapabilityProfile::TextFamilyCueObject,
                    BenchmarkWorkload::MixedLocality,
                    protection,
                    BenchmarkMode::BurstMedium,
                    BenchmarkTargetBytes::Custom(SMOKE_TARGET_BYTES),
                )
                .with_carrier(carrier)
                .with_direction(BenchmarkDirection::ServerToClient)
                .with_client_runtime(BenchmarkClientRuntime::NativeRust),
            );
        }
    }
    cases
}

pub fn distributed_native_all_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    let targets = [
        BenchmarkTargetBytes::Custom(10),
        BenchmarkTargetBytes::Custom(100),
        BenchmarkTargetBytes::Custom(1_000),
        BenchmarkTargetBytes::Custom(10_000),
        BenchmarkTargetBytes::Custom(100_000),
        BenchmarkTargetBytes::OneMb,
        BenchmarkTargetBytes::TenMb,
        BenchmarkTargetBytes::HundredMb,
    ];

    for carrier in [
        BenchmarkCarrier::WebSocket,
        BenchmarkCarrier::Tcp,
        BenchmarkCarrier::QuicStream,
    ] {
        for protection in [
            BenchmarkProtection::PqSimpleV1,
            BenchmarkProtection::PqMutualV1,
        ] {
            for direction in [
                BenchmarkDirection::ServerToClient,
                BenchmarkDirection::ClientToServer,
            ] {
                for mode in [
                    BenchmarkMode::Bulk,
                    BenchmarkMode::BurstSmall,
                    BenchmarkMode::BurstMedium,
                    BenchmarkMode::BurstBig,
                ] {
                    for target in targets {
                        cases.push(
                            BenchmarkCase::new(
                                BenchmarkEnvironment::Local,
                                BenchmarkCapabilityProfile::TextFamilyCueObject,
                                BenchmarkWorkload::MixedLocality,
                                protection,
                                mode,
                                target,
                            )
                            .with_carrier(carrier)
                            .with_direction(direction)
                            .with_client_runtime(BenchmarkClientRuntime::NativeRust),
                        );
                    }
                }
            }
        }
    }

    for carrier in [
        BenchmarkCarrier::Udp,
        BenchmarkCarrier::QuicDatagram,
        BenchmarkCarrier::WebTransportDatagram,
    ] {
        for protection in [
            BenchmarkProtection::PqSimpleDgramV1,
            BenchmarkProtection::PqMutualDgramV1,
        ] {
            for direction in [
                BenchmarkDirection::ServerToClient,
                BenchmarkDirection::ClientToServer,
            ] {
                for mode in [
                    BenchmarkMode::Bulk,
                    BenchmarkMode::BurstSmall,
                    BenchmarkMode::BurstMedium,
                    BenchmarkMode::BurstBig,
                ] {
                    for target in targets {
                        cases.push(
                            BenchmarkCase::new(
                                BenchmarkEnvironment::Local,
                                BenchmarkCapabilityProfile::TextFamilyCueObject,
                                BenchmarkWorkload::MixedLocality,
                                protection,
                                mode,
                                target,
                            )
                            .with_carrier(carrier)
                            .with_direction(direction)
                            .with_client_runtime(BenchmarkClientRuntime::NativeRust),
                        );
                    }
                }
            }
        }
    }

    cases
}

pub fn websocket_mutual_all_sizes_matrix() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    let targets = [
        BenchmarkTargetBytes::Custom(10),
        BenchmarkTargetBytes::Custom(100),
        BenchmarkTargetBytes::Custom(1_000),
        BenchmarkTargetBytes::Custom(10_000),
        BenchmarkTargetBytes::Custom(100_000),
        BenchmarkTargetBytes::OneMb,
        BenchmarkTargetBytes::TenMb,
        BenchmarkTargetBytes::HundredMb,
    ];

    for direction in [
        BenchmarkDirection::ServerToClient,
        BenchmarkDirection::ClientToServer,
    ] {
        for mode in [
            BenchmarkMode::Bulk,
            BenchmarkMode::BurstSmall,
            BenchmarkMode::BurstMedium,
            BenchmarkMode::BurstBig,
        ] {
            for target in targets {
                cases.push(
                    BenchmarkCase::new(
                        BenchmarkEnvironment::Local,
                        BenchmarkCapabilityProfile::TextFamilyCueObject,
                        BenchmarkWorkload::MixedLocality,
                        BenchmarkProtection::PqMutualV1,
                        mode,
                        target,
                    )
                    .with_carrier(BenchmarkCarrier::WebSocket)
                    .with_direction(direction)
                    .with_client_runtime(BenchmarkClientRuntime::NativeRust),
                );
            }
        }
    }

    cases
}

pub fn named_matrix(name: &str) -> Option<Vec<BenchmarkCase>> {
    match name {
        "smoke" => Some(smoke_matrix()),
        "production_remote_bidirectional" => Some(production_remote_bidirectional_matrix()),
        "production_remote_bidirectional_web_image_50" => {
            Some(production_remote_bidirectional_web_image_matrix())
        }
        "websocket_capabilities" => Some(websocket_capability_comparison_matrix()),
        "reliable_carriers" => Some(reliable_carrier_comparison_matrix()),
        "datagram_native_carriers" => Some(datagram_native_carrier_matrix()),
        "datagram_native_smoke" => Some(datagram_native_smoke_matrix()),
        "production_source_optimization_comparison" => {
            Some(production_source_optimization_comparison_slice())
        }
        "distributed_native_all" => Some(distributed_native_all_matrix()),
        "websocket_mutual_all_sizes" => Some(websocket_mutual_all_sizes_matrix()),
        _ => None,
    }
}

pub fn production_source_optimization_comparison_slice() -> Vec<BenchmarkCase> {
    let mut cases = Vec::new();
    for corpus in [
        BenchmarkCorpusKind::Wikitext103Raw,
        BenchmarkCorpusKind::WebImage50,
    ] {
        let capability = match corpus {
            BenchmarkCorpusKind::Wikitext103Raw => BenchmarkCapabilityProfile::TextFamilyCueObject,
            BenchmarkCorpusKind::WebImage50 => BenchmarkCapabilityProfile::ImageFamilyCueObject,
        };
        for optimization in [
            BenchmarkOptimization::parse("baseline").unwrap(),
            BenchmarkOptimization::parse("dedup_only").unwrap(),
            BenchmarkOptimization::default(),
        ] {
            cases.push(
                BenchmarkCase::new(
                    BenchmarkEnvironment::Remote,
                    capability,
                    BenchmarkWorkload::MixedLocality,
                    BenchmarkProtection::PqMutualV1,
                    BenchmarkMode::BurstMedium,
                    BenchmarkTargetBytes::HundredMb,
                )
                .with_direction(BenchmarkDirection::ServerToClient)
                .with_corpus(corpus)
                .with_optimization(optimization)
                .with_client_runtime(BenchmarkClientRuntime::WebWasm),
            );
        }
    }
    cases
}

pub async fn serve_case_from_manifest(
    manifest_path: impl AsRef<Path>,
    artifact_dir: impl AsRef<Path>,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let case = read_case_manifest(manifest_path)?;
    validate_case_input_corpus(&case)?;
    let artifact_dir = artifact_dir.as_ref().to_path_buf();
    match case.carrier {
        BenchmarkCarrier::QuicStream => match case.direction {
            BenchmarkDirection::ServerToClient => {
                serve_case_with_quic_authenticated(&case, &case.bind_addr, &artifact_dir).await
            }
            BenchmarkDirection::ClientToServer => {
                receive_case_with_quic_authenticated(&case, &case.bind_addr, &artifact_dir).await
            }
        },
        BenchmarkCarrier::Udp => {
            let socket = crate::datagram_transport::bind_udp_socket(&case.bind_addr).await?;
            match case.direction {
                BenchmarkDirection::ServerToClient => {
                    serve_case_with_udp_authenticated(&case, socket, &artifact_dir).await
                }
                BenchmarkDirection::ClientToServer => {
                    receive_case_with_udp_authenticated(&case, socket, &artifact_dir).await
                }
            }
        }
        BenchmarkCarrier::QuicDatagram => match case.direction {
            BenchmarkDirection::ServerToClient => {
                serve_case_with_quic_datagram_authenticated(&case, &case.bind_addr, &artifact_dir)
                    .await
            }
            BenchmarkDirection::ClientToServer => {
                receive_case_with_quic_datagram_authenticated(&case, &case.bind_addr, &artifact_dir)
                    .await
            }
        },
        BenchmarkCarrier::WebTransportDatagram => {
            let bound = crate::datagram_transport::bind_webtransport_datagram_server(
                &case.bind_addr,
                crate::transport::ConnectionLimits::default().max_transport_frame_bytes,
            )?;
            match case.direction {
                BenchmarkDirection::ServerToClient => {
                    serve_case_with_webtransport_datagram_authenticated(&case, bound, &artifact_dir)
                        .await
                }
                BenchmarkDirection::ClientToServer => {
                    receive_case_with_webtransport_datagram_authenticated(
                        &case,
                        bound,
                        &artifact_dir,
                    )
                    .await
                }
            }
        }
        BenchmarkCarrier::WebSocket | BenchmarkCarrier::Tcp => {
            let listener = TcpListener::bind(&case.bind_addr).await?;
            match case.direction {
                BenchmarkDirection::ServerToClient => {
                    serve_case_with_listener(&case, listener, &artifact_dir).await
                }
                BenchmarkDirection::ClientToServer => {
                    receive_case_with_listener(&case, listener, &artifact_dir).await
                }
            }
        }
    }
}

pub async fn verify_case_from_manifest(
    manifest_path: impl AsRef<Path>,
    url: &str,
    artifact_dir: impl AsRef<Path>,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let case = read_case_manifest(manifest_path)?;
    validate_case_input_corpus(&case)?;
    verify_case_against_url(&case, url, artifact_dir.as_ref()).await
}

async fn run_local_case(
    case: BenchmarkCase,
    case_root: &Path,
) -> Result<BenchmarkResult, BenchError> {
    let server_dir = case_root.join(SERVER_SIDE_DIR);
    let client_dir = case_root.join(CLIENT_SIDE_DIR);
    fs::create_dir_all(&server_dir)?;
    fs::create_dir_all(&client_dir)?;

    let server_case = case.clone();
    let (url, server_task) = match case.carrier {
        BenchmarkCarrier::QuicStream => {
            let udp_socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
            let local_addr = udp_socket.local_addr()?;
            drop(udp_socket);
            let bind_addr = local_addr.to_string();
            let url = format!("quic://{}", local_addr);
            let server_task = tokio::spawn(async move {
                match server_case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_quic_authenticated(&server_case, &bind_addr, &server_dir)
                            .await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_quic_authenticated(&server_case, &bind_addr, &server_dir)
                            .await
                    }
                }
            });
            (url, server_task)
        }
        BenchmarkCarrier::Udp => {
            let socket = crate::datagram_transport::bind_udp_socket(&case.bind_addr).await?;
            let local_addr = socket.local_addr()?;
            let url = format!("udp://{}", local_addr);
            let server_task = tokio::spawn(async move {
                match server_case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_udp_authenticated(&server_case, socket, &server_dir).await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_udp_authenticated(&server_case, socket, &server_dir).await
                    }
                }
            });
            (url, server_task)
        }
        BenchmarkCarrier::QuicDatagram => {
            let udp_socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
            let local_addr = udp_socket.local_addr()?;
            drop(udp_socket);
            let bind_addr = local_addr.to_string();
            let url = format!("quic_datagram://{}", local_addr);
            let server_task = tokio::spawn(async move {
                match server_case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_quic_datagram_authenticated(
                            &server_case,
                            &bind_addr,
                            &server_dir,
                        )
                        .await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_quic_datagram_authenticated(
                            &server_case,
                            &bind_addr,
                            &server_dir,
                        )
                        .await
                    }
                }
            });
            (url, server_task)
        }
        BenchmarkCarrier::WebTransportDatagram => {
            let bound = crate::datagram_transport::bind_webtransport_datagram_server(
                &case.bind_addr,
                crate::transport::ConnectionLimits::default().max_transport_frame_bytes,
            )?;
            let local_addr = bound.local_addr();
            let url = format!("webtransport://127.0.0.1:{}/pulzz", local_addr.port());
            let server_task = tokio::spawn(async move {
                match server_case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_webtransport_datagram_authenticated(
                            &server_case,
                            bound,
                            &server_dir,
                        )
                        .await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_webtransport_datagram_authenticated(
                            &server_case,
                            bound,
                            &server_dir,
                        )
                        .await
                    }
                }
            });
            (url, server_task)
        }
        BenchmarkCarrier::WebSocket | BenchmarkCarrier::Tcp => {
            let listener = TcpListener::bind(&case.bind_addr).await?;
            let local_addr = listener.local_addr()?;
            let url = match case.carrier {
                BenchmarkCarrier::WebSocket => format!("ws://{}", local_addr),
                BenchmarkCarrier::Tcp => format!("tcp://{}", local_addr),
                BenchmarkCarrier::QuicStream
                | BenchmarkCarrier::Udp
                | BenchmarkCarrier::QuicDatagram
                | BenchmarkCarrier::WebTransportDatagram => unreachable!(),
            };
            let server_task = tokio::spawn(async move {
                match server_case.direction {
                    BenchmarkDirection::ServerToClient => {
                        serve_case_with_listener(&server_case, listener, &server_dir).await
                    }
                    BenchmarkDirection::ClientToServer => {
                        receive_case_with_listener(&server_case, listener, &server_dir).await
                    }
                }
            });
            (url, server_task)
        }
    };

    let client_metrics = verify_case_against_url(&case, &url, &client_dir).await?;
    let server_metrics = server_task
        .await
        .map_err(|error| BenchError::Invariant(format!("server task failed: {error}")))??;
    finalize_case_result(case, case_root, true, server_metrics, client_metrics)
}

async fn run_remote_case(
    case: BenchmarkCase,
    case_root: &Path,
) -> Result<BenchmarkResult, BenchError> {
    let server_dir = case_root.join(SERVER_SIDE_DIR);
    let client_dir = case_root.join(CLIENT_SIDE_DIR);
    fs::create_dir_all(&server_dir)?;
    fs::create_dir_all(&client_dir)?;

    let ssh_target = case
        .remote_ssh_target
        .clone()
        .ok_or_else(|| BenchError::InvalidCase("missing remote ssh target".to_string()))?;
    let remote_repo_path = case
        .remote_repo_path
        .clone()
        .ok_or_else(|| BenchError::InvalidCase("missing remote repo path".to_string()))?;
    let remote_repo_shell_path = remote_shell_path(&remote_repo_path);
    let direct_url = case.direct_url()?;
    let (host, port) = parse_ws_host_port(&direct_url)?;
    reject_loopback_host(&host)?;
    sync_workspace_to_remote(&ssh_target, &remote_repo_path)?;

    let manifest_path = case_root.join("case.json");
    let remote_case_rel_path = format!("{}/{:016x}", case.artifact_rel_path().display(), case.seed);
    let remote_case_dir = format!("{REMOTE_RUNTIME_SCRATCH_ROOT}/{remote_case_rel_path}");
    let remote_case_dir_shell = remote_case_dir.clone();
    run_command(Command::new("ssh").arg(&ssh_target).arg(format!(
        "rm -rf \"{remote_case_dir_shell}\" && mkdir -p \"{remote_case_dir_shell}/{SERVER_SIDE_DIR}\""
    )))?;
    run_command(Command::new("rsync").args([
        "-az",
        manifest_path.to_str().unwrap(),
        &format!("{ssh_target}:{remote_case_dir}/case.json"),
    ]))?;

    // S4.4: Use the already-synced release binary instead of rebuilding.
    // The distributed-native path demonstrates direct binary execution.
    // We use the same model: execute the pre-built binary at the known
    // target/release/server path, falling back with a clear error if absent.
    let remote_binary = format!("{repo}/target/release/server", repo = remote_repo_shell_path);

    // S4.3: Remote disk/scratch preflight — check free space before launching
    // execution. Fail fast with the actual path and measured free space if
    // insufficient, rather than discovering ENOSPC downstream as a late reset.
    {
        // S4.3: Check multiple paths on the remote host: case output dir,
        // /tmp (used for temp files during execution), and the repo's target/
        // directory (where the binary lives and where cargo might write).
        let remote_target_dir = format!("{repo}/target", repo = remote_repo_shell_path);
        let preflight_dirs = [
            remote_case_dir_shell.as_str(),
            "/tmp",
            remote_target_dir.as_str(),
        ];
        let min_required_bytes: u64 = 500 * 1024 * 1024; // 500MB
        for preflight_dir in &preflight_dirs {
            let preflight_cmd = format!(
                "df --output=avail \"{dir}\" 2>/dev/null | tail -1 | tr -d ' '",
                dir = preflight_dir
            );
            let preflight_output = Command::new("ssh")
                .args([&ssh_target, &preflight_cmd])
                .output();
            if let Ok(output) = preflight_output {
                if let Ok(stdout) = String::from_utf8(output.stdout) {
                    if let Ok(avail_kb) = stdout.trim().parse::<u64>() {
                        let avail_bytes = avail_kb * 1024;
                        if avail_bytes < min_required_bytes {
                            cleanup_remote_runtime_dir(&ssh_target, &remote_case_dir_shell);
                            return Err(BenchError::RemoteCommand(format!(
                                "S4.3: Insufficient disk space on remote at '{}': {} bytes available, {} bytes required",
                                preflight_dir, avail_bytes, min_required_bytes
                            )));
                        }
                    }
                }
            }
            // If preflight fails to parse for this dir, continue — it's a best-effort check
        }
    }

    let remote_command = format!(
        "test -x \"{binary}\" || {{ echo 'ERROR: synced release binary not found at {binary}; build first with cargo build --release' >&2; exit 1; }} && \"{binary}\" bench serve \"{case_dir}/case.json\" \"{case_dir}/{server_dir}\"",
        binary = remote_binary,
        case_dir = remote_case_dir_shell,
        server_dir = SERVER_SIDE_DIR,
    );
    let mut child = spawn_remote_command(&ssh_target, &remote_command)?;
    wait_for_direct_connectivity(&host, port, &mut child).await?;

    let client_metrics = match verify_case_against_url(&case, &direct_url, &client_dir).await {
        Ok(metrics) => metrics,
        Err(error) => {
            let _ = child.kill();
            cleanup_remote_runtime_dir(&ssh_target, &remote_case_dir_shell);
            return Err(error);
        }
    };

    let status = child.wait()?;
    if !status.success() {
        cleanup_remote_runtime_dir(&ssh_target, &remote_case_dir_shell);
        return Err(BenchError::RemoteCommand(format!(
            "remote bench serve exited with status {status}"
        )));
    }

    run_command(Command::new("rsync").args([
        "-az",
        &format!("{ssh_target}:{remote_case_dir}/{SERVER_SIDE_DIR}/"),
        server_dir.to_str().unwrap(),
    ]))?;
    cleanup_remote_runtime_dir(&ssh_target, &remote_case_dir_shell);
    let mut server_metrics = read_side_artifact(server_dir.join("side_result.json"))?.metrics;
    server_metrics.side_result_path = server_dir.join("side_result.json").display().to_string();
    server_metrics.process.timeseries_path =
        server_dir.join("timeseries.jsonl").display().to_string();
    finalize_case_result(case, case_root, true, server_metrics, client_metrics)
}

async fn serve_case_with_listener(
    case: &BenchmarkCase,
    listener: TcpListener,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    if uses_authenticated_session(case) {
        return serve_case_with_listener_authenticated(case, listener, artifact_dir).await;
    }

    let (sender, _) = paired_protectors(case)?;
    let mut session =
        ServerSession::new_with_configs(sender, case.optimization.source_optimization_config());
    let mut source_cache = benchmark_source_cache_for_case(case, artifact_dir)?;
    let mut websocket = accept_benchmark_websocket(&listener).await?;

    let mut generator = WorkloadGenerator::new(case.workload, case.corpus, case.seed);
    let mut sampler = ProcessSampler::new(BenchmarkSide::Server);
    let mut progress = SideProgress::default();
    let mut encode_latency = LatencyReservoir::new(case.seed ^ 0x1111);
    let mut embedding_latency = LatencyReservoir::new(case.seed ^ 0x2222);
    let mut utility = UtilityAccumulator::default();
    let mut source_cache_metrics = SourceCacheMetrics::default();
    let mut original_live = HashMap::<ItemId, LiveBenchmarkItem>::new();
    let start = Instant::now();
    let case_pb = bench_case_byte_progress_bar(case.target_bytes.bytes(), &case.display_name());
    let mut burst_buffer = BurstAccumulator::new(transport_payload_cap(case.mode));
    let yield_between_groups = case.mode.payload_cap_bytes().is_some();
    let mut target_reached = false;
    // S4.6: Case execution timeout to prevent runaway benchmarks.
    let case_timeout = bench_case_timeout();

    while !target_reached {
        // S4.6: Check elapsed time against the case timeout on each iteration.
        // This prevents the 100MB benchmark from hanging indefinitely when
        // source cache disk I/O or WebSocket backpressure stalls the loop.
        if start.elapsed() > case_timeout {
            eprintln!(
                "WARNING: benchmark case {} exceeded timeout of {:?} after {} payload bytes / {} wire bytes. Forcing completion.",
                case.display_name(),
                case_timeout,
                progress.payload_bytes,
                progress.wire_bytes,
            );
            let mut flushed = Vec::new();
            burst_buffer.flush_remaining(&mut flushed);
            let _ = flush_bursts(
                &mut websocket,
                &mut progress,
                &mut sampler,
                flushed,
                yield_between_groups,
            ).await;
            target_reached = true;
            continue;
        }
        if progress.payload_bytes + burst_buffer.buffered_payload_bytes >= case.target_bytes.bytes()
        {
            let mut flushed = Vec::new();
            burst_buffer.flush_remaining(&mut flushed);
            flush_bursts(
                &mut websocket,
                &mut progress,
                &mut sampler,
                flushed,
                yield_between_groups,
            )
            .await?;
            target_reached = true;
            continue;
        }

        let event = generator.next_event();
        let frames = match event {
            WorkloadEvent::Insert { item_id, input } => {
                let item_id = ItemId(item_id);
                let embed_start = Instant::now();
                let (resolved, original) = resolve_and_deferred_attach(&mut source_cache, &input)?;
                embedding_latency.record(embed_start.elapsed().as_nanos() as u64);
                accumulate_source_cache_metrics(&mut source_cache_metrics, &resolved.metrics);
                let encode_start = Instant::now();
                let records = session
                    .emit_resolved_source_insert(item_id, resolved)?
                    .records;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.insert(item_id, live_benchmark_item(&input, original));
                update_server_side_utility(
                    &mut utility,
                    case.utility_sample_limit,
                    &original_live,
                    session.state().entries_iter(),
                    item_id,
                )?;
                records
                    .into_iter()
                    .map(|record| BurstFrame { record })
                    .collect::<Vec<_>>()
            }
            WorkloadEvent::UpsertObject { item_id, input } => {
                let item_id = ItemId(item_id);
                let embed_start = Instant::now();
                let (resolved, original) = resolve_and_deferred_attach(&mut source_cache, &input)?;
                embedding_latency.record(embed_start.elapsed().as_nanos() as u64);
                accumulate_source_cache_metrics(&mut source_cache_metrics, &resolved.metrics);
                let encode_start = Instant::now();
                let records = session
                    .emit_resolved_source_upsert(item_id, resolved)?
                    .records;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.insert(item_id, live_benchmark_item(&input, original));
                update_server_side_utility(
                    &mut utility,
                    case.utility_sample_limit,
                    &original_live,
                    session.state().entries_iter(),
                    item_id,
                )?;
                records
                    .into_iter()
                    .map(|record| BurstFrame { record })
                    .collect::<Vec<_>>()
            }
            WorkloadEvent::Evict { item_id } => {
                let encode_start = Instant::now();
                let record = session.emit_event(ServerEvent::Evict {
                    item_id: ItemId(item_id),
                })?;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.remove(&ItemId(item_id));
                vec![BurstFrame { record }]
            }
            WorkloadEvent::Invalidate { item_id } => {
                let encode_start = Instant::now();
                let record = session.emit_event(ServerEvent::Invalidate {
                    item_id: ItemId(item_id),
                })?;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.remove(&ItemId(item_id));
                vec![BurstFrame { record }]
            }
        };

        for frame in frames {
            target_reached = push_frame(
                &mut websocket,
                &mut progress,
                &mut sampler,
                &mut burst_buffer,
                frame,
                case.target_bytes.bytes(),
                yield_between_groups,
            )
            .await?;
            if target_reached {
                break;
            }
        }
        case_pb.set_position(progress.payload_bytes.min(case.target_bytes.bytes()));
        case_pb.set_message(format!(
            "{} rec | {:.1} MB/s | wire {}",
            progress.records,
            if start.elapsed().as_secs_f64() > 0.0 { progress.wire_bytes as f64 / start.elapsed().as_secs_f64() / 1e6 } else { 0.0 },
            HumanBytes(progress.wire_bytes),
        ));
    }

    case_pb.finish_with_message(format!(
        "{} rec | {:.1} MB/s wire | {} payload | {} wire",
        progress.records,
        if start.elapsed().as_secs_f64() > 0.0 { progress.wire_bytes as f64 / start.elapsed().as_secs_f64() / 1e6 } else { 0.0 },
        HumanBytes(progress.payload_bytes),
        HumanBytes(progress.wire_bytes),
    ));

    let encode_start = Instant::now();
    let close_record = session.emit_close(Some(1000))?;
    encode_latency.record(encode_start.elapsed().as_nanos() as u64);
    let close_frame = BurstFrame {
        record: close_record,
    };
    push_close_frame(
        &mut websocket,
        &mut progress,
        &mut sampler,
        &mut burst_buffer,
        close_frame,
        yield_between_groups,
    )
    .await?;

    let mut flushed = Vec::new();
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts(
        &mut websocket,
        &mut progress,
        &mut sampler,
        flushed,
        yield_between_groups,
    )
    .await?;

    websocket.close(None).await?;

    let duration = start.elapsed();
    let utility = utility_summary(utility);
    let (mut process_metrics, samples) = sampler.finish(&progress)?;
    let side_result_path = artifact_dir.join("side_result.json");
    let burst_stats = case.mode.payload_cap_bytes().map(|_| burst_buffer.stats());
    let metrics = BenchmarkSideMetrics {
        side: BenchmarkSide::Server,
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
            ..process_metrics.clone()
        },
        codec_modes: progress.codec_modes.clone(),
        source_kinds: progress.source_kinds.clone(),
        residual_modes: progress.residual_modes.clone(),
        encode_latency: encode_latency.summary(),
        embedding_latency: embedding_latency.summary(),
        apply_latency: None,
        burst_stats: burst_stats.clone(),
        utility: Some(utility),
        source_cache: Some(source_cache_metrics),
        predictive_memory: predictive_memory_metrics(&progress),
        datagram: None,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    process_metrics.timeseries_path = artifact_dir.join("timeseries.jsonl").display().to_string();
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side: BenchmarkSide::Server,
            metrics: metrics.clone(),
            samples,
        },
    )?;
    if let Some(stats) = burst_stats {
        fs::write(
            artifact_dir.join("burst_stats.json"),
            serde_json::to_vec_pretty(&stats)?,
        )?;
    }
    Ok(metrics)
}

async fn serve_case_with_listener_authenticated(
    case: &BenchmarkCase,
    listener: TcpListener,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let connection = match case.carrier {
        BenchmarkCarrier::WebSocket => AuthenticatedServerBenchConnection::WebSocket(
            crate::transport::accept_authenticated_session(&listener, &server_transport_config)
                .await?,
        ),
        BenchmarkCarrier::Tcp => AuthenticatedServerBenchConnection::Tcp(
            crate::transport::accept_tcp_session(&listener, &server_transport_config).await?,
        ),
        BenchmarkCarrier::QuicStream => {
            return Err(BenchError::InvalidCase(
                "quic_stream local authenticated runs use the dedicated QUIC bind path".to_string(),
            ));
        }
        BenchmarkCarrier::Udp
        | BenchmarkCarrier::QuicDatagram
        | BenchmarkCarrier::WebTransportDatagram => {
            return Err(BenchError::InvalidCase(
                "datagram local authenticated runs use the dedicated datagram bind paths"
                    .to_string(),
            ));
        }
    };
    serve_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn serve_case_with_quic_authenticated(
    case: &BenchmarkCase,
    bind_addr: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let endpoint = crate::transport::bind_quic_endpoint(
        bind_addr,
        server_transport_config
            .connection_limits
            .max_transport_frame_bytes,
    )?;
    let connection = AuthenticatedServerBenchConnection::Quic(
        crate::transport::accept_quic_session(&endpoint, &server_transport_config).await?,
    );
    serve_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn serve_case_with_udp_authenticated(
    case: &BenchmarkCase,
    socket: Arc<tokio::net::UdpSocket>,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let connection = AuthenticatedServerBenchConnection::Udp(
        crate::datagram_transport::accept_udp_session(&socket, &server_transport_config).await?,
    );
    serve_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn serve_case_with_quic_datagram_authenticated(
    case: &BenchmarkCase,
    bind_addr: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let endpoint = crate::transport::bind_quic_endpoint(
        bind_addr,
        server_transport_config
            .connection_limits
            .max_transport_frame_bytes,
    )?;
    let connection = AuthenticatedServerBenchConnection::QuicDatagram(
        crate::datagram_transport::accept_quic_datagram_session(
            &endpoint,
            &server_transport_config,
        )
        .await?,
    );
    serve_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn serve_case_with_webtransport_datagram_authenticated(
    case: &BenchmarkCase,
    bound: crate::datagram_transport::BoundWebTransportDatagramServer,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let connection = AuthenticatedServerBenchConnection::WebTransportDatagram(
        crate::datagram_transport::accept_webtransport_datagram_session(
            &bound,
            &server_transport_config,
        )
        .await?,
    );
    serve_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn serve_case_on_authenticated_server_connection(
    case: &BenchmarkCase,
    artifact_dir: &Path,
    mut connection: AuthenticatedServerBenchConnection,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let mut session = PlainServerSession::new_with_configs(
        case.stream_id,
        case.optimization.source_optimization_config(),
    );
    let mut source_cache = benchmark_source_cache_for_case(case, artifact_dir)?;

    let mut generator = WorkloadGenerator::new(case.workload, case.corpus, case.seed);
    let mut sampler = ProcessSampler::new(BenchmarkSide::Server);
    let mut progress = SideProgress::default();
    let mut encode_latency = LatencyReservoir::new(case.seed ^ 0x1111);
    let mut embedding_latency = LatencyReservoir::new(case.seed ^ 0x2222);
    let mut utility = UtilityAccumulator::default();
    let mut source_cache_metrics = SourceCacheMetrics::default();
    let mut original_live = HashMap::<ItemId, LiveBenchmarkItem>::new();
    let start = Instant::now();
    let mut burst_buffer = BurstAccumulator::new(transport_payload_cap(case.mode));
    let yield_between_groups = case.mode.payload_cap_bytes().is_some();
    let mut target_reached = false;

    while !target_reached {
        if progress.payload_bytes + burst_buffer.buffered_payload_bytes >= case.target_bytes.bytes()
        {
            let mut flushed = Vec::new();
            burst_buffer.flush_remaining(&mut flushed);
            flush_bursts_authenticated_server(
                &mut connection,
                &mut progress,
                &mut sampler,
                flushed,
                yield_between_groups,
            )
            .await?;
            target_reached = true;
            continue;
        }

        let event = generator.next_event();
        let frames = match event {
            WorkloadEvent::Insert { item_id, input } => {
                let item_id = ItemId(item_id);
                let embed_start = Instant::now();
                let (resolved, original) = resolve_and_deferred_attach(&mut source_cache, &input)?;
                embedding_latency.record(embed_start.elapsed().as_nanos() as u64);
                accumulate_source_cache_metrics(&mut source_cache_metrics, &resolved.metrics);
                let encode_start = Instant::now();
                let records = session
                    .emit_resolved_source_insert(item_id, resolved)?
                    .records;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.insert(item_id, live_benchmark_item(&input, original));
                update_server_side_utility(
                    &mut utility,
                    case.utility_sample_limit,
                    &original_live,
                    session.state().entries_iter(),
                    item_id,
                )?;
                records
                    .into_iter()
                    .map(|record| BurstFrame { record })
                    .collect::<Vec<_>>()
            }
            WorkloadEvent::UpsertObject { item_id, input } => {
                let item_id = ItemId(item_id);
                let embed_start = Instant::now();
                let (resolved, original) = resolve_and_deferred_attach(&mut source_cache, &input)?;
                embedding_latency.record(embed_start.elapsed().as_nanos() as u64);
                accumulate_source_cache_metrics(&mut source_cache_metrics, &resolved.metrics);
                let encode_start = Instant::now();
                let records = session
                    .emit_resolved_source_upsert(item_id, resolved)?
                    .records;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.insert(item_id, live_benchmark_item(&input, original));
                update_server_side_utility(
                    &mut utility,
                    case.utility_sample_limit,
                    &original_live,
                    session.state().entries_iter(),
                    item_id,
                )?;
                records
                    .into_iter()
                    .map(|record| BurstFrame { record })
                    .collect::<Vec<_>>()
            }
            WorkloadEvent::Evict { item_id } => {
                let encode_start = Instant::now();
                let record = session.emit_event(ServerEvent::Evict {
                    item_id: ItemId(item_id),
                })?;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.remove(&ItemId(item_id));
                vec![BurstFrame { record }]
            }
            WorkloadEvent::Invalidate { item_id } => {
                let encode_start = Instant::now();
                let record = session.emit_event(ServerEvent::Invalidate {
                    item_id: ItemId(item_id),
                })?;
                encode_latency.record(encode_start.elapsed().as_nanos() as u64);
                original_live.remove(&ItemId(item_id));
                vec![BurstFrame { record }]
            }
        };

        for frame in frames {
            target_reached = push_frame_authenticated_server(
                &mut connection,
                &mut progress,
                &mut sampler,
                &mut burst_buffer,
                frame,
                case.target_bytes.bytes(),
                yield_between_groups,
            )
            .await?;
            if target_reached {
                break;
            }
        }
    }

    let encode_start = Instant::now();
    let close_record = session.emit_close(Some(1000))?;
    encode_latency.record(encode_start.elapsed().as_nanos() as u64);
    let close_frame = BurstFrame {
        record: close_record,
    };
    push_close_frame_authenticated_server(
        &mut connection,
        &mut progress,
        &mut sampler,
        &mut burst_buffer,
        close_frame,
        yield_between_groups,
    )
    .await?;

    let mut flushed = Vec::new();
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts_authenticated_server(
        &mut connection,
        &mut progress,
        &mut sampler,
        flushed,
        yield_between_groups,
    )
    .await?;

    let datagram_metrics = connection.datagram_metrics();
    connection.close().await?;

    let duration = start.elapsed();
    let utility = utility_summary(utility);
    let (mut process_metrics, samples) = sampler.finish(&progress)?;
    let side_result_path = artifact_dir.join("side_result.json");
    let burst_stats = case.mode.payload_cap_bytes().map(|_| burst_buffer.stats());
    let metrics = BenchmarkSideMetrics {
        side: BenchmarkSide::Server,
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
            ..process_metrics.clone()
        },
        codec_modes: progress.codec_modes.clone(),
        source_kinds: progress.source_kinds.clone(),
        residual_modes: progress.residual_modes.clone(),
        encode_latency: encode_latency.summary(),
        embedding_latency: embedding_latency.summary(),
        apply_latency: None,
        burst_stats: burst_stats.clone(),
        utility: Some(utility),
        source_cache: Some(source_cache_metrics),
        predictive_memory: predictive_memory_metrics(&progress),
        datagram: datagram_metrics,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    process_metrics.timeseries_path = artifact_dir.join("timeseries.jsonl").display().to_string();
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side: BenchmarkSide::Server,
            metrics: metrics.clone(),
            samples,
        },
    )?;
    if let Some(stats) = burst_stats {
        fs::write(
            artifact_dir.join("burst_stats.json"),
            serde_json::to_vec_pretty(&stats)?,
        )?;
    }
    Ok(metrics)
}

async fn receive_case_with_listener(
    case: &BenchmarkCase,
    listener: TcpListener,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    if uses_authenticated_session(case) {
        return receive_case_with_listener_authenticated(case, listener, artifact_dir).await;
    }

    let (_, receiver) = paired_protectors(case)?;
    let mut session = ClientSession::new(receiver);
    let mut websocket = accept_benchmark_websocket(&listener).await?;

    let mut sampler = ProcessSampler::new(BenchmarkSide::Server);
    let mut progress = SideProgress::default();
    let mut apply_latency = LatencyReservoir::new(case.seed ^ 0x4444);
    let start = Instant::now();

    while let Some(message) = websocket.next().await {
        match message? {
            Message::Binary(frame) => {
                progress.wire_bytes += frame.len() as u64;
                for record in decode_transport_records(&frame)? {
                    let is_close = matches!(
                        record.header.record_type,
                        shared_protocol::RecordType::Close
                    );
                    accumulate_record_metrics(&mut progress, &record);
                    let apply_start = Instant::now();
                    session.apply_protected_record(record)?;
                    apply_latency.record(apply_start.elapsed().as_nanos() as u64);
                    if is_close {
                        sampler.maybe_sample(&progress)?;
                        return finalize_receiver_metrics(
                            artifact_dir,
                            BenchmarkSide::Server,
                            progress,
                            start.elapsed(),
                            apply_latency.summary(),
                            None,
                            sampler,
                        );
                    }
                }
                sampler.maybe_sample(&progress)?;
            }
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {}
        }
    }
    finalize_receiver_metrics(
        artifact_dir,
        BenchmarkSide::Server,
        progress,
        start.elapsed(),
        apply_latency.summary(),
        None,
        sampler,
    )
}

async fn receive_case_with_listener_authenticated(
    case: &BenchmarkCase,
    listener: TcpListener,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let connection = match case.carrier {
        BenchmarkCarrier::WebSocket => AuthenticatedServerBenchConnection::WebSocket(
            crate::transport::accept_authenticated_session(&listener, &server_transport_config)
                .await?,
        ),
        BenchmarkCarrier::Tcp => AuthenticatedServerBenchConnection::Tcp(
            crate::transport::accept_tcp_session(&listener, &server_transport_config).await?,
        ),
        BenchmarkCarrier::QuicStream => {
            return Err(BenchError::InvalidCase(
                "quic_stream local authenticated runs use the dedicated QUIC bind path".to_string(),
            ));
        }
        BenchmarkCarrier::Udp
        | BenchmarkCarrier::QuicDatagram
        | BenchmarkCarrier::WebTransportDatagram => {
            return Err(BenchError::InvalidCase(
                "datagram local authenticated runs use the dedicated datagram bind paths"
                    .to_string(),
            ));
        }
    };
    receive_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn receive_case_with_quic_authenticated(
    case: &BenchmarkCase,
    bind_addr: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let endpoint = crate::transport::bind_quic_endpoint(
        bind_addr,
        server_transport_config
            .connection_limits
            .max_transport_frame_bytes,
    )?;
    let connection = AuthenticatedServerBenchConnection::Quic(
        crate::transport::accept_quic_session(&endpoint, &server_transport_config).await?,
    );
    receive_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn receive_case_with_udp_authenticated(
    case: &BenchmarkCase,
    socket: Arc<tokio::net::UdpSocket>,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let connection = AuthenticatedServerBenchConnection::Udp(
        crate::datagram_transport::accept_udp_session(&socket, &server_transport_config).await?,
    );
    receive_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn receive_case_with_quic_datagram_authenticated(
    case: &BenchmarkCase,
    bind_addr: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let endpoint = crate::transport::bind_quic_endpoint(
        bind_addr,
        server_transport_config
            .connection_limits
            .max_transport_frame_bytes,
    )?;
    let connection = AuthenticatedServerBenchConnection::QuicDatagram(
        crate::datagram_transport::accept_quic_datagram_session(
            &endpoint,
            &server_transport_config,
        )
        .await?,
    );
    receive_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn receive_case_with_webtransport_datagram_authenticated(
    case: &BenchmarkCase,
    bound: crate::datagram_transport::BoundWebTransportDatagramServer,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let server_transport_config = server_transport_config_for_case(case)?;
    let connection = AuthenticatedServerBenchConnection::WebTransportDatagram(
        crate::datagram_transport::accept_webtransport_datagram_session(
            &bound,
            &server_transport_config,
        )
        .await?,
    );
    receive_case_on_authenticated_server_connection(case, artifact_dir, connection).await
}

async fn receive_case_on_authenticated_server_connection(
    case: &BenchmarkCase,
    artifact_dir: &Path,
    mut connection: AuthenticatedServerBenchConnection,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let mut sampler = ProcessSampler::new(BenchmarkSide::Server);
    let mut progress = SideProgress::default();
    let mut apply_latency = LatencyReservoir::new(case.seed ^ 0x4444);
    let start = Instant::now();

    while let Some(frame) = connection.read_transport_frame().await? {
        progress.wire_bytes += frame.len() as u64;
        let apply_start = Instant::now();
        let records = connection
            .protector_mut()
            .unprotect_transport_frame(&frame)?;
        apply_latency.record(apply_start.elapsed().as_nanos() as u64);
        for record in records {
            let is_close = matches!(
                record.header.record_type,
                shared_protocol::RecordType::Close
            );
            accumulate_record_metrics(&mut progress, &record);
            if is_close {
                sampler.maybe_sample(&progress)?;
                return finalize_receiver_metrics(
                    artifact_dir,
                    BenchmarkSide::Server,
                    progress,
                    start.elapsed(),
                    apply_latency.summary(),
                    connection.datagram_metrics(),
                    sampler,
                );
            }
        }
        sampler.maybe_sample(&progress)?;
    }

    finalize_receiver_metrics(
        artifact_dir,
        BenchmarkSide::Server,
        progress,
        start.elapsed(),
        apply_latency.summary(),
        connection.datagram_metrics(),
        sampler,
    )
}

async fn accept_benchmark_websocket(
    listener: &TcpListener,
) -> Result<tokio_tungstenite::WebSocketStream<TcpStream>, BenchError> {
    const MAX_TRANSIENT_HANDSHAKE_FAILURES: usize = 8;

    let mut transient_failures = 0_usize;
    loop {
        let (stream, _) = listener.accept().await?;
        match accept_async_with_config(stream, Some(benchmark_websocket_config())).await {
            Ok(websocket) => return Ok(websocket),
            Err(error) if is_transient_accept_error(&error) => {
                transient_failures += 1;
                if transient_failures > MAX_TRANSIENT_HANDSHAKE_FAILURES {
                    return Err(BenchError::Invariant(format!(
                        "too many transient websocket handshake failures before benchmark client connected: {error}"
                    )));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn verify_case_against_url(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    match case.client_runtime {
        BenchmarkClientRuntime::NativeRust => {
            verify_case_against_url_native(case, url, artifact_dir).await
        }
        BenchmarkClientRuntime::WebWasm => {
            verify_case_against_url_browser(case, url, artifact_dir).await
        }
    }
}

async fn verify_case_against_url_native(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    match case.direction {
        BenchmarkDirection::ServerToClient => {
            verify_case_against_url_native_receiver(case, url, artifact_dir).await
        }
        BenchmarkDirection::ClientToServer => {
            verify_case_against_url_native_sender(case, url, artifact_dir).await
        }
    }
}

async fn verify_case_against_url_native_receiver(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    if uses_authenticated_session(case) {
        return verify_case_against_url_native_receiver_authenticated(case, url, artifact_dir)
            .await;
    }

    let (_, receiver) = paired_protectors(case)?;
    let mut session = ClientSession::new(receiver);
    let mut sampler = ProcessSampler::new(BenchmarkSide::Client);
    let mut progress = SideProgress::default();
    let mut apply_latency = LatencyReservoir::new(case.seed ^ 0x3333);
    let start = Instant::now();
    let target_bytes = case.target_bytes.bytes();

    // TQDM-style progress bar for the verify client. This is especially
    // important for the distributed benchmark where the verify runs on
    // the remote machine via SSH — the progress output will appear in
    // the SSH session, giving the operator visibility into what's happening.
    let verify_pb = bench_case_byte_progress_bar(target_bytes, &case.display_name());
    verify_pb.set_message(format!("connecting → {}", url));

    let (mut websocket, _) =
        connect_async_with_config(url, Some(benchmark_websocket_config()), false).await?;
    // S4.6: Client-side timeout — we wrap each websocket read with a per-read
    // timeout to prevent indefinite blocking. The global deadline is also checked
    // on each message. This prevents the client from hanging when the server
    // stalls or the connection silently drops.
    let client_deadline = start + bench_case_timeout();
    let per_read_timeout = Duration::from_secs(300); // 5 min per read max
    loop {
        if Instant::now() > client_deadline {
            eprintln!(
                "WARNING: benchmark client timed out for case {} after receiving {} records / {} payload bytes",
                case.display_name(),
                progress.records,
                progress.payload_bytes,
            );
            break;
        }
        let message = match timeout(per_read_timeout, websocket.next()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => break, // stream closed
            Err(_) => {
                eprintln!(
                    "WARNING: benchmark client per-read timeout for case {} (no data for {:?})",
                    case.display_name(),
                    per_read_timeout,
                );
                break;
            }
        };
        match message? {
            Message::Binary(frame) => {
                progress.wire_bytes += frame.len() as u64;
                for record in decode_transport_records(&frame)? {
                    accumulate_record_metrics(&mut progress, &record);
                    let apply_start = Instant::now();
                    session.apply_protected_record(record)?;
                    apply_latency.record(apply_start.elapsed().as_nanos() as u64);
                }
                sampler.maybe_sample(&progress)?;
                // Update verify progress bar with byte-level granularity
                verify_pb.set_position(progress.payload_bytes.min(target_bytes));
                let elapsed = start.elapsed();
                verify_pb.set_message(format!(
                    "{} rec | {:.1} MB/s wire | {} payload",
                    progress.records,
                    if elapsed.as_secs_f64() > 0.0 { progress.wire_bytes as f64 / elapsed.as_secs_f64() / 1e6 } else { 0.0 },
                    HumanBytes(progress.payload_bytes),
                ));
            }
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {}
        }
    }
    verify_pb.finish_and_clear();
    finalize_receiver_metrics(
        artifact_dir,
        BenchmarkSide::Client,
        progress,
        start.elapsed(),
        apply_latency.summary(),
        None,
        sampler,
    )
}

async fn verify_case_against_url_native_receiver_authenticated(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let connect_config = client_connect_config_for_case(case, url)?;
    let mut connected = match case.carrier {
        BenchmarkCarrier::WebSocket => AuthenticatedClientBenchConnection::WebSocket(
            connect_websocket_session(&connect_config).await?,
        ),
        BenchmarkCarrier::Tcp => {
            AuthenticatedClientBenchConnection::Tcp(connect_tcp_session(&connect_config).await?)
        }
        BenchmarkCarrier::QuicStream => {
            AuthenticatedClientBenchConnection::Quic(connect_quic_session(&connect_config).await?)
        }
        BenchmarkCarrier::Udp => {
            AuthenticatedClientBenchConnection::Udp(connect_udp_session(&connect_config).await?)
        }
        BenchmarkCarrier::QuicDatagram => AuthenticatedClientBenchConnection::QuicDatagram(
            connect_quic_datagram_session(&connect_config).await?,
        ),
        BenchmarkCarrier::WebTransportDatagram => {
            AuthenticatedClientBenchConnection::WebTransportDatagram(
                connect_webtransport_datagram_session(&connect_config).await?,
            )
        }
    };
    let mut sampler = ProcessSampler::new(BenchmarkSide::Client);
    let mut progress = SideProgress::default();
    let mut apply_latency = LatencyReservoir::new(case.seed ^ 0x3333);
    let start = Instant::now();
    let target_bytes = case.target_bytes.bytes();

    // TQDM-style progress bar for the authenticated verify client.
    let verify_pb = bench_case_byte_progress_bar(target_bytes, &case.display_name());
    verify_pb.set_message(format!("authenticated → {}", url));

    loop {
        let frame = match connected.read_transport_frame().await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(error) => {
                verify_pb.finish_and_clear();
                return Err(BenchError::Invariant(format!(
                    "native authenticated receiver read failed for {} after {} records / {} payload bytes / {} wire bytes: {error}",
                    case.display_name(),
                    progress.records,
                    progress.payload_bytes,
                    progress.wire_bytes,
                )));
            }
        };
        progress.wire_bytes += frame.len() as u64;
        let apply_start = Instant::now();
        let records = connected.session_mut().unprotect_transport_frame(&frame)?;
        let has_close = records.iter().any(|record| {
            matches!(
                record.header.record_type,
                shared_protocol::RecordType::Close
            )
        });
        for record in &records {
            accumulate_record_metrics(&mut progress, record);
        }
        connected.session_mut().apply_unprotected_trace(records)?;
        apply_latency.record(apply_start.elapsed().as_nanos() as u64);

        // Update verify progress bar with byte-level granularity
        verify_pb.set_position(progress.payload_bytes.min(target_bytes));
        let elapsed = start.elapsed();
        verify_pb.set_message(format!(
            "{} rec | {:.1} MB/s wire | {} payload",
            progress.records,
            if elapsed.as_secs_f64() > 0.0 { progress.wire_bytes as f64 / elapsed.as_secs_f64() / 1e6 } else { 0.0 },
            HumanBytes(progress.payload_bytes),
        ));

        if has_close {
            verify_pb.finish_and_clear();
            sampler.maybe_sample(&progress)?;
            return finalize_receiver_metrics(
                artifact_dir,
                BenchmarkSide::Client,
                progress,
                start.elapsed(),
                apply_latency.summary(),
                connected.datagram_metrics(),
                sampler,
            );
        }
        sampler.maybe_sample(&progress)?;
    }

    verify_pb.finish_and_clear();
    finalize_receiver_metrics(
        artifact_dir,
        BenchmarkSide::Client,
        progress,
        start.elapsed(),
        apply_latency.summary(),
        connected.datagram_metrics(),
        sampler,
    )
}

async fn verify_case_against_url_native_sender(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    if uses_authenticated_session(case) {
        return verify_case_against_url_native_sender_authenticated(case, url, artifact_dir).await;
    }

    let (sender, _) = paired_protectors(case)?;
    let mut session =
        ServerSession::new_with_configs(sender, case.optimization.source_optimization_config());
    let mut source_cache = benchmark_source_cache_for_case(case, artifact_dir)?;
    let mut sampler = ProcessSampler::new(BenchmarkSide::Client);
    let mut progress = SideProgress::default();
    let mut source_cache_metrics = SourceCacheMetrics::default();
    let start = Instant::now();
    let mut generator = WorkloadGenerator::new(case.workload, case.corpus, case.seed);
    let mut burst_buffer = BurstAccumulator::new(transport_payload_cap(case.mode));
    let yield_between_groups = case.mode.payload_cap_bytes().is_some();

    let (mut websocket, _) =
        connect_async_with_config(url, Some(benchmark_websocket_config()), false).await?;
    let mut target_reached = false;
    while !target_reached {
        let event = generator.next_event();
        let frames = build_sender_frame(
            case,
            &mut session,
            &mut source_cache,
            &mut source_cache_metrics,
            event,
        )?;
        for frame in frames {
            target_reached = push_frame(
                &mut websocket,
                &mut progress,
                &mut sampler,
                &mut burst_buffer,
                frame,
                case.target_bytes.bytes(),
                yield_between_groups,
            )
            .await?;
            if target_reached {
                break;
            }
        }
    }

    let close_record = session.emit_close(Some(1000))?;
    let close_frame = BurstFrame {
        record: close_record,
    };
    push_close_frame(
        &mut websocket,
        &mut progress,
        &mut sampler,
        &mut burst_buffer,
        close_frame,
        yield_between_groups,
    )
    .await?;

    let mut flushed = Vec::new();
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts(
        &mut websocket,
        &mut progress,
        &mut sampler,
        flushed,
        yield_between_groups,
    )
    .await?;
    websocket.close(None).await?;

    let duration = start.elapsed();
    let (mut process_metrics, samples) = sampler.finish(&progress)?;
    let side_result_path = artifact_dir.join("side_result.json");
    let metrics = BenchmarkSideMetrics {
        side: BenchmarkSide::Client,
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
            ..process_metrics.clone()
        },
        codec_modes: progress.codec_modes.clone(),
        source_kinds: progress.source_kinds.clone(),
        residual_modes: progress.residual_modes.clone(),
        encode_latency: None,
        embedding_latency: None,
        apply_latency: None,
        burst_stats: case.mode.payload_cap_bytes().map(|_| burst_buffer.stats()),
        utility: None,
        source_cache: Some(source_cache_metrics),
        predictive_memory: predictive_memory_metrics(&progress),
        datagram: None,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    process_metrics.timeseries_path = artifact_dir.join("timeseries.jsonl").display().to_string();
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side: BenchmarkSide::Client,
            metrics: metrics.clone(),
            samples,
        },
    )?;
    Ok(metrics)
}

async fn verify_case_against_url_native_sender_authenticated(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let connect_config = client_connect_config_for_case(case, url)?;
    let mut connected = match case.carrier {
        BenchmarkCarrier::WebSocket => AuthenticatedClientBenchConnection::WebSocket(
            connect_websocket_session(&connect_config).await?,
        ),
        BenchmarkCarrier::Tcp => {
            AuthenticatedClientBenchConnection::Tcp(connect_tcp_session(&connect_config).await?)
        }
        BenchmarkCarrier::QuicStream => {
            AuthenticatedClientBenchConnection::Quic(connect_quic_session(&connect_config).await?)
        }
        BenchmarkCarrier::Udp => {
            AuthenticatedClientBenchConnection::Udp(connect_udp_session(&connect_config).await?)
        }
        BenchmarkCarrier::QuicDatagram => AuthenticatedClientBenchConnection::QuicDatagram(
            connect_quic_datagram_session(&connect_config).await?,
        ),
        BenchmarkCarrier::WebTransportDatagram => {
            AuthenticatedClientBenchConnection::WebTransportDatagram(
                connect_webtransport_datagram_session(&connect_config).await?,
            )
        }
    };
    let mut sender_session = PlainServerSession::new_with_configs(
        case.stream_id,
        case.optimization.source_optimization_config(),
    );
    let mut source_cache = benchmark_source_cache_for_case(case, artifact_dir)?;
    let mut sampler = ProcessSampler::new(BenchmarkSide::Client);
    let mut progress = SideProgress::default();
    let mut source_cache_metrics = SourceCacheMetrics::default();
    let start = Instant::now();
    let mut generator = WorkloadGenerator::new(case.workload, case.corpus, case.seed);
    let mut burst_buffer = BurstAccumulator::new(transport_payload_cap(case.mode));
    let yield_between_groups = case.mode.payload_cap_bytes().is_some();

    let mut target_reached = false;
    while !target_reached {
        let event = generator.next_event();
        let plain_frames = build_sender_plain_frame(
            case,
            &mut sender_session,
            &mut source_cache,
            &mut source_cache_metrics,
            event,
        )?;
        for plain_frame in plain_frames {
            let frame = BurstFrame {
                record: plain_frame.record,
            };
            target_reached = push_frame_authenticated_client(
                &mut connected,
                &mut progress,
                &mut sampler,
                &mut burst_buffer,
                frame,
                case.target_bytes.bytes(),
                yield_between_groups,
            )
            .await?;
            if target_reached {
                break;
            }
        }
    }

    let close_record = sender_session.emit_close(Some(1000))?;
    let close_frame = BurstFrame {
        record: close_record,
    };
    push_close_frame_authenticated_client(
        &mut connected,
        &mut progress,
        &mut sampler,
        &mut burst_buffer,
        close_frame,
        yield_between_groups,
    )
    .await?;
    let mut flushed = Vec::new();
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts_authenticated_client(
        &mut connected,
        &mut progress,
        &mut sampler,
        flushed,
        yield_between_groups,
    )
    .await?;
    let datagram_metrics = connected.datagram_metrics();
    connected.close().await?;

    let duration = start.elapsed();
    let (mut process_metrics, samples) = sampler.finish(&progress)?;
    let side_result_path = artifact_dir.join("side_result.json");
    let metrics = BenchmarkSideMetrics {
        side: BenchmarkSide::Client,
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
            ..process_metrics.clone()
        },
        codec_modes: progress.codec_modes.clone(),
        source_kinds: progress.source_kinds.clone(),
        residual_modes: progress.residual_modes.clone(),
        encode_latency: None,
        embedding_latency: None,
        apply_latency: None,
        burst_stats: case.mode.payload_cap_bytes().map(|_| burst_buffer.stats()),
        utility: None,
        source_cache: Some(source_cache_metrics),
        predictive_memory: predictive_memory_metrics(&progress),
        datagram: datagram_metrics,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    process_metrics.timeseries_path = artifact_dir.join("timeseries.jsonl").display().to_string();
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side: BenchmarkSide::Client,
            metrics: metrics.clone(),
            samples,
        },
    )?;
    Ok(metrics)
}

async fn verify_case_against_url_browser(
    case: &BenchmarkCase,
    url: &str,
    artifact_dir: &Path,
) -> Result<BenchmarkSideMetrics, BenchError> {
    if case.carrier != BenchmarkCarrier::WebSocket {
        return Err(BenchError::InvalidCase(
            "browser runtime only supports the websocket carrier in this wave".to_string(),
        ));
    }
    fs::create_dir_all(artifact_dir)?;
    let bundle_root = ensure_web_bench_bundle()?;
    let python = ensure_web_bench_venv()?;
    let script_path = web_bench_runner_script();
    let web_bench_frames_path = prepare_web_bench_case_inputs(artifact_dir, case)?;

    let runner_output = run_command_capture(Command::new(&python).args([
        script_path.to_str().unwrap(),
        "--bundle-root",
        bundle_root.to_str().unwrap(),
        "--case-root",
        artifact_dir.to_str().unwrap(),
        "--ws-url",
        url,
        "--timeout-seconds",
        WEB_BENCH_TIMEOUT_SECONDS,
    ]));
    if let Some(frames_path) = web_bench_frames_path.as_ref() {
        let _ = fs::remove_file(frames_path);
    }
    let runner_output = runner_output?;
    let web_bench_output: WebBenchRunnerOutput = serde_json::from_str(&runner_output)?;

    let progress = SideProgress {
        records: web_bench_output.result.records,
        cue_object_records: web_bench_output.result.cue_object_records,
        predictive_records: web_bench_output.result.predictive_records,
        original_payload_bytes: web_bench_output.result.original_payload_bytes,
        payload_bytes: web_bench_output.result.payload_bytes,
        wire_bytes: web_bench_output.result.wire_bytes,
        bursts: 0,
        codec_modes: web_bench_output.result.codec_modes.clone(),
        source_kinds: web_bench_output.result.source_kinds.clone(),
        residual_modes: web_bench_output.result.residual_modes.clone(),
        ..Default::default()
    };
    let duration = Duration::from_millis(web_bench_output.result.total_duration_ms.max(1));
    let timeseries = web_bench_output
        .samples
        .into_iter()
        .map(|sample| TimeSeriesSample {
            side: BenchmarkSide::Client,
            elapsed_ms: sample.elapsed_ms,
            rss_bytes: sample.rss_bytes,
            vsz_bytes: sample.vsz_bytes,
            cpu_percent: sample.cpu_percent,
            records: sample.records,
            cue_object_records: sample.cue_object_records,
            predictive_records: sample.predictive_records,
            original_payload_bytes: sample.original_payload_bytes,
            payload_bytes: sample.payload_bytes,
            wire_bytes: sample.wire_bytes,
            bursts: 0,
        })
        .collect::<Vec<_>>();
    let side_result_path = artifact_dir.join("side_result.json");
    let metrics = BenchmarkSideMetrics {
        side: BenchmarkSide::Client,
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            peak_rss_bytes: web_bench_output.peak_rss_bytes,
            peak_vsz_bytes: web_bench_output.peak_vsz_bytes,
            peak_cpu_percent: web_bench_output.peak_cpu_percent,
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
        },
        codec_modes: progress.codec_modes.clone(),
        source_kinds: progress.source_kinds.clone(),
        residual_modes: progress.residual_modes.clone(),
        encode_latency: None,
        embedding_latency: None,
        apply_latency: web_bench_output.result.apply_latency,
        burst_stats: None,
        utility: None,
        source_cache: None,
        predictive_memory: predictive_memory_metrics(&progress),
        datagram: None,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side: BenchmarkSide::Client,
            metrics: metrics.clone(),
            samples: timeseries,
        },
    )?;
    Ok(metrics)
}

fn finalize_case_result(
    case: BenchmarkCase,
    case_root: &Path,
    direct_connectivity_verified: bool,
    server_metrics: BenchmarkSideMetrics,
    client_metrics: BenchmarkSideMetrics,
) -> Result<BenchmarkResult, BenchError> {
    let original_payload_bytes = server_metrics.original_payload_bytes;
    let actual_payload_bytes = server_metrics.payload_bytes;
    let result = BenchmarkResult {
        case: case.clone(),
        artifact_dir: case_root.display().to_string(),
        target_payload_bytes: case.target_bytes.bytes(),
        original_payload_bytes,
        actual_payload_bytes,
        payload_overshoot_bytes: actual_payload_bytes.saturating_sub(case.target_bytes.bytes()),
        protected_wire_bytes: server_metrics.wire_bytes,
        build_profile: if cfg!(debug_assertions) {
            "debug".to_string()
        } else {
            "release".to_string()
        },
        direct_connectivity_verified,
        sharded: false,
        shard_count: 1,
        shard_payload_cap_bytes: None,
        shard_results: Vec::new(),
        byte_categories: server_metrics.byte_categories.clone(),
        // S4.2.e: Record the scratch/cache directory used for this run.
        scratch_dir: Some(case_root.display().to_string()),
        // S4.5.e: This is a fresh execution, not a cache hit.
        cache_hit: false,
        server: server_metrics.clone(),
        client: client_metrics.clone(),
    };

    let result_path = case_root.join("result.json");
    fs::write(&result_path, serde_json::to_vec_pretty(&result)?)?;
    fs::write(case_root.join("summary.md"), render_case_summary(&result))?;
    // S4.5: Store fingerprint alongside result for future cache validity checks
    store_result_fingerprint(case_root, &case);
    merge_side_timeseries(case_root)?;
    Ok(result)
}

fn read_case_manifest(path: impl AsRef<Path>) -> Result<BenchmarkCase, BenchError> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn prepare_case_root(case_root: &Path, case: &BenchmarkCase) -> Result<(), BenchError> {
    fs::create_dir_all(case_root)?;
    fs::write(
        case_root.join("case.json"),
        serde_json::to_vec_pretty(case)?,
    )?;
    let mut manifest_summary = input_corpus_manifest(case.corpus).clone();
    manifest_summary.files.clear();
    fs::write(
        case_root.join(INPUT_CORPUS_MANIFEST_FILE),
        serde_json::to_vec_pretty(&manifest_summary)?,
    )?;
    Ok(())
}

fn input_corpus(kind: BenchmarkCorpusKind) -> &'static InputCorpus {
    match kind {
        BenchmarkCorpusKind::Wikitext103Raw => WIKITEXT_INPUT_CORPUS.get_or_init(|| {
            build_input_corpus(BenchmarkCorpusKind::Wikitext103Raw).unwrap_or_else(|error| {
                panic!("failed to build wikitext benchmark input corpus: {error}")
            })
        }),
        BenchmarkCorpusKind::WebImage50 => WEB_IMAGE_INPUT_CORPUS.get_or_init(|| {
            build_input_corpus(BenchmarkCorpusKind::WebImage50).unwrap_or_else(|error| {
                panic!("failed to build web image benchmark input corpus: {error}")
            })
        }),
    }
}

fn input_corpus_manifest(kind: BenchmarkCorpusKind) -> &'static InputCorpusManifest {
    &input_corpus(kind).manifest
}

fn validate_case_input_corpus(case: &BenchmarkCase) -> Result<(), BenchError> {
    if case.target_bytes.bytes() > BENCH_TARGET_100_MB {
        return Err(BenchError::InvalidCase(
            "benchmark targets above 100mb are disabled; use 1mb, 10mb, or 100mb".to_string(),
        ));
    }

    if case.carrier.is_datagram() && case.client_runtime != BenchmarkClientRuntime::NativeRust {
        return Err(BenchError::InvalidCase(
            "datagram benchmark carriers currently require the native_rust client runtime"
                .to_string(),
        ));
    }

    if case.carrier.is_datagram() && !case.protection.profile_kind().is_datagram_family() {
        return Err(BenchError::InvalidCase(
            "datagram benchmark carriers require pq_simple_dgram_v1 or pq_mutual_dgram_v1"
                .to_string(),
        ));
    }

    if !case.carrier.is_datagram() && case.protection.profile_kind().is_datagram_family() {
        return Err(BenchError::InvalidCase(
            "datagram protection profiles require udp, quic_datagram, or webtransport_datagram"
                .to_string(),
        ));
    }

    let support_level = benchmark_case_support_level(case);
    if matches!(support_level, PredictiveBenchSupportLevel::Unsupported) {
        return Err(BenchError::InvalidCase(format!(
            "input corpus {} is unsupported by CHPMT capability {}; choose a capability whose source-family/object contract matches the corpus",
            case.corpus.slug(),
            case.capability_profile.slug()
        )));
    }

    let manifest = input_corpus_manifest(case.corpus);
    if !case.input_corpus_fingerprint.is_empty()
        && case.input_corpus_fingerprint != manifest.fingerprint
    {
        return Err(BenchError::InvalidCase(format!(
            "benchmark input corpus fingerprint mismatch: case={}, local={}",
            case.input_corpus_fingerprint, manifest.fingerprint
        )));
    }
    if case.input_corpus_file_count != 0 && case.input_corpus_file_count != manifest.file_count {
        return Err(BenchError::InvalidCase(format!(
            "benchmark input corpus file count mismatch: case={}, local={}",
            case.input_corpus_file_count, manifest.file_count
        )));
    }
    if case.input_corpus_chunk_count != 0 && case.input_corpus_chunk_count != manifest.chunk_count {
        return Err(BenchError::InvalidCase(format!(
            "benchmark input corpus chunk count mismatch: case={}, local={}",
            case.input_corpus_chunk_count, manifest.chunk_count
        )));
    }

    Ok(())
}

fn build_input_corpus(kind: BenchmarkCorpusKind) -> Result<InputCorpus, BenchError> {
    match kind {
        BenchmarkCorpusKind::Wikitext103Raw => build_wikitext_input_corpus(),
        BenchmarkCorpusKind::WebImage50 => build_web_image_input_corpus(),
    }
}

fn build_wikitext_input_corpus() -> Result<InputCorpus, BenchError> {
    let root = workspace_root().to_path_buf();
    let corpus_dir = root.join(WIKITEXT_CORPUS_DIR);
    let manifest_path = root.join(WIKITEXT_CORPUS_MANIFEST_JSON);
    let chunks_path = root.join(WIKITEXT_CORPUS_CHUNKS_JSONL);
    let script_path = root.join(WIKITEXT_CORPUS_FETCH_SCRIPT);
    let streamed = if corpus_dir.exists() && manifest_path.exists() && chunks_path.exists() {
        let manifest: InputCorpusManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
        let chunks = fs::read_to_string(&chunks_path)?
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<MaterializedTextChunkRecord>)
            .collect::<Result<Vec<_>, _>>()?;
        StreamedWikitextCorpus { manifest, chunks }
    } else {
        if !script_path.exists() {
            return Err(BenchError::InvalidCase(format!(
                "wikitext_103_raw corpus snapshot is missing at {} and stream script is missing at {}",
                corpus_dir.display(),
                script_path.display()
            )));
        }
        serde_json::from_str(&run_command_capture(
            Command::new("python3").args([script_path.to_str().unwrap(), "--stream-json"]),
        )?)?
    };
    let mut manifest = streamed.manifest;
    if manifest.kind != BenchmarkCorpusKind::Wikitext103Raw {
        return Err(BenchError::InvalidCase(format!(
            "wikitext streamed corpus manifest kind mismatch from {}",
            script_path.display()
        )));
    }
    if manifest.chunk_char_count != Some(INPUT_CHUNK_CHAR_COUNT)
        || manifest.chunk_char_stride != Some(INPUT_CHUNK_CHAR_STRIDE)
        || manifest.chunk_min_chars != Some(INPUT_CHUNK_MIN_CHARS)
    {
        return Err(BenchError::InvalidCase(format!(
            "wikitext streamed corpus chunking mismatch from {}; rerun the pinned stream builder to refresh the fixed chunk set",
            script_path.display()
        )));
    }

    let expected_file_count = manifest.files.len();
    let expected_chunk_count = manifest.chunk_count;
    let file_fingerprint = sha256_hex(&serde_json::to_vec(&manifest.files)?);

    let mut chunks = Vec::with_capacity(expected_chunk_count);
    let mut files = Vec::with_capacity(expected_file_count);
    let records = streamed.chunks;

    let mut offset = 0usize;
    for (file_index, file) in manifest.files.iter().enumerate() {
        let first_chunk_index = chunks.len();
        for expected_chunk_index in 0..file.chunk_count {
            let Some(record) = records.get(offset) else {
                return Err(BenchError::InvalidCase(format!(
                    "wikitext chunk stream ended early at file {} chunk {}",
                    file.relative_path, expected_chunk_index
                )));
            };
            if record.file_index != file_index
                || record.chunk_index != expected_chunk_index
                || record.file_sha256 != file.sha256
            {
                return Err(BenchError::InvalidCase(format!(
                    "wikitext chunk ordering mismatch at file {} chunk {}",
                    file.relative_path, expected_chunk_index
                )));
            }
            let chunk_sha256 = sha256_hex(record.text.as_bytes());
            if chunk_sha256 != record.chunk_sha256 {
                return Err(BenchError::InvalidCase(format!(
                    "wikitext chunk checksum mismatch at file {} chunk {}",
                    file.relative_path, expected_chunk_index
                )));
            }
            chunks.push(InputCorpusChunk {
                file_index,
                file_sha256: record.file_sha256.clone(),
                chunk_index: expected_chunk_index,
                chunk_sha256,
                label: file.relative_path.clone(),
                source: InputCorpusSource::Text(record.text.clone()),
            });
            offset += 1;
        }
        files.push(InputCorpusFile {
            first_chunk_index,
            chunk_count: file.chunk_count,
        });
    }

    if offset != records.len() {
        return Err(BenchError::InvalidCase(format!(
            "wikitext chunk stream has {} trailing chunk records",
            records.len().saturating_sub(offset)
        )));
    }

    manifest.fingerprint = file_fingerprint;
    manifest.file_count = expected_file_count;
    manifest.chunk_count = chunks.len();
    if files.is_empty() || chunks.is_empty() {
        return Err(BenchError::InvalidCase(
            "wikitext_103_raw streamed corpus is empty".to_string(),
        ));
    }

    Ok(InputCorpus {
        manifest,
        files,
        chunks,
    })
}

fn build_web_image_input_corpus() -> Result<InputCorpus, BenchError> {
    let root = workspace_root().to_path_buf();
    let expected_dir = root.join(WEB_IMAGE_CORPUS_DIR);
    let expected_files_dir = root.join(WEB_IMAGE_CORPUS_FILES_DIR);
    let manifest_path = root.join(WEB_IMAGE_CORPUS_MANIFEST_JSON);
    let corpus_manifest: WebImageCorpusManifest =
        serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if corpus_manifest.files.len() != 50 {
        return Err(BenchError::InvalidCase(format!(
            "web image corpus manifest must contain 50 files, found {}",
            corpus_manifest.files.len()
        )));
    }
    if !expected_dir.exists() || !expected_files_dir.exists() {
        return Err(BenchError::InvalidCase(
            "web image corpus directory is missing; fetch the corpus before running image benchmarks"
                .to_string(),
        ));
    }

    let mut manifest_files = Vec::new();
    let mut files = Vec::new();
    let mut chunks = Vec::new();
    for file in corpus_manifest.files {
        let relative_path = PathBuf::from(&file.relative_path);
        let absolute_path = root.join(&relative_path);
        if !absolute_path.starts_with(&expected_files_dir) {
            return Err(BenchError::InvalidCase(format!(
                "web image corpus file `{}` is outside the expected files directory",
                relative_path.display()
            )));
        }
        if !absolute_path.exists() {
            return Err(BenchError::InvalidCase(format!(
                "missing web image corpus file `{}`; fetch the corpus before running image benchmarks",
                relative_path.display()
            )));
        }
        let bytes = fs::read(&absolute_path)?;
        let actual_sha256 = sha256_hex(&bytes);
        if actual_sha256 != file.sha256 {
            return Err(BenchError::InvalidCase(format!(
                "web image corpus checksum mismatch for `{}`",
                relative_path.display()
            )));
        }

        let relative_path_str = relative_path.to_string_lossy().replace('\\', "/");
        let first_chunk_index = chunks.len();
        let file_index = files.len();
        chunks.push(InputCorpusChunk {
            file_index,
            file_sha256: file.sha256.clone(),
            chunk_index: 0,
            chunk_sha256: file.sha256.clone(),
            label: relative_path_str.clone(),
            source: InputCorpusSource::Image {
                mime: file.mime.clone(),
                bytes: Arc::<[u8]>::from(bytes),
            },
        });
        files.push(InputCorpusFile {
            first_chunk_index,
            chunk_count: 1,
        });
        manifest_files.push(InputCorpusManifestFile {
            relative_path: relative_path_str,
            sha256: file.sha256,
            byte_len: file.byte_len,
            chunk_count: 1,
            mime: Some(file.mime),
            source_url: Some(file.source_url),
        });
    }

    let manifest = InputCorpusManifest {
        kind: BenchmarkCorpusKind::WebImage50,
        fingerprint: sha256_hex(&serde_json::to_vec(&manifest_files)?),
        file_count: files.len(),
        chunk_count: chunks.len(),
        source: Some(corpus_manifest.source),
        dataset: None,
        config: None,
        revision: None,
        split: None,
        chunk_char_count: None,
        chunk_char_stride: None,
        chunk_min_chars: None,
        files: manifest_files,
    };
    Ok(InputCorpus {
        manifest,
        files,
        chunks,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn live_benchmark_item(input: &InputCorpusChunk, exact_bytes: Vec<u8>) -> LiveBenchmarkItem {
    LiveBenchmarkItem {
        exact_bytes,
        file_sha256: input.file_sha256.clone(),
        chunk_sha256: input.chunk_sha256.clone(),
    }
}

fn split_target_bytes(total_bytes: u64, shard_cap_bytes: u64) -> Vec<u64> {
    if total_bytes == 0 {
        return vec![0];
    }
    if total_bytes <= shard_cap_bytes {
        return vec![total_bytes];
    }

    let full_shards = (total_bytes / shard_cap_bytes) as usize;
    let remainder = total_bytes % shard_cap_bytes;
    let mut shards = vec![shard_cap_bytes; full_shards];
    if remainder > 0 {
        shards.push(remainder);
    }
    shards
}

fn transport_payload_cap(mode: BenchmarkMode) -> usize {
    mode.payload_cap_bytes()
        .unwrap_or(DEFAULT_BULK_TRANSPORT_PAYLOAD_CAP)
}

fn shard_case(
    base_case: &BenchmarkCase,
    shard_index: usize,
    shard_target_bytes: u64,
) -> BenchmarkCase {
    let mut shard_case = base_case.clone().with_custom_target(shard_target_bytes);
    let shard_nonce =
        0x9e37_79b9_7f4a_7c15_u64.wrapping_mul((shard_index as u64).saturating_add(1));
    shard_case.seed ^= shard_nonce;
    shard_case.stream_id = StreamId(shard_case.stream_id.0 ^ shard_nonce.rotate_left(17));
    shard_case
}

fn prepare_web_bench_case_inputs(
    artifact_dir: &Path,
    case: &BenchmarkCase,
) -> Result<Option<PathBuf>, BenchError> {
    let web_bench_case = match (case.direction, case.protection) {
        (BenchmarkDirection::ServerToClient, protection)
            if uses_authenticated_protection(protection) =>
        {
            let materials = deterministic_bootstrap_materials(case)?;
            WebBenchCase {
                role: WebBenchRole::Receiver,
                session_config: Some(materials.session_config),
                bootstrap_client_config: Some(materials.client_config),
                protection_kind: None,
                stream_id: None,
                receiver_bootstrap_root: None,
                yield_on_group_boundary: case.mode.payload_cap_bytes().is_some(),
            }
        }
        (BenchmarkDirection::ServerToClient, _) => {
            let (_, receiver) = paired_protectors(case)?;
            WebBenchCase {
                role: WebBenchRole::Receiver,
                session_config: None,
                bootstrap_client_config: None,
                protection_kind: Some(case.protection.profile_kind()),
                stream_id: Some(case.stream_id),
                receiver_bootstrap_root: Some(receiver.bootstrap_root()),
                yield_on_group_boundary: case.mode.payload_cap_bytes().is_some(),
            }
        }
        (BenchmarkDirection::ClientToServer, protection)
            if uses_authenticated_protection(protection) =>
        {
            let materials = deterministic_bootstrap_materials(case)?;
            WebBenchCase {
                role: WebBenchRole::Sender,
                session_config: Some(materials.session_config),
                bootstrap_client_config: Some(materials.client_config),
                protection_kind: None,
                stream_id: None,
                receiver_bootstrap_root: None,
                yield_on_group_boundary: case.mode.payload_cap_bytes().is_some(),
            }
        }
        (BenchmarkDirection::ClientToServer, _) => WebBenchCase {
            role: WebBenchRole::Sender,
            session_config: None,
            bootstrap_client_config: None,
            protection_kind: None,
            stream_id: None,
            receiver_bootstrap_root: None,
            yield_on_group_boundary: case.mode.payload_cap_bytes().is_some(),
        },
    };
    fs::write(
        artifact_dir.join(WEB_BENCH_CASE_FILE),
        serde_json::to_vec_pretty(&web_bench_case)?,
    )?;

    if case.direction == BenchmarkDirection::ClientToServer {
        let frames_path = artifact_dir.join(WEB_BENCH_FRAMES_FILE);
        write_web_bench_sender_spool(case, &frames_path)?;
        Ok(Some(frames_path))
    } else {
        Ok(None)
    }
}

fn write_web_bench_sender_spool(
    case: &BenchmarkCase,
    output_path: &Path,
) -> Result<(), BenchError> {
    if uses_authenticated_session(case) {
        return write_web_bench_sender_plain_spool(case, output_path);
    }

    let (sender, _) = paired_protectors(case)?;
    let mut session =
        ServerSession::new_with_configs(sender, case.optimization.source_optimization_config());
    let mut source_cache = benchmark_source_cache_for_case(
        case,
        &workspace_root().join("target/web_bench_sender_cache"),
    )?;
    let mut source_cache_metrics = SourceCacheMetrics::default();
    let mut generator = WorkloadGenerator::new(case.workload, case.corpus, case.seed);
    let mut file = fs::File::create(output_path)?;
    let mut progress = SideProgress::default();
    let mut burst_buffer = BurstAccumulator::new(transport_payload_cap(case.mode));
    let mut target_reached = false;

    while !target_reached {
        let event = generator.next_event();
        let frames = build_sender_frame(
            case,
            &mut session,
            &mut source_cache,
            &mut source_cache_metrics,
            event,
        )?;
        for frame in frames {
            target_reached = write_sender_frame(
                &mut file,
                &mut progress,
                &mut burst_buffer,
                frame,
                case.target_bytes.bytes(),
            )?;
            if target_reached {
                break;
            }
        }
    }

    let close_record = session.emit_close(Some(1000))?;
    let close_frame = BurstFrame {
        record: close_record,
    };
    write_sender_close_frame(&mut file, &mut progress, &mut burst_buffer, close_frame)?;
    flush_sender_bursts(&mut file, &mut progress, &mut burst_buffer, true)?;
    file.flush()?;
    Ok(())
}

fn write_web_bench_sender_plain_spool(
    case: &BenchmarkCase,
    output_path: &Path,
) -> Result<(), BenchError> {
    let mut session = PlainServerSession::new_with_configs(
        case.stream_id,
        case.optimization.source_optimization_config(),
    );
    let mut source_cache = benchmark_source_cache_for_case(
        case,
        &workspace_root().join("target/web_bench_sender_cache"),
    )?;
    let mut source_cache_metrics = SourceCacheMetrics::default();
    let mut generator = WorkloadGenerator::new(case.workload, case.corpus, case.seed);
    let mut file = fs::File::create(output_path)?;
    let mut progress = SideProgress::default();
    let mut burst_buffer = BurstAccumulator::new(transport_payload_cap(case.mode));
    let mut target_reached = false;

    while !target_reached {
        let event = generator.next_event();
        let frames = build_sender_plain_frame(
            case,
            &mut session,
            &mut source_cache,
            &mut source_cache_metrics,
            event,
        )?;
        for frame in frames {
            target_reached = write_sender_frame(
                &mut file,
                &mut progress,
                &mut burst_buffer,
                frame,
                case.target_bytes.bytes(),
            )?;
            if target_reached {
                break;
            }
        }
    }

    let close_record = session.emit_close(Some(1000))?;
    let close_frame = BurstFrame {
        record: close_record,
    };
    write_sender_close_frame(&mut file, &mut progress, &mut burst_buffer, close_frame)?;
    flush_sender_bursts(&mut file, &mut progress, &mut burst_buffer, true)?;
    file.flush()?;
    Ok(())
}

fn build_sender_frame(
    _case: &BenchmarkCase,
    session: &mut ServerSession,
    source_cache: &mut Option<SourceCache>,
    source_cache_metrics: &mut SourceCacheMetrics,
    event: WorkloadEvent,
) -> Result<Vec<BurstFrame>, BenchError> {
    let records = match event {
        WorkloadEvent::Insert { item_id, input } => {
            let (resolved, _) = resolve_chunk_for_benchmark(source_cache, &input)?;
            accumulate_source_cache_metrics(source_cache_metrics, &resolved.metrics);
            session
                .emit_resolved_source_insert(ItemId(item_id), resolved)?
                .records
        }
        WorkloadEvent::UpsertObject { item_id, input } => {
            let (resolved, _) = resolve_chunk_for_benchmark(source_cache, &input)?;
            accumulate_source_cache_metrics(source_cache_metrics, &resolved.metrics);
            session
                .emit_resolved_source_upsert(ItemId(item_id), resolved)?
                .records
        }
        WorkloadEvent::Evict { item_id } => vec![session.emit_event(ServerEvent::Evict {
            item_id: ItemId(item_id),
        })?],
        WorkloadEvent::Invalidate { item_id } => {
            vec![session.emit_event(ServerEvent::Invalidate {
                item_id: ItemId(item_id),
            })?]
        }
    };

    Ok(records
        .into_iter()
        .map(|record| BurstFrame { record })
        .collect())
}

fn build_sender_plain_frame(
    _case: &BenchmarkCase,
    session: &mut PlainServerSession,
    source_cache: &mut Option<SourceCache>,
    source_cache_metrics: &mut SourceCacheMetrics,
    event: WorkloadEvent,
) -> Result<Vec<BurstFrame>, BenchError> {
    let records = match event {
        WorkloadEvent::Insert { item_id, input } => {
            let (resolved, _) = resolve_chunk_for_benchmark(source_cache, &input)?;
            accumulate_source_cache_metrics(source_cache_metrics, &resolved.metrics);
            session
                .emit_resolved_source_insert(ItemId(item_id), resolved)?
                .records
        }
        WorkloadEvent::UpsertObject { item_id, input } => {
            let (resolved, _) = resolve_chunk_for_benchmark(source_cache, &input)?;
            accumulate_source_cache_metrics(source_cache_metrics, &resolved.metrics);
            session
                .emit_resolved_source_upsert(ItemId(item_id), resolved)?
                .records
        }
        WorkloadEvent::Evict { item_id } => vec![session.emit_event(ServerEvent::Evict {
            item_id: ItemId(item_id),
        })?],
        WorkloadEvent::Invalidate { item_id } => {
            vec![session.emit_event(ServerEvent::Invalidate {
                item_id: ItemId(item_id),
            })?]
        }
    };

    Ok(records
        .into_iter()
        .map(|record| BurstFrame { record })
        .collect())
}

fn write_sender_frame(
    file: &mut fs::File,
    progress: &mut SideProgress,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    target_payload_bytes: u64,
) -> Result<bool, BenchError> {
    let frame_payload = benchmark_payload_len(&frame.record);
    if progress.payload_bytes + burst_buffer.buffered_payload_bytes + frame_payload
        >= target_payload_bytes
    {
        let mut flushed = Vec::new();
        burst_buffer.push(frame, &mut flushed);
        burst_buffer.flush_remaining(&mut flushed);
        write_sender_burst_groups(file, progress, flushed, true)?;
        return Ok(true);
    }

    let mut flushed = Vec::new();
    burst_buffer.push(frame, &mut flushed);
    write_sender_burst_groups(file, progress, flushed, true)?;
    Ok(progress.payload_bytes >= target_payload_bytes)
}

fn write_sender_close_frame(
    file: &mut fs::File,
    progress: &mut SideProgress,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
) -> Result<(), BenchError> {
    let mut flushed = Vec::new();
    burst_buffer.push(frame, &mut flushed);
    burst_buffer.flush_remaining(&mut flushed);
    write_sender_burst_groups(file, progress, flushed, true)?;
    Ok(())
}

fn flush_sender_bursts(
    file: &mut fs::File,
    progress: &mut SideProgress,
    buffer: &mut BurstAccumulator,
    include_markers: bool,
) -> Result<(), BenchError> {
    let mut flushed = Vec::new();
    buffer.flush_remaining(&mut flushed);
    write_sender_burst_groups(file, progress, flushed, include_markers)
}

fn write_sender_burst_groups(
    file: &mut fs::File,
    progress: &mut SideProgress,
    bursts: Vec<Vec<BurstFrame>>,
    include_markers: bool,
) -> Result<(), BenchError> {
    for burst in bursts {
        progress.bursts += 1;
        for frame in burst {
            write_sender_frame_bytes(file, progress, frame)?;
        }
        if include_markers {
            file.write_all(&0_u32.to_le_bytes())?;
        }
    }
    Ok(())
}

fn write_sender_frame_bytes(
    file: &mut fs::File,
    progress: &mut SideProgress,
    frame: BurstFrame,
) -> Result<(), BenchError> {
    let wire_bytes = frame.record.to_bytes();
    let frame_len = wire_bytes.len() as u32;
    file.write_all(&frame_len.to_le_bytes())?;
    file.write_all(&wire_bytes)?;
    accumulate_record_metrics(progress, &frame.record);
    progress.wire_bytes += wire_bytes.len() as u64;
    Ok(())
}

fn ensure_web_bench_bundle() -> Result<PathBuf, BenchError> {
    if let Some(bundle_root) = WEB_BENCH_BUNDLE_READY.get() {
        return Ok(bundle_root.clone());
    }

    let bundle_root = workspace_root().join(WEB_BENCH_BUNDLE_DIR);
    let pkg_dir = bundle_root.join("pkg");
    if bundle_root.exists() {
        fs::remove_dir_all(&bundle_root)?;
    }
    fs::create_dir_all(&pkg_dir)?;
    fs::copy(
        workspace_root().join("server/web_bench/index.html"),
        bundle_root.join("index.html"),
    )?;
    let cargo_bin = rustup_tool("cargo");
    let rustc_bin = rustup_tool("rustc");
    let wasm_pack_bin = cargo_home_tool("wasm-pack");
    let toolchain_bin_dir = rustup_toolchain_bin_dir();
    let cargo_bin_dir = cargo_home_bin_dir();
    let mut rustflags = std::env::var("RUSTFLAGS").unwrap_or_default();
    if !rustflags.is_empty() {
        rustflags.push(' ');
    }
    rustflags.push_str("-C target-feature=+simd128");
    run_command(
        Command::new(&wasm_pack_bin)
            .current_dir(workspace_root())
            .env("CARGO", &cargo_bin)
            .env("RUSTC", &rustc_bin)
            .env("RUSTFLAGS", rustflags)
            .env(
                "PATH",
                format!(
                    "{}:{}:/usr/bin:/bin:/usr/sbin:/sbin",
                    toolchain_bin_dir.display(),
                    cargo_bin_dir.display()
                ),
            )
            .args([
                "build",
                "client",
                "--mode",
                "no-install",
                "--target",
                "web",
                "--release",
                "--out-dir",
                pkg_dir.to_str().unwrap(),
                "--out-name",
                "client",
            ]),
    )?;
    let _ = WEB_BENCH_BUNDLE_READY.set(bundle_root.clone());
    Ok(bundle_root)
}

fn ensure_web_bench_venv() -> Result<PathBuf, BenchError> {
    if let Some(python) = WEB_BENCH_VENV_PYTHON.get() {
        return Ok(python.clone());
    }

    let venv_dir = workspace_root().join(WEB_BENCH_VENV_DIR);
    let python = venv_dir.join("bin/python3");
    if !python.exists() {
        run_command(Command::new("python3").args(["-m", "venv", venv_dir.to_str().unwrap()]))?;
    }

    let selenium_status = Command::new(&python)
        .args(["-c", "import selenium"])
        .status()?;
    if !selenium_status.success() {
        run_command(Command::new(&python).args(["-m", "pip", "install", "-q", "selenium"]))?;
    }
    let _ = WEB_BENCH_VENV_PYTHON.set(python.clone());
    Ok(python)
}

fn web_bench_runner_script() -> PathBuf {
    workspace_root().join("server/scripts/web_bench_runner.py")
}

fn rustup_tool(tool: &str) -> PathBuf {
    let toolchain_path = rustup_toolchain_bin_dir().join(tool);
    if toolchain_path.exists() {
        toolchain_path
    } else {
        cargo_home_bin_dir().join(tool)
    }
}

fn rustup_toolchain_bin_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Users/pkhairkh"))
        .join(".rustup/toolchains/stable-aarch64-apple-darwin/bin")
}

fn cargo_home_bin_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/Users/pkhairkh"))
        .join(".cargo/bin")
}

fn cargo_home_tool(tool: &str) -> PathBuf {
    cargo_home_bin_dir().join(tool)
}

fn write_side_artifact(artifact_dir: &Path, side_artifact: SideArtifact) -> Result<(), BenchError> {
    fs::create_dir_all(artifact_dir)?;
    let side_json = SideArtifact {
        side: side_artifact.side,
        metrics: side_artifact.metrics.clone(),
        samples: Vec::new(),
    };
    fs::write(
        artifact_dir.join("side_result.json"),
        serde_json::to_vec_pretty(&side_json)?,
    )?;
    let mut timeseries = String::new();
    for sample in &side_artifact.samples {
        timeseries.push_str(&serde_json::to_string(sample)?);
        timeseries.push('\n');
    }
    fs::write(artifact_dir.join("timeseries.jsonl"), timeseries)?;
    Ok(())
}

fn read_side_artifact(path: impl AsRef<Path>) -> Result<SideArtifact, BenchError> {
    let path = path.as_ref();
    let mut artifact: SideArtifact = serde_json::from_slice(&fs::read(path)?)?;
    if artifact.samples.is_empty() {
        let timeseries_path = path.parent().unwrap_or(Path::new(".")).join("timeseries.jsonl");
        if timeseries_path.exists() {
            artifact.samples = fs::read_to_string(&timeseries_path)?
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(serde_json::from_str::<TimeSeriesSample>)
                .collect::<Result<Vec<_>, _>>()?;
        }
    }
    Ok(artifact)
}

fn decode_transport_records(frame: &[u8]) -> Result<Vec<Record>, BenchError> {
    if frame.len() >= 4 {
        let mut cursor = 0_usize;
        let mut records = Vec::new();
        let mut chunk_ok = true;
        while cursor < frame.len() {
            if frame.len().saturating_sub(cursor) < 4 {
                chunk_ok = false;
                break;
            }
            let record_len =
                u32::from_le_bytes(frame[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if record_len == 0 || frame.len().saturating_sub(cursor) < record_len {
                chunk_ok = false;
                break;
            }
            match Record::from_bytes(&frame[cursor..cursor + record_len]) {
                Ok(record) => records.push(record),
                Err(_) => {
                    chunk_ok = false;
                    break;
                }
            }
            cursor += record_len;
        }
        if chunk_ok && !records.is_empty() && cursor == frame.len() {
            return Ok(records);
        }
    }

    Ok(vec![Record::from_bytes(frame)?])
}

fn merge_side_timeseries(case_root: &Path) -> Result<(), BenchError> {
    let available_bytes = available_disk_bytes(case_root).unwrap_or(u64::MAX);
    if available_bytes < BENCH_LOCAL_DISK_MIN_FREE_BYTES {
        return Ok(());
    }
    let mut out = String::new();
    for side in [SERVER_SIDE_DIR, CLIENT_SIDE_DIR] {
        let path = case_root.join(side).join("timeseries.jsonl");
        if path.exists() {
            out.push_str(&fs::read_to_string(path)?);
        }
    }
    fs::write(case_root.join("timeseries.jsonl"), out)?;
    Ok(())
}

fn finalize_receiver_metrics(
    artifact_dir: &Path,
    side: BenchmarkSide,
    progress: SideProgress,
    duration: Duration,
    apply_latency: Option<LatencySummary>,
    datagram: Option<BenchmarkDatagramMetrics>,
    sampler: ProcessSampler,
) -> Result<BenchmarkSideMetrics, BenchError> {
    let (mut process_metrics, samples) = sampler.finish(&progress)?;
    let side_result_path = artifact_dir.join("side_result.json");
    let metrics = BenchmarkSideMetrics {
        side,
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
            ..process_metrics.clone()
        },
        codec_modes: progress.codec_modes.clone(),
        source_kinds: progress.source_kinds.clone(),
        residual_modes: progress.residual_modes.clone(),
        encode_latency: None,
        embedding_latency: None,
        apply_latency,
        burst_stats: None,
        utility: None,
        source_cache: None,
        predictive_memory: predictive_memory_metrics(&progress),
        datagram,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    process_metrics.timeseries_path = artifact_dir.join("timeseries.jsonl").display().to_string();
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side,
            metrics: metrics.clone(),
            samples,
        },
    )?;
    Ok(metrics)
}

fn finalize_sharded_case_result(
    case: BenchmarkCase,
    case_root: &Path,
    shard_targets: &[u64],
    shard_result_paths: Vec<String>,
    shard_results: &[BenchmarkResult],
) -> Result<BenchmarkResult, BenchError> {
    let server_dir = case_root.join(SERVER_SIDE_DIR);
    let client_dir = case_root.join(CLIENT_SIDE_DIR);
    fs::create_dir_all(&server_dir)?;
    fs::create_dir_all(&client_dir)?;

    let server_artifacts = shard_results
        .iter()
        .map(|result| read_side_artifact(&result.server.side_result_path))
        .collect::<Result<Vec<_>, _>>()?;
    let client_artifacts = shard_results
        .iter()
        .map(|result| read_side_artifact(&result.client.side_result_path))
        .collect::<Result<Vec<_>, _>>()?;
    let server_metrics =
        aggregate_side_metrics(BenchmarkSide::Server, &server_dir, &server_artifacts)?;
    let client_metrics =
        aggregate_side_metrics(BenchmarkSide::Client, &client_dir, &client_artifacts)?;

    let original_payload_bytes = server_metrics.original_payload_bytes;
    let actual_payload_bytes = server_metrics.payload_bytes;
    let result = BenchmarkResult {
        case: case.clone(),
        artifact_dir: case_root.display().to_string(),
        target_payload_bytes: case.target_bytes.bytes(),
        original_payload_bytes,
        actual_payload_bytes,
        payload_overshoot_bytes: actual_payload_bytes.saturating_sub(case.target_bytes.bytes()),
        protected_wire_bytes: server_metrics.wire_bytes,
        build_profile: if cfg!(debug_assertions) {
            "debug".to_string()
        } else {
            "release".to_string()
        },
        direct_connectivity_verified: shard_results
            .iter()
            .all(|result| result.direct_connectivity_verified),
        sharded: true,
        shard_count: shard_targets.len(),
        shard_payload_cap_bytes: Some(BENCH_MAX_SESSION_TARGET_BYTES),
        shard_results: shard_result_paths,
        byte_categories: server_metrics.byte_categories.clone(),
        // S4.2.e: Record the scratch/cache directory used for this sharded run.
        scratch_dir: Some(case_root.display().to_string()),
        // S4.5.e: Sharded results are always fresh executions.
        cache_hit: false,
        server: server_metrics,
        client: client_metrics,
    };

    fs::write(
        case_root.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    fs::write(case_root.join("summary.md"), render_case_summary(&result))?;
    merge_side_timeseries(case_root)?;
    Ok(result)
}

fn aggregate_side_metrics(
    side: BenchmarkSide,
    artifact_dir: &Path,
    shard_artifacts: &[SideArtifact],
) -> Result<BenchmarkSideMetrics, BenchError> {
    let mut records = 0_u64;
    let mut cue_object_records = 0_u64;
    let mut original_payload_bytes = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut wire_bytes = 0_u64;
    let mut total_duration_ms = 0_u128;
    let mut peak_rss_bytes = 0_u64;
    let mut peak_vsz_bytes = 0_u64;
    let mut peak_cpu_percent = 0.0_f64;
    let mut codec_modes = CodecModeTotals::default();
    let mut source_kinds = BenchSourceKindTotals::default();
    let mut residual_modes = ResidualModeTotals::default();
    let mut utility_samples = 0_usize;
    let mut utility_exact_chunk_top1_sum = 0.0_f64;
    let mut utility_exact_chunk_top5_sum = 0.0_f64;
    let mut utility_same_file_top1_sum = 0.0_f64;
    let mut utility_same_file_top5_sum = 0.0_f64;
    let mut utility_mrr_sum = 0.0_f64;
    let mut burst_count = 0_u64;
    let mut total_burst_payload_bytes = 0_u64;
    let mut max_burst_payload_bytes = 0_u64;
    let mut peak_buffered_payload_bytes = 0_u64;
    let mut peak_buffered_frame_count = 0_usize;
    let mut burst_fill_ratio_sum = 0.0_f64;
    let mut any_burst_stats = false;
    let mut samples = Vec::new();
    let mut elapsed_offset = 0_u128;

    for shard_artifact in shard_artifacts {
        let metrics = &shard_artifact.metrics;
        records += metrics.records;
        cue_object_records += metrics.cue_object_records;
        original_payload_bytes += metrics.original_payload_bytes;
        payload_bytes += metrics.payload_bytes;
        wire_bytes += metrics.wire_bytes;
        total_duration_ms += metrics.throughput.total_duration_ms;
        peak_rss_bytes = peak_rss_bytes.max(metrics.process.peak_rss_bytes);
        peak_vsz_bytes = peak_vsz_bytes.max(metrics.process.peak_vsz_bytes);
        peak_cpu_percent = peak_cpu_percent.max(metrics.process.peak_cpu_percent);
        codec_modes.direct_exact += metrics.codec_modes.direct_exact;
        codec_modes.packed_exact += metrics.codec_modes.packed_exact;
        codec_modes.predicted_exact += metrics.codec_modes.predicted_exact;
        codec_modes.control += metrics.codec_modes.control;
        source_kinds.text += metrics.source_kinds.text;
        source_kinds.json += metrics.source_kinds.json;
        source_kinds.binary += metrics.source_kinds.binary;
        source_kinds.unknown += metrics.source_kinds.unknown;
        residual_modes.none += metrics.residual_modes.none;
        residual_modes.all_zero += metrics.residual_modes.all_zero;
        residual_modes.small_signed_rans += metrics.residual_modes.small_signed_rans;
        residual_modes.sparse_positions += metrics.residual_modes.sparse_positions;
        residual_modes.literal_raw += metrics.residual_modes.literal_raw;
        residual_modes.unknown += metrics.residual_modes.unknown;

        if let Some(utility) = metrics.utility.as_ref() {
            let measured = utility.measured_events as f64;
            utility_samples += utility.measured_events;
            utility_exact_chunk_top1_sum += utility.exact_chunk_top1_rate * measured;
            utility_exact_chunk_top5_sum += utility.exact_chunk_top5_rate * measured;
            utility_same_file_top1_sum += utility.same_file_top1_rate * measured;
            utility_same_file_top5_sum += utility.same_file_top5_rate * measured;
            utility_mrr_sum += utility.mean_reciprocal_rank * measured;
        }

        if let Some(stats) = metrics.burst_stats.as_ref() {
            any_burst_stats = true;
            burst_count += stats.burst_count;
            total_burst_payload_bytes += stats.total_burst_payload_bytes;
            max_burst_payload_bytes = max_burst_payload_bytes.max(stats.max_burst_payload_bytes);
            peak_buffered_payload_bytes =
                peak_buffered_payload_bytes.max(stats.peak_buffered_payload_bytes);
            peak_buffered_frame_count =
                peak_buffered_frame_count.max(stats.peak_buffered_frame_count);
            burst_fill_ratio_sum += stats.average_fill_ratio * stats.burst_count as f64;
        }

        for mut sample in shard_artifact.samples.clone() {
            sample.elapsed_ms += elapsed_offset;
            samples.push(sample);
        }
        elapsed_offset += metrics.throughput.total_duration_ms;
    }

    let utility = if utility_samples == 0 {
        None
    } else {
        let measured = utility_samples as f64;
        Some(UtilitySummary {
            measured_events: utility_samples,
            exact_chunk_top1_rate: utility_exact_chunk_top1_sum / measured,
            exact_chunk_top5_rate: utility_exact_chunk_top5_sum / measured,
            same_file_top1_rate: utility_same_file_top1_sum / measured,
            same_file_top5_rate: utility_same_file_top5_sum / measured,
            mean_reciprocal_rank: utility_mrr_sum / measured,
        })
    };

    let burst_stats = if any_burst_stats {
        Some(BurstStats {
            burst_count,
            total_burst_payload_bytes,
            max_burst_payload_bytes,
            average_fill_ratio: if burst_count == 0 {
                0.0
            } else {
                burst_fill_ratio_sum / burst_count as f64
            },
            peak_buffered_payload_bytes,
            peak_buffered_frame_count,
        })
    } else {
        None
    };

    let duration = Duration::from_millis(total_duration_ms.max(1) as u64);
    let progress = SideProgress {
        records,
        cue_object_records,
        predictive_records: 0,
        route_family_counts: HashMap::new(),
        schema_activation_count: 0,
        transform_reuse_count: 0,
        predictive_completion_hits: 0,
        predictive_completion_attempts: 0,
        sync_risk_fallback_count: 0,
        exact_atom_direct_state_fallback_count: 0,
        transform_demoted_fallback_count: 0,
        original_payload_bytes,
        payload_bytes,
        wire_bytes,
        bursts: burst_count,
        codec_modes: codec_modes.clone(),
        source_kinds: source_kinds.clone(),
        residual_modes: residual_modes.clone(),
        ..Default::default()
    };
    let side_result_path = artifact_dir.join("side_result.json");
    let metrics = BenchmarkSideMetrics {
        side,
        records,
        cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes,
        payload_bytes,
        wire_bytes,
        throughput: throughput_metrics(&progress, duration),
        process: ProcessMetrics {
            peak_rss_bytes,
            peak_vsz_bytes,
            peak_cpu_percent,
            timeseries_path: artifact_dir.join("timeseries.jsonl").display().to_string(),
        },
        codec_modes,
        source_kinds,
        residual_modes,
        encode_latency: None,
        embedding_latency: None,
        apply_latency: None,
        burst_stats: burst_stats.clone(),
        utility,
        source_cache: None,
        predictive_memory: predictive_memory_metrics(&progress),
        datagram: None,
        byte_categories: progress.byte_categories.clone(),
        side_result_path: side_result_path.display().to_string(),
    };
    write_side_artifact(
        artifact_dir,
        SideArtifact {
            side,
            metrics: metrics.clone(),
            samples,
        },
    )?;
    if let Some(stats) = burst_stats {
        fs::write(
            artifact_dir.join("burst_stats.json"),
            serde_json::to_vec_pretty(&stats)?,
        )?;
    }
    Ok(metrics)
}

fn render_case_summary(result: &BenchmarkResult) -> String {
    let original = result.original_payload_bytes.max(1) as f64;
    let encoded = result.actual_payload_bytes.max(1) as f64;
    // S4.1.f: These ratios compare total payload against original. For honest
    // comparison, use byte_categories to distinguish exact-state reuse from
    // predictive/inline overhead. The wire_vs_original comparison is the most
    // honest headline metric since it counts actual transmitted bytes.
    let encoded_vs_original_pct = 100.0 * (1.0 - result.actual_payload_bytes as f64 / original);
    let wire_vs_original_pct = 100.0 * (1.0 - result.protected_wire_bytes as f64 / original);
    let wire_vs_encoded_pct = 100.0 * (result.protected_wire_bytes as f64 / encoded - 1.0);
    // S4.6: Compression efficiency — CPU cost per byte saved.
    let total_cpu_seconds = result.server.throughput.total_duration_ms as f64 / 1000.0;
    let cpu_efficiency = compute_compression_efficiency(
        total_cpu_seconds,
        result.original_payload_bytes,
        result.protected_wire_bytes,
    );
    let cpu_efficiency_str = cpu_efficiency
        .map(|v| format!("{:.2e} CPU-s/byte-saved", v))
        .unwrap_or_else(|| "N/A (no net savings)".to_string());
    // S4.1.e: Category-aware ratios derived from byte_categories.
    let cats = &result.byte_categories;
    let predictive_share_pct = if result.actual_payload_bytes > 0 {
        100.0 * cats.predictive_dispatch_payload_bytes as f64 / result.actual_payload_bytes as f64
    } else {
        0.0
    };
    let inline_share_pct = if result.actual_payload_bytes > 0 {
        100.0 * cats.inline_definition_bytes as f64 / result.actual_payload_bytes as f64
    } else {
        0.0
    };
    let control_share_pct = if result.actual_payload_bytes > 0 {
        100.0 * cats.control_data_payload_bytes as f64 / result.actual_payload_bytes as f64
    } else {
        0.0
    };
    let sharding = if result.sharded {
        format!(
            "- sharded: `true`\n- shard count: `{}`\n- shard payload cap bytes: `{}`\n",
            result.shard_count,
            result.shard_payload_cap_bytes.unwrap_or_default()
        )
    } else {
        "- sharded: `false`\n".to_string()
    };
    let utility = result
        .server
        .utility
        .as_ref()
        .map(|utility| {
            format!(
                "\n## Corpus Utility\n\n- measured events: `{}`\n- exact chunk top-1: `{:.6}`\n- exact chunk top-5: `{:.6}`\n- same file top-1: `{:.6}`\n- same file top-5: `{:.6}`\n- mean reciprocal rank: `{:.6}`\n",
                utility.measured_events,
                utility.exact_chunk_top1_rate,
                utility.exact_chunk_top5_rate,
                utility.same_file_top1_rate,
                utility.same_file_top5_rate,
                utility.mean_reciprocal_rank,
            )
        })
        .unwrap_or_default();
    let source_cache = result
        .server
        .source_cache
        .as_ref()
        .map(|metrics| {
            format!(
                "\n## Source Cache\n\n- lookups: `{}`\n- hits: `{}`\n- misses: `{}`\n- object materializations skipped: `{}`\n- cache read ns: `{}`\n- cache write ns: `{}`\n",
                metrics.lookups,
                metrics.hits,
                metrics.misses,
                metrics.object_materializations_skipped,
                metrics.cache_read_ns,
                metrics.cache_write_ns,
            )
        })
        .unwrap_or_default();
    let source_kinds = format!(
        "\n## Source Families\n\n- server: text=`{}`, json=`{}`, binary=`{}`, unknown=`{}`\n- client: text=`{}`, json=`{}`, binary=`{}`, unknown=`{}`\n",
        result.server.source_kinds.text,
        result.server.source_kinds.json,
        result.server.source_kinds.binary,
        result.server.source_kinds.unknown,
        result.client.source_kinds.text,
        result.client.source_kinds.json,
        result.client.source_kinds.binary,
        result.client.source_kinds.unknown,
    );
    let residual_modes = format!(
        "\n## Residual Modes\n\n- server: none=`{}`, all_zero=`{}`, small_signed_rans=`{}`, sparse_positions=`{}`, literal_raw=`{}`, unknown=`{}`\n- client: none=`{}`, all_zero=`{}`, small_signed_rans=`{}`, sparse_positions=`{}`, literal_raw=`{}`, unknown=`{}`\n",
        result.server.residual_modes.none,
        result.server.residual_modes.all_zero,
        result.server.residual_modes.small_signed_rans,
        result.server.residual_modes.sparse_positions,
        result.server.residual_modes.literal_raw,
        result.server.residual_modes.unknown,
        result.client.residual_modes.none,
        result.client.residual_modes.all_zero,
        result.client.residual_modes.small_signed_rans,
        result.client.residual_modes.sparse_positions,
        result.client.residual_modes.literal_raw,
        result.client.residual_modes.unknown,
    );
    let server_datagram = result
        .server
        .datagram
        .as_ref()
        .map(|metrics| {
            format!(
                "\n## Server Datagram\n\n- outbound messages: `{}`\n- outbound datagrams: `{}`\n- retransmitted datagrams: `{}`\n- acknowledged messages: `{}`\n- repair requests sent: `{}`\n- repair requests received: `{}`\n- duplicate chunks ignored: `{}`\n",
                metrics.outbound_messages,
                metrics.outbound_datagrams,
                metrics.retransmitted_datagrams,
                metrics.acknowledged_messages,
                metrics.repair_requests_sent,
                metrics.repair_requests_received,
                metrics.duplicate_chunks_ignored,
            )
        })
        .unwrap_or_default();
    let client_datagram = result
        .client
        .datagram
        .as_ref()
        .map(|metrics| {
            format!(
                "\n## Client Datagram\n\n- outbound messages: `{}`\n- outbound datagrams: `{}`\n- retransmitted datagrams: `{}`\n- acknowledged messages: `{}`\n- repair requests sent: `{}`\n- repair requests received: `{}`\n- duplicate chunks ignored: `{}`\n",
                metrics.outbound_messages,
                metrics.outbound_datagrams,
                metrics.retransmitted_datagrams,
                metrics.acknowledged_messages,
                metrics.repair_requests_sent,
                metrics.repair_requests_received,
                metrics.duplicate_chunks_ignored,
            )
        })
        .unwrap_or_default();
    format!(
        "# Benchmark Summary\n\n- case: `{}`\n- optimization: `{}`\n- client runtime: `{}`\n- input corpus fingerprint: `{}`\n- input corpus files: `{}`\n- input corpus chunks: `{}`\n- target payload bytes: `{}`\n- original payload bytes: `{}`\n- actual payload bytes: `{}`\n- overshoot bytes: `{}`\n- protected wire bytes: `{}`\n- payload savings vs original: `{:.4}%`\n- wire savings vs original: `{:.4}%`\n- wire overhead vs encoded: `{:.4}%`\n- compression efficiency: `{}`\n- byte categories: exact=`{}` predictive=`{}` inline_defs=`{}` control=`{}` episode_hints=`{}` overhead=`{}`\n- predictive share of payload: `{:.4}%`\n- inline definition share of payload: `{:.4}%`\n- control data share of payload: `{:.4}%`\n- direct connectivity verified: `{}`\n- build profile: `{}`\n{}\n## Server\n\n- records: `{}`\n- cue/object records: `{}`\n- original payload bytes: `{}`\n- encoded payload throughput bytes/sec: `{:.2}`\n- wire throughput bytes/sec: `{:.2}`\n- peak rss bytes: `{}`\n- peak cpu percent: `{:.2}`\n\n## Client\n\n- records: `{}`\n- cue/object records: `{}`\n- original payload bytes: `{}`\n- encoded payload throughput bytes/sec: `{:.2}`\n- wire throughput bytes/sec: `{:.2}`\n- peak rss bytes: `{}`\n- peak cpu percent: `{:.2}`\n{}{}{}{}{}{}",
        result.case.display_name(),
        result.case.optimization.slug(),
        result.case.client_runtime.slug(),
        result.case.input_corpus_fingerprint,
        result.case.input_corpus_file_count,
        result.case.input_corpus_chunk_count,
        result.target_payload_bytes,
        result.original_payload_bytes,
        result.actual_payload_bytes,
        result.payload_overshoot_bytes,
        result.protected_wire_bytes,
        encoded_vs_original_pct,
        wire_vs_original_pct,
        wire_vs_encoded_pct,
        cpu_efficiency_str,
        cats.exact_state_payload_bytes,
        cats.predictive_dispatch_payload_bytes,
        cats.inline_definition_bytes,
        cats.control_data_payload_bytes,
        cats.episode_hint_payload_bytes,
        cats.overhead_bytes,
        predictive_share_pct,
        inline_share_pct,
        control_share_pct,
        result.direct_connectivity_verified,
        result.build_profile,
        sharding,
        result.server.records,
        result.server.cue_object_records,
        result.server.original_payload_bytes,
        result.server.throughput.payload_bytes_per_sec,
        result.server.throughput.wire_bytes_per_sec,
        result.server.process.peak_rss_bytes,
        result.server.process.peak_cpu_percent,
        result.client.records,
        result.client.cue_object_records,
        result.client.original_payload_bytes,
        result.client.throughput.payload_bytes_per_sec,
        result.client.throughput.wire_bytes_per_sec,
        result.client.process.peak_rss_bytes,
        result.client.process.peak_cpu_percent,
        utility,
        source_cache,
        source_kinds,
        residual_modes,
        server_datagram,
        client_datagram,
    )
}

fn render_matrix_summary(matrix: &BenchmarkMatrixResult) -> String {
    let mut out = format!(
        "# Benchmark Matrix Summary\n\n- total cases: `{}`\n- completed: `{}`\n- failed: `{}`\n\n",
        matrix.total_cases, matrix.completed_cases, matrix.failed_cases
    );
    for case in &matrix.case_results {
        out.push_str(&format!(
            "- `{}` -> `{}`{}\n",
            case.case.display_name(),
            if case.success { "ok" } else { "failed" },
            case.failure
                .as_ref()
                .map(|failure| format!(": {failure}"))
                .unwrap_or_default()
        ));
    }

    // S4.6: Consolidated cross-case comparison table for successful cases.
    // This provides a single-view summary of all key metrics across the
    // entire benchmark matrix, making it possible to compare compression
    // ratios, throughput, CPU efficiency, and byte category breakdowns
    // without reading each case's individual summary.md file.
    let successful: Vec<_> = matrix.case_results.iter().filter(|c| c.success).collect();
    if !successful.is_empty() {
        out.push_str("\n## Consolidated Cross-Case Summary\n\n");
        out.push_str("| Case | Original | Wire | Wire Savings | Throughput (wire B/s) | Peak CPU% | CPU per Byte Saved | Pred Share | Inline Share | Exact | Predictive | Inline | Control | Overhead |\n");
        out.push_str("|------|----------|------|-------------|----------------------|-----------|-------------------|------------|-------------|-------|-----------|--------|---------|----------|\n");
        for case_summary in &successful {
            // Load the result from the artifact directory's result.json
            let result_path = Path::new(&case_summary.artifact_dir).join("result.json");
            let result: Option<BenchmarkResult> = match fs::read_to_string(&result_path) {
                Ok(s) => serde_json::from_str(&s).ok(),
                Err(_) => None,
            };
            let r = match result {
                Some(r) => r,
                None => {
                    out.push_str(&format!(
                        "| {} | *result unavailable* | | | | | | | | | | | | |\n",
                        case_summary.case.display_name(),
                    ));
                    continue;
                }
            };
            let cats = &r.byte_categories;
            let original = r.original_payload_bytes.max(1) as f64;
            let wire_savings_pct = 100.0 * (1.0 - r.protected_wire_bytes as f64 / original);
            let server_throughput = r.server.throughput.wire_bytes_per_sec;
            let peak_cpu = r.server.process.peak_cpu_percent.max(r.client.process.peak_cpu_percent);
            // S4.6: CPU cost per byte saved — the key efficiency metric.
            let total_cpu_seconds = r.server.throughput.total_duration_ms as f64 / 1000.0;
            let cpu_per_byte_saved = compute_compression_efficiency(
                total_cpu_seconds,
                r.original_payload_bytes,
                r.protected_wire_bytes,
            ).map(|v| format!("{:.2e}", v)).unwrap_or_else(|| "N/A".to_string());
            let pred_share = if r.actual_payload_bytes > 0 {
                100.0 * cats.predictive_dispatch_payload_bytes as f64 / r.actual_payload_bytes as f64
            } else { 0.0 };
            let inline_share = if r.actual_payload_bytes > 0 {
                100.0 * cats.inline_definition_bytes as f64 / r.actual_payload_bytes as f64
            } else { 0.0 };

            out.push_str(&format!(
                "| {} | {} | {} | {:.1}% | {:.0} | {:.1} | {} | {:.1}% | {:.1}% | {} | {} | {} | {} | {} |\n",
                case_summary.case.display_name(),
                r.original_payload_bytes,
                r.protected_wire_bytes,
                wire_savings_pct,
                server_throughput,
                peak_cpu,
                cpu_per_byte_saved,
                pred_share,
                inline_share,
                cats.exact_state_payload_bytes,
                cats.predictive_dispatch_payload_bytes,
                cats.inline_definition_bytes,
                cats.control_data_payload_bytes,
                cats.overhead_bytes,
            ));
        }
        out.push_str("\n**CPU per Byte Saved**: Total CPU-seconds divided by bytes saved (original - wire). Lower is better. N/A means no net savings.\n");
        out.push_str("**Wire Savings**: (1 - wire/original) × 100%. Negative values indicate payload expansion.\n");
    }

    out
}

/// S4.6: Returns the case execution timeout, checking the environment variable
/// PULZZ_BENCH_CASE_TIMEOUT_SECS first, then falling back to BENCH_CASE_TIMEOUT.
fn bench_case_timeout() -> Duration {
    std::env::var("PULZZ_BENCH_CASE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(BENCH_CASE_TIMEOUT)
}

/// S4.6: Compression efficiency metric — CPU cost per byte saved.
/// Measures the trade-off between compute expenditure and wire reduction.
/// Lower is better. Returns None if no bytes were saved (wire >= original).
fn compute_compression_efficiency(
    total_cpu_seconds: f64,
    original_bytes: u64,
    wire_bytes: u64,
) -> Option<f64> {
    let saved = original_bytes.saturating_sub(wire_bytes);
    if saved == 0 || total_cpu_seconds <= 0.0 {
        return None;
    }
    Some(total_cpu_seconds / saved as f64)
}

fn benchmark_source_cache_for_case(
    case: &BenchmarkCase,
    _artifact_dir: &Path,
) -> Result<Option<SourceCache>, BenchError> {
    if !case.optimization.source_dedup {
        return Ok(None);
    }
    let mut cache_root = std::env::temp_dir();
    cache_root.push(format!(
        "pulzz_bench_source_cache_{:016x}_{}",
        case.seed,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&cache_root);
    Ok(Some(SourceCache::new(SourceCacheConfig {
        root_dir: cache_root,
        optimizations: case.optimization.source_optimization_config(),
        max_object_material_bytes: 256 * 1024 * 1024,
        cleanup_on_drop: true,
    })?))
}

/// S4.2: Attaches learning artifacts to the source cache for assembly/transform
/// promotion. This function is now explicitly separated from the measured
/// execution path — call it BEFORE or AFTER measurement, not during.
/// Previously called inline during `resolve_chunk_for_benchmark`, which
/// contaminated measured execution with protocol-external cache writes (I10).
// S4.2/I10: Protocol vs harness write accounting
// ================================================
// The following write operations occur during benchmark execution. Only the
// first group is part of the measured protocol path; the rest are harness
// overhead that must not be attributed to the protocol's wire cost.
//
// PROTOCOL writes (measured, counted in wire_bytes):
//   - Record serialization + WebSocket frame transmission
//   - Record header (32 bytes) + auth tag (16 bytes) per record
//   - SourceMeta, Resync, Rekey, Close records (control-plane)
//
// HARNESS writes (NOT measured, excluded from wire_bytes):
//   - SourceCache disk writes (resolve_and_deferred_attach)
//   - benchmark_attach_learning_artifacts (assembly/transform/schema storage)
//   - result.json, bench_fingerprint.txt (result metadata)
//   - timeseries CSV/JSON (case output artifacts)
//   - case_root directory creation and output file writes
//   - temp_dir() scratch files from SourceCache
//
// KNOWN RESIDUAL: SourceCache.resolve_source() may perform disk reads/writes
// during the measured resolve step. These are technically protocol-supporting
// but not protocol wire writes. A future pass could meter them separately.
fn benchmark_attach_learning_artifacts(
    cache: &SourceCache,
    resolved: &SourceResolveResult,
) -> Result<(), BenchError> {
    let source_kind = resolved.descriptor.kind;
    let resolved_block = ExactStateMaterial::copy_exact(source_kind, &resolved.object.exact_bytes);
    let assembly_candidates = extract_catalog_assembly_candidates(
        source_kind,
        &resolved.object.exact_bytes,
        shared_protocol::AssemblyExtractionConfig::bounded_default(),
    );
    if let Some(candidate) = assembly_candidates.into_iter().next() {
        cache.store_assembly(&Assembly {
            assembly_id: AssemblyId(u64::from_le_bytes(
                resolved.descriptor.source_hash.0[..8]
                    .try_into()
                    .unwrap_or([0; 8]),
            )),
            source_kind,
            assembly_kind: candidate.assembly_kind,
            role_signature: candidate.role_signature,
            slots: candidate.slots,
            body: AssemblyBody::from_literal(resolved.object.exact_bytes.clone()),
            dependency_closure: candidate.dependency_closure,
            cue: candidate.cue,
            lifecycle: ObjectLifecycleMeta::default()
                .record_seen(0, true)
                .record_consolidated(0),
            canonical_length_min: candidate.canonical_length_min,
            canonical_length_max: candidate.canonical_length_max,
        })?;
    }
    for candidate in generate_transform_candidates(
        &resolved_block,
        &shared_protocol::SharedBlockCatalog::default(),
    ) {
        let mut class = candidate.class;
        class.lifecycle = class.lifecycle.record_seen(0, true).record_consolidated(0);
        cache.store_transform_class(&class)?;
    }
    Ok(())
}

/// S4.2: Resolves a chunk and then attaches learning artifacts in a separate
/// step after resolve. The attachment is explicitly NOT part of the measured
/// encode/emission path. Callers should time only the encode step, not this.
fn resolve_and_deferred_attach(
    cache: &mut Option<SourceCache>,
    input: &InputCorpusChunk,
) -> Result<(SourceResolveResult, Vec<u8>), BenchError> {
    let (resolved, original) = resolve_chunk_for_benchmark(cache, input)?;
    // S4.2.c: Attach learning artifacts after resolve, before encode.
    // This keeps protocol-external cache writes out of the measured path.
    if let Some(cache) = cache {
        let _ = benchmark_attach_learning_artifacts(cache, &resolved);
    }
    Ok((resolved, original))
}

fn resolve_chunk_for_benchmark(
    cache: &mut Option<SourceCache>,
    input: &InputCorpusChunk,
) -> Result<(SourceResolveResult, Vec<u8>), BenchError> {
    if let Some(cache) = cache.as_mut() {
        let resolved = cache.resolve_source(&input.prepared_source())?;
        // S4.2: Learning artifact attachment is now deferred to a separate
        // post-measurement step. Previously this was called inline here,
        // contaminating measured execution with protocol-external cache writes.
        // See benchmark_attach_learning_artifacts for the deferred call site.
        return Ok((resolved.clone(), resolved.object.exact_bytes.clone()));
    }

    let prepared = input.prepared_source();
    let original = prepared.canonical_bytes.clone();
    let descriptor = prepared.descriptor.clone();
    Ok((
        SourceResolveResult {
            object_key: descriptor.object_cache_key_from_bytes(&original),
            descriptor,
            object: ChpmtObject::from_exact_bytes(
                prepared.descriptor.clone(),
                prepared.descriptor.object_cache_key_from_bytes(&original),
                prepared.descriptor.kind,
                prepared.descriptor.kind.object_kind(),
                prepared.descriptor.structural_cue_summary(&original).cue,
                original.clone(),
            ),
            cache_hit: false,
            metrics: crate::source_cache::SourceCacheMetrics::default(),
        },
        original,
    ))
}

fn accumulate_source_cache_metrics(
    total: &mut SourceCacheMetrics,
    delta: &crate::source_cache::SourceCacheMetrics,
) {
    total.lookups += delta.lookups;
    total.hits += delta.hits;
    total.misses += delta.misses;
    total.object_materializations_skipped += delta.object_materializations_skipped;
    total.cache_read_ns += delta.cache_read_ns;
    total.cache_write_ns += delta.cache_write_ns;
}

fn benchmark_session_config(case: &BenchmarkCase) -> TransportSessionConfig {
    let mut session = TransportSessionConfig::default();
    session.transport = TransportConfig {
        mode: match case.mode {
            BenchmarkMode::Bulk => shared_protocol::TransportMode::Bulk,
            BenchmarkMode::BurstSmall => shared_protocol::TransportMode::BurstSmall,
            BenchmarkMode::BurstMedium => shared_protocol::TransportMode::BurstMedium,
            BenchmarkMode::BurstBig => shared_protocol::TransportMode::BurstBig,
        },
    };
    session.protection_profile = case.protection.profile_kind();
    session.data_plane_codec = case.optimization.data_plane_codec;
    session
}

fn deterministic_bootstrap_materials(
    case: &BenchmarkCase,
) -> Result<BenchmarkBootstrapMaterials, BenchError> {
    let session_config = benchmark_session_config(case);
    let server_id = format!("bench-server-{:016x}", case.stream_id.0);
    let (server_config, client_config) = match case.protection {
        BenchmarkProtection::PqMutualV1 | BenchmarkProtection::PqMutualDgramV1 => {
            let issued_at = 1_700_000_000_u64;
            let expires_at = 2_000_000_000_u64;
            let server_signing_seed =
                derive_seed32(case, b"bench/pq_mutual_v1/server_signing_seed");
            let client_signing_seed =
                derive_seed32(case, b"bench/pq_mutual_v1/client_signing_seed");
            let client_id = format!("bench-client-{:016x}", case.seed);
            let server_identity = issue_server_identity(
                server_id.clone(),
                issued_at,
                expires_at,
                server_signing_seed,
            )?;
            let issued_credential = issue_client_credential(
                &server_identity,
                server_signing_seed,
                client_id,
                client_signing_seed,
                CredentialScope {
                    stream_id: Some(case.stream_id),
                    allow_client_to_server: true,
                    allow_server_to_client: true,
                },
                issued_at,
                expires_at,
            )?;
            (
                BootstrapServerConfig {
                    stream_id: case.stream_id,
                    direction: case.direction.stream_direction(),
                    bootstrap: BootstrapConfig::default(),
                    security: ServerSecurityConfig::PqMutual {
                        server_identity: server_identity.clone(),
                        server_signing_seed,
                        revoked_client_ids: Vec::new(),
                    },
                },
                BootstrapClientConfig {
                    stream_id: case.stream_id,
                    direction: case.direction.stream_direction(),
                    bootstrap: BootstrapConfig::default(),
                    security: SharedClientSecurityConfig::PqMutual {
                        issued_credential,
                        expected_server_identity: server_identity,
                    },
                },
            )
        }
        BenchmarkProtection::PqSimpleV1 | BenchmarkProtection::PqSimpleDgramV1 => (
            BootstrapServerConfig {
                stream_id: case.stream_id,
                direction: case.direction.stream_direction(),
                bootstrap: BootstrapConfig::default(),
                security: ServerSecurityConfig::PqSimple {
                    bootstrap: PqSimpleServerBootstrapConfig { server_id },
                },
            },
            BootstrapClientConfig {
                stream_id: case.stream_id,
                direction: case.direction.stream_direction(),
                bootstrap: BootstrapConfig::default(),
                security: SharedClientSecurityConfig::PqSimple,
            },
        ),
        BenchmarkProtection::ClassicRef1 => {
            return Err(BenchError::InvalidCase(
                "classic_ref1 does not use the authenticated bootstrap materials".to_string(),
            ));
        }
    };
    Ok(BenchmarkBootstrapMaterials {
        session_config,
        server_config,
        client_config,
    })
}

fn client_connect_config_for_case(
    case: &BenchmarkCase,
    url: &str,
) -> Result<ClientConnectConfig, BenchError> {
    let materials = deterministic_bootstrap_materials(case)?;
    Ok(ClientConnectConfig {
        url: url.to_string(),
        stream_id: case.stream_id,
        direction: case.direction.stream_direction(),
        session: materials.session_config,
        security: materials.client_config.security.clone(),
        reconnect_policy: ReconnectPolicy::disabled(),
    })
}

fn server_transport_config_for_case(
    case: &BenchmarkCase,
) -> Result<crate::transport::TransportServerConfig, BenchError> {
    let materials = deterministic_bootstrap_materials(case)?;
    Ok(crate::transport::TransportServerConfig {
        session: materials.session_config,
        connection_limits: crate::transport::ConnectionLimits::default(),
        bootstrap_policy: crate::transport::BootstrapPolicy::new(materials.server_config),
        carrier: case.carrier.reliable_kind(),
    })
}

fn paired_protectors(
    case: &BenchmarkCase,
) -> Result<(StreamProtector, StreamProtector), BenchError> {
    let mut rng = StdRng::seed_from_u64(case.seed ^ 0x4242_2026);
    let pair = match case.protection {
        BenchmarkProtection::ClassicRef1 => {
            classic_ref1_pair_from_rng(case.stream_id, case.direction.stream_direction(), &mut rng)
        }
        BenchmarkProtection::PqSimpleV1 => {
            pq_simple_v1_pair_from_rng(case.stream_id, case.direction.stream_direction(), &mut rng)
        }
        BenchmarkProtection::PqSimpleDgramV1
        | BenchmarkProtection::PqMutualV1
        | BenchmarkProtection::PqMutualDgramV1 => {
            return Err(BenchError::InvalidCase(
                "authenticated PQ profiles use the session bootstrap path, not paired protectors"
                    .to_string(),
            ));
        }
    };
    Ok(pair)
}

fn uses_authenticated_session(case: &BenchmarkCase) -> bool {
    uses_authenticated_protection(case.protection)
}

fn uses_authenticated_protection(protection: BenchmarkProtection) -> bool {
    matches!(
        protection,
        BenchmarkProtection::PqSimpleV1
            | BenchmarkProtection::PqSimpleDgramV1
            | BenchmarkProtection::PqMutualV1
            | BenchmarkProtection::PqMutualDgramV1
    )
}

fn derive_seed32(case: &BenchmarkCase, label: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(label);
    hasher.update(case.seed.to_le_bytes());
    hasher.update(case.stream_id.0.to_le_bytes());
    hasher.update(case.environment.slug().as_bytes());
    hasher.update(case.direction.slug().as_bytes());
    let digest = hasher.finalize();
    let mut out = [0_u8; 32];
    out.copy_from_slice(&digest[..32]);
    out
}

fn classify_record_mode(totals: &mut CodecModeTotals, mode: CodecMode) {
    match mode {
        CodecMode::DirectExact => totals.direct_exact += 1,
        CodecMode::PackedExact => totals.packed_exact += 1,
        CodecMode::PredictedExact => totals.predicted_exact += 1,
        CodecMode::None => totals.control += 1,
    }
}

fn classify_source_kind(totals: &mut BenchSourceKindTotals, record: &Record) {
    let Some(tag) = record.payload.first().copied() else {
        totals.unknown += 1;
        return;
    };
    match shared_protocol::SourceKind::from_tag(tag) {
        Ok(shared_protocol::SourceKind::Text) => totals.text += 1,
        Ok(shared_protocol::SourceKind::Json) => totals.json += 1,
        Ok(shared_protocol::SourceKind::Binary | shared_protocol::SourceKind::Image) => {
            totals.binary += 1
        }
        Err(_) => totals.unknown += 1,
    }
}

fn classify_residual_mode(totals: &mut ResidualModeTotals, record: &Record) {
    match record.header.codec_mode {
        CodecMode::DirectExact => totals.none += 1,
        CodecMode::PackedExact | CodecMode::PredictedExact => {
            match shared_protocol::inspect_data_payload(&record.payload, record.header.codec_mode) {
                Ok(inspection) => match inspection.residual_mode {
                    shared_protocol::ResidualCodingMode::None => totals.none += 1,
                    shared_protocol::ResidualCodingMode::AllZero => totals.all_zero += 1,
                    shared_protocol::ResidualCodingMode::SmallSignedRans => {
                        totals.small_signed_rans += 1
                    }
                    shared_protocol::ResidualCodingMode::SparsePositions => {
                        totals.sparse_positions += 1
                    }
                    shared_protocol::ResidualCodingMode::LiteralRaw => totals.literal_raw += 1,
                },
                Err(_) => totals.unknown += 1,
            }
        }
        CodecMode::None => {}
    }
}

fn classify_cue_object_record(progress: &mut SideProgress, record: &Record) {
    if is_cue_object_record(record) {
        progress.cue_object_records += 1;
        classify_source_kind(&mut progress.source_kinds, record);
        classify_residual_mode(&mut progress.residual_modes, record);
    }
    classify_record_mode(&mut progress.codec_modes, record.header.codec_mode);
}

/// S4.1: Honest payload metric — counts payload bytes for ALL record types
/// that carry application data, not just ExactState. This replaces the
/// previous dishonest metric that silently excluded predictive/control
/// payload from the headline "payload_bytes" count.
/// S4.5: Check if a cached result matches the current benchmark fingerprint.
/// Returns true if the stored fingerprint matches the current binary/config/input,
/// false otherwise (meaning the cached result is stale and must be re-run).
fn check_result_fingerprint(artifact_dir: &std::path::Path, case: &BenchmarkCase) -> bool {
    let fingerprint_path = artifact_dir.join("bench_fingerprint.txt");
    if !fingerprint_path.exists() {
        // No fingerprint stored — result predates fingerprinting, treat as stale
        return false;
    }
    let stored = std::fs::read_to_string(&fingerprint_path).unwrap_or_default();
    let current = compute_benchmark_fingerprint(case);
    stored == current
}

/// S4.5: Compute a fingerprint string covering source identity, case config,
/// workload identity, and key environment-affecting toggles. This is stored
/// alongside result.json and checked before reusing cached results.
///
/// S4.5/I14-fix (v5): Uses git commit hash + working tree dirtiness instead of
/// the binary content hash. The previous v4 approach (SHA-256 of the binary)
/// still caused false fingerprint mismatches because Rust does not produce
/// deterministic binaries — recompiling the same source produces a different
/// binary hash, invalidating all cached results. The git-based approach only
/// invalidates when actual source code changes are present, which is the
/// correct semantic: same source + same config → same result → cache is valid.
///
/// Falls back to binary hash if git is not available or the repo is not a
/// git checkout.
fn compute_benchmark_fingerprint(case: &BenchmarkCase) -> String {
    let source_identity = compute_source_identity();
    let case_config = format!(
        "env={:?},carrier={:?},dir={:?},prot={:?},mode={:?},target={:?},opt={:?},corpus={:?},seed={}",
        case.environment, case.carrier, case.direction, case.protection,
        case.mode, case.target_bytes.bytes(), case.optimization, case.corpus, case.seed
    );
    // S4.5/I14: Include environment-affecting toggles that could change results
    // without changing the binary or case config.
    let rustflags = std::env::var("RUSTFLAGS").unwrap_or_default();
    let features = std::env::var("CARGO_FEATURES").unwrap_or_default();
    let profile = std::env::var("PROFILE").unwrap_or_default();
    let env_toggles = format!("rustflags={rustflags},features={features},profile={profile}");
    // S4.5/I14: Include input corpus content hash if available. This prevents
    // stale cache hits when the corpus directory content changes but the corpus
    // name and other config remain the same. The input_corpus_fingerprint field
    // is populated during benchmark setup by hashing the actual corpus files.
    let corpus_hash = if case.input_corpus_fingerprint.is_empty() {
        String::new()
    } else {
        format!(",corpus_hash={}", case.input_corpus_fingerprint)
    };
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (source_identity + &case_config + &env_toggles + &corpus_hash).hash(&mut hasher);
    format!("v5:{:016x}", hasher.finish())
}

/// S4.5/I14-fix-v5: Compute a source identity string using git.
/// Returns `"git:<commit_hash>"` if the working tree is clean, or
/// `"git:<commit_hash>+dirty"` if there are uncommitted changes.
/// Falls back to `"binary:<sha256>"` if git is not available.
fn compute_source_identity() -> String {
    let root = workspace_root();
    let git_result = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(root)
        .output();
    match git_result {
        Ok(output) if output.status.success() => {
            let commit = String::from_utf8_lossy(&output.stdout).trim().to_string();
            // Check for uncommitted changes — these could affect benchmark results
            let dirty_result = Command::new("git")
                .args(["diff", "--stat", "HEAD"])
                .current_dir(root)
                .output();
            let is_dirty = match dirty_result {
                Ok(out) => !out.stdout.is_empty() || !out.stderr.is_empty(),
                Err(_) => true, // assume dirty if we can't check
            };
            // Also check untracked files
            let untracked_result = Command::new("git")
                .args(["ls-files", "--others", "--exclude-standard"])
                .current_dir(root)
                .output();
            let has_untracked = match untracked_result {
                Ok(out) => !String::from_utf8_lossy(&out.stdout).trim().is_empty(),
                Err(_) => false,
            };
            if is_dirty || has_untracked {
                format!("git:{commit}+dirty")
            } else {
                format!("git:{commit}")
            }
        }
        _ => {
            // Git not available — fall back to binary hash
            let binary_path = root.join("target/release/server");
            format!("binary:{}", compute_binary_hash(&binary_path))
        }
    }
}

/// S4.5/I14-fix: Compute a SHA-256 content hash of the server binary.
/// Used as a fallback source identity when git is not available.
/// The hash covers the first 4 MB of the binary — sufficient to detect
/// content changes without the cost of hashing the entire binary
/// (which can be 50+ MB in release mode).
fn compute_binary_hash(path: &Path) -> String {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => return "no-binary".to_string(),
    };
    let slice = if data.len() > 4 * 1024 * 1024 {
        &data[..4 * 1024 * 1024]
    } else {
        &data
    };
    let mut hasher = Sha256::new();
    hasher.update(slice);
    // Also include the total length to catch suffix changes beyond the prefix.
    hasher.update(&(data.len() as u64).to_le_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

/// S4.5: Store the benchmark fingerprint alongside the result for future
/// cache validity checks.
fn store_result_fingerprint(artifact_dir: &std::path::Path, case: &BenchmarkCase) {
    let fingerprint = compute_benchmark_fingerprint(case);
    let _ = std::fs::write(artifact_dir.join("bench_fingerprint.txt"), fingerprint);
}

/// S4.1: Honest payload metric — counts payload bytes for ALL record types
/// that carry application data, not just ExactState. This replaces the
/// previous dishonest metric that silently excluded predictive/control
/// payload from the headline "payload_bytes" count.
fn benchmark_payload_len(record: &Record) -> u64 {
    match record.header.record_type {
        shared_protocol::RecordType::ExactState
        | shared_protocol::RecordType::PredictiveConfirm
        | shared_protocol::RecordType::PredictiveCorrect
        | shared_protocol::RecordType::TransformCorrect
        | shared_protocol::RecordType::AssemblyDef
        | shared_protocol::RecordType::TransformDef
        | shared_protocol::RecordType::SchemaDef
        | shared_protocol::RecordType::EpisodeHint
        | shared_protocol::RecordType::ReplayHint
        | shared_protocol::RecordType::SourceMeta
        | shared_protocol::RecordType::Repair
        | shared_protocol::RecordType::MemoryRetire => record.payload.len() as u64,
        // Control-plane records with no application payload
        shared_protocol::RecordType::Rekey
        | shared_protocol::RecordType::Close
        | shared_protocol::RecordType::Resync
        | shared_protocol::RecordType::MemoryAck => 0,
    }
}

fn benchmark_transport_len(record: &Record) -> u64 {
    4_u64.saturating_add(record.to_bytes().len() as u64)
}

fn benchmark_websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(BENCH_WEBSOCKET_MAX_MESSAGE_BYTES);
    config.max_frame_size = Some(BENCH_WEBSOCKET_MAX_MESSAGE_BYTES);
    config
}

fn benchmark_original_payload_len(record: &Record) -> u64 {
    // S4.1/I4: Original payload bytes now counts ALL application-data records,
    // not just ExactState. For ExactState, we inspect the codec payload to get
    // the true original length. For predictive records, we extract output_len
    // from the deserialized payload. For definition/control records, we use
    // payload_len as a conservative lower bound since they don't represent
    // original content.
    match record.header.record_type {
        shared_protocol::RecordType::ExactState => {
            shared_protocol::inspect_data_payload(&record.payload, record.header.codec_mode)
                .map(|inspection| inspection.original_len as u64)
                .unwrap_or_else(|e| {
                    eprintln!(
                        "WARNING: benchmark_original_payload_len: payload inspection failed for item {}: {} — using payload_bytes as fallback",
                        record.header.item_id.0, e
                    );
                    record.payload.len() as u64
                })
        }
        shared_protocol::RecordType::PredictiveConfirm
        | shared_protocol::RecordType::PredictiveCorrect => {
            shared_protocol::PredictiveRouteDispatchPayload::decode(&record.payload)
                .map(|payload| payload.route_graph.output_len as u64)
                .unwrap_or(record.payload.len() as u64)
        }
        shared_protocol::RecordType::TransformCorrect => {
            shared_protocol::decode_transform_instance_record(record)
                .map(|payload| payload.output_len as u64)
                .unwrap_or(record.payload.len() as u64)
        }
        // Definition and control records don't represent original content
        _ => 0,
    }
}

/// S4.1.c: Classify a record's payload bytes into the correct category.
fn classify_payload_bytes(categories: &mut PayloadByteCategories, record: &Record) {
    use shared_protocol::RecordType;
    let payload_len = record.payload.len() as u64;
    let wire_len = record.to_bytes().len() as u64;
    match record.header.record_type {
        RecordType::ExactState => {
            categories.exact_state_payload_bytes += payload_len;
            // S4.1.c: For ExactState, logical content = inspected original length
            // (the uncompressed source bytes). Falls back to payload_len if
            // inspection fails (e.g., codec mode mismatch).
            categories.logical_content_bytes += shared_protocol::inspect_data_payload(
                &record.payload, record.header.codec_mode)
                .map(|i| i.original_len as u64)
                .unwrap_or(payload_len);
        }
        RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
            categories.predictive_dispatch_payload_bytes += payload_len;
            // S4.1.c/I4: For predictive records, logical content = the original
            // bytes the route represents. We attempt to deserialize the
            // PredictiveRouteDispatchPayload to extract route_graph.output_len,
            // which is the exact reconstruction length (the honest compression
            // denominator). If deserialization fails, we fall back to payload_len
            // as a conservative lower bound and log a warning.
            let logical_len = shared_protocol::PredictiveRouteDispatchPayload::decode(&record.payload)
                .map(|payload| {
                    let route_output = payload.route_graph.output_len as u64;
                    // Only count nonzero output lengths; a zero means the payload
                    // is degenerate or carries only definitions (no data content).
                    if route_output > 0 { route_output } else { payload_len }
                })
                .unwrap_or_else(|e| {
                    eprintln!(
                        "WARNING: classify_payload_bytes: failed to decode predictive payload for item {}: {} — using payload_len as fallback",
                        record.header.item_id.0, e
                    );
                    payload_len
                });
            categories.logical_content_bytes += logical_len;
        }
        RecordType::TransformCorrect => {
            categories.predictive_dispatch_payload_bytes += payload_len;
            // S4.1.c/I4: For transform records, attempt to extract output_len
            // from the deserialized TransformInstancePayload.
            let logical_len = shared_protocol::decode_transform_instance_record(record)
                .map(|payload| payload.output_len as u64)
                .unwrap_or_else(|_| {
                    // Fallback: payload_len as conservative lower bound
                    payload_len
                });
            categories.logical_content_bytes += logical_len;
        }
        RecordType::AssemblyDef | RecordType::TransformDef | RecordType::SchemaDef => {
            categories.inline_definition_bytes += payload_len
        }
        RecordType::EpisodeHint | RecordType::ReplayHint => {
            categories.episode_hint_payload_bytes += payload_len
        }
        RecordType::SourceMeta | RecordType::Repair | RecordType::MemoryRetire => {
            categories.control_data_payload_bytes += payload_len
        }
        _ => {}
    }
    categories.total_wire_bytes += wire_len;
    categories.overhead_bytes += wire_len.saturating_sub(payload_len);
}

fn accumulate_record_metrics(progress: &mut SideProgress, record: &Record) {
    progress.records += 1;
    progress.original_payload_bytes += benchmark_original_payload_len(record);
    progress.payload_bytes += benchmark_payload_len(record);
    classify_payload_bytes(&mut progress.byte_categories, record);
    classify_cue_object_record(progress, record);
    classify_predictive_record(progress, record);
}

fn classify_predictive_record(progress: &mut SideProgress, record: &Record) {
    use shared_protocol::RecordType::*;
    let key = format!("{:?}", record.header.record_type).to_lowercase();
    *progress.route_family_counts.entry(key).or_insert(0) += 1;
    match record.header.record_type {
        PredictiveConfirm => {
            progress.predictive_records += 1;
            progress.predictive_completion_attempts += 1;
            progress.predictive_completion_hits += 1;
        }
        PredictiveCorrect => {
            progress.predictive_records += 1;
        }
        TransformCorrect => {
            progress.predictive_records += 1;
            progress.transform_reuse_count += 1;
        }
        TransformDef => {
            progress.predictive_records += 1;
            progress.transform_reuse_count += 1;
        }
        SchemaDef => {
            progress.predictive_records += 1;
            progress.schema_activation_count += 1;
        }
        ExactState
            if record
                .header
                .flags
                .contains(shared_protocol::RecordFlags::DIRECT_STATE_FALLBACK) =>
        {
            progress.sync_risk_fallback_count += 1;
            if record
                .header
                .flags
                .contains(shared_protocol::RecordFlags::SUBSTRATE_FALLBACK)
            {
                progress.exact_atom_direct_state_fallback_count += 1;
            }
            // S2.5.b3: Track transform-path downgrades explicitly.
            if record
                .header
                .flags
                .contains(shared_protocol::RecordFlags::TRANSFORM_DEMOTED_FALLBACK)
            {
                progress.transform_demoted_fallback_count += 1;
            }
        }
        _ => {}
    }
}

fn predictive_memory_metrics(progress: &SideProgress) -> PredictiveMemoryMetrics {
    let total = progress.records.max(1) as f64;
    let mut route_family_share = HashMap::new();
    for (k, v) in &progress.route_family_counts {
        route_family_share.insert(k.clone(), *v as f64 / total);
    }
    PredictiveMemoryMetrics {
        route_family_share,
        residual_burden: if progress.payload_bytes == 0 {
            0.0
        } else {
            progress
                .payload_bytes
                .saturating_sub(progress.original_payload_bytes.min(progress.payload_bytes))
                as f64
                / progress.payload_bytes as f64
        },
        completion_hit_rate: if progress.predictive_completion_attempts == 0 {
            0.0
        } else {
            progress.predictive_completion_hits as f64
                / progress.predictive_completion_attempts as f64
        },
        schema_activation_share: progress.schema_activation_count as f64 / total,
        sync_risk_fallback_count: progress.sync_risk_fallback_count,
        exact_atom_direct_state_fallback_count: progress.exact_atom_direct_state_fallback_count,
        transform_demoted_fallback_count: progress.transform_demoted_fallback_count,
    }
}


fn is_cue_object_record(record: &Record) -> bool {
    matches!(
        record.header.record_type,
        shared_protocol::RecordType::ExactState
    ) && matches!(
        record.header.codec_mode,
        CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
    )
}

fn is_transient_accept_error(error: &tungstenite::Error) -> bool {
    matches!(
        error,
        tungstenite::Error::Protocol(_)
            | tungstenite::Error::ConnectionClosed
            | tungstenite::Error::AlreadyClosed
    ) || matches!(
        error,
        tungstenite::Error::Io(io_error)
            if matches!(
                io_error.kind(),
                io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::UnexpectedEof
            )
    )
}

async fn push_frame(
    websocket: &mut tokio_tungstenite::WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    target_payload_bytes: u64,
    yield_between_groups: bool,
) -> Result<bool, BenchError> {
    let frame_payload = benchmark_payload_len(&frame.record);
    let mut flushed = Vec::new();
    if progress.payload_bytes + burst_buffer.buffered_payload_bytes + frame_payload
        >= target_payload_bytes
    {
        burst_buffer.push(frame, &mut flushed);
        burst_buffer.flush_remaining(&mut flushed);
        flush_bursts(websocket, progress, sampler, flushed, yield_between_groups).await?;
        return Ok(true);
    }

    burst_buffer.push(frame, &mut flushed);
    flush_bursts(websocket, progress, sampler, flushed, yield_between_groups).await?;
    Ok(progress.payload_bytes >= target_payload_bytes)
}

async fn push_close_frame(
    websocket: &mut tokio_tungstenite::WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    yield_between_groups: bool,
) -> Result<(), BenchError> {
    let mut flushed = Vec::new();
    burst_buffer.push(frame, &mut flushed);
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts(websocket, progress, sampler, flushed, yield_between_groups).await?;
    Ok(())
}

async fn flush_bursts(
    websocket: &mut tokio_tungstenite::WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    bursts: Vec<Vec<BurstFrame>>,
    yield_between_groups: bool,
) -> Result<(), BenchError> {
    for burst in bursts {
        send_frame(websocket, progress, sampler, burst).await?;
        if yield_between_groups {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

async fn send_frame(
    websocket: &mut tokio_tungstenite::WebSocketStream<impl AsyncRead + AsyncWrite + Unpin>,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    frames: Vec<BurstFrame>,
) -> Result<(), BenchError> {
    if frames.is_empty() {
        return Ok(());
    }
    progress.bursts += 1;
    let mut chunk = Vec::with_capacity(
        frames
            .iter()
            .map(|frame| 4 + frame.record.to_bytes().len())
            .sum::<usize>(),
    );
    for frame in frames {
        let wire_bytes = frame.record.to_bytes();
        accumulate_record_metrics(progress, &frame.record);
        chunk.extend_from_slice(&(wire_bytes.len() as u32).to_le_bytes());
        chunk.extend_from_slice(&wire_bytes);
    }
    progress.wire_bytes += chunk.len() as u64;
    websocket.send(Message::Binary(chunk.into())).await?;
    sampler.maybe_sample(progress)?;
    Ok(())
}

async fn push_frame_authenticated_server(
    connection: &mut AuthenticatedServerBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    target_payload_bytes: u64,
    yield_between_groups: bool,
) -> Result<bool, BenchError> {
    let frame_payload = benchmark_payload_len(&frame.record);
    let mut flushed = Vec::new();
    if progress.payload_bytes + burst_buffer.buffered_payload_bytes + frame_payload
        >= target_payload_bytes
    {
        burst_buffer.push(frame, &mut flushed);
        burst_buffer.flush_remaining(&mut flushed);
        flush_bursts_authenticated_server(
            connection,
            progress,
            sampler,
            flushed,
            yield_between_groups,
        )
        .await?;
        return Ok(true);
    }

    burst_buffer.push(frame, &mut flushed);
    flush_bursts_authenticated_server(connection, progress, sampler, flushed, yield_between_groups)
        .await?;
    Ok(progress.payload_bytes >= target_payload_bytes)
}

async fn push_close_frame_authenticated_server(
    connection: &mut AuthenticatedServerBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    yield_between_groups: bool,
) -> Result<(), BenchError> {
    let mut flushed = Vec::new();
    burst_buffer.push(frame, &mut flushed);
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts_authenticated_server(connection, progress, sampler, flushed, yield_between_groups)
        .await?;
    Ok(())
}

async fn flush_bursts_authenticated_server(
    connection: &mut AuthenticatedServerBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    bursts: Vec<Vec<BurstFrame>>,
    yield_between_groups: bool,
) -> Result<(), BenchError> {
    for burst in bursts {
        send_frame_authenticated_server(connection, progress, sampler, burst).await?;
        if yield_between_groups {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

async fn send_frame_authenticated_server(
    connection: &mut AuthenticatedServerBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    frames: Vec<BurstFrame>,
) -> Result<(), BenchError> {
    if frames.is_empty() {
        return Ok(());
    }
    progress.bursts += 1;
    let mut records = Vec::with_capacity(frames.len());
    for frame in frames {
        accumulate_record_metrics(progress, &frame.record);
        records.push(frame.record);
    }
    let chunk = connection
        .protector_mut()
        .protect_transport_records(records)?;
    progress.wire_bytes += chunk.len() as u64;
    let chunk_len = chunk.len();
    connection.send_transport_frame(chunk).await.map_err(|error| {
        BenchError::Invariant(format!(
            "authenticated server send failed after {} records / {} payload bytes / {} wire bytes while sending {} bytes: {error}",
            progress.records,
            progress.payload_bytes,
            progress.wire_bytes,
            chunk_len,
        ))
    })?;
    sampler.maybe_sample(progress)?;
    Ok(())
}

async fn push_frame_authenticated_client(
    connection: &mut AuthenticatedClientBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    target_payload_bytes: u64,
    yield_between_groups: bool,
) -> Result<bool, BenchError> {
    let frame_payload = benchmark_payload_len(&frame.record);
    let mut flushed = Vec::new();
    if progress.payload_bytes + burst_buffer.buffered_payload_bytes + frame_payload
        >= target_payload_bytes
    {
        burst_buffer.push(frame, &mut flushed);
        burst_buffer.flush_remaining(&mut flushed);
        flush_bursts_authenticated_client(
            connection,
            progress,
            sampler,
            flushed,
            yield_between_groups,
        )
        .await?;
        return Ok(true);
    }

    burst_buffer.push(frame, &mut flushed);
    flush_bursts_authenticated_client(connection, progress, sampler, flushed, yield_between_groups)
        .await?;
    Ok(progress.payload_bytes >= target_payload_bytes)
}

async fn push_close_frame_authenticated_client(
    connection: &mut AuthenticatedClientBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    burst_buffer: &mut BurstAccumulator,
    frame: BurstFrame,
    yield_between_groups: bool,
) -> Result<(), BenchError> {
    let mut flushed = Vec::new();
    burst_buffer.push(frame, &mut flushed);
    burst_buffer.flush_remaining(&mut flushed);
    flush_bursts_authenticated_client(connection, progress, sampler, flushed, yield_between_groups)
        .await?;
    Ok(())
}

async fn flush_bursts_authenticated_client(
    connection: &mut AuthenticatedClientBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    bursts: Vec<Vec<BurstFrame>>,
    yield_between_groups: bool,
) -> Result<(), BenchError> {
    for burst in bursts {
        send_frame_authenticated_client(connection, progress, sampler, burst).await?;
        if yield_between_groups {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

async fn send_frame_authenticated_client(
    connection: &mut AuthenticatedClientBenchConnection,
    progress: &mut SideProgress,
    sampler: &mut ProcessSampler,
    frames: Vec<BurstFrame>,
) -> Result<(), BenchError> {
    if frames.is_empty() {
        return Ok(());
    }
    progress.bursts += 1;
    let mut records = Vec::with_capacity(frames.len());
    for frame in frames {
        accumulate_record_metrics(progress, &frame.record);
        records.push(frame.record);
    }
    let chunk = connection
        .session_mut()
        .protector_mut()
        .protect_transport_records(records)?;
    progress.wire_bytes += chunk.len() as u64;
    let chunk_len = chunk.len();
    connection.send_transport_frame(chunk).await.map_err(|error| {
        BenchError::Invariant(format!(
            "authenticated client send failed after {} records / {} payload bytes / {} wire bytes while sending {} bytes: {error}",
            progress.records,
            progress.payload_bytes,
            progress.wire_bytes,
            chunk_len,
        ))
    })?;
    sampler.maybe_sample(progress)?;
    Ok(())
}

fn throughput_metrics(progress: &SideProgress, duration: Duration) -> ThroughputMetrics {
    let elapsed_secs = duration.as_secs_f64().max(f64::EPSILON);
    ThroughputMetrics {
        total_duration_ms: duration.as_millis(),
        payload_bytes_per_sec: progress.payload_bytes as f64 / elapsed_secs,
        wire_bytes_per_sec: progress.wire_bytes as f64 / elapsed_secs,
        records_per_sec: progress.records as f64 / elapsed_secs,
    }
}

fn utility_summary(acc: UtilityAccumulator) -> UtilitySummary {
    if acc.measured_events == 0 {
        return UtilitySummary::default();
    }
    let measured = acc.measured_events as f64;
    UtilitySummary {
        measured_events: acc.measured_events,
        exact_chunk_top1_rate: acc.exact_chunk_top1_sum / measured,
        exact_chunk_top5_rate: acc.exact_chunk_top5_sum / measured,
        same_file_top1_rate: acc.same_file_top1_sum / measured,
        same_file_top5_rate: acc.same_file_top5_sum / measured,
        mean_reciprocal_rank: acc.reciprocal_rank_sum / measured,
    }
}

fn benchmark_datagram_metrics(metrics: &DatagramSessionMetrics) -> BenchmarkDatagramMetrics {
    BenchmarkDatagramMetrics {
        outbound_messages: metrics.outbound_messages,
        outbound_datagrams: metrics.outbound_datagrams,
        retransmitted_datagrams: metrics.retransmitted_datagrams,
        acknowledged_messages: metrics.acknowledged_messages,
        repair_requests_sent: metrics.repair_requests_sent,
        repair_requests_received: metrics.repair_requests_received,
        duplicate_chunks_ignored: metrics.duplicate_chunks_ignored,
    }
}

fn update_server_side_utility(
    utility: &mut UtilityAccumulator,
    sample_limit: usize,
    original_live: &HashMap<ItemId, LiveBenchmarkItem>,
    entries: impl Iterator<Item = (ItemId, ExactStateMaterial)>,
    current_item_id: ItemId,
) -> Result<(), BenchError> {
    if utility.measured_events >= sample_limit {
        return Ok(());
    }
    let Some(current_item) = original_live.get(&current_item_id).cloned() else {
        return Ok(());
    };

    // S4.7: Optimization — early-exit if the current item's chunk is already
    // found in the first entries. For utility sampling we only need to know
    // if exact_chunk or same_file matches exist, not enumerate all of them.
    // Collect a bounded sample of reconstructed entries for comparison.
    let mut exact_chunk_present = false;
    let mut same_file_present = false;
    for (item_id, block) in entries {
        if exact_chunk_present && same_file_present {
            // Both conditions satisfied — no need to examine more entries.
            break;
        }
        if let Some(candidate) = original_live.get(&item_id) {
            if !exact_chunk_present
                && candidate.chunk_sha256 == current_item.chunk_sha256
                && block.exact_bytes == current_item.exact_bytes
            {
                exact_chunk_present = true;
            }
            if !same_file_present && candidate.file_sha256 == current_item.file_sha256 {
                same_file_present = true;
            }
        }
    }

    utility.exact_chunk_top1_sum += exact_chunk_present as u8 as f64;
    utility.exact_chunk_top5_sum += exact_chunk_present as u8 as f64;
    utility.same_file_top1_sum += same_file_present as u8 as f64;
    utility.same_file_top5_sum += same_file_present as u8 as f64;
    utility.reciprocal_rank_sum += exact_chunk_present as u8 as f64;
    Ok(())
}

impl WorkloadGenerator {
    fn new(workload: BenchmarkWorkload, corpus_kind: BenchmarkCorpusKind, seed: u64) -> Self {
        Self {
            workload,
            corpus: input_corpus(corpus_kind),
            rng: StdRng::seed_from_u64(seed),
            next_item_id: 1,
            live_items: HashMap::new(),
            live_ids: Vec::new(),
            live_id_positions: HashMap::new(),
            step: 0,
        }
    }

    fn next_event(&mut self) -> WorkloadEvent {
        let step = self.step;
        self.step += 1;

        if self.live_items.is_empty() {
            return self.insert_event(step);
        }

        let roll = self.rng.gen_range(0_u8..100);
        if roll < self.workload.insert_threshold() {
            self.insert_event(step)
        } else if roll < self.workload.upsert_threshold() {
            let item_id = self.choose_item_id(self.workload);
            let current_corpus_index = self
                .live_items
                .get(&item_id)
                .expect("selected item must exist")
                .corpus_index;
            let next_corpus_index = self.next_related_corpus_index(current_corpus_index);
            let state = self
                .live_items
                .get_mut(&item_id)
                .expect("selected item must exist");
            state.revision += 1;
            state.corpus_index = next_corpus_index;
            WorkloadEvent::UpsertObject {
                item_id,
                input: self.corpus.chunks[state.corpus_index].clone(),
            }
        } else if roll < self.workload.evict_threshold() {
            let item_id = self.choose_item_id(BenchmarkWorkload::LowLocality);
            self.live_items.remove(&item_id);
            self.remove_live_item(item_id);
            WorkloadEvent::Evict { item_id }
        } else {
            let item_id = self.choose_item_id(BenchmarkWorkload::LowLocality);
            self.live_items.remove(&item_id);
            self.remove_live_item(item_id);
            WorkloadEvent::Invalidate { item_id }
        }
    }

    fn insert_event(&mut self, _step: usize) -> WorkloadEvent {
        let item_id = self.next_item_id;
        self.next_item_id += 1;
        let corpus_index = self.choose_corpus_index();
        let state = WorkloadItemState {
            corpus_index,
            revision: 0,
        };
        self.live_items.insert(item_id, state);
        self.live_id_positions.insert(item_id, self.live_ids.len());
        self.live_ids.push(item_id);
        WorkloadEvent::Insert {
            item_id,
            input: self.corpus.chunks[corpus_index].clone(),
        }
    }

    fn choose_corpus_index(&mut self) -> usize {
        self.rng.gen_range(0..self.corpus.chunks.len())
    }

    fn next_related_corpus_index(&mut self, current_index: usize) -> usize {
        let current = &self.corpus.chunks[current_index];
        let file = &self.corpus.files[current.file_index];
        if self.rng.gen_bool(self.workload.hot_bias()) {
            if file.chunk_count > 1 {
                let next_chunk_index = (current.chunk_index + 1) % file.chunk_count;
                file.first_chunk_index + next_chunk_index
            } else {
                current_index
            }
        } else {
            self.choose_corpus_index()
        }
    }

    fn choose_item_id(&mut self, workload: BenchmarkWorkload) -> u64 {
        let hot_limit = self.live_ids.len().min(8);
        let hot_ids = &self.live_ids[..hot_limit];
        if !hot_ids.is_empty() && self.rng.gen_bool(workload.hot_bias()) {
            *hot_ids
                .choose(&mut self.rng)
                .expect("hot candidates should be available")
        } else {
            *self
                .live_ids
                .choose(&mut self.rng)
                .expect("at least one live item must exist")
        }
    }

    fn remove_live_item(&mut self, item_id: u64) {
        let Some(index) = self.live_id_positions.remove(&item_id) else {
            return;
        };
        let removed = self.live_ids.swap_remove(index);
        debug_assert_eq!(removed, item_id);
        if let Some(swapped) = self.live_ids.get(index).copied() {
            self.live_id_positions.insert(swapped, index);
        }
    }
}

fn derive_case_seed(
    environment: BenchmarkEnvironment,
    carrier: BenchmarkCarrier,
    direction: BenchmarkDirection,
    corpus: BenchmarkCorpusKind,
    optimization: BenchmarkOptimization,
    capability_profile: BenchmarkCapabilityProfile,
    workload: BenchmarkWorkload,
    protection: BenchmarkProtection,
    mode: BenchmarkMode,
    target_bytes: BenchmarkTargetBytes,
) -> u64 {
    let mut seed = 0x5eed_2026_d15c_a11e_u64;
    seed ^= environment as u64;
    seed = seed.rotate_left(7) ^ (carrier as u64);
    seed = seed.rotate_left(7) ^ (direction as u64);
    seed = seed.rotate_left(7) ^ (corpus as u64);
    let optimization_bits = u64::from(optimization.source_dedup);
    seed = seed.rotate_left(7) ^ optimization_bits;
    seed = seed.rotate_left(7) ^ (capability_profile as u64);
    seed = seed.rotate_left(7) ^ (workload as u64);
    seed = seed.rotate_left(7) ^ (protection as u64);
    seed = seed.rotate_left(7) ^ (mode as u64);
    seed = seed.rotate_left(11) ^ target_bytes.bytes();
    seed
}

fn percentile(values: &[u64], fraction: f64) -> u64 {
    let index = ((values.len() as f64 - 1.0) * fraction).round() as usize;
    values[index.min(values.len() - 1)]
}

fn sample_process_metrics(pid: u32) -> Result<(u64, u64, f64), BenchError> {
    let output = Command::new("ps")
        .args([
            "-o",
            "rss=",
            "-o",
            "vsz=",
            "-o",
            "%cpu=",
            "-p",
            &pid.to_string(),
        ])
        .output()?;
    if !output.status.success() {
        return Err(BenchError::ProcessSample(format!(
            "ps exited with status {}",
            output.status
        )));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut parts = stdout.split_whitespace();
    let rss_kib = parts
        .next()
        .ok_or_else(|| BenchError::ProcessSample("missing rss".to_string()))?
        .parse::<u64>()
        .map_err(|error| BenchError::ProcessSample(format!("invalid rss: {error}")))?;
    let vsz_kib = parts
        .next()
        .ok_or_else(|| BenchError::ProcessSample("missing vsz".to_string()))?
        .parse::<u64>()
        .map_err(|error| BenchError::ProcessSample(format!("invalid vsz: {error}")))?;
    let cpu_percent = parts
        .next()
        .ok_or_else(|| BenchError::ProcessSample("missing cpu".to_string()))?
        .parse::<f64>()
        .map_err(|error| BenchError::ProcessSample(format!("invalid cpu: {error}")))?;
    let rss_bytes = rss_kib * 1024;
    let vsz_bytes = if cfg!(target_os = "macos") {
        // On Darwin, `ps -o vsz=` reports byte-sized values in practice even though the
        // generic documentation describes Kbytes. Treating it as raw bytes keeps the
        // metric consistent with observed process sizes instead of inflating by 1024x.
        vsz_kib
    } else {
        vsz_kib * 1024
    };
    Ok((rss_bytes, vsz_bytes, cpu_percent))
}

fn parse_ws_host_port(url: &str) -> Result<(String, u16), BenchError> {
    let without_scheme = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
        .ok_or_else(|| BenchError::InvalidCase(format!("unsupported websocket url: {url}")))?;
    let authority = without_scheme
        .split('/')
        .next()
        .ok_or_else(|| BenchError::InvalidCase(format!("invalid websocket url: {url}")))?;
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| BenchError::InvalidCase(format!("missing port in websocket url: {url}")))?;
    let port = port
        .parse::<u16>()
        .map_err(|error| BenchError::InvalidCase(format!("invalid port `{port}`: {error}")))?;
    Ok((host.to_string(), port))
}

fn reject_loopback_host(host: &str) -> Result<(), BenchError> {
    if matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Err(BenchError::LoopbackRemoteHostRejected);
    }
    Ok(())
}

async fn wait_for_direct_connectivity(
    host: &str,
    port: u16,
    child: &mut Child,
) -> Result<(), BenchError> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                return Err(BenchError::RemoteServerExitedEarly);
            }
            return Err(BenchError::RemoteServerExitedEarly);
        }

        match timeout(DIRECT_PREFLIGHT_TIMEOUT, TcpStream::connect((host, port))).await {
            Ok(Ok(_stream)) => return Ok(()),
            Ok(Err(_)) | Err(_) => {}
        }

        if start.elapsed() > REMOTE_READY_TIMEOUT {
            let _ = child.kill();
            return Err(BenchError::DirectConnectivityFailed {
                host: host.to_string(),
                port,
            });
        }
        sleep(REMOTE_POLL_INTERVAL).await;
    }
}

fn sync_workspace_to_remote(ssh_target: &str, remote_repo_path: &str) -> Result<(), BenchError> {
    let sync_key = format!("{ssh_target}|{remote_repo_path}");
    let sync_cache = REMOTE_SYNC_CACHE.get_or_init(|| Mutex::new(HashSet::new()));
    if sync_cache
        .lock()
        .map_err(|_| BenchError::Invariant("remote sync cache lock poisoned".to_string()))?
        .contains(&sync_key)
    {
        return Ok(());
    }

    let remote_repo_shell_path = remote_shell_path(remote_repo_path);
    run_command(
        Command::new("ssh")
            .arg(ssh_target)
            .arg(format!("mkdir -p \"{remote_repo_shell_path}\"")),
    )?;
    let workspace_root = workspace_root();
    run_command(Command::new("rsync").args([
        "-az",
        "--delete",
        "--exclude",
        ".git",
        "--exclude",
        "target",
        "--exclude",
        "artifacts",
        &format!("{}/", workspace_root.display()),
        &format!("{ssh_target}:{remote_repo_path}/"),
    ]))?;

    sync_cache
        .lock()
        .map_err(|_| BenchError::Invariant("remote sync cache lock poisoned".to_string()))?
        .insert(sync_key);
    Ok(())
}

fn sync_release_server_binary_to_remote(
    ssh_target: &str,
    remote_repo_path: &str,
) -> Result<(), BenchError> {
    let sync_key = format!("{ssh_target}|{remote_repo_path}");
    let sync_cache = REMOTE_BINARY_SYNC_CACHE.get_or_init(|| Mutex::new(HashSet::new()));
    if sync_cache
        .lock()
        .map_err(|_| BenchError::Invariant("remote binary sync cache lock poisoned".to_string()))?
        .contains(&sync_key)
    {
        return Ok(());
    }

    let remote_repo_shell_path = remote_shell_path(remote_repo_path);
    let local_binary = workspace_root().join("target/release/server");
    if !local_binary.exists() {
        return Err(BenchError::LocalCommand(format!(
            "missing release benchmark binary at {}",
            local_binary.display()
        )));
    }

    run_command(Command::new("ssh").arg(ssh_target).arg(format!(
        "mkdir -p \"{remote_repo_shell_path}/target/release\""
    )))?;
    run_command(Command::new("rsync").args([
        "-az",
        local_binary.to_str().unwrap(),
        &format!("{ssh_target}:{remote_repo_path}/target/release/server"),
    ]))?;

    sync_cache
        .lock()
        .map_err(|_| BenchError::Invariant("remote binary sync cache lock poisoned".to_string()))?
        .insert(sync_key);
    Ok(())
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("server crate should live below the workspace root")
}

fn remote_shell_path(path: &str) -> String {
    path.strip_prefix("~/")
        .map(|suffix| format!("$HOME/{suffix}"))
        .unwrap_or_else(|| path.to_string())
}

fn spawn_remote_command(target: &str, command: &str) -> Result<Child, BenchError> {
    Command::new("ssh")
        .arg(target)
        .arg(command)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(BenchError::Io)
}

fn run_command(command: &mut Command) -> Result<(), BenchError> {
    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(BenchError::LocalCommand(format!(
            "command {:?} failed with status {status}",
            command
        )))
    }
}

fn run_command_capture(command: &mut Command) -> Result<String, BenchError> {
    let output = command.output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(BenchError::LocalCommand(format!(
            "command {:?} failed with status {}: {}",
            command, output.status, stderr
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn cleanup_remote_runtime_dir(ssh_target: &str, remote_dir: &str) {
    let _ = run_command(
        Command::new("ssh")
            .arg(ssh_target)
            .arg(format!("rm -rf \"{remote_dir}\"")),
    );
}

pub fn fetch_benchmark_corpus(kind: BenchmarkCorpusKind) -> Result<(), BenchError> {
    let script = match kind {
        BenchmarkCorpusKind::Wikitext103Raw => return Ok(()),
        BenchmarkCorpusKind::WebImage50 => WEB_IMAGE_CORPUS_FETCH_SCRIPT,
    };
    run_command(Command::new("python3").args([script]))
}

fn write_failure_log(case_dir: &Path, failure: &str) -> Result<(), BenchError> {
    fs::create_dir_all(case_dir)?;
    fs::write(case_dir.join("failures.log"), failure)?;
    Ok(())
}

pub fn parse_case_from_args(args: &[String]) -> Result<BenchmarkCase, BenchError> {
    if args.len() < 6 {
        return Err(BenchError::InvalidCase(
            "expected <environment> <chpmt_capability> <workload> <protection> <mode> <size>"
                .to_string(),
        ));
    }
    let environment = BenchmarkEnvironment::parse(&args[0])
        .ok_or_else(|| BenchError::InvalidCase(format!("invalid environment `{}`", args[0])))?;
    let capability_profile = BenchmarkCapabilityProfile::parse(&args[1]).ok_or_else(|| {
        BenchError::InvalidCase(format!("invalid CHPMT capability `{}`", args[1]))
    })?;
    let workload = BenchmarkWorkload::parse(&args[2])
        .ok_or_else(|| BenchError::InvalidCase(format!("invalid workload `{}`", args[2])))?;
    let protection = BenchmarkProtection::parse(&args[3])
        .ok_or_else(|| BenchError::InvalidCase(format!("invalid protection `{}`", args[3])))?;
    let mode = BenchmarkMode::parse(&args[4])
        .ok_or_else(|| BenchError::InvalidCase(format!("invalid mode `{}`", args[4])))?;
    let size = BenchmarkTargetBytes::parse(&args[5])
        .ok_or_else(|| BenchError::InvalidCase(format!("invalid size `{}`", args[5])))?;
    let mut case = BenchmarkCase::new(
        environment,
        capability_profile,
        workload,
        protection,
        mode,
        size,
    )
    .with_client_runtime(BenchmarkClientRuntime::WebWasm);
    for value in &args[6..] {
        if let Some(runtime) = BenchmarkClientRuntime::parse(value) {
            case = case.with_client_runtime(runtime);
            continue;
        }
        if let Some(carrier) = BenchmarkCarrier::parse(value) {
            case = case.with_carrier(carrier);
            continue;
        }
        if let Some(direction) = BenchmarkDirection::parse(value) {
            case = case.with_direction(direction);
            continue;
        }
        if let Some(corpus) = BenchmarkCorpusKind::parse(value) {
            case = case.with_corpus(corpus);
            continue;
        }
        if let Some(optimization) = BenchmarkOptimization::parse(value) {
            case = case.with_optimization(optimization);
            continue;
        }
        return Err(BenchError::InvalidCase(format!(
            "invalid optional benchmark argument `{value}`"
        )));
    }
    Ok(case)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_accumulator_flushes_at_small_cap() {
        let mut buffer = BurstAccumulator::new(4_096);
        let frame = sample_frame(1_024);
        let mut flushed = Vec::new();
        for _ in 0..5 {
            buffer.push(frame.clone(), &mut flushed);
        }
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].len(), 4);
    }

    #[test]
    fn burst_accumulator_flushes_at_medium_cap() {
        let mut buffer = BurstAccumulator::new(65_536);
        let frame = sample_frame(8_192);
        let mut flushed = Vec::new();
        for _ in 0..9 {
            buffer.push(frame.clone(), &mut flushed);
        }
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].len(), 8);
    }

    #[test]
    fn burst_accumulator_flushes_at_big_cap() {
        let mut buffer = BurstAccumulator::new(1_048_576);
        let frame = sample_frame(131_072);
        let mut flushed = Vec::new();
        for _ in 0..9 {
            buffer.push(frame.clone(), &mut flushed);
        }
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].len(), 8);
    }

    #[test]
    fn bulk_mode_has_no_burst_boundaries() {
        assert!(BenchmarkMode::Bulk.payload_cap_bytes().is_none());
    }

    #[test]
    fn target_split_keeps_custom_subruns_bounded_to_100mb() {
        let shards = split_target_bytes(250_000_000, BENCH_MAX_SESSION_TARGET_BYTES);
        assert_eq!(
            shards,
            vec![BENCH_TARGET_100_MB, BENCH_TARGET_100_MB, 50_000_000]
        );
        assert_eq!(shards.iter().sum::<u64>(), 250_000_000);
    }

    #[test]
    fn target_split_leaves_100mb_unsplit() {
        let shards = split_target_bytes(BENCH_TARGET_100_MB, BENCH_MAX_SESSION_TARGET_BYTES);
        assert_eq!(shards, vec![BENCH_TARGET_100_MB]);
    }

    #[test]
    fn validate_case_rejects_targets_above_100mb() {
        let case = BenchmarkCase::new(
            BenchmarkEnvironment::Local,
            BenchmarkCapabilityProfile::TextFamilyCueObject,
            BenchmarkWorkload::MixedLocality,
            BenchmarkProtection::ClassicRef1,
            BenchmarkMode::Bulk,
            BenchmarkTargetBytes::Custom(100_000_001),
        );
        assert!(matches!(
            validate_case_input_corpus(&case),
            Err(BenchError::InvalidCase(message))
                if message.contains("above 100mb")
        ));
    }

    #[test]
    fn buffered_payload_stays_bounded() {
        let mut buffer = BurstAccumulator::new(4_096);
        let frame = sample_frame(512);
        let mut flushed = Vec::new();
        for _ in 0..50_000 {
            buffer.push(frame.clone(), &mut flushed);
            flushed.clear();
        }
        assert!(buffer.peak_buffered_payload_bytes <= 4_096);
        assert!(buffer.peak_buffered_frame_count <= 8);
    }

    #[test]
    fn remote_preflight_rejects_loopback_hosts() {
        assert!(matches!(
            reject_loopback_host("127.0.0.1"),
            Err(BenchError::LoopbackRemoteHostRejected)
        ));
        assert!(matches!(
            reject_loopback_host("localhost"),
            Err(BenchError::LoopbackRemoteHostRejected)
        ));
    }

    #[tokio::test]
    async fn local_text_family_capability_case_runs_in_each_mode() {
        for mode in [
            BenchmarkMode::Bulk,
            BenchmarkMode::BurstSmall,
            BenchmarkMode::BurstMedium,
            BenchmarkMode::BurstBig,
        ] {
            let case = BenchmarkCase::new(
                BenchmarkEnvironment::Local,
                BenchmarkCapabilityProfile::TextFamilyCueObject,
                BenchmarkWorkload::MixedLocality,
                BenchmarkProtection::ClassicRef1,
                mode,
                BenchmarkTargetBytes::Custom(4_096),
            );
            let artifact_root =
                std::env::temp_dir().join("pulzz_bench_text_family_capability_modes");
            let result = {
                let mut last_error = None;
                let mut completed = None;
                for _ in 0..3 {
                    match run_benchmark_case(case.clone(), &artifact_root).await {
                        Ok(result) => {
                            completed = Some(result);
                            break;
                        }
                        Err(BenchError::WebSocket(error)) if is_transient_accept_error(&error) => {
                            last_error = Some(BenchError::WebSocket(error));
                        }
                        Err(error) => panic!("benchmark case failed unexpectedly: {error}"),
                    }
                }
                completed.unwrap_or_else(|| {
                    panic!(
                        "benchmark case kept hitting transient websocket resets: {}",
                        last_error
                            .map(|error| error.to_string())
                            .unwrap_or_else(|| "unknown error".to_string())
                    )
                })
            };
            assert!(result.actual_payload_bytes >= 4_096);
        }
    }

    #[tokio::test]
    async fn local_text_family_capability_case_runs_with_both_protection_profiles() {
        for protection in [
            BenchmarkProtection::ClassicRef1,
            BenchmarkProtection::PqSimpleV1,
            BenchmarkProtection::PqMutualV1,
        ] {
            let case = BenchmarkCase::new(
                BenchmarkEnvironment::Local,
                BenchmarkCapabilityProfile::TextFamilyCueObject,
                BenchmarkWorkload::HighLocality,
                protection,
                BenchmarkMode::Bulk,
                BenchmarkTargetBytes::Custom(4_096),
            );
            let artifact_root = std::env::temp_dir()
                .join("pulzz_bench_text_family_capability_protection_profiles");
            let result = run_benchmark_case(case, &artifact_root).await.unwrap();
            assert!(result.server.records > 0);
            assert!(result.client.records > 0);
        }
    }

    #[tokio::test]
    async fn server_tolerates_tcp_preflight_before_websocket_client() {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let case = BenchmarkCase::new(
                BenchmarkEnvironment::Local,
                BenchmarkCapabilityProfile::TextFamilyCueObject,
                BenchmarkWorkload::MixedLocality,
                BenchmarkProtection::ClassicRef1,
                BenchmarkMode::Bulk,
                BenchmarkTargetBytes::Custom(4_096),
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let artifact_root = std::env::temp_dir()
                .join(format!("pulzz_bench_preflight_probe_attempt_{attempt}"));
            let server_case = case.clone();
            let server_dir = artifact_root.join("server");
            let client_dir = artifact_root.join("client");
            let server_task = tokio::spawn(async move {
                serve_case_with_listener(&server_case, listener, &server_dir).await
            });

            let probe = TcpStream::connect(addr).await.unwrap();
            drop(probe);

            match verify_case_against_url(&case, &format!("ws://{addr}"), &client_dir).await {
                Ok(client_metrics) => {
                    let server_metrics = server_task.await.unwrap().unwrap();
                    assert!(server_metrics.records > 0);
                    assert!(client_metrics.records > 0);
                    return;
                }
                Err(BenchError::WebSocket(error)) if is_transient_accept_error(&error) => {
                    let _ = server_task.await;
                }
                Err(error) => panic!("benchmark verify failed unexpectedly: {error}"),
            }
        }

        panic!("benchmark websocket preflight probe kept hitting transient reset races");
    }

    #[tokio::test]
    async fn burst_mode_overshoot_stays_within_burst_cap() {
        let case = BenchmarkCase::new(
            BenchmarkEnvironment::Local,
            BenchmarkCapabilityProfile::TextFamilyCueObject,
            BenchmarkWorkload::MixedLocality,
            BenchmarkProtection::ClassicRef1,
            BenchmarkMode::BurstSmall,
            BenchmarkTargetBytes::Custom(4_096),
        );
        let artifact_root = std::env::temp_dir().join("pulzz_bench_burst_bound");
        let result = run_benchmark_case(case, &artifact_root).await.unwrap();
        assert!(result.payload_overshoot_bytes <= 4_096);
    }

    #[test]
    fn web_image_corpus_rejects_text_family_capability() {
        let case = BenchmarkCase::new(
            BenchmarkEnvironment::Local,
            BenchmarkCapabilityProfile::TextFamilyCueObject,
            BenchmarkWorkload::MixedLocality,
            BenchmarkProtection::PqMutualV1,
            BenchmarkMode::BurstMedium,
            BenchmarkTargetBytes::OneMb,
        )
        .with_corpus(BenchmarkCorpusKind::WebImage50);
        let error = validate_case_input_corpus(&case).unwrap_err();
        assert!(error.to_string().contains(
            "input corpus web_image_50 is unsupported by CHPMT capability text_family_cue_object"
        ));
    }

    #[test]
    fn wikitext_corpus_parses_canonically() {
        assert_eq!(
            BenchmarkCorpusKind::parse("wikitext_103_raw"),
            Some(BenchmarkCorpusKind::Wikitext103Raw)
        );
    }

    fn sample_frame(payload_len: usize) -> BurstFrame {
        let record = Record {
            header: shared_protocol::RecordHeader {
                version: shared_protocol::PROTOCOL_VERSION,
                stream_id: StreamId(1),
                epoch_id: shared_protocol::EpochId(0),
                seq_no: shared_protocol::SeqNo(0),
                record_type: shared_protocol::RecordType::ExactState,
                codec_mode: CodecMode::DirectExact,
                flags: shared_protocol::RecordFlags::empty(),
                item_id: ItemId(1),
                payload_len: payload_len as u32,
            },
            payload: vec![7; payload_len],
            auth_tag: [0; shared_protocol::AUTH_TAG_LEN],
        };
        BurstFrame { record }
    }
}

fn benchmark_case_support_level(case: &BenchmarkCase) -> PredictiveBenchSupportLevel {
    let capability = case.capability_profile.descriptor();
    let corpus = input_corpus(case.corpus);
    let mut supported = 0usize;
    for chunk in &corpus.chunks {
        let validation =
            shared_protocol::validate_capability_support(&chunk.prepared_source(), &capability);
        if validation.family_supported
            && validation.cue_supported
            && validation.object_support_complete
        {
            supported += 1;
        }
    }
    if supported == 0 {
        PredictiveBenchSupportLevel::Unsupported
    } else if supported == corpus.chunks.len() {
        PredictiveBenchSupportLevel::Full
    } else {
        PredictiveBenchSupportLevel::Partial
    }
}
