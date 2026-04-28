use std::{fs, path::Path};

#[cfg(test)]
use client::ClientSession;
use client::{
    ClientConnectConfig, ClientConnectError, ClientState, ReconnectPolicy,
    connect_websocket_session,
};
use futures_util::StreamExt;
use rand::{SeedableRng, rngs::StdRng};
use serde::Serialize;
use shared_protocol::{
    BOOTSTRAP_SIGNING_SEED_LEN, BootstrapConfig, BootstrapServerConfig, ClientSecurityConfig,
    CodecMode, CredentialScope, EpochId, ExactStateMaterial, ItemId, Record, RecordFlags,
    RecordType, SeqNo, ServerSecurityConfig, StreamDirection, StreamId, TransportSessionConfig,
    issue_client_credential, issue_server_identity, pq_simple_v1_pair_from_rng,
};
use thiserror::Error;
use tokio_tungstenite::tungstenite::{self, Message};

use crate::{PlainServerSession, ServerEvent, ServerSession, transport};

pub const DEFAULT_SCENARIO_ADDR: &str = "0.0.0.0:9010";
pub const DEFAULT_SCENARIO_URL: &str = "ws://127.0.0.1:9010";
pub const DISTRIBUTED_STREAM_ID: StreamId = StreamId(0x9010);
const TEST_ONLY_SERVER_SIGNING_SEED_LABEL: &[u8] = b"scenario/test_only/server_signing_seed";
const TEST_ONLY_CLIENT_SIGNING_SEED_LABEL: &[u8] = b"scenario/test_only/client_signing_seed";
const TEST_ONLY_SERVER_ID: &str = "scenario-test-server";
const TEST_ONLY_CLIENT_ID: &str = "scenario-test-client";
const TEST_ONLY_ISSUED_AT_UNIX_SECS: u64 = 1_700_000_000;
const TEST_ONLY_EXPIRES_AT_UNIX_SECS: u64 = 2_000_000_000;
const FULL_SCENARIO_RECORD_COUNT: usize = 7;

fn is_runtime_data_or_predictive_route(
    record: &Record,
    allow_delta: bool,
    require_non_delta_if_exact: bool,
) -> bool {
    match record.header.record_type {
        RecordType::ExactState => {
            if require_non_delta_if_exact {
                matches!(
                    record.header.codec_mode,
                    CodecMode::PackedExact | CodecMode::DirectExact
                )
            } else if allow_delta {
                matches!(
                    record.header.codec_mode,
                    CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
                )
            } else {
                matches!(
                    record.header.codec_mode,
                    CodecMode::PackedExact | CodecMode::DirectExact
                )
            }
        }
        RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
            record.header.codec_mode == CodecMode::None
        }
        _ => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioKind {
    Full,
}

impl ScenarioKind {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Full => "full",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "full" => Some(Self::Full),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationReport {
    pub scenario: ScenarioKind,
    pub url: String,
    pub repetitions: usize,
    pub runs: Vec<ScenarioRunReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioRunReport {
    pub run_index: usize,
    pub record_count: usize,
    pub saw_predicted_exact: bool,
    pub saw_rekey: bool,
    pub saw_resync: bool,
    pub saw_close: bool,
    pub first_record_non_delta: bool,
    pub post_rekey_epoch_confirmed: bool,
    pub post_resync_first_vector_non_delta: bool,
    pub final_cache_len: usize,
    pub final_predictor_len: usize,
}

#[derive(Debug, Error)]
pub enum ScenarioError {
    #[error(transparent)]
    Server(#[from] crate::ServerError),
    #[error(transparent)]
    Client(#[from] client::ClientApplyError),
    #[error(transparent)]
    ClientConnect(#[from] ClientConnectError),
    #[error(transparent)]
    Transport(#[from] transport::TransportError),
    #[error(transparent)]
    Bootstrap(#[from] shared_protocol::BootstrapError),
    #[error(transparent)]
    Protection(#[from] shared_protocol::ProtectionError),
    #[error(transparent)]
    Wire(#[from] shared_protocol::WireError),
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("scenario invariant failed: {0}")]
    Invariant(String),
}

pub fn build_records(kind: ScenarioKind) -> Result<Vec<Record>, ScenarioError> {
    match kind {
        ScenarioKind::Full => build_full_records(),
    }
}

pub async fn verify_url(
    kind: ScenarioKind,
    url: &str,
    repetitions: usize,
) -> Result<VerificationReport, ScenarioError> {
    let mut runs = Vec::with_capacity(repetitions);
    for run_index in 0..repetitions {
        runs.push(verify_authenticated_session(kind, run_index, url).await?);
    }

    Ok(VerificationReport {
        scenario: kind,
        url: url.to_string(),
        repetitions,
        runs,
    })
}

pub fn write_report(
    report: &VerificationReport,
    output_path: impl AsRef<Path>,
) -> Result<(), ScenarioError> {
    let output_path = output_path.as_ref();
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, serde_json::to_vec_pretty(report)?)?;
    Ok(())
}

pub fn render_report(report: &VerificationReport) -> String {
    let mut out = String::new();
    out.push_str("# Distributed Verification Report\n\n");
    out.push_str(&format!(
        "- scenario: `{}`\n- url: `{}`\n- repetitions: `{}`\n\n",
        report.scenario.slug(),
        report.url,
        report.repetitions,
    ));
    for run in &report.runs {
        out.push_str(&format!("## run {}\n\n", run.run_index + 1));
        out.push_str(&format!(
            "- records: `{}`\n- predicted exact observed: `{}`\n- rekey observed: `{}`\n- resync observed: `{}`\n- close observed: `{}`\n- first record non-delta: `{}`\n- post-rekey epoch confirmed: `{}`\n- post-resync first vector non-delta: `{}`\n- final cache entries: `{}`\n- final predictor entries: `{}`\n\n",
            run.record_count,
            run.saw_predicted_exact,
            run.saw_rekey,
            run.saw_resync,
            run.saw_close,
            run.first_record_non_delta,
            run.post_rekey_epoch_confirmed,
            run.post_resync_first_vector_non_delta,
            run.final_cache_len,
            run.final_predictor_len,
        ));
    }
    out
}

pub async fn serve_at(
    kind: ScenarioKind,
    addr: &str,
    connections: usize,
) -> Result<(), ScenarioError> {
    let records = build_plain_records(kind)?;
    transport::serve_session_at(
        addr,
        records,
        connections,
        test_only_server_transport_config(StreamDirection::ServerToClient)?,
    )
    .await?;
    Ok(())
}

fn distributed_pair() -> (
    shared_protocol::StreamProtector,
    shared_protocol::StreamProtector,
) {
    let mut rng = StdRng::seed_from_u64(0x9010_2026_0406_0001);
    pq_simple_v1_pair_from_rng(
        DISTRIBUTED_STREAM_ID,
        StreamDirection::ServerToClient,
        &mut rng,
    )
}

fn build_full_records() -> Result<Vec<Record>, ScenarioError> {
    let (sender, _) = distributed_pair();
    let mut session = ServerSession::new(sender);
    let (raw_block, delta_block) = find_delta_pair()?;
    let mut records = Vec::with_capacity(FULL_SCENARIO_RECORD_COUNT);

    records.push(session.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: raw_block.clone(),
    })?);

    let delta_record = session.emit_event(ServerEvent::UpsertObject {
        item_id: ItemId(1),
        block: delta_block,
    })?;
    if !is_runtime_data_or_predictive_route(&delta_record, true, false) {
        return Err(ScenarioError::Invariant(
            "full scenario expected a runtime data or predictive route update".to_string(),
        ));
    }
    records.push(delta_record);

    let mut rng = StdRng::seed_from_u64(0x5151_2026);
    let (rekey_record, _) = session.emit_rekey(&mut rng)?;
    records.push(rekey_record);

    records.push(session.emit_event(ServerEvent::Insert {
        item_id: ItemId(2),
        block: sample_block(0.021),
    })?);

    records.push(session.emit_resync()?);

    let final_raw = session.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: sample_block(0.033),
    })?;
    if !is_runtime_data_or_predictive_route(&final_raw, false, true) {
        return Err(ScenarioError::Invariant(
            "post-RESYNC update should be a non-delta exact-state record or predictive route"
                .to_string(),
        ));
    }
    records.push(final_raw);
    records.push(session.emit_close(Some(1000))?);

    Ok(records)
}

fn build_plain_records(kind: ScenarioKind) -> Result<Vec<Record>, ScenarioError> {
    match kind {
        ScenarioKind::Full => build_full_plain_records(),
    }
}

fn build_full_plain_records() -> Result<Vec<Record>, ScenarioError> {
    let mut session = PlainServerSession::new(DISTRIBUTED_STREAM_ID);
    let (raw_block, delta_block) = find_delta_pair()?;
    let mut records = Vec::with_capacity(FULL_SCENARIO_RECORD_COUNT);

    records.push(session.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: raw_block.clone(),
    })?);

    let delta_record = session.emit_event(ServerEvent::UpsertObject {
        item_id: ItemId(1),
        block: delta_block,
    })?;
    if !matches!(
        delta_record.header.codec_mode,
        CodecMode::PredictedExact | CodecMode::PackedExact | CodecMode::DirectExact
    ) {
        return Err(ScenarioError::Invariant(
            "full scenario expected an exact-state DATA update".to_string(),
        ));
    }
    records.push(delta_record);

    let mut rng = StdRng::seed_from_u64(0x5151_2026);
    let (rekey_record, _) = session.emit_rekey(&mut rng)?;
    records.push(rekey_record);

    records.push(session.emit_event(ServerEvent::Insert {
        item_id: ItemId(2),
        block: sample_block(0.021),
    })?);

    records.push(session.emit_resync()?);

    let final_raw = session.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: sample_block(0.033),
    })?;
    if final_raw.header.codec_mode != CodecMode::PackedExact
        && final_raw.header.codec_mode != CodecMode::DirectExact
    {
        return Err(ScenarioError::Invariant(
            "post-RESYNC vector should fall back to RAW_Q8".to_string(),
        ));
    }
    records.push(final_raw);
    records.push(session.emit_close(Some(1000))?);

    Ok(records)
}

#[cfg(test)]
fn verify_records(
    kind: ScenarioKind,
    run_index: usize,
    records: &[Record],
) -> Result<ScenarioRunReport, ScenarioError> {
    match kind {
        ScenarioKind::Full => verify_full_records(run_index, records),
    }
}

async fn verify_authenticated_session(
    kind: ScenarioKind,
    run_index: usize,
    url: &str,
) -> Result<ScenarioRunReport, ScenarioError> {
    let mut connected = connect_websocket_session(&test_only_client_connect_config(url)?).await?;
    let mut plaintext_records = Vec::new();
    let mut state = ClientState::default();

    'outer: while let Some(message) = connected.websocket_mut().next().await {
        match message? {
            Message::Binary(frame) => {
                for protected in shared_protocol::decode_transport_records(&frame)? {
                    let is_close = protected.header.record_type == RecordType::Close;
                    let record = connected
                        .session_mut()
                        .protector_mut()
                        .unprotect_record(protected)?;
                    state.apply_record(record.clone())?;
                    plaintext_records.push(record);
                    if is_close {
                        break 'outer;
                    }
                }
            }
            Message::Close(_) => break,
            Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {}
        }
    }

    connected.close().await?;
    summarize_records(
        kind,
        run_index,
        &plaintext_records,
        state.cache_len(),
        state.predictor_len(),
    )
}

#[cfg(test)]
fn verify_full_records(
    run_index: usize,
    records: &[Record],
) -> Result<ScenarioRunReport, ScenarioError> {
    if records.len() != FULL_SCENARIO_RECORD_COUNT {
        return Err(ScenarioError::Invariant(format!(
            "expected {FULL_SCENARIO_RECORD_COUNT} records, got {}",
            records.len()
        )));
    }

    let (_, receiver) = distributed_pair();
    let mut client = ClientSession::new(receiver);

    let first = &records[0];
    let second = &records[1];
    let third = &records[2];
    let fourth = &records[3];
    let fifth = &records[4];
    let sixth = &records[5];
    let seventh = &records[6];

    if !is_runtime_data_or_predictive_route(first, false, true) {
        return Err(ScenarioError::Invariant(
            "first record should be a non-delta exact-state record or predictive route".to_string(),
        ));
    }
    if !is_runtime_data_or_predictive_route(second, true, false) {
        return Err(ScenarioError::Invariant(
            "second record should be a runtime data or predictive route update".to_string(),
        ));
    }
    if third.header.record_type != RecordType::Rekey {
        return Err(ScenarioError::Invariant(
            "third record should be REKEY".to_string(),
        ));
    }
    if fourth.header.epoch_id != EpochId(1)
        || fourth.header.seq_no != SeqNo(0)
        || !fourth
            .header
            .flags
            .contains(RecordFlags::POST_REKEY_CONFIRM)
    {
        return Err(ScenarioError::Invariant(
            "fourth record should confirm epoch 1 with POST_REKEY_CONFIRM".to_string(),
        ));
    }
    if fifth.header.record_type != RecordType::Resync {
        return Err(ScenarioError::Invariant(
            "fifth record should be RESYNC".to_string(),
        ));
    }
    if !is_runtime_data_or_predictive_route(sixth, false, true) {
        return Err(ScenarioError::Invariant(
            "post-RESYNC update should be a non-delta exact-state record or predictive route"
                .to_string(),
        ));
    }
    if seventh.header.record_type != RecordType::Close {
        return Err(ScenarioError::Invariant(
            "seventh record should be CLOSE".to_string(),
        ));
    }

    for record in records.iter().cloned() {
        client.apply_protected_record(record)?;
    }

    summarize_full_records(
        run_index,
        records,
        client.state().cache_len(),
        client.state().predictor_len(),
    )
}

fn summarize_records(
    kind: ScenarioKind,
    run_index: usize,
    records: &[Record],
    final_cache_len: usize,
    final_predictor_len: usize,
) -> Result<ScenarioRunReport, ScenarioError> {
    match kind {
        ScenarioKind::Full => {
            summarize_full_records(run_index, records, final_cache_len, final_predictor_len)
        }
    }
}

fn summarize_full_records(
    run_index: usize,
    records: &[Record],
    final_cache_len: usize,
    final_predictor_len: usize,
) -> Result<ScenarioRunReport, ScenarioError> {
    let saw_predicted_exact = records
        .iter()
        .any(|record| record.header.codec_mode == CodecMode::PredictedExact);
    let saw_rekey = records
        .iter()
        .any(|record| record.header.record_type == RecordType::Rekey);
    let saw_resync = records
        .iter()
        .any(|record| record.header.record_type == RecordType::Resync);
    let saw_close = records
        .iter()
        .any(|record| record.header.record_type == RecordType::Close);
    let first_record_non_delta = records
        .first()
        .map(|record| {
            matches!(
                record.header.codec_mode,
                CodecMode::PackedExact | CodecMode::DirectExact
            )
        })
        .unwrap_or(false);
    let post_rekey_epoch_confirmed = records.iter().any(|record| {
        record.header.epoch_id == EpochId(1)
            && record.header.seq_no == SeqNo(0)
            && record
                .header
                .flags
                .contains(RecordFlags::POST_REKEY_CONFIRM)
    });
    let post_resync_first_vector_non_delta = records
        .iter()
        .skip_while(|record| record.header.record_type != RecordType::Resync)
        .skip(1)
        .find(|record| record.header.record_type == RecordType::ExactState)
        .map(|record| {
            matches!(
                record.header.codec_mode,
                CodecMode::PackedExact | CodecMode::DirectExact
            )
        })
        .unwrap_or(false);

    Ok(ScenarioRunReport {
        run_index,
        record_count: records.len(),
        saw_predicted_exact,
        saw_rekey,
        saw_resync,
        saw_close,
        first_record_non_delta,
        post_rekey_epoch_confirmed,
        post_resync_first_vector_non_delta,
        final_cache_len,
        final_predictor_len,
    })
}

fn test_only_client_connect_config(url: &str) -> Result<ClientConnectConfig, ScenarioError> {
    let session = TransportSessionConfig::default();
    let server_signing_seed = derive_test_only_signing_seed(TEST_ONLY_SERVER_SIGNING_SEED_LABEL);
    let client_signing_seed = derive_test_only_signing_seed(TEST_ONLY_CLIENT_SIGNING_SEED_LABEL);
    let server_identity = issue_server_identity(
        TEST_ONLY_SERVER_ID,
        TEST_ONLY_ISSUED_AT_UNIX_SECS,
        TEST_ONLY_EXPIRES_AT_UNIX_SECS,
        server_signing_seed,
    )?;
    let issued_credential = issue_client_credential(
        &server_identity,
        server_signing_seed,
        TEST_ONLY_CLIENT_ID,
        client_signing_seed,
        CredentialScope {
            stream_id: Some(DISTRIBUTED_STREAM_ID),
            allow_client_to_server: true,
            allow_server_to_client: true,
        },
        TEST_ONLY_ISSUED_AT_UNIX_SECS,
        TEST_ONLY_EXPIRES_AT_UNIX_SECS,
    )?;

    Ok(ClientConnectConfig {
        url: url.to_string(),
        stream_id: DISTRIBUTED_STREAM_ID,
        direction: StreamDirection::ServerToClient,
        session,
        security: ClientSecurityConfig::PqMutual {
            issued_credential,
            expected_server_identity: server_identity,
        },
        reconnect_policy: ReconnectPolicy::disabled(),
    })
}

fn test_only_server_transport_config(
    direction: StreamDirection,
) -> Result<transport::TransportServerConfig, ScenarioError> {
    let session = TransportSessionConfig::default();
    let server_signing_seed = derive_test_only_signing_seed(TEST_ONLY_SERVER_SIGNING_SEED_LABEL);
    let server_identity = issue_server_identity(
        TEST_ONLY_SERVER_ID,
        TEST_ONLY_ISSUED_AT_UNIX_SECS,
        TEST_ONLY_EXPIRES_AT_UNIX_SECS,
        server_signing_seed,
    )?;

    Ok(transport::TransportServerConfig {
        session,
        connection_limits: transport::ConnectionLimits::default(),
        bootstrap_policy: transport::BootstrapPolicy::new(BootstrapServerConfig {
            stream_id: DISTRIBUTED_STREAM_ID,
            direction,
            bootstrap: BootstrapConfig::default(),
            security: ServerSecurityConfig::PqMutual {
                server_identity,
                server_signing_seed,
                revoked_client_ids: Vec::new(),
            },
        }),
        carrier: shared_protocol::carrier::reliable::ReliableCarrierKind::WebSocket,
    })
}

fn derive_test_only_signing_seed(label: &[u8]) -> [u8; BOOTSTRAP_SIGNING_SEED_LEN] {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    hash.update(b"pulzz/scenario/test_only");
    hash.update(label);
    hash.update(DISTRIBUTED_STREAM_ID.0.to_le_bytes());
    let digest = hash.finalize();
    let mut seed = [0_u8; BOOTSTRAP_SIGNING_SEED_LEN];
    seed.copy_from_slice(&digest[..BOOTSTRAP_SIGNING_SEED_LEN]);
    seed
}

fn find_delta_pair() -> Result<(ExactStateMaterial, ExactStateMaterial), ScenarioError> {
    for len in [256_usize, 512, 1024, 4096] {
        let baseline = patterned_block(len, 0x31);
        for mutation_stride in [usize::MAX, 1024, 257, 113, 53, 17] {
            let candidate = mutate_patterned_block(&baseline, mutation_stride);
            if candidate.exact_bytes != baseline.exact_bytes {
                return Ok((baseline, candidate));
            }
        }
    }

    let baseline = sample_block(0.011);
    let candidate = sample_block(0.0111);
    if candidate.exact_bytes != baseline.exact_bytes {
        return Ok((baseline, candidate));
    }

    Err(ScenarioError::Invariant(
        "failed to construct a distinct update pair for the full scenario".to_string(),
    ))
}

fn patterned_block(len: usize, salt: u8) -> ExactStateMaterial {
    const PHRASE: &[u8] = br#"{"kind":"text","value":"the quick brown fox jumps over the lazy dog ","flag":true,"count":42}"#;
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        for (index, byte) in PHRASE.iter().enumerate() {
            if bytes.len() == len {
                break;
            }
            bytes.push(byte.wrapping_add(((index as u8) ^ salt) & 0x03));
        }
    }
    ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, &bytes)
}

fn mutate_patterned_block(
    baseline: &ExactStateMaterial,
    mutation_stride: usize,
) -> ExactStateMaterial {
    let mut bytes = baseline.exact_bytes.clone();
    if mutation_stride == usize::MAX {
        if let Some(last) = bytes.last_mut() {
            *last = last.wrapping_add(1);
        }
    } else {
        for index in (0..bytes.len()).step_by(mutation_stride.max(1)) {
            bytes[index] = bytes[index].wrapping_add(((index % 7) as u8) + 1);
        }
    }
    ExactStateMaterial::copy_exact(baseline.source_kind, &bytes)
}

fn sample_block(seed: f32) -> ExactStateMaterial {
    let mut bytes = Vec::with_capacity(256);
    for index in 0..256 {
        let value = (((index as f32 + 1.0) * seed).cos() * 127.0).round() as i16;
        bytes.push((value & 0xff) as u8);
    }
    ExactStateMaterial::copy_exact(shared_protocol::SourceKind::Text, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_scenario_is_built_with_expected_structure() {
        let records = build_records(ScenarioKind::Full).unwrap();
        assert_eq!(records.len(), FULL_SCENARIO_RECORD_COUNT);
        assert!(is_runtime_data_or_predictive_route(
            &records[0],
            false,
            true
        ));
        assert!(is_runtime_data_or_predictive_route(
            &records[1],
            true,
            false
        ));
        assert_eq!(records[2].header.record_type, RecordType::Rekey);
        assert_eq!(records[4].header.record_type, RecordType::Resync);
        assert!(is_runtime_data_or_predictive_route(
            &records[5],
            false,
            true
        ));
        assert_eq!(records[6].header.record_type, RecordType::Close);
    }

    #[test]
    fn full_scenario_verifier_accepts_local_records() {
        let records = build_records(ScenarioKind::Full).unwrap();
        let report = verify_records(ScenarioKind::Full, 0, &records).unwrap();
        assert!(report.saw_rekey);
        assert!(report.saw_resync);
        assert!(report.first_record_non_delta);
        assert!(report.post_rekey_epoch_confirmed);
        assert!(report.post_resync_first_vector_non_delta);
    }
}
