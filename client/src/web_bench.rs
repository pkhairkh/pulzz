use std::{cell::RefCell, rc::Rc};

use futures_channel::{mpsc, oneshot};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use shared_protocol::{
    BOOTSTRAP_KEM_SEED_LEN, BOOTSTRAP_NONCE_LEN, BootstrapClientConfig, BootstrapMessage,
    ClientBootstrapState, CodecMode, ProtectionProfileKind, Record, RecordFlags, RecordType,
    ResidualCodingMode, STREAM_ROOT_LEN, SourceKind, StreamDirection, StreamId, StreamProtector,
    TransportSessionConfig, decode_transport_records, inspect_data_payload,
};
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};
use wasm_bindgen_futures::JsFuture;
use web_sys::{BinaryType, CloseEvent, Crypto, Event, MessageEvent, Response, WebSocket};

use crate::ClientSession;

const LATENCY_SAMPLE_CAP: usize = 8_192;
const PROGRESS_UPDATE_RECORD_INTERVAL: u64 = 64;
const WEB_BENCH_FRAMES_URL: &str = "/web_bench_frames.bin";
const MAX_WEB_BENCH_BUFFERED_SEND_BYTES: u32 = 4 * 1024 * 1024;
const DRAIN_PROGRESS_POLL_INTERVAL: u32 = 32;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WebBenchRole {
    Receiver,
    Sender,
}

#[derive(Debug, Deserialize)]
struct WebBenchCase {
    role: WebBenchRole,
    #[serde(rename = "session_config")]
    _session_config: Option<TransportSessionConfig>,
    bootstrap_client_config: Option<BootstrapClientConfig>,
    protection_kind: Option<ProtectionProfileKind>,
    stream_id: Option<StreamId>,
    receiver_bootstrap_root: Option<[u8; STREAM_ROOT_LEN]>,
    yield_on_group_boundary: bool,
}

#[derive(Debug, Serialize, Default, Clone)]
struct WebBenchCodecModeTotals {
    direct_exact: u64,
    packed_exact: u64,
    predicted_exact: u64,
    control: u64,
}

#[derive(Debug, Serialize, Default, Clone)]
struct WebBenchSourceKindTotals {
    text: u64,
    json: u64,
    binary: u64,
    unknown: u64,
}

#[derive(Debug, Serialize, Default, Clone)]
struct WebBenchResidualModeTotals {
    none: u64,
    all_zero: u64,
    small_signed_rans: u64,
    sparse_positions: u64,
    literal_raw: u64,
    unknown: u64,
}

/// S4.1.d: Same byte category taxonomy as server-side bench.rs PayloadByteCategories.
/// Web bench uses this local mirror because PayloadByteCategories is in server crate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WebBenchByteCategories {
    exact_state_payload_bytes: u64,
    predictive_dispatch_payload_bytes: u64,
    inline_definition_bytes: u64,
    control_data_payload_bytes: u64,
    episode_hint_payload_bytes: u64,
    /// S4.1.c: Logical content bytes — original byte length of all transmitted items.
    logical_content_bytes: u64,
    total_wire_bytes: u64,
    overhead_bytes: u64,
}

#[derive(Debug, Serialize)]
struct WebBenchResult {
    records: u64,
    #[serde(alias = "vector_records")]
    cue_object_records: u64,
    predictive_records: u64,
    original_payload_bytes: u64,
    payload_bytes: u64,
    wire_bytes: u64,
    total_duration_ms: u64,
    codec_modes: WebBenchCodecModeTotals,
    source_kinds: WebBenchSourceKindTotals,
    residual_modes: WebBenchResidualModeTotals,
    apply_latency: Option<WebBenchLatencySummary>,
    /// S4.1.d: Same byte category taxonomy as server-side bench.rs PayloadByteCategories.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    byte_categories: Option<WebBenchByteCategories>,
}

#[derive(Debug, Serialize, Default)]
struct WebBenchProgressSnapshot {
    elapsed_ms: u64,
    records: u64,
    #[serde(alias = "vector_records")]
    cue_object_records: u64,
    predictive_records: u64,
    original_payload_bytes: u64,
    payload_bytes: u64,
    wire_bytes: u64,
}

#[derive(Debug, Serialize)]
struct WebBenchLatencySummary {
    samples: usize,
    min_ns: u64,
    avg_ns: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
}

#[derive(Debug)]
enum WebBenchSocketEvent {
    Binary(Vec<u8>),
    Closed,
    Error(String),
}

struct WebBenchSocketHandle {
    websocket: WebSocket,
    event_rx: mpsc::UnboundedReceiver<WebBenchSocketEvent>,
    _onopen: Closure<dyn FnMut(Event)>,
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
    _onclose: Closure<dyn FnMut(CloseEvent)>,
    _onerror: Closure<dyn FnMut(Event)>,
}

#[derive(Debug, Default)]
struct WebBenchProgress {
    records: u64,
    cue_object_records: u64,
    predictive_records: u64,
    original_payload_bytes: u64,
    payload_bytes: u64,
    wire_bytes: u64,
    codec_modes: WebBenchCodecModeTotals,
    source_kinds: WebBenchSourceKindTotals,
    residual_modes: WebBenchResidualModeTotals,
    /// S4.1.d: Byte category tracker, same taxonomy as server-side PayloadByteCategories.
    byte_categories: WebBenchByteCategories,
}

#[derive(Debug, Default)]
struct WebBenchLatencySampler {
    seen: u64,
    values: Vec<u64>,
}

impl WebBenchLatencySampler {
    fn record(&mut self, value: u64) {
        self.seen += 1;
        if self.values.len() < LATENCY_SAMPLE_CAP {
            self.values.push(value);
            return;
        }

        let replace_index = ((self.seen.wrapping_mul(1_103_515_245).wrapping_add(12_345))
            % LATENCY_SAMPLE_CAP as u64) as usize;
        self.values[replace_index] = value;
    }

    fn summary(&self) -> Option<WebBenchLatencySummary> {
        if self.values.is_empty() {
            return None;
        }

        let mut sorted = self.values.clone();
        sorted.sort_unstable();
        let count = sorted.len();
        let sum = sorted.iter().copied().sum::<u64>();
        Some(WebBenchLatencySummary {
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

#[wasm_bindgen]
pub async fn run_web_bench_benchmark(case_json: String, ws_url: String) -> Result<String, JsValue> {
    console_error_panic_hook::set_once();
    let case: WebBenchCase = serde_json::from_str(&case_json)
        .map_err(|error| JsValue::from_str(&format!("invalid web bench case: {error}")))?;

    let result = match case.role {
        WebBenchRole::Receiver => run_web_bench_receiver(case, ws_url).await?,
        WebBenchRole::Sender => run_web_bench_sender(case, ws_url).await?,
    };

    serde_json::to_string(&result).map_err(|error| {
        JsValue::from_str(&format!("failed to serialize web bench result: {error}"))
    })
}

async fn run_web_bench_receiver(
    case: WebBenchCase,
    ws_url: String,
) -> Result<WebBenchResult, JsValue> {
    let (mut websocket_handle, session) = connect_web_bench_session(&case, &ws_url).await?;
    let mut session =
        session.ok_or_else(|| JsValue::from_str("receiver bootstrap did not produce a session"))?;
    let mut progress = WebBenchProgress::default();
    let mut apply_latency = WebBenchLatencySampler::default();

    let start_ms = now_ms();
    publish_progress(&progress, start_ms)?;

    while let Some(event) = websocket_handle.event_rx.next().await {
        match event {
            WebBenchSocketEvent::Binary(frame) => {
                progress.wire_bytes += frame.len() as u64;
                for record in decode_web_bench_transport_records(&frame)? {
                    let is_close = matches!(record.header.record_type, RecordType::Close);
                    accumulate_record_metrics(&mut progress, &record);
                    let apply_start_ns = now_ns();
                    session.apply_protected_record(record).map_err(|error| {
                        JsValue::from_str(&format!("client apply failed: {error}"))
                    })?;
                    apply_latency.record(now_ns().saturating_sub(apply_start_ns));

                    if is_close {
                        publish_progress(&progress, start_ms)?;
                        return Ok(WebBenchResult {
                            records: progress.records,
                            cue_object_records: progress.cue_object_records,
                            predictive_records: progress.predictive_records,
                            original_payload_bytes: progress.original_payload_bytes,
                            payload_bytes: progress.payload_bytes,
                            wire_bytes: progress.wire_bytes,
                            total_duration_ms: (now_ms() - start_ms).round() as u64,
                            codec_modes: progress.codec_modes,
                            source_kinds: progress.source_kinds,
                            residual_modes: progress.residual_modes,
                            apply_latency: apply_latency.summary(),
                        });
                    }
                }

                if progress.records % PROGRESS_UPDATE_RECORD_INTERVAL == 0 {
                    publish_progress(&progress, start_ms)?;
                }
            }
            WebBenchSocketEvent::Closed => break,
            WebBenchSocketEvent::Error(message) => return Err(JsValue::from_str(&message)),
        }
    }

    publish_progress(&progress, start_ms)?;
    Ok(WebBenchResult {
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        total_duration_ms: (now_ms() - start_ms).round() as u64,
        codec_modes: progress.codec_modes,
        source_kinds: progress.source_kinds,
        residual_modes: progress.residual_modes,
        apply_latency: apply_latency.summary(),
    })
}

async fn run_web_bench_sender(
    case: WebBenchCase,
    ws_url: String,
) -> Result<WebBenchResult, JsValue> {
    let (mut websocket_handle, mut session) = connect_web_bench_session(&case, &ws_url).await?;
    let mut progress = WebBenchProgress::default();
    let start_ms = now_ms();
    publish_progress(&progress, start_ms)?;

    let response_value = JsFuture::from(
        web_sys::window()
            .ok_or_else(|| JsValue::from_str("window is unavailable"))?
            .fetch_with_str(WEB_BENCH_FRAMES_URL),
    )
    .await?;
    let response: Response = response_value
        .dyn_into()
        .map_err(|_| JsValue::from_str("failed to cast web bench frame response"))?;
    if !response.ok() {
        return Err(JsValue::from_str(&format!(
            "failed to fetch web bench frames: HTTP {}",
            response.status()
        )));
    }
    let body = response
        .body()
        .ok_or_else(|| JsValue::from_str("web bench frame response has no body"))?;
    let reader = body
        .get_reader()
        .dyn_into::<web_sys::ReadableStreamDefaultReader>()
        .map_err(|_| JsValue::from_str("failed to open web bench frame reader"))?;

    let mut pending = Vec::<u8>::new();
    let mut outbound_chunk = Vec::<u8>::new();
    let mut cursor = 0_usize;
    loop {
        let chunk = JsFuture::from(reader.read()).await?;
        let done = js_sys::Reflect::get(&chunk, &JsValue::from_str("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            break;
        }

        let value = js_sys::Reflect::get(&chunk, &JsValue::from_str("value"))?;
        let bytes = js_sys::Uint8Array::new(&value).to_vec();
        pending.extend_from_slice(&bytes);

        loop {
            if pending.len().saturating_sub(cursor) < 4 {
                break;
            }
            let frame_len = u32::from_le_bytes(pending[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            if frame_len == 0 {
                if !outbound_chunk.is_empty() {
                    websocket_handle
                        .websocket
                        .send_with_u8_array(&outbound_chunk)?;
                    progress.wire_bytes += outbound_chunk.len() as u64;
                    wait_for_websocket_drain(
                        &websocket_handle.websocket,
                        MAX_WEB_BENCH_BUFFERED_SEND_BYTES,
                        &progress,
                        start_ms,
                    )
                    .await?;
                    outbound_chunk.clear();
                }
                if case.yield_on_group_boundary {
                    yield_web_bench_turn().await?;
                }
                continue;
            }

            let frame_len = frame_len as usize;
            if pending.len().saturating_sub(cursor) < frame_len {
                cursor -= 4;
                break;
            }

            let frame = pending[cursor..cursor + frame_len].to_vec();
            cursor += frame_len;
            let record = Record::from_bytes(&frame)
                .map_err(|error| JsValue::from_str(&format!("wire decode failed: {error}")))?;
            accumulate_record_metrics(&mut progress, &record);
            let protected = if case.bootstrap_client_config.is_some() {
                let session = session.as_mut().ok_or_else(|| {
                    JsValue::from_str("sender bootstrap did not produce a session")
                })?;
                session.protect_record(record).map_err(|error| {
                    JsValue::from_str(&format!("client protect failed: {error}"))
                })?
            } else {
                record
            };
            let protected_bytes = protected.to_bytes();
            outbound_chunk.extend_from_slice(&(protected_bytes.len() as u32).to_le_bytes());
            outbound_chunk.extend_from_slice(&protected_bytes);

            if progress.records % PROGRESS_UPDATE_RECORD_INTERVAL == 0 {
                publish_progress(&progress, start_ms)?;
            }
        }
        compact_pending(&mut pending, &mut cursor);
    }

    if pending.len().saturating_sub(cursor) != 0 {
        return Err(JsValue::from_str(
            "web bench sender frame stream ended with a truncated record",
        ));
    }
    if !outbound_chunk.is_empty() {
        websocket_handle
            .websocket
            .send_with_u8_array(&outbound_chunk)?;
        progress.wire_bytes += outbound_chunk.len() as u64;
        wait_for_websocket_drain(
            &websocket_handle.websocket,
            MAX_WEB_BENCH_BUFFERED_SEND_BYTES,
            &progress,
            start_ms,
        )
        .await?;
    }

    wait_for_websocket_drain(&websocket_handle.websocket, 0, &progress, start_ms).await?;
    websocket_handle.websocket.close()?;
    while let Some(event) = websocket_handle.event_rx.next().await {
        match event {
            WebBenchSocketEvent::Closed => break,
            WebBenchSocketEvent::Error(message) => return Err(JsValue::from_str(&message)),
            WebBenchSocketEvent::Binary(_) => {}
        }
    }

    publish_progress(&progress, start_ms)?;
    Ok(WebBenchResult {
        records: progress.records,
        cue_object_records: progress.cue_object_records,
        predictive_records: progress.predictive_records,
        original_payload_bytes: progress.original_payload_bytes,
        payload_bytes: progress.payload_bytes,
        wire_bytes: progress.wire_bytes,
        total_duration_ms: (now_ms() - start_ms).round() as u64,
        codec_modes: progress.codec_modes,
        source_kinds: progress.source_kinds,
        residual_modes: progress.residual_modes,
        apply_latency: None,
    })
}

async fn connect_web_bench_session(
    case: &WebBenchCase,
    ws_url: &str,
) -> Result<(WebBenchSocketHandle, Option<ClientSession>), JsValue> {
    let mut websocket_handle = connect_web_bench_socket(ws_url).await?;
    let protector = if let Some(bootstrap_client_config) = case.bootstrap_client_config.clone() {
        let mut client_nonce = [0_u8; BOOTSTRAP_NONCE_LEN];
        let mut client_kem_seed = [0_u8; BOOTSTRAP_KEM_SEED_LEN];
        web_bench_random_bytes(&mut client_nonce)?;
        web_bench_random_bytes(&mut client_kem_seed)?;
        let (mut bootstrap_state, client_hello) = ClientBootstrapState::start(
            bootstrap_client_config.clone(),
            client_nonce,
            client_kem_seed,
        )
        .map_err(|error| JsValue::from_str(&format!("bootstrap start failed: {error}")))?;
        websocket_handle.websocket.send_with_u8_array(
            &client_hello
                .to_frame(&bootstrap_client_config.bootstrap)
                .map_err(|error| JsValue::from_str(&format!("bootstrap encode failed: {error}")))?,
        )?;
        let server_hello =
            receive_bootstrap_message(&mut websocket_handle, &bootstrap_client_config.bootstrap)
                .await?;
        let progress = bootstrap_state
            .handle_server_hello(server_hello, unix_time_secs())
            .map_err(|error| {
                JsValue::from_str(&format!("bootstrap server hello failed: {error}"))
            })?;
        let completed = if let Some(outbound) = progress.outbound {
            websocket_handle.websocket.send_with_u8_array(
                &outbound
                    .to_frame(&bootstrap_client_config.bootstrap)
                    .map_err(|error| {
                        JsValue::from_str(&format!("bootstrap encode failed: {error}"))
                    })?,
            )?;
            let server_finish = receive_bootstrap_message(
                &mut websocket_handle,
                &bootstrap_client_config.bootstrap,
            )
            .await?;
            bootstrap_state
                .handle_server_finish(server_finish)
                .map_err(|error| {
                    JsValue::from_str(&format!("bootstrap server finish failed: {error}"))
                })?
        } else {
            progress.completed.ok_or_else(|| {
                JsValue::from_str("bootstrap completed without a completion payload")
            })?
        };
        Some(StreamProtector::from_bootstrap_root(
            completed.protection_profile,
            completed.stream_id,
            completed.direction,
            completed.root,
        ))
    } else {
        match case.role {
            WebBenchRole::Receiver => Some(StreamProtector::from_bootstrap_root(
                case.protection_kind
                    .ok_or_else(|| JsValue::from_str("receiver case is missing protection_kind"))?,
                case.stream_id
                    .ok_or_else(|| JsValue::from_str("receiver case is missing stream_id"))?,
                StreamDirection::ServerToClient,
                case.receiver_bootstrap_root
                    .ok_or_else(|| JsValue::from_str("receiver case is missing bootstrap root"))?,
            )),
            WebBenchRole::Sender => None,
        }
    };
    Ok((websocket_handle, protector.map(ClientSession::new)))
}

async fn connect_web_bench_socket(ws_url: &str) -> Result<WebBenchSocketHandle, JsValue> {
    let websocket = WebSocket::new(ws_url)?;
    websocket.set_binary_type(BinaryType::Arraybuffer);

    let (open_tx, open_rx) = oneshot::channel::<()>();
    let open_tx = Rc::new(RefCell::new(Some(open_tx)));
    let (event_tx, event_rx) = mpsc::unbounded::<WebBenchSocketEvent>();

    let onopen = {
        let open_tx = Rc::clone(&open_tx);
        Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_event: Event| {
            if let Some(tx) = open_tx.borrow_mut().take() {
                let _ = tx.send(());
            }
        }))
    };
    websocket.set_onopen(Some(onopen.as_ref().unchecked_ref()));

    let onmessage = {
        let event_tx = event_tx.clone();
        Closure::<dyn FnMut(MessageEvent)>::wrap(Box::new(move |event: MessageEvent| {
            if let Ok(buffer) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes = js_sys::Uint8Array::new(&buffer).to_vec();
                let _ = event_tx.unbounded_send(WebBenchSocketEvent::Binary(bytes));
                return;
            }

            let _ = event_tx.unbounded_send(WebBenchSocketEvent::Error(
                "received non-binary websocket frame during web benchmark".to_string(),
            ));
        }))
    };
    websocket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

    let onclose = {
        let event_tx = event_tx.clone();
        Closure::<dyn FnMut(CloseEvent)>::wrap(Box::new(move |_event: CloseEvent| {
            let _ = event_tx.unbounded_send(WebBenchSocketEvent::Closed);
        }))
    };
    websocket.set_onclose(Some(onclose.as_ref().unchecked_ref()));

    let onerror = {
        let event_tx = event_tx.clone();
        Closure::<dyn FnMut(Event)>::wrap(Box::new(move |_event: Event| {
            let _ = event_tx.unbounded_send(WebBenchSocketEvent::Error("websocket error".into()));
        }))
    };
    websocket.set_onerror(Some(onerror.as_ref().unchecked_ref()));

    open_rx
        .await
        .map_err(|_| JsValue::from_str("websocket open event was dropped before connect"))?;

    Ok(WebBenchSocketHandle {
        websocket,
        event_rx,
        _onopen: onopen,
        _onmessage: onmessage,
        _onclose: onclose,
        _onerror: onerror,
    })
}

async fn receive_bootstrap_message(
    websocket_handle: &mut WebBenchSocketHandle,
    config: &shared_protocol::BootstrapConfig,
) -> Result<BootstrapMessage, JsValue> {
    while let Some(event) = websocket_handle.event_rx.next().await {
        match event {
            WebBenchSocketEvent::Binary(frame) => {
                return BootstrapMessage::from_frame(&frame, config).map_err(|error| {
                    JsValue::from_str(&format!("bootstrap decode failed: {error}"))
                });
            }
            WebBenchSocketEvent::Closed => {
                return Err(JsValue::from_str(
                    "websocket closed during bootstrap before a full handshake completed",
                ));
            }
            WebBenchSocketEvent::Error(message) => return Err(JsValue::from_str(&message)),
        }
    }
    Err(JsValue::from_str(
        "websocket ended during bootstrap before a full handshake completed",
    ))
}

async fn yield_web_bench_turn() -> Result<(), JsValue> {
    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        if let Some(window) = web_sys::window() {
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, 0);
        } else {
            let _ = resolve.call0(&JsValue::NULL);
        }
    });
    JsFuture::from(promise).await.map(|_| ())
}

async fn wait_for_websocket_drain(
    websocket: &WebSocket,
    target_buffered_bytes: u32,
    progress: &WebBenchProgress,
    start_ms: f64,
) -> Result<(), JsValue> {
    let mut polls = 0_u32;
    while websocket.buffered_amount() > target_buffered_bytes {
        if polls % DRAIN_PROGRESS_POLL_INTERVAL == 0 {
            publish_progress(progress, start_ms)?;
        }
        polls += 1;
        yield_web_bench_turn().await?;
    }
    Ok(())
}

fn compact_pending(pending: &mut Vec<u8>, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    if *cursor >= pending.len() {
        pending.clear();
        *cursor = 0;
        return;
    }

    pending.drain(..*cursor);
    *cursor = 0;
}

fn publish_progress(progress: &WebBenchProgress, start_ms: f64) -> Result<(), JsValue> {
    set_window_json(
        "__pulzzRuntimeProgress",
        &WebBenchProgressSnapshot {
            elapsed_ms: (now_ms() - start_ms).round() as u64,
            records: progress.records,
            cue_object_records: progress.cue_object_records,
            original_payload_bytes: progress.original_payload_bytes,
            payload_bytes: progress.payload_bytes,
            wire_bytes: progress.wire_bytes,
        },
    )
}

fn set_window_json(name: &str, value: &impl Serialize) -> Result<(), JsValue> {
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("window is unavailable"))?;
    let json = serde_json::to_string(value)
        .map_err(|error| JsValue::from_str(&format!("failed to encode window payload: {error}")))?;
    let js_value = js_sys::JSON::parse(&json)?;
    js_sys::Reflect::set(window.as_ref(), &JsValue::from_str(name), &js_value)?;
    Ok(())
}

fn now_ms() -> f64 {
    web_sys::window()
        .and_then(|window| window.performance())
        .map(|performance| performance.now())
        .unwrap_or_else(js_sys::Date::now)
}

fn now_ns() -> u64 {
    (now_ms() * 1_000_000.0) as u64
}

fn unix_time_secs() -> u64 {
    (js_sys::Date::now() / 1_000.0).round() as u64
}

fn web_bench_random_bytes<const N: usize>(output: &mut [u8; N]) -> Result<(), JsValue> {
    let crypto = web_bench_crypto()?;
    crypto.get_random_values_with_u8_array(output)?;
    Ok(())
}

fn web_bench_crypto() -> Result<Crypto, JsValue> {
    web_sys::window()
        .ok_or_else(|| JsValue::from_str("window is unavailable"))?
        .crypto()
        .map_err(JsValue::from)
}

fn decode_web_bench_transport_records(frame: &[u8]) -> Result<Vec<Record>, JsValue> {
    if let Ok(records) = decode_transport_records(frame) {
        return Ok(records);
    }

    if frame.len() < 4 {
        return Err(JsValue::from_str(
            "wire decode failed: frame was neither a canonical packed transport frame nor a length-prefixed record group",
        ));
    }

    let mut cursor = 0_usize;
    let mut records = Vec::new();
    while cursor < frame.len() {
        if frame.len().saturating_sub(cursor) < 4 {
            return Err(JsValue::from_str(
                "wire decode failed: frame ended mid length prefix",
            ));
        }

        let record_len = u32::from_le_bytes(frame[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        if record_len == 0 {
            return Err(JsValue::from_str(
                "wire decode failed: unexpected zero-length record prefix",
            ));
        }
        if frame.len().saturating_sub(cursor) < record_len {
            return Err(JsValue::from_str(&format!(
                "wire decode failed: declared record length {record_len} exceeds remaining frame bytes {}",
                frame.len().saturating_sub(cursor),
            )));
        }

        let record = Record::from_bytes(&frame[cursor..cursor + record_len])
            .map_err(|error| JsValue::from_str(&format!("wire decode failed: {error}")))?;
        records.push(record);
        cursor += record_len;
    }

    if records.is_empty() {
        return Err(JsValue::from_str(
            "wire decode failed: empty length-prefixed record group",
        ));
    }

    Ok(records)
}

fn accumulate_record_metrics(progress: &mut WebBenchProgress, record: &Record) {
    progress.records += 1;
    progress.original_payload_bytes += benchmark_original_payload_len(record);
    progress.payload_bytes += benchmark_payload_len(record);
    // S4.1.d: Classify payload bytes into category counters.
    classify_web_bench_bytes(&mut progress.byte_categories, record);
    if is_vector_record(record) {
        progress.cue_object_records += 1;
        progress.predictive_records += 1;
        classify_source_kind(&mut progress.source_kinds, record);
        classify_residual_mode(&mut progress.residual_modes, record);
    }
    classify_record_mode(&mut progress.codec_modes, record.header.codec_mode);
}

fn classify_record_mode(totals: &mut WebBenchCodecModeTotals, mode: CodecMode) {
    match mode {
        CodecMode::DirectExact => totals.direct_exact += 1,
        CodecMode::PackedExact => totals.packed_exact += 1,
        CodecMode::PredictedExact => totals.predicted_exact += 1,
        CodecMode::None => totals.control += 1,
    }
}

fn classify_source_kind(totals: &mut WebBenchSourceKindTotals, record: &Record) {
    let Some(tag) = record.payload.first().copied() else {
        totals.unknown += 1;
        return;
    };
    match SourceKind::from_tag(tag) {
        Some(SourceKind::Text) => totals.text += 1,
        Some(SourceKind::Json) => totals.json += 1,
        Some(SourceKind::Binary) => totals.binary += 1,
        None => totals.unknown += 1,
    }
}

/// S4.1.d: Classify payload bytes into category counters, mirroring the
/// server-side classify_payload_bytes function in bench.rs.
fn classify_web_bench_bytes(categories: &mut WebBenchByteCategories, record: &Record) {
    let payload_len = record.payload.len() as u64;
    let wire_len = record.to_bytes().len() as u64;
    match record.header.record_type {
        RecordType::ExactState => {
            categories.exact_state_payload_bytes += payload_len;
            // S4.1.c: For ExactState, logical content = inspected original length.
            categories.logical_content_bytes += shared_protocol::inspect_data_payload(
                &record.payload, record.header.codec_mode)
                .map(|i| i.original_len as u64)
                .unwrap_or(payload_len);
        }
        RecordType::PredictiveConfirm | RecordType::PredictiveCorrect => {
            categories.predictive_dispatch_payload_bytes += payload_len;
            // S4.1.c/I4: Attempt to extract route_graph.output_len from
            // deserialized predictive payload for honest logical content bytes.
            let logical_len = shared_protocol::PredictiveRouteDispatchPayload::decode(&record.payload)
                .map(|payload| {
                    let route_output = payload.route_graph.output_len as u64;
                    if route_output > 0 { route_output } else { payload_len }
                })
                .unwrap_or(payload_len);
            categories.logical_content_bytes += logical_len;
        }
        RecordType::TransformCorrect => {
            categories.predictive_dispatch_payload_bytes += payload_len;
            // S4.1.c/I4: Extract output_len from deserialized transform payload.
            let logical_len = shared_protocol::decode_transform_instance_record(record)
                .map(|payload| payload.output_len as u64)
                .unwrap_or(payload_len);
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

fn classify_residual_mode(totals: &mut WebBenchResidualModeTotals, record: &Record) {
    match record.header.codec_mode {
        CodecMode::DirectExact => totals.none += 1,
        CodecMode::PackedExact | CodecMode::PredictedExact => {
            match inspect_data_payload(&record.payload, record.header.codec_mode) {
                Ok(inspection) => match inspection.residual_mode {
                    ResidualCodingMode::None => totals.none += 1,
                    ResidualCodingMode::AllZero => totals.all_zero += 1,
                    ResidualCodingMode::SmallSignedRans => totals.small_signed_rans += 1,
                    ResidualCodingMode::SparsePositions => totals.sparse_positions += 1,
                    ResidualCodingMode::LiteralRaw => totals.literal_raw += 1,
                },
                Err(_) => totals.unknown += 1,
            }
        }
        CodecMode::None => {}
    }
}

/// I4 fix / S4.1: Honest payload metric — counts payload bytes for ALL record types
/// that carry application data, not just ExactState. Mirrors server-side benchmark_payload_len.
fn benchmark_payload_len(record: &Record) -> u64 {
    match record.header.record_type {
        RecordType::ExactState
        | RecordType::PredictiveConfirm
        | RecordType::PredictiveCorrect
        | RecordType::TransformCorrect
        | RecordType::AssemblyDef
        | RecordType::TransformDef
        | RecordType::SchemaDef
        | RecordType::EpisodeHint
        | RecordType::ReplayHint
        | RecordType::SourceMeta
        | RecordType::Repair
        | RecordType::MemoryRetire => record.payload.len() as u64,
        // Control-plane records with no application payload
        RecordType::Rekey
        | RecordType::Close
        | RecordType::Resync
        | RecordType::MemoryAck => 0,
    }
}

fn benchmark_original_payload_len(record: &Record) -> u64 {
    if !matches!(record.header.record_type, RecordType::ExactState) {
        return 0;
    }

    inspect_data_payload(&record.payload, record.header.codec_mode)
        .map(|inspection| inspection.original_len as u64)
        .unwrap_or_else(|e| {
            // S4.1: Log warning when payload inspection fails rather than
            // silently falling back to payload_bytes.
            eprintln!(
                "WARNING: benchmark_original_payload_len: payload inspection failed for item {}: {} — using payload_bytes as fallback",
                record.header.item_id.0, e
            );
            benchmark_payload_len(record)
        })
}

fn is_vector_record(record: &Record) -> bool {
    matches!(record.header.record_type, RecordType::ExactState)
        && matches!(
            record.header.codec_mode,
            CodecMode::PackedExact | CodecMode::PredictedExact | CodecMode::DirectExact
        )
}

fn percentile(values: &[u64], fraction: f64) -> u64 {
    let index = ((values.len() as f64 - 1.0) * fraction).round() as usize;
    values[index.min(values.len() - 1)]
}
