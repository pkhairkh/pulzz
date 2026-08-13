//! WASM/JS binding for the pulzZ SDK. Exposes `PulzzClient` to JS via
//! `wasm-bindgen`. Server-side surfaces are intentionally omitted because
//! browsers/Node do not run the pulzZ server.
//!
//! **Build target:** this crate is only compiled for `wasm32-unknown-unknown`.
//! On native targets it compiles to an empty library so the workspace still
//! builds cleanly without clang/zstd-sys.
//!
//! See `docs/SDK_PROPOSAL.md` §7 for the API contract.

#![cfg(target_arch = "wasm32")]

use js_sys::{Object, Reflect, Uint8Array};
use pulzz_sdk::{
    CarrierKind, ClientConfig, CompressionConfig, PulzzClient, PulzzClientBuilder, SecurityProfile,
};
use wasm_bindgen::prelude::*;

/// Convert a JS error string into a JsValue.
fn js_err(msg: impl Into<String>) -> JsValue {
    JsValue::from_str(&msg.into())
}

/// Parse a JS config object into a `ClientConfig`.
fn parse_config(js: &JsValue) -> Result<ClientConfig, JsValue> {
    let mut cfg = ClientConfig::default();
    cfg.compression = CompressionConfig::wasm();
    cfg.security = SecurityProfile::PqSimpleV1;
    cfg.carrier = CarrierKind::WebSocket;

    if js.is_object() {
        let obj: &Object = js.as_ref().unchecked_ref();
        if let Ok(carrier) = Reflect::get(obj, &"carrier".into()) {
            if let Some(s) = carrier.as_string() {
                cfg.carrier = match s.as_str() {
                    "websocket" | "ws" => CarrierKind::WebSocket,
                    "webtransport" | "wt" => CarrierKind::WebTransport,
                    _ => return Err(js_err(format!("unsupported carrier: {s}"))),
                };
            }
        }
        if let Ok(security) = Reflect::get(obj, &"security".into()) {
            if let Some(s) = security.as_string() {
                cfg.security = match s.as_str() {
                    "pq_simple_v1" | "pq_simple" => SecurityProfile::PqSimpleV1,
                    "classic_ref1" | "classic" => SecurityProfile::ClassicRef1,
                    "pq_mutual_v1" | "pq_mutual" => SecurityProfile::PqSimpleV1,
                    _ => return Err(js_err(format!("unsupported security: {s}"))),
                };
            }
        }
        if let Ok(batch) = Reflect::get(obj, &"batchSize".into()) {
            if let Some(n) = batch.as_f64() {
                cfg.batch_size = if n > 0.0 { Some(n as usize) } else { None };
            }
        }
        if let Ok(level) = Reflect::get(obj, &"zstdLevel".into()) {
            if let Some(n) = level.as_f64() {
                cfg.compression.zstd_level = n as i32;
                cfg.compression.enabled = n > 0.0;
            }
        }
        if let Ok(timeout) = Reflect::get(obj, &"timeoutMs".into()) {
            if let Some(n) = timeout.as_f64() {
                cfg.timeout = std::time::Duration::from_millis(n as u64);
            }
        }
    }
    Ok(cfg)
}

#[wasm_bindgen]
pub struct PulzzWasmClient {
    inner: Option<PulzzClient>,
    config: ClientConfig,
}

#[wasm_bindgen]
impl PulzzWasmClient {
    #[wasm_bindgen(constructor)]
    pub fn new(config: JsValue) -> Result<PulzzWasmClient, JsValue> {
        let cfg = parse_config(&config)?;
        Ok(Self {
            inner: None,
            config: cfg,
        })
    }

    /// Connect to a pulzZ server at `url`.
    ///
    /// **NOTE:** Real network-mode connect is not yet wired through the
    /// `pulzz-sdk` crate on WASM. The SDK's `PulzzClient::connect_with_config`
    /// depends on native-only transport code (tokio + quinn/rustls/aws-lc-sys)
    /// which cannot compile for `wasm32-unknown-unknown`.
    ///
    /// To use the SDK in a browser/Node context today, construct a
    /// `PulzzClient` via `PulzzClient::from_session` using a `ClientSession`
    /// built from the lower-level `client` crate's wasm32 WebSocket /
    /// WebTransport backend, then call `send` / `recv` / `close` on the
    /// returned `PulzzWasmClient`.
    ///
    /// This method always returns an error so callers fail fast instead of
    /// silently getting a broken connection.
    pub async fn connect(&mut self, _url: String) -> Result<(), JsValue> {
        Err(js_err(
            "PulzzWasmClient::connect is not implemented for WASM; \
             real network-mode connect is not yet wired through pulzz-sdk on \
             wasm32. Use PulzzClient::from_session with a ClientSession built \
             from the lower-level `client` crate's wasm32 WebSocket backend \
             for in-browser / Node usage.",
        ))
    }

    pub async fn send(&mut self, item_id: u64, payload: &[u8]) -> Result<(), JsValue> {
        let client = self.inner.as_mut().ok_or_else(|| js_err("not connected"))?;
        client
            .send(pulzz_sdk::ItemId(item_id), payload)
            .await
            .map_err(|e| js_err(format!("{e}")))
    }

    pub async fn send_batch(&mut self, items: JsValue) -> Result<(), JsValue> {
        let client = self.inner.as_mut().ok_or_else(|| js_err("not connected"))?;
        let arr: js_sys::Array = items
            .dyn_into()
            .map_err(|_| js_err("items must be an array"))?;
        let mut collected: Vec<(pulzz_sdk::ItemId, Vec<u8>)> = Vec::with_capacity(arr.length() as usize);
        for item in arr.iter() {
            let obj: Object = item
                .dyn_into()
                .map_err(|_| js_err("each item must be an object"))?;
            let id_val = Reflect::get(&obj, &"itemId".into())
                .map_err(|_| js_err("item missing itemId"))?;
            let id = id_val
                .as_f64()
                .ok_or_else(|| js_err("itemId must be a number"))? as u64;
            let payload_val = Reflect::get(&obj, &"payload".into())
                .map_err(|_| js_err("item missing payload"))?;
            let payload_u8: Uint8Array = payload_val
                .dyn_into()
                .map_err(|_| js_err("payload must be a Uint8Array"))?;
            let mut bytes = vec![0u8; payload_u8.length() as usize];
            payload_u8.copy_to(&mut bytes);
            collected.push((pulzz_sdk::ItemId(id), bytes));
        }
        client
            .send_batch(collected)
            .await
            .map_err(|e| js_err(format!("{e}")))
    }

    pub async fn recv(&mut self, _timeout_ms: u32) -> Result<JsValue, JsValue> {
        let client = self.inner.as_mut().ok_or_else(|| js_err("not connected"))?;
        let record = client
            .recv()
            .await
            .map_err(|e| js_err(format!("{e}")))?;
        let Some(record) = record else {
            return Ok(JsValue::NULL);
        };
        let obj = Object::new();
        Reflect::set(&obj, &"itemId".into(), &JsValue::from(record.header.item_id.0))
            .map_err(|_| js_err("set itemId failed"))?;
        let payload = Uint8Array::from(&record.payload[..]);
        Reflect::set(&obj, &"payload".into(), &payload)
            .map_err(|_| js_err("set payload failed"))?;
        Reflect::set(
            &obj,
            &"recordType".into(),
            &JsValue::from(record.header.record_type as u8),
        )
        .map_err(|_| js_err("set recordType failed"))?;
        Ok(obj.into())
    }

    pub async fn close(self) -> Result<(), JsValue> {
        if let Some(client) = self.inner {
            client.close().await.map_err(|e| js_err(format!("{e}")))?;
        }
        Ok(())
    }
}

#[wasm_bindgen]
pub fn pulzz_version() -> String {
    "pulzZ 0.5.0-sdk-hardened (WASM)".to_string()
}

#[allow(dead_code)]
fn _builder_unused() -> PulzzClientBuilder {
    PulzzClientBuilder::default()
}
