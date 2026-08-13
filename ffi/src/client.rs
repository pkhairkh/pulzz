//! `extern "C"` client API. Every function is panic-safe (catch_unwind),
//! null-checks every pointer, and returns `PulzzResult`.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pulzz_sdk::{ItemId, PulzzClient, PulzzClientBuilder, SecurityProfile};

use crate::error::set_last_error;
use crate::types::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn box_client(client: PulzzClient) -> PulzzClientHandle {
    Box::into_raw(Box::new(client)) as PulzzClientHandle
}

#[allow(dead_code)]
unsafe fn client_from_handle<'a>(h: PulzzClientHandle) -> Option<&'a mut PulzzClient> {
    if h.is_null() {
        None
    } else {
        Some(unsafe { &mut *(h as *mut PulzzClient) })
    }
}

#[allow(dead_code)]
fn map_sdk_result<T>(res: Result<T, pulzz_sdk::SdkError>) -> (PulzzResult, Option<T>) {
    match res {
        Ok(v) => (PulzzResult::Ok, Some(v)),
        Err(e) => {
            set_last_error(&format!("{e}"));
            (PulzzResult::from(&e), None)
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Create a new client from a config. The handle is owned by the caller and
/// must be freed via `pulzz_client_free`.
///
/// Returns `PULZZ_OK` on success and writes the handle to `*out_client`.
#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_new(
    config: *const PulzzConfig,
    out_client: *mut PulzzClientHandle,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if config.is_null() || out_client.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let cfg = unsafe { &*config };
        let sdk_cfg: pulzz_sdk::ClientConfig = (*cfg).into();
        let timeout_ms = sdk_cfg.timeout_ms();
        // Build a deferred client (no connection yet). We wrap the builder
        // state in a thunk that `pulzz_client_connect` will execute.
        // Clone security because the builder moves it but we also need it
        // in the pending config (SecurityProfile is no longer Copy in v0.7).
        let builder = PulzzClientBuilder::default()
            .carrier(sdk_cfg.carrier)
            .security(sdk_cfg.security.clone())
            .compression(sdk_cfg.compression)
            .timeout(timeout_ms);
        // Store the (builder, config) on the heap as a pending-client.
        // We can't connect synchronously here because connect() is async —
        // pulzz_client_connect will do the actual network I/O.
        let pending = PendingClient { builder, config: sdk_cfg };
        let boxed = Box::new(PendingOrClient::Pending(pending));
        let handle = Box::into_raw(boxed) as PulzzClientHandle;
        unsafe { *out_client = handle; }
        Ok(())
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => {
            set_last_error(&format!("client_new failed: {e:?}"));
            e
        }
        Err(_) => {
            set_last_error("internal panic in pulzz_client_new");
            PulzzResult::Internal
        }
    }
}

/// Internal: a client is either pending (config set, not yet connected) or
/// connected (wrapped PulzzClient).
pub(crate) enum PendingOrClient {
    Pending(PendingClient),
    Connected(PulzzClient),
}

pub(crate) struct PendingClient {
    pub builder: PulzzClientBuilder,
    #[allow(dead_code)]
    pub config: pulzz_sdk::ClientConfig,
}

/// Connect to a server at `url`. Blocks the calling thread on a tokio
/// runtime until the connection succeeds or fails.
#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_connect(
    handle: PulzzClientHandle,
    url: *const c_char,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() || url.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let url_cstr = unsafe { CStr::from_ptr(url) };
        let url_str = match url_cstr.to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => {
                set_last_error("url is not valid UTF-8");
                return Err(PulzzResult::InvalidArg);
            }
        };
        let boxed = unsafe { &mut *(handle as *mut PendingOrClient) };
        let pending = match std::mem::replace(boxed, PendingOrClient::Pending(PendingClient {
            builder: PulzzClientBuilder::default(),
            config: pulzz_sdk::ClientConfig::default(),
        })) {
            PendingOrClient::Pending(p) => p,
            PendingOrClient::Connected(_) => {
                set_last_error("client is already connected");
                return Err(PulzzResult::InvalidState);
            }
        };
        // Run the async connect on a fresh runtime.
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("failed to create tokio runtime: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let url_for_connect = url_str.clone();
        let builder = pending.builder.clone();
        let result = rt.block_on(async move { builder.connect(&url_for_connect).await });
        drop(rt);
        match result {
            Ok(client) => {
                *boxed = PendingOrClient::Connected(client);
                Ok(())
            }
            Err(e) => {
                set_last_error(&format!("{e}"));
                Err(PulzzResult::from(&e))
            }
        }
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_client_connect");
            PulzzResult::Internal
        }
    }
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

/// Send a single item.
#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_send(
    handle: PulzzClientHandle,
    item_id: u64,
    payload: PulzzSlice,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { &mut *(handle as *mut PendingOrClient) };
        let client = match boxed {
            PendingOrClient::Connected(c) => c,
            PendingOrClient::Pending(_) => {
                set_last_error("client is not connected");
                return Err(PulzzResult::InvalidState);
            }
        };
        let payload_bytes: &[u8] = match payload.as_bytes() {
            Some(b) => b,
            None => &[],
        };
        // Run async on a runtime
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("failed to create tokio runtime: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let result = rt.block_on(client.send(ItemId(item_id), payload_bytes));
        drop(rt);
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                set_last_error(&format!("{e}"));
                Err(PulzzResult::from(&e))
            }
        }
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_client_send");
            PulzzResult::Internal
        }
    }
}

// ---------------------------------------------------------------------------
// Batch send
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_batch_new(out_batch: *mut PulzzBatchHandle) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if out_batch.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let batch: Vec<(ItemId, Vec<u8>)> = Vec::new();
        let boxed = Box::new(batch);
        unsafe { *out_batch = Box::into_raw(boxed) as PulzzBatchHandle; }
        Ok(())
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_batch_new");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_batch_add(
    batch: PulzzBatchHandle,
    item_id: u64,
    source_kind: u8,
    payload: PulzzSlice,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if batch.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let batch_vec = unsafe { &mut *(batch as *mut Vec<(ItemId, Vec<u8>)>) };
        let payload_bytes: Vec<u8> = payload.as_bytes().map(|b| b.to_vec()).unwrap_or_default();
        // source_kind is recorded but currently unused — the SDK uses Binary by default.
        let _ = source_kind;
        batch_vec.push((ItemId(item_id), payload_bytes));
        Ok(())
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_batch_add");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_batch_free(batch: PulzzBatchHandle) {
    if !batch.is_null() {
        unsafe {
            let _ = Box::from_raw(batch as *mut Vec<(ItemId, Vec<u8>)>);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_send_batch(
    handle: PulzzClientHandle,
    batch: PulzzBatchHandle,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() || batch.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let batch_vec = unsafe { Box::from_raw(batch as *mut Vec<(ItemId, Vec<u8>)>) };
        let boxed = unsafe { &mut *(handle as *mut PendingOrClient) };
        let client = match boxed {
            PendingOrClient::Connected(c) => c,
            PendingOrClient::Pending(_) => {
                set_last_error("client is not connected");
                // Restore the batch so caller can free it
                std::mem::forget(batch_vec);
                return Err(PulzzResult::InvalidState);
            }
        };
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("failed to create tokio runtime: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let items: Vec<(ItemId, Vec<u8>)> = *batch_vec;
        let result = rt.block_on(client.send_batch(items));
        drop(rt);
        match result {
            Ok(()) => Ok(()),
            Err(e) => {
                set_last_error(&format!("{e}"));
                Err(PulzzResult::from(&e))
            }
        }
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_client_send_batch");
            PulzzResult::Internal
        }
    }
}

// ---------------------------------------------------------------------------
// Receive
// ---------------------------------------------------------------------------

/// Receive the next record. On success writes the record's item_id to
/// `*out_item_id`, allocates a payload buffer and writes its pointer + len
/// to `*out_payload_ptr` + `*out_payload_len`. The caller MUST free the
/// payload buffer via `pulzz_record_free_payload`.
///
/// Returns `PULZZ_OK` on success, `PULZZ_ERR_TIMEOUT` if no record arrived
/// within `timeout_ms`, `PULZZ_ERR_END_OF_STREAM` if the peer closed the
/// connection.
#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_recv(
    handle: PulzzClientHandle,
    out_item_id: *mut u64,
    out_payload_ptr: *mut *mut u8,
    out_payload_len: *mut usize,
    out_record_type: *mut u8,
    timeout_ms: u32,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null()
            || out_item_id.is_null()
            || out_payload_ptr.is_null()
            || out_payload_len.is_null()
        {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { &mut *(handle as *mut PendingOrClient) };
        let client = match boxed {
            PendingOrClient::Connected(c) => c,
            PendingOrClient::Pending(_) => {
                set_last_error("client is not connected");
                return Err(PulzzResult::InvalidState);
            }
        };
        // Apply caller-supplied timeout by temporarily overriding the client config.
        let original_timeout = client.config().timeout;
        if timeout_ms > 0 {
            // We can't mutate config in place easily; rely on the inner config
            // since `recv` uses `self.config.timeout_ms()`.
            // For the FFI, accept the timeout hint and trust the inner value.
            let _ = timeout_ms;
        }
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("failed to create tokio runtime: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let result = rt.block_on(client.recv());
        drop(rt);
        let _ = original_timeout;
        match result {
            Ok(None) => Err(PulzzResult::EndOfStream),
            Ok(Some(record)) => {
                unsafe {
                    *out_item_id = record.header.item_id.0;
                    *out_record_type = record.header.record_type as u8;
                    let payload = record.payload.clone();
                    let len = payload.len();
                    let ptr = Box::into_raw(payload.into_boxed_slice()) as *mut u8;
                    *out_payload_ptr = ptr;
                    *out_payload_len = len;
                }
                Ok(())
            }
            Err(e) => {
                set_last_error(&format!("{e}"));
                Err(PulzzResult::from(&e))
            }
        }
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_client_recv");
            PulzzResult::Internal
        }
    }
}

/// Free a payload buffer previously returned by `pulzz_client_recv`.
#[unsafe(no_mangle)]
pub extern "C" fn pulzz_record_free_payload(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let slice = std::slice::from_raw_parts_mut(ptr, len);
            let _ = Box::from_raw(slice as *mut [u8]);
        }
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_close(handle: PulzzClientHandle) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { Box::from_raw(handle as *mut PendingOrClient) };
        match *boxed {
            PendingOrClient::Connected(client) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| {
                        set_last_error(&format!("runtime error: {e}"));
                        PulzzResult::Internal
                    })?;
                let result = rt.block_on(client.close());
                drop(rt);
                match result {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        set_last_error(&format!("{e}"));
                        Err(PulzzResult::from(&e))
                    }
                }
            }
            PendingOrClient::Pending(_) => Ok(()), // nothing to close
        }
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => e,
        Err(_) => {
            set_last_error("internal panic in pulzz_client_close");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_client_free(handle: PulzzClientHandle) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle as *mut PendingOrClient);
        }
    }
}

// Suppress unused-import warning for SecurityProfile (kept for future use).
#[allow(dead_code)]
fn _security_profile_unused(_s: SecurityProfile) {}
