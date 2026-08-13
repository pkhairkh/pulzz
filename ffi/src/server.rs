//! `extern "C"` server API. Same panic-safe, null-checked pattern as the
//! client surface.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};

use pulzz_sdk::{ItemId, PulzzServer, PulzzServerBuilder};

use crate::error::set_last_error;
use crate::types::*;

pub(crate) enum PendingOrServer {
    Pending {
        builder: PulzzServerBuilder,
        config: pulzz_sdk::ClientConfig,
    },
    Bound(PulzzServer),
}

pub(crate) enum PendingOrSession {
    Connected(pulzz_sdk::PulzzSession),
}

fn box_server(s: PulzzServer) -> PulzzServerHandle {
    Box::into_raw(Box::new(PendingOrServer::Bound(s))) as PulzzServerHandle
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_server_new(
    config: *const PulzzConfig,
    out_server: *mut PulzzServerHandle,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if config.is_null() || out_server.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let cfg = unsafe { &*config };
        let sdk_cfg: pulzz_sdk::ClientConfig = (*cfg).into();
        let builder = PulzzServerBuilder::default()
            .carrier(sdk_cfg.carrier)
            .security(sdk_cfg.security)
            .timeout(sdk_cfg.timeout_ms());
        let pending = PendingOrServer::Pending { builder, config: sdk_cfg };
        let boxed = Box::new(pending);
        unsafe { *out_server = Box::into_raw(boxed) as PulzzServerHandle; }
        Ok(())
    }));
    match res {
        Ok(Ok(())) => PulzzResult::Ok,
        Ok(Err(e)) => {
            set_last_error(&format!("server_new failed: {e:?}"));
            e
        }
        Err(_) => {
            set_last_error("internal panic in pulzz_server_new");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_server_bind(
    handle: PulzzServerHandle,
    addr: *const c_char,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() || addr.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let addr_cstr = unsafe { CStr::from_ptr(addr) };
        let addr_str = match addr_cstr.to_str() {
            Ok(s) => s.to_owned(),
            Err(_) => {
                set_last_error("addr is not valid UTF-8");
                return Err(PulzzResult::InvalidArg);
            }
        };
        let boxed = unsafe { &mut *(handle as *mut PendingOrServer) };
        let (builder, _config) = match std::mem::replace(boxed, PendingOrServer::Pending {
            builder: PulzzServerBuilder::default(),
            config: pulzz_sdk::ClientConfig::default(),
        }) {
            PendingOrServer::Pending { builder, config } => (builder, config),
            PendingOrServer::Bound(_) => {
                set_last_error("server is already bound");
                return Err(PulzzResult::InvalidState);
            }
        };
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("runtime error: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let builder_for_bind = builder.clone();
        let addr_for_bind = addr_str.clone();
        let result = rt.block_on(async move { builder_for_bind.bind_addr(addr_for_bind).bind().await });
        drop(rt);
        match result {
            Ok(server) => {
                *boxed = PendingOrServer::Bound(server);
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
            set_last_error("internal panic in pulzz_server_bind");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_server_accept(
    handle: PulzzServerHandle,
    out_session: *mut PulzzSessionHandle,
    _timeout_ms: u32,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() || out_session.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { &mut *(handle as *mut PendingOrServer) };
        let server = match boxed {
            PendingOrServer::Bound(s) => s,
            PendingOrServer::Pending { .. } => {
                set_last_error("server is not bound");
                return Err(PulzzResult::InvalidState);
            }
        };
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("runtime error: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let result = rt.block_on(server.accept());
        drop(rt);
        match result {
            Ok(None) => Err(PulzzResult::EndOfStream),
            Ok(Some(session)) => {
                let boxed_session = Box::new(PendingOrSession::Connected(session));
                unsafe {
                    *out_session = Box::into_raw(boxed_session) as PulzzSessionHandle;
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
            set_last_error("internal panic in pulzz_server_accept");
            PulzzResult::Internal
        }
    }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_session_send(
    handle: PulzzSessionHandle,
    item_id: u64,
    payload: PulzzSlice,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { &mut *(handle as *mut PendingOrSession) };
        let session = match boxed {
            PendingOrSession::Connected(s) => s,
        };
        let payload_bytes: &[u8] = payload.as_bytes().unwrap_or(&[]);
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("runtime error: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let result = rt.block_on(session.send(ItemId(item_id), payload_bytes));
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
            set_last_error("internal panic in pulzz_session_send");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_session_recv(
    handle: PulzzSessionHandle,
    out_item_id: *mut u64,
    out_payload_ptr: *mut *mut u8,
    out_payload_len: *mut usize,
    out_record_type: *mut u8,
    _timeout_ms: u32,
) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null()
            || out_item_id.is_null()
            || out_payload_ptr.is_null()
            || out_payload_len.is_null()
        {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { &mut *(handle as *mut PendingOrSession) };
        let session = match boxed {
            PendingOrSession::Connected(s) => s,
        };
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                set_last_error(&format!("runtime error: {e}"));
                return Err(PulzzResult::Internal);
            }
        };
        let result = rt.block_on(session.recv());
        drop(rt);
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
            set_last_error("internal panic in pulzz_session_recv");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_session_close(handle: PulzzSessionHandle) -> PulzzResult {
    let res = catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() {
            return Err(PulzzResult::InvalidArg);
        }
        let boxed = unsafe { Box::from_raw(handle as *mut PendingOrSession) };
        // PendingOrSession has only one variant; just unwrap it.
        let PendingOrSession::Connected(session) = *boxed;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                set_last_error(&format!("runtime error: {e}"));
                PulzzResult::Internal
            })?;
        let result = rt.block_on(session.close());
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
            set_last_error("internal panic in pulzz_session_close");
            PulzzResult::Internal
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_session_free(handle: PulzzSessionHandle) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle as *mut PendingOrSession);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pulzz_server_free(handle: PulzzServerHandle) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle as *mut PendingOrServer);
        }
    }
}

// Re-export box_server for symmetry with client API; not currently used externally.
#[allow(dead_code)]
fn _box_server_unused(s: PulzzServer) -> PulzzServerHandle {
    box_server(s)
}
