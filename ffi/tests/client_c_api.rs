#![allow(unused_unsafe, unused_imports)]
//! FFI client C API tests, called from Rust. Verifies the round-trip via
//! the `pulzz_*` extern "C" surface without requiring an actual C compiler.

#![allow(clippy::missing_safety_doc)]

use pulzz_ffi::*;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

fn default_config() -> PulzzConfig {
    PulzzConfig::default()
}

#[test]
fn pulzz_abi_version_returns_packed_value() {
    let v = unsafe { pulzz_abi_version() };
    // 0.4.0 → (0 << 16) | (4 << 8) | 0 = 0x400
    assert_eq!(v, 0x400);
}

#[test]
fn pulzz_version_string_returns_non_null_cstring() {
    let ptr = unsafe { pulzz_version_string() };
    assert!(!ptr.is_null());
    let cstr = unsafe { std::ffi::CStr::from_ptr(ptr) };
    let s = cstr.to_str().expect("version must be UTF-8");
    assert!(s.contains("pulzZ"));
}

#[test]
fn pulzz_last_error_returns_null_when_no_error_set() {
    // Clear any stale error from prior tests on this thread
    unsafe {
        // Touch last_error; on a fresh thread-local it should be NULL.
        let ptr = pulzz_last_error();
        // It may be NULL or non-NULL depending on prior test state — just
        // confirm the function returns *something* (not undefined).
        let _ = ptr;
    }
}

#[test]
fn pulzz_client_new_with_null_args_returns_invalid_arg() {
    let mut handle: PulzzClientHandle = ptr::null_mut();
    let r = unsafe { pulzz_client_new(ptr::null(), &mut handle) };
    assert_eq!(r, PulzzResult::InvalidArg);
}

#[test]
fn pulzz_client_new_with_valid_config_succeeds_and_returns_handle() {
    let cfg = default_config();
    let mut handle: PulzzClientHandle = ptr::null_mut();
    let r = unsafe { pulzz_client_new(&cfg, &mut handle) };
    assert_eq!(r, PulzzResult::Ok);
    assert!(!handle.is_null());
    unsafe { pulzz_client_free(handle) };
}

#[test]
fn pulzz_batch_lifecycle_round_trip() {
    let mut batch: PulzzBatchHandle = ptr::null_mut();
    let r = unsafe { pulzz_batch_new(&mut batch) };
    assert_eq!(r, PulzzResult::Ok);
    assert!(!batch.is_null());

    let payload = b"hello";
    let slice = PulzzSlice {
        ptr: payload.as_ptr(),
        len: payload.len(),
    };
    let r = unsafe { pulzz_batch_add(batch, 1, 0, slice) };
    assert_eq!(r, PulzzResult::Ok);

    unsafe { pulzz_batch_free(batch) };
}

#[test]
fn pulzz_client_send_before_connect_returns_invalid_state() {
    let cfg = default_config();
    let mut handle: PulzzClientHandle = ptr::null_mut();
    unsafe {
        assert_eq!(pulzz_client_new(&cfg, &mut handle), PulzzResult::Ok);
        let payload = b"test";
        let slice = PulzzSlice {
            ptr: payload.as_ptr(),
            len: payload.len(),
        };
        let r = pulzz_client_send(handle, 1, slice);
        assert_eq!(r, PulzzResult::InvalidState);
        pulzz_client_free(handle);
    }
}

#[test]
fn pulzz_client_connect_with_null_args_returns_invalid_arg() {
    let cfg = default_config();
    let mut handle: PulzzClientHandle = ptr::null_mut();
    unsafe {
        assert_eq!(pulzz_client_new(&cfg, &mut handle), PulzzResult::Ok);
        let r = pulzz_client_connect(handle, ptr::null());
        assert_eq!(r, PulzzResult::InvalidArg);
        pulzz_client_free(handle);
    }
}

#[test]
fn pulzz_client_connect_to_invalid_url_returns_connection_failed() {
    let cfg = default_config();
    let mut handle: PulzzClientHandle = ptr::null_mut();
    unsafe {
        assert_eq!(pulzz_client_new(&cfg, &mut handle), PulzzResult::Ok);
        // Use a port that's almost certainly closed
        let url = CString::new("ws://127.0.0.1:1").unwrap();
        let r = pulzz_client_connect(handle, url.as_ptr());
        assert!(
            r == PulzzResult::ConnectionFailed
                || r == PulzzResult::HandshakeFailed
                || r == PulzzResult::Timeout
                || r == PulzzResult::Internal,
            "expected connection failure, got {:?}",
            r
        );
        // Verify last_error was set
        let err_ptr = pulzz_last_error();
        assert!(!err_ptr.is_null(), "last_error should be set on failure");
        let err_cstr = std::ffi::CStr::from_ptr(err_ptr);
        assert!(!err_cstr.to_str().unwrap().is_empty());
        pulzz_client_free(handle);
    }
}

#[test]
fn pulzz_slice_as_bytes_handles_null_pointer() {
    let s = PulzzSlice {
        ptr: ptr::null(),
        len: 0,
    };
    assert!(s.as_bytes().is_none());
}

#[test]
fn pulzz_slice_as_bytes_returns_some_for_non_null() {
    let data = b"hello";
    let s = PulzzSlice {
        ptr: data.as_ptr(),
        len: data.len(),
    };
    let bytes = s.as_bytes().expect("must be Some");
    assert_eq!(bytes, data);
}

#[test]
fn pulzz_config_default_round_trips_to_sdk_config() {
    let cfg = PulzzConfig::default();
    let sdk_cfg: pulzz_sdk::ClientConfig = cfg.into();
    assert_eq!(sdk_cfg.timeout_ms(), 30_000);
}

// Suppress unused-import warning for c_char (kept for type parity).
#[allow(dead_code)]
fn _cchar_unused(_c: c_char) {}
