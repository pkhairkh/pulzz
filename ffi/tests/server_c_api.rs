//! FFI server C API tests, called from Rust. Verifies handle lifecycle
//! and basic null-checks without spinning up a real listener.

#![allow(clippy::missing_safety_doc)]

use pulzz::*;
use std::ptr;

#[test]
fn pulzz_server_new_with_null_args_returns_invalid_arg() {
    let mut handle: PulzzServerHandle = ptr::null_mut();
    let r = unsafe { pulzz_server_new(ptr::null(), &mut handle) };
    assert_eq!(r, PulzzResult::InvalidArg);
}

#[test]
fn pulzz_server_new_with_valid_config_returns_handle() {
    let cfg = PulzzConfig::default();
    let mut handle: PulzzServerHandle = ptr::null_mut();
    let r = unsafe { pulzz_server_new(&cfg, &mut handle) };
    assert_eq!(r, PulzzResult::Ok);
    assert!(!handle.is_null());
    unsafe { pulzz_server_free(handle) };
}

#[test]
fn pulzz_server_accept_before_bind_returns_invalid_state() {
    let cfg = PulzzConfig::default();
    let mut handle: PulzzServerHandle = ptr::null_mut();
    unsafe {
        assert_eq!(pulzz_server_new(&cfg, &mut handle), PulzzResult::Ok);
        let mut session: PulzzSessionHandle = ptr::null_mut();
        let r = pulzz_server_accept(handle, &mut session, 0);
        assert_eq!(r, PulzzResult::InvalidState);
        pulzz_server_free(handle);
    }
}

#[test]
fn pulzz_server_free_on_null_is_a_noop() {
    unsafe { pulzz_server_free(ptr::null_mut()) };
}

#[test]
fn pulzz_session_free_on_null_is_a_noop() {
    unsafe { pulzz_session_free(ptr::null_mut()) };
}

#[test]
fn pulzz_session_send_with_null_handle_returns_invalid_arg() {
    let slice = PulzzSlice {
        ptr: b"x".as_ptr(),
        len: 1,
    };
    let r = unsafe { pulzz_session_send(ptr::null_mut(), 1, slice) };
    assert_eq!(r, PulzzResult::InvalidArg);
}

#[test]
fn pulzz_session_recv_with_null_handle_returns_invalid_arg() {
    let mut item_id: u64 = 0;
    let mut payload_ptr: *mut u8 = ptr::null_mut();
    let mut payload_len: usize = 0;
    let mut record_type: u8 = 0;
    let r = unsafe {
        pulzz_session_recv(
            ptr::null_mut(),
            &mut item_id,
            &mut payload_ptr,
            &mut payload_len,
            &mut record_type,
            0,
        )
    };
    assert_eq!(r, PulzzResult::InvalidArg);
}
