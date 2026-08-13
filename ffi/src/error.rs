//! Thread-local last-error string. Used by `pulzz_last_error()` to surface
//! human-readable error messages across the FFI boundary.

use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

pub fn set_last_error(err: &str) {
    let cstring = CString::new(err).unwrap_or_else(|_| {
        // err contained an interior NUL — replace with a placeholder.
        CString::new("<error message contained interior NUL>").unwrap()
    });
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(cstring);
    });
}

pub fn clear_last_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

pub fn last_error_ptr() -> *const c_char {
    LAST_ERROR.with(|cell| match cell.borrow().as_ref() {
        Some(cstring) => cstring.as_ptr(),
        None => ptr::null(),
    })
}

// Thread-local payload buffer for pulzz_parse_record. The pointer returned
// to the caller is valid until the next pulzz_parse_record call on the same
// thread. This avoids requiring the caller to free the buffer.
thread_local! {
    static PAYLOAD_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Store a payload buffer in thread-local storage and return a pointer to
/// its contents. The pointer is valid until the next call to this function
/// on the same thread.
pub fn store_payload_buffer(payload: Vec<u8>) -> *mut u8 {
    PAYLOAD_BUFFER.with(|cell| {
        let mut buf = cell.borrow_mut();
        *buf = payload;
        buf.as_mut_ptr()
    })
}
