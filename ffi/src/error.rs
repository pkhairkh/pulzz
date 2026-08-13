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
