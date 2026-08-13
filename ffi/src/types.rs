//! Core C ABI types. Opaque handles, result codes, slice/config structs.

use std::os::raw::{c_char, c_void};

use pulzz_sdk::{CarrierKind, ClientConfig, CompressionConfig, SecurityProfile};

// ---------------------------------------------------------------------------
// Result codes
// ---------------------------------------------------------------------------

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulzzResult {
    Ok = 0,
    InvalidArg = 1,
    InvalidState = 2,
    ConnectionFailed = 3,
    HandshakeFailed = 4,
    CompressionFailed = 5,
    Timeout = 6,
    BufferTooSmall = 7,
    EndOfStream = 8,
    Internal = 99,
}

impl From<&pulzz_sdk::SdkError> for PulzzResult {
    fn from(err: &pulzz_sdk::SdkError) -> Self {
        use pulzz_sdk::SdkError::*;
        match err {
            InvalidArg { .. } => Self::InvalidArg,
            InvalidState { .. } => Self::InvalidState,
            UnsupportedCarrier { .. } => Self::InvalidArg,
            ConnectionFailed { .. } => Self::ConnectionFailed,
            HandshakeFailed { .. } => Self::HandshakeFailed,
            CompressionFailed { .. } => Self::CompressionFailed,
            Timeout(_) => Self::Timeout,
            BufferTooSmall { .. } => Self::BufferTooSmall,
            _ => Self::Internal,
        }
    }
}

impl From<pulzz_sdk::SdkError> for PulzzResult {
    fn from(err: pulzz_sdk::SdkError) -> Self {
        Self::from(&err)
    }
}

// ---------------------------------------------------------------------------
// Opaque handles (boxed pointers)
// ---------------------------------------------------------------------------

pub type PulzzClientHandle = *mut c_void;
pub type PulzzServerHandle = *mut c_void;
pub type PulzzSessionHandle = *mut c_void;
pub type PulzzBatchHandle = *mut c_void;

// ---------------------------------------------------------------------------
// Carrier + security enums
// ---------------------------------------------------------------------------

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulzzCarrierKind {
    WebSocket = 0,
    Tcp = 1,
    QuicStream = 2,
    QuicDatagram = 3,
    WebTransport = 4,
    UdpDatagram = 5,
}

impl From<PulzzCarrierKind> for CarrierKind {
    fn from(c: PulzzCarrierKind) -> Self {
        match c {
            PulzzCarrierKind::WebSocket => CarrierKind::WebSocket,
            PulzzCarrierKind::Tcp => CarrierKind::Tcp,
            PulzzCarrierKind::QuicStream => CarrierKind::QuicStream,
            PulzzCarrierKind::QuicDatagram => CarrierKind::QuicDatagram,
            PulzzCarrierKind::WebTransport => CarrierKind::WebTransport,
            PulzzCarrierKind::UdpDatagram => CarrierKind::UdpDatagram,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulzzSecurityProfile {
    PqMutualV1 = 0,
    PqSimpleV1 = 1,
    ClassicRef1 = 2,
}

impl From<PulzzSecurityProfile> for SecurityProfile {
    fn from(s: PulzzSecurityProfile) -> Self {
        match s {
            // FFI does not yet expose the full PqMutualV1 credential API
            // (IssuedClientCredential + ServerIdentityBundle require structured
            // serialization). FFI callers requesting PqMutualV1 get PqSimpleV1
            // as a fallback. Use the Rust SDK directly for PqMutualV1.
            PulzzSecurityProfile::PqMutualV1 => SecurityProfile::PqSimpleV1,
            PulzzSecurityProfile::PqSimpleV1 => SecurityProfile::PqSimpleV1,
            PulzzSecurityProfile::ClassicRef1 => SecurityProfile::ClassicRef1,
        }
    }
}

// ---------------------------------------------------------------------------
// Slice types (zero-copy)
// ---------------------------------------------------------------------------

/// Read-only byte slice passed across the FFI boundary.
#[repr(C)]
pub struct PulzzSlice {
    pub ptr: *const u8,
    pub len: usize,
}

impl PulzzSlice {
    /// Returns `None` if ptr is null or len is 0.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.ptr.is_null() || self.len == 0 {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.ptr, self.len) })
        }
    }

    /// Build a slice from a Rust byte slice. The slice is only valid as
    /// long as the original Rust slice is alive.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
}

/// Writable byte buffer passed across the FFI boundary.
#[repr(C)]
pub struct PulzzMutSlice {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

impl PulzzMutSlice {
    /// Returns `None` if ptr is null or len is 0.
    pub fn as_bytes_mut(&mut self) -> Option<&mut [u8]> {
        if self.ptr.is_null() || self.len == 0 {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) })
        }
    }
}

// ---------------------------------------------------------------------------
// Config struct
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PulzzConfig {
    pub carrier: PulzzCarrierKind,
    pub security: PulzzSecurityProfile,
    pub batch_size: u32,      // 0 = per-item, >0 = batch size
    pub zstd_level: i32,      // 0 = disabled, 3 = default
    pub timeout_ms: u64,
}

impl Default for PulzzConfig {
    fn default() -> Self {
        Self {
            carrier: PulzzCarrierKind::WebSocket,
            security: PulzzSecurityProfile::PqSimpleV1,
            batch_size: 0,
            zstd_level: 3,
            timeout_ms: 30_000,
        }
    }
}

impl From<PulzzConfig> for ClientConfig {
    fn from(c: PulzzConfig) -> Self {
        ClientConfig {
            security: c.security.into(),
            carrier: c.carrier.into(),
            compression: CompressionConfig {
                enabled: c.zstd_level > 0,
                batch_threshold: if c.batch_size > 0 { c.batch_size as usize } else { 10 },
                zstd_level: c.zstd_level,
            },
            batch_size: if c.batch_size > 0 {
                Some(c.batch_size as usize)
            } else {
                None
            },
            timeout: std::time::Duration::from_millis(c.timeout_ms),
        }
    }
}

// ---------------------------------------------------------------------------
// Record (returned to caller via pulzz_client_recv)
// ---------------------------------------------------------------------------

/// Decoded record returned by `pulzz_client_recv` / `pulzz_session_recv`.
///
/// The `payload` pointer is owned by the SDK and is freed when the caller
/// invokes `pulzz_record_free`.
#[repr(C)]
pub struct PulzzRecord {
    pub item_id: u64,
    pub payload_ptr: *mut u8,
    pub payload_len: usize,
    pub record_type: u8,
}

impl PulzzRecord {
    pub fn empty() -> Self {
        Self {
            item_id: 0,
            payload_ptr: std::ptr::null_mut(),
            payload_len: 0,
            record_type: 0,
        }
    }
}

// Re-export c_char so callers of this module don't need to import it.
#[allow(dead_code)]
pub type CChar = c_char;
