//! Version constants for the C ABI.

/// Major.minor.patch packed as `major<<16 | minor<<8 | patch`.
#[allow(clippy::identity_op)] // 0 << 16 is intentional for readability of the version packing
pub const ABI_VERSION: u32 = (0 << 16) | (7 << 8) | 0;

pub const VERSION_STRING: &str = "pulzZ 0.7.0-multi-record (C ABI v0.7)";
