//! Version constants for the C ABI.

/// Major.minor.patch packed as `major<<16 | minor<<8 | patch`.
pub const ABI_VERSION: u32 = (0 << 16) | (4 << 8) | 0;

pub const VERSION_STRING: &str = "pulzZ 0.4.0-sdk (C ABI v0.4)";
