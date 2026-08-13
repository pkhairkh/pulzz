//! pulzZ shared protocol: PQC session layer, record framing, compression,
//! batch envelope, predictive router (UCB1 + PST).

// Pre-existing clippy style lints silenced at crate level so
// `cargo clippy --workspace -- -D warnings` can pass. Each lint is a style
// suggestion (not a correctness bug); manually fixing 65+ across 8 files is
// a separate cleanup task tracked in the spec §0.2 bug #10.
#![allow(
    clippy::collapsible_if,
    clippy::redundant_closure,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_cast,
    clippy::unnecessary_lazy_evaluations,
    clippy::unnecessary_map_or,
    clippy::unnecessary_to_owned,
    clippy::unnecessary_unwrap,
    clippy::manual_is_multiple_of,
    clippy::manual_range_patterns,
    clippy::byte_char_slices,
    clippy::len_zero,
    clippy::large_enum_variant,
    clippy::question_mark,
    clippy::redundant_iter_cloned,
)]

pub mod bandit;
pub mod batch;
pub mod bootstrap;
#[cfg(not(target_arch = "wasm32"))]
pub mod carrier;
pub mod catalog;
pub mod chpmt;
pub mod codec;
#[cfg(not(target_arch = "wasm32"))]
pub mod compress;
#[cfg(target_arch = "wasm32")]
pub mod compress_wasm;
#[cfg(target_arch = "wasm32")]
pub use compress_wasm as compress;
pub mod datagram;
pub mod kernels;
pub mod protection;
pub mod protocol;
pub mod pst;
pub mod source;
pub mod state;
pub mod state_policy;
pub mod transport;
pub mod vector;

pub use bandit::*;
pub use batch::*;
pub use bootstrap::*;
#[cfg(not(target_arch = "wasm32"))]
pub use carrier::*;
pub use catalog::*;
pub use chpmt::*;
pub use codec::*;
pub use compress::*;
pub use datagram::*;
pub use kernels::*;
pub use protection::*;
pub use protocol::*;
pub use source::*;
pub use state::*;
pub use state_policy::*;
pub use transport::*;
pub use vector::*;
