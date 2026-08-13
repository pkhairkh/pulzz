//! pulzZ shared protocol: PQC session layer, record framing, compression,
//! batch envelope, predictive router (UCB1 + PST).

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
