// Shared protocol surface for CHPMT predictive-memory transport.
// Active CHPMT routing, predictive caches, and predictor state are cue/object-native; native exact-state objects are the only active payload form.

pub mod bootstrap;
#[cfg(not(target_arch = "wasm32"))]
pub mod carrier;
pub mod catalog;
pub mod chpmt;
pub mod codec;
pub mod datagram;
pub mod kernels;
pub mod protection;
pub mod protocol;
pub mod source;
pub mod state;
pub mod state_policy;
pub mod transport;
#[doc = "Byte-latent helpers for native exact-state payload encoding and decode."]
pub mod vector;

pub use bootstrap::*;
#[cfg(not(target_arch = "wasm32"))]
pub use carrier::*;
pub use catalog::*;
pub use chpmt::*;
pub use codec::*;
pub use datagram::*;
pub use kernels::*;
pub use protection::*;
pub use protocol::*;
pub use source::*;
pub use state::*;
pub use state_policy::*;
pub use transport::*;
pub use vector::*;
