//! `BatchBuilder` — ergonomic helper for constructing a batch of items to
//! emit in a single `BatchEnvelope` record. Mirrors `docs/SDK_PROPOSAL.md`
//! §5.3.
//!
//! Each item carries an `ItemId`, a `SourceKind`, and a raw payload. The
//! `build()` method returns a `BuiltBatch` that can be passed to
//! `PulzzServer::emit_batch` or `PulzzClient::send_batch_raw`.

use shared_protocol::{ExactStateMaterial, ItemId, SourceKind};

/// A built batch ready for emission. Wraps the inner `Vec` so we can attach
/// methods like `wire_bytes_estimate` later without breaking the API.
#[derive(Debug, Clone)]
pub struct BuiltBatch {
    pub items: Vec<(ItemId, ExactStateMaterial)>,
}

impl BuiltBatch {
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    /// Estimated wire size if emitted per-item (header + AEAD tag per item).
    pub fn estimated_per_item_wire_bytes(&self) -> usize {
        self.items
            .iter()
            .map(|(_, m)| m.exact_bytes.len() + shared_protocol::HEADER_LEN + shared_protocol::AUTH_TAG_LEN)
            .sum()
    }
    /// Sum of raw payload bytes.
    pub fn raw_payload_bytes(&self) -> usize {
        self.items.iter().map(|(_, m)| m.exact_bytes.len()).sum()
    }
}

impl<I> FromIterator<I> for BuiltBatch
where
    I: Into<(ItemId, ExactStateMaterial)>,
{
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        Self {
            items: iter.into_iter().map(Into::into).collect(),
        }
    }
}

/// Builder for `BuiltBatch`.
#[derive(Debug, Default)]
pub struct BatchBuilder {
    items: Vec<(ItemId, ExactStateMaterial)>,
}

impl BatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an item with an explicit `SourceKind`.
    pub fn item(mut self, id: ItemId, kind: SourceKind, payload: &[u8]) -> Self {
        self.items
            .push((id, ExactStateMaterial::new(kind, payload.to_vec())));
        self
    }

    /// Add an item using a pre-built `ExactStateMaterial` (preserves any
    /// attached source descriptor metadata).
    pub fn material(mut self, id: ItemId, material: ExactStateMaterial) -> Self {
        self.items.push((id, material));
        self
    }

    /// Finalize into a `BuiltBatch`.
    pub fn build(self) -> BuiltBatch {
        BuiltBatch { items: self.items }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_builder_collects_items() {
        let batch = BatchBuilder::new()
            .item(ItemId(1), SourceKind::Text, b"hello")
            .item(ItemId(2), SourceKind::Json, b"{}")
            .item(ItemId(3), SourceKind::Binary, &[0u8, 1, 2])
            .build();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.raw_payload_bytes(), 5 + 2 + 3);
    }
}
