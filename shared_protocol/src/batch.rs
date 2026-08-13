//! Wave 13 Task 13-a: Batch envelope format.
//!
//! Defines the wire format for `BatchEnvelope` records — a single AEAD-
//! protected transport frame that wraps N items, amortizing the per-item
//! overhead (AEAD tag 16B + record header 32B + transport envelope 16B +
//! source descriptor 30B ≈ 94B) across all items in the batch.
//!
//! For a batch of 100 items at ~90B each (original = 9000B):
//! - Per-item framing: 100 × (90 + 94) = 18400B wire
//! - Batched: 9000B payload + 100 × 14B (item header) + 94B envelope = 10494B wire
//! - Savings: ~43%
//!
//! For 1000 items at ~90B each (original = 90000B):
//! - Per-item: 184000B wire
//! - Batched: 90000B + 1000 × 14B + 94B = 104094B wire
//! - Savings: ~43%
//!
//! Wire format (postcard-encoded inside the AEAD-protected payload):
//! ```text
//!   [item_count: u16]
//!   [item_0: BatchItem]
//!   [item_1: BatchItem]
//!   ...
//!   [item_N-1: BatchItem]
//! ```
//! where each `BatchItem` is:
//! ```text
//!   [item_id: u64]
//!   [source_kind: u8]
//!   [payload_len: u32]
//!   [payload: [u8; payload_len]]
//! ```

use serde::{Deserialize, Serialize};

use crate::{ItemId, SourceKind};

/// Maximum number of items in a single batch. Bounded to prevent unbounded
/// memory allocation on decode. 1024 items × ~100B each = ~100KB max batch
/// payload, well within the 2MB transport frame limit.
pub const MAX_BATCH_ITEMS: usize = 1024;

/// A single item in a batch envelope. Each item carries its own item_id,
/// source_kind, and (already-compressed) payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchItem {
    pub item_id: ItemId,
    pub source_kind: SourceKind,
    /// Already-compressed payload (output of compress_exact_bytes).
    pub payload: Vec<u8>,
}

/// A batch envelope wrapping N items. Encoded as the `Record.payload` of a
/// `RecordType::BatchEnvelope` record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchEnvelope {
    pub items: Vec<BatchItem>,
}

impl BatchEnvelope {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, item_id: ItemId, source_kind: SourceKind, payload: Vec<u8>) {
        self.items.push(BatchItem {
            item_id,
            source_kind,
            payload,
        });
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Total bytes of all payloads (compressed). Excludes per-item framing
    /// overhead. Used for benchmark reporting.
    pub fn total_payload_bytes(&self) -> usize {
        self.items.iter().map(|i| i.payload.len()).sum()
    }

    /// Encode to postcard bytes for wire transmission.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.items.len() > MAX_BATCH_ITEMS {
            return Err(EncodeError::TooManyItems {
                actual: self.items.len(),
                max: MAX_BATCH_ITEMS,
            });
        }
        postcard::to_allocvec(self).map_err(|e| EncodeError::Postcard(e.to_string()))
    }

    /// Decode from postcard bytes received on the wire.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let envelope: Self =
            postcard::from_bytes(bytes).map_err(|e| DecodeError::Postcard(e.to_string()))?;
        if envelope.items.len() > MAX_BATCH_ITEMS {
            return Err(DecodeError::TooManyItems {
                actual: envelope.items.len(),
                max: MAX_BATCH_ITEMS,
            });
        }
        Ok(envelope)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("too many items: {actual} > {max}")]
    TooManyItems { actual: usize, max: usize },
    #[error("postcard encode error: {0}")]
    Postcard(String),
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("too many items: {actual} > {max}")]
    TooManyItems { actual: usize, max: usize },
    #[error("postcard decode error: {0}")]
    Postcard(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_batch_encodes_and_decodes() {
        let batch = BatchEnvelope::new();
        let encoded = batch.encode().expect("encode must succeed");
        let decoded = BatchEnvelope::decode(&encoded).expect("decode must succeed");
        assert_eq!(batch, decoded);
    }

    #[test]
    fn single_item_batch_round_trips() {
        let mut batch = BatchEnvelope::new();
        batch.push(ItemId(42), SourceKind::Text, b"hello world".to_vec());
        let encoded = batch.encode().expect("encode must succeed");
        let decoded = BatchEnvelope::decode(&encoded).expect("decode must succeed");
        assert_eq!(batch, decoded);
        assert_eq!(decoded.items[0].item_id, ItemId(42));
        assert_eq!(decoded.items[0].payload, b"hello world");
    }

    #[test]
    fn multi_item_batch_round_trips() {
        let mut batch = BatchEnvelope::new();
        for i in 1..=10 {
            batch.push(
                ItemId(i),
                if i % 2 == 0 { SourceKind::Text } else { SourceKind::Json },
                format!("payload {}", i).into_bytes(),
            );
        }
        let encoded = batch.encode().expect("encode must succeed");
        let decoded = BatchEnvelope::decode(&encoded).expect("decode must succeed");
        assert_eq!(batch, decoded);
        assert_eq!(decoded.len(), 10);
        // Total payload bytes = sum of "payload N" lengths for N=1..10.
        // "payload 1".."payload 9" = 9 bytes each (9 × 9 = 81),
        // "payload 10" = 10 bytes. Total = 91.
        let expected: usize = (1..=10).map(|i| format!("payload {}", i).len()).sum();
        assert_eq!(decoded.total_payload_bytes(), expected);
    }

    #[test]
    fn batch_with_empty_payload_round_trips() {
        let mut batch = BatchEnvelope::new();
        batch.push(ItemId(1), SourceKind::Binary, Vec::new());
        let encoded = batch.encode().expect("encode must succeed");
        let decoded = BatchEnvelope::decode(&encoded).expect("decode must succeed");
        assert_eq!(batch, decoded);
        assert!(decoded.items[0].payload.is_empty());
    }

    #[test]
    fn batch_over_max_items_rejected() {
        let mut batch = BatchEnvelope::new();
        for i in 0..(MAX_BATCH_ITEMS + 1) {
            batch.push(ItemId(i as u64), SourceKind::Text, vec![0u8; 10]);
        }
        assert!(batch.encode().is_err());
    }

    #[test]
    fn batch_wire_format_is_compact() {
        // Verify that the encoded batch is smaller than N individual records.
        // 10 items × ~20B payload each = 200B total payload.
        // Individual records: 10 × (20 + 94) = 1140B wire.
        // Batched: 200B + 10 × 14B item header + ~10B envelope overhead ≈ 350B.
        let mut batch = BatchEnvelope::new();
        for i in 1..=10 {
            batch.push(ItemId(i), SourceKind::Text, format!("payload-{}", i).into_bytes());
        }
        let encoded = batch.encode().expect("encode must succeed");
        // The encoded batch must be smaller than 10 individual records would be.
        // Conservative bound: 10 × (10 + 94) = 1040B for individual records.
        assert!(
            encoded.len() < 1040,
            "batch must be smaller than individual records; got {}B",
            encoded.len()
        );
    }
}
