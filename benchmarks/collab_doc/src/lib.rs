//! Collaborative-doc benchmark harness for pulzZ predictive-memory transport.
//!
//! Doc model: a plain-text document is a `Vec<String>` (lines). Each line is
//! an `ItemId`. The server holds the authoritative doc state; edits produce
//! records that sync the change to the client.
//!
//! Two sync paths:
//! - **Naive**: every edit produces an `ExactState` record with the full new
//!   line content. Wire bytes = HEADER_LEN + payload + AUTH_TAG_LEN per edit.
//! - **Predictive**: for append-to-line and similar-replace edits, send a
//!   `PredictiveConfirm`/`PredictiveCorrect` record with only the delta.
//!   For insert/delete, fall back to naive.
//!
//! See `DESIGN.md` for the full design.

// Benchmark crate — allow style lints that would be fixed in production code.
#![allow(clippy::manual_strip)]

use shared_protocol::{ItemId, Record, RecordHeader, RecordType, RecordFlags, StreamId,
                     EpochId, SeqNo, CodecMode, PROTOCOL_VERSION, AUTH_TAG_LEN,
                     SourceKind};

/// A single edit in a collaborative-doc trace.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "edit_type", rename_all = "lowercase")]
pub enum Edit {
    /// Insert a new line at `line_index` with `content`.
    Insert { line_index: usize, content: String },
    /// Delete the line at `line_index`.
    Delete { line_index: usize },
    /// Replace the line at `line_index` with `content`.
    Replace { line_index: usize, content: String },
    /// Append `appended` text to the line at `line_index`.
    Append { line_index: usize, appended: String },
}

/// The authoritative doc state on the server side.
#[derive(Debug, Clone, Default)]
pub struct DocState {
    /// Line contents indexed by ItemId (line number).
    pub lines: Vec<String>,
}

impl DocState {
    /// Initialize from a chunk of text: split into lines.
    pub fn from_text(text: &str) -> Self {
        let lines: Vec<String> = text.lines().map(|l| l.to_string()).collect();
        Self { lines }
    }

    /// Apply an edit, returning the ItemId it affected.
    pub fn apply(&mut self, edit: &Edit) -> ItemId {
        match edit {
            Edit::Insert { line_index, content } => {
                let idx = (*line_index).min(self.lines.len());
                self.lines.insert(idx, content.clone());
                ItemId(idx as u64)
            }
            Edit::Delete { line_index } => {
                if *line_index < self.lines.len() {
                    self.lines.remove(*line_index);
                }
                ItemId(*line_index as u64)
            }
            Edit::Replace { line_index, content } => {
                if *line_index < self.lines.len() {
                    self.lines[*line_index] = content.clone();
                }
                ItemId(*line_index as u64)
            }
            Edit::Append { line_index, appended } => {
                if *line_index < self.lines.len() {
                    self.lines[*line_index].push_str(appended);
                }
                ItemId(*line_index as u64)
            }
        }
    }

    /// Get the content of a line.
    pub fn line(&self, index: usize) -> Option<&str> {
        self.lines.get(index).map(|s| s.as_str())
    }
}

/// Build a naive ExactState record for an edit.
///
/// For insert/replace: sends the full new line content.
/// For append: sends the full new line (old + appended) — the naive server
///   doesn't know the client has the old content, so it resends everything.
/// For delete: sends a MemoryRetire record.
pub fn naive_record_for_edit(edit: &Edit, old_content: Option<&str>) -> Record {
    match edit {
        Edit::Insert { line_index, content }
        | Edit::Replace { line_index, content } => {
            build_exact_state_record(ItemId(*line_index as u64), content.as_bytes())
        }
        Edit::Append { line_index, appended } => {
            // Naive: send the FULL new line (old + appended). The naive server
            // doesn't track what the client already has — it resends the
            // complete line every time.
            let full_new = match old_content {
                Some(old) => format!("{old}{appended}"),
                None => appended.clone(),
            };
            build_exact_state_record(ItemId(*line_index as u64), full_new.as_bytes())
        }
        Edit::Delete { line_index } => {
            build_memory_retire_record(ItemId(*line_index as u64))
        }
    }
}

/// Build a predictive record for an edit.
///
/// For append: sends only the appended bytes (delta) as a PredictiveConfirm.
/// For replace where new content starts with old: sends only the suffix.
/// For insert/delete: falls back to naive (full content).
pub fn predictive_record_for_edit(edit: &Edit, old_content: Option<&str>) -> Record {
    match edit {
        Edit::Append { line_index, appended } => {
            // Predictive: send only the delta (appended bytes).
            // The client predicts "line unchanged" and applies the delta.
            build_predictive_confirm_record(ItemId(*line_index as u64), appended.as_bytes())
        }
        Edit::Replace { line_index, content } => {
            if let Some(old) = old_content {
                if content.starts_with(old) {
                    // New content = old + suffix. Send only the suffix.
                    let suffix = &content[old.len()..];
                    return build_predictive_confirm_record(
                        ItemId(*line_index as u64),
                        suffix.as_bytes(),
                    );
                }
            }
            // Fallback: full content (naive).
            build_exact_state_record(ItemId(*line_index as u64), content.as_bytes())
        }
        Edit::Insert { line_index, content } => {
            // Insert is a new item — no prediction possible. Naive.
            build_exact_state_record(ItemId(*line_index as u64), content.as_bytes())
        }
        Edit::Delete { line_index } => {
            // Delete is a retire — no prediction. Naive.
            build_memory_retire_record(ItemId(*line_index as u64))
        }
    }
}

/// Calculate wire bytes for a record (the bytes that would go on the wire).
pub fn wire_bytes(record: &Record) -> usize {
    record.to_bytes().len()
}

/// Sum wire bytes for a list of records.
pub fn total_wire_bytes(records: &[Record]) -> usize {
    records.iter().map(wire_bytes).sum()
}

// --- Internal record builders ---

fn build_exact_state_record(item_id: ItemId, payload: &[u8]) -> Record {
    // DirectExact codec: payload = [source_kind_byte, ...exact_bytes]
    let mut encoded = Vec::with_capacity(1 + payload.len());
    encoded.push(SourceKind::Text as u8);
    encoded.extend_from_slice(payload);
    Record {
        header: RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(1),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::DirectExact,
            flags: RecordFlags::empty(),
            item_id,
            payload_len: encoded.len() as u32,
        },
        payload: encoded,
        auth_tag: [0u8; AUTH_TAG_LEN],
    }
}

fn build_memory_retire_record(item_id: ItemId) -> Record {
    Record {
        header: RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(1),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::MemoryRetire,
            codec_mode: CodecMode::None,
            flags: RecordFlags::empty(),
            item_id,
            payload_len: 0,
        },
        payload: Vec::new(),
        auth_tag: [0u8; AUTH_TAG_LEN],
    }
}

fn build_predictive_confirm_record(item_id: ItemId, delta: &[u8]) -> Record {
    // PredictiveConfirm: the client predicts "no change" and the server
    // confirms with a delta. The payload is just the delta bytes.
    Record {
        header: RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(1),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::PredictiveConfirm,
            codec_mode: CodecMode::PredictedExact,
            flags: RecordFlags::empty(),
            item_id,
            payload_len: delta.len() as u32,
        },
        payload: delta.to_vec(),
        auth_tag: [0u8; AUTH_TAG_LEN],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn naive_record_for_append_has_content() {
        let edit = Edit::Append {
            line_index: 0,
            appended: " world".to_string(),
        };
        let record = naive_record_for_edit(&edit, Some("hello"));
        assert_eq!(record.header.record_type, RecordType::ExactState);
        assert!(record.payload.len() > 1); // source kind byte + content
    }

    #[test]
    fn predictive_record_for_append_is_smaller() {
        let edit = Edit::Append {
            line_index: 0,
            appended: " world".to_string(),
        };
        let naive = naive_record_for_edit(&edit, Some("hello"));
        let predictive = predictive_record_for_edit(&edit, Some("hello"));
        assert!(wire_bytes(&predictive) < wire_bytes(&naive));
    }

    #[test]
    fn predictive_falls_back_for_unrelated_replace() {
        let edit = Edit::Replace {
            line_index: 0,
            content: "completely new".to_string(),
        };
        let old = "old line";
        let predictive = predictive_record_for_edit(&edit, Some(old));
        // Should fall back to ExactState (naive) since new doesn't start with old.
        assert_eq!(predictive.header.record_type, RecordType::ExactState);
    }
}
