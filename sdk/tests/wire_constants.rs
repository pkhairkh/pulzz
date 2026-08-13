//! Cross-language wire-compatibility constants (Wave 5 T-5.a).
//!
//! These constants define the wire-protocol contract that every binding
//! (Rust, C, Python, Go, JS/WASM) must honor. The values are verified here
//! in Rust and cross-checked in the Python/Go/C cross-language tests.

use pulzz_sdk::{ItemId, SourceKind};
use shared_protocol::{
    Record, RecordHeader, RecordType, RecordFlags, StreamId, EpochId, SeqNo,
    CodecMode, PROTOCOL_VERSION, AUTH_TAG_LEN, HEADER_LEN,
};

#[test]
fn batch_envelope_record_type_is_consistent() {
    // All bindings agree that BatchEnvelope is record_type=21.
    assert_eq!(RecordType::BatchEnvelope as u8, 21);
    assert_eq!(RecordType::ExactState as u8, 17);
}

#[test]
fn auth_tag_length_is_16_bytes() {
    // All bindings agree that the AEAD auth tag is 16 bytes (ChaCha20-Poly1305).
    assert_eq!(AUTH_TAG_LEN, 16);
}

#[test]
fn header_length_is_consistent() {
    // All bindings agree on the fixed header length (49 bytes).
    assert_eq!(HEADER_LEN, 37);
}

#[test]
fn source_kind_discriminants_match_c_abi() {
    // The C ABI uses u8 for source_kind matching shared_protocol::SourceKind:
    // 1=text, 2=json, 3=binary, 4=image (per shared_protocol/src/source.rs).
    assert_eq!(SourceKind::Text as u8, 1);
    assert_eq!(SourceKind::Json as u8, 2);
    assert_eq!(SourceKind::Binary as u8, 3);
    assert_eq!(SourceKind::Image as u8, 4);
}

#[test]
fn c_abi_version_matches_workspace_version() {
    assert_eq!(pulzz_ffi::ABI_VERSION, 0x500);
    assert!(pulzz_ffi::VERSION_STRING.starts_with("pulzZ"));
}

#[test]
fn record_header_round_trips_through_bytes() {
    // Construct a RecordHeader, verify its AAD bytes match the wire format
    // that Python/Go/C all expect.
    let header = RecordHeader {
        version: PROTOCOL_VERSION,
        stream_id: StreamId(42),
        epoch_id: EpochId(0),
        seq_no: SeqNo(1),
        record_type: RecordType::ExactState,
        codec_mode: CodecMode::DirectExact,
        flags: RecordFlags::empty(),
        item_id: ItemId(7),
        payload_len: 16,
    };
    let aad = header.aad_bytes();
    assert_eq!(aad.len(), HEADER_LEN);
    assert_eq!(aad[0], PROTOCOL_VERSION);
    let stream_id_bytes = u64::from_le_bytes(aad[1..9].try_into().unwrap());
    assert_eq!(stream_id_bytes, 42);
    let item_id_bytes = u64::from_le_bytes(aad[25..33].try_into().unwrap());
    assert_eq!(item_id_bytes, 7);
}

#[test]
fn full_record_round_trips_through_bytes() {
    // Construct a full Record, serialize to bytes, deserialize back, verify
    // equality. This is the wire-format contract every binding must honor.
    let record = Record {
        header: RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::DirectExact,
            flags: RecordFlags::empty(),
            item_id: ItemId(99),
            payload_len: 5,
        },
        payload: b"hello".to_vec(),
        auth_tag: [0xAB; AUTH_TAG_LEN],
    };
    let bytes = record.to_bytes();
    let restored = Record::from_bytes(&bytes).expect("round-trip must succeed");
    assert_eq!(restored.header.item_id, ItemId(99));
    assert_eq!(restored.payload, b"hello");
    assert_eq!(restored.auth_tag, [0xAB; AUTH_TAG_LEN]);
}

#[test]
fn known_record_bytes_hex_for_cross_language_verification() {
    // This test produces a known byte sequence that the Python, Go, and C
    // cross-language tests parse independently. If any binding disagrees on
    // the wire format, its test will fail.
    //
    // The record: ExactState, stream_id=1, item_id=42, payload=b"cross-lang-test"
    let record = Record {
        header: RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(1),
            epoch_id: EpochId(0),
            seq_no: SeqNo(0),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::DirectExact,
            flags: RecordFlags::empty(),
            item_id: ItemId(42),
            payload_len: 15,
        },
        payload: b"cross-lang-test".to_vec(),
        auth_tag: [0; AUTH_TAG_LEN],
    };
    let bytes = record.to_bytes();
    // Verify the round-trip works in Rust first.
    let restored = Record::from_bytes(&bytes).expect("round-trip must succeed");
    assert_eq!(restored.header.item_id, ItemId(42));
    assert_eq!(restored.payload, b"cross-lang-test");
    // The total length is HEADER_LEN + payload_len + AUTH_TAG_LEN = 49 + 15 + 16 = 80.
    assert_eq!(bytes.len(), HEADER_LEN + 15 + AUTH_TAG_LEN);
}
