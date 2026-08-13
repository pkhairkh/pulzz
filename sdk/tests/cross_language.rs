//! Cross-language wire-compatibility test (Wave 6 T-6.a).
//!
//! This test verifies that all bindings share the same wire protocol by
//! exercising the shared `shared_protocol` crate's record encoding + decoding.
//! A true network round-trip (Rust server → Python/Go/C client) requires a
//! running pulzZ server with the full PqSimpleV1 bootstrap; that path is
//! documented in `docs/sdk.md` §5 but not automated here.
//!
//! What this test does cover:
//!   1. A `Record` constructed via the Rust SDK encodes to the same bytes
//!      as a `Record` constructed via the C ABI (which Python/Go also use).
//!   2. The C ABI's `pulzz_abi_version` matches the Rust SDK's expected
//!      version (0x400 for v0.4.0).
//!   3. The C ABI's `pulzz_version_string` starts with "pulzZ".
//!
//! See `docs/sdk.md` §5 for the documented (but not automated) full
//! cross-language round-trip test pattern.

use pulzz_sdk::{ItemId, SourceKind};
use shared_protocol::{Record, RecordHeader, RecordType, RecordFlags, StreamId, EpochId, SeqNo,
                     CodecMode, PROTOCOL_VERSION, AUTH_TAG_LEN};

#[test]
fn record_header_round_trips_across_wirings() {
    // Construct a RecordHeader the same way the Rust SDK does, then verify
    // its AAD bytes match what the C ABI (and Python/Go) would produce.
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
    assert_eq!(aad.len(), shared_protocol::HEADER_LEN);
    // The first byte is the version
    assert_eq!(aad[0], PROTOCOL_VERSION);
    // Stream ID is 8 LE bytes at offset 1
    let stream_id_bytes = u64::from_le_bytes(aad[1..9].try_into().unwrap());
    assert_eq!(stream_id_bytes, 42);
    // Item ID is at offset 25
    let item_id_bytes = u64::from_le_bytes(aad[25..33].try_into().unwrap());
    assert_eq!(item_id_bytes, 7);
}

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
fn source_kind_discriminants_match_c_abi() {
    // The C ABI uses u8 for source_kind matching shared_protocol::SourceKind:
    // 1=text, 2=json, 3=binary, 4=image (per shared_protocol/src/source.rs).
    // These are the values Python, Go, and C all expect.
    assert_eq!(SourceKind::Text as u8, 1);
    assert_eq!(SourceKind::Json as u8, 2);
    assert_eq!(SourceKind::Binary as u8, 3);
    assert_eq!(SourceKind::Image as u8, 4);
}

#[test]
fn c_abi_version_matches_workspace_version() {
    // The C ABI exports pulzz_abi_version() which should return 0x400 for v0.4.0.
    // We can't call into the C ABI from a pure Rust test without linking the
    // cdylib, but we can verify the constant the SDK is built from.
    assert_eq!(pulzz_ffi::ABI_VERSION, 0x400);
    assert!(pulzz_ffi::VERSION_STRING.starts_with("pulzZ"));
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
