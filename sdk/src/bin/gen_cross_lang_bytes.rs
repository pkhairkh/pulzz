//! Generate the known wire bytes for cross-language tests.
//! Run: cargo run --bin gen_cross_lang_bytes -p pulzz-sdk
use shared_protocol::{
    ItemId, Record, RecordHeader, RecordType, RecordFlags, StreamId, EpochId, SeqNo,
    CodecMode, PROTOCOL_VERSION, AUTH_TAG_LEN,
};

fn main() {
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
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    println!("BYTES_HEX={}", hex);
    println!("LEN={}", bytes.len());

    // Also generate a second record with different values.
    let record2 = Record {
        header: RecordHeader {
            version: PROTOCOL_VERSION,
            stream_id: StreamId(7),
            epoch_id: EpochId(0),
            seq_no: SeqNo(1),
            record_type: RecordType::ExactState,
            codec_mode: CodecMode::DirectExact,
            flags: RecordFlags::empty(),
            item_id: ItemId(99),
            payload_len: 5,
        },
        payload: b"hello".to_vec(),
        auth_tag: [0xAB; AUTH_TAG_LEN],
    };
    let bytes2 = record2.to_bytes();
    let hex2: String = bytes2.iter().map(|b| format!("{:02x}", b)).collect();
    println!("BYTES_HEX_2={}", hex2);
    println!("LEN_2={}", bytes2.len());
}
