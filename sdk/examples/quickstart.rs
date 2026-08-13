//! pulzZ SDK quickstart: minimal client + server round-trip in Rust.
//!
//! Run with:
//! ```sh
//! cargo run --example quickstart -p pulzz-sdk
//! ```

use pulzz_sdk::{
    CarrierKind, ClientConfig, CompressionConfig, PulzzClient, PulzzServer, SecurityProfile,
    classic_pair_for_test,
};
use shared_protocol::{ItemId, SourceKind, source::ExactStateMaterial};
use server::ServerEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("pulzZ SDK quickstart — in-memory round-trip");

    // 1. Build a classic_ref1 protector pair (no network, no PQC bootstrap)
    let stream_id = shared_protocol::StreamId(42);
    let (sender, receiver) = classic_pair_for_test(stream_id);

    // 2. Construct SDK server + client wrapping the pair
    let config = ClientConfig {
        security: SecurityProfile::ClassicRef1,
        carrier: CarrierKind::WebSocket,
        compression: CompressionConfig::default(),
        batch_size: None,
        timeout: std::time::Duration::from_secs(5),
    };
    let mut server = PulzzServer::from_protector(sender, config.clone());
    let mut client = PulzzClient::from_session(
        client::ClientSession::new(receiver),
        config,
    );

    // 3. Server emits an Insert event for item 1
    let payload = b"hello pulzZ".to_vec();
    let material = ExactStateMaterial::new(SourceKind::Text, payload);
    let record = server.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: material,
    })?;
    println!("  server emitted record (type={:?})", record.header.record_type);

    // 4. Client receives the record (in-memory push + recv)
    client.push_record(record);
    let received = client.recv().await?.expect("client should receive a record");
    println!("  client received record for item {}", received.header.item_id.0);

    // 5. Verify the item is in the client cache
    let cached = client
        .session()
        .state()
        .cache_entry(ItemId(1))
        .expect("item 1 must be cached");
    println!(
        "  client cached item 1 ({} bytes)",
        cached.object.exact_bytes.len()
    );

    println!("quickstart complete");
    Ok(())
}
