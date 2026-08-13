//! pulzZ Rust quickstart — cross-language example.
//!
//! Run with:
//! ```sh
//! cargo run --example rust_quickstart -p pulzz-sdk
//! ```

use pulzz_sdk::{
    CarrierKind, ClientConfig, CompressionConfig, PulzzClient, PulzzServer, SecurityProfile,
    classic_pair_for_test,
};
use shared_protocol::{ItemId, SourceKind, source::ExactStateMaterial};
use server::ServerEvent;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("pulzZ Rust quickstart — cross-language example");

    let stream_id = shared_protocol::StreamId(7);
    let (sender, receiver) = classic_pair_for_test(stream_id);

    let config = ClientConfig {
        security: SecurityProfile::ClassicRef1,
        carrier: CarrierKind::WebSocket,
        compression: CompressionConfig::default(),
        batch_size: None,
        timeout: std::time::Duration::from_secs(5),
    };
    let mut server = PulzzServer::from_protector(sender, config.clone());
    let mut client = PulzzClient::from_session(client::ClientSession::new(receiver), config);

    // Server emits a single item
    let payload = b"cross-language example".to_vec();
    let material = ExactStateMaterial::new(SourceKind::Text, payload);
    let record = server.emit_event(ServerEvent::Insert {
        item_id: ItemId(1),
        block: material,
    })?;
    println!("  server emitted record (type={:?})", record.header.record_type);

    // Client receives it (in-memory)
    client.push_record(record);
    let received = client.recv().await?.expect("client should receive a record");
    println!(
        "  client received record for item {} ({} bytes payload)",
        received.header.item_id.0,
        received.payload.len()
    );

    println!("pulzZ Rust quickstart complete");
    Ok(())
}
