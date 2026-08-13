//! WebSocket server-side bind+accept integration tests (Wave 1 T-1.b).
//!
//! Verifies that `PulzzServer::bind` opens a real `TcpListener` and that
//! `accept()` performs the WebSocket upgrade on incoming connections.

#![cfg(not(target_arch = "wasm32"))]

use pulzz_sdk::PulzzServer;

#[tokio::test]
async fn pulzz_server_binds_to_real_tcp_socket() {
    let server = PulzzServer::bind("127.0.0.1:0")
        .await
        .expect("bind must succeed");
    assert!(
        server.session_acceptor_is_some(),
        "server should be in network mode after bind"
    );
    let addr = server
        .local_addr()
        .expect("local_addr must succeed after bind");
    assert_eq!(addr.ip(), std::net::Ipv4Addr::new(127, 0, 0, 1));
    assert!(addr.port() > 0, "OS-assigned port must be non-zero");
}

#[tokio::test]
async fn pulzz_server_accept_performs_websocket_upgrade() {
    // Bind a real server on an ephemeral port.
    let mut server = PulzzServer::bind("127.0.0.1:0")
        .await
        .expect("bind must succeed");
    let addr = server.local_addr().expect("local_addr must succeed");

    // Spawn the accept task. It will perform the WS upgrade when a client
    // connects, then wait for the pulzZ bootstrap (which the raw WS client
    // below won't send — so accept() will hang on the bootstrap read).
    // We don't wait for accept() to complete; we just need it running so
    // the WS upgrade happens when the client connects.
    let accept_task = tokio::spawn(async move {
        // This will hang waiting for the bootstrap message. We don't care
        // about the result — the test just needs the WS upgrade to happen.
        let _ = server.accept().await;
    });

    // Connect a raw WebSocket client (no pulzZ bootstrap) to the server.
    // If connect_async succeeds, it means the server's accept() loop
    // accepted the TCP connection AND completed the WebSocket HTTP
    // upgrade handshake (tokio_tungstenite::accept_async). This proves
    // the WS carrier path works end-to-end at the transport layer.
    let ws_url = format!("ws://{addr}");
    let (ws_stream, _response) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("raw WS client must connect — if this fails, the server's \
                 WS upgrade (tokio_tungstenite::accept_async) is broken");

    // The WS connection is up. Read briefly to let the server progress.
    use futures_util::StreamExt;
    let (_write, mut read) = ws_stream.split();
    let _first_msg = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        read.next(),
    )
    .await;

    // Cancel the accept task (it's hanging on bootstrap read, which is
    // expected — the raw client didn't send client_hello).
    accept_task.abort();
}
