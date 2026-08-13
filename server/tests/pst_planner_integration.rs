//! Wave 8 Task 8-b: PST planner integration tests.

use std::sync::atomic::{AtomicU64, Ordering};

use rand::rngs::StdRng;
use rand::SeedableRng;
use shared_protocol::protection::classic_ref1_pair_from_rng;
use shared_protocol::protection::StreamDirection;
use shared_protocol::protocol::StreamId;
use shared_protocol::source::ExactStateMaterial;
use shared_protocol::{ItemId, SourceKind};
use server::{ServerEvent, ServerSession};

static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(9000);

fn next_stream_id() -> StreamId {
    StreamId(NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed))
}

#[test]
fn pst_is_trained_on_every_emission() {
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(88001);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);

    for i in 1..=20 {
        let payload = format!("payload number {} with some content", i);
        let _ = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: ExactStateMaterial::new(SourceKind::Text, payload.into_bytes()),
            })
            .expect("emit must succeed");
    }

    let pst_tokens = session.state().route_pst.total_tokens();
    assert!(
        pst_tokens >= 20,
        "PST must be trained on every emission; observed {} tokens, expected >= 20",
        pst_tokens
    );
}

#[test]
fn pst_context_window_is_bounded() {
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(88002);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);

    for i in 1..=50 {
        let payload = format!("payload {} ", i);
        let _ = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: ExactStateMaterial::new(SourceKind::Text, payload.into_bytes()),
            })
            .expect("emit must succeed");
    }

    let ctx_len = session.state().pst_context.len();
    assert!(
        ctx_len <= 16,
        "PST context window must be bounded at 16; was {}",
        ctx_len
    );
}

#[test]
fn ucb1_bandit_records_outcomes() {
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(88003);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);

    for i in 1..=10 {
        let payload = format!("payload {} ", i);
        let _ = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: ExactStateMaterial::new(SourceKind::Text, payload.into_bytes()),
            })
            .expect("emit must succeed");
    }

    use shared_protocol::chpmt::ControllerRouteFamily;
    let direct_state_pulls = session.state().route_bandit.pulls_for(ControllerRouteFamily::DirectState);
    assert!(
        direct_state_pulls > 0,
        "UCB1 bandit must record DirectState outcomes; got {}",
        direct_state_pulls
    );
}

#[test]
fn pst_predicts_after_sufficient_training() {
    let stream_id = next_stream_id();
    let mut rng = StdRng::seed_from_u64(88004);
    let (sender, _receiver) =
        classic_ref1_pair_from_rng(stream_id, StreamDirection::ServerToClient, &mut rng);

    let mut session = ServerSession::new(sender);

    for i in 1..=30 {
        let payload = format!("payload {} ", i);
        let _ = session
            .emit_event(ServerEvent::Insert {
                item_id: ItemId(i),
                block: ExactStateMaterial::new(SourceKind::Text, payload.into_bytes()),
            })
            .expect("emit must succeed");
    }

    let ctx = session.state().pst_context.clone();
    if !ctx.is_empty() {
        let pred = session.state().route_pst.predict(&ctx);
        assert!(
            pred.is_some(),
            "PST must predict after 30 emissions with non-empty context"
        );
    }
}
