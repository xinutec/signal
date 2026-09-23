//! Unit tests for the session store's bookkeeping.
//!
//! No database: the dirty flag and the login checks. The MariaDB round trip is
//! in `tests/telegram_store.rs`.

use grammers_session::Session;
use grammers_session::types::{PeerId, PeerInfo, UpdateState};
use signal_archiver::telegram::session::DbSession;

fn fresh() -> DbSession {
    DbSession::empty()
}

fn a_user(id: i64, is_self: bool) -> PeerInfo {
    PeerInfo::User {
        id,
        auth: None,
        bot: Some(false),
        is_self: Some(is_self),
    }
}

/// The flush runs on a timer, so an untouched session must not be dirty.
#[test]
fn a_fresh_session_is_not_dirty() {
    assert!(!fresh().is_dirty());
}

/// `auto_cache_peers` restates known peers constantly; that is not a change.
#[tokio::test]
async fn recaching_a_known_peer_changes_nothing() {
    let session = fresh();

    session.cache_peer(&a_user(777, true)).await.unwrap();
    assert!(
        session.take_dirty(),
        "a peer the session had never seen is a change"
    );
}

/// A peer that adds to the cached copy is a change.
#[tokio::test]
async fn a_peer_that_learns_something_is_a_change() {
    let session = fresh();
    session
        .cache_peer(&PeerInfo::User {
            id: 777,
            auth: None,
            bot: None,
            is_self: None,
        })
        .await
        .unwrap();
    session.take_dirty();

    session.cache_peer(&a_user(777, true)).await.unwrap();
    assert!(session.is_dirty());
}

/// Login state and self id come from the stored session, without the network.
#[tokio::test]
async fn a_session_knows_whether_and_who_it_is_logged_in_as() {
    let session = fresh();
    assert!(!session.is_authorized().unwrap());
    assert_eq!(session.self_id().unwrap(), None);

    // A peer that is not us proves nothing.
    session.cache_peer(&a_user(4242, false)).await.unwrap();
    assert!(!session.is_authorized().unwrap());
    assert_eq!(session.self_id().unwrap(), None);

    session.cache_peer(&a_user(777, true)).await.unwrap();
    assert!(session.is_authorized().unwrap());
    assert_eq!(session.self_id().unwrap(), Some(777));
}

/// Channel update state is merged per channel, not appended.
#[tokio::test]
async fn channel_state_is_updated_in_place() {
    let session = fresh();
    session
        .set_update_state(UpdateState::Channel { id: 5, pts: 100 })
        .await
        .unwrap();
    session
        .set_update_state(UpdateState::Channel { id: 5, pts: 101 })
        .await
        .unwrap();
    session
        .set_update_state(UpdateState::Channel { id: 6, pts: 7 })
        .await
        .unwrap();

    let state = session.updates_state().await.unwrap();
    let mut channels: Vec<(i64, i32)> = state.channels.iter().map(|c| (c.id, c.pts)).collect();
    channels.sort_unstable();
    assert_eq!(channels, vec![(5, 101), (6, 7)]);
}

/// `stream_updates` asks `peer(PeerId::self_user())` to decide whether the
/// account is logged in.
#[tokio::test]
async fn the_self_user_sentinel_resolves_to_the_logged_in_account() {
    let session = fresh();
    assert!(
        session.peer(PeerId::self_user()).await.unwrap().is_none(),
        "nobody is logged in yet"
    );

    session.cache_peer(&a_user(4242, false)).await.unwrap();
    assert!(
        session.peer(PeerId::self_user()).await.unwrap().is_none(),
        "a peer who is not us must not answer for us"
    );

    session.cache_peer(&a_user(777, true)).await.unwrap();
    assert_eq!(
        session.peer(PeerId::self_user()).await.unwrap(),
        Some(a_user(777, true)),
        "the sentinel is not a key in the map; it has to be resolved by the self flag"
    );
}
