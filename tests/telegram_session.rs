//! Unit tests for the session store's bookkeeping.
//!
//! What is here needs no database: the dirty flag, and the two questions a
//! restart asks before it dials Telegram. The load/flush round trip through
//! MariaDB is `tests/telegram_session.rs`, because a store that serialises
//! correctly into nothing is not a store.

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

/// A session nobody has touched has nothing to write. Pinned because the flush is
/// on a timer: a session that reports itself dirty from birth writes a row every
/// interval forever, for the lifetime of the pod.
#[test]
fn a_fresh_session_is_not_dirty() {
    assert!(!fresh().is_dirty());
}

/// ⚠ **The short-circuit that keeps the flush honest.** `auto_cache_peers` hands
/// over every peer in every response, so during a backfill the SAME peers arrive
/// thousands of times. Marking the session dirty for a peer already known in full
/// would mean the flag is always set and "flush when something changed" quietly
/// becomes "flush on every tick".
#[tokio::test]
async fn recaching_a_known_peer_changes_nothing() {
    let session = fresh();

    session.cache_peer(&a_user(777, true)).await.unwrap();
    assert!(
        session.take_dirty(),
        "a peer the session had never seen is a change"
    );
}

/// A peer that arrives knowing MORE than the cached copy is a change, even though
/// its id is already there. The pair with the test above is the point: the
/// question is whether anything was learned, not whether the id is new.
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

/// The two questions a restart asks before it touches the network: am I logged
/// in, and who am I. Answering them from the stored session is what lets a pod
/// tell "never logged in" from "logged in, Telegram unreachable" — and `self_id`
/// is what `map::map_message` needs to attribute a DM at all.
#[tokio::test]
async fn a_session_knows_whether_and_who_it_is_logged_in_as() {
    let session = fresh();
    assert!(!session.is_authorized().unwrap());
    assert_eq!(session.self_id().unwrap(), None);

    // A peer that is not us proves nothing about being logged in.
    session.cache_peer(&a_user(4242, false)).await.unwrap();
    assert!(!session.is_authorized().unwrap());
    assert_eq!(session.self_id().unwrap(), None);

    session.cache_peer(&a_user(777, true)).await.unwrap();
    assert!(session.is_authorized().unwrap());
    assert_eq!(session.self_id().unwrap(), Some(777));
}

/// Channel update state is MERGED per channel rather than appended, or the list
/// grows without bound and the reader of it takes whichever copy it finds first.
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

/// ⚠ The divergence from `MemorySession`, tested through the trait method
/// `grammers` actually calls. `Client::stream_updates` asks
/// `peer(PeerId::self_user())` to decide whether it must fetch a pristine update
/// state; a storage that answers `None` there makes a signed-in account start
/// from scratch every time the stream opens, which is a gap nothing reports.
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
