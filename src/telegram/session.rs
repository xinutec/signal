//! The MTProto session, kept in MariaDB.
//!
//! A Telegram session is four things: the authorisation key per datacentre, which
//! datacentre is home, a cache of the peers seen so far, and how far through the
//! update sequence the account has been read. `grammers` asks for them through
//! the [`Session`] trait and does not care where they live.
//!
//! ⚠ **This is a credential, not a cache.** The row holds a logged-in session:
//! reading it is reading the account. It lives in the archive's own database
//! because it is exactly as sensitive as the messages it authorises, it is
//! covered by their backup, and re-logging-in is rate-limited by Telegram in
//! hours rather than seconds — so "just delete it and sign in again" is not a
//! recovery plan. See the v20 migration in `db.rs`.
//!
//! ⚠ **Why not the storage `grammers` ships.** `grammers-session`'s default
//! feature is a local SQLite file via `libsql`, which drags `bindgen`, `clang-sys`
//! and a C toolchain into an image that has never needed one. The trait is nine
//! methods; the dependency was the expensive way to get them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use anyhow::{Context, Result};
use grammers_session::types::{
    ChannelState, DcOption, PeerId, PeerInfo, UpdateState, UpdatesState,
};
use grammers_session::{BoxFuture, Session, SessionData};
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlPool;

/// The session as one JSON document.
///
/// Deliberately NOT `SessionData` itself: that type does not derive `Serialize`
/// (only its four components do), and its peer cache is a map keyed by a
/// bit-packed id — which JSON cannot express as an object key. Storing the peers
/// as a list instead is also exactly the shape `SessionData::import_to` wants, so
/// loading is a replay of what was saved rather than a second representation.
#[derive(Serialize, Deserialize)]
struct Persisted {
    home_dc: i32,
    dc_options: Vec<DcOption>,
    peers: Vec<PeerInfo>,
    updates: UpdatesState,
}

/// A [`Session`] whose state is a row in `telegram_session`.
pub struct DbSession {
    data: Mutex<SessionData>,
    /// Set by every mutating method, cleared by [`DbSession::flush`].
    ///
    /// The trait's setters are called on the hot path — `auto_cache_peers` caches
    /// every peer of every response, so a backfill touches this thousands of
    /// times a minute — and a database write per call would make the session the
    /// slowest part of ingesting. A flag plus a periodic flush costs one write
    /// per interval in which anything changed, and nothing at all in one where
    /// nothing did.
    dirty: AtomicBool,
}

#[derive(Debug)]
pub enum SessionError {
    /// The lock was poisoned, which means another thread panicked while holding
    /// the session. Surfaced rather than papered over: the state it was mutating
    /// is of unknown shape, and continuing would persist that.
    Poisoned,
}

impl std::error::Error for SessionError {}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Poisoned => write!(f, "the Telegram session lock is poisoned"),
        }
    }
}

impl DbSession {
    /// A session with nothing in it: the state a first run starts from.
    ///
    /// Public because the tests live in `tests/` and exercise the public API — the
    /// repository's rule, and it costs nothing here: this IS what [`Self::load`]
    /// returns when the table holds no row, so a test built on it is testing the
    /// real starting state rather than a fixture resembling it.
    pub fn empty() -> Self {
        Self {
            data: Mutex::new(SessionData::default()),
            dirty: AtomicBool::new(false),
        }
    }

    /// Whether anything has changed since the last flush.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::SeqCst)
    }

    /// Claim the change: report whether anything had changed, and mark it handled.
    ///
    /// This is the first half of [`Self::flush`] rather than a hook bolted on for
    /// the tests — the order matters and is stated there. It is public because
    /// whether this session thinks it has something to write is the one property of
    /// the store with no observable consequence until much later: a session that
    /// reports itself dirty forever writes a row every interval for the life of the
    /// pod, and nothing else would ever say so.
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::SeqCst)
    }

    /// Read the session from the database, or start a fresh one if there is no
    /// row yet.
    ///
    /// A fresh session is not an error and not a warning: it is what the first
    /// run looks like, and `telegram login` is what turns it into an authorised
    /// one.
    pub async fn load(pool: &MySqlPool) -> Result<Self> {
        let stored: Option<String> =
            sqlx::query_scalar("SELECT data FROM telegram_session WHERE single_row = 1")
                .fetch_optional(pool)
                .await
                .context("reading telegram_session")?;
        let data = match stored {
            None => SessionData::default(),
            Some(json) => {
                let p: Persisted = serde_json::from_str(&json)
                    // ⚠ Not recoverable by starting fresh. A session that fails
                    // to parse still EXISTS at Telegram's end, and silently
                    // replacing it with a default would log in again — spending a
                    // flood wait and orphaning the update state, which is how an
                    // archive develops a gap it cannot see.
                    .context("telegram_session holds a document this build cannot read")?;
                let mut data = SessionData {
                    home_dc: p.home_dc,
                    dc_options: p.dc_options.into_iter().map(|o| (o.id, o)).collect(),
                    peer_infos: HashMap::new(),
                    updates_state: p.updates,
                };
                for peer in p.peers {
                    data.peer_infos.insert(peer.id(), peer);
                }
                data
            }
        };
        Ok(Self {
            data: Mutex::new(data),
            dirty: AtomicBool::new(false),
        })
    }

    /// Write the session back if anything has changed since the last flush.
    ///
    /// Returns whether it wrote. The clear happens BEFORE the write rather than
    /// after: a mutation that lands during the write then leaves the flag set and
    /// is picked up by the next flush, where the other order would clear a change
    /// it had not persisted.
    pub async fn flush(&self, pool: &MySqlPool) -> Result<bool> {
        if !self.take_dirty() {
            return Ok(false);
        }
        let json = {
            let data = self.data().map_err(anyhow::Error::new)?;
            serde_json::to_string(&Persisted {
                home_dc: data.home_dc,
                dc_options: data.dc_options.values().cloned().collect(),
                peers: data.peer_infos.values().cloned().collect(),
                updates: data.updates_state.clone(),
            })
            .context("serialising the Telegram session")?
        };
        sqlx::query(
            "INSERT INTO telegram_session (single_row, data) VALUES (1, ?)
             ON DUPLICATE KEY UPDATE data = VALUES(data)",
        )
        .bind(&json)
        .execute(pool)
        .await
        .context("writing telegram_session")?;
        Ok(true)
    }

    /// Whether this session has a logged-in user bound to it.
    ///
    /// Asked of the stored state rather than over the network, so a restart can
    /// tell "never logged in" from "logged in, Telegram unreachable" without
    /// dialling anybody.
    pub fn is_authorized(&self) -> Result<bool> {
        Ok(self.self_id()?.is_some())
    }

    /// The logged-in account's own user id, which `map::map_message` needs to
    /// attribute a DM that names no sender.
    pub fn self_id(&self) -> Result<Option<i64>> {
        let data = self.data().map_err(anyhow::Error::new)?;
        Ok(find_self(&data).and_then(|p| match p {
            PeerInfo::User { id, .. } => Some(*id),
            _ => None,
        }))
    }

    fn data(&self) -> Result<MutexGuard<'_, SessionData>, SessionError> {
        self.data.lock().map_err(|_| SessionError::Poisoned)
    }

    fn touch(&self) {
        self.dirty.store(true, Ordering::SeqCst);
    }
}

/// The cached peer that is the logged-in account, if it has been seen.
///
/// A free function because both [`DbSession::self_id`] and `Session::peer` need
/// it and they hold the lock differently.
fn find_self(data: &SessionData) -> Option<&PeerInfo> {
    data.peer_infos.values().find(|p| {
        matches!(
            p,
            PeerInfo::User {
                is_self: Some(true),
                ..
            }
        )
    })
}

/// The trait, delegating to the in-memory state and marking it dirty.
///
/// Mostly this is `grammers_session::storages::MemorySession` plus persistence,
/// and staying close to it is deliberate: a divergence in how channel state is
/// merged would be a bug with no symptom until an update stream skipped
/// something.
///
/// ⚠ **Two places diverge on purpose, and both are marked below.** `peer` answers
/// the self-user sentinel, which `MemorySession` does not; and `cache_peer`
/// decides "changed" by comparing rather than by trusting `extend_info`'s return
/// value. The first is required of a session that persists (`SqliteSession` does
/// it too), the second only of one that flushes on a timer. Neither is a
/// preference.
impl Session for DbSession {
    type Error = SessionError;

    fn home_dc_id(&self) -> Result<i32, SessionError> {
        Ok(self.data()?.home_dc)
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            self.data()?.home_dc = dc_id;
            self.touch();
            Ok(())
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, SessionError> {
        Ok(self.data()?.dc_options.get(&dc_id).cloned())
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), SessionError>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            self.data()?.dc_options.insert(dc_option.id, dc_option);
            self.touch();
            Ok(())
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, SessionError>> {
        Box::pin(async move {
            let data = self.data()?;
            // ⚠ **`PeerId::self_user()` IS NOT A KEY IN THIS MAP, and answering
            // it is not optional.** It is a sentinel outside the id ranges, and
            // `PeerInfo::id()` never produces it — so a storage that only looks
            // the argument up returns `None` for "am I logged in?" no matter how
            // logged in it is. `Client::stream_updates` asks exactly this to
            // decide whether it needs to fetch a pristine update state, so
            // getting it wrong makes a signed-in account behave like a fresh one
            // every time the stream starts.
            //
            // The `grammers` storage this one is otherwise modelled on —
            // `MemorySession` — does NOT handle it, which is what its own
            // documentation means by "should only be used in very few select
            // cases". `SqliteSession` does, by looking for the self flag, and
            // that is what this mirrors.
            Ok(if peer == PeerId::self_user() {
                find_self(&data).cloned()
            } else {
                data.peer_infos.get(&peer).cloned()
            })
        })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), SessionError>> {
        let peer = peer.clone();
        Box::pin(async move {
            let mut data = self.data()?;
            match data.peer_infos.get_mut(&peer.id()) {
                Some(existing) => {
                    // ⚠ **`extend_info`'s bool is NOT "did anything change".** It
                    // reports whether the two infos matched in type and id, so it
                    // is `true` for every restatement of a peer already known in
                    // full — and `auto_cache_peers` restates every peer of every
                    // response, thousands of times during a backfill. Reading it
                    // as a change signal turns "flush when something changed"
                    // into "flush on every tick", forever, and the symptom is a
                    // write rate rather than a wrong answer. Compare instead.
                    let before = existing.clone();
                    existing.extend_info(&peer);
                    if *existing != before {
                        self.touch();
                    }
                }
                None => {
                    data.peer_infos.insert(peer.id(), peer);
                    self.touch();
                }
            }
            Ok(())
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, SessionError>> {
        Box::pin(async move { Ok(self.data()?.updates_state.clone()) })
    }

    fn set_update_state(&self, update: UpdateState) -> BoxFuture<'_, Result<(), SessionError>> {
        Box::pin(async move {
            let mut data = self.data()?;
            match update {
                UpdateState::All(state) => data.updates_state = state,
                UpdateState::Primary { pts, date, seq } => {
                    data.updates_state.pts = pts;
                    data.updates_state.date = date;
                    data.updates_state.seq = seq;
                }
                UpdateState::Secondary { qts } => data.updates_state.qts = qts,
                UpdateState::Channel { id, pts } => {
                    match data
                        .updates_state
                        .channels
                        .iter_mut()
                        .find(|channel| channel.id == id)
                    {
                        Some(channel) => channel.pts = pts,
                        None => data.updates_state.channels.push(ChannelState { id, pts }),
                    }
                }
            }
            self.touch();
            Ok(())
        })
    }
}
