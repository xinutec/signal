//! The MTProto session, kept in MariaDB: auth keys per datacentre, the home
//! datacentre, the peer cache, and the update state. It is a credential; see the
//! v20 migration in `db.rs`.
//!
//! Not `grammers-session`'s SQLite storage, whose `libsql` dependency needs a C
//! toolchain in the image.

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

/// The session as one JSON document. Not `SessionData` itself, which does not
/// derive `Serialize` and keys its peers by an id JSON cannot use as a key.
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
    /// Set by every mutating method, cleared by [`DbSession::flush`]. The setters
    /// run for every peer of every response, too often to write each time.
    dirty: AtomicBool,
}

#[derive(Debug)]
pub enum SessionError {
    /// Another thread panicked holding the session; its state is unknown.
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
    /// A session with nothing in it, as [`Self::load`] returns with no row.
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

    /// Report whether anything had changed, and clear the flag. The first half of
    /// [`Self::flush`].
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::SeqCst)
    }

    /// Read the session from the database, or start empty if there is no row.
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
                    // Fatal: starting fresh would force a rate-limited re-login.
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
    /// Returns whether it wrote. The flag is cleared before the write, so a
    /// mutation during the write is picked up by the next flush.
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

    /// Whether this session has a logged-in user, from stored state alone.
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

/// `grammers_session::storages::MemorySession` plus persistence, differing only
/// where marked in `peer` and `cache_peer`.
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
            // `PeerId::self_user()` is a sentinel no cached peer has as its id.
            // `stream_updates` asks it to decide whether the account is logged
            // in, so it is answered from the self flag, as `SqliteSession` does.
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
                    // `extend_info`'s bool reports a type/id match, not a change.
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
