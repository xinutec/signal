//! Telegram. `map` is the pure wire-to-row mapping; `session` persists the
//! login; the network and database work is in `src/bin/telegram.rs`.
use grammers_client::peer::Peer;

pub mod map;
pub mod session;

/// What a conversation is. Not derivable from the id, since supergroups and
/// broadcasts share an id space; grammers reads the peer's flag and presents a
/// supergroup as `Peer::Group`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvKind {
    /// One-to-one with a person.
    Dm,
    /// A group of people, whether a basic group or a supergroup.
    Group,
    /// A broadcast: an audience rather than a conversation.
    Channel,
}

impl ConvKind {
    /// The `telegram_conversations.kind` enum value.
    pub fn as_str(self) -> &'static str {
        match self {
            ConvKind::Dm => "dm",
            ConvKind::Group => "group",
            ConvKind::Channel => "channel",
        }
    }

    pub fn from_peer(peer: &Peer) -> Self {
        match peer {
            Peer::User(_) => ConvKind::Dm,
            Peer::Group(_) => ConvKind::Group,
            Peer::Channel(_) => ConvKind::Channel,
        }
    }
}

/// The name the archive files a peer under: a user's full name, since
/// `Peer::name` gives only the first.
pub fn peer_name(peer: &Peer) -> Option<String> {
    match peer {
        Peer::User(u) => {
            let full = u.full_name();
            (!full.is_empty()).then_some(full)
        }
        other => other.name().map(str::to_owned),
    }
}
