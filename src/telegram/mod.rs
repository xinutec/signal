//! Telegram, the fourth origin.
//!
//! Split the way the Signal side is split, and for the same reason: everything
//! that can be got wrong about a message is in `map`, which is pure and tested;
//! everything that needs the network or the database is in the binary that uses
//! it (`src/bin/telegram.rs`). `session` is the one piece that is neither — it is
//! how a login survives a restart.
use grammers_client::peer::Peer;

pub mod map;
pub mod session;

/// What a conversation IS, as the archive files it.
///
/// ⚠ Not derivable from the id. Telegram gives a supergroup and a broadcast
/// channel the same id space, so [`map::PeerSpace`] cannot tell them apart —
/// which would file every supergroup as a broadcast and put it behind whatever
/// filter a reader uses to keep announcement feeds out of their conversations.
/// The distinction lives on the peer, as a flag, and `grammers` has already read
/// it: it presents a megagroup as a `Peer::Group` and only a broadcast as a
/// `Peer::Channel`. So this is derived from the peer and stored, rather than
/// recomputed from a number later.
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

/// The name the archive files a peer under.
///
/// `Peer::name` gives a user's FIRST name only, which is not enough to tell two
/// Simons apart in a conversation list. A group or channel has one title and
/// `name` is it.
pub fn peer_name(peer: &Peer) -> Option<String> {
    match peer {
        Peer::User(u) => {
            let full = u.full_name();
            (!full.is_empty()).then_some(full)
        }
        other => other.name().map(str::to_owned),
    }
}
