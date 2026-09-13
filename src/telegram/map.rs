//! Telegram's wire types → archive rows. **Pure**: no client, no network, no
//! clock.
//!
//! This is `parse.rs`'s role for the other origin, and it exists for the same
//! reason. Everything that can be got wrong about a Telegram message is got
//! wrong HERE — which peer a message belongs to, who sent one that names no
//! sender, whether a reaction is an emoji or a document id — and none of it
//! needs an account to test. `grammers_client::types::Message` exposes its
//! `raw: tl::enums::Message`, so the ingester hands that over and keeps the
//! network on its own side of this boundary.
//!
//! What is NOT here is anything that needs the peer map: a sender's *name* is
//! looked up from the peers a response carried, so this layer reports the
//! sender's id and the caller names it.

use grammers_tl_types as tl;

/// Which of Telegram's three id spaces a peer came from.
///
/// ⚠ **This is NOT what a conversation IS**, and conflating the two would put
/// every supergroup in the archive under "channel". Telegram models a supergroup
/// and a broadcast channel with the same `channel` id space and tells them apart
/// by a flag on the peer — so the space says where the number came from, and
/// [`crate::telegram::ConvKind`] says what the thing is. `map` only ever needs
/// the space, because the one inference it makes is about DMs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSpace {
    /// A user: a one-to-one conversation.
    User,
    /// A basic group (`chat`), the small kind that predates supergroups.
    Chat,
    /// A channel id — a broadcast channel OR a supergroup.
    Channel,
}

/// Whether a row is something somebody said or something that happened.
///
/// The same distinction `irc_messages.kind` draws between a line and a join, and
/// it matters for the same reason: a reader that wants the conversation wants
/// one of these and not the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgKind {
    Message,
    Service,
}

impl MsgKind {
    /// The `telegram_messages.kind` enum value.
    pub fn as_str(self) -> &'static str {
        match self {
            MsgKind::Message => "message",
            MsgKind::Service => "service",
        }
    }
}

/// What KIND of media a message carried. Not the media itself: this archive
/// stores Telegram bytes nowhere yet (see the v16 migration), so a photo is
/// recorded as having been a photo — with, since v21, its SIZE and mime beside it,
/// which is what makes the decision about downloading a measured one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Photo,
    Document,
    Sticker,
    Video,
    Audio,
    Voice,
    GeoPoint,
    Contact,
    Poll,
    Dice,
    Game,
    Invoice,
    WebPage,
    Story,
    Giveaway,
    /// A media variant this build does not name. Recorded rather than dropped:
    /// "there was something here" is true and useful, and a reader that sees
    /// this knows to come back rather than believing the message was bare text.
    Other,
}

impl MediaKind {
    /// The `telegram_messages.media_kind` value. Kept to 32 characters by the
    /// column, which every variant here is comfortably inside.
    pub fn as_str(self) -> &'static str {
        match self {
            MediaKind::Photo => "photo",
            MediaKind::Document => "document",
            MediaKind::Sticker => "sticker",
            MediaKind::Video => "video",
            MediaKind::Audio => "audio",
            MediaKind::Voice => "voice",
            MediaKind::GeoPoint => "geo",
            MediaKind::Contact => "contact",
            MediaKind::Poll => "poll",
            MediaKind::Dice => "dice",
            MediaKind::Game => "game",
            MediaKind::Invoice => "invoice",
            MediaKind::WebPage => "webpage",
            MediaKind::Story => "story",
            MediaKind::Giveaway => "giveaway",
            MediaKind::Other => "other",
        }
    }
}

/// One reaction bucket on a message: how many chose it, and which it was.
///
/// Telegram reports reactions aggregated, so this is a count and not a list of
/// people — the same limit `gchat_reactions` has and for the same reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaction {
    /// A unicode emoticon, when the reaction was one.
    pub emoji: Option<String>,
    /// A custom emoji's document id. Mutually exclusive with `emoji`: a custom
    /// reaction has no characters to render, so the id is what there is.
    pub custom_emoji_id: Option<i64>,
    pub cnt: i32,
    /// Whether the logged-in account is one of the counted.
    pub chosen: bool,
}

/// A message as the archive stores it, less the parts that need a peer map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub conversation_id: i64,
    /// Which id space `conversation_id` came from. Not the conversation's KIND —
    /// see [`PeerSpace`].
    pub peer_space: PeerSpace,
    pub msg_id: i32,
    /// Unix seconds, UTC. Telegram's own unit, unconverted.
    pub sent_at: i64,
    /// Normalised the same way `conversation_id` is, so a sender in a group and
    /// the DM with that same person are one id.
    pub sender_id: Option<i64>,
    pub is_outgoing: bool,
    pub kind: MsgKind,
    pub text: Option<String>,
    pub media_kind: Option<MediaKind>,
    /// What a download would cost, from the message rather than from a request.
    pub media_size: Option<i64>,
    pub media_mime: Option<String>,
    /// Unix seconds of the most recent edit, or `None` for a message never
    /// edited. This is Telegram's `edit_date` and is the only ordering there is
    /// for an edit chain — there is no revision number.
    pub edited_at: Option<i64>,
    pub reply_to_msg_id: Option<i32>,
    /// Who a forward came from, when the header carried a NAME. A forward from a
    /// peer whose name is only in the peer map is left to the caller.
    pub fwd_from_name: Option<String>,
    pub reactions: Vec<Reaction>,
}

/// Fold one of Telegram's three peer spaces into the single signed space the
/// archive keys conversations by.
///
/// ⚠ The raw `user_id`, `chat_id` and `channel_id` are each unique only WITHIN
/// their own space, so a raw number is not a conversation. This is the
/// normalisation every Telegram client uses (and the one the Bot API exposes),
/// so an id from this archive is the id somebody else's tooling would print.
pub fn normalise_peer(peer: &tl::enums::Peer) -> (i64, PeerSpace) {
    match peer {
        tl::enums::Peer::User(u) => (u.user_id, PeerSpace::User),
        tl::enums::Peer::Chat(c) => (-c.chat_id, PeerSpace::Chat),
        // -100 prepended to the channel id, which is what the constant is: a
        // channel 1234 becomes -1001234, and no channel can collide with a basic
        // group because a group's id is far below the offset.
        tl::enums::Peer::Channel(c) => (CHANNEL_ID_OFFSET - c.channel_id, PeerSpace::Channel),
    }
}

/// The offset that separates channel ids from basic-group ids in the folded
/// space. Not a magic number: it is "-100" written in front of the id.
const CHANNEL_ID_OFFSET: i64 = -1_000_000_000_000;

/// Map a wire message onto a row, or `None` for one that carries nothing to
/// store.
///
/// `self_id` is the logged-in account's own user id, and it is a parameter
/// rather than a lookup because of the rule below — which is the single most
/// wrong-able thing in this file.
///
/// ⚠ **A message in a DM usually names no sender.** `from_id` is omitted when
/// Telegram considers it implied, which in a one-to-one chat it always is: the
/// sender is either you or the person you are talking to, and `out` says which.
/// Read literally, every incoming DM in this archive would have a NULL sender
/// and every outgoing one too — which is how a conversation loses the only
/// column that says who was speaking. In a group or channel `from_id` is
/// present when there is an author at all, so no such inference is made or
/// needed there: an anonymous channel post genuinely has no sender.
pub fn map_message(msg: &tl::enums::Message, self_id: i64) -> Option<Row> {
    match msg {
        // `messageEmpty` is a hole: a message id Telegram will acknowledge and
        // has no content for, which is what a deleted message looks like when it
        // is fetched by id. Nothing to store, and storing a blank row would put
        // an empty bubble in a conversation.
        tl::enums::Message::Empty(_) => None,
        tl::enums::Message::Message(m) => {
            let (conversation_id, peer_space) = normalise_peer(&m.peer_id);
            let media = m.media.as_ref().map(media_of);
            Some(Row {
                conversation_id,
                peer_space,
                msg_id: m.id,
                sent_at: i64::from(m.date),
                sender_id: sender_of(
                    m.from_id.as_ref(),
                    conversation_id,
                    peer_space,
                    m.out,
                    self_id,
                ),
                is_outgoing: m.out,
                kind: MsgKind::Message,
                // An empty body is stored as NULL rather than "": a message with
                // only a photo said nothing, and the viewer already distinguishes
                // "no body" from "a body that is blank" (its copied-log rule).
                text: non_empty(&m.message),
                media_kind: media.as_ref().map(|f| f.kind),
                media_size: media.as_ref().and_then(|f| f.size),
                media_mime: media.as_ref().and_then(|f| f.mime.clone()),
                edited_at: m.edit_date.map(i64::from),
                reply_to_msg_id: m.reply_to.as_ref().and_then(reply_target),
                fwd_from_name: m.fwd_from.as_ref().and_then(fwd_name),
                reactions: m.reactions.as_ref().map(reactions).unwrap_or_default(),
            })
        }
        tl::enums::Message::Service(m) => {
            let (conversation_id, peer_space) = normalise_peer(&m.peer_id);
            Some(Row {
                conversation_id,
                peer_space,
                msg_id: m.id,
                sent_at: i64::from(m.date),
                sender_id: sender_of(
                    m.from_id.as_ref(),
                    conversation_id,
                    peer_space,
                    m.out,
                    self_id,
                ),
                is_outgoing: m.out,
                kind: MsgKind::Service,
                // ⚠ DERIVED, not Telegram's words. A service message carries an
                // action rather than a sentence — the words a Telegram client
                // shows are that client's, in the reader's language. This label
                // is ours, in English, and is marked `service` so nobody mistakes
                // it for something a person typed.
                text: Some(describe_action(&m.action).to_owned()),
                media_kind: None,
                media_size: None,
                media_mime: None,
                edited_at: None,
                reply_to_msg_id: m.reply_to.as_ref().and_then(reply_target),
                fwd_from_name: None,
                reactions: m.reactions.as_ref().map(reactions).unwrap_or_default(),
            })
        }
    }
}

/// Who sent it — see the ⚠ on [`map_message`] for why a DM needs the inference.
fn sender_of(
    from_id: Option<&tl::enums::Peer>,
    conversation_id: i64,
    space: PeerSpace,
    out: bool,
    self_id: i64,
) -> Option<i64> {
    match from_id {
        Some(peer) => Some(normalise_peer(peer).0),
        None if space == PeerSpace::User => Some(if out { self_id } else { conversation_id }),
        None => None,
    }
}

fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_owned())
}

fn reply_target(header: &tl::enums::MessageReplyHeader) -> Option<i32> {
    match header {
        tl::enums::MessageReplyHeader::Header(h) => h.reply_to_msg_id,
        // A reply to a story is a reply to something this archive does not hold,
        // so there is no message id to point at.
        tl::enums::MessageReplyHeader::MessageReplyStoryHeader(_) => None,
    }
}

fn fwd_name(header: &tl::enums::MessageFwdHeader) -> Option<String> {
    match header {
        tl::enums::MessageFwdHeader::Header(h) => h.from_name.clone(),
    }
}

fn reactions(r: &tl::enums::MessageReactions) -> Vec<Reaction> {
    let tl::enums::MessageReactions::Reactions(r) = r;
    r.results
        .iter()
        .map(|count| {
            let tl::enums::ReactionCount::Count(c) = count;
            let (emoji, custom_emoji_id) = match &c.reaction {
                tl::enums::Reaction::Emoji(e) => (Some(e.emoticon.clone()), None),
                tl::enums::Reaction::CustomEmoji(e) => (None, Some(e.document_id)),
                // `reactionEmpty` and paid reactions have no emoji and no
                // document: the count is all there is to keep.
                _ => (None, None),
            };
            Reaction {
                emoji,
                custom_emoji_id,
                cnt: c.count,
                chosen: c.chosen_order.is_some(),
            }
        })
        .collect()
}

/// What a message's media IS, how big it is, and what type it holds.
///
/// ⚠ **Through `grammers_client::media::Media` rather than the raw enum, and that
/// is what makes the finer answers possible.** A sticker, a video, a voice note
/// and a PDF are all `messageMediaDocument` on the wire; which one it is lives in
/// the document's ATTRIBUTES. `Media::from_raw` reads them, and it needs no client
/// — so this layer stays pure and gains `sticker` and a mime type it could not
/// otherwise see.
///
/// ⚠ **`size()` AND `mime` COST NO NETWORK REQUEST.** They come out of the message
/// itself, which is what lets the archive record what a download would cost before
/// anything is downloaded.
fn media_of(media: &tl::enums::MessageMedia) -> MediaFacts {
    use grammers_client::media::Media as M;
    let Some(m) = M::from_raw(media.clone()) else {
        // `messageMediaEmpty` and media this build of grammers does not model. It
        // was there, and that is all this can say.
        return MediaFacts {
            kind: MediaKind::Other,
            size: None,
            mime: None,
        };
    };
    let size = m.size().and_then(|s| i64::try_from(s).ok());
    let (kind, mime) = match &m {
        // Telegram photos are compressed JPEG — that is what the variant MEANS, so
        // the mime is knowable without asking.
        M::Photo(_) => (MediaKind::Photo, Some("image/jpeg".to_owned())),
        M::Sticker(s) => (
            MediaKind::Sticker,
            s.document.mime_type().map(str::to_owned),
        ),
        M::Document(d) => (
            // The mime is the honest finer label: a `video/mp4` and a
            // `application/pdf` are both documents, and the column that separates
            // them is the one that says so rather than a taxonomy of ours.
            match d.mime_type() {
                Some(t) if t.starts_with("video/") => MediaKind::Video,
                Some(t) if t.starts_with("audio/") => MediaKind::Audio,
                _ => MediaKind::Document,
            },
            d.mime_type().map(str::to_owned),
        ),
        M::Contact(_) => (MediaKind::Contact, None),
        M::Poll(_) => (MediaKind::Poll, None),
        M::Geo(_) | M::GeoLive(_) | M::Venue(_) => (MediaKind::GeoPoint, None),
        M::Dice(_) => (MediaKind::Dice, None),
        M::WebPage(_) => (MediaKind::WebPage, None),
        // `Media` is `#[non_exhaustive]`: a variant grammers adds later lands here
        // rather than stopping the build, and is recorded as having been something.
        _ => (MediaKind::Other, None),
    };
    MediaFacts { kind, size, mime }
}

/// What [`media_of`] found. A struct because the three travel together and a
/// tuple of two `Option`s at the call site is the shape nobody reads correctly.
pub struct MediaFacts {
    pub kind: MediaKind,
    /// Bytes a download would take, when Telegram said. `None` for media that is
    /// not a file at all — a poll has no size.
    pub size: Option<i64>,
    pub mime: Option<String>,
}

/// An English label for a service action.
///
/// Deliberately short and deliberately OURS: see the ⚠ in [`map_message`]. The
/// list covers what a private archive actually contains; anything else is
/// recorded as having happened rather than dropped, because a conversation with
/// silent holes in it is worse than one with a vague line.
fn describe_action(action: &tl::enums::MessageAction) -> &'static str {
    use tl::enums::MessageAction as A;
    match action {
        A::ChatCreate(_) => "created the group",
        A::ChatEditTitle(_) => "changed the title",
        A::ChatEditPhoto(_) => "changed the photo",
        A::ChatDeletePhoto => "removed the photo",
        A::ChatAddUser(_) => "added a member",
        A::ChatDeleteUser(_) => "removed a member",
        A::ChatJoinedByLink(_) => "joined by link",
        A::ChannelCreate(_) => "created the channel",
        A::ChatMigrateTo(_) => "became a supergroup",
        A::ChannelMigrateFrom(_) => "was migrated from a group",
        A::PinMessage => "pinned a message",
        A::HistoryClear => "cleared the history",
        A::PhoneCall(_) => "a call",
        A::ContactSignUp => "joined Telegram",
        A::SetMessagesTtl(_) => "changed the message timer",
        A::GroupCall(_) => "a group call",
        A::SetChatTheme(_) => "changed the theme",
        _ => "an event",
    }
}
