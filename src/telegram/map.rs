//! Telegram's wire types → archive rows. Pure: no client, no network, no clock,
//! so all of it is testable without an account.
//!
//! Anything needing the peer map stays with the caller: this reports a sender's
//! id, and the caller names it.

use grammers_tl_types as tl;

/// Which of Telegram's three id spaces a peer came from. Not the conversation's
/// kind: supergroups and broadcast channels share the `channel` space; see
/// [`crate::telegram::ConvKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSpace {
    /// A user: a one-to-one conversation.
    User,
    /// A basic group (`chat`), the small kind that predates supergroups.
    Chat,
    /// A broadcast channel or a supergroup.
    Channel,
}

/// Whether a row is something somebody said or something that happened.
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

/// What kind of media a message carried.
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
    /// A media variant this build does not name.
    Other,
}

impl MediaKind {
    /// The `telegram_messages.media_kind` value (at most 32 characters).
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

/// One reaction bucket on a message: which reaction, and how many chose it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaction {
    /// A unicode emoticon, when the reaction was one.
    pub emoji: Option<String>,
    /// A custom emoji's document id. Mutually exclusive with `emoji`.
    pub custom_emoji_id: Option<i64>,
    pub cnt: i32,
    /// Whether the logged-in account is one of the counted.
    pub chosen: bool,
}

/// Who reacted, and when. The source list can be truncated; see
/// [`Reactions::complete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReactionAuthor {
    /// Normalised the way `conversation_id` and `sender_id` are.
    pub peer_id: i64,
    pub emoji: Option<String>,
    pub custom_emoji_id: Option<i64>,
    /// Unix seconds: when they reacted, per Telegram.
    pub reacted_at: i64,
}

/// What a message's reactions amount to: the tally, and who — if Telegram said.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reactions {
    /// One bucket per distinct reaction, with its count. Authoritative.
    pub counts: Vec<Reaction>,
    pub authors: Vec<ReactionAuthor>,
    /// Whether `authors` names every reactor the counts add up to. Only a
    /// complete list may retract anybody.
    pub complete: bool,
}

/// One formatted span: bold, a link, a mention, a spoiler. Offsets and lengths
/// are UTF-16 code units and cannot index a Rust string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entity {
    /// The TL constructor's name, less the `messageEntity` prefix: `bold`,
    /// `textUrl`, `spoiler`.
    pub kind: &'static str,
    pub offset_utf16: i32,
    pub length_utf16: i32,
    /// A `textUrl`'s target, which the visible text does not contain.
    pub url: Option<String>,
    /// Who a `mentionName` meant.
    pub user_id: Option<i64>,
    /// A `pre` block's syntax, when it declares one.
    pub language: Option<String>,
    /// A `customEmoji`'s document id.
    pub document_id: Option<i64>,
}

/// A call, as the service message describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub call_id: i64,
    /// `None` for an unanswered call.
    pub duration_s: Option<i32>,
    /// `busy`, `hangup`, `missed` or `disconnect`.
    pub reason: Option<&'static str>,
    pub video: bool,
}

/// A message as the archive stores it, less the parts that need a peer map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub conversation_id: i64,
    /// Which id space `conversation_id` came from.
    pub peer_space: PeerSpace,
    pub msg_id: i32,
    /// Unix seconds.
    pub sent_at: i64,
    /// Normalised like `conversation_id`, so a group sender and the DM with them
    /// share an id.
    pub sender_id: Option<i64>,
    pub is_outgoing: bool,
    pub kind: MsgKind,
    pub text: Option<String>,
    pub media_kind: Option<MediaKind>,
    /// Bytes, from the message itself.
    pub media_size: Option<i64>,
    pub media_mime: Option<String>,
    /// Telegram's `edit_date`: unix seconds of the latest edit. The only ordering
    /// of an edit chain.
    pub edited_at: Option<i64>,
    /// Telegram's `edit_hide`: show the message as unmodified despite its
    /// `edit_date`. Governs display only.
    pub edit_hidden: bool,
    pub reply_to_msg_id: Option<i32>,
    /// Who a forward came from, as a normalised peer. Set for ordinary forwards;
    /// see [`Row::fwd_from_name`] for the rest.
    pub fwd_from_id: Option<i64>,
    /// Who a forward came from, in words: an account with forward privacy on, or
    /// a channel post's author.
    pub fwd_from_name: Option<String>,
    /// When the original was written, in unix seconds.
    pub fwd_date: Option<i64>,
    /// The original's id inside the channel it was posted to.
    pub fwd_channel_post: Option<i32>,
    /// The album this message belongs to.
    pub grouped_id: Option<i64>,
    pub via_bot_id: Option<i64>,
    /// The disappearing-message timer.
    pub ttl_period: Option<i32>,
    /// The fragment a reply quoted, when it answered part of its target.
    pub reply_quote: Option<String>,
    /// The conversation a reply's target is in, when it is another one.
    pub reply_to_peer_id: Option<i64>,
    /// The TL constructor of a service message's action; `text` is our English
    /// rendering.
    pub service_action: Option<&'static str>,
    pub call: Option<Call>,
    pub entities: Vec<Entity>,
    pub reactions: Reactions,
}

/// Fold Telegram's three peer spaces into the one signed space the archive keys
/// conversations by: the Bot API's normalisation.
pub fn normalise_peer(peer: &tl::enums::Peer) -> (i64, PeerSpace) {
    match peer {
        tl::enums::Peer::User(u) => (u.user_id, PeerSpace::User),
        tl::enums::Peer::Chat(c) => (-c.chat_id, PeerSpace::Chat),
        tl::enums::Peer::Channel(c) => (CHANNEL_ID_OFFSET - c.channel_id, PeerSpace::Channel),
    }
}

/// "-100" written in front of a channel id: channel 1234 becomes -1001234.
const CHANNEL_ID_OFFSET: i64 = -1_000_000_000_000;

/// Map a wire message onto a row, or `None` for one with nothing to store.
///
/// A DM message omits `from_id`: the sender is `self_id` or the peer, and `out`
/// says which. Elsewhere a missing `from_id` means no sender, as on an anonymous
/// channel post.
pub fn map_message(msg: &tl::enums::Message, self_id: i64) -> Option<Row> {
    match msg {
        // A hole: what a deleted message looks like when fetched by id.
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
                text: non_empty(&m.message),
                media_kind: media.as_ref().map(|f| f.kind),
                media_size: media.as_ref().and_then(|f| f.size),
                media_mime: media.as_ref().and_then(|f| f.mime.clone()),
                edited_at: m.edit_date.map(i64::from),
                edit_hidden: m.edit_hide,
                reply_to_msg_id: m.reply_to.as_ref().and_then(reply_target),
                fwd_from_id: m.fwd_from.as_ref().and_then(fwd_peer),
                fwd_from_name: m.fwd_from.as_ref().and_then(fwd_name),
                fwd_date: m.fwd_from.as_ref().map(fwd_date),
                fwd_channel_post: m.fwd_from.as_ref().and_then(fwd_channel_post),
                grouped_id: m.grouped_id,
                via_bot_id: m.via_bot_id,
                ttl_period: m.ttl_period,
                reply_quote: m.reply_to.as_ref().and_then(reply_quote),
                reply_to_peer_id: m.reply_to.as_ref().and_then(reply_peer),
                service_action: None,
                call: None,
                entities: m.entities.as_deref().map(entities).unwrap_or_default(),
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
                // Our English label; Telegram sends an action, not words.
                text: Some(describe_action(&m.action).to_owned()),
                media_kind: None,
                media_size: None,
                media_mime: None,
                edited_at: None,
                edit_hidden: false,
                reply_to_msg_id: m.reply_to.as_ref().and_then(reply_target),
                fwd_from_id: None,
                fwd_from_name: None,
                fwd_date: None,
                fwd_channel_post: None,
                grouped_id: None,
                via_bot_id: None,
                ttl_period: m.ttl_period,
                reply_quote: m.reply_to.as_ref().and_then(reply_quote),
                reply_to_peer_id: m.reply_to.as_ref().and_then(reply_peer),
                service_action: Some(action_name(&m.action)),
                call: call_of(&m.action),
                entities: Vec::new(),
                reactions: m.reactions.as_ref().map(reactions).unwrap_or_default(),
            })
        }
    }
}

/// Who sent it; see [`map_message`].
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
        tl::enums::MessageReplyHeader::MessageReplyStoryHeader(_) => None,
    }
}

/// When the forwarded original was written; always present.
fn fwd_date(header: &tl::enums::MessageFwdHeader) -> i64 {
    match header {
        tl::enums::MessageFwdHeader::Header(h) => i64::from(h.date),
    }
}

/// The original's id inside the channel it was posted to.
fn fwd_channel_post(header: &tl::enums::MessageFwdHeader) -> Option<i32> {
    match header {
        tl::enums::MessageFwdHeader::Header(h) => h.channel_post,
    }
}

fn reply_quote(header: &tl::enums::MessageReplyHeader) -> Option<String> {
    match header {
        tl::enums::MessageReplyHeader::Header(h) => h.quote_text.as_deref().and_then(non_empty),
        tl::enums::MessageReplyHeader::MessageReplyStoryHeader(_) => None,
    }
}

/// The conversation a reply reached into, when it was not this one.
fn reply_peer(header: &tl::enums::MessageReplyHeader) -> Option<i64> {
    match header {
        tl::enums::MessageReplyHeader::Header(h) => {
            h.reply_to_peer_id.as_ref().map(|p| normalise_peer(p).0)
        }
        tl::enums::MessageReplyHeader::MessageReplyStoryHeader(_) => None,
    }
}

/// Every formatted span, with its payload, offsets unconverted.
fn entities(list: &[tl::enums::MessageEntity]) -> Vec<Entity> {
    use tl::enums::MessageEntity as E;
    list.iter()
        .map(|e| {
            // Spelled out, not derived from `Debug`: the kind is a stored identity.
            let (kind, offset_utf16, length_utf16) = match e {
                E::Unknown(x) => ("unknown", x.offset, x.length),
                E::Mention(x) => ("mention", x.offset, x.length),
                E::Hashtag(x) => ("hashtag", x.offset, x.length),
                E::BotCommand(x) => ("botCommand", x.offset, x.length),
                E::Url(x) => ("url", x.offset, x.length),
                E::Email(x) => ("email", x.offset, x.length),
                E::Bold(x) => ("bold", x.offset, x.length),
                E::Italic(x) => ("italic", x.offset, x.length),
                E::Code(x) => ("code", x.offset, x.length),
                E::Pre(x) => ("pre", x.offset, x.length),
                E::TextUrl(x) => ("textUrl", x.offset, x.length),
                E::MentionName(x) => ("mentionName", x.offset, x.length),
                E::Phone(x) => ("phone", x.offset, x.length),
                E::Cashtag(x) => ("cashtag", x.offset, x.length),
                E::Underline(x) => ("underline", x.offset, x.length),
                E::Strike(x) => ("strike", x.offset, x.length),
                E::BankCard(x) => ("bankCard", x.offset, x.length),
                E::Spoiler(x) => ("spoiler", x.offset, x.length),
                E::CustomEmoji(x) => ("customEmoji", x.offset, x.length),
                E::Blockquote(x) => ("blockquote", x.offset, x.length),
                E::InputMessageEntityMentionName(x) => ("inputMentionName", x.offset, x.length),
                // New TL entity kinds must not break the build.
                _ => ("other", 0, 0),
            };
            Entity {
                kind,
                offset_utf16,
                length_utf16,
                url: match e {
                    E::TextUrl(x) => non_empty(&x.url),
                    _ => None,
                },
                user_id: match e {
                    E::MentionName(x) => Some(x.user_id),
                    _ => None,
                },
                language: match e {
                    E::Pre(x) => non_empty(&x.language),
                    _ => None,
                },
                document_id: match e {
                    E::CustomEmoji(x) => Some(x.document_id),
                    _ => None,
                },
            }
        })
        .collect()
}

/// The TL constructor's name for a service action; [`describe_action`] renders
/// it for a reader.
fn action_name(action: &tl::enums::MessageAction) -> &'static str {
    use tl::enums::MessageAction as A;
    match action {
        A::Empty => "empty",
        A::ChatCreate(_) => "chatCreate",
        A::ChatEditTitle(_) => "chatEditTitle",
        A::ChatEditPhoto(_) => "chatEditPhoto",
        A::ChatDeletePhoto => "chatDeletePhoto",
        A::ChatAddUser(_) => "chatAddUser",
        A::ChatDeleteUser(_) => "chatDeleteUser",
        A::ChatJoinedByLink(_) => "chatJoinedByLink",
        A::ChannelCreate(_) => "channelCreate",
        A::ChatMigrateTo(_) => "chatMigrateTo",
        A::ChannelMigrateFrom(_) => "channelMigrateFrom",
        A::PinMessage => "pinMessage",
        A::HistoryClear => "historyClear",
        A::GameScore(_) => "gameScore",
        A::PhoneCall(_) => "phoneCall",
        A::ScreenshotTaken => "screenshotTaken",
        A::ContactSignUp => "contactSignUp",
        A::SetMessagesTtl(_) => "setMessagesTtl",
        A::GroupCall(_) => "groupCall",
        A::SetChatTheme(_) => "setChatTheme",
        A::WebViewDataSent(_) => "webViewDataSent",
        A::GiftPremium(_) => "giftPremium",
        A::TopicCreate(_) => "topicCreate",
        A::TopicEdit(_) => "topicEdit",
        A::SetChatWallPaper(_) => "setChatWallPaper",
        _ => "other",
    }
}

/// A call's duration, outcome and kind, from the service action that reports it.
fn call_of(action: &tl::enums::MessageAction) -> Option<Call> {
    let tl::enums::MessageAction::PhoneCall(c) = action else {
        return None;
    };
    use tl::enums::PhoneCallDiscardReason as R;
    Some(Call {
        call_id: c.call_id,
        duration_s: c.duration,
        reason: c.reason.as_ref().map(|r| match r {
            R::Missed => "missed",
            R::Disconnect => "disconnect",
            R::Hangup => "hangup",
            R::Busy => "busy",
            R::MigrateConferenceCall(_) => "migrateConferenceCall",
        }),
        video: c.video,
    })
}

/// The forwarded-from peer, normalised like every other peer here.
fn fwd_peer(header: &tl::enums::MessageFwdHeader) -> Option<i64> {
    match header {
        tl::enums::MessageFwdHeader::Header(h) => h.from_id.as_ref().map(|p| normalise_peer(p).0),
    }
}

/// A forward's origin in words: `from_name` for a sender with forward privacy
/// on, else `post_author`, who signed a channel post.
fn fwd_name(header: &tl::enums::MessageFwdHeader) -> Option<String> {
    match header {
        tl::enums::MessageFwdHeader::Header(h) => h
            .from_name
            .clone()
            .or_else(|| h.post_author.clone())
            .and_then(|n| non_empty(&n)),
    }
}

fn reactions(r: &tl::enums::MessageReactions) -> Reactions {
    let tl::enums::MessageReactions::Reactions(r) = r;
    let counts: Vec<Reaction> = r
        .results
        .iter()
        .map(|count| {
            let tl::enums::ReactionCount::Count(c) = count;
            let (emoji, custom_emoji_id) = emoji_of(&c.reaction);
            Reaction {
                emoji,
                custom_emoji_id,
                cnt: c.count,
                chosen: c.chosen_order.is_some(),
            }
        })
        .collect();

    let authors: Vec<ReactionAuthor> = r
        .recent_reactions
        .iter()
        .flatten()
        .map(|peer_reaction| {
            let tl::enums::MessagePeerReaction::Reaction(pr) = peer_reaction;
            let (emoji, custom_emoji_id) = emoji_of(&pr.reaction);
            ReactionAuthor {
                peer_id: normalise_peer(&pr.peer_id).0,
                emoji,
                custom_emoji_id,
                reacted_at: i64::from(pr.date),
            }
        })
        .collect();

    // Complete when the list is no shorter than the tally. `>=` because a reactor
    // can appear under several emoji. An absent list gives no authors, so it is
    // never complete while anything is counted.
    let counted: i32 = counts.iter().map(|c| c.cnt).sum();
    let complete = i32::try_from(authors.len()).is_ok_and(|named| named >= counted);

    Reactions {
        counts,
        authors,
        complete,
    }
}

/// Which reaction it was: a unicode emoticon, or a custom emoji's document id.
/// `reactionEmpty` and paid reactions have neither.
fn emoji_of(reaction: &tl::enums::Reaction) -> (Option<String>, Option<i64>) {
    match reaction {
        tl::enums::Reaction::Emoji(e) => (Some(e.emoticon.clone()), None),
        tl::enums::Reaction::CustomEmoji(e) => (None, Some(e.document_id)),
        _ => (None, None),
    }
}

/// What a message's media is, its size and mime, all from the message itself.
///
/// Through `grammers_client::media::Media`, which reads the document attributes
/// that tell a sticker, video or PDF apart; all are `messageMediaDocument` on
/// the wire.
fn media_of(media: &tl::enums::MessageMedia) -> MediaFacts {
    use grammers_client::media::Media as M;
    let Some(m) = M::from_raw(media.clone()) else {
        // `messageMediaEmpty`, or media this grammers does not model.
        return MediaFacts {
            kind: MediaKind::Other,
            size: None,
            mime: None,
        };
    };
    let size = m.size().and_then(|s| i64::try_from(s).ok());
    let (kind, mime) = match &m {
        // Telegram photos are always JPEG.
        M::Photo(_) => (MediaKind::Photo, Some("image/jpeg".to_owned())),
        M::Sticker(s) => (
            MediaKind::Sticker,
            s.document.mime_type().map(str::to_owned),
        ),
        M::Document(d) => (
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
        // `Media` is `#[non_exhaustive]`.
        _ => (MediaKind::Other, None),
    };
    MediaFacts { kind, size, mime }
}

/// What [`media_of`] found.
pub struct MediaFacts {
    pub kind: MediaKind,
    /// Bytes; `None` for media that is not a file, such as a poll.
    pub size: Option<i64>,
    pub mime: Option<String>,
}

/// An English label for a service action; unlisted ones read "an event".
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
