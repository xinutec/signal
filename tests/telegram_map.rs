//! Tests for the Telegram mapping — pure, so no database and no account.
//!
//! **These fixtures cannot drift from Telegram's contract**, which is why they
//! are struct literals rather than captured JSON. `tl::types::Message` is
//! generated from Telegram's own TL schema, so a field that changes name, type or
//! optionality stops this file compiling — the opposite of a hand-written mock,
//! which goes on describing a shape the real thing has left behind. Every one of
//! the forty-nine fields has to be written down once, in [`dm`], and that is the
//! whole cost.

use grammers_tl_types as tl;
use signal_archiver::telegram::map::{
    MediaKind, MsgKind, PeerSpace, Reaction, Row, map_message, normalise_peer,
};

/// The logged-in account, in every test below.
const SELF_ID: i64 = 777;
/// The other person in the DM fixtures.
const THEM: i64 = 4242;

fn user(id: i64) -> tl::enums::Peer {
    tl::enums::Peer::User(tl::types::PeerUser { user_id: id })
}

/// An ordinary incoming text message in a one-to-one chat, with `from_id` absent
/// — which is how Telegram actually sends a DM. Tests override what they are
/// about and nothing else.
fn dm() -> tl::types::Message {
    tl::types::Message {
        out: false,
        mentioned: false,
        media_unread: false,
        silent: false,
        post: false,
        from_scheduled: false,
        legacy: false,
        edit_hide: false,
        pinned: false,
        noforwards: false,
        invert_media: false,
        offline: false,
        video_processing_pending: false,
        paid_suggested_post_stars: false,
        paid_suggested_post_ton: false,
        id: 9001,
        from_id: None,
        from_boosts_applied: None,
        from_rank: None,
        peer_id: user(THEM),
        saved_peer_id: None,
        fwd_from: None,
        via_bot_id: None,
        via_business_bot_id: None,
        guestchat_via_from: None,
        reply_to: None,
        date: 1_700_000_000,
        message: "hoi".to_owned(),
        media: None,
        reply_markup: None,
        entities: None,
        views: None,
        forwards: None,
        replies: None,
        edit_date: None,
        post_author: None,
        grouped_id: None,
        reactions: None,
        restriction_reason: None,
        ttl_period: None,
        quick_reply_shortcut_id: None,
        effect: None,
        factcheck: None,
        report_delivery_until_date: None,
        paid_message_stars: None,
        suggested_post: None,
        schedule_repeat_period: None,
        summary_from_language: None,
        rich_message: None,
    }
}

fn mapped(m: tl::types::Message) -> Row {
    map_message(&tl::enums::Message::Message(m), SELF_ID).expect("a text message is storable")
}

/// The property the normalisation exists for: the three peer spaces become one
/// space with no overlap.
///
/// Asserting the three formulas separately would only restate the code. What
/// matters is that the SAME raw number in each space lands on three different
/// conversations, and that each is still labelled with the space it came from —
/// because an id that can mean two conversations is an archive that merges two
/// people's messages.
#[test]
fn one_raw_id_in_three_spaces_is_three_conversations() {
    let raw = 1234;
    let ids: Vec<(i64, PeerSpace)> = [
        tl::enums::Peer::User(tl::types::PeerUser { user_id: raw }),
        tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: raw }),
        tl::enums::Peer::Channel(tl::types::PeerChannel { channel_id: raw }),
    ]
    .iter()
    .map(normalise_peer)
    .collect();

    let spaces: Vec<PeerSpace> = ids.iter().map(|(_, k)| *k).collect();
    assert_eq!(
        spaces,
        vec![PeerSpace::User, PeerSpace::Chat, PeerSpace::Channel]
    );

    let mut folded: Vec<i64> = ids.iter().map(|(id, _)| *id).collect();
    folded.sort_unstable();
    folded.dedup();
    assert_eq!(
        folded.len(),
        3,
        "two peer spaces folded onto one id: {ids:?}"
    );
}

/// ⚠ The channel offset has to sit below every basic-group id, or a group and a
/// channel collide. Checked against a group id far larger than any real one
/// rather than against a plausible value, because "plausible" is what changes.
#[test]
fn no_channel_can_collide_with_a_basic_group() {
    let biggest_imaginable_group = 999_999_999_999;
    let (group, _) = normalise_peer(&tl::enums::Peer::Chat(tl::types::PeerChat {
        chat_id: biggest_imaginable_group,
    }));
    let (channel, _) = normalise_peer(&tl::enums::Peer::Channel(tl::types::PeerChannel {
        channel_id: 1,
    }));
    assert!(
        channel < group,
        "channel ids must sit below group ids; got channel {channel} and group {group}"
    );
}

/// ⚠ The rule from [`map_message`]'s warning, both ways round. A DM names no
/// sender, so `out` is the only thing that says who spoke — and swapping the two
/// arms is a mistake that reads correctly and files every message under the wrong
/// person.
#[test]
fn a_dm_that_names_no_sender_is_attributed_by_direction() {
    let incoming = mapped(dm());
    assert_eq!(incoming.sender_id, Some(THEM));
    assert!(!incoming.is_outgoing);

    let outgoing = mapped(tl::types::Message { out: true, ..dm() });
    assert_eq!(outgoing.sender_id, Some(SELF_ID));
    assert!(outgoing.is_outgoing);
}

/// ⚠ And the inference must not leak out of DMs. A group message with no author
/// genuinely has none — an anonymous admin post — so guessing would invent a
/// speaker rather than admit to not knowing one.
#[test]
fn a_group_message_with_no_author_keeps_none() {
    let anonymous = tl::types::Message {
        peer_id: tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 55 }),
        from_id: None,
        ..dm()
    };
    assert_eq!(mapped(anonymous).sender_id, None);
}

/// An explicit sender always wins, in either kind of conversation. The inference
/// is a fallback, and a fallback that overrides what it was told is not one.
#[test]
fn a_named_sender_beats_the_inference() {
    let in_a_dm = tl::types::Message {
        out: true,
        from_id: Some(user(THEM)),
        ..dm()
    };
    assert_eq!(
        mapped(in_a_dm).sender_id,
        Some(THEM),
        "an outgoing DM that names someone else as sender is a forward-like case, \
         and the header is still what Telegram said"
    );

    let in_a_group = tl::types::Message {
        peer_id: tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 55 }),
        from_id: Some(user(THEM)),
        ..dm()
    };
    assert_eq!(mapped(in_a_group).sender_id, Some(THEM));
}

/// A message whose content is a photo said nothing, and the archive has to be
/// able to tell that from a message whose text is the empty string. The viewer
/// leans on it: an empty body produces no line in a copied log.
#[test]
fn a_message_with_only_media_has_no_text() {
    let photo = tl::types::Message {
        message: String::new(),
        media: Some(tl::enums::MessageMedia::Photo(
            tl::types::MessageMediaPhoto {
                spoiler: false,
                live_photo: false,
                photo: None,
                ttl_seconds: None,
                video: None,
            },
        )),
        ..dm()
    };
    let row = mapped(photo);
    assert_eq!(row.text, None);
    assert_eq!(row.media_kind, Some(MediaKind::Photo));
}

/// ⚠ A sticker, a video note and a PDF are all `messageMediaDocument` on the
/// wire. This pins the honest answer — `document` — rather than a finer label
/// this build does not earn, so that a later pass which reads the document's
/// attributes has a test to change deliberately.
#[test]
fn every_document_shaped_media_reports_document() {
    let doc = tl::types::Message {
        media: Some(tl::enums::MessageMedia::Document(
            tl::types::MessageMediaDocument {
                nopremium: false,
                spoiler: false,
                video: false,
                round: false,
                voice: false,
                document: None,
                alt_documents: None,
                video_cover: None,
                video_timestamp: None,
                ttl_seconds: None,
            },
        )),
        ..dm()
    };
    assert_eq!(mapped(doc).media_kind, Some(MediaKind::Document));
}

/// A hole in the id sequence is not a message. Telegram answers a fetch of a
/// deleted id with `messageEmpty`, and storing that would put an empty bubble in
/// the conversation where something used to be.
#[test]
fn an_empty_message_is_not_a_row() {
    let hole = tl::enums::Message::Empty(tl::types::MessageEmpty {
        id: 9001,
        peer_id: Some(user(THEM)),
    });
    assert!(map_message(&hole, SELF_ID).is_none());
}

/// A service message is recorded as an event, with a label that is ours. Both
/// halves matter: the `kind` is what lets a reader ask for what was SAID, and the
/// text being present is what stops the conversation having a silent hole.
#[test]
fn a_service_message_is_an_event_with_a_label() {
    let joined = tl::enums::Message::Service(tl::types::MessageService {
        out: false,
        mentioned: false,
        media_unread: false,
        reactions_are_possible: false,
        silent: false,
        post: false,
        legacy: false,
        id: 9002,
        from_id: Some(user(THEM)),
        peer_id: tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 55 }),
        saved_peer_id: None,
        reply_to: None,
        date: 1_700_000_100,
        action: tl::enums::MessageAction::ChatAddUser(tl::types::MessageActionChatAddUser {
            users: vec![THEM],
        }),
        reactions: None,
        ttl_period: None,
    });
    let row = map_message(&joined, SELF_ID).expect("an event is worth a row");
    assert_eq!(row.kind, MsgKind::Service);
    assert_eq!(row.text.as_deref(), Some("added a member"));
}

/// An action this build has never heard of is still an event. A new Telegram
/// feature must not make messages vanish from the archive, so the fallback is
/// asserted rather than left to be discovered.
#[test]
fn an_unknown_action_is_still_recorded() {
    let odd = tl::enums::Message::Service(tl::types::MessageService {
        out: false,
        mentioned: false,
        media_unread: false,
        reactions_are_possible: false,
        silent: false,
        post: false,
        legacy: false,
        id: 9003,
        from_id: None,
        peer_id: user(THEM),
        saved_peer_id: None,
        reply_to: None,
        date: 1_700_000_200,
        action: tl::enums::MessageAction::ScreenshotTaken,
        reactions: None,
        ttl_period: None,
    });
    let row = map_message(&odd, SELF_ID).expect("an unnamed action is still an event");
    assert_eq!(row.kind, MsgKind::Service);
    assert_eq!(row.text.as_deref(), Some("an event"));
}

/// Reactions: a unicode emoticon and a custom emoji are stored in DIFFERENT
/// columns and never both, because a reader that draws `emoji` would otherwise
/// draw a blank for a custom one and silently count it as nothing.
#[test]
fn a_custom_reaction_keeps_its_id_instead_of_an_emoji() {
    let reacted = tl::types::Message {
        reactions: Some(tl::enums::MessageReactions::Reactions(
            tl::types::MessageReactions {
                min: false,
                can_see_list: false,
                reactions_as_tags: false,
                results: vec![
                    tl::enums::ReactionCount::Count(tl::types::ReactionCount {
                        chosen_order: Some(0),
                        reaction: tl::enums::Reaction::Emoji(tl::types::ReactionEmoji {
                            emoticon: "👍".to_owned(),
                        }),
                        count: 3,
                    }),
                    tl::enums::ReactionCount::Count(tl::types::ReactionCount {
                        chosen_order: None,
                        reaction: tl::enums::Reaction::CustomEmoji(
                            tl::types::ReactionCustomEmoji { document_id: 55555 },
                        ),
                        count: 1,
                    }),
                ],
                recent_reactions: None,
                top_reactors: None,
            },
        )),
        ..dm()
    };
    assert_eq!(
        mapped(reacted).reactions,
        vec![
            Reaction {
                emoji: Some("👍".to_owned()),
                custom_emoji_id: None,
                cnt: 3,
                chosen: true,
            },
            Reaction {
                emoji: None,
                custom_emoji_id: Some(55555),
                cnt: 1,
                chosen: false,
            },
        ]
    );
}

/// `edit_date` is the only ordering an edit chain has, so it has to reach the row
/// — `telegram_message_edits` keys on it. A message never edited must report
/// `None` rather than its send time, or every message looks edited.
#[test]
fn an_edit_date_reaches_the_row_and_an_unedited_message_has_none() {
    assert_eq!(mapped(dm()).edited_at, None);
    let edited = tl::types::Message {
        edit_date: Some(1_700_000_500),
        ..dm()
    };
    assert_eq!(mapped(edited).edited_at, Some(1_700_000_500));
}

/// Timestamps are carried through in Telegram's own unit, unconverted. Pinned
/// because the temptation to "helpfully" turn seconds into the milliseconds the
/// viewer shows is exactly how a unit gets applied twice.
#[test]
fn the_timestamp_stays_in_seconds() {
    assert_eq!(mapped(dm()).sent_at, 1_700_000_000);
}

/// A reply to a story points at nothing this archive holds, so it must not be
/// filed as a reply to a message id — least of all to a DIFFERENT message that
/// happens to have that id.
#[test]
fn a_story_reply_is_not_a_message_reply() {
    let to_a_message = tl::types::Message {
        reply_to: Some(tl::enums::MessageReplyHeader::Header(
            tl::types::MessageReplyHeader {
                reply_to_scheduled: false,
                forum_topic: false,
                quote: false,
                reply_to_ephemeral: false,
                reply_to_msg_id: Some(8000),
                reply_to_peer_id: None,
                reply_from: None,
                reply_media: None,
                reply_to_top_id: None,
                quote_text: None,
                quote_entities: None,
                quote_offset: None,
                todo_item_id: None,
                poll_option: None,
            },
        )),
        ..dm()
    };
    assert_eq!(mapped(to_a_message).reply_to_msg_id, Some(8000));

    let to_a_story = tl::types::Message {
        reply_to: Some(tl::enums::MessageReplyHeader::MessageReplyStoryHeader(
            tl::types::MessageReplyStoryHeader {
                peer: user(THEM),
                story_id: 5,
            },
        )),
        ..dm()
    };
    assert_eq!(mapped(to_a_story).reply_to_msg_id, None);
}
