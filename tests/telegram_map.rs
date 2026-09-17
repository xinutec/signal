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

/// ⚠ **A sticker, a video and a PDF are all `messageMediaDocument` on the wire**,
/// and this test used to pin the coarse answer `document` — "a finer label this
/// build does not earn" — with a note that a later pass reading the document's
/// attributes would have a test to change deliberately. This is that change.
///
/// The finer label now comes from `Media::from_raw`, which reads the attributes and
/// needs no client. A document with NO mime is still `document`: the taxonomy is
/// the mime type's, not ours, so an absent mime leaves nothing to be finer about.
#[test]
fn a_document_with_no_mime_type_is_still_just_a_document() {
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

/// The size and the mime come out of the MESSAGE, with no request — which is the
/// whole reason the archive can say what a download would cost before deciding to
/// make one.
///
/// ⚠ A poll has no size and that is not a failure: `None` means "not a file",
/// where 0 would mean "an empty file". The column is NULLable for that reason.
#[test]
fn media_reports_its_size_without_a_download() {
    let doc = tl::types::Document {
        id: 11,
        access_hash: 22,
        file_reference: vec![],
        date: 1_700_000_000,
        mime_type: "video/mp4".to_owned(),
        size: 3_145_728,
        thumbs: None,
        video_thumbs: None,
        dc_id: 2,
        attributes: vec![],
    };
    let with_video = tl::types::Message {
        media: Some(tl::enums::MessageMedia::Document(
            tl::types::MessageMediaDocument {
                nopremium: false,
                spoiler: false,
                video: false,
                round: false,
                voice: false,
                document: Some(tl::enums::Document::Document(doc)),
                alt_documents: None,
                video_cover: None,
                video_timestamp: None,
                ttl_seconds: None,
            },
        )),
        ..dm()
    };
    let row = mapped(with_video);
    assert_eq!(row.media_size, Some(3_145_728));
    assert_eq!(row.media_mime.as_deref(), Some("video/mp4"));
    // ⚠ And the mime is what makes it a VIDEO rather than a document — the finer
    // label is the mime's judgement, not a second taxonomy of ours that could
    // disagree with it.
    assert_eq!(row.media_kind, Some(MediaKind::Video));
}

/// ⚠ **AN `edit_date` IS NOT "SOMEBODY EDITED THIS".** Telegram carries `edit_hide`
/// beside it — "whether the message should be shown as not modified to the user,
/// even if an edit date is present" — and sets an edit date for its own reasons.
/// Reading the date without the flag is reading half the contract, and the visible
/// consequence was this archive printing "Edited" on a message Telegram itself
/// shows as untouched.
///
/// Both halves are asserted: the date is still RECORDED (an archive keeps what it
/// saw), and the flag travels with it so the reader can honour it.
#[test]
fn an_edit_telegram_asks_us_to_hide_is_recorded_but_marked_hidden() {
    let ordinary = tl::types::Message {
        edit_date: Some(1_700_000_500),
        ..dm()
    };
    let row = mapped(ordinary);
    assert_eq!(row.edited_at, Some(1_700_000_500));
    assert!(!row.edit_hidden);

    let hidden = tl::types::Message {
        edit_date: Some(1_700_000_500),
        edit_hide: true,
        ..dm()
    };
    let row = mapped(hidden);
    assert_eq!(
        row.edited_at,
        Some(1_700_000_500),
        "the archive keeps the date it was given"
    );
    assert!(
        row.edit_hidden,
        "and carries the instruction not to show it"
    );
}

/// A forward header, with only the fields any one case needs set.
fn fwd(
    from_id: Option<tl::enums::Peer>,
    from_name: Option<String>,
    post_author: Option<String>,
) -> tl::enums::MessageFwdHeader {
    tl::enums::MessageFwdHeader::Header(tl::types::MessageFwdHeader {
        imported: false,
        saved_out: false,
        from_id,
        from_name,
        date: 1_699_000_000,
        channel_post: None,
        post_author,
        saved_from_peer: None,
        saved_from_msg_id: None,
        saved_from_id: None,
        saved_from_name: None,
        saved_date: None,
        psa_type: None,
    })
}

/// ⚠ **A forward is recorded by PEER, which is the case that was being dropped.**
///
/// The header carries `from_id` for an ordinary forward and falls back to
/// `from_name` only when the original sender has forward-privacy on. Reading the
/// name alone meant the archive recorded a forward exactly when its sender had
/// asked not to be identified, and recorded nothing at all otherwise — so a
/// forwarded message was indistinguishable from something the sender had
/// written. Zero rows out of 159,946 when that was noticed, which is what a
/// silently-never-true condition looks like from the outside.
#[test]
fn an_ordinary_forward_is_recorded_by_its_peer() {
    let mut m = dm();
    m.fwd_from = Some(fwd(Some(user(555)), None, None));
    let row = mapped(m);

    // Normalised like every other peer here, so a forwarder in a group and the
    // DM with that same person are one id.
    assert_eq!(row.fwd_from_id, Some(normalise_peer(&user(555)).0));
    assert_eq!(row.fwd_from_name, None, "a peer is not a name");
}

/// The rarer half, which was the ONLY half being read.
#[test]
fn a_forward_from_a_hidden_account_keeps_the_name_it_offered() {
    let mut m = dm();
    m.fwd_from = Some(fwd(None, Some("Someone".to_owned()), None));
    let row = mapped(m);

    assert_eq!(row.fwd_from_name.as_deref(), Some("Someone"));
    assert_eq!(row.fwd_from_id, None, "privacy on → there is no peer");
}

/// ⚠ A CHANNEL POST'S AUTHOR IS NOT ITS PEER. `from_id` for a forwarded channel
/// post names the CHANNEL, so the person who signed it is only in `post_author`
/// — and reading `from_id` alone would credit the channel for what a named human
/// wrote. Both are kept, because they answer different questions.
#[test]
fn a_forwarded_channel_post_keeps_the_channel_and_the_author() {
    let mut m = dm();
    let channel = tl::enums::Peer::Channel(tl::types::PeerChannel { channel_id: 55 });
    m.fwd_from = Some(fwd(Some(channel.clone()), None, Some("Ada".to_owned())));
    let row = mapped(m);

    assert_eq!(row.fwd_from_id, Some(normalise_peer(&channel).0));
    assert_eq!(row.fwd_from_name.as_deref(), Some("Ada"));
}

/// An empty string is not a name. Telegram sends `Some("")` rather than `None`
/// in places, and storing that gives a row that claims to know who forwarded it
/// and then shows nothing — the distinction the rest of this mapping keeps with
/// `non_empty`.
#[test]
fn an_empty_forward_name_is_no_name() {
    let mut m = dm();
    m.fwd_from = Some(fwd(None, Some(String::new()), None));
    assert_eq!(mapped(m).fwd_from_name, None);
}

/// A message nobody forwarded says so in both fields, rather than in one.
#[test]
fn a_message_that_was_not_forwarded_carries_neither_half() {
    let row = mapped(dm());
    assert_eq!((row.fwd_from_id, row.fwd_from_name), (None, None));
}
