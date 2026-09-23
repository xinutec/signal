//! Tests for the Telegram mapping — pure, so no database and no account.
//!
//! Fixtures are struct literals of the TL-generated types, so a schema change
//! stops this file compiling rather than leaving a stale mock.

use grammers_tl_types as tl;
use signal_archiver::telegram::map::{
    MediaKind, MsgKind, PeerSpace, Reaction, ReactionAuthor, Row, map_message, normalise_peer,
};

/// The logged-in account, in every test below.
const SELF_ID: i64 = 777;
/// The other person in the DM fixtures.
const THEM: i64 = 4242;

fn user(id: i64) -> tl::enums::Peer {
    tl::enums::Peer::User(tl::types::PeerUser { user_id: id })
}

/// An ordinary incoming DM, with `from_id` absent as Telegram sends it.
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

/// The same raw number in each space lands on three different conversations.
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

/// The channel offset sits below every basic-group id, however large.
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

#[test]
fn a_dm_that_names_no_sender_is_attributed_by_direction() {
    let incoming = mapped(dm());
    assert_eq!(incoming.sender_id, Some(THEM));
    assert!(!incoming.is_outgoing);

    let outgoing = mapped(tl::types::Message { out: true, ..dm() });
    assert_eq!(outgoing.sender_id, Some(SELF_ID));
    assert!(outgoing.is_outgoing);
}

/// A group message with no author (an anonymous admin post) has no sender.
#[test]
fn a_group_message_with_no_author_keeps_none() {
    let anonymous = tl::types::Message {
        peer_id: tl::enums::Peer::Chat(tl::types::PeerChat { chat_id: 55 }),
        from_id: None,
        ..dm()
    };
    assert_eq!(mapped(anonymous).sender_id, None);
}

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

/// The finer label comes from the document's mime. Without a mime it stays
/// `document`.
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

#[test]
fn an_empty_message_is_not_a_row() {
    let hole = tl::enums::Message::Empty(tl::types::MessageEmpty {
        id: 9001,
        peer_id: Some(user(THEM)),
    });
    assert!(map_message(&hole, SELF_ID).is_none());
}

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

/// A unicode emoticon and a custom emoji use different columns, never both.
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
    let got = mapped(reacted).reactions;
    assert_eq!(
        got.counts,
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
    // Telegram named nobody, so the list cannot be complete.
    assert_eq!(got.authors, vec![]);
    assert!(!got.complete);
}

/// A message never edited reports `None`, not its send time.
#[test]
fn an_edit_date_reaches_the_row_and_an_unedited_message_has_none() {
    assert_eq!(mapped(dm()).edited_at, None);
    let edited = tl::types::Message {
        edit_date: Some(1_700_000_500),
        ..dm()
    };
    assert_eq!(mapped(edited).edited_at, Some(1_700_000_500));
}

/// Timestamps stay in Telegram's seconds; the viewer converts.
#[test]
fn the_timestamp_stays_in_seconds() {
    assert_eq!(mapped(dm()).sent_at, 1_700_000_000);
}

/// A reply to a story is not filed as a reply to a message id.
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

/// Size and mime come from the message itself. A poll's size is `None`: not a
/// file, as opposed to an empty one.
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
    assert_eq!(row.media_kind, Some(MediaKind::Video));
}

/// `edit_hide` travels with the recorded `edit_date`, so the reader can honour it.
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

/// The date every [`fwd`] header carries, distinct from the forwarder's clock.
const FWD_DATE: i64 = 1_699_000_000;

/// A service message carrying one action.
fn service(action: tl::enums::MessageAction) -> tl::enums::Message {
    tl::enums::Message::Service(tl::types::MessageService {
        out: false,
        mentioned: false,
        media_unread: false,
        reactions_are_possible: false,
        silent: false,
        post: false,
        legacy: false,
        id: 9003,
        from_id: Some(user(THEM)),
        peer_id: user(THEM),
        saved_peer_id: None,
        reply_to: None,
        date: 1_700_000_300,
        action,
        reactions: None,
        ttl_period: None,
    })
}

/// A forward header, with only the fields a case needs set.
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

/// An ordinary forward names its sender by peer.
#[test]
fn an_ordinary_forward_is_recorded_by_its_peer() {
    let mut m = dm();
    m.fwd_from = Some(fwd(Some(user(555)), None, None));
    let row = mapped(m);

    assert_eq!(row.fwd_from_id, Some(normalise_peer(&user(555)).0));
    assert_eq!(row.fwd_from_name, None, "a peer is not a name");
}

/// A sender with forward privacy on is named in words.
#[test]
fn a_forward_from_a_hidden_account_keeps_the_name_it_offered() {
    let mut m = dm();
    m.fwd_from = Some(fwd(None, Some("Someone".to_owned()), None));
    let row = mapped(m);

    assert_eq!(row.fwd_from_name.as_deref(), Some("Someone"));
    assert_eq!(row.fwd_from_id, None, "privacy on → there is no peer");
}

/// A forwarded channel post's `from_id` is the channel; the person who signed
/// it is in `post_author`.
#[test]
fn a_forwarded_channel_post_keeps_the_channel_and_the_author() {
    let mut m = dm();
    let channel = tl::enums::Peer::Channel(tl::types::PeerChannel { channel_id: 55 });
    m.fwd_from = Some(fwd(Some(channel.clone()), None, Some("Ada".to_owned())));
    let row = mapped(m);

    assert_eq!(row.fwd_from_id, Some(normalise_peer(&channel).0));
    assert_eq!(row.fwd_from_name.as_deref(), Some("Ada"));
}

/// Telegram sends `Some("")` in places; that is not a name.
#[test]
fn an_empty_forward_name_is_no_name() {
    let mut m = dm();
    m.fwd_from = Some(fwd(None, Some(String::new()), None));
    assert_eq!(mapped(m).fwd_from_name, None);
}

#[test]
fn a_message_that_was_not_forwarded_carries_neither_half() {
    let row = mapped(dm());
    assert_eq!((row.fwd_from_id, row.fwd_from_name), (None, None));
}

/// Build a reactions block with a tally and a (possibly short) list of names.
fn reacted(counts: &[(&str, i32)], names: &[(i64, &str, i32)]) -> tl::enums::MessageReactions {
    tl::enums::MessageReactions::Reactions(tl::types::MessageReactions {
        min: false,
        can_see_list: true,
        reactions_as_tags: false,
        results: counts
            .iter()
            .map(|(emoticon, count)| {
                tl::enums::ReactionCount::Count(tl::types::ReactionCount {
                    chosen_order: None,
                    reaction: tl::enums::Reaction::Emoji(tl::types::ReactionEmoji {
                        emoticon: (*emoticon).to_owned(),
                    }),
                    count: *count,
                })
            })
            .collect(),
        recent_reactions: Some(
            names
                .iter()
                .map(|(peer, emoticon, date)| {
                    tl::enums::MessagePeerReaction::Reaction(tl::types::MessagePeerReaction {
                        big: false,
                        unread: false,
                        my: false,
                        peer_id: user(*peer),
                        date: *date,
                        reaction: tl::enums::Reaction::Emoji(tl::types::ReactionEmoji {
                            emoticon: (*emoticon).to_owned(),
                        }),
                    })
                })
                .collect(),
        ),
        top_reactors: None,
    })
}

/// Both directions, or a hardcoded `complete: true` would pass.
#[test]
fn a_truncated_list_of_reactors_is_not_a_complete_one() {
    let full = tl::types::Message {
        reactions: Some(reacted(&[("👍", 2)], &[(1, "👍", 111), (2, "👍", 222)])),
        ..dm()
    };
    let got = mapped(full).reactions;
    assert_eq!(got.authors.len(), 2);
    assert!(got.complete, "two names for a tally of two names everybody");

    // The same two names against a tally of twenty.
    let sampled = tl::types::Message {
        reactions: Some(reacted(&[("👍", 20)], &[(1, "👍", 111), (2, "👍", 222)])),
        ..dm()
    };
    let got = mapped(sampled).reactions;
    assert_eq!(got.authors.len(), 2);
    assert!(
        !got.complete,
        "two names for a tally of twenty is a SAMPLE, and must retract nobody"
    );
}

#[test]
fn a_reaction_carries_who_and_when() {
    let m = tl::types::Message {
        reactions: Some(reacted(&[("❤", 1)], &[(THEM, "❤", 1_700_000_500)])),
        ..dm()
    };
    let got = mapped(m).reactions;
    assert_eq!(
        got.authors,
        vec![ReactionAuthor {
            peer_id: THEM,
            emoji: Some("❤".to_owned()),
            custom_emoji_id: None,
            reacted_at: 1_700_000_500,
        }]
    );
    assert!(got.complete);
}

/// A `textUrl`'s url is not in the text.
#[test]
fn a_link_keeps_the_address_the_words_do_not_say() {
    let m = tl::types::Message {
        message: "see here".to_owned(),
        entities: Some(vec![
            tl::enums::MessageEntity::Bold(tl::types::MessageEntityBold {
                offset: 0,
                length: 3,
            }),
            tl::enums::MessageEntity::TextUrl(tl::types::MessageEntityTextUrl {
                offset: 4,
                length: 4,
                url: "https://example.org/somewhere".to_owned(),
            }),
        ]),
        ..dm()
    };
    let got = mapped(m).entities;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].kind, "bold");
    assert_eq!(got[0].url, None);
    assert_eq!(got[1].kind, "textUrl");
    assert_eq!(got[1].offset_utf16, 4);
    assert_eq!(
        got[1].url.as_deref(),
        Some("https://example.org/somewhere"),
        "the address is nowhere in the eight characters of the message"
    );
}

#[test]
fn an_unanswered_call_is_a_reason_without_a_duration() {
    let missed = service(tl::enums::MessageAction::PhoneCall(
        tl::types::MessageActionPhoneCall {
            video: false,
            call_id: 77,
            reason: Some(tl::enums::PhoneCallDiscardReason::Missed),
            duration: None,
        },
    ));
    let row = map_message(&missed, SELF_ID).expect("a service message is storable");
    assert_eq!(row.service_action, Some("phoneCall"));
    let call = row.call.expect("a phone call action carries a call");
    assert_eq!(call.reason, Some("missed"));
    assert_eq!(call.duration_s, None);
    assert!(!call.video);

    let answered = service(tl::enums::MessageAction::PhoneCall(
        tl::types::MessageActionPhoneCall {
            video: true,
            call_id: 78,
            reason: Some(tl::enums::PhoneCallDiscardReason::Hangup),
            duration: Some(2_820),
        },
    ));
    let call = map_message(&answered, SELF_ID)
        .expect("storable")
        .call
        .expect("a call");
    assert_eq!(call.duration_s, Some(2_820));
    assert_eq!(call.reason, Some("hangup"));
    assert!(call.video);
}

#[test]
fn a_forward_remembers_when_the_original_was_written() {
    let m = tl::types::Message {
        fwd_from: Some(fwd(Some(user(THEM)), None, None)),
        date: 1_700_000_000,
        ..dm()
    };
    let row = mapped(m);
    assert_eq!(row.sent_at, 1_700_000_000);
    assert_eq!(
        row.fwd_date,
        Some(FWD_DATE),
        "the forward's own clock, not the forwarder's"
    );
    assert_ne!(row.sent_at, row.fwd_date.unwrap());
}
