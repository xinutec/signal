//! Unit tests for the frame parser. Run with `cargo test`.

use serde_json::json;
use signal_archiver::parse::{
    Action, Attachment, CallEvent, CallEventKind, Contact, Edit, Message, Reaction, Receipt,
    ReceiptKind, ThreadId, ThreadKind, display_name_of, parse_frame,
};

#[test]
fn incoming_text_dm() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "sourceNumber": "+441234", "sourceName": "Alice",
        "timestamp": 1000, "dataMessage": {"message": "hi there"}
    }});
    let p = parse_frame(&f);
    assert_eq!(
        p.action,
        Action::Message(Message {
            thread_id: ThreadId::Dm("u1".into()),
            sender: "u1".into(),
            server_ts: 1000,
            server_received_ts: None,
            server_delivered_ts: None,
            expires_in_seconds: None,
            body: Some("hi there".into()),
            quote_target_ts: None,
            is_outgoing: false,
            attachments: vec![],
        })
    );
    assert_eq!(
        p.contact,
        Some(Contact {
            uuid: "u1".into(),
            phone: Some("+441234".into()),
            name: Some("Alice".into()),
        })
    );
    assert_eq!(p.dm_name, Some(("dm:u1".into(), "Alice".into())));
}

#[test]
fn outgoing_sync_dm_keys_thread_by_destination() {
    let f = json!({"envelope": {
        "sourceUuid": "me", "timestamp": 2000,
        "syncMessage": {"sentMessage": {"destinationUuid": "u2", "timestamp": 2000, "message": "yo"}}
    }});
    let p = parse_frame(&f);
    assert_eq!(
        p.action,
        Action::Message(Message {
            thread_id: ThreadId::Dm("u2".into()),
            sender: "me".into(),
            server_ts: 2000,
            // This envelope carries no server times.
            server_received_ts: None,
            server_delivered_ts: None,
            expires_in_seconds: None,
            body: Some("yo".into()),
            quote_target_ts: None,
            is_outgoing: true,
            attachments: vec![],
        })
    );
    assert_eq!(p.contact, None, "outgoing sync should not upsert a contact");
    assert_eq!(p.dm_name, None);
}

#[test]
fn group_message_keys_thread_by_group_id() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 3,
        "dataMessage": {"message": "g", "groupInfo": {"groupId": "GID=="}}
    }});
    let p = parse_frame(&f);
    match p.action {
        Action::Message(m) => {
            assert_eq!(m.thread_id, ThreadId::Group("GID==".into()));
            assert_eq!(m.thread_id.kind(), ThreadKind::Group);
            assert_eq!(m.thread_id.to_string(), "group:GID==");
        }
        other => panic!("expected Message, got {other:?}"),
    }
    assert_eq!(
        p.dm_name, None,
        "group threads aren't named from sourceName"
    );
}

#[test]
fn reaction_maps_to_reaction_action() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 900,
        "dataMessage": {"reaction": {"emoji": "👍", "targetSentTimestamp": 500, "isRemove": false}}
    }});
    let p = parse_frame(&f);
    assert_eq!(
        p.action,
        Action::Reaction(Reaction {
            thread_id: ThreadId::Dm("u1".into()),
            target_ts: 500,
            author: "u1".into(),
            emoji: Some("👍".into()),
            reaction_ts: 900,
            removed: false,
        })
    );
}

#[test]
fn remote_delete_target_sent_timestamp() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 10, "dataMessage": {"remoteDelete": {"targetSentTimestamp": 777}}
    }});
    assert_eq!(
        parse_frame(&f).action,
        Action::Delete {
            sender: "u1".into(),
            target_ts: 777
        }
    );
}

#[test]
fn remote_delete_timestamp_fallback_field() {
    // Some signal-cli versions use `timestamp` instead of `targetSentTimestamp`.
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 10, "dataMessage": {"remoteDelete": {"timestamp": 888}}
    }});
    assert_eq!(
        parse_frame(&f).action,
        Action::Delete {
            sender: "u1".into(),
            target_ts: 888
        }
    );
}

#[test]
fn outgoing_remote_delete_marks_self_sender() {
    let f = json!({"envelope": {
        "sourceUuid": "me", "timestamp": 5,
        "syncMessage": {"sentMessage": {"destinationUuid": "u2", "timestamp": 5, "remoteDelete": {"targetSentTimestamp": 999}}}
    }});
    assert_eq!(
        parse_frame(&f).action,
        Action::Delete {
            sender: "me".into(),
            target_ts: 999
        }
    );
}

/// The fixture is the record signal-cli sends: `JsonSticker` is
/// `(String packId, int stickerId)`, with no emoji.
#[test]
fn sticker_only_message_names_the_pack_and_the_index() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7,
        "dataMessage": {"sticker": {"packId": "9acc9e8aba563d26a4994e69263e3b25", "stickerId": 4}}
    }});
    match parse_frame(&f).action {
        Action::Message(m) => assert_eq!(
            m.body,
            Some("[sticker 9acc9e8aba563d26a4994e69263e3b25#4]".into())
        ),
        other => panic!("expected Message, got {other:?}"),
    }
}

#[test]
fn a_sticker_with_no_ids_is_still_marked() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 8, "dataMessage": {"sticker": {}}
    }});
    match parse_frame(&f).action {
        Action::Message(m) => assert_eq!(m.body, Some("[sticker]".into())),
        other => panic!("expected Message, got {other:?}"),
    }
}

#[test]
fn attachment_and_quote_are_extracted() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 8,
        "dataMessage": {
            "message": "see pic", "quote": {"id": 111},
            "attachments": [{"id": "AID", "contentType": "image/jpeg", "filename": "x.jpg", "size": 123}]
        }
    }});
    match parse_frame(&f).action {
        Action::Message(m) => {
            assert_eq!(m.quote_target_ts, Some(111));
            assert_eq!(
                m.attachments,
                vec![Attachment {
                    id: Some("AID".into()),
                    content_type: Some("image/jpeg".into()),
                    file_name: Some("x.jpg".into()),
                    size: Some(123),
                }]
            );
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[test]
fn jsonrpc_params_wrapped_envelope_is_accepted() {
    let f = json!({"jsonrpc": "2.0", "method": "receive", "params": {"envelope": {
        "sourceUuid": "u1", "timestamp": 12, "dataMessage": {"message": "wrapped"}
    }}});
    match parse_frame(&f).action {
        Action::Message(m) => assert_eq!(m.body, Some("wrapped".into())),
        other => panic!("expected Message, got {other:?}"),
    }
}

/// Typing is skipped, and so is a frame the parser does not recognise.
#[test]
fn typing_and_unknown_frames_are_skipped() {
    let typing = json!({"envelope": {"sourceUuid": "u1", "typingMessage": {"action": "STARTED"}}});
    let junk = json!({"hello": "world"});
    assert_eq!(parse_frame(&typing).action, Action::Skip);
    assert_eq!(parse_frame(&junk).action, Action::Skip);
}

#[test]
fn incoming_edit_maps_to_edit_action() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "sourceName": "Alice", "timestamp": 2000,
        "editMessage": {"targetSentTimestamp": 1000, "dataMessage": {"message": "fixed typo"}}
    }});
    let p = parse_frame(&f);
    assert_eq!(
        p.action,
        Action::Edit(Edit {
            thread_id: ThreadId::Dm("u1".into()),
            sender: "u1".into(),
            edit_ts: 2000,
            target_ts: 1000,
            body: Some("fixed typo".into()),
            is_outgoing: false,
        })
    );
    // An incoming edit still refreshes the contact and DM name.
    assert_eq!(
        p.contact,
        Some(Contact {
            uuid: "u1".into(),
            phone: None,
            name: Some("Alice".into())
        })
    );
    assert_eq!(p.dm_name, Some(("dm:u1".into(), "Alice".into())));
}

#[test]
fn outgoing_sync_edit_maps_to_edit_action() {
    let f = json!({"envelope": {
        "sourceUuid": "me", "timestamp": 50,
        "syncMessage": {"sentMessage": {
            "destinationUuid": "u2", "timestamp": 3000,
            "editMessage": {"targetSentTimestamp": 1500, "dataMessage": {"message": "edited (sync)"}}
        }}
    }});
    assert_eq!(
        parse_frame(&f).action,
        Action::Edit(Edit {
            thread_id: ThreadId::Dm("u2".into()),
            sender: "me".into(),
            edit_ts: 3000,
            target_ts: 1500,
            body: Some("edited (sync)".into()),
            is_outgoing: true,
        })
    );
}

#[test]
fn group_edit_keys_thread_by_group_id() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 2000,
        "editMessage": {"targetSentTimestamp": 1000,
            "dataMessage": {"message": "g edit", "groupInfo": {"groupId": "GID=="}}}
    }});
    match parse_frame(&f).action {
        Action::Edit(e) => {
            assert_eq!(e.thread_id, ThreadId::Group("GID==".into()));
            assert_eq!(e.thread_id.kind(), ThreadKind::Group);
        }
        other => panic!("expected Edit, got {other:?}"),
    }
}

#[test]
fn timestamp_falls_back_to_data_message_timestamp() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "dataMessage": {"message": "no-env-ts", "timestamp": 4242}
    }});
    match parse_frame(&f).action {
        Action::Message(m) => assert_eq!(m.server_ts, 4242),
        other => panic!("expected Message, got {other:?}"),
    }
}

#[test]
fn message_with_no_timestamp_anywhere_is_skipped() {
    // No timestamp anywhere: skipped rather than stored at ts=0.
    let f = json!({"envelope": {"sourceUuid": "u1", "dataMessage": {"message": "no ts"}}});
    assert_eq!(parse_frame(&f).action, Action::Skip);
}

#[test]
fn outgoing_sync_with_no_timestamp_is_skipped() {
    let f = json!({"envelope": {
        "sourceUuid": "me",
        "syncMessage": {"sentMessage": {"destinationUuid": "u2", "message": "no ts"}}
    }});
    assert_eq!(parse_frame(&f).action, Action::Skip);
}

#[test]
fn edit_with_no_target_timestamp_is_skipped() {
    // An edit with no targetSentTimestamp can't be linked to its original.
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 2000,
        "editMessage": {"dataMessage": {"message": "orphan edit"}}
    }});
    assert_eq!(parse_frame(&f).action, Action::Skip);
}

/// One `receiptMessage` can report delivery and read at once; the stronger wins.
#[test]
fn a_receipt_reports_the_strongest_thing_it_says() {
    let both = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 5000,
        "receiptMessage": {"when": 4321, "isDelivery": true, "isRead": true,
                           "isViewed": false, "timestamps": [1000, 2000]}
    }});
    assert_eq!(
        parse_frame(&both).action,
        Action::Receipt(Receipt {
            author: "u1".into(),
            kind: ReceiptKind::Read,
            when_ts: 4321,
            targets: vec![1000, 2000],
        }),
        "delivery AND read is a READ receipt, and it acknowledges BOTH messages"
    );

    let delivered = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 5000,
        "receiptMessage": {"when": 4000, "isDelivery": true, "isRead": false,
                           "isViewed": false, "timestamps": [1000]}
    }});
    let Action::Receipt(r) = parse_frame(&delivered).action else {
        panic!("a delivery receipt is a receipt");
    };
    assert_eq!(r.kind, ReceiptKind::Delivery);

    // A receipt naming no message cannot be attached to one.
    let empty = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 5000,
        "receiptMessage": {"when": 4000, "isDelivery": true, "timestamps": []}
    }});
    assert_eq!(parse_frame(&empty).action, Action::Skip);
}

/// Our own read, synced from another device inside `syncMessage`; the author
/// is us.
#[test]
fn reading_on_the_phone_is_a_receipt_from_ourselves() {
    let f = json!({"envelope": {
        "sourceUuid": "me", "timestamp": 9000,
        "syncMessage": {"readMessages": [
            {"senderUuid": "u1", "timestamp": 1000},
            {"senderUuid": "u2", "timestamp": 2000}
        ]}
    }});
    assert_eq!(
        parse_frame(&f).action,
        Action::Receipt(Receipt {
            author: "me".into(),
            kind: ReceiptKind::Read,
            when_ts: 9000,
            targets: vec![1000, 2000],
        })
    );
}

/// Each call signalling frame is its own row.
#[test]
fn a_call_arrives_as_the_frames_it_is_made_of() {
    let offer = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7000,
        "callMessage": {"offerMessage": {"id": 42, "type": "audio_call", "opaque": "x"}}
    }});
    assert_eq!(
        parse_frame(&offer).action,
        Action::Call(CallEvent {
            call_id: 42,
            peer: "u1".into(),
            event: CallEventKind::Offer,
            detail: Some("audio_call".into()),
            device_id: None,
            event_ts: 7000,
        })
    );

    let hangup = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7600,
        "callMessage": {"hangupMessage": {"id": 42, "type": "normal", "deviceId": 2}}
    }});
    let Action::Call(c) = parse_frame(&hangup).action else {
        panic!("a hangup is a call event");
    };
    assert_eq!(
        (c.call_id, c.event, c.device_id),
        (42, CallEventKind::Hangup, Some(2))
    );

    let ice = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7100,
        "callMessage": {"iceUpdateMessages": [{"id": 42, "opaque": "y"}]}
    }});
    assert_eq!(parse_frame(&ice).action, Action::Skip);
}

// ---- what Signal would call somebody ---------------------------------------

/// The fixtures are real `/v1/contacts` records. The order under test is
/// signal-cli 0.14.7's `getContactOrProfileName`: nickname, else system contact
/// name, else profile name.
#[test]
fn a_nickname_outranks_the_profile_name_the_person_chose() {
    // She calls herself Tata; Pippijn typed Tania Boiko.
    let c = json!({
        "uuid": "fb07a20e", "name": "", "given_name": "",
        "profile": {"given_name": "Tata", "lastname": ""},
        "nickname": {"name": "", "given_name": "Tania", "family_name": "Boiko"}
    });
    assert_eq!(display_name_of(&c).as_deref(), Some("Tania Boiko"));
}

/// The common shape: an address-book name, no nickname, a profile name beneath.
#[test]
fn the_address_book_outranks_the_profile_name() {
    let c = json!({
        "uuid": "u1", "name": "Alice Andersson", "given_name": "Alice",
        "profile": {"given_name": "ali", "lastname": ""},
        "nickname": {"name": "", "given_name": "", "family_name": ""}
    });
    assert_eq!(display_name_of(&c).as_deref(), Some("Alice Andersson"));
}

/// signal-cli sends the nickname object with blank strings rather than
/// omitting it.
#[test]
fn blank_name_fields_fall_through_rather_than_winning() {
    let c = json!({
        "uuid": "u2", "name": "", "given_name": "",
        "profile": {"given_name": "Carol", "lastname": "Danvers"},
        "nickname": {"name": "", "given_name": "", "family_name": ""}
    });
    assert_eq!(display_name_of(&c).as_deref(), Some("Carol Danvers"));
}

/// Half a name is still a name; `getDisplayNickname` joins what it has.
#[test]
fn one_half_of_a_name_is_used_without_a_stray_space() {
    let given = json!({"uuid": "u3", "nickname": {"given_name": "Mononym"}});
    assert_eq!(display_name_of(&given).as_deref(), Some("Mononym"));
    let family = json!({"uuid": "u4", "nickname": {"family_name": "Surname"}});
    assert_eq!(display_name_of(&family).as_deref(), Some("Surname"));
}

/// `None`, not an empty string, which `upsert_contact` would store.
#[test]
fn a_contact_with_no_name_anywhere_resolves_to_nothing() {
    let c = json!({"uuid": "u5", "name": "", "given_name": "",
                   "profile": {"given_name": "", "lastname": ""},
                   "nickname": {"name": "", "given_name": "", "family_name": ""}});
    assert_eq!(display_name_of(&c), None);
    assert_eq!(display_name_of(&json!({"uuid": "u6"})), None);
}

// ---- the times Signal puts on, and the timer it was sent under --------------

/// The fixture is a real frame from `signal_frames`, trimmed.
#[test]
fn a_message_carries_signals_own_times_and_its_timer() {
    let f = json!({"envelope": {
        "source": "+447700900123", "sourceUuid": "u1", "sourceName": "Someone",
        "timestamp": 1790001402745i64,
        "serverReceivedTimestamp": 1790001399818i64,
        "serverDeliveredTimestamp": 1790001400170i64,
        "dataMessage": {"message": "hello", "timestamp": 1790001402745i64, "expiresInSeconds": 604800}
    }});
    match parse_frame(&f).action {
        Action::Message(m) => {
            // Distinct from the server times, so returning the wrong one fails.
            assert_eq!(m.server_ts, 1790001402745);
            assert_eq!(m.server_received_ts, Some(1790001399818));
            assert_eq!(m.server_delivered_ts, Some(1790001400170));
            assert_eq!(m.expires_in_seconds, Some(604800), "a one-week timer");
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

/// No timer and a timer of 0 are different statements.
#[test]
fn no_timer_and_a_timer_of_zero_are_different_answers() {
    let absent = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7, "dataMessage": {"message": "hi"}
    }});
    let off = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 8,
        "dataMessage": {"message": "hi", "expiresInSeconds": 0}
    }});
    let timer = |f: &serde_json::Value| match parse_frame(f).action {
        Action::Message(m) => m.expires_in_seconds,
        other => panic!("expected Message, got {other:?}"),
    };
    assert_eq!(timer(&absent), None, "no timer mentioned");
    assert_eq!(timer(&off), Some(0), "the timer was switched off");
}

/// A frame without server times leaves both `None` rather than borrowing the
/// sender's clock.
#[test]
fn missing_server_times_are_not_invented_from_the_senders_clock() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 1234, "dataMessage": {"message": "hi"}
    }});
    match parse_frame(&f).action {
        Action::Message(m) => {
            assert_eq!(m.server_ts, 1234);
            assert_eq!(m.server_received_ts, None);
            assert_eq!(m.server_delivered_ts, None);
        }
        other => panic!("expected Message, got {other:?}"),
    }
}
