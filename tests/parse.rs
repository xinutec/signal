//! Unit tests for the frame parser. Run with `cargo test`.

use serde_json::json;
use signal_archiver::parse::{
    Action, Attachment, CallEvent, CallEventKind, Contact, Edit, Message, Reaction, Receipt,
    ReceiptKind, ThreadId, ThreadKind, parse_frame,
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

/// ⚠ **THIS TEST INVENTED THE FIELD IT WAS TESTING, AND SO PASSED FOR THREE
/// MONTHS WHILE THE ARCHIVE LOST EVERY STICKER'S IDENTITY.** The fixture said
/// `{"emoji": "🎉"}`; signal-cli's `JsonSticker` is `(String packId, int
/// stickerId)` and has never carried an emoji. A mock cannot contradict the thing
/// it stands for — so the only check on the shape was this file agreeing with
/// itself, and the live rows read `[sticker]` with a blank where the emoji went.
///
/// The fixture is now the record signal-cli actually sends, copied from the
/// deployed tag (v0.14.5) rather than from memory.
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

/// ⚠ **AND A STICKER WITH NEITHER FIELD STILL HAS TO SAY A STICKER WAS SENT.**
/// The old code reached that wording by accident, through a missing field and an
/// `unwrap_or("")`; it is a deliberate branch now, so the marker cannot quietly
/// become the thing it means for an unrecognised shape.
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

/// ⚠ **THIS TEST USED TO SAY RECEIPTS WERE SKIPPED, AND THAT WAS NOT A
/// DECISION.** The arm had simply never been written, and the name made the
/// omission read as intent — it went on passing after receipts started being
/// stored, because its receipt carried no `timestamps` and so skipped for an
/// entirely different reason. A green test asserting the wrong rule is worse
/// than a missing one.
///
/// What is actually skipped: typing, which is not a fact about a conversation,
/// and a frame this parser does not recognise. Receipts have their own tests.
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
    // an incoming edit still refreshes the contact + DM name
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
    // No env timestamp and none on the dataMessage: we can't anchor it in time,
    // so it's skipped rather than stored at ts=0.
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

/// ⚠ **THE THREE RECEIPT FLAGS ARE NOT EXCLUSIVE.**
///
/// A single `receiptMessage` can report delivery AND read at once, so matching
/// on them in the wrong order stores the weaker fact and loses the stronger —
/// silently, because a `delivery` row looks perfectly correct on its own. This
/// asserts the precedence rather than the arms, which is the part that can drift.
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

    // ⚠ A receipt naming nothing is not a receipt about nothing — it is a frame
    // we cannot attach to any message, and storing it would make a row that no
    // query can ever reach.
    let empty = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 5000,
        "receiptMessage": {"when": 4000, "isDelivery": true, "timestamps": []}
    }});
    assert_eq!(parse_frame(&empty).action, Action::Skip);
}

/// ⚠ A read receipt from ANOTHER OF OUR DEVICES arrives inside `syncMessage`,
/// where the `sentMessage` arm does not match it — so before this existed it
/// fell through to `Skip` like every other receipt.
///
/// The author is US: it is our own device saying what we have read.
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

/// A call is signalling frames sharing an id, not a finished call with a
/// duration — see `CallEvent`. Each frame is its own row.
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

    // ⚠ Ice updates are transport plumbing and are deliberately NOT stored: many
    // frames per call carrying opaque blobs, which would bury the four that say
    // what happened.
    let ice = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7100,
        "callMessage": {"iceUpdateMessages": [{"id": 42, "opaque": "y"}]}
    }});
    assert_eq!(parse_frame(&ice).action, Action::Skip);
}
