//! Unit tests for the frame parser. Run with `cargo test`.

use serde_json::json;
use signal_archiver::parse::{
    Action, Attachment, Contact, Edit, Message, Reaction, ThreadId, ThreadKind, parse_frame,
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

#[test]
fn sticker_only_message_gets_marker_body() {
    let f = json!({"envelope": {
        "sourceUuid": "u1", "timestamp": 7, "dataMessage": {"sticker": {"emoji": "🎉"}}
    }});
    match parse_frame(&f).action {
        Action::Message(m) => assert_eq!(m.body, Some("[sticker 🎉]".into())),
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

#[test]
fn receipt_typing_and_unknown_frames_are_skipped() {
    let receipt = json!({"envelope": {"sourceUuid": "u1", "receiptMessage": {"isDelivery": true}}});
    let typing = json!({"envelope": {"sourceUuid": "u1", "typingMessage": {"action": "STARTED"}}});
    let junk = json!({"hello": "world"});
    assert_eq!(parse_frame(&receipt).action, Action::Skip);
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
