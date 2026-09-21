//! Pure parsing of signal-cli-rest-api receive frames into archive actions.
//!
//! This module has NO I/O — it turns a `serde_json::Value` frame into a
//! `Parsed` describing what to write, so the mapping logic (the bug-prone part)
//! is unit-testable without a database. `main.rs` executes the resulting action.

use serde_json::Value;

/// Which kind of conversation a message belongs to. Replaces a stringly-typed
/// `"dm"`/`"group"`: the DB `conversations.type` ENUM and every call site now
/// share one type, so a typo'd kind is a compile error, not a runtime one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadKind {
    Dm,
    Group,
}

impl ThreadKind {
    /// The value stored in the `conversations.type` ENUM.
    pub fn as_str(self) -> &'static str {
        match self {
            ThreadKind::Dm => "dm",
            ThreadKind::Group => "group",
        }
    }
}

/// A conversation identity. A DM carries the other party's id (ACI UUID or
/// E.164); a group carries its group id. The `dm:`/`group:` storage prefix is
/// defined ONLY here (`Display`), and the kind is *derived* from the variant —
/// so a thread id and its kind can never disagree, and the prefix can't be
/// typo'd at a call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadId {
    Dm(String),
    Group(String),
}

impl ThreadId {
    pub fn kind(&self) -> ThreadKind {
        match self {
            ThreadId::Dm(_) => ThreadKind::Dm,
            ThreadId::Group(_) => ThreadKind::Group,
        }
    }
}

impl std::fmt::Display for ThreadId {
    /// The stored key in `conversations.thread_id` / `messages.thread_id`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ThreadId::Dm(id) => write!(f, "dm:{id}"),
            ThreadId::Group(id) => write!(f, "group:{id}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contact {
    pub uuid: String,
    pub phone: Option<String>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub id: Option<String>,
    pub content_type: Option<String>,
    pub file_name: Option<String>,
    pub size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub thread_id: ThreadId,
    pub sender: String,
    pub server_ts: i64,
    pub body: Option<String>,
    pub quote_target_ts: Option<i64>,
    pub is_outgoing: bool,
    pub attachments: Vec<Attachment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaction {
    pub thread_id: ThreadId,
    pub target_ts: i64,
    pub author: String,
    pub emoji: Option<String>,
    pub reaction_ts: i64,
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    pub thread_id: ThreadId,
    pub sender: String,
    pub edit_ts: i64,   // when the edit was made (this version's timestamp)
    pub target_ts: i64, // server_ts of the original message being edited
    pub body: Option<String>,
    pub is_outgoing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Message(Message),
    Reaction(Reaction),
    Edit(Edit),
    Delete {
        sender: String,
        target_ts: i64,
    },
    /// ⚠ **THE ONLY THING SIGNAL SAYS ONCE.** A receipt is an EVENT with its own
    /// clock — who, which messages, delivered/read/viewed, and when — not a
    /// high-water mark like Telegram's `read_outbox_max_id`. Nothing restates it,
    /// so a receipt that is not stored as it arrives is gone. See migration v37.
    Receipt(Receipt),
    /// One WebRTC signalling frame of a call. Stored raw rather than folded into
    /// a duration — see [`CallEvent`].
    Call(CallEvent),
    Skip,
}

/// Delivery, read or viewed, for one or more messages at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// Who acknowledged. For `syncMessage.readMessages` this is US, reading on
    /// another device, which is why the column is an author rather than a peer.
    pub author: String,
    pub kind: ReceiptKind,
    /// Signal's own `when`, in milliseconds — when the receipt was generated,
    /// not when we saw it.
    pub when_ts: i64,
    /// ⚠ One receipt acknowledges MANY messages. Flattening to one row per
    /// target is what makes the table answerable per message.
    pub targets: Vec<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptKind {
    Delivery,
    Read,
    Viewed,
}

impl ReceiptKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ReceiptKind::Delivery => "delivery",
            ReceiptKind::Read => "read",
            ReceiptKind::Viewed => "viewed",
        }
    }
}

/// One frame of a call's signalling.
///
/// ⚠ **RAW EVENTS, NOT A DURATION, AND THAT IS DELIBERATE.** Telegram hands over
/// a finished call as one service message with `duration` and `reason` already
/// computed. Signal hands over WebRTC signalling: an offer, maybe an answer,
/// maybe a busy, maybe a hangup, all sharing a `call_id`, each arriving as its
/// own envelope with its own timestamp. A duration is offer→hangup — but only if
/// both frames reached THIS device, and whether they do depends on where the
/// call was answered.
///
/// So this stores what arrived. Computing a duration from events we have not yet
/// seen in the wild would be inventing a state machine and then trusting it; the
/// events are the unrecoverable part and can be interpreted later, against data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallEvent {
    pub call_id: i64,
    /// The other party. A call has no thread of its own.
    pub peer: String,
    pub event: CallEventKind,
    /// `audio` or `video` on an offer; the hangup's reason on a hangup.
    pub detail: Option<String>,
    pub device_id: Option<i64>,
    /// The envelope's timestamp, in milliseconds.
    pub event_ts: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallEventKind {
    Offer,
    Answer,
    Busy,
    Hangup,
}

impl CallEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CallEventKind::Offer => "offer",
            CallEventKind::Answer => "answer",
            CallEventKind::Busy => "busy",
            CallEventKind::Hangup => "hangup",
        }
    }
}

/// The full outcome of parsing one frame: the primary action, plus optional
/// enrichment (contact to upsert / DM thread name) that the dispatcher applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
    pub action: Action,
    pub contact: Option<Contact>,
    pub dm_name: Option<(String, String)>, // (thread_id, name)
}

impl Parsed {
    fn skip() -> Self {
        Parsed {
            action: Action::Skip,
            contact: None,
            dm_name: None,
        }
    }
}

/// Prefer the stable ACI UUID; fall back to the E.164 number, then "unknown".
pub fn id_of(uuid: Option<&Value>, fallback: Option<&Value>) -> String {
    uuid.and_then(Value::as_str)
        .or_else(|| fallback.and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string()
}

/// A message belongs to its `groupInfo.groupId` group if present, else the DM
/// with `dm_peer` (the other party for incoming, the destination for outgoing).
fn thread_of(msg: &Value, dm_peer: &str) -> ThreadId {
    match msg
        .get("groupInfo")
        .and_then(|g| g.get("groupId"))
        .and_then(Value::as_str)
    {
        Some(gid) => ThreadId::Group(gid.to_string()),
        None => ThreadId::Dm(dm_peer.to_string()),
    }
}

/// Turn the message payload (dataMessage or sentMessage — same shape) into an
/// Action: a delete request, a reaction, or a stored message.
fn payload_action(msg: &Value, sender: &str, ts: i64, is_outgoing: bool, dm_peer: &str) -> Action {
    let thread_id = thread_of(msg, dm_peer);

    if let Some(rd) = msg.get("remoteDelete") {
        if let Some(target) = rd
            .get("targetSentTimestamp")
            .or_else(|| rd.get("timestamp"))
            .and_then(Value::as_i64)
        {
            return Action::Delete {
                sender: sender.to_string(),
                target_ts: target,
            };
        }
        return Action::Skip;
    }

    if let Some(reaction) = msg.get("reaction") {
        if let Some(target) = reaction.get("targetSentTimestamp").and_then(Value::as_i64) {
            return Action::Reaction(Reaction {
                thread_id,
                target_ts: target,
                author: sender.to_string(),
                emoji: reaction
                    .get("emoji")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                reaction_ts: ts,
                removed: reaction
                    .get("isRemove")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            });
        }
        return Action::Skip;
    }

    let body = match msg.get("message").and_then(Value::as_str) {
        Some(t) => Some(t.to_string()),
        // ⚠ **THIS READ `sticker.emoji`, WHICH SIGNAL-CLI HAS NEVER SENT.**
        // `JsonSticker` is `(String packId, int stickerId)` — checked against the
        // deployed tag, v0.14.5 — so the lookup always missed, `unwrap_or("")`
        // turned the miss into a blank, and every sticker in the archive reads
        // `[sticker]` with a space where the picture should be identified. Three
        // rows, and nothing in the pipeline could have said so: a field that does
        // not exist and a field that is empty are the same `None` here.
        //
        // The two fields that DO identify it are what Signal uses itself: a pack
        // and an index within it. They are recorded rather than resolved — naming
        // the sticker needs the pack manifest, which is a separate fetch, and the
        // ids keep that possible instead of leaving the body to stand for it.
        None if msg.get("sticker").is_some() => {
            let sticker = msg.get("sticker");
            let pack = sticker
                .and_then(|s| s.get("packId"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let id = sticker
                .and_then(|s| s.get("stickerId"))
                .and_then(Value::as_i64);
            Some(match (pack, id) {
                ("", None) => "[sticker]".to_string(),
                (p, Some(i)) => format!("[sticker {p}#{i}]"),
                (p, None) => format!("[sticker {p}]"),
            })
        }
        None => None,
    };
    let quote = msg
        .get("quote")
        .and_then(|q| q.get("id"))
        .and_then(Value::as_i64);
    let attachments = msg
        .get("attachments")
        .and_then(Value::as_array)
        .map(|atts| {
            atts.iter()
                .map(|a| Attachment {
                    id: a.get("id").and_then(Value::as_str).map(str::to_string),
                    content_type: a
                        .get("contentType")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    file_name: a
                        .get("filename")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    size: a.get("size").and_then(Value::as_i64),
                })
                .collect()
        })
        .unwrap_or_default();

    Action::Message(Message {
        thread_id,
        sender: sender.to_string(),
        server_ts: ts,
        body,
        quote_target_ts: quote,
        is_outgoing,
        attachments,
    })
}

/// Build an Edit action from an edit's inner dataMessage (same shape as a
/// normal message payload — has `message`, `groupInfo`, …).
fn edit_action(
    inner: &Value,
    sender: &str,
    edit_ts: i64,
    target_ts: i64,
    is_outgoing: bool,
    dm_peer: &str,
) -> Action {
    let thread_id = thread_of(inner, dm_peer);
    Action::Edit(Edit {
        thread_id,
        sender: sender.to_string(),
        edit_ts,
        target_ts,
        body: inner
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_string),
        is_outgoing,
    })
}

/// Parse one received frame. Accepts both the unwrapped `{"envelope": {...}}`
/// shape and a JSON-RPC `{"params": {"envelope": {...}}}` notification.
pub fn parse_frame(frame: &Value) -> Parsed {
    let Some(env) = frame
        .get("envelope")
        .or_else(|| frame.get("params").and_then(|p| p.get("envelope")))
    else {
        return Parsed::skip();
    };

    // Incoming edit ("edit for everyone"): editMessage wraps the new content.
    if let Some(edit) = env.get("editMessage") {
        let Some(inner) = edit.get("dataMessage") else {
            return Parsed::skip();
        };
        let sender = id_of(env.get("sourceUuid"), env.get("source"));
        let Some(edit_ts) = env
            .get("timestamp")
            .and_then(Value::as_i64)
            .or_else(|| inner.get("timestamp").and_then(Value::as_i64))
        else {
            return Parsed::skip();
        };
        let Some(target) = edit.get("targetSentTimestamp").and_then(Value::as_i64) else {
            return Parsed::skip();
        };
        let name = env.get("sourceName").and_then(Value::as_str);
        let contact = Some(Contact {
            uuid: sender.clone(),
            phone: env
                .get("sourceNumber")
                .and_then(Value::as_str)
                .map(str::to_string),
            name: name.map(str::to_string),
        });
        let action = edit_action(inner, &sender, edit_ts, target, false, &sender);
        let dm_name = match (inner.get("groupInfo").is_none(), name) {
            (true, Some(n)) => Some((ThreadId::Dm(sender.clone()).to_string(), n.to_string())),
            _ => None,
        };
        return Parsed {
            action,
            contact,
            dm_name,
        };
    }

    // Outgoing edit, synced from another of our devices.
    if let Some(sync) = env.get("syncMessage") {
        let sent = sync.get("sentMessage");
        if let Some(edit) = sync
            .get("editMessage")
            .or_else(|| sent.and_then(|s| s.get("editMessage")))
        {
            if let Some(inner) = edit.get("dataMessage") {
                let sender = id_of(env.get("sourceUuid"), env.get("source"));
                let Some(edit_ts) = sent
                    .and_then(|s| s.get("timestamp"))
                    .and_then(Value::as_i64)
                    .or_else(|| inner.get("timestamp").and_then(Value::as_i64))
                else {
                    return Parsed::skip();
                };
                let Some(target) = edit.get("targetSentTimestamp").and_then(Value::as_i64) else {
                    return Parsed::skip();
                };
                let dest = id_of(
                    sent.and_then(|s| s.get("destinationUuid")),
                    sent.and_then(|s| s.get("destination")),
                );
                let action = edit_action(inner, &sender, edit_ts, target, true, &dest);
                return Parsed {
                    action,
                    contact: None,
                    dm_name: None,
                };
            }
            return Parsed::skip();
        }
    }

    if let Some(dm) = env.get("dataMessage") {
        let sender = id_of(env.get("sourceUuid"), env.get("source"));
        let Some(ts) = env
            .get("timestamp")
            .and_then(Value::as_i64)
            .or_else(|| dm.get("timestamp").and_then(Value::as_i64))
        else {
            return Parsed::skip();
        };
        let name = env.get("sourceName").and_then(Value::as_str);
        let contact = Some(Contact {
            uuid: sender.clone(),
            phone: env
                .get("sourceNumber")
                .and_then(Value::as_str)
                .map(str::to_string),
            name: name.map(str::to_string),
        });
        let action = payload_action(dm, &sender, ts, false, &sender);
        // Name a DM thread after the other party (not for groups/deletes).
        let dm_name = match (&action, dm.get("groupInfo").is_none(), name) {
            (Action::Message(_) | Action::Reaction(_), true, Some(n)) => {
                Some((ThreadId::Dm(sender.clone()).to_string(), n.to_string()))
            }
            _ => None,
        };
        return Parsed {
            action,
            contact,
            dm_name,
        };
    }

    // ⚠ **BEFORE the syncMessage arm below, because a read receipt synced from
    // another device arrives INSIDE `syncMessage` and the `sentMessage` arm would
    // not match it — it would simply fall through to `Skip`, which is how every
    // receipt since this feed started was lost.**
    if let Some(reads) = env
        .get("syncMessage")
        .and_then(|s| s.get("readMessages"))
        .and_then(Value::as_array)
        && !reads.is_empty()
    {
        // ⚠ The AUTHOR here is us. `readMessages` is our own device telling the
        // others what we have read, so the receipt is ours about somebody else's
        // message — the mirror of an inbound `receiptMessage`, and the reason the
        // column is `author_uuid` rather than a peer.
        let me = id_of(env.get("sourceUuid"), env.get("source"));
        let targets: Vec<i64> = reads
            .iter()
            .filter_map(|r| r.get("timestamp").and_then(Value::as_i64))
            .collect();
        if !targets.is_empty() {
            return Parsed {
                action: Action::Receipt(Receipt {
                    author: me,
                    kind: ReceiptKind::Read,
                    // No `when` on a sync read; the envelope's own clock is the
                    // closest honest answer and is what we saw it at.
                    when_ts: env.get("timestamp").and_then(Value::as_i64).unwrap_or(0),
                    targets,
                }),
                contact: None,
                dm_name: None,
            };
        }
    }

    if let Some(receipt) = env.get("receiptMessage") {
        let targets: Vec<i64> = receipt
            .get("timestamps")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_i64).collect())
            .unwrap_or_default();
        // ⚠ The three flags are NOT exclusive — a single receipt can report both
        // delivery and read — so this yields the STRONGEST one rather than
        // matching. Storing only "delivery" for a frame that also said "read"
        // would record the weaker fact and silently lose the stronger.
        let flag = |k: &str| receipt.get(k).and_then(Value::as_bool).unwrap_or(false);
        let kind = if flag("isViewed") {
            Some(ReceiptKind::Viewed)
        } else if flag("isRead") {
            Some(ReceiptKind::Read)
        } else if flag("isDelivery") {
            Some(ReceiptKind::Delivery)
        } else {
            None
        };
        if let Some(kind) = kind
            && !targets.is_empty()
        {
            return Parsed {
                action: Action::Receipt(Receipt {
                    author: id_of(env.get("sourceUuid"), env.get("source")),
                    kind,
                    when_ts: receipt
                        .get("when")
                        .and_then(Value::as_i64)
                        .or_else(|| env.get("timestamp").and_then(Value::as_i64))
                        .unwrap_or(0),
                    targets,
                }),
                contact: None,
                dm_name: None,
            };
        }
        return Parsed::skip();
    }

    if let Some(call) = env.get("callMessage") {
        let peer = id_of(env.get("sourceUuid"), env.get("source"));
        let event_ts = env.get("timestamp").and_then(Value::as_i64).unwrap_or(0);
        // ⚠ `iceUpdateMessages` is deliberately absent: it is transport plumbing,
        // many frames per call carrying only opaque blobs, and storing it would
        // bury the four frames that say what happened.
        let found = [
            ("offerMessage", CallEventKind::Offer),
            ("answerMessage", CallEventKind::Answer),
            ("busyMessage", CallEventKind::Busy),
            ("hangupMessage", CallEventKind::Hangup),
        ]
        .into_iter()
        .find_map(|(key, kind)| call.get(key).map(|v| (kind, v)));
        if let Some((event, body)) = found
            && let Some(call_id) = body.get("id").and_then(Value::as_i64)
        {
            return Parsed {
                action: Action::Call(CallEvent {
                    call_id,
                    peer,
                    event,
                    detail: body.get("type").and_then(Value::as_str).map(str::to_string),
                    device_id: body.get("deviceId").and_then(Value::as_i64),
                    event_ts,
                }),
                contact: None,
                dm_name: None,
            };
        }
        return Parsed::skip();
    }

    if let Some(sent) = env.get("syncMessage").and_then(|s| s.get("sentMessage")) {
        let sender = id_of(env.get("sourceUuid"), env.get("source")); // ourselves
        let Some(ts) = sent.get("timestamp").and_then(Value::as_i64) else {
            return Parsed::skip();
        };
        let dest = id_of(sent.get("destinationUuid"), sent.get("destination"));
        let action = payload_action(sent, &sender, ts, true, &dest);
        return Parsed {
            action,
            contact: None,
            dm_name: None,
        };
    }

    Parsed::skip()
}
