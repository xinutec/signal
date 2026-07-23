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
    Delete { sender: String, target_ts: i64 },
    Skip,
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
        None if msg.get("sticker").is_some() => {
            let emoji = msg
                .get("sticker")
                .and_then(|s| s.get("emoji"))
                .and_then(Value::as_str)
                .unwrap_or("");
            Some(format!("[sticker {emoji}]"))
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
