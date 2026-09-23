//! Pure parsing of signal-cli-rest-api receive frames into archive actions.
//!
//! No I/O: a `serde_json::Value` frame becomes a `Parsed` describing what to
//! write, which `main.rs` executes.

use serde_json::Value;

/// Which kind of conversation a message belongs to.
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
/// E.164); a group carries its group id. `Display` gives the stored
/// `dm:`/`group:` form.
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
    /// `envelope.timestamp`: the sending device's clock, and the message's
    /// identity.
    pub server_ts: i64,
    /// When Signal's server received it.
    pub server_received_ts: Option<i64>,
    /// When Signal's server delivered it to this device.
    pub server_delivered_ts: Option<i64>,
    /// The disappearing-message timer. `None` means the frame carried none;
    /// `Some(0)` means it was turned off.
    pub expires_in_seconds: Option<i32>,
    pub body: Option<String>,
    pub quote_target_ts: Option<i64>,
    /// Who the quoted message was from, as the quote names them.
    pub quote_author: Option<String>,
    /// The quoted message's text as the quote carries it, for a target the
    /// archive does not hold.
    pub quote_text: Option<String>,
    pub is_outgoing: bool,
    pub attachments: Vec<Attachment>,
    pub styles: Vec<TextStyle>,
    pub previews: Vec<LinkPreview>,
}

/// A link preview the sender's app attached. Empty strings are absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkPreview {
    pub url: String,
    pub title: Option<String>,
    pub description: Option<String>,
}

fn link_previews(msg: &Value) -> Vec<LinkPreview> {
    let text = |p: &Value, k: &str| {
        p.get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    msg.get("previews")
        .and_then(Value::as_array)
        .map(|ps| {
            ps.iter()
                .filter_map(|p| {
                    Some(LinkPreview {
                        url: text(p, "url")?,
                        title: text(p, "title"),
                        description: text(p, "description"),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// One styled run of a message body, as Signal sends it: `style` is Signal's
/// own name (`BOLD`, `ITALIC`, `STRIKETHROUGH`, `MONOSPACE`, `SPOILER`), and the
/// positions are UTF-16 code units. Runs may overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextStyle {
    pub style: String,
    pub start_utf16: i32,
    pub length_utf16: i32,
}

/// A payload's `textStyles`, dropping any run that lacks a field or has a
/// negative position.
fn text_styles(msg: &Value) -> Vec<TextStyle> {
    let int = |r: &Value, k: &str| {
        r.get(k)
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok())
            .filter(|n| *n >= 0)
    };
    msg.get("textStyles")
        .and_then(Value::as_array)
        .map(|runs| {
            runs.iter()
                .filter_map(|r| {
                    Some(TextStyle {
                        style: r.get("style")?.as_str()?.to_string(),
                        start_utf16: int(r, "start")?,
                        length_utf16: int(r, "length")?,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
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
    pub styles: Vec<TextStyle>,
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
    /// Delivery, read or viewed; see migration v40.
    Receipt(Receipt),
    /// One WebRTC signalling frame of a call.
    Call(CallEvent),
    Skip,
}

/// Delivery, read or viewed, for one or more messages at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    /// Who acknowledged; us, for `syncMessage.readMessages`.
    pub author: String,
    pub kind: ReceiptKind,
    /// Signal's own `when`, in milliseconds.
    pub when_ts: i64,
    /// Every message this receipt acknowledges.
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

/// One frame of a call's signalling, uninterpreted; see migration v38.
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

/// The Signal server's two timestamps from the envelope, which no sender can set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServerTimes {
    pub received: Option<i64>,
    pub delivered: Option<i64>,
}

impl ServerTimes {
    /// Read them off an `envelope`.
    fn of(env: &Value) -> Self {
        ServerTimes {
            received: env.get("serverReceivedTimestamp").and_then(Value::as_i64),
            delivered: env.get("serverDeliveredTimestamp").and_then(Value::as_i64),
        }
    }
}

/// Turn the message payload (dataMessage or sentMessage — same shape) into an
/// Action: a delete request, a reaction, or a stored message.
fn payload_action(
    msg: &Value,
    sender: &str,
    ts: i64,
    is_outgoing: bool,
    dm_peer: &str,
    times: ServerTimes,
) -> Action {
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
        // signal-cli's `JsonSticker` is `(packId, stickerId)`; it has no emoji.
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
    let quote_obj = msg.get("quote");
    let quote = quote_obj.and_then(|q| q.get("id")).and_then(Value::as_i64);
    let quote_author = quote_obj
        .filter(|q| q.get("authorUuid").is_some() || q.get("author").is_some())
        .map(|q| id_of(q.get("authorUuid"), q.get("author")));
    let quote_text = quote_obj
        .and_then(|q| q.get("text"))
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(str::to_string);
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
        server_received_ts: times.received,
        server_delivered_ts: times.delivered,
        expires_in_seconds: msg
            .get("expiresInSeconds")
            .and_then(Value::as_i64)
            .and_then(|n| i32::try_from(n).ok()),
        body,
        quote_target_ts: quote,
        quote_author,
        quote_text,
        is_outgoing,
        attachments,
        styles: text_styles(msg),
        previews: link_previews(msg),
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
        styles: text_styles(inner),
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
        let action = payload_action(dm, &sender, ts, false, &sender, ServerTimes::of(env));
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

    // Our own reads, synced from another device, arrive inside `syncMessage`.
    if let Some(reads) = env
        .get("syncMessage")
        .and_then(|s| s.get("readMessages"))
        .and_then(Value::as_array)
        && !reads.is_empty()
    {
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
                    // A sync read has no `when`; the envelope's clock is closest.
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
        // The flags can co-occur; keep the strongest.
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
        // Not `iceUpdateMessages`: opaque transport, many per call.
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
        let action = payload_action(sent, &sender, ts, true, &dest, ServerTimes::of(env));
        return Parsed {
            action,
            contact: None,
            dm_name: None,
        };
    }

    Parsed::skip()
}

/// The display name Signal itself would show for a contact, by signal-cli's
/// `ManagerImpl.getContactOrProfileName` at v0.14.7:
///
/// ```text
/// final var nickname = contact.getDisplayNickname();
/// if (!Util.isEmpty(nickname)) return nickname;
/// if (!Util.isEmpty(contact.getName())) return contact.getName();
/// return profile.getDisplayName();
/// ```
///
/// `getDisplayNickname` and `getName` are each `given + " " + family`, or
/// whichever half is non-empty.
///
/// The deployed 0.14.5 lacks the nickname branch when filling
/// `envelope.sourceName`, but `/v1/contacts` serves the nickname, so this
/// applies the newer precedence to it.
pub fn display_name_of(c: &Value) -> Option<String> {
    let joined = |obj: Option<&Value>, given: &str, family: &str| -> Option<String> {
        let obj = obj?;
        let part = |k: &str| {
            obj.get(k)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        };
        match (part(given), part(family)) {
            (Some(g), Some(f)) => Some(format!("{g} {f}")),
            (Some(g), None) => Some(g.to_string()),
            (None, Some(f)) => Some(f.to_string()),
            (None, None) => None,
        }
    };
    let flat = |k: &str| {
        c.get(k)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let nick = c.get("nickname");
    // The single-field form first, as `getDisplayNickname` does.
    nick.and_then(|n| {
        n.get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    })
    .or_else(|| joined(nick, "given_name", "family_name"))
    .or_else(|| flat("name"))
    .or_else(|| joined(Some(c), "given_name", "family_name"))
    .or_else(|| joined(c.get("profile"), "given_name", "lastname"))
}
