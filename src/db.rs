//! MariaDB archive store. Each migration runs once, tracked by its array index in
//! `schema_version`: append new entries, never insert or edit one.

use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::AssertSqlSafe;
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

use crate::parse::ThreadId;

/// The MariaDB DSN, from the `DB_*` environment every binary shares.
///
/// The password is not escaped, so one containing `@` or `/` would break the DSN.
pub fn url_from_env() -> Result<String> {
    let host = std::env::var("DB_HOST").context("DB_HOST not set")?;
    let port = std::env::var("DB_PORT").unwrap_or_else(|_| "3306".to_string());
    let name = std::env::var("DB_NAME").context("DB_NAME not set")?;
    let user = std::env::var("DB_USER").context("DB_USER not set")?;
    let pass = std::env::var("DB_PASSWORD").context("DB_PASSWORD not set")?;
    Ok(format!("mysql://{user}:{pass}@{host}:{port}/{name}"))
}

const MIGRATIONS: &[&str] = &[
    // v0: people, keyed by ACI UUID (E.164 when there is none).
    r"CREATE TABLE IF NOT EXISTS contacts (
        uuid VARCHAR(64) NOT NULL PRIMARY KEY,
        phone VARCHAR(32) NULL,
        profile_name VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v1: `thread_id` is `dm:<uuid>` or `group:<id>`, the group id being signal-cli's
    // base64 `groupInfo.groupId`. The JSONL importer keys groups on the export's
    // masterKey instead, so imported and live group threads do not merge.
    r"CREATE TABLE IF NOT EXISTS conversations (
        thread_id VARCHAR(80) NOT NULL PRIMARY KEY,
        type ENUM('dm','group') NOT NULL,
        name VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v2: a Signal timestamp is unique per sender, so `(sender_uuid, server_ts)`
    // dedupes the live feed against the history import.
    r"CREATE TABLE IF NOT EXISTS messages (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        thread_id VARCHAR(80) NOT NULL,
        sender_uuid VARCHAR(64) NOT NULL,
        server_ts BIGINT NOT NULL,
        body TEXT NULL,
        quote_target_ts BIGINT NULL,
        is_outgoing TINYINT(1) NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_sender_ts (sender_uuid, server_ts),
        INDEX idx_thread_ts (thread_id, server_ts)
    )",
    // v3: attachment metadata; `stored_path` is NULL until the bytes are on the volume.
    r"CREATE TABLE IF NOT EXISTS attachments (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT NOT NULL,
        content_type VARCHAR(255) NULL,
        file_name VARCHAR(512) NULL,
        size_bytes BIGINT NULL,
        stored_path VARCHAR(1024) NULL,
        INDEX idx_msg (message_id)
    )",
    // v4: reactions, as add/remove events.
    r"CREATE TABLE IF NOT EXISTS reactions (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        thread_id VARCHAR(80) NOT NULL,
        target_ts BIGINT NOT NULL,
        author_uuid VARCHAR(64) NOT NULL,
        emoji VARCHAR(32) NULL,
        reaction_ts BIGINT NOT NULL,
        removed TINYINT(1) NOT NULL DEFAULT 0,
        UNIQUE KEY uniq_reaction (author_uuid, target_ts, reaction_ts)
    )",
    // v5: delete-for-everyone flags the row; the text is kept.
    r"ALTER TABLE messages
        ADD COLUMN deleted TINYINT(1) NOT NULL DEFAULT 0,
        ADD COLUMN deleted_at TIMESTAMP NULL",
    // v6: an edit is its own row whose `edit_of_ts` points at the original, which is
    // flagged `edited`. The current text is the group's newest row.
    r"ALTER TABLE messages
        ADD COLUMN edited TINYINT(1) NOT NULL DEFAULT 0,
        ADD COLUMN edit_of_ts BIGINT NULL,
        ADD INDEX idx_edit_of (edit_of_ts)",
    // v7: one IRC conversation per (network, target), from irssi's `autolog_path`.
    //
    // `is_status` marks irssi's server-notice window. It is named after your own
    // nick, so it looks like a DM with yourself; the flag lets a reader leave it out
    // without knowing that nick.
    r"CREATE TABLE IF NOT EXISTS irc_conversations (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        network VARCHAR(64) NOT NULL,
        target VARCHAR(255) NOT NULL,
        is_channel TINYINT(1) NOT NULL DEFAULT 0,
        is_status TINYINT(1) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_irc_conv (network, target)
    )",
    // v8: one row per logged line.
    //
    // `source_tag` is in the dedupe key because irssi tags a second simultaneous
    // connection `net2`, so one conversation-day can exist as two files. Without it
    // the second file's lines collide and INSERT IGNORE drops them.
    //
    // Seconds are always zero (irssi's default `%H:%M`); `id` keeps order within a
    // minute.
    r"CREATE TABLE IF NOT EXISTS irc_messages (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id INT NOT NULL,
        source_tag VARCHAR(64) NOT NULL,
        file_date DATE NOT NULL,
        line_no INT NOT NULL,
        sent_at DATETIME NOT NULL,
        nick VARCHAR(255) NULL,
        is_self TINYINT(1) NOT NULL DEFAULT 0,
        kind ENUM('message','action','event','notice') NOT NULL,
        text TEXT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_irc_line (conversation_id, source_tag, file_date, line_no),
        INDEX idx_irc_conv_ts (conversation_id, sent_at)
    )",
    // v9: answers the conversation list's per-conversation COUNT/MAX over
    // `kind IN ('message','action')` from the index alone. The optimizer picks it
    // only when `irc_messages` is aggregated in a derived table and then joined, and
    // `messages`' `archive.rs` keeps that shape.
    //
    // `IF NOT EXISTS`: the live database had this index before this entry did.
    "ALTER TABLE irc_messages
        ADD INDEX IF NOT EXISTS idx_irc_conv_kind_ts (conversation_id, kind, sent_at)",
    // v10: files already imported, so a run reads only what changed.
    //
    // `(mtime, size)` is rsync's own quick-check: irssi logs are append-only, so a
    // change moves both, and a content hash would mean reading every file.
    //
    // A row is written only after the file's lines land, and only under `--apply`;
    // progress recorded by a dry run would make the next real run skip work.
    r"CREATE TABLE IF NOT EXISTS irc_import_state (
        rel_path VARCHAR(512) NOT NULL PRIMARY KEY,
        mtime_ns BIGINT NOT NULL,
        size_bytes BIGINT NOT NULL,
        imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v11: per-conversation count and last time, maintained on write. The `kind`
    // filter defeats MariaDB's loose index scan and no query shape recovers it, so
    // the list reads this table instead of counting. Maintained rather than
    // refreshed because a lagging count shows in the UI.
    r"CREATE TABLE IF NOT EXISTS irc_conversation_stats (
        conversation_id INT NOT NULL PRIMARY KEY,
        cnt BIGINT NOT NULL DEFAULT 0,
        last_sent_at DATETIME NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v12: a trigger rather than application code, so every writer maintains the
    // stats: the importer, `irc_tail`, and the send echo in the `messages` repo.
    //
    // An ignored `INSERT IGNORE` fires no trigger, so replay cannot inflate a count.
    // Lines arrive out of timestamp order, hence `GREATEST`.
    //
    // A database that already holds lines needs a one-shot backfill, run with the
    // writers paused so no line is counted twice or not at all:
    //     DELETE FROM irc_conversation_stats;
    //     INSERT INTO irc_conversation_stats (conversation_id, cnt, last_sent_at)
    //     SELECT conversation_id, COUNT(*), MAX(sent_at) FROM irc_messages
    //      WHERE kind IN ('message','action') GROUP BY conversation_id;
    r"CREATE OR REPLACE TRIGGER trg_irc_stats_ai AFTER INSERT ON irc_messages FOR EACH ROW
    BEGIN
        IF NEW.kind IN ('message', 'action') THEN
            INSERT INTO irc_conversation_stats (conversation_id, cnt, last_sent_at)
                 VALUES (NEW.conversation_id, 1, NEW.sent_at)
            ON DUPLICATE KEY UPDATE
                 cnt = cnt + 1,
                 last_sent_at = GREATEST(COALESCE(last_sent_at, NEW.sent_at), NEW.sent_at);
        END IF;
    END",
    // v13: a trigger cannot recompute `MAX(sent_at)` after a delete (it may not
    // read its own table), so deletes are refused. To delete deliberately: drop this
    // trigger, delete, rebuild with v12's backfill, recreate it.
    r"CREATE OR REPLACE TRIGGER trg_irc_stats_bd BEFORE DELETE ON irc_messages FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'irc_messages is append-only: a DELETE would drift irc_conversation_stats';
    END",
    // v14: refuses only the updates that would drift the stats; `is_self`, `text`
    // and `nick` stay correctable.
    r"CREATE OR REPLACE TRIGGER trg_irc_stats_bu BEFORE UPDATE ON irc_messages FOR EACH ROW
    BEGIN
        IF NEW.conversation_id <> OLD.conversation_id
           OR NEW.kind <> OLD.kind
           OR NEW.sent_at <> OLD.sent_at THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
                'changing conversation_id, kind or sent_at would drift irc_conversation_stats';
        END IF;
    END",
    // v15: Telegram conversations. `id` is the Bot-API normalisation, folding
    // MTProto's three id spaces into one:
    //
    //     user    →  user_id
    //     chat    → -chat_id                       (a basic group)
    //     channel → -1_000_000_000_000 - channel_id
    //
    // `src/telegram/map.rs::normalise_peer` implements it. `kind` is stored rather
    // than read off the sign.
    r"CREATE TABLE IF NOT EXISTS telegram_conversations (
        id BIGINT NOT NULL PRIMARY KEY,
        kind ENUM('dm','group','channel') NOT NULL,
        name VARCHAR(255) NULL,
        username VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v16: Telegram messages. `(conversation_id, msg_id)` is Telegram's own stable
    // identity, so backfill and live stream overlap safely under INSERT IGNORE.
    //
    // `sent_at` is unix seconds, as Telegram sends it; the viewer converts units.
    // `sender_name` is the name when the row landed, denormalised so the list needs
    // no join. `kind` separates service events from messages.
    r"CREATE TABLE IF NOT EXISTS telegram_messages (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        sent_at BIGINT NOT NULL,
        sender_id BIGINT NULL,
        sender_name VARCHAR(255) NULL,
        is_outgoing TINYINT(1) NOT NULL DEFAULT 0,
        kind ENUM('message','service') NOT NULL DEFAULT 'message',
        text TEXT NULL,
        media_kind VARCHAR(32) NULL,
        edited_at BIGINT NULL,
        reply_to_msg_id INT NULL,
        fwd_from_name VARCHAR(255) NULL,
        deleted TINYINT(1) NOT NULL DEFAULT 0,
        deleted_at TIMESTAMP NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_tg_msg (conversation_id, msg_id),
        INDEX idx_tg_conv_kind_ts (conversation_id, kind, sent_at)
    )",
    // v17: reaction counts per emoji; v31 records who.
    //
    // A custom emoji has no characters, so it is stored by `custom_emoji_id` with
    // `emoji` NULL. `reaction_key` is the one non-null identity of that pair: MariaDB
    // makes primary-key columns NOT NULL, a generated column cannot be the primary
    // key, and the `''` fallback stops NULLs escaping the unique key.
    //
    // `GENERATED ALWAYS AS … STORED` rather than MariaDB's `PERSISTENT`, because
    // dev-lint's DDL parser reads only the standard spelling.
    r"CREATE TABLE IF NOT EXISTS telegram_reactions (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        emoji VARCHAR(32) NULL,
        custom_emoji_id BIGINT NULL,
        reaction_key VARCHAR(64) GENERATED ALWAYS AS
            (COALESCE(emoji, CONCAT('custom:', custom_emoji_id), '')) STORED,
        cnt INT NOT NULL DEFAULT 0,
        chosen TINYINT(1) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_tg_reaction (conversation_id, msg_id, reaction_key)
    )",
    // v18: text an edit replaced. Telegram edits in place under the same `msg_id`,
    // so the superseded text is appended here before the row is updated.
    //
    // `was_edited_at` is the replaced version's `edit_date`, NULL for the original.
    // The unique key makes replays idempotent; the NULL original is written only
    // while the stored `edited_at` is NULL.
    r"CREATE TABLE IF NOT EXISTS telegram_message_edits (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        was_edited_at BIGINT NULL,
        text TEXT NULL,
        recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_tg_edit (conversation_id, msg_id, was_edited_at),
        INDEX idx_tg_edit_msg (conversation_id, msg_id)
    )",
    // v19: backfill progress, so a restart resumes. `oldest_seen` is the lowest
    // `msg_id` stored and the next page asks for older. `complete` is set only when a
    // page comes back empty, Telegram's one end-of-history signal.
    r"CREATE TABLE IF NOT EXISTS telegram_backfill_state (
        conversation_id BIGINT NOT NULL PRIMARY KEY,
        oldest_seen INT NULL,
        complete TINYINT(1) NOT NULL DEFAULT 0,
        messages_stored BIGINT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v20: the MTProto session (auth key, DC list, peer cache, update state) as one
    // row. It is a credential: whoever reads it holds the account. Stored here it
    // shares the messages' backup and needs no writable volume.
    //
    // Losing it forces a re-login, which Telegram rate-limits for hours.
    // `single_row` prevents a second session, whose update stream would fight this
    // one. LONGTEXT because it is read and written whole.
    r"CREATE TABLE IF NOT EXISTS telegram_session (
        single_row TINYINT(1) NOT NULL PRIMARY KEY DEFAULT 1
            CHECK (single_row = 1),
        data LONGTEXT NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v21: size and mime, from the message itself with no download. NULL means not
    // yet known; the enrichment in `store_telegram_message` fills it.
    r"ALTER TABLE telegram_messages
        ADD COLUMN media_size BIGINT NULL,
        ADD COLUMN media_mime VARCHAR(128) NULL",
    // v22: Telegram's `edit_hide`: show the message as unmodified although it has an
    // `edit_date`. The edit is still recorded; this governs display. NULL, not 0, on
    // rows stored before the column, which were never told.
    r"ALTER TABLE telegram_messages ADD COLUMN edit_hidden TINYINT(1) NULL",
    // v23–v24: v21 made `media_kind` finer, but enrichment fills only NULLs, so rows
    // stored before it stay `document`. These relabel them. Idempotent.
    r"UPDATE telegram_messages SET media_kind = 'video'
       WHERE media_kind = 'document' AND media_mime LIKE 'video/%'",
    r"UPDATE telegram_messages SET media_kind = 'audio'
       WHERE media_kind = 'document' AND media_mime LIKE 'audio/%'",
    // v25: bytes this archive holds for a Telegram message; the media columns on
    // `telegram_messages` say what Telegram reported.
    //
    // Photos are fetched eagerly; larger media is `offered` and fetched when a reader
    // asks. `failed` keeps its reason in `note`.
    //
    // `stored_name` is a file name under `TELEGRAM_MEDIA_DIR`. The reader takes only
    // the name, so it cannot escape the mount.
    r"CREATE TABLE IF NOT EXISTS telegram_media (
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        state ENUM('offered','stored','failed') NOT NULL,
        stored_name VARCHAR(255) NULL,
        size_bytes BIGINT NULL,
        content_type VARCHAR(128) NULL,
        note VARCHAR(255) NULL,
        requested_at TIMESTAMP NULL,
        stored_at TIMESTAMP NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (conversation_id, msg_id),
        INDEX idx_tg_media_state (state, requested_at)
    )",
    // v26: a stat straight after `download_media` races the write and reads short or
    // zero. The size lives in `telegram_messages.media_size`.
    r"ALTER TABLE telegram_media DROP COLUMN size_bytes",
    // v27: `wanted` is the fetch queue. Only the feed holds a Telegram session and it
    // listens on no port, so the viewer writes a row and the feed polls;
    // `(state, requested_at)` makes the poll a lookup.
    r"ALTER TABLE telegram_media
        MODIFY COLUMN state ENUM('offered','wanted','stored','failed') NOT NULL",
    // v28: a forward's original sender. Telegram fills `from_name` only when that
    // account hides behind forward privacy; otherwise it sends `from_id`, normalised
    // like every other peer.
    r"ALTER TABLE telegram_messages
        ADD COLUMN fwd_from_id BIGINT NULL",
    // v29: a reaction that goes away is dated, not deleted. `removed_at IS NULL` is
    // current.
    r"ALTER TABLE telegram_reactions
        ADD COLUMN removed_at TIMESTAMP NULL",
    // v30: read marks. Telegram keeps only the current high-water marks, so a mark
    // not recorded as it happens is lost; each advance is its own row.
    //
    // `observed_at` is when we saw it: `updateReadHistoryOutbox` carries no date.
    // `direction` uses Telegram's words: `outbox` is how far they have read mine,
    // `inbox` how far I have read theirs.
    r"CREATE TABLE IF NOT EXISTS telegram_read_marks (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        direction ENUM('inbox','outbox') NOT NULL,
        max_id INT NOT NULL,
        observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_tg_read (conversation_id, direction, max_id)
    ) DEFAULT CHARSET=utf8mb4",
    // v31: who reacted, and when, from `recent_reactions`. `telegram_reactions`
    // keeps the tally, which is authoritative. A list shorter than the tally is
    // truncated: it upserts whom it names and retracts nobody.
    //
    // `reacted_at` is Telegram's own date. For `reaction_key`, see v17.
    r"CREATE TABLE IF NOT EXISTS telegram_reaction_authors (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        peer_id BIGINT NOT NULL,
        emoji VARCHAR(32) NULL,
        custom_emoji_id BIGINT NULL,
        reaction_key VARCHAR(64) GENERATED ALWAYS AS
            (COALESCE(emoji, CONCAT('custom:', custom_emoji_id), '')) STORED,
        reacted_at BIGINT NOT NULL,
        removed_at TIMESTAMP NULL,
        UNIQUE KEY uniq_tg_reaction_author (conversation_id, msg_id, peer_id, reaction_key),
        KEY idx_tg_reaction_author_peer (peer_id)
    ) DEFAULT CHARSET=utf8mb4",
    // v32: formatting entities, including links whose URL is not in the visible text
    // and mentions whose user id is the only record of who was meant.
    //
    // Offsets and lengths are UTF-16 code units, as Telegram counts. Identity is the
    // span, not the list position, so an inserted entity does not renumber the rest.
    r"CREATE TABLE IF NOT EXISTS telegram_message_entities (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        kind VARCHAR(32) NOT NULL,
        offset_utf16 INT NOT NULL,
        length_utf16 INT NOT NULL,
        url TEXT NULL,
        user_id BIGINT NULL,
        language VARCHAR(32) NULL,
        document_id BIGINT NULL,
        removed_at TIMESTAMP NULL,
        UNIQUE KEY uniq_tg_entity (conversation_id, msg_id, kind, offset_utf16, length_utf16)
    ) DEFAULT CHARSET=utf8mb4",
    // v33: the TL constructor of a service action. `text` is our English rendering,
    // and an unknown action renders as "an event".
    r"ALTER TABLE telegram_messages
        ADD COLUMN service_action VARCHAR(64) NULL",
    // v34: how a call ended and how long it took. An unanswered call has no
    // duration.
    r"CREATE TABLE IF NOT EXISTS telegram_calls (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        msg_id INT NOT NULL,
        call_id BIGINT NULL,
        duration_s INT NULL,
        reason VARCHAR(32) NULL,
        video TINYINT(1) NOT NULL DEFAULT 0,
        UNIQUE KEY uniq_tg_call (conversation_id, msg_id)
    ) DEFAULT CHARSET=utf8mb4",
    // v35: `grouped_id` is the album. `fwd_date` is when the original was written.
    // `ttl_period` is the disappearing-message timer.
    r"ALTER TABLE telegram_messages
        ADD COLUMN grouped_id BIGINT NULL,
        ADD COLUMN fwd_date BIGINT NULL,
        ADD COLUMN fwd_channel_post INT NULL,
        ADD COLUMN via_bot_id BIGINT NULL,
        ADD COLUMN ttl_period INT NULL",
    // v36: a reply can quote a fragment of its target, and the target can be in
    // another conversation.
    r"ALTER TABLE telegram_messages
        ADD COLUMN reply_quote TEXT NULL,
        ADD COLUMN reply_to_peer_id BIGINT NULL",
    // v37: dead. It was inserted rather than appended, so databases that had already
    // recorded v37 skipped it; v40 is the live copy. Removing it would renumber every
    // later entry.
    r"CREATE TABLE IF NOT EXISTS signal_receipts (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        target_ts BIGINT NOT NULL,
        author_uuid VARCHAR(64) NOT NULL,
        kind ENUM('delivery','read','viewed') NOT NULL,
        when_ts BIGINT NOT NULL,
        observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_signal_receipt (target_ts, author_uuid, kind),
        INDEX idx_signal_receipt_target (target_ts)
    ) DEFAULT CHARSET=utf8mb4",
    // v38: Signal call signalling as it arrives: offer, answer, busy and hangup
    // frames sharing a `call_id`. Stored uninterpreted, since a duration needs both
    // ends to reach this device. `iceUpdateMessages` are left out: opaque transport,
    // many per call.
    r"CREATE TABLE IF NOT EXISTS signal_call_events (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        call_id BIGINT NOT NULL,
        peer_uuid VARCHAR(64) NOT NULL,
        event ENUM('offer','answer','busy','hangup') NOT NULL,
        detail VARCHAR(32) NULL,
        device_id BIGINT NULL,
        event_ts BIGINT NOT NULL,
        observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_signal_call_event (call_id, peer_uuid, event, event_ts),
        INDEX idx_signal_call (call_id)
    ) DEFAULT CHARSET=utf8mb4",
    // v39: re-capture progress, a forward walk through stored messages whose
    // enrichment fills columns added after they landed. Separate from
    // `telegram_backfill_state`, which walks older and whose `complete` means
    // something else. Deleting a row re-runs that conversation harmlessly.
    r"CREATE TABLE IF NOT EXISTS telegram_recapture_state (
        conversation_id BIGINT NOT NULL PRIMARY KEY,
        through_msg_id INT NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) DEFAULT CHARSET=utf8mb4",
    // v40: Signal delivery, read and viewed receipts. Signal sends each once, on the
    // live socket, and nothing restates it. One receipt covers many messages, so it
    // is flattened to a row per (message, author, kind).
    r"CREATE TABLE IF NOT EXISTS signal_receipts (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        target_ts BIGINT NOT NULL,
        author_uuid VARCHAR(64) NOT NULL,
        kind ENUM('delivery','read','viewed') NOT NULL,
        when_ts BIGINT NOT NULL,
        observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_signal_receipt (target_ts, author_uuid, kind),
        INDEX idx_signal_receipt_target (target_ts)
    ) DEFAULT CHARSET=utf8mb4",
    // v41–v42: `envelope.sourceName` is signal-cli's `getContactOrProfileName`:
    // nickname (0.14.7 and later), else system contact name, else profile name.
    r"ALTER TABLE contacts ADD COLUMN display_name VARCHAR(255) NULL",
    r"UPDATE contacts SET display_name = profile_name WHERE display_name IS NULL",
    // v43: names over time. A rename closes the current row and opens a new one, so
    // an old thread can show what someone was called then. `seen_from` on a
    // backfilled row is when the archive last touched it; Signal sends no such date.
    r"CREATE TABLE IF NOT EXISTS contact_names (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        uuid VARCHAR(64) NOT NULL,
        name VARCHAR(255) NOT NULL,
        seen_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        seen_until TIMESTAMP NULL,
        INDEX idx_contact_names_current (uuid, seen_until)
    ) DEFAULT CHARSET=utf8mb4",
    // v44: seeds the history with each contact's current name.
    r"INSERT INTO contact_names (uuid, name, seen_from)
        SELECT uuid, display_name, updated_at FROM contacts WHERE display_name IS NOT NULL",
    // v45: every frame as it arrived, before parsing. Signal says everything once
    // and the columns hold only part of it; any field can later be backfilled from
    // here, as v48 does.
    //
    // Keyed by content hash: a frame has no id, and signal-cli re-delivers on every
    // reconnect.
    r"CREATE TABLE IF NOT EXISTS signal_frames (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        digest BINARY(32) NOT NULL,
        envelope_ts BIGINT NULL,
        source_uuid VARCHAR(64) NULL,
        frame JSON NOT NULL,
        received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_signal_frame (digest),
        INDEX idx_signal_frame_ts (envelope_ts),
        INDEX idx_signal_frame_source (source_uuid, envelope_ts)
    ) DEFAULT CHARSET=utf8mb4",
    // v46: `signal-ingester` is RollingUpdate, so this drop ships only after a
    // deploy that no longer writes the column.
    r"ALTER TABLE contacts DROP COLUMN profile_name",
    // v47: `server_received_ts` is Signal's clock. `server_ts` is the sender's and
    // doubles as the message's identity, so it cannot be corrected.
    // `expires_in_seconds`: NULL is no timer in the frame, 0 is the timer turned off.
    r"ALTER TABLE messages
        ADD COLUMN server_received_ts BIGINT NULL,
        ADD COLUMN server_delivered_ts BIGINT NULL,
        ADD COLUMN expires_in_seconds INT NULL",
    // v48: backfills v47 from `signal_frames`, so it reaches only as far back as the
    // frames. `JSON_VALUE` because MariaDB rejects `->>`.
    r"UPDATE messages m
        JOIN signal_frames f ON f.envelope_ts = m.server_ts
         SET m.server_received_ts = COALESCE(
                 m.server_received_ts,
                 JSON_VALUE(f.frame, '$.envelope.serverReceivedTimestamp')),
             m.server_delivered_ts = COALESCE(
                 m.server_delivered_ts,
                 JSON_VALUE(f.frame, '$.envelope.serverDeliveredTimestamp')),
             m.expires_in_seconds = COALESCE(
                 m.expires_in_seconds,
                 JSON_VALUE(f.frame, '$.envelope.dataMessage.expiresInSeconds'),
                 JSON_VALUE(f.frame, '$.envelope.syncMessage.sentMessage.expiresInSeconds'))",
];

#[derive(Clone)]
pub struct Db {
    pool: MySqlPool,
}

impl Db {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;
        let db = Self { pool };
        db.migrate().await?;
        Ok(db)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query("CREATE TABLE IF NOT EXISTS schema_version (version INT PRIMARY KEY)")
            .execute(&self.pool)
            .await?;
        // Serialise migrations across restarts/replicas with an advisory lock.
        sqlx::query("SELECT GET_LOCK('signal_migrate', 30)")
            .execute(&self.pool)
            .await?;
        let applied: Vec<i32> = sqlx::query_scalar("SELECT version FROM schema_version")
            .fetch_all(&self.pool)
            .await?;
        for (i, sql) in MIGRATIONS.iter().enumerate() {
            let v = i as i32;
            if !applied.contains(&v) {
                tracing::info!("applying migration v{v}");
                // dev-lint replays each literal as DDL but cannot follow this loop.
                // dev-lint: allow-sqlx migration runner over const literals
                sqlx::query(*sql).execute(&self.pool).await?;
                sqlx::query("INSERT INTO schema_version (version) VALUES (?)")
                    .bind(v)
                    .execute(&self.pool)
                    .await?;
            }
        }
        sqlx::query("SELECT RELEASE_LOCK('signal_migrate')")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn upsert_conversation(&self, thread: &ThreadId) -> Result<()> {
        sqlx::query(
            "INSERT INTO conversations (thread_id, type) VALUES (?, ?)
             ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP",
        )
        .bind(thread.to_string())
        .bind(thread.kind().as_str())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Set a conversation's display name (DM contact name or group title). No-op
    /// for an empty name.
    pub async fn set_conversation_name(&self, thread_id: &str, name: &str) -> Result<()> {
        if name.is_empty() {
            return Ok(());
        }
        sqlx::query("UPDATE conversations SET name = ? WHERE thread_id = ?")
            .bind(name)
            .bind(thread_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Record a contact, dating the name it wore when it is renamed.
    ///
    /// The name is written by a second statement whose `rows_affected` is the
    /// rename: a name arrives with every message, almost always unchanged, and
    /// the `<>` makes that a no-op. A sighting without a name (receipts, typing,
    /// unknown group members) never blanks one we hold.
    ///
    /// `name` is `envelope.sourceName`, a display name; see migration v41.
    pub async fn upsert_contact(
        &self,
        uuid: &str,
        phone: Option<&str>,
        name: Option<&str>,
    ) -> Result<()> {
        let phone = phone.filter(|s| !s.is_empty());
        let name = name.filter(|s| !s.is_empty());
        // The duplicate branch leaves the name alone, so a rename happens only below.
        // `rows_affected` is 1 exactly when this inserted.
        let inserted = sqlx::query(
            "INSERT INTO contacts (uuid, phone, display_name) VALUES (?, ?, ?)
             ON DUPLICATE KEY UPDATE phone = COALESCE(VALUES(phone), phone)",
        )
        .bind(uuid)
        .bind(phone)
        .bind(name)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1;

        let Some(name) = name else { return Ok(()) };

        // `IS NULL`: a contact first seen nameless gets its first name here.
        let moved = sqlx::query(
            "UPDATE contacts SET display_name = ?
              WHERE uuid = ? AND (display_name IS NULL OR display_name <> ?)",
        )
        .bind(name)
        .bind(uuid)
        .bind(name)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if !inserted && moved == 0 {
            return Ok(());
        }
        self.record_contact_name(uuid, name).await
    }

    /// Keep the frame exactly as it arrived, before parsing. Callers treat a
    /// failure here as non-fatal: losing the row beats losing the message.
    ///
    /// Returns whether the frame was new; a re-delivery hashes the same and is
    /// ignored.
    pub async fn record_signal_frame(&self, frame: &serde_json::Value) -> Result<bool> {
        use sha2::{Digest, Sha256};
        // The same bytes are hashed and stored.
        let bytes = serde_json::to_vec(frame)?;
        let digest = Sha256::digest(&bytes);
        let env = frame
            .get("envelope")
            .or_else(|| frame.get("params").and_then(|p| p.get("envelope")));
        let envelope_ts = env
            .and_then(|e| e.get("timestamp"))
            .and_then(|t| t.as_i64());
        let source_uuid = env
            .and_then(|e| e.get("sourceUuid"))
            .and_then(|s| s.as_str());
        let n = sqlx::query(
            "INSERT IGNORE INTO signal_frames (digest, envelope_ts, source_uuid, frame)
             VALUES (?, ?, ?, ?)",
        )
        .bind(&digest[..])
        .bind(envelope_ts)
        .bind(source_uuid)
        .bind(&bytes)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n > 0)
    }

    /// Close the previous name and open the current one.
    ///
    /// Separate statements because MariaDB refuses an `INSERT … WHERE NOT EXISTS`
    /// whose subquery reads the target table.
    async fn record_contact_name(&self, uuid: &str, name: &str) -> Result<()> {
        sqlx::query(
            "UPDATE contact_names SET seen_until = CURRENT_TIMESTAMP
              WHERE uuid = ? AND seen_until IS NULL AND name <> ?",
        )
        .bind(uuid)
        .bind(name)
        .execute(&self.pool)
        .await?;
        let open: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM contact_names WHERE uuid = ? AND seen_until IS NULL",
        )
        .bind(uuid)
        .fetch_one(&self.pool)
        .await?;
        if open == 0 {
            sqlx::query("INSERT INTO contact_names (uuid, name) VALUES (?, ?)")
                .bind(uuid)
                .bind(name)
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    /// Insert a message, returning its row id, or `None` for a duplicate.
    pub async fn insert_message(&self, m: &crate::parse::Message) -> Result<Option<u64>> {
        let res = sqlx::query(
            "INSERT IGNORE INTO messages
                (thread_id, sender_uuid, server_ts, body, quote_target_ts, is_outgoing,
                 server_received_ts, server_delivered_ts, expires_in_seconds)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(m.thread_id.to_string())
        .bind(&m.sender)
        .bind(m.server_ts)
        .bind(m.body.as_deref())
        .bind(m.quote_target_ts)
        .bind(m.is_outgoing)
        .bind(m.server_received_ts)
        .bind(m.server_delivered_ts)
        .bind(m.expires_in_seconds)
        .execute(&self.pool)
        .await?;
        Ok((res.rows_affected() != 0).then(|| res.last_insert_id()))
    }

    /// Flag an archived message as deleted-for-everyone (content is kept).
    /// Returns the number of rows marked (0 if we never archived the original).
    pub async fn mark_deleted(&self, sender_uuid: &str, target_ts: i64) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE messages SET deleted = 1, deleted_at = CURRENT_TIMESTAMP \
             WHERE sender_uuid = ? AND server_ts = ? AND deleted = 0",
        )
        .bind(sender_uuid)
        .bind(target_ts)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Flag an archived original as edited (content kept; edits are separate rows).
    /// Returns rows marked (0 if we never archived the original).
    pub async fn mark_edited(&self, sender_uuid: &str, target_ts: i64) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE messages SET edited = 1 \
             WHERE sender_uuid = ? AND server_ts = ? AND edit_of_ts IS NULL",
        )
        .bind(sender_uuid)
        .bind(target_ts)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    /// Store an edited version as its own row, linked to the original via edit_of_ts.
    pub async fn insert_edit(
        &self,
        thread_id: &ThreadId,
        sender_uuid: &str,
        edit_ts: i64,
        body: Option<&str>,
        edit_of_ts: i64,
        is_outgoing: bool,
    ) -> Result<()> {
        sqlx::query(
            "INSERT IGNORE INTO messages \
                (thread_id, sender_uuid, server_ts, body, is_outgoing, edit_of_ts) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(thread_id.to_string())
        .bind(sender_uuid)
        .bind(edit_ts)
        .bind(body)
        .bind(is_outgoing)
        .bind(edit_of_ts)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_attachment(
        &self,
        message_id: u64,
        content_type: Option<&str>,
        file_name: Option<&str>,
        size_bytes: Option<i64>,
        stored_path: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO attachments (message_id, content_type, file_name, size_bytes, stored_path)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(message_id)
        .bind(content_type)
        .bind(file_name)
        .bind(size_bytes)
        .bind(stored_path)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_reaction(
        &self,
        thread_id: &ThreadId,
        target_ts: i64,
        author_uuid: &str,
        emoji: Option<&str>,
        reaction_ts: i64,
        removed: bool,
    ) -> Result<()> {
        sqlx::query(
            "INSERT IGNORE INTO reactions
                (thread_id, target_ts, author_uuid, emoji, reaction_ts, removed)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(thread_id.to_string())
        .bind(target_ts)
        .bind(author_uuid)
        .bind(emoji)
        .bind(reaction_ts)
        .bind(removed)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Ensure the conversation exists and return its id. `LAST_INSERT_ID(id)`
    /// makes the duplicate branch return the existing row's id.
    pub async fn upsert_irc_conversation(
        &self,
        network: &str,
        target: &str,
        is_channel: bool,
        is_status: bool,
    ) -> Result<u64> {
        let res = sqlx::query(
            "INSERT INTO irc_conversations (network, target, is_channel, is_status)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), is_status = ?",
        )
        .bind(network)
        .bind(target)
        .bind(is_channel)
        .bind(is_status)
        .bind(is_status)
        .execute(&self.pool)
        .await?;
        Ok(res.last_insert_id())
    }

    /// Insert a log file's lines, returning how many were new.
    ///
    /// One statement per [`INSERT_CHUNK`] lines rather than per line: a history
    /// import runs over a port-forward, where each round trip is expensive.
    pub async fn insert_irc_lines(
        &self,
        conversation_id: u64,
        source_tag: &str,
        file_date: &str,
        lines: &[IrcLine],
    ) -> Result<u64> {
        let mut written = 0;
        for chunk in lines.chunks(INSERT_CHUNK) {
            let mut qb = sqlx::QueryBuilder::new(
                "INSERT IGNORE INTO irc_messages
                    (conversation_id, source_tag, file_date, line_no, sent_at, nick, is_self, kind, text) ",
            );
            qb.push_values(chunk, |mut row, line| {
                row.push_bind(conversation_id)
                    .push_bind(source_tag)
                    .push_bind(file_date)
                    .push_bind(line.line_no)
                    .push_bind(&line.sent_at)
                    .push_bind(&line.nick)
                    .push_bind(line.is_self)
                    .push_bind(line.kind)
                    .push_bind(&line.text);
            });
            written += qb.build().execute(&self.pool).await?.rows_affected();
        }
        Ok(written)
    }

    /// Every file the importer has already read, as `rel_path → (mtime_ns, size)`,
    /// read whole to save a round trip per file.
    pub async fn irc_import_state(&self) -> Result<HashMap<String, (i64, i64)>> {
        let rows = sqlx::query("SELECT rel_path, mtime_ns, size_bytes FROM irc_import_state")
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    r.try_get("rel_path")?,
                    (r.try_get("mtime_ns")?, r.try_get("size_bytes")?),
                ))
            })
            .collect()
    }

    /// Mark files as imported at the state they were read in.
    ///
    /// Call only after their lines are in, and only under `--apply`: the next run
    /// skips whatever is marked. A run that dies before a flush leaves files
    /// unmarked, which is safe.
    pub async fn record_irc_imports(&self, files: &[(String, i64, i64)]) -> Result<()> {
        for chunk in files.chunks(INSERT_CHUNK) {
            let mut qb = sqlx::QueryBuilder::new(
                "INSERT INTO irc_import_state (rel_path, mtime_ns, size_bytes) ",
            );
            qb.push_values(chunk, |mut row, (rel, mtime, size)| {
                row.push_bind(rel).push_bind(mtime).push_bind(size);
            });
            qb.push(
                " ON DUPLICATE KEY UPDATE mtime_ns = VALUES(mtime_ns), size_bytes = VALUES(size_bytes)",
            );
            qb.build().execute(&self.pool).await?;
        }
        Ok(())
    }

    /// The pool, for the Telegram session store, which implements a `grammers` trait.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    pub async fn upsert_telegram_conversation(
        &self,
        id: i64,
        kind: crate::telegram::ConvKind,
        name: Option<&str>,
        username: Option<&str>,
    ) -> Result<()> {
        // Telegram routinely sends minimal peers without a title; those must not
        // blank a stored name.
        sqlx::query(
            "INSERT INTO telegram_conversations (id, kind, name, username)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 kind = VALUES(kind),
                 name = COALESCE(VALUES(name), name),
                 username = COALESCE(VALUES(username), username)",
        )
        .bind(id)
        .bind(kind.as_str())
        .bind(name)
        .bind(username)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Store a mapped message, keeping any text it is replacing.
    ///
    /// Telegram edits in place, so the old text is appended to
    /// `telegram_message_edits` in the same transaction as the update. An
    /// unchanged `edit_date` makes replay a no-op.
    ///
    /// No `SELECT … FOR UPDATE`: a locking read of a missing row takes an InnoDB
    /// gap lock, and the backfill and live stream then deadlock. The edit is a
    /// compare-and-swap on `edited_at <=> <value read>` instead (`<=>` because
    /// that value is NULL before the first edit); losing the race changes
    /// nothing.
    pub async fn store_telegram_message(
        &self,
        row: &crate::telegram::map::Row,
        sender_name: Option<&str>,
    ) -> Result<TelegramStored> {
        let inserted = sqlx::query(
            "INSERT IGNORE INTO telegram_messages
                (conversation_id, msg_id, sent_at, sender_id, sender_name,
                 is_outgoing, kind, text, media_kind, media_size, media_mime,
                 edited_at, edit_hidden, reply_to_msg_id, fwd_from_id, fwd_from_name,
                 fwd_date, fwd_channel_post, grouped_id, via_bot_id, ttl_period,
                 reply_quote, reply_to_peer_id, service_action)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(row.conversation_id)
        .bind(row.msg_id)
        .bind(row.sent_at)
        .bind(row.sender_id)
        .bind(sender_name)
        .bind(row.is_outgoing)
        .bind(row.kind.as_str())
        .bind(row.text.as_deref())
        .bind(row.media_kind.map(|m| m.as_str()))
        .bind(row.media_size)
        .bind(row.media_mime.as_deref())
        .bind(row.edited_at)
        .bind(row.edit_hidden)
        .bind(row.reply_to_msg_id)
        .bind(row.fwd_from_id)
        .bind(row.fwd_from_name.as_deref())
        .bind(row.fwd_date)
        .bind(row.fwd_channel_post)
        .bind(row.grouped_id)
        .bind(row.via_bot_id)
        .bind(row.ttl_period)
        .bind(row.reply_quote.as_deref())
        .bind(row.reply_to_peer_id)
        .bind(row.service_action)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() != 0 {
            return Ok(TelegramStored::Inserted);
        }

        // Enrichment: fill columns the stored row has as NULL, so a re-walk can
        // populate columns added after the row landed. Every column that can be
        // NULL for a reason other than "the message has none" belongs here.
        // A known value is never overwritten: these facts do not change, so a
        // disagreement is a bug to see, not to paper over.
        let enriched = sqlx::query(
            "UPDATE telegram_messages
                SET media_kind = COALESCE(media_kind, ?),
                    media_size = COALESCE(media_size, ?),
                    media_mime = COALESCE(media_mime, ?),
                    edit_hidden = COALESCE(edit_hidden, ?),
                    fwd_from_id = COALESCE(fwd_from_id, ?),
                    fwd_from_name = COALESCE(fwd_from_name, ?),
                    sender_name = COALESCE(sender_name, ?),
                    fwd_date = COALESCE(fwd_date, ?),
                    fwd_channel_post = COALESCE(fwd_channel_post, ?),
                    grouped_id = COALESCE(grouped_id, ?),
                    via_bot_id = COALESCE(via_bot_id, ?),
                    ttl_period = COALESCE(ttl_period, ?),
                    reply_quote = COALESCE(reply_quote, ?),
                    reply_to_peer_id = COALESCE(reply_to_peer_id, ?),
                    service_action = COALESCE(service_action, ?)
              WHERE conversation_id = ? AND msg_id = ?
                AND ((media_size IS NULL AND ? IS NOT NULL)
                  OR (media_mime IS NULL AND ? IS NOT NULL)
                  OR (media_kind IS NULL AND ? IS NOT NULL)
                  OR (fwd_from_id IS NULL AND ? IS NOT NULL)
                  OR (fwd_from_name IS NULL AND ? IS NOT NULL)
                  OR (sender_name IS NULL AND ? IS NOT NULL)
                  OR (fwd_date IS NULL AND ? IS NOT NULL)
                  OR (fwd_channel_post IS NULL AND ? IS NOT NULL)
                  OR (grouped_id IS NULL AND ? IS NOT NULL)
                  OR (via_bot_id IS NULL AND ? IS NOT NULL)
                  OR (ttl_period IS NULL AND ? IS NOT NULL)
                  OR (reply_quote IS NULL AND ? IS NOT NULL)
                  OR (reply_to_peer_id IS NULL AND ? IS NOT NULL)
                  OR (service_action IS NULL AND ? IS NOT NULL)
                  OR edit_hidden IS NULL)",
        )
        .bind(row.media_kind.map(|m| m.as_str()))
        .bind(row.media_size)
        .bind(row.media_mime.as_deref())
        .bind(row.edit_hidden)
        .bind(row.fwd_from_id)
        .bind(row.fwd_from_name.as_deref())
        .bind(sender_name)
        .bind(row.fwd_date)
        .bind(row.fwd_channel_post)
        .bind(row.grouped_id)
        .bind(row.via_bot_id)
        .bind(row.ttl_period)
        .bind(row.reply_quote.as_deref())
        .bind(row.reply_to_peer_id)
        .bind(row.service_action)
        .bind(row.conversation_id)
        .bind(row.msg_id)
        .bind(row.media_size)
        .bind(row.media_mime.as_deref())
        .bind(row.media_kind.map(|m| m.as_str()))
        .bind(row.fwd_from_id)
        .bind(row.fwd_from_name.as_deref())
        .bind(sender_name)
        .bind(row.fwd_date)
        .bind(row.fwd_channel_post)
        .bind(row.grouped_id)
        .bind(row.via_bot_id)
        .bind(row.ttl_period)
        .bind(row.reply_quote.as_deref())
        .bind(row.reply_to_peer_id)
        .bind(row.service_action)
        .execute(&self.pool)
        .await?
        .rows_affected()
            != 0;

        let existing: Option<(Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT text, edited_at FROM telegram_messages
              WHERE conversation_id = ? AND msg_id = ?",
        )
        .bind(row.conversation_id)
        .bind(row.msg_id)
        .fetch_optional(&self.pool)
        .await?;

        let mut tx = self.pool.begin().await?;
        let outcome = match existing {
            // Gone between the insert and the read; the next delivery inserts it.
            None => TelegramStored::Unchanged,
            Some((_, stored_edit)) if stored_edit == row.edited_at => {
                if enriched {
                    TelegramStored::Enriched
                } else {
                    TelegramStored::Unchanged
                }
            }
            Some((stored_text, stored_edit)) => {
                // The superseded version, under the `edit_date` it carried; see v18.
                sqlx::query(
                    "INSERT IGNORE INTO telegram_message_edits
                        (conversation_id, msg_id, was_edited_at, text)
                     VALUES (?, ?, ?, ?)",
                )
                .bind(row.conversation_id)
                .bind(row.msg_id)
                .bind(stored_edit)
                .bind(stored_text.as_deref())
                .execute(&mut *tx)
                .await?;
                let updated = sqlx::query(
                    "UPDATE telegram_messages
                        SET text = ?, media_kind = ?, edited_at = ?
                      WHERE conversation_id = ? AND msg_id = ? AND edited_at <=> ?",
                )
                .bind(row.text.as_deref())
                .bind(row.media_kind.map(|m| m.as_str()))
                .bind(row.edited_at)
                .bind(row.conversation_id)
                .bind(row.msg_id)
                .bind(stored_edit)
                .execute(&mut *tx)
                .await?;
                if updated.rows_affected() == 0 {
                    // Another writer edited it since the read, and filed its history.
                    TelegramStored::Unchanged
                } else {
                    TelegramStored::Edited
                }
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    /// Set a message's reaction counts to what Telegram last reported, dating any
    /// reaction missing from a non-empty report.
    ///
    /// An empty report marks nothing: Telegram omits the field both when there are
    /// no reactions and when the delivery does not carry them. So removing the
    /// last reaction goes unrecorded.
    pub async fn replace_telegram_reactions(
        &self,
        conversation_id: i64,
        msg_id: i32,
        reactions: &[crate::telegram::map::Reaction],
    ) -> Result<()> {
        if reactions.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;

        // A reaction marked removed that reappears is current again.
        for r in reactions {
            sqlx::query(
                "INSERT INTO telegram_reactions
                    (conversation_id, msg_id, emoji, custom_emoji_id, cnt, chosen)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                    cnt = VALUES(cnt), chosen = VALUES(chosen), removed_at = NULL",
            )
            .bind(conversation_id)
            .bind(msg_id)
            .bind(r.emoji.as_deref())
            .bind(r.custom_emoji_id)
            .bind(r.cnt)
            .bind(r.chosen)
            .execute(&mut *tx)
            .await?;
        }

        // On `reaction_key`, the non-null identity: `NOT IN` over a nullable column
        // is never true.
        let keep = reactions
            .iter()
            .map(|r| match (&r.emoji, r.custom_emoji_id) {
                (Some(e), _) => e.clone(),
                (None, Some(id)) => format!("custom:{id}"),
                (None, None) => String::new(),
            })
            .collect::<Vec<_>>();
        let placeholders = vec!["?"; keep.len()].join(",");
        let sql = format!(
            "UPDATE telegram_reactions SET removed_at = CURRENT_TIMESTAMP
              WHERE conversation_id = ? AND msg_id = ? AND removed_at IS NULL
                AND reaction_key NOT IN ({placeholders})",
        );
        // Fixed template, computed placeholder count, every value bound.
        let mut q = sqlx::query(AssertSqlSafe(sql))
            .bind(conversation_id)
            .bind(msg_id);
        for k in &keep {
            q = q.bind(k);
        }
        q.execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Record who reacted. Retracts only when the list is
    /// [`complete`](crate::telegram::map::Reactions::complete): Telegram truncates
    /// `recent_reactions` for many reactors, and dating the unnamed would invent
    /// removals.
    pub async fn record_telegram_reaction_authors(
        &self,
        conversation_id: i64,
        msg_id: i32,
        reactions: &crate::telegram::map::Reactions,
    ) -> Result<()> {
        if reactions.authors.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;

        // `reacted_at` keeps its first observation.
        for a in &reactions.authors {
            sqlx::query(
                "INSERT INTO telegram_reaction_authors
                    (conversation_id, msg_id, peer_id, emoji, custom_emoji_id, reacted_at)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE removed_at = NULL",
            )
            .bind(conversation_id)
            .bind(msg_id)
            .bind(a.peer_id)
            .bind(a.emoji.as_deref())
            .bind(a.custom_emoji_id)
            .bind(a.reacted_at)
            .execute(&mut *tx)
            .await?;
        }

        if reactions.complete {
            // On `reaction_key`, as in `replace_telegram_reactions`.
            let keep = reactions
                .authors
                .iter()
                .map(|a| {
                    let key = match (&a.emoji, a.custom_emoji_id) {
                        (Some(e), _) => e.clone(),
                        (None, Some(id)) => format!("custom:{id}"),
                        (None, None) => String::new(),
                    };
                    (a.peer_id, key)
                })
                .collect::<Vec<_>>();
            let placeholders = vec!["(?,?)"; keep.len()].join(",");
            let sql = format!(
                "UPDATE telegram_reaction_authors SET removed_at = CURRENT_TIMESTAMP
                  WHERE conversation_id = ? AND msg_id = ? AND removed_at IS NULL
                    AND (peer_id, reaction_key) NOT IN ({placeholders})",
            );
            // Fixed template, computed placeholder count, every value bound.
            let mut q = sqlx::query(AssertSqlSafe(sql))
                .bind(conversation_id)
                .bind(msg_id);
            for (peer_id, key) in &keep {
                q = q.bind(peer_id).bind(key);
            }
            q.execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Record a message's entities, dating any missing from a non-empty list. An
    /// empty list marks nothing, for the reason given on
    /// [`Self::replace_telegram_reactions`].
    pub async fn replace_telegram_entities(
        &self,
        conversation_id: i64,
        msg_id: i32,
        entities: &[crate::telegram::map::Entity],
    ) -> Result<()> {
        if entities.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;

        for e in entities {
            sqlx::query(
                "INSERT INTO telegram_message_entities
                    (conversation_id, msg_id, kind, offset_utf16, length_utf16,
                     url, user_id, language, document_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                    url = VALUES(url), user_id = VALUES(user_id),
                    language = VALUES(language), document_id = VALUES(document_id),
                    removed_at = NULL",
            )
            .bind(conversation_id)
            .bind(msg_id)
            .bind(e.kind)
            .bind(e.offset_utf16)
            .bind(e.length_utf16)
            .bind(e.url.as_deref())
            .bind(e.user_id)
            .bind(e.language.as_deref())
            .bind(e.document_id)
            .execute(&mut *tx)
            .await?;
        }

        let placeholders = vec!["(?,?,?)"; entities.len()].join(",");
        let sql = format!(
            "UPDATE telegram_message_entities SET removed_at = CURRENT_TIMESTAMP
              WHERE conversation_id = ? AND msg_id = ? AND removed_at IS NULL
                AND (kind, offset_utf16, length_utf16) NOT IN ({placeholders})",
        );
        // Fixed template, computed placeholder count, every value bound.
        let mut q = sqlx::query(AssertSqlSafe(sql))
            .bind(conversation_id)
            .bind(msg_id);
        for e in entities {
            q = q.bind(e.kind).bind(e.offset_utf16).bind(e.length_utf16);
        }
        q.execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }

    /// Record how long a call was and how it ended. The service message exists
    /// from the call's start, so an early delivery must not erase a duration a
    /// later one learned.
    pub async fn record_telegram_call(
        &self,
        conversation_id: i64,
        msg_id: i32,
        call: &crate::telegram::map::Call,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO telegram_calls
                (conversation_id, msg_id, call_id, duration_s, reason, video)
             VALUES (?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                duration_s = COALESCE(duration_s, VALUES(duration_s)),
                reason = COALESCE(reason, VALUES(reason)),
                video = VALUES(video)",
        )
        .bind(conversation_id)
        .bind(msg_id)
        .bind(call.call_id)
        .bind(call.duration_s)
        .bind(call.reason)
        .bind(call.video)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record a receipt, keeping the first observation's time. Returns how many
    /// rows were new.
    pub async fn record_signal_receipt(&self, receipt: &crate::parse::Receipt) -> Result<u64> {
        let mut written = 0;
        for target in &receipt.targets {
            written += sqlx::query(
                "INSERT IGNORE INTO signal_receipts
                    (target_ts, author_uuid, kind, when_ts)
                 VALUES (?, ?, ?, ?)",
            )
            .bind(target)
            .bind(&receipt.author)
            .bind(receipt.kind.as_str())
            .bind(receipt.when_ts)
            .execute(&self.pool)
            .await?
            .rows_affected();
        }
        Ok(written)
    }

    /// Record one frame of a call's signalling. `event_ts` is in the key because
    /// a call can carry two frames of one kind, such as a hangup per device.
    pub async fn record_signal_call_event(&self, call: &crate::parse::CallEvent) -> Result<u64> {
        Ok(sqlx::query(
            "INSERT IGNORE INTO signal_call_events
                (call_id, peer_uuid, event, detail, device_id, event_ts)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(call.call_id)
        .bind(&call.peer)
        .bind(call.event.as_str())
        .bind(call.detail.as_deref())
        .bind(call.device_id)
        .bind(call.event_ts)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// The next batch of stored message ids to re-read, oldest first; empty when
    /// the conversation is done.
    pub async fn telegram_recapture_batch(
        &self,
        conversation_id: i64,
        limit: u32,
    ) -> Result<Vec<i32>> {
        Ok(sqlx::query_scalar(
            "SELECT m.msg_id FROM telegram_messages m
               WHERE m.conversation_id = ?
                 AND m.msg_id > COALESCE(
                     (SELECT s.through_msg_id FROM telegram_recapture_state s
                       WHERE s.conversation_id = m.conversation_id), 0)
               ORDER BY m.msg_id
               LIMIT ?",
        )
        .bind(conversation_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Move the re-capture frontier, after the batch is written. It never moves
    /// backwards.
    pub async fn record_telegram_recapture(
        &self,
        conversation_id: i64,
        through_msg_id: i32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO telegram_recapture_state (conversation_id, through_msg_id)
             VALUES (?, ?)
             ON DUPLICATE KEY UPDATE
                through_msg_id = GREATEST(through_msg_id, VALUES(through_msg_id))",
        )
        .bind(conversation_id)
        .bind(through_msg_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// How many messages this conversation holds.
    pub async fn telegram_message_count(&self, conversation_id: i64) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_messages WHERE conversation_id = ?")
                .bind(conversation_id)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// Record a read mark, keeping its first observation's time. Returns whether
    /// it was new. A `max_id` of 0 is Telegram saying nothing has been read.
    pub async fn record_telegram_read_mark(
        &self,
        conversation_id: i64,
        direction: TelegramReadDirection,
        max_id: i32,
    ) -> Result<bool> {
        if max_id <= 0 {
            return Ok(false);
        }
        let done = sqlx::query(
            "INSERT IGNORE INTO telegram_read_marks (conversation_id, direction, max_id)
             VALUES (?, ?, ?)",
        )
        .bind(conversation_id)
        .bind(direction.as_str())
        .bind(max_id)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() != 0)
    }

    /// The furthest mark the archive holds for one conversation and direction, and
    /// when it was first seen. `None` when nothing has been read.
    pub async fn telegram_read_mark(
        &self,
        conversation_id: i64,
        direction: TelegramReadDirection,
    ) -> Result<Option<(i32, i64)>> {
        let row: Option<(i32, i64)> = sqlx::query_as(
            "SELECT max_id, UNIX_TIMESTAMP(observed_at) FROM telegram_read_marks
              WHERE conversation_id = ? AND direction = ?
              ORDER BY max_id DESC LIMIT 1",
        )
        .bind(conversation_id)
        .bind(direction.as_str())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Whether this message's bytes are already accounted for (stored, offered or
    /// failed), so the eager pass can skip it.
    pub async fn telegram_media_state(
        &self,
        conversation_id: i64,
        msg_id: i32,
    ) -> Result<Option<String>> {
        Ok(sqlx::query_scalar(
            "SELECT state FROM telegram_media WHERE conversation_id = ? AND msg_id = ?",
        )
        .bind(conversation_id)
        .bind(msg_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Record bytes written to the volume. Call after the download returns: the
    /// row tells the reader the file is complete.
    pub async fn record_telegram_media_stored(
        &self,
        conversation_id: i64,
        msg_id: i32,
        stored_name: &str,
        content_type: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO telegram_media
                (conversation_id, msg_id, state, stored_name, content_type, stored_at)
             VALUES (?, ?, 'stored', ?, ?, CURRENT_TIMESTAMP)
             ON DUPLICATE KEY UPDATE
                 state = 'stored', stored_name = VALUES(stored_name),
                 content_type = VALUES(content_type),
                 note = NULL, stored_at = CURRENT_TIMESTAMP",
        )
        .bind(conversation_id)
        .bind(msg_id)
        .bind(stored_name)
        .bind(content_type)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record that this message's bytes are available but not fetched, or that
    /// fetching them failed and why.
    ///
    /// Never downgrades `stored`: a re-walk offers everything it sees.
    pub async fn record_telegram_media_state(
        &self,
        conversation_id: i64,
        msg_id: i32,
        state: TelegramMediaState,
        note: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO telegram_media (conversation_id, msg_id, state, note)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 state = IF(state = 'stored', 'stored', VALUES(state)),
                 note = IF(state = 'stored', note, VALUES(note))",
        )
        .bind(conversation_id)
        .bind(msg_id)
        .bind(state.as_str())
        .bind(note)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// A reader asked for these bytes. Only `offered` or `failed` rows are queued,
    /// so a stored file is not re-fetched and a second tap does not reset the
    /// clock. Returns whether anything was queued.
    pub async fn request_telegram_media(&self, row_id: i64) -> Result<bool> {
        let changed = sqlx::query(
            "UPDATE telegram_media d
               JOIN telegram_messages m
                 ON m.conversation_id = d.conversation_id AND m.msg_id = d.msg_id
                SET d.state = 'wanted', d.requested_at = CURRENT_TIMESTAMP
              WHERE m.id = ? AND d.state IN ('offered', 'failed')",
        )
        .bind(row_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(changed != 0)
    }

    /// What readers have asked for, oldest request first.
    pub async fn wanted_telegram_media(&self, limit: i64) -> Result<Vec<(i64, i32)>> {
        Ok(sqlx::query_as(
            "SELECT conversation_id, msg_id FROM telegram_media
              WHERE state = 'wanted' ORDER BY requested_at ASC LIMIT ?",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    /// How far back a conversation has been walked, or `None` if it never has.
    pub async fn telegram_backfill_state(
        &self,
        conversation_id: i64,
    ) -> Result<Option<TelegramBackfill>> {
        let row: Option<(Option<i32>, i8, i64)> = sqlx::query_as(
            "SELECT oldest_seen, complete, messages_stored
               FROM telegram_backfill_state WHERE conversation_id = ?",
        )
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(oldest_seen, complete, messages_stored)| TelegramBackfill {
                oldest_seen,
                complete: complete != 0,
                messages_stored,
            },
        ))
    }

    /// Record progress through a conversation's history. `LEAST` because the live
    /// stream reports high ids through the same path, which must not reset the
    /// frontier.
    pub async fn record_telegram_backfill(
        &self,
        conversation_id: i64,
        oldest_seen: Option<i32>,
        complete: bool,
        stored_delta: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO telegram_backfill_state
                (conversation_id, oldest_seen, complete, messages_stored)
             VALUES (?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                 oldest_seen = LEAST(COALESCE(VALUES(oldest_seen), oldest_seen),
                                     COALESCE(oldest_seen, VALUES(oldest_seen))),
                 complete = GREATEST(complete, VALUES(complete)),
                 messages_stored = messages_stored + VALUES(messages_stored)",
        )
        .bind(conversation_id)
        .bind(oldest_seen)
        .bind(complete)
        .bind(stored_delta)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Flag deleted messages, keeping their text, and return how many rows matched.
    ///
    /// `updateDeleteMessages` names no peer: private chats and basic groups share
    /// one id sequence per account. Channels have their own sequences and
    /// `updateDeleteChannelMessages`, so a peer-less deletion skips channels.
    pub async fn mark_telegram_deleted(
        &self,
        msg_ids: &[i32],
        scope: TelegramDeleteScope,
    ) -> Result<u64> {
        if msg_ids.is_empty() {
            return Ok(0);
        }
        let mut affected = 0;
        for id in msg_ids {
            let res = match scope {
                TelegramDeleteScope::Channel(conversation_id) => {
                    sqlx::query(
                        "UPDATE telegram_messages
                        SET deleted = 1, deleted_at = COALESCE(deleted_at, CURRENT_TIMESTAMP)
                      WHERE conversation_id = ? AND msg_id = ?",
                    )
                    .bind(conversation_id)
                    .bind(id)
                    .execute(&self.pool)
                    .await?
                }
                TelegramDeleteScope::SharedSequence => {
                    sqlx::query(
                        "UPDATE telegram_messages m
                       JOIN telegram_conversations c ON c.id = m.conversation_id
                        SET m.deleted = 1,
                            m.deleted_at = COALESCE(m.deleted_at, CURRENT_TIMESTAMP)
                      WHERE m.msg_id = ? AND c.kind <> 'channel'",
                    )
                    .bind(id)
                    .execute(&self.pool)
                    .await?
                }
            };
            affected += res.rows_affected();
        }
        Ok(affected)
    }
}

/// Rows per statement, well inside MySQL's 65,535-placeholder cap.
const INSERT_CHUNK: usize = 1_000;

/// What [`Db::store_telegram_message`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramStored {
    /// A message the archive had never seen.
    Inserted,
    /// A message whose text was replaced, with the old version kept.
    Edited,
    /// Already stored, and this delivery filled a NULL column.
    Enriched,
    /// Already stored, and nothing new.
    Unchanged,
}

/// How far back through a conversation the walk has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelegramBackfill {
    /// The lowest `msg_id` stored so far, or `None` before the first page.
    pub oldest_seen: Option<i32>,
    /// Set once a page came back empty, which is Telegram's only signal that a
    /// conversation has no more history.
    pub complete: bool,
    pub messages_stored: i64,
}

/// Whose reading a mark describes, in Telegram's words.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramReadDirection {
    /// How far I have read their messages.
    Inbox,
    /// How far they have read mine.
    Outbox,
}

impl TelegramReadDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            TelegramReadDirection::Inbox => "inbox",
            TelegramReadDirection::Outbox => "outbox",
        }
    }
}

/// What the archive holds, or does not, for one message's media.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramMediaState {
    /// Telegram has the bytes and we have not fetched them.
    Offered,
    /// A reader asked for it and the feed has not got to it yet.
    Wanted,
    /// On the volume.
    Stored,
    /// Tried and failed; `note` says why.
    Failed,
}

impl TelegramMediaState {
    pub fn as_str(self) -> &'static str {
        match self {
            TelegramMediaState::Offered => "offered",
            TelegramMediaState::Wanted => "wanted",
            TelegramMediaState::Stored => "stored",
            TelegramMediaState::Failed => "failed",
        }
    }
}

/// Which conversations a deletion applies to; see [`Db::mark_telegram_deleted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramDeleteScope {
    /// `updateDeleteChannelMessages`, which names its channel.
    Channel(i64),
    /// `updateDeleteMessages`, whose ids are in the sequence private chats and
    /// basic groups share.
    SharedSequence,
}

/// One logged line, ready to write.
pub struct IrcLine {
    pub line_no: u32,
    pub sent_at: String,
    pub nick: Option<String>,
    pub is_self: bool,
    pub kind: &'static str,
    pub text: String,
}
