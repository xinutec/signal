//! MariaDB archive store. Append-only migrations, same convention as the
//! `home`/`health` services: each entry runs exactly once, tracked by index in
//! `schema_version`. To evolve the schema, APPEND a new entry — never edit an
//! existing one.

use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

use crate::parse::ThreadId;

/// The MariaDB DSN, from the `DB_*` environment the whole namespace shares.
///
/// ⚠ Here rather than in a binary because there were THREE identical copies of
/// it — `main.rs`, `irc_tail` and `import_irclogs` — and a fourth was about to be
/// written for Telegram. They agreed, which is the only reason nothing had gone
/// wrong yet; the next edit to one of them is where that would have ended.
///
/// The password is interpolated unescaped, exactly as all three copies did. That
/// is a real limit rather than an oversight: a password containing `@` or `/`
/// would produce a DSN that parses wrongly, and the fleet's does not. Changing it
/// means re-testing against the live secret, so it is recorded here instead of
/// quietly "fixed".
pub fn url_from_env() -> Result<String> {
    let host = std::env::var("DB_HOST").context("DB_HOST not set")?;
    let port = std::env::var("DB_PORT").unwrap_or_else(|_| "3306".to_string());
    let name = std::env::var("DB_NAME").context("DB_NAME not set")?;
    let user = std::env::var("DB_USER").context("DB_USER not set")?;
    let pass = std::env::var("DB_PASSWORD").context("DB_PASSWORD not set")?;
    Ok(format!("mysql://{user}:{pass}@{host}:{port}/{name}"))
}

const MIGRATIONS: &[&str] = &[
    // v0: contacts (people). Keyed by Signal ACI UUID (or E.164 if no UUID).
    r"CREATE TABLE IF NOT EXISTS contacts (
        uuid VARCHAR(64) NOT NULL PRIMARY KEY,
        phone VARCHAR(32) NULL,
        profile_name VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v1: conversations (threads). thread_id is `dm:<uuid>` or `group:<id>`, where
    // the group id is signal-cli's base64 `groupInfo.groupId` (== the groups-API
    // `internal_id`). NB the JSONL importer keys groups on the export's masterKey
    // instead — a different value — so history/live group threads don't yet merge.
    r"CREATE TABLE IF NOT EXISTS conversations (
        thread_id VARCHAR(80) NOT NULL PRIMARY KEY,
        type ENUM('dm','group') NOT NULL,
        name VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v2: messages. UNIQUE(sender_uuid, server_ts) is the dedupe key — a Signal
    // message timestamp is unique per sender, so the live feed and the one-time
    // history import (signalbackup-tools) can overlap safely (INSERT IGNORE).
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
    // v3: attachment metadata. Bytes are NOT downloaded in v1 (see main.rs note);
    // this records the pointer so a later pass can fetch + fill `stored_path`.
    r"CREATE TABLE IF NOT EXISTS attachments (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT NOT NULL,
        content_type VARCHAR(255) NULL,
        file_name VARCHAR(512) NULL,
        size_bytes BIGINT NULL,
        stored_path VARCHAR(1024) NULL,
        INDEX idx_msg (message_id)
    )",
    // v4: reactions (emoji), as discrete add/remove events.
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
    // v5: deletion tracking. When a sender "deletes for everyone", we KEEP the
    // archived message and just flag it — the content is never removed.
    r"ALTER TABLE messages
        ADD COLUMN deleted TINYINT(1) NOT NULL DEFAULT 0,
        ADD COLUMN deleted_at TIMESTAMP NULL",
    // v6: edit tracking (append-only). The ORIGINAL message is flagged
    // `edited=1`; each edited version is a separate row whose `edit_of_ts`
    // points to the original's server_ts. Current text = the row in a group
    // (original + its edits) with the greatest server_ts. Nothing is overwritten.
    r"ALTER TABLE messages
        ADD COLUMN edited TINYINT(1) NOT NULL DEFAULT 0,
        ADD COLUMN edit_of_ts BIGINT NULL,
        ADD INDEX idx_edit_of (edit_of_ts)",
    // v7: IRC conversations — one per (network, target), where the target is a
    // channel (`#name`) or a nick, straight out of irssi's `autolog_path`.
    //
    // `is_status` marks the pseudo-conversation irssi files server notices
    // into: it is named after your *own* nick, so it looks exactly like a DM
    // with yourself and is nothing of the kind — 385,012 of one network's
    // 966,039 logged lines land there. The reader needs to be able to leave it
    // out without knowing whose nick it was.
    r"CREATE TABLE IF NOT EXISTS irc_conversations (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        network VARCHAR(64) NOT NULL,
        target VARCHAR(255) NOT NULL,
        is_channel TINYINT(1) NOT NULL DEFAULT 0,
        is_status TINYINT(1) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_irc_conv (network, target)
    )",
    // v8: IRC lines, one row per logged line.
    //
    // ⚠ **`source_tag` is in the dedupe key, and that is what makes merging two
    // irssi tags into one conversation safe.** irssi invents a second tag
    // (`net2`) for a second simultaneous connection, and both write
    // `<tag>/<Y>/<M>/<D>/<target>.log` — so the same conversation on the same
    // day exists as two files, 18 such pairs in the measured tree. Keyed on
    // `(conversation, date, line)` alone the second file's lines would collide
    // with the first's and be dropped by the INSERT IGNORE, silently and
    // exactly where two connections overlapped.
    //
    // Seconds are always zero: irssi's default `timestamp_format` is `%H:%M`
    // and the date comes from the path, so `sent_at` is as precise as the
    // source. Lines within a minute keep file order, which `id` preserves.
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
    // v9: the index the viewer's conversation list needs, once the archive
    // stopped being one network.
    //
    // ⚠ MEASURED, and the first two guesses were both wrong. Opening ingestion
    // to the five networks Pippijn has tabs open on took the archive from
    // 860,380 rows to 3,683,670, and the list query — `COUNT(*)` and
    // `MAX(sent_at)` per conversation, restricted to `kind IN
    // ('message','action')` — went to **27 seconds**. That is the app's landing
    // screen.
    //
    // `idx_irc_conv_ts` cannot serve it: `kind` is not in it, so every candidate
    // row has to be read to be filtered. With `kind` between the conversation
    // and the timestamp the whole aggregate is answerable from the index alone.
    //
    // ⚠ ADDING IT IS NOT ENOUGH, which is the part worth writing down. The
    // optimizer went on choosing `uniq_irc_line` — same leading column, and a
    // row estimate 15x lower than the truth — and the query got no faster.
    // `FORCE INDEX` proved the ceiling at 3.3s, but the fix is the query's
    // shape: aggregating `irc_messages` alone in a derived table and joining
    // that to the conversations picks this index unprompted, and runs in 1.6s.
    // See `messages`' `archive.rs`, which must keep that shape for this index to
    // earn its keep.
    //
    // `IF NOT EXISTS` because this index existed on the live database before it
    // existed here: it was created by hand to measure whether it helped, which
    // is the only way that question could be answered.
    "ALTER TABLE irc_messages
        ADD INDEX IF NOT EXISTS idx_irc_conv_kind_ts (conversation_id, kind, sent_at)",
    // v10: what the importer has already read, so a run costs what is NEW.
    //
    // ⚠ MEASURED: an import took 5–7 minutes to write 3 rows. The work was never
    // proportional to what arrived — every run re-read all 36,201 staged files
    // and re-issued `INSERT IGNORE` for all 3.68M lines, letting the unique key
    // throw away 99.9999% of them. Hourly was a consequence of that cost, not a
    // decision about latency.
    //
    // `(mtime, size)` rather than a content hash: irssi's logs are append-only,
    // so a change always moves both, and hashing would mean reading every file —
    // which is the cost being removed. It is also exactly the pair `rsync`'s own
    // quick-check uses, so a file rsync did not transfer is a file this skips,
    // and the two cannot disagree about what changed.
    //
    // ⚠ The row is written AFTER the lines land, so a file that fails mid-import
    // is simply not marked and the next run does it again. And the importer only
    // writes here under `--apply`: a dry run that recorded progress would make
    // the next real run skip work it never did.
    r"CREATE TABLE IF NOT EXISTS irc_import_state (
        rel_path VARCHAR(512) NOT NULL PRIMARY KEY,
        mtime_ns BIGINT NOT NULL,
        size_bytes BIGINT NOT NULL,
        imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v11: the viewer's landing screen, answered without reading the messages.
    //
    // ⚠ MEASURED, and the obvious rewrite was measured WRONG. The list needs
    // `COUNT(*)` and `MAX(sent_at)` per conversation over `kind IN
    // ('message','action')`. Without that filter MariaDB answers it with a loose
    // index scan — `Using index for group-by`, **431 rows, 1.4ms**. With it, the
    // filter sits on the middle column of `idx_irc_conv_kind_ts` and the plan
    // becomes a full index scan: **3,614,079 rows, 1.29s**.
    //
    // Rewriting `IN ('message','action')` as a UNION of two `kind = …` groups is
    // what the loose-index-scan documentation suggests, and it is slower, not
    // faster: 2.15s for the MAX and 1.75s for the COUNT, because it buys two
    // scans instead of one. There is no query shape that recovers the loose scan
    // while the filter stands, and 0.75s is the floor for counting by scanning.
    // So the read stops scanning: one row per conversation, maintained on write.
    //
    // `cnt`/`last_sent_at` rather than a materialised view because MariaDB has
    // none, and rather than a periodic refresh because a count that lags is the
    // bug this app already had once — the UI showed a total three behind the
    // database and it was noticed.
    r"CREATE TABLE IF NOT EXISTS irc_conversation_stats (
        conversation_id INT NOT NULL PRIMARY KEY,
        cnt BIGINT NOT NULL DEFAULT 0,
        last_sent_at DATETIME NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v12: maintain it on insert.
    //
    // ⚠ A TRIGGER RATHER THAN APPLICATION CODE, and that is the point. THREE
    // writers insert these rows — the importer and `irc_tail` here, and the send
    // echo in the `messages` repo — so maintaining the count in Rust would mean
    // the same logic in two repositories, and a fourth writer would silently not
    // maintain it. The trigger is attached to the table, so every writer that
    // exists or ever will is covered by construction.
    //
    // ⚠ **An `INSERT IGNORE` that ignores fires no trigger** — verified, not
    // assumed. That is what keeps replay free: `irc_tail` re-offers the plugin's
    // whole ring after every restart and the importer re-reads a file whenever
    // its mtime moves, and neither can inflate a count.
    //
    // `GREATEST(COALESCE(last_sent_at, NEW.sent_at), NEW.sent_at)` because lines
    // do NOT arrive in timestamp order: the importer walks files by path, so
    // yesterday's log can land after today's, and a plain assignment would move
    // the conversation's last-message time backwards.
    //
    // ⚠ A FRESH DATABASE NEEDS NO BACKFILL — the triggers maintain from row
    // zero. Only a database that already held rows when this landed does, and
    // that is a one-shot with the writers paused:
    //     DELETE FROM irc_conversation_stats;
    //     INSERT INTO irc_conversation_stats (conversation_id, cnt, last_sent_at)
    //     SELECT conversation_id, COUNT(*), MAX(sent_at) FROM irc_messages
    //      WHERE kind IN ('message','action') GROUP BY conversation_id;
    // Paused because otherwise a row inserted between the aggregate's snapshot
    // and its write is counted by the trigger and by the aggregate, or by
    // neither, depending on which side of the statement it lands.
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
    // v13: refuse the delete rather than drift.
    //
    // A count kept incrementally can be maintained through an insert and cannot
    // be maintained through a delete: recovering `MAX(sent_at)` after removing
    // the newest line means re-reading the conversation, and MariaDB forbids a
    // trigger from reading the table it is defined on. The archive is append-only
    // by design — nothing in either repo issues a DELETE — so the honest move is
    // to make the unmaintainable case impossible to express rather than to let it
    // silently produce a wrong number.
    //
    // To genuinely delete: drop this trigger, delete, rebuild the affected rows
    // with the backfill statement in v12, and recreate it.
    r"CREATE OR REPLACE TRIGGER trg_irc_stats_bd BEFORE DELETE ON irc_messages FOR EACH ROW
    BEGIN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'irc_messages is append-only: a DELETE would drift irc_conversation_stats';
    END",
    // v14: refuse only the updates that would drift it.
    //
    // ⚠ Deliberately NOT a blanket refusal. `is_self` has already needed
    // correcting in production — one row filed as somebody else's before
    // `irc_tail` learned the self-nicks — and `text`/`nick` are equally
    // repairable. None of those three change a count or a last-message time.
    // Only `conversation_id`, `kind` and `sent_at` do, and those are the three
    // this refuses.
    r"CREATE OR REPLACE TRIGGER trg_irc_stats_bu BEFORE UPDATE ON irc_messages FOR EACH ROW
    BEGIN
        IF NEW.conversation_id <> OLD.conversation_id
           OR NEW.kind <> OLD.kind
           OR NEW.sent_at <> OLD.sent_at THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
                'changing conversation_id, kind or sent_at would drift irc_conversation_stats';
        END IF;
    END",
    // v15: Telegram conversations — the fourth origin, and the first whose
    // history and live feed come from ONE login.
    //
    // ⚠ **`id` is the Bot-API normalisation, not the raw MTProto id, and that is
    // deliberate.** MTProto names a peer by a `user_id`, a `chat_id` or a
    // `channel_id`, each from its own space, so the raw number identifies a
    // conversation only when you also carry which of the three it was. Every
    // Telegram tool folds them into one signed space instead, and this does too:
    //
    //     user    →  user_id
    //     chat    → -chat_id                       (a basic group)
    //     channel → -1_000_000_000_000 - channel_id
    //
    // so a conversation is one BIGINT the viewer can put in a URL, and no user
    // can collide with a group. `kind` is still stored, because recovering it by
    // looking at the sign of an id is exactly the kind of cleverness that reads
    // as a bug three years later. `src/telegram/map.rs::normalise_peer` is the
    // one implementation and is unit-tested against all three.
    //
    // `kind` has a value the other origins do not: a `channel` is a broadcast
    // with an audience rather than a conversation, so the viewer may well want
    // to leave it out of a list of people. Stored, not filtered, here.
    r"CREATE TABLE IF NOT EXISTS telegram_conversations (
        id BIGINT NOT NULL PRIMARY KEY,
        kind ENUM('dm','group','channel') NOT NULL,
        name VARCHAR(255) NULL,
        username VARCHAR(255) NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v16: Telegram messages.
    //
    // **The dedupe key is honest here in a way the other origins' are not.** A
    // Telegram message id is assigned by the server and is stable for the life
    // of the message, so `(conversation_id, msg_id)` IS the message's identity —
    // no guessing that a timestamp is unique per sender (Signal) and no
    // synthesising a key out of a file path and a line number (IRC). Backfill
    // and the live stream therefore overlap for free under `INSERT IGNORE`, and
    // a re-run of either costs nothing.
    //
    // `sent_at` is unix SECONDS, UTC — Telegram's own unit, stored unconverted
    // for the reason `gchat_messages.ts_us` keeps microseconds: the viewer
    // normalises units at the edge (`archive.rs`), and a conversion on the way
    // IN is a conversion that cannot be checked against the source afterwards.
    // Seconds is also all Telegram gives: there is no sub-second field in the
    // message constructor.
    //
    // `sender_name` is denormalised, as `gchat_messages` does it and for the same
    // reason: the conversation list's index lesson (v9/v11) was that a read which
    // has to join to name a row is a read that scans. What it records is the name
    // the archive SAW when the row landed, which is not the same as the name the
    // person has now — that is a property of an archive, not a defect.
    //
    // `media_kind` is a label, not a file. Bytes are Signal-only in this archive
    // (the `attachments` PVC and the viewer's `/attachments` route), so a
    // Telegram photo is recorded as having been a photo and is not downloaded.
    // The column is what a later pass would fill; nothing fills it today.
    //
    // `kind` separates a message from a service event ("X joined", a pinned
    // notice) the way `irc_messages.kind` separates a line from a join — same
    // problem, same answer, so the viewer can restrict to what was SAID.
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
    // v17: reactions, aggregated per emoji.
    //
    // Closer to `gchat_reactions` than to Signal's: Telegram hands over a count
    // per reaction rather than a stream of add/remove events, so this stores what
    // it is given. The consequence is the one the viewer already documents for
    // Google Chat — you can see that four people laughed, not which four.
    //
    // `emoji` holds a unicode emoticon; a custom emoji is a document id with no
    // characters to show, so it is recorded as its id under `custom_emoji_id` and
    // `emoji` stays NULL. A reader that cannot draw one at least knows it is
    // there rather than silently counting nothing.
    //
    // ⚠ **`reaction_key` IS GENERATED, AND THE OBVIOUS KEY DOES NOT WORK.** The
    // identity of a reaction is "the emoji, or the custom emoji's id" — a sum, one
    // side of which is always NULL. A primary key over `(…, emoji,
    // custom_emoji_id)` is what this entry said while it was being written, and
    // MariaDB silently makes every primary-key column NOT NULL: the result
    // rejected every unicode reaction, with `Column 'custom_emoji_id' cannot be
    // null`. It would have failed on the first reaction in production and been
    // caught by nothing, because no mapping test touches SQL.
    //
    // (Corrected in place rather than by appending a repair, because v15–v20 had
    // never been applied anywhere but a throwaway test database. The append-only
    // rule at the top of this file is about migrations that have RUN.)
    //
    // So the database derives one non-null identity from the pair. Generated
    // rather than composed by the writer for the reason the IRC stats are a
    // trigger: a second writer cannot forget to do it.
    //
    // ⚠ And it is a UNIQUE KEY over a surrogate primary key, not the primary key
    // itself: MariaDB answers `PRIMARY KEY (…, reaction_key)` with "Primary key
    // cannot be defined upon a generated column" — and because a failed migration
    // stops the whole list, that mistake took every test in this repository down,
    // not just the Telegram ones. The `COALESCE(…, '')` tail is what keeps the
    // unique key honest: a NULL never compares equal to another NULL, so without
    // a non-null fallback the index would happily hold duplicates.
    //
    // ⚠ `GENERATED ALWAYS AS (…) STORED` and not MariaDB's own `AS (…) PERSISTENT`,
    // which means the identical thing and which MariaDB documents first. The
    // spelling matters because dev-lint parses this DDL to know which columns
    // exist, and it could not read the MariaDB-only form — so it printed
    // "unparseable DDL disables absence checks" and went quietly green over this
    // table while still passing the gate. A check that has silently stopped
    // checking is worse than one that fails, so the schema uses the spelling both
    // engines and the linter understand.
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
    // v18: the text a message used to have.
    //
    // ⚠ **Telegram's edit is a MUTATION, which is why this table exists.** Signal
    // sends an edit as a new message pointing at the original (v6 above), so its
    // history is the archive's natural shape — append a row and nothing is lost.
    // Telegram sends the SAME `msg_id` with different text, so an archive that
    // only upserts `telegram_messages` would overwrite the words it exists to
    // keep, silently, with no trace that there had been others.
    //
    // So the row in `telegram_messages` is the CURRENT text, and the text it is
    // replacing is appended here first. `was_edited_at` is the `edit_date` the
    // superseded version carried, NULL for the original — which is what orders
    // the chain, since Telegram gives no revision number.
    //
    // ⚠ An edit seen twice must not append twice. The unique key is
    // `(conversation_id, msg_id, was_edited_at)`, so a replayed update — the
    // whole point of `catch_up` — is idempotent. NULL does not compare equal to
    // NULL in a unique index, so the ORIGINAL text is instead guarded by only
    // being written when the row's stored `edited_at` was NULL: the first edit is
    // the only moment the pre-edit text is knowable at all.
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
    // v19: how far back each conversation has been walked, so a backfill is
    // resumable and a restart costs what is LEFT rather than what is done.
    //
    // The lesson `irc_import_state` (v10) records, applied before it can be
    // learned the expensive way: an import whose cost is the size of the archive
    // rather than the size of what is new gets run rarely, and a feed that is run
    // rarely is a feed that is behind.
    //
    // `oldest_seen` is the lowest `msg_id` this walk has stored; the next page
    // asks Telegram for what is older than it. `complete` is set when a page
    // comes back empty, which is the only signal Telegram gives that a
    // conversation has no more history — and it is recorded ONLY then, so an
    // interrupted walk resumes rather than declaring itself finished.
    r"CREATE TABLE IF NOT EXISTS telegram_backfill_state (
        conversation_id BIGINT NOT NULL PRIMARY KEY,
        oldest_seen INT NULL,
        complete TINYINT(1) NOT NULL DEFAULT 0,
        messages_stored BIGINT NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
    // v20: the MTProto session — the authorisation key, the datacentre list, the
    // peer cache and the update state — as one row.
    //
    // ⚠ **IN THE DATABASE RATHER THAN ON A VOLUME, and it is a credential.** A
    // row here is a logged-in Telegram session: whoever reads it reads the
    // account. It lives with the messages because it is exactly as sensitive as
    // they are, it is covered by their backup, and the alternative was a PVC that
    // one pod writes — which is how `messages` spent 26 hours answering 502 over
    // a 0400 file in an emptyDir it could no longer write (see that repo's
    // `IrcSender::prepare`).
    //
    // ⚠ **Losing this row is NOT free.** Logging in again is rate-limited by
    // Telegram with flood waits measured in hours, so this is not a cache to be
    // dropped when convenient. `single_row` is a CHECKed constant so a second
    // session cannot be inserted by accident: two pods with two keys is two
    // update streams, each acknowledging state the other needs.
    //
    // LONGTEXT rather than JSON: MariaDB's JSON is an alias for LONGTEXT with a
    // validity constraint, and nothing here queries inside the document — it is
    // read whole at boot and written whole when it changes.
    r"CREATE TABLE IF NOT EXISTS telegram_session (
        single_row TINYINT(1) NOT NULL PRIMARY KEY DEFAULT 1
            CHECK (single_row = 1),
        data LONGTEXT NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )",
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
                // MIGRATIONS holds &'static str literals; sqlx 0.9's SqlSafeStr
                // accepts those directly (deref the &&str from the iterator).
                // Each MIGRATIONS literal is judged as DDL by dev-lint's schema
                // replay; the checker just can't resolve a module-static loop.
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

    /// Record/refresh a contact. Only overwrites phone/name when a non-NULL
    /// value is supplied, so a later sighting without a name won't wipe one.
    pub async fn upsert_contact(
        &self,
        uuid: &str,
        phone: Option<&str>,
        name: Option<&str>,
    ) -> Result<()> {
        let phone = phone.filter(|s| !s.is_empty());
        let name = name.filter(|s| !s.is_empty());
        sqlx::query(
            "INSERT INTO contacts (uuid, phone, profile_name) VALUES (?, ?, ?)
             ON DUPLICATE KEY UPDATE
                phone = COALESCE(VALUES(phone), phone),
                profile_name = COALESCE(VALUES(profile_name), profile_name)",
        )
        .bind(uuid)
        .bind(phone)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Inserts a message, returning its new row id — or `None` if it was a
    /// duplicate that `INSERT IGNORE` dropped. Encoding the duplicate case as
    /// `None` (rather than a `0` sentinel) means a caller can't fetch children
    /// for a row that was never written without the type forcing the check.
    pub async fn insert_message(
        &self,
        thread_id: &ThreadId,
        sender_uuid: &str,
        server_ts: i64,
        body: Option<&str>,
        quote_target_ts: Option<i64>,
        is_outgoing: bool,
    ) -> Result<Option<u64>> {
        let res = sqlx::query(
            "INSERT IGNORE INTO messages
                (thread_id, sender_uuid, server_ts, body, quote_target_ts, is_outgoing)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(thread_id.to_string())
        .bind(sender_uuid)
        .bind(server_ts)
        .bind(body)
        .bind(quote_target_ts)
        .bind(is_outgoing)
        .execute(&self.pool)
        .await?;
        // INSERT IGNORE skips a duplicate: 0 rows affected, no new id.
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

    /// Ensure the conversation exists and return its id.
    ///
    /// `LAST_INSERT_ID(id)` on the duplicate branch is what makes this one round
    /// trip: MariaDB hands back the *existing* row's id rather than zero, so the
    /// caller never has to decide between an insert and a select.
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

    /// Insert a log file's lines, returning how many were new — the rest were
    /// duplicates `INSERT IGNORE` dropped, which is the normal result of
    /// re-running an import over logs already read.
    ///
    /// ⚠ **Batched because the unit of this import is 860,359 lines.** One
    /// statement per line is one network round trip per line: tolerable
    /// in-cluster, hours over a port-forward from a laptop, which is where a
    /// history import is actually run. A log file averages ~72 lines, so a
    /// statement per file is a ~72× cut in round trips at no cost in
    /// idempotence — the dedupe key does that work, not the batching.
    ///
    /// Chunked at [`INSERT_CHUNK`] rows regardless, because MySQL's protocol
    /// caps a statement at 65,535 placeholders and a busy channel's day can run
    /// to thousands of lines.
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

    /// Every file the importer has already read, as `rel_path → (mtime_ns, size)`.
    ///
    /// Read whole, once, rather than a `SELECT` per file: it is one row per log
    /// file — 36,201 today, a few MB — and the alternative is 36,201 round trips
    /// to decide whether to do nothing.
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
    /// ⚠ Call this only AFTER those files' lines are in, and only under
    /// `--apply`. It is the record of work done; writing it before, or during a
    /// dry run, converts the next run's skip into data loss that nothing
    /// reports.
    ///
    /// ⚠ **BATCHED, and the unbatched version was measured being wrong.** One
    /// statement per file is one network round trip per file — exactly what the
    /// note on [`Self::insert_irc_lines`] says about lines, recreated one level
    /// up. A full pass went from ~7ms to ~30ms a file, so the audit mode that
    /// re-reads all 36,201 of them went from 5 minutes to over 20.
    ///
    /// Batching does not weaken the guarantee above, because it can only fail in
    /// the safe direction: a run that dies before a flush leaves those files
    /// unmarked and the next run reads them again.
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

    /// The pool, for the Telegram session store — which implements a `grammers`
    /// trait and so cannot be a method on `Db`.
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
        // ⚠ `COALESCE(VALUES(name), name)` rather than `VALUES(name)`: a response
        // that carries a peer without its title must not blank a name the archive
        // already has. Telegram sends minimal peers routinely (the `min` flag),
        // and the visible symptom of getting this wrong is a conversation list
        // that loses its names as you use it.
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
    /// ⚠ **This is the one write in the archive that can destroy something, and
    /// the transaction is what stops it.** Telegram's edit is a mutation of a
    /// message that keeps its id, so the row has to be updated in place — and the
    /// words being replaced exist nowhere else the moment that update lands. So
    /// the old text is appended to `telegram_message_edits` and the row is updated
    /// in the same transaction: either both happen or neither does, and a crash
    /// between them cannot leave the archive holding only the new version.
    ///
    /// Replay is free. An unchanged message is recognised by its `edit_date`
    /// matching what is stored and costs one insert that does nothing, which is
    /// what makes it safe for the backfill and the live stream to cover the same
    /// ground.
    ///
    /// ⚠ **THERE IS NO `SELECT … FOR UPDATE` HERE, AND THAT IS THE FIX FOR A
    /// DEADLOCK, not a weakening.** The first version opened with a locking read
    /// of a row that usually does not exist yet, which in InnoDB takes a GAP lock
    /// — and two transactions inserting different messages into the same gap
    /// deadlock each other. This archive has exactly the two concurrent writers
    /// that provokes: the backfill walking history while the update stream stores
    /// what is arriving. It surfaced as `1213 Deadlock found` under parallel
    /// tests, which is the only reason it was seen before production.
    ///
    /// What replaces it is a compare-and-swap. `INSERT IGNORE` needs no gap lock;
    /// the edit path then reads without locking and updates under
    /// `edited_at <=> <the value it read>`, so if another writer edited the same
    /// message in between, this update matches nothing and reports that rather
    /// than overwriting a version whose text it never filed. `<=>` and not `=`
    /// because the value being compared is NULL for a message not yet edited, and
    /// `NULL = NULL` is not true.
    pub async fn store_telegram_message(
        &self,
        row: &crate::telegram::map::Row,
        sender_name: Option<&str>,
    ) -> Result<TelegramStored> {
        let inserted = sqlx::query(
            "INSERT IGNORE INTO telegram_messages
                (conversation_id, msg_id, sent_at, sender_id, sender_name,
                 is_outgoing, kind, text, media_kind, edited_at,
                 reply_to_msg_id, fwd_from_name)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .bind(row.edited_at)
        .bind(row.reply_to_msg_id)
        .bind(row.fwd_from_name.as_deref())
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() != 0 {
            return Ok(TelegramStored::Inserted);
        }

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
            // Gone between the insert and the read. Nothing to preserve and
            // nothing to update; the next delivery will insert it.
            None => TelegramStored::Unchanged,
            Some((_, stored_edit)) if stored_edit == row.edited_at => TelegramStored::Unchanged,
            Some((stored_text, stored_edit)) => {
                // The superseded version, filed under the `edit_date` it carried.
                // `INSERT IGNORE` because an update seen twice must not append
                // twice — and for the ORIGINAL text `was_edited_at` is NULL, which
                // no unique index can deduplicate, so that case is guarded by
                // only ever being reachable while the row's stored `edited_at` is
                // still NULL: the first edit is the only moment the pre-edit text
                // is knowable.
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
                    // Another writer edited the same message between the read and
                    // here. Its history row is already filed; ours would describe a
                    // version that is no longer current, so this reports having
                    // changed nothing rather than racing.
                    TelegramStored::Unchanged
                } else {
                    TelegramStored::Edited
                }
            }
        };
        tx.commit().await?;
        Ok(outcome)
    }

    /// Replace a message's reaction counts with what Telegram last reported.
    ///
    /// ⚠ DELETE-then-insert rather than upsert, because a reaction that has been
    /// taken away is absent from the new list rather than present with a count of
    /// zero. An upsert alone would leave every reaction a message has ever had
    /// visible forever, at its high-water mark.
    ///
    /// Skipped entirely when there is nothing to say: Telegram omits the field for
    /// a message with no reactions, and an empty list would then delete real rows
    /// every time a message was re-read. Removing the LAST reaction is therefore
    /// invisible to this archive — recorded here as the known limit it is.
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
        sqlx::query("DELETE FROM telegram_reactions WHERE conversation_id = ? AND msg_id = ?")
            .bind(conversation_id)
            .bind(msg_id)
            .execute(&mut *tx)
            .await?;
        for r in reactions {
            sqlx::query(
                "INSERT INTO telegram_reactions
                    (conversation_id, msg_id, emoji, custom_emoji_id, cnt, chosen)
                 VALUES (?, ?, ?, ?, ?, ?)",
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
        tx.commit().await?;
        Ok(())
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

    /// Record progress through a conversation's history.
    ///
    /// ⚠ `LEAST` on `oldest_seen`, so a resumed walk cannot move the frontier
    /// backwards: the live stream stores NEW messages with high ids through the
    /// same path, and a plain assignment would let one of those reset the backfill
    /// to the top and walk the whole conversation again.
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

    /// Flag messages a `updateDeleteMessages` named, and report how many rows it
    /// reached.
    ///
    /// ⚠ **`updateDeleteMessages` CARRIES NO PEER, and that is why this takes a
    /// kind rather than a conversation.** Telegram can leave the peer out because
    /// private chats and basic groups share ONE message-id sequence per account —
    /// an id is enough to identify the message among them. Channels each have
    /// their own sequence and get `updateDeleteChannelMessages`, which does name
    /// the channel. So a peer-less deletion is applied to non-channel
    /// conversations only; applying it everywhere would retract an unrelated
    /// channel post that happens to share the number.
    ///
    /// The text is kept. `deleted` is a flag, as it is for Signal, and the viewer
    /// decides what to put on screen.
    pub async fn mark_telegram_deleted(
        &self,
        msg_ids: &[i32],
        scope: TelegramDeleteScope,
    ) -> Result<u64> {
        if msg_ids.is_empty() {
            return Ok(0);
        }
        // One statement per id rather than one `IN (...)`: the shared-sequence
        // case has to join to the conversations to exclude channels, and a
        // deletion names a handful of ids, so there is no scan to save here.
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

/// Rows per statement. 9 columns × 1,000 is well inside MySQL's 65,535
/// placeholder cap, with room for the column list to grow.
const INSERT_CHUNK: usize = 1_000;

/// One logged line, ready to write. Owned rather than borrowed: it is built per
/// file and handed straight to the batch, and threading a lifetime through that
/// buys nothing at 72 rows.
/// What [`Db::store_telegram_message`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramStored {
    /// A message the archive had never seen.
    Inserted,
    /// A message whose text was replaced, with the old version kept.
    Edited,
    /// Already stored, at the same `edit_date`. The common case on replay.
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

/// Which conversations a deletion applies to — see [`Db::mark_telegram_deleted`]
/// for why this is not simply a conversation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelegramDeleteScope {
    /// `updateDeleteChannelMessages`, which names its channel.
    Channel(i64),
    /// `updateDeleteMessages`, which does not — the ids belong to the one
    /// sequence that private chats and basic groups share.
    SharedSequence,
}

pub struct IrcLine {
    pub line_no: u32,
    pub sent_at: String,
    pub nick: Option<String>,
    pub is_self: bool,
    pub kind: &'static str,
    pub text: String,
}
