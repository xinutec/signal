//! MariaDB archive store. Append-only migrations, same convention as the
//! `home`/`health` services: each entry runs exactly once, tracked by index in
//! `schema_version`. To evolve the schema, APPEND a new entry — never edit an
//! existing one.

use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::AssertSqlSafe;
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
    // v21: what a media download WOULD cost, and what it is.
    //
    // ⚠ **RECORDED BEFORE ANYTHING IS DOWNLOADED, which is the point.** `grammers`
    // reports a file's size and mime from the message itself — no network request —
    // so the archive can say exactly what fetching the photos would take before
    // anybody commits a volume to it. The alternative was estimating from a
    // per-photo guess, and the documents are precisely where a guess goes wrong: a
    // couple of videos outweigh every photo in the account.
    //
    // `media_mime` is here too, one column early, because it comes from the same
    // call and because serving bytes later needs a content type. A row that knows
    // it is a 3 MB `video/mp4` is answerable without a download; a row that knows
    // only "document" is not.
    //
    // ⚠ NULL means NOT YET KNOWN, not zero. Everything stored before this migration
    // has NULL here, and no re-walk fills it by itself — see the enrichment note on
    // `store_telegram_message`, which is what does.
    r"ALTER TABLE telegram_messages
        ADD COLUMN media_size BIGINT NULL,
        ADD COLUMN media_mime VARCHAR(128) NULL",
    // v22: whether Telegram asked for the edit NOT to be shown.
    //
    // ⚠ **`edit_date` IS NOT "SOMEBODY EDITED THIS".** Telegram's `message`
    // constructor carries `edit_hide` beside it, documented as "whether the message
    // should be shown as not modified to the user, EVEN IF AN EDIT DATE IS
    // PRESENT". Telegram sets an edit date for its own reasons and then asks
    // clients not to surface it — so its apps show no marker where this archive
    // showed "Edited", on a photo in a live conversation, which is how it was
    // noticed.
    //
    // The edit is still RECORDED. This governs display only, which is why it is a
    // column here rather than a reason to drop `edited_at`.
    //
    // ⚠ NULLable, and the default is NOT `0`. `0` would assert "Telegram did not
    // ask us to hide it", which is a claim about every row stored before this
    // column existed and cannot be true of them — they were never asked. NULL means
    // NOT YET KNOWN, which is what lets the enrichment path in
    // `store_telegram_message` fill it on a re-walk.
    r"ALTER TABLE telegram_messages ADD COLUMN edit_hidden TINYINT(1) NULL",
    // v23: the relabel that was run by HAND on production, written down so it is
    // part of the schema rather than part of nobody's memory.
    //
    // ⚠ **ENRICHMENT CAN ADD A FACT AND CANNOT CORRECT ONE.** v21 made
    // `media_kind` finer — a `video/mp4` reports `video` rather than `document` —
    // but the enrichment path in `store_telegram_message` fills only what is NULL,
    // deliberately, so it will not rewrite a kind an earlier build already wrote.
    // The consequence was 731 videos and 58 audio files still filed as
    // `document`: the column meant two different things depending on WHEN the row
    // was written, which is the one-concept-two-readers trap in time rather than
    // in space.
    //
    // These two statements are what I ran against the live database at the time.
    // They are here because a hand-run data fix that exists nowhere in the
    // repository is invisible to a rebuild: restore this database from the
    // migrations alone and the correction would silently not happen. Idempotent —
    // a second run matches nothing.
    r"UPDATE telegram_messages SET media_kind = 'video'
       WHERE media_kind = 'document' AND media_mime LIKE 'video/%'",
    r"UPDATE telegram_messages SET media_kind = 'audio'
       WHERE media_kind = 'document' AND media_mime LIKE 'audio/%'",
    // v24: the bytes this archive actually HOLDS for a Telegram message.
    //
    // Separate from the media columns on `telegram_messages` because they answer
    // different questions and can disagree honestly: those record what TELEGRAM
    // said about the file (its size, its type, knowable with no request), and this
    // records what is on the volume. A row here with no row there would be bytes we
    // cannot describe; a row there with none here is a file we have not fetched,
    // which is the normal state for everything large.
    //
    // ⚠ **`state` IS THE WHOLE DESIGN, and it mirrors `link_images` in the
    // `messages` repo deliberately.** Photos are fetched EAGERLY, because a photo in
    // a conversation is the conversation and the backfill is already at that message
    // with the connection open. Everything larger is `offered` and fetched only when
    // a reader asks — 832 videos come to 3.8GB and one of them is 1.5GB, measured
    // before any of this was built, which is what made the split a decision rather
    // than a guess.
    //
    // `failed` keeps the reason. A fetch that failed silently and left no row would
    // be retried forever by the eager pass; one that left a row with no reason would
    // be a mystery nobody could act on.
    //
    // `stored_name` is a NAME, not a path: the directory is configuration
    // (`TELEGRAM_MEDIA_DIR`), and a stored absolute path would survive a remount
    // pointing at nothing. The reader joins the two and takes only the file name, so
    // a name cannot escape the mount.
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
    // v25: the size column comes out again, one day after going in.
    //
    // ⚠ **IT WAS A SECOND COPY OF A NUMBER, AND THE COPY WAS WRONG.** It recorded
    // `metadata(path).len()` taken straight after `download_media` returned, which
    // reads the length before the write is visible: 950 files totalling 218MB on
    // disk were recorded as 86MB, and **the four smallest rows said 0 bytes for
    // files of 158KB, 214KB, 288KB and 126KB**. `tokio::fs::File` performs its work
    // on a blocking pool and does not promise the inode reflects it when the write
    // call returns, so stat-after-download is a race and always was.
    //
    // The size is already in `telegram_messages.media_size`, from the message itself
    // at no network cost, and it is accurate: 246KB mean reported against 240KB mean
    // actually on disk, which is agreement within the noise of a set still growing.
    //
    // So the archive keeps ONE size, in the table whose subject is what Telegram
    // said, and `telegram_media` records only what is on the volume. The
    // consequence worth stating: **nothing here independently verifies the byte
    // count.** A stat that can read zero is worse than no stat, and the flush that
    // would make one reliable belongs to a file handle this code does not own.
    r"ALTER TABLE telegram_media DROP COLUMN size_bytes",
    // v26: a reader can ask for what was only offered.
    //
    // ⚠ **`wanted` IS A QUEUE, AND THE DATABASE IS DELIBERATELY THE WHOLE OF IT.**
    // The thing that must do the fetching is the feed, because it is the only
    // process holding a Telegram session — and the feed listens on no port, which is
    // a property worth keeping: nothing in the cluster can dial the pod that holds a
    // logged-in account. So the viewer writes a row and the feed reads it, which
    // needs no endpoint, no second credential and no service.
    //
    // The cost is latency bounded by the poll interval rather than by the network,
    // which for a 1.5GB video nobody is watching load is not the part that matters.
    //
    // `requested_at` was in the table from the first version for this, and the index
    // on `(state, requested_at)` is what makes the poll a lookup rather than a scan.
    r"ALTER TABLE telegram_media
        MODIFY COLUMN state ENUM('offered','wanted','stored','failed') NOT NULL",
    // v27: WHO a forward came from, when the header names a peer rather than a
    // string.
    //
    // ⚠ **`fwd_from_name` was the ONLY thing read, and it is the RARE half.**
    // Telegram's forward header carries `from_id` — the original sender's peer —
    // and fills `from_name` only when that account has forward-privacy on, so it
    // hides behind a bare string instead. Reading the string alone meant a
    // forward was recorded exactly when the sender had asked not to be
    // identified, and dropped in every ordinary case: 0 rows out of 159,946 at
    // the point this was found, in an archive spanning years.
    //
    // A forward was therefore INDISTINGUISHABLE from something the sender wrote,
    // which is the part that matters — not the missing name, but the missing fact
    // that the words are somebody else's.
    //
    // The id is normalised the way every other peer here is, so a forwarder in a
    // group and the DM with that same person are one id.
    //
    // ⚠ This fills from now on, and the rows already walked stay NULL until
    // something re-reads them — the backfill is marked complete and does not
    // return on its own. They are not unfillable: both forward columns are in the
    // enrichment UPDATE below, which is what a deliberate re-walk needs to repair
    // history. That is a decision to take with the cost in view, not a migration.
    r"ALTER TABLE telegram_messages
        ADD COLUMN fwd_from_id BIGINT NULL",
    // v28: a reaction that goes away STOPS BEING CURRENT rather than ceasing to
    // have happened.
    //
    // ⚠ **`replace_telegram_reactions` used to DELETE, and a re-walk could
    // therefore lose history.** A message reacted to with 👍 and ❤️ whose ❤️ was
    // later taken back came back from Telegram carrying only the 👍 — and the
    // replace threw the ❤️ away, so the archive forgot a thing that had genuinely
    // happened. The whole point of this archive is that it remembers what the
    // service no longer shows: a deleted message keeps its words, an edited one
    // keeps every version, and a reaction should be no different.
    //
    // So the row stays and gains a date. `removed_at IS NULL` is "currently on the
    // message", which is what the viewer draws; a non-NULL one is what the archive
    // remembers and Telegram does not.
    r"ALTER TABLE telegram_reactions
        ADD COLUMN removed_at TIMESTAMP NULL",
    // v29: who has read how far.
    //
    // ⚠ **THIS IS THE ONE THING IN THIS ARCHIVE WITH NO HISTORY TO GO BACK FOR.**
    // Every other column here can be recovered by re-reading Telegram, because
    // Telegram keeps the messages. It keeps NO log of reading — a dialog carries
    // only `read_inbox_max_id` and `read_outbox_max_id`, the CURRENT high-water
    // marks — so a read that is not recorded as it happens is gone permanently.
    // That is why this table exists at all, and why it went in the day it was
    // asked for rather than after the re-walk.
    //
    // **APPEND-ONLY, which is the point.** A high-water mark is a moving value, and
    // storing only the latest would make this a cache of Telegram's current state
    // rather than a record. Each ADVANCE is its own row, so the table answers
    // "when did they read this?" and not merely "how far have they read?".
    //
    // ⚠ **`observed_at` IS WHEN WE SAW IT, NOT WHEN THEY READ IT**, and the two are
    // not the same. `updateReadHistoryOutbox` carries a peer, a `max_id` and a
    // `pts` — no date — so Telegram never says when the reading happened. A live
    // update lands within seconds; a mark first seen by the hourly sweep may be up
    // to an hour late, and one seen after downtime later still. The column is named
    // for what it can honestly hold.
    //
    // ⚠ **`direction` uses Telegram's OWN words, which read backwards at first.**
    // `outbox` is the OUT-tray: MY messages, and how far the other side has read
    // them — the blue-tick marker. `inbox` is theirs, and how far I have read. The
    // vendor vocabulary is kept because anyone checking this against Telegram's
    // documentation will be searching for those two words.
    r"CREATE TABLE IF NOT EXISTS telegram_read_marks (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        conversation_id BIGINT NOT NULL,
        direction ENUM('inbox','outbox') NOT NULL,
        max_id INT NOT NULL,
        observed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_tg_read (conversation_id, direction, max_id)
    ) DEFAULT CHARSET=utf8mb4",
    // v30: WHICH PERSON reacted, and when.
    //
    // ⚠ **v17 says Telegram hands over a count rather than a list of people. That
    // was wrong, and it cost 21,696 reactions their authors.** `messageReactions`
    // carries `recent_reactions: Vector<MessagePeerReaction>` in the same struct
    // whose `results` the aggregate was read from — `{peer_id, date, reaction}`,
    // arriving free with every message already being fetched. Measured over 901
    // messages on 2026-09-18: present on 295 of 296 reacted messages, and naming
    // EVERY reactor on all 295, not merely the most recent few.
    //
    // The aggregate in `telegram_reactions` STAYS. The two are different facts and
    // the count is the authoritative one: `results` is a complete tally by
    // construction, while `recent_reactions` is a list Telegram may truncate.
    //
    // ⚠ **A SHORT LIST IS NOT A RETRACTION.** The rule mirrors v28's, one step
    // sharper. An absent `recent_reactions` says nothing at all. A present one is a
    // complete statement ONLY when it names at least as many reactors as `results`
    // counts; below that it has been truncated, and dating the unnamed would
    // invent removals for people who are still there. So a short list upserts what
    // it names and retracts nothing. Everything in this archive reacts alone today,
    // so the truncated case is untested by the data and guarded by a test instead.
    //
    // `reacted_at` is Telegram's own `date` on the peer reaction — unlike
    // `telegram_read_marks.observed_at`, this one really is when the person acted.
    // `reaction_key` is generated exactly as v17 explains at length; read that
    // entry before touching the spelling here.
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
    // v31: the formatting, and the LINKS THAT ARE NOT IN THE TEXT.
    //
    // ⚠ **THIS IS CONTENT LOSS, NOT DECORATION.** `messageEntityTextUrl` carries a
    // url the visible text does not contain — "see here" linking somewhere is
    // stored as the word "here" and nothing else. Same for `messageEntityMentionName`,
    // whose user id is the only record of who was meant. Measured at 17.8% of
    // messages, which over this archive is on the order of 28,000.
    //
    // ⚠ **`offset` AND `length` ARE UTF-16 CODE UNITS.** Not bytes, not Rust chars.
    // Every emoji outside the BMP counts as TWO, so slicing a Rust string by these
    // numbers silently misplaces every span after the first emoji — and this is a
    // chat archive, where that is most of them. The columns are named for the unit
    // so the next reader cannot use them innocently.
    //
    // Identity is the SPAN, not a position in the list. Keying on an index would
    // mean that an edit inserting one bold run at the start renumbers everything
    // after it, and the dating below would then record removals that never
    // happened. A span is what an entity actually is.
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
    // v32: WHICH event a service message was.
    //
    // ⚠ **`telegram_messages.text` for a service message is OUR ENGLISH, not
    // Telegram's.** `describe_action` maps the action to a phrase, and its final arm
    // is `_ => "an event"` — so an action this archive had never seen was stored as
    // two words that name nothing, unrecoverably. This column holds the TL
    // constructor name, which is the identity rather than a rendering, so an
    // unhandled action is still exactly identifiable afterwards.
    r"ALTER TABLE telegram_messages
        ADD COLUMN service_action VARCHAR(64) NULL",
    // v33: how long the call was, and how it ended.
    //
    // ⚠ **65 CALLS WERE STORED AS THE WORDS "a call".** `messageActionPhoneCall`
    // carries `duration`, `video` and `reason` — busy, hangup, missed, disconnect —
    // and `describe_action` returns `&'static str`, so all of it was discarded at
    // the mapper. In a personal archive the fact that a call happened is the least
    // interesting part of it.
    //
    // A table rather than columns on `telegram_messages`, because these are facts
    // about a call and only 0.05% of rows are one. Measured 2026-09-18: `reason` on
    // 65 of 65, `duration` on 38 — an unanswered call has no duration, which is
    // itself the record of it being unanswered.
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
    // v34: four facts the message already carried.
    //
    // `grouped_id` is the ALBUM: without it a set of photos sent together is N
    // unrelated messages, and nothing can put them back. 4.0% of messages.
    //
    // ⚠ `fwd_date` is when the ORIGINAL was written, and it is NOT optional in the
    // header — `messageFwdHeader` has `date:int` outright. A forward stored with
    // only the forwarder's clock says a thing was said today that was said years
    // ago. `fwd_channel_post` is the original's id in its channel.
    //
    // `ttl_period` is the disappearing-message timer, which is the only explanation
    // an archive can offer for why a conversation has holes.
    r"ALTER TABLE telegram_messages
        ADD COLUMN grouped_id BIGINT NULL,
        ADD COLUMN fwd_date BIGINT NULL,
        ADD COLUMN fwd_channel_post INT NULL,
        ADD COLUMN via_bot_id BIGINT NULL,
        ADD COLUMN ttl_period INT NULL",
    // v35: WHICH PART of the message was replied to.
    //
    // Telegram lets a reply quote a fragment rather than the whole message, and
    // `reply_to_msg_id` alone cannot express that — the archive would show a reply
    // to a long message with no way to tell which sentence it answered.
    // `quote_text` is that fragment, verbatim.
    //
    // `reply_to_peer_id` is a reply reaching into ANOTHER conversation, normalised
    // the way every other peer here is. Without it such a reply points at a
    // `msg_id` that does not exist in its own conversation, which reads as a
    // dangling reference rather than a cross-chat one.
    r"ALTER TABLE telegram_messages
        ADD COLUMN reply_quote TEXT NULL,
        ADD COLUMN reply_to_peer_id BIGINT NULL",
    // v38 (written as v36, and it ran at BOTH — at 36 on the first deploy and
    // again at 38 after the insertion below shifted it; `IF NOT EXISTS` is why
    // that cost nothing). How far a re-capture has got, so hours of work survive
    // a restart.
    //
    // ⚠ **NOT `telegram_backfill_state`, and the difference is the direction.**
    // That table walks OLDER, from `oldest_seen` outward, and is finished when it
    // reaches the start of a conversation. This one walks FORWARD through messages
    // the archive already holds, re-reading them so columns added after they were
    // stored get filled by the enrichment. Sharing one table would make
    // `complete` mean two things and a re-capture would end the backfill.
    //
    // ⚠ **The frontier is `through_msg_id`, and it only moves once a batch is
    // WRITTEN.** A pass that recorded progress before storing would skip whatever
    // was in flight when the pod died — silently, and exactly the way the archive
    // cannot detect afterwards.
    //
    // Re-runnable by deleting a row: the next pass re-reads that conversation from
    // the beginning, which costs time and changes nothing, because every write it
    // makes is an enrichment of a NULL.
    // v36 (DEAD — SEE v39). ⚠ **THIS ENTRY NEVER RAN AND NEVER WILL.**
    //
    // It was INSERTED here rather than appended, which renumbered the slot
    // `telegram_recapture_state` had already occupied. `schema_version` had 36
    // recorded from the earlier deploy, so this statement was skipped, the two
    // below shifted up by one, and the result went to production as a table that
    // does not exist while `SELECT MAX(version)` read 39 and looked healthy.
    //
    // The rule at the top of this file says append-only, and this is what
    // breaking it looks like: not an error, not a failed migration — a silent
    // no-op, and a schema the version number vouches for. Left in place rather
    // than deleted, because removing it would shift every index after it and
    // break the next database to migrate from scratch.
    //
    // Signal's delivery and read receipts.
    //
    // ⚠ **RICHER THAN TELEGRAM'S AND LESS RECOVERABLE, WHICH IS THE WHOLE
    // POINT.** Telegram gives a high-water mark per conversation and restates it
    // on every `getDialogs`, so a missed update costs lateness. Signal gives an
    // EVENT — who, which messages, delivered/read/viewed, and its own `when` —
    // and says it exactly once, on the live socket. Nothing restates it, and the
    // Android export carries none, so the 15 months before this table existed are
    // permanently blank.
    //
    // `parse.rs` handled `dataMessage`, `sentMessage` and `editMessage`, and every
    // `receiptMessage` fell through to `Skip`. It was never a decision — the arm
    // was simply not written, and a test named `the_other_origins_report_no_read_state`
    // passing green made the absence look like a property of Signal.
    //
    // ⚠ **ONE RECEIPT ACKNOWLEDGES MANY MESSAGES**, so it is flattened: a row per
    // (message, author, kind). Keyed that way rather than on the receipt, because
    // the question is always "when was THIS message read", never "what did that
    // frame say".
    //
    // ⚠ **`when_ts` IS NEVER UPDATED.** Re-seeing a receipt must not restamp it —
    // same rule as `telegram_read_marks.observed_at`, and for the same reason: the
    // first observation is the one that answers the question.
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
    // v37 (labelled v38 when written; it landed at 37). Signal calls, as the
    // frames that actually arrive.
    //
    // ⚠ **NOT A DURATION, BECAUSE SIGNAL DOES NOT SEND ONE.** Telegram reports a
    // finished call as one service message with `duration` and `reason` already
    // computed — 65 of them in this archive. Signal sends WebRTC signalling: an
    // offer, maybe an answer, maybe a busy, maybe a hangup, sharing a `call_id`,
    // each its own envelope with its own timestamp. Offer→hangup IS the duration,
    // but only when both frames reach THIS device, which depends on where the
    // call was picked up.
    //
    // So this stores what arrived and interprets nothing. The events are the
    // unrecoverable half; a duration derived from a state machine nobody has
    // watched run would be a guess wearing a number's clothes, and it can be
    // computed later against real rows.
    //
    // ⚠ `iceUpdateMessages` is excluded on purpose — many frames per call,
    // carrying only opaque transport blobs, which would bury the four that say
    // what happened.
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
    r"CREATE TABLE IF NOT EXISTS telegram_recapture_state (
        conversation_id BIGINT NOT NULL PRIMARY KEY,
        through_msg_id INT NOT NULL,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) DEFAULT CHARSET=utf8mb4",
    // v40: Signal's receipts, APPENDED this time.
    //
    // ⚠ The label said v39 and the entry is the 41st, index 40. A version number
    // in a comment is what the next person counts from when deciding where to put
    // theirs, so a label that is off by one is the same hazard as the dead v36
    // below — checked against the array on 2026-09-21 and corrected.
    //
    // The same statement as the dead v36 above. It is here because appending is
    // the only way to add one: every index below 39 is already in
    // `schema_version`, so a statement placed anywhere earlier is skipped
    // regardless of whether it ever ran. See v36's note for what that cost.
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
    // v41: the name is a DISPLAY name, and only sometimes the profile name.
    //
    // ⚠ **THE COLUMN IS NAMED AFTER THE LAST BRANCH OF A THREE-BRANCH FALLBACK.**
    // What arrives in `envelope.sourceName` is signal-cli's
    // `getContactOrProfileName`, whose order is Signal's own. In the version
    // deployed here (0.14.5) it is:
    //
    //     if (contact != null && !isEmpty(contact.getName())) return contact.getName();
    //     return profile.getDisplayName();
    //
    // So for 38 of 52 recipients it holds the ADDRESS-BOOK name and the profile
    // name is the fallback, not the meaning. Renamed rather than re-documented,
    // because a column called `profile_name` is what a reader believes.
    //
    // ⚠ **0.14.7 ADDS A BRANCH ABOVE BOTH** — `contact.getDisplayNickname()`, the
    // first/last name you type in Signal's own UI — so the same field starts
    // carrying a third kind of name on upgrade without anything here changing.
    // That is the reason the column is not called `contact_name` either: it is
    // whatever Signal currently thinks this person is called.
    //
    // ⚠ **`profile_name` IS DELIBERATELY LEFT IN PLACE**, and as of 2026-09-21 the
    // reason has CHANGED — the viewer has moved (messages 7098b95, verified: the
    // serving binary holds 0 references to `profile_name` and 13 to
    // `display_name`). What keeps the column is the WRITER, and the hazard is the
    // rollout rather than the reader.
    //
    // ⚠ **`signal-ingester` IS `RollingUpdate`, SO OLD AND NEW PODS OVERLAP.** A
    // single deploy that both stopped writing the column and dropped it would
    // leave the old pod INSERTing into a column that no longer exists; its writes
    // fail, and Signal keeps no server-side history to re-walk, so those messages
    // are gone. Dropping it therefore needs two deploys: stop writing, then drop.
    // All 41 rows are identical across the two columns, so the drop itself loses
    // nothing — the sequencing is the whole of the risk.
    r"ALTER TABLE contacts ADD COLUMN display_name VARCHAR(255) NULL",
    r"UPDATE contacts SET display_name = profile_name WHERE display_name IS NULL",
    // v43: what somebody was called, and until when.
    //
    // ⚠ **A NAME HAS ALWAYS BEEN OVERWRITTEN IN PLACE, AND THE NEXT RENAME IS A
    // BULK ONE.** `contacts` keeps one name per person, so a rename answers "what
    // is she called" and destroys "what was she called when she said this" — and
    // the second is the question a reader of an old thread actually has. That has
    // cost nothing so far because the resolved names have not moved; upgrading
    // signal-cli past 0.14.7 adds the nickname branch above the other two and
    // renames everybody who has one, all at once, on the first message after the
    // pod restarts.
    //
    // So the old name is dated rather than dropped, the same shape as
    // `telegram_reactions.removed_at`: the row stays and gains an end.
    //
    // ⚠ **THIS IS NOT FIXING A BUG.** It was written while chasing one that turned
    // out not to exist — the claim was that `COALESCE(VALUES(x), x)` made the name
    // write-once, and it does the opposite: it overwrites whenever a value is
    // supplied. Kept because the archive's rule is that history is dated, never
    // deleted, and a bulk rename is exactly the event that would have broken it.
    //
    // ⚠ **`seen_from` ON A BACKFILLED ROW IS WHEN THE ARCHIVE LAST TOUCHED IT**, not
    // when the person began being called that. Signal sends no such date and never
    // did. The column is named for what it can hold.
    r"CREATE TABLE IF NOT EXISTS contact_names (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        uuid VARCHAR(64) NOT NULL,
        name VARCHAR(255) NOT NULL,
        seen_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        seen_until TIMESTAMP NULL,
        INDEX idx_contact_names_current (uuid, seen_until)
    ) DEFAULT CHARSET=utf8mb4",
    // The name each contact is wearing now, so the history starts complete rather
    // than from the next time somebody is renamed.
    r"INSERT INTO contact_names (uuid, name, seen_from)
        SELECT uuid, display_name, updated_at FROM contacts WHERE display_name IS NOT NULL",
    // v45: the frame as it arrived, before anything reads it.
    //
    // ⚠ **SIGNAL SAYS EVERYTHING EXACTLY ONCE, so a field this archive has no
    // column for is gone the moment the socket moves on.** Telegram can be
    // re-walked — that is what the 4h36m recapture did, and why a gap there costs
    // an afternoon. Signal keeps no server-side history: `signal-cli` hands over
    // one envelope on the live socket and nothing ever restates it.
    //
    // ⚠ **`JsonDataMessage` AT 0.14.5 HAS 23 FIELDS AND THIS ARCHIVE READS FOUR.**
    // Checked against the deployed tag on 2026-09-21, not master. Dropped so far:
    // `expiresInSeconds`, `isExpirationUpdate`, `viewOnce`, everything in `quote`
    // except its id, `mentions`, `previews`, `textStyles`, `sticker.packId` and
    // `.stickerId`, `payment`, `contacts`, the three poll kinds, `storyContext`,
    // `pinMessage`, `unpinMessage`, `adminDelete` — plus the envelope's own
    // `serverReceivedTimestamp` and `serverDeliveredTimestamp`.
    //
    // ⚠ **SO THE FIX IS NOT FIFTEEN COLUMNS, IT IS KEEPING THE FRAME.** Columns
    // can be added whenever there is a reason and BACKFILLED from here, because
    // the bytes will still be on disk; a field not captured today cannot be
    // recovered by any amount of later work. This inverts which half is urgent.
    // It also costs nothing when signal-cli grows a field: the frame carries it
    // whether or not this code has heard of it.
    //
    // ⚠ **KEYED BY CONTENT HASH, because a frame has no id of its own.** An
    // envelope is (timestamp, source) and a receipt or a sync can repeat both;
    // signal-cli also re-delivers on reconnect, which happens on every deploy.
    // The digest makes replay free and makes it impossible to store the same
    // frame twice while still storing two genuinely different frames that share
    // a timestamp.
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

    /// Record a contact, keeping the name it is wearing and dating the one it wore.
    ///
    /// ⚠ **`COALESCE(VALUES(x), x)` OVERWRITES — it is not fill-only, and reading
    /// it as fill-only cost an afternoon.** `VALUES(x)` is the value from the
    /// INSERT list, so the COALESCE returns the NEW value whenever one was
    /// supplied and the stored one only when it was NULL. That is the right
    /// behaviour for `phone` and it is why the name has always tracked signal-cli
    /// correctly. What it cannot do is notice that it changed something.
    ///
    /// ⚠ **SO THE NAME IS WRITTEN BY A SECOND STATEMENT, WHOSE `rows_affected` IS
    /// THE RENAME.** A name arrives with EVERY message, and almost always the same
    /// one; the `<>` makes that an indexed no-op that writes no history, and makes
    /// the rare change announce itself without a SELECT to compare against.
    ///
    /// ⚠ **A SIGHTING WITH NO NAME MUST NOT BLANK ONE WE HOLD.** Receipts, typing
    /// frames and group members we have no profile for all arrive nameless. They
    /// skip the second statement entirely rather than passing NULL through it.
    ///
    /// ⚠ **`envelope.sourceName` IS A DISPLAY NAME, NOT A PROFILE NAME.** signal-cli
    /// resolves it with Signal's own precedence, which in 0.14.5 is the system
    /// contact name and then the profile name, and from 0.14.7 the nickname above
    /// both.
    ///
    /// ⚠ **`profile_name` IS NO LONGER WRITTEN**, which is the first of the two
    /// deploys its removal needs: the column still EXISTS, so an old pod mid-
    /// rollout keeps working, and the DROP is safe only once no running pod
    /// writes it. See the v41 migration for why the order is not optional.
    pub async fn upsert_contact(
        &self,
        uuid: &str,
        phone: Option<&str>,
        name: Option<&str>,
    ) -> Result<()> {
        let phone = phone.filter(|s| !s.is_empty());
        let name = name.filter(|s| !s.is_empty());
        // ⚠ The duplicate branch deliberately leaves BOTH name columns alone: every
        // later change goes through the statement below, so there is exactly one
        // place a rename can happen and exactly one place it can be noticed.
        // `rows_affected` is MySQL's — 1 means this INSERT really inserted.
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

        // `display_name IS NULL` is not the same as a rename and still belongs
        // here: a contact first seen without a name gets its first one this way,
        // and that opening chapter has to be recorded like any other.
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

    /// Keep the frame exactly as it arrived, before anything has read it.
    ///
    /// ⚠ **THIS RUNS BEFORE PARSING AND ITS FAILURE MUST NOT BE FATAL** — see the
    /// caller. A frame this archive cannot store is still a frame it can act on,
    /// and losing the row is better than losing the message.
    ///
    /// ⚠ **`INSERT IGNORE` ON THE DIGEST, because replay is routine.** signal-cli
    /// re-delivers on reconnect, and this archive reconnects on every deploy. The
    /// hash is over the frame's own bytes, so a re-delivery is recognised and two
    /// genuinely different frames sharing a timestamp both survive.
    ///
    /// Returns whether the frame was new, which is the only way to tell a first
    /// sighting from a replay — `rows_affected` on an IGNOREd duplicate is 0.
    pub async fn record_signal_frame(&self, frame: &serde_json::Value) -> Result<bool> {
        use sha2::{Digest, Sha256};
        // Serialised once, and the SAME bytes are both hashed and stored — hashing
        // a re-serialisation would let a formatting difference read as a new frame.
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

    /// Close whatever they were called before, and open the name they wear now.
    ///
    /// ⚠ **TWO STATEMENTS RATHER THAN ONE, because the one would reference the
    /// table it writes.** `INSERT ... WHERE NOT EXISTS (SELECT FROM contact_names)`
    /// is the obvious spelling and MySQL/MariaDB refuses it — the target table
    /// cannot appear in the statement's own subquery. Read then write, which is
    /// also the only version a reader can check by hand.
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

        // ⚠ **WHAT MAKES A NEW COLUMN FILLABLE FOR ROWS ALREADY STORED.** Without
        // this, adding `media_size` would have left it NULL forever on everything
        // ingested before it existed: the backfill marks a conversation `complete`
        // and never returns, and even a forced re-walk stores nothing because the
        // INSERT above is IGNOREd and the edit path only fires when `edit_date`
        // moves. The archive would have had a column it could never populate.
        //
        // So a message the archive already holds is ENRICHED when this delivery
        // knows something the stored row does not. Keyed on the stored value being
        // NULL — "not yet known" — rather than on a version number, so it is
        // idempotent and costs one statement that matches nothing once the row is
        // complete.
        //
        // ⚠ It does NOT overwrite a known value with a different one. That would be
        // the wrong instinct here: media facts come from the message and do not
        // change, so a disagreement means one of the two readings is wrong, and
        // silently taking the newer one would hide that.
        //
        // ⚠ **`sender_name` BELONGS HERE AND WAS LEFT OUT, WHICH IS WHY A RE-WALK
        // WOULD NOT HAVE BEEN COMPLETE.** It is not derived from the row — it comes
        // from the caller's peer lookup, which returns nothing when the peer is not
        // in the session cache — so 652 stored messages have a `sender_id` and no
        // name, and the viewer draws them with a BLANK sender. 527 of them are one
        // conversation whose peer never resolved at all. Every other column here
        // was added the moment its column was; this one predates the enrichment and
        // was never revisited, so the gap was silent in exactly the way a missing
        // enrichment always is: nothing fails, the column simply stays NULL.
        //
        // The lesson generalises past this row — **a column that can be NULL for a
        // reason OTHER than "the message does not have one" needs to be here.**
        //
        // ⚠ **v30–v35 ARE ALL HERE, AND THAT IS NOT OPTIONAL.** Every one of those
        // columns is NULL on all 159,956 rows stored before it existed, which is
        // precisely the "NULL for a reason other than the message not having one"
        // case above. Leaving any of them out would mean a column the re-capture
        // pass could never fill — the failure `sender_name` already demonstrated
        // once, silently, for months.
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
            // Gone between the insert and the read. Nothing to preserve and
            // nothing to update; the next delivery will insert it.
            None => TelegramStored::Unchanged,
            Some((_, stored_edit)) if stored_edit == row.edited_at => {
                if enriched {
                    TelegramStored::Enriched
                } else {
                    TelegramStored::Unchanged
                }
            }
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
        // ⚠ **AN EMPTY SET IS NOT EVIDENCE OF REMOVAL, so it marks nothing.**
        // `None` reactions on the wire and "every reaction was taken back" arrive
        // here identically, and treating the pair as removal would retract a
        // message's reactions every time a delivery simply did not carry them.
        // A NON-empty set is different: Telegram reports the current reactions in
        // full, so anything missing from one is genuinely gone.
        if reactions.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;

        // Present again: the count moves, and a reaction that had been marked
        // removed is current once more — somebody put it back.
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

        // Gone from a complete report: dated, not deleted.
        //
        // ⚠ Matched on `reaction_key`, the generated column, because that is the
        // only NON-NULL identity a reaction has — an emoji and a custom emoji id
        // are a sum type with one NULL half, and `NOT IN` over a nullable column
        // is never true for anything. The same reason the UNIQUE key is on it.
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

    /// Record WHO reacted, without letting a truncated list retract anybody.
    ///
    /// ⚠ **The rule is [`crate::telegram::map::Reactions::complete`], and it is NOT
    /// the aggregate's rule.** `replace_telegram_reactions` may retract whenever it
    /// is given a non-empty set, because `results` is a complete tally by
    /// construction.
    /// `recent_reactions` is a SAMPLE: Telegram truncates it for a message with
    /// many reactors, so a list of three when the tally says twenty must mark
    /// nothing at all. Dating the seventeen it could not see would invent
    /// removals for people who are still there — the same error v28 fixed, one
    /// layer down and easier to make, because here the short list looks like data
    /// rather than like absence.
    ///
    /// Everything in this archive reacts alone today, so the truncated branch is
    /// exercised by a test rather than by the data.
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

        // ⚠ `reacted_at` is NOT in the update list. Telegram reports the date of
        // the reaction as it currently stands, and re-reading the same reaction
        // must not restamp it — the first observation is the one that answers
        // "when did they do this?". A reaction taken back and put again is the
        // same row by key, and the `removed_at = NULL` is what says it is current
        // once more.
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
            // Matched on `(peer_id, reaction_key)` for the reason v17 and
            // `replace_telegram_reactions` both give: `reaction_key` is the only
            // non-null identity a reaction has, and `NOT IN` over a nullable
            // column is never true for anything.
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

    /// Record the formatting and the links the text does not carry.
    ///
    /// ⚠ **An empty list is not evidence of removal**, the same asymmetry as
    /// everywhere else here: a message with no formatting and a delivery that did
    /// not mention entities arrive identically, as `None` flattened to nothing.
    /// So an empty list marks nothing, and only a non-empty one — which is a
    /// complete statement of the current text's spans — may date what is missing.
    ///
    /// ⚠ **Identity is the SPAN, not a position.** An edit that inserts a bold run
    /// at the start renumbers every entity after it, so keying on an index would
    /// date spans that merely moved. See migration v31.
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

    /// Record how long a call was and how it ended.
    ///
    /// ⚠ **`duration_s` is enriched, never overwritten with NULL.** A call's
    /// service message can be delivered before the call ends — it is created when
    /// the call starts — so a later read is the one that knows how long it took.
    /// A plain upsert would let an early re-delivery erase a duration already
    /// learned, which is `COALESCE`'s job here and the same shape as the message
    /// enrichment above.
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

    /// Record a receipt, once, without ever restamping when it happened.
    ///
    /// ⚠ `INSERT IGNORE`, not an upsert. A receipt can be re-delivered; the first
    /// observation is the one that answers "when was this read", and an upsert
    /// would walk that answer forward every time the socket replayed.
    ///
    /// Returns how many rows were new, so a caller can log a receipt that taught
    /// the archive something and stay quiet about one that did not.
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

    /// Record one frame of a call's signalling.
    ///
    /// ⚠ `event_ts` is part of the KEY rather than a value, because a call can
    /// legitimately carry two frames of the same kind — a hangup from each of the
    /// other party's devices, say — and collapsing them on `(call, peer, event)`
    /// would keep one and silently drop the rest.
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

    /// The next batch of stored message ids to re-read, oldest first.
    ///
    /// Reads from what the archive HOLDS rather than from Telegram, because the
    /// point is to re-ask about messages already stored. Empty means this
    /// conversation is done.
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

    /// Move the re-capture frontier, AFTER the batch has been written.
    ///
    /// ⚠ `GREATEST` rather than an assignment: two passes over one conversation
    /// must not be able to walk the frontier backwards, which would silently
    /// re-do work already finished and — worse — look like progress.
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

    /// How many messages this conversation holds, for a progress line that means
    /// something.
    pub async fn telegram_message_count(&self, conversation_id: i64) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM telegram_messages WHERE conversation_id = ?")
                .bind(conversation_id)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// Whether this message's bytes are already accounted for — stored, offered or
    /// failed — so the eager pass can skip it without a download attempt.
    /// Record how far somebody has read, if it is further than we already knew.
    ///
    /// Returns whether a row was written — a mark we had already seen writes
    /// nothing, which is what keeps the hourly sweep from adding 21 rows an hour
    /// to a quiet archive.
    ///
    /// ⚠ **`INSERT IGNORE` on `(conversation, direction, max_id)`, NOT an upsert.**
    /// Each distinct mark keeps its own first-observation time forever; re-seeing
    /// one must not move that time, or the record would drift forward every hour
    /// and the answer to "when was this read?" would always be "recently".
    ///
    /// ⚠ **A `max_id` of 0 is NOT a mark**, it is Telegram's way of saying nothing
    /// has been read in that direction. Storing it would put a row at the bottom of
    /// every conversation claiming a read that never happened.
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

    /// Record bytes written to the volume.
    ///
    /// ⚠ Called AFTER the download returns, never before. The row is what tells the
    /// reader a file is there; writing it first would publish a path to a
    /// half-written file, and the reader has no way to tell a short file from a
    /// small one. Same ordering, and the same reason, as the Signal attachment path.
    ///
    /// ⚠ **It records no SIZE, deliberately — see the v25 migration.** The size lives
    /// in `telegram_messages.media_size`, where it came from the message rather than
    /// from a `stat` that races the write's visibility.
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
    /// ⚠ `offered` does NOT overwrite `stored`: a re-walk offers everything it sees,
    /// and without the guard it would retract files already on the volume.
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

    /// A reader asked for these bytes.
    ///
    /// ⚠ Only from `offered` or `failed`, and the `WHERE` is what enforces it. A
    /// request against something already `stored` would move a file that is on the
    /// volume back into a queue, and the fetch would then overwrite a good file with
    /// a fresh download of the same bytes. A request against something already
    /// `wanted` is a reader tapping twice and must not restart the clock.
    ///
    /// Returns whether anything changed, so the caller can tell "queued" from
    /// "there was nothing to queue" rather than reporting success either way.
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
    ///
    /// Oldest first because a queue that served the newest request would starve
    /// whoever asked while a large file was downloading — which is precisely the
    /// case this exists for.
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
    /// Already stored, and this delivery knew something the stored row did not —
    /// a media size or mime recorded by a build that came after the row.
    Enriched,
    /// Already stored, and nothing new. The common case on replay.
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

/// Whose reading a mark describes.
///
/// ⚠ Telegram's own words, and they read backwards until you hold the metaphor:
/// the OUT-tray is MY messages, so `Outbox` is how far THE OTHER SIDE has read
/// what I sent — the blue ticks. `Inbox` is how far I have read what they sent.
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
    /// Telegram has the bytes and we have not fetched them. The normal state for
    /// anything large.
    Offered,
    /// A reader asked for it and the feed has not got to it yet.
    Wanted,
    /// On the volume.
    Stored,
    /// Tried and could not — `note` says why.
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
