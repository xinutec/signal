//! MariaDB archive store. Append-only migrations, same convention as the
//! `home`/`health` services: each entry runs exactly once, tracked by index in
//! `schema_version`. To evolve the schema, APPEND a new entry — never edit an
//! existing one.

use anyhow::Result;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};

use crate::parse::ThreadId;

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
}

/// Rows per statement. 9 columns × 1,000 is well inside MySQL's 65,535
/// placeholder cap, with room for the column list to grow.
const INSERT_CHUNK: usize = 1_000;

/// One logged line, ready to write. Owned rather than borrowed: it is built per
/// file and handed straight to the batch, and threading a lifetime through that
/// buys nothing at 72 rows.
pub struct IrcLine {
    pub line_no: u32,
    pub sent_at: String,
    pub nick: Option<String>,
    pub is_self: bool,
    pub kind: &'static str,
    pub text: String,
}
