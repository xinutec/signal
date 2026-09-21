#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p "python3.withPackages(ps: [ps.pymysql])"
"""Import the Google Chat archive into its OWN tables in the signal MariaDB.

Google Chat and Signal are different enough (aggregated emoji-count reactions vs
Signal's per-author reaction events, Google message threading, numeric sender ids)
that they get SEPARATE `gchat_*` tables rather than being forced into the Signal
schema. They share the database only.

Source is the decoded archive produced by ~/Code/gchat-archive (NOT a Takeout):
each `conversations/<group_id>.json` has {group_id, name, message_count, messages[]},
and each message has {msg_id, thread_id, sender_id, sender_name, text, ts, ts_raw,
reactions[{emoji, count, reactors[{id, name}]}]}. `sender_name` carries a trailing
" (you)" for self.

⚠ `reactors` is the ONLY record of who reacted — Google Chat's `list_topics`
gives a reaction as `[emoji, count]` and never an author, so the names come from
a second rpc sync.py replays and merges into this export. It is budget-capped per
run, so a reaction with no `reactors` means "not resolved yet", never "nobody".

Idempotent: messages dedupe on (group_id, msg_id) via INSERT IGNORE; conversation
names and reaction counts are upserted, so re-running picks up a fresh export.

Usage (env: DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME):
    ./import_gchat.py [conversations_dir] [--apply]
Defaults the dir to ~/Code/gchat-archive/archive/conversations and to a dry-run;
pass --apply to write.
"""
import datetime as dt
import glob
import json
import os
import sys

import pymysql

DDL = [
    """CREATE TABLE IF NOT EXISTS gchat_conversations (
        group_id   VARCHAR(64) NOT NULL PRIMARY KEY,
        name       VARCHAR(255) NULL,
        is_dm      TINYINT(1) NOT NULL DEFAULT 0,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS gchat_messages (
        id          BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        group_id    VARCHAR(64) NOT NULL,
        msg_id      VARCHAR(64) NOT NULL,
        thread_id   VARCHAR(64) NULL,
        reply_to_msg_id VARCHAR(64) NULL,
        sender_id   VARCHAR(32) NULL,
        sender_name VARCHAR(255) NULL,
        is_self     TINYINT(1) NOT NULL DEFAULT 0,
        ts_us       BIGINT NOT NULL,
        sent_at     DATETIME(6) NULL,
        text        TEXT NULL,
        created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_gchat_msg (group_id, msg_id),
        INDEX idx_gchat_conv_ts (group_id, ts_us)
    ) DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS gchat_attachments (
        id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT NOT NULL,
        name       VARCHAR(255) NULL,
        mime       VARCHAR(128) NULL,
        width      INT NULL,
        height     INT NULL,
        -- ⚠ **NOT A UUID, AND 64 WAS TOO NARROW.** Google Chat synthesises this
        -- id and it can be FILENAME-DERIVED: one attachment in the archive
        -- carries 138 characters of Windows path ending in `.pdf972130`. At
        -- VARCHAR(64) it was silently truncated on insert — the importer's
        -- session had no strict mode, so no error — and the row could then no
        -- longer be found by its own id. Worse, `UNIQUE (message_id, uuid)`
        -- means two long ids on one message would collide at 64 characters and
        -- the second INSERT would be dropped rather than refused.
        uuid       VARCHAR(255) NULL,
        token      TEXT NULL,
        hash1      VARCHAR(128) NULL,
        hash2      VARCHAR(128) NULL,
        UNIQUE KEY uniq_gchat_attachment (message_id, uuid),
        INDEX idx_gchat_attachment_msg (message_id)
    ) DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS gchat_reaction_authors (
        id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT NOT NULL,
        emoji      VARCHAR(64) NOT NULL,
        reactor_id VARCHAR(32) NOT NULL,
        UNIQUE KEY uniq_gchat_reactor (message_id, emoji, reactor_id),
        INDEX idx_gchat_reactor_msg (message_id)
    ) DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS gchat_reactions (
        id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT NOT NULL,
        emoji      VARCHAR(64) NULL,
        cnt        INT NOT NULL DEFAULT 0,
        UNIQUE KEY uniq_gchat_reaction (message_id, emoji),
        INDEX idx_gchat_react_msg (message_id)
    ) DEFAULT CHARSET=utf8mb4""",
]


def self_split(sender_name):
    """Strip the trailing " (you)" self-marker; return (display_name, is_self)."""
    if sender_name and sender_name.endswith(" (you)"):
        return sender_name[: -len(" (you)")], 1
    return sender_name, 0


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    opts = [a for a in sys.argv[1:] if a.startswith("--")]
    apply = "--apply" in opts  # dry-run unless explicitly applied
    conv_dir = args[0] if args else os.path.expanduser(
        "~/Code/gchat-archive/archive/conversations")

    files = sorted(glob.glob(os.path.join(conv_dir, "*.json")))
    if not files:
        sys.exit(f"no conversation JSON found in {conv_dir}")

    conn = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"], charset="utf8mb4", autocommit=False)
    cur = conn.cursor()
    if apply:
        for stmt in DDL:
            cur.execute(stmt)

    # What the fetcher managed to pull, by the same key it wrote.
    stored = {}
    by_msg = os.path.join(os.path.dirname(conv_dir), "attachments", "by_message.json")
    if os.path.exists(by_msg):
        with open(by_msg) as fh:
            stored = json.load(fh)

    stats = {"conversations": 0, "messages": 0, "dups": 0, "reactions": 0,
             "reactors": 0, "attachments": 0, "skipped": 0}
    for path in files:
        with open(path) as f:
            conv = json.load(f)
        gid = conv.get("group_id")
        if not gid:
            continue
        name = conv.get("name") or None
        is_dm = 1 if (name or "").startswith("DM with ") else 0
        stats["conversations"] += 1
        if apply:
            cur.execute(
                "INSERT INTO gchat_conversations (group_id, name, is_dm) VALUES (%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE name=COALESCE(VALUES(name), name), is_dm=VALUES(is_dm)",
                (gid, name, is_dm))

        for m in conv.get("messages", []):
            msg_id = m.get("msg_id")
            ts_raw = m.get("ts_raw")
            if not msg_id or not ts_raw:
                stats["skipped"] += 1
                continue
            ts_us = int(ts_raw)
            sent_at = dt.datetime.fromtimestamp(ts_us / 1_000_000, dt.timezone.utc).replace(tzinfo=None)
            disp, is_self = self_split(m.get("sender_name"))

            if not apply:
                stats["messages"] += 1
                stats["reactions"] += len(m.get("reactions") or [])
                continue

            cur.execute(
                # ⚠ `reply_to_msg_id` is NOT `thread_id`. A topic id says which
                # conversation a message belongs to; this says which MESSAGE it
                # answers. Chat DMs have no topics and do have quote-replies, which
                # is exactly why reading one as the other concluded — wrongly —
                # that DMs carried no reply information at all.
                "INSERT IGNORE INTO gchat_messages "
                "(group_id, msg_id, thread_id, reply_to_msg_id, sender_id, sender_name, "
                " is_self, ts_us, sent_at, text) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (gid, msg_id, m.get("thread_id"),
                 (m.get("reply_to") or {}).get("msg_id"),
                 m.get("sender_id"), disp, is_self,
                 ts_us, sent_at, m.get("text")))
            if cur.rowcount != 0:
                stats["messages"] += 1
                message_id = cur.lastrowid
            else:
                stats["dups"] += 1
                cur.execute("SELECT id FROM gchat_messages WHERE group_id=%s AND msg_id=%s",
                            (gid, msg_id))
                message_id = cur.fetchone()[0]

            # ⚠ **THE PICTURES, WHICH THIS ARCHIVE HAD NEVER RECORDED AT ALL.**
            # A Google Chat message is a 39-element array and the capture read six
            # indices; attachments are at 10. 326 of 7,042 messages carry one —
            # and only 20 of those are wordless, so the other 306 rendered as
            # ordinary text messages with a caption and no picture. Nothing said a
            # picture had been there.
            #
            # ⚠ **THE BYTES ARE NOT HERE AND THIS ROW CANNOT FETCH THEM.** The
            # client mints a `lh3.googleusercontent.com/chat_attachment/AP1Ws4…`
            # URL at render time from `token`; that URL appears nowhere in the
            # capture, and it answers 403 without Pippijn's session. So this table
            # records that a picture EXISTED, who sent it, when, its name and its
            # dimensions — and the two content hashes, which are the only way a
            # byte stream obtained later could ever be matched back to it.
            for a in m.get("attachments") or []:
                stats["attachments"] += 1
                cur.execute(
                    "INSERT INTO gchat_attachments "
                    "(message_id, name, mime, width, height, uuid, token, hash1, hash2) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE name=VALUES(name), mime=VALUES(mime), "
                    "width=VALUES(width), height=VALUES(height), token=VALUES(token)",
                    (message_id, a.get("name"), a.get("mime"), a.get("width"),
                     a.get("height"), a.get("uuid"), a.get("token"),
                     a.get("hash1"), a.get("hash2")))
                # ⚠ The bytes, if `fetch_attachments.py` has them. Keyed on
                # (group, message, uuid) — the attachment's identity in the
                # archive — because a file that cannot name its message is a file
                # the viewer can only guess about, and guessing hangs the wrong
                # photo on the wrong message.
                held = stored.get(f"{gid}\t{msg_id}\t{a.get('uuid')}")
                if held:
                    # ⚠ **`<=>`, NOT `=`.** 58 of this archive's 326 attachments
                    # have NO uuid, and `uuid = NULL` is never true — so a plain
                    # `=` silently updated nothing for every one of them and the
                    # pictures stayed unreachable while the import reported
                    # success. `<=>` is the NULL-safe comparison and matches the
                    # row the manifest is talking about.
                    cur.execute(
                        "UPDATE gchat_attachments SET stored_path=%s "
                        "WHERE message_id=%s AND uuid <=> %s",
                        (held["file"], message_id, a.get("uuid")))

            for r in m.get("reactions") or []:
                emoji = r.get("emoji")
                if not emoji:
                    continue
                stats["reactions"] += 1
                cur.execute(
                    "INSERT INTO gchat_reactions (message_id, emoji, cnt) VALUES (%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE cnt=VALUES(cnt)",
                    (message_id, emoji, int(r.get("count") or 0)))

                # ⚠ **WHO reacted, which `list_topics` does NOT give.** A reaction
                # arrives as [emoji, count]; the names come from a second rpc that
                # gchat-archive's sync.py replays per reacted message and merges
                # into this same export as `reactors: [{id, name}]`.
                #
                # ⚠ **AN ABSENT `reactors` IS NOT "NOBODY REACTED".** That replay
                # is budget-capped per run and carries an unfinished backlog on
                # purpose, so most reaction groups have no names yet and running
                # the sync again resolves more. Rows are therefore only ever
                # ADDED here — never cleared to match a short list, which would
                # discard on every import what the previous one had learned.
                for who in (r.get("reactors") or []):
                    rid = who.get("id") if isinstance(who, dict) else who
                    if not rid:
                        continue
                    stats["reactors"] += 1
                    cur.execute(
                        "INSERT IGNORE INTO gchat_reaction_authors "
                        "(message_id, emoji, reactor_id) VALUES (%s,%s,%s)",
                        (message_id, emoji, str(rid)))

    if apply:
        conn.commit()
    conn.close()
    print(f"{'' if apply else 'DRY-RUN '}done ({len(files)} files): {stats}")


if __name__ == "__main__":
    main()
