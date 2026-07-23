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
reactions[{emoji, count}]}. `sender_name` carries a trailing " (you)" for self.

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

    stats = {"conversations": 0, "messages": 0, "dups": 0, "reactions": 0, "skipped": 0}
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
                "INSERT IGNORE INTO gchat_messages "
                "(group_id, msg_id, thread_id, sender_id, sender_name, is_self, ts_us, sent_at, text) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (gid, msg_id, m.get("thread_id"), m.get("sender_id"), disp, is_self,
                 ts_us, sent_at, m.get("text")))
            if cur.rowcount != 0:
                stats["messages"] += 1
                message_id = cur.lastrowid
            else:
                stats["dups"] += 1
                cur.execute("SELECT id FROM gchat_messages WHERE group_id=%s AND msg_id=%s",
                            (gid, msg_id))
                message_id = cur.fetchone()[0]

            for r in m.get("reactions") or []:
                emoji = r.get("emoji")
                if not emoji:
                    continue
                stats["reactions"] += 1
                cur.execute(
                    "INSERT INTO gchat_reactions (message_id, emoji, cnt) VALUES (%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE cnt=VALUES(cnt)",
                    (message_id, emoji, int(r.get("count") or 0)))

    if apply:
        conn.commit()
    conn.close()
    print(f"{'' if apply else 'DRY-RUN '}done ({len(files)} files): {stats}")


if __name__ == "__main__":
    main()
