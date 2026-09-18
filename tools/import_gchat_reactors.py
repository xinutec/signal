#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p "python3.withPackages(ps: [ps.pymysql])"
"""Import WHO reacted in Google Chat, from the reactor cache sync.py builds.

⚠ **`list_topics` GIVES A REACTION AS `[emoji, count]` AND NEVER WHO.** That is
why `gchat_reactions` has no author column and why the viewer drew Google Chat
reactions as a bare count while Signal and Telegram could name people. The names
exist, but only because ~/Code/gchat-archive's `sync.py` replays a SECOND rpc
(Q3DB7e) per reacted message and caches what it learns in
`archive/captures-raw-backfill/reactors.json`. Nothing has ever read that file
into the database.

⚠ **THAT FILE IS ALSO THE ONLY COPY.** `archive/` is gitignored, so the cache
lives on one Mac, in a directory no backup covers — every declared artifact in
xinutec-infra's backup plan is a fleet PVC. Importing is therefore not only a
feature: it moves the names somewhere that gets backed up.

Cache shape: `{"<msg_id>\\t<emoji>\\t<count>": ["<google user id>", ...]}`. The
count in the key is the reaction's total, which can exceed the number of ids —
Q3DB7e is capped like Telegram's `recent_reactions`, so a short list here means
"these are the ones we learned", never "these are all there were".

Measured 2026-09-18: 89 keys → 115 reactor rows, 15 distinct people, every row
matching a stored message and every reactor nameable from `gchat_messages`.

Usage (env: DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME):

    ./import_gchat_reactors.py [reactors.json] --apply
    ./import_gchat_reactors.py [reactors.json] --sql > load.sql

⚠ `--sql` exists because the documented import path needs `signal-db`
port-forwarded and that is not always available — the database lives in the
cluster and the cache lives on this Mac. It prints the same statements `--apply`
would run, from the same code, so the two cannot drift; pipe it wherever the
database can be reached. Without either flag this is a dry run that still
connects, matching `import_gchat.py`.
"""
import json
import os
import sys

DDL = [
    """CREATE TABLE IF NOT EXISTS gchat_reaction_authors (
        id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT NOT NULL,
        emoji      VARCHAR(64) NOT NULL,
        reactor_id VARCHAR(32) NOT NULL,
        UNIQUE KEY uniq_gchat_reactor (message_id, emoji, reactor_id),
        INDEX idx_gchat_reactor_msg (message_id)
    ) DEFAULT CHARSET=utf8mb4""",
]

# ⚠ Resolved against `gchat_messages.msg_id` rather than taken from the key,
# because `gchat_reactions.message_id` is the surrogate `id` and the cache is
# keyed by Google's own message id. Joining in SQL rather than pre-resolving in
# Python keeps this a single statement per row and idempotent: a row whose
# message is not (yet) imported inserts nothing rather than failing.
INSERT = """INSERT IGNORE INTO gchat_reaction_authors (message_id, emoji, reactor_id)
            SELECT m.id, %s, %s FROM gchat_messages m WHERE m.msg_id = %s"""


def rows(cache):
    """Every (emoji, reactor_id, msg_id) the cache names, deduplicated.

    A key whose value is an empty list is a message sync.py asked about and
    could not resolve — it is NOT a message nobody reacted to, so it yields
    nothing rather than an empty marker.
    """
    out = []
    seen = set()
    for key, reactors in cache.items():
        if not reactors:
            continue
        parts = key.split("\t")
        if len(parts) != 3:
            continue
        msg_id, emoji, _count = parts
        for who in reactors:
            row = (emoji, who, msg_id)
            if row not in seen:
                seen.add(row)
                out.append(row)
    return out


def sql_literal(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    opts = [a for a in sys.argv[1:] if a.startswith("--")]
    path = args[0] if args else os.path.expanduser(
        "~/Code/gchat-archive/archive/captures-raw-backfill/reactors.json")
    with open(path) as f:
        cache = json.load(f)
    found = rows(cache)

    if "--sql" in opts:
        for stmt in DDL:
            print(stmt.strip() + ";")
        for emoji, who, msg_id in found:
            print(
                INSERT.replace("%s", "{}", 1).format(sql_literal(emoji))
                .replace("%s", sql_literal(who), 1)
                .replace("%s", sql_literal(msg_id), 1)
                + ";"
            )
        print(
            f"-- {len(cache)} cached keys -> {len(found)} reactor rows",
            file=sys.stderr,
        )
        return

    import pymysql

    conn = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"], charset="utf8mb4", autocommit=False)
    cur = conn.cursor()
    apply = "--apply" in opts
    if apply:
        for stmt in DDL:
            cur.execute(stmt)
        for emoji, who, msg_id in found:
            cur.execute(INSERT, (emoji, who, msg_id))
        conn.commit()
    print(f"{len(cache)} cached keys -> {len(found)} reactor rows"
          f"{'' if apply else ' (dry run, nothing written)'}")


if __name__ == "__main__":
    main()
