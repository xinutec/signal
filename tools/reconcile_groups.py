#!/usr/bin/env nix-shell
#!nix-shell -i python3 -p "python3.withPackages(ps: [ps.pymysql])"
"""One-time reconciler: merge masterKey-keyed group threads into their groupId.

The history importer (`import_jsonl.py`) keyed group conversations on the Signal
export's `masterKey`, while the live ingester keys them on signal-cli's derived
`groupInfo.groupId` (== the groups-API `internal_id`). Those are different values,
so a group's history and its live messages landed in two separate `group:` threads.

This rekeys every `group:<masterKey>` thread to `group:<groupId>`, matching on the
group NAME against the signal-cli groups list, and merges into the live thread
where one already exists. The global UNIQUE keys (messages on (sender_uuid,
server_ts), reactions on (author_uuid, target_ts, reaction_ts)) are thread-id
independent, so rekeying can never collide — each logical row exists exactly once.

Get the groups JSON the same way the README's deploy step does:
    NUM=$(curl -s localhost:8080/v1/accounts | sed 's/[][\"]//g')
    curl -s localhost:8080/v1/groups/$NUM > groups.json

Usage (env: DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME):
    ./reconcile_groups.py groups.json [--dry-run]
Defaults to --dry-run; pass --apply to write.
"""
import json
import os
import sys

import pymysql


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    opts = [a for a in sys.argv[1:] if a.startswith("--")]
    if not args:
        sys.exit("usage: reconcile_groups.py groups.json [--apply]")
    apply = "--apply" in opts  # dry-run unless explicitly applied

    with open(args[0]) as f:
        groups = json.load(f)

    # name -> groupId (internal_id). Abort on a duplicate name: matching would be
    # ambiguous and could merge the wrong threads.
    name_to_gid = {}
    dupes = set()
    for g in groups:
        name = g.get("name")
        gid = g.get("internal_id")
        if not name or not gid:
            continue
        if name in name_to_gid and name_to_gid[name] != gid:
            dupes.add(name)
        name_to_gid[name] = gid
    if dupes:
        sys.exit(f"ambiguous: {len(dupes)} group name(s) map to >1 group: {sorted(dupes)}")
    known_gids = set(name_to_gid.values())

    conn = pymysql.connect(
        host=os.environ["DB_HOST"], port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"], charset="utf8mb4", autocommit=False)
    cur = conn.cursor()

    cur.execute("SELECT thread_id, name FROM conversations WHERE type='group'")
    rows = cur.fetchall()

    plans, skipped, unmatched = [], [], []
    for thread_id, name in rows:
        cur_id = thread_id[len("group:"):] if thread_id.startswith("group:") else thread_id
        if cur_id in known_gids:
            skipped.append((thread_id, name))           # already the real groupId
            continue
        gid = name_to_gid.get(name)
        if not gid:
            unmatched.append((thread_id, name))          # not in the groups list
            continue
        plans.append((thread_id, f"group:{gid}", name))

    print(f"{len(skipped)} already correct, {len(plans)} to rekey, "
          f"{len(unmatched)} unmatched")
    for old, _ in skipped:
        print(f"  ok    {old}")
    for old, new, name in plans:
        cur.execute("SELECT COUNT(*) FROM messages WHERE thread_id=%s", (old,))
        nmsg = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM conversations WHERE thread_id=%s", (new,))
        merge = " (merge into existing live thread)" if cur.fetchone()[0] else ""
        print(f"  rekey {name!r}: {old} -> {new}  [{nmsg} msgs]{merge}")
    for old, name in unmatched:
        print(f"  WARN  no groups-list match for {name!r} ({old}); left as-is")

    if not apply:
        print("\nDRY-RUN (pass --apply to write). No changes made.")
        return

    for old, new, name in plans:
        cur.execute("UPDATE messages  SET thread_id=%s WHERE thread_id=%s", (new, old))
        cur.execute("UPDATE reactions SET thread_id=%s WHERE thread_id=%s", (new, old))
        cur.execute(
            "INSERT INTO conversations (thread_id, type, name) VALUES (%s,'group',%s) "
            "ON DUPLICATE KEY UPDATE name=COALESCE(name, VALUES(name))",
            (new, name))
        cur.execute("DELETE FROM conversations WHERE thread_id=%s", (old,))
    conn.commit()
    conn.close()
    print(f"\nAPPLIED: rekeyed {len(plans)} group thread(s).")


if __name__ == "__main__":
    main()
