# signal — Signal message archive

Archives Signal messages into MariaDB on the **isis** k3s cluster, the same way
`home`/`health` archive their data. Two feeds into one schema:

- **Ongoing** — [`signal-cli-rest-api`](https://github.com/bbernhard/signal-cli-rest-api)
  links as a Signal **secondary device** and exposes received messages on a
  websocket; a small Rust **ingester** parses each frame into MariaDB.
- **History** (one-time, done) — an Android Signal **plaintext export**
  (backup-v2 JSONL) imported into the same tables by `tools/import_jsonl.py`.

```
                  one-time, on Mac
 Android Signal ──plaintext export──▶ main.jsonl ──▶ import_jsonl.py ─┐
   (history)        (backup-v2 JSONL)                                 │
                                                                      ▼
 Android Signal ──link (QR)──▶ signal-cli-rest-api ──ws──▶ ingester ──▶ MariaDB
   (ongoing)                   [json-rpc, PVC=keys]    (Rust)        [ns: signal]
```

Both feeds dedupe on `(sender_uuid, server_ts)` — a Signal timestamp is unique
per sender — so history and live overlap safely.

## Why signal-cli (not presage)
We first tried presage (all-Rust, in-process). Its secondary-device **linking
fails against current Signal servers with HTTP 409 / missing-capabilities**, even
on its latest commit. `signal-cli` (≥0.14.x, via the bbernhard REST image)
tracks Signal's required capabilities and links cleanly, so it owns the Signal
protocol; our Rust binary is reduced to a dumb, dependency-light websocket→DB
ingester (no libsignal/sqlcipher — fast, small build).

## Components
- `src/parse.rs` — **pure** frame→action mapping (`parse_frame`), no I/O. Unit-tested.
- `src/db.rs` — MariaDB schema (append-only `MIGRATIONS`, run on startup) + inserts.
- `src/main.rs` — the binary: connects to `ws://…/v1/receive/<number>`, parses each
  frame via `parse`, and executes the action against the DB; a heartbeat probes
  idle sockets and reconnects on drop. Also downloads attachment bytes and
  periodically refreshes group titles.
- `src/lib.rs` — exposes `parse`/`db` as a library so the logic is testable.
- `tools/import_jsonl.py` — the one-time history importer (plaintext JSONL
  export → the same tables, deduped against the live feed). See *History backfill*.
- `tools/reconcile_groups.py` — one-time fixer that rekeys master-key group
  threads to the live group ids (for history imported before `--groups-json`).
- `tools/import_gchat.py` — imports the Google Chat archive into SEPARATE
  `gchat_*` tables in the same DB (see *Other origins* below).

## Tests
The bug-prone part — mapping signal-cli's JSON to archive actions — is unit-tested
in `tests/parse.rs` (incoming/outgoing, groups, reactions, deletes, stickers,
attachments, jsonrpc-wrapping, skips). Run locally:
```
nix-shell -p cargo rustc --run "cargo test"
```
(`db.rs` SQL is not unit-tested — it needs a live MariaDB; CI gates the image on
`cargo test` via the `signal-verify` job.)
- `Dockerfile` — pure-Rust build (no C toolchain).
- `k8s/` — `00-namespace`, `01-pvc` (DB + signal-cli data), `02-db` (MariaDB),
  `03-signal-cli` (the rest-api engine), `04-ingester` (the Rust binary),
  `secret.sh` (DB creds; `SIGNAL_NUMBER` added after linking).

## Schema
`contacts`, `conversations` (`dm:<uuid>` / `group:<id>`), `messages`
(UNIQUE `(sender_uuid, server_ts)`), `attachments`, `reactions`. Identities are
keyed on the Signal ACI UUID (E.164 number as fallback). Deletes and edits are
non-destructive: a delete only flags `deleted`, and each edited version is a
separate row linked to the original via `edit_of_ts` — content is never removed.

Group threads are keyed by the signal-cli **group id** (base64 `groupInfo.groupId`,
== the groups-API `internal_id`). The Android export instead carries each group's
**master key** (a different value), so the importer maps master-key → group id by
group name (`--groups-json`, below) to land history in the same thread as the live
feed. DM threads (`dm:<uuid>`) need no mapping.

## Deploy (isis k3s, namespace `signal`)
1. Push to `main` → CI builds `xinutec/signal-archiver:latest` (the ingester).
2. `./k8s/secret.sh` (random DB creds; refuses to overwrite).
3. `kubectl apply -f k8s/00-namespace.yaml -f k8s/01-pvc.yaml -f k8s/02-db.yaml -f k8s/03-signal-cli.yaml`
4. **Link the device (your phone).** Fetch a QR PNG from the rest-api and scan it
   in **Signal → Settings → Linked devices → Link new device**:
   ```
   kubectl -n signal exec deploy/signal-cli-rest-api -- \
     curl -s 'http://localhost:8080/v1/qrcodelink?device_name=signal-archiver' -o /tmp/qr.png
   kubectl -n signal cp signal-cli-rest-api-<pod>:/tmp/qr.png ./qr.png   # then open/scan
   ```
   (The QR is a fresh, short-TTL provisioning link — scan promptly. signal-cli
   ≥0.14.x links without the 409.)
5. Discover the linked number and add it to the secret, then deploy the ingester:
   ```
   kubectl -n signal exec deploy/signal-cli-rest-api -- curl -s localhost:8080/v1/accounts
   kubectl -n signal patch secret signal-secret -p '{"stringData":{"SIGNAL_NUMBER":"+44..."}}'
   kubectl apply -f k8s/04-ingester.yaml
   ```
6. Verify: `kubectl -n signal exec deploy/signal-db -- mariadb -usignal -p signal -e 'SELECT COUNT(*) FROM messages;'`

## History backfill (one-time, done)
Source is a Signal Android **plaintext export**, not the encrypted `.backup`:
Signal Android (beta) → "export" → `Documents/signal-export-*/main.jsonl`, a
stream of `account` / `recipient` / `chat` / `chatItem` / `stickerPack` frames.
Fetch the groups list (so group history merges with the live feed — see *Schema*):
```
NUM=$(curl -s localhost:8080/v1/accounts | sed 's/[][\"]//g')   # inside the rest-api pod
curl -s localhost:8080/v1/groups/$NUM > groups.json
```
Copy `main.jsonl` (and `groups.json`) off the device/cluster and run on the Mac:
```
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal SELF_UUID=<your ACI> \
  ./tools/import_jsonl.py main.jsonl --groups-json=groups.json [--dry-run] [--limit=N]
```
It resolves the export's internal recipient/chat ids and writes messages,
contacts, conversations, reactions, attachment **metadata**, and edit history
into the same tables, deduped on `(sender_uuid, server_ts)` via `INSERT IGNORE`
— so it is safe to run alongside the live feed and to re-run. Attachment **bytes**
are not imported (they live in the export's `files/` tree keyed by hash).

If you imported without `--groups-json`, group history sits under master-key
threads; `tools/reconcile_groups.py groups.json [--apply]` rekeys them to the live
group ids (dry-run by default).

## Scope / known follow-ups
- Live feed archives **incoming** text + quotes + attachment metadata + **bytes**
  + reactions, and **outgoing** messages (linked-device "Sent" sync); it resolves
  contact names (DM thread names) and refreshes group titles.
- The JSONL importer archives the same, **minus attachment bytes** (metadata only).
- Group threads unify across feeds via the `--groups-json` name→groupId map
  (`reconcile_groups.py` fixes any pre-mapping import). The only soft spot is the
  name match — a renamed or duplicate group title can't be mapped and falls back
  to the masterKey key (the importer warns); a future hardening is matching on the
  member set instead of the name.

## Other origins — Google Chat (separate tables)
This DB is also the store for **other** message origins, each in its OWN tables
(not merged into the Signal schema — the shapes differ too much to unify cleanly).

**Google Chat** (`gchat_conversations`, `gchat_messages`, `gchat_reactions`):
imported from the decoded archive produced by `~/Code/gchat-archive` (a CDP
reverse-engineering capture, NOT a Takeout) by `tools/import_gchat.py`. Differences
from Signal that justify separate tables: reactions are **aggregated** (`emoji` +
`cnt`) not per-author events; messages carry Google **threading** (`thread_id`) and
numeric `sender_id`; self is a `(you)` name suffix → stored as `is_self`. Messages
dedupe on `(group_id, msg_id)`, names/reaction-counts upsert, so it is re-runnable.
```
# port-forward the DB, then on the machine holding the archive:
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal \
  ./tools/import_gchat.py [conversations_dir] [--apply]   # dry-run by default
```
The importer creates the `gchat_*` tables itself (`CREATE TABLE IF NOT EXISTS`);
they are independent of the Rust ingester's `MIGRATIONS` (which owns only the
Signal tables).

## Security
The signal-cli data PVC holds linked-device keys — secret-class; keep its odin
backup encrypted. The DB holds private conversations (Signal + the imported Google
Chat history, same private class); real content stays out of git.
