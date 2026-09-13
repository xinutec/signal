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
- `src/telegram/` + `src/bin/telegram.rs` — the Telegram feed. `map.rs` is the
  pure wire-type→row mapping (the `parse.rs` of that origin, unit-tested with no
  account); `session.rs` keeps the MTProto session in MariaDB; the binary does
  login, backfill and the live update stream. See *Other origins* below.

## Tests
`tests/parse.rs` unit-tests the bug-prone part — mapping signal-cli's JSON to
archive actions (incoming/outgoing, groups, reactions, deletes, stickers,
attachments, jsonrpc-wrapping, skips). No I/O, always runs.

Two suites need a real MariaDB and **skip silently without one**, so a bare
`cargo test` proves less than it looks like it does:
- `tests/irc_stats.rs` — the `irc_conversation_stats` triggers (v11–v14): what
  counts, that replay is free, and that DELETE and edits to
  `conversation_id`/`kind`/`sent_at` are refused.
- `tests/import_irclogs.rs` — the importer's incremental behaviour, the one
  failure mode that produces no error and no row.

`tests/telegram_store.rs` is a third, and what it pins is everything only the
DATABASE can decide: that an edit keeps the text it replaces and a replayed edit
appends nothing, that a peer-less deletion does not reach a channel sharing the
number, that a withdrawn reaction stops being counted, that the backfill frontier
never moves backwards, and that a session round-trips WITH its auth key. Two real
defects were found by running it and by nothing else — a primary key over nullable
columns, which MariaDB silently makes `NOT NULL` and which rejected every unicode
reaction; and a gap-lock deadlock between the backfill and the live stream, which
only appears when two writers are in flight. Both are written up where they were
fixed (`db.rs` v17, and `store_telegram_message`).

Telegram's mapping is unit-tested in `src/telegram/map/` and needs no account and
no database: `tl::types::Message` is generated from Telegram's own schema, so a
fixture is a struct literal the compiler checks against the real contract rather
than a captured blob that can drift from it. The DM-attribution rule was ablated
both ways (arms swapped, and the inference let out of DMs) and each fails exactly
one test.

All key off `SIGNAL_TEST_DATABASE_URL`. CI supplies a `mariadb:11.8` service;
locally use `dev-lint#with-test-db`. Each test tags its rows uniquely per run as
well as per test, so running the suite twice against one database is safe.
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
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal \
  SELF_UUID=<your ACI> SELF_PHONE=<your E.164 number> \
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

## Other origins — Telegram (separate tables, one login)
`telegram_conversations`, `telegram_messages`, `telegram_reactions`,
`telegram_message_edits`, `telegram_backfill_state`, `telegram_session` — created by
the Rust `MIGRATIONS` (v15–v20), unlike the `gchat_*` tables.

**The only origin whose past and present come from one feed.** Telegram keeps
history server-side, so a single authorised session pages backwards through
everything AND holds the update stream. No export, no second import path, and the
two halves cannot disagree because they write the same rows on the same key:
`(conversation_id, msg_id)` is the message's own server-assigned identity, which is
a stronger dedupe key than either of the other origins has.

```
# once per account lifetime, interactively (a code arrives on the phone):
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal \
  TELEGRAM_API_ID=… TELEGRAM_API_HASH=… \
  cargo run --bin telegram -- login '+31…' 
# then, as a Deployment: no arguments.
```

⚠ **`telegram login` cannot be run with `kubectl exec` into the Deployment.** That
pod refuses to start until a session exists — deliberately, because a feed which is
quietly not logged in looks exactly like a quiet week — so there is no running
container to exec into. In-cluster it is a throwaway pod with the same environment;
`kubes/signal/k8s/secret.sh` prints the command. Locally it is the form above, with
`signal-db` port-forwarded.

`TELEGRAM_API_ID` / `TELEGRAM_API_HASH` come from <https://my.telegram.org>, are
Pippijn's own, and live in `signal-secret`.

⚠ **A BOT CANNOT DO THIS.** A Telegram bot is a separate account and cannot read
the chats of the person who owns it, so this is a USER client speaking MTProto.
That is also why `telegram_session` holds a credential rather than a cache: the row
is a logged-in session, and re-logging-in is rate-limited by Telegram in hours.
Losing it is not free.

⚠ **`grammers` owns the protocol, for the reason `signal-cli` owns Signal's.** The
DH handshake, AES-IGE, the message containers and the TL schema are the parts that
rot silently when the other side bumps a layer. What is ours is the mapping and the
rows. Unlike signal-cli it is a crate rather than a sidecar, so there is no REST
hop and no third-party container holding the keys.

⚠ **`Cargo.lock` pins `glass_pumpkin` to `2.0.0-rc0` and a `cargo update` undoes
it**, breaking the build inside `grammers-crypto`. The note in `Cargo.toml` has the
one-line fix.

⚠ **Secret chats are not here and cannot be**: device-local by construction, so no
login reaches them.

What Telegram does that the others do not, and where each is handled:

| | how Telegram does it | where |
| --- | --- | --- |
| a DM names no sender | `from_id` omitted; `out` says which end | `map.rs`, inferred and tested both ways |
| an edit date is not an edit to SHOW | `edit_hide` sits beside `edit_date`: "shown as not modified to the user, even if an edit date is present" | recorded in `edit_hidden`, honoured by the viewer — 606 hidden against 51 genuine when the archive re-read itself |
| an edit MUTATES the message | same `msg_id`, new text, new `edit_date` | the prior text is filed in `telegram_message_edits` before the update, in one transaction — but only when the archive HELD the prior text, so backfilled edits have none and never will (558 such on the first ingest) |
| a deletion names no peer | private chats and basic groups share ONE id sequence; channels have their own | `Db::mark_telegram_deleted` takes a SCOPE, and a peer-less deletion never reaches a channel |
| a supergroup looks like a channel | same id space, told apart by a flag on the peer | the id gives `PeerSpace`, the peer gives `ConvKind`; the dialog sweep is what corrects it |
| reactions are counts | aggregated per emoji, not per author | stored as given; a custom emoji keeps its document id |

Media is NOT downloaded: `media_kind` records that there was a photo. Attachment
bytes are Signal-only in this archive.

**First ingest, 2026-09-13** — 21 conversations, all of them DMs, so nothing here
has yet exercised the group, supergroup or channel paths. Of 472 reaction rows,
**none** were custom emoji, so that branch is unit-tested and unexercised by real
data. The DM sender inference — the one rule in `map.rs` that guesses — produced
**zero NULL senders across 5,523 DM messages**, which is the number worth having:
it is the only thing that says the inference fires at all on real rows. It does
NOT say the two arms are the right way round; the 2,646/2,879
incoming/outgoing split would look just as plausible reversed, and only the
ablated unit tests speak to that.

## Security
The signal-cli data PVC holds linked-device keys — secret-class; keep its odin
backup encrypted. The DB holds private conversations (Signal + the imported Google
Chat history, same private class); real content stays out of git.
