# signal — message archive

Archives Signal, Telegram, IRC and Google Chat messages into one MariaDB on the
**isis** k3s cluster (namespace `signal`). Each origin has its own tables.

```
 Android Signal ──plaintext export──▶ import_jsonl.py ──────────────┐  (history, once)
 Android Signal ──link (QR)──▶ signal-cli-rest-api ──ws──▶ ingester ─┤
 Telegram ──MTProto──▶ telegram (history + live, one session) ───────┤
 irssi autologs ──rsync──▶ import_irclogs; irssi plugin ──▶ irc_tail ┼──▶ MariaDB
 gchat-archive capture ──▶ import_gchat.py ──────────────────────────┘
```

signal-cli owns the Signal protocol (presage's linking fails against Signal's
servers), so the ingester is a websocket-to-database client with no libsignal.

## Components
- `src/parse.rs` — pure Signal frame → action mapping.
- `src/db.rs` — the schema (append-only `MIGRATIONS`, applied on startup) and
  every write.
- `src/main.rs` — the Signal ingester: holds `ws://…/v1/receive/<number>`,
  executes parsed actions, stores attachment bytes, refreshes contact and group
  names.
- `src/telegram/` + `src/bin/telegram.rs` — the Telegram feed. `map.rs` is the
  pure wire → row mapping; `session.rs` keeps the MTProto session in MariaDB.
- `src/irclog.rs`, `src/bin/import_irclogs.rs`, `src/bin/irc_tail.rs` — irssi
  autolog parsing, the periodic importer, and the live tail. Both write the same
  rows on the same dedupe key.
- `tools/import_jsonl.py` — the Signal history import.
- `tools/reconcile_groups.py` — rekeys master-key group threads to live group ids.
- `tools/import_gchat.py` — the Google Chat import.
- `tools/check_known_truths.py` — checks the live database still answers the
  facts in `tools/known_truths.tsv`.

Manifests are in `kubes/dhall/apps/signal.dhall` in the `pippijn/code` repo.

## Tests
`tests/parse.rs`, `tests/telegram_map.rs`, `tests/irclog.rs`,
`tests/telegram_session.rs` and `tests/attach.rs` need nothing.

`tests/contacts.rs`, `tests/telegram_store.rs`, `tests/irc_stats.rs` and
`tests/import_irclogs.rs` need a MariaDB at `SIGNAL_TEST_DATABASE_URL` and skip
without one; the first three fail instead when `CI` is set. CI supplies a
`mariadb:11.8` service; locally use `dev-lint#with-test-db`. Each test's rows are
unique per run, so the suite can run repeatedly against one database.

## Schema
Signal: `contacts` (+ `contact_names`, names over time), `conversations`
(`dm:<uuid>` / `group:<id>`), `messages` (unique `(sender_uuid, server_ts)`),
`attachments`, `reactions`, `signal_receipts`, `signal_call_events`, and
`signal_frames`, every raw frame as it arrived. Identities are ACI UUIDs, E.164
as fallback. Deletes flag the row and edits are separate rows linked by
`edit_of_ts`; nothing is overwritten.

Group threads are keyed by signal-cli's group id (base64 `groupInfo.groupId`).
The Android export carries each group's master key instead, so the importer maps
master key → group id by name (`--groups-json`).

Telegram: `telegram_*`, keyed by Telegram's own `(conversation_id, msg_id)`.
IRC: `irc_*`. Google Chat: `gchat_*`, created by `tools/import_gchat.py` itself
rather than by `MIGRATIONS`.

## Linking Signal
Fetch a QR code and scan it in Signal → Settings → Linked devices → Link new
device. It expires quickly.
```
kubectl -n signal exec deploy/signal-cli-rest-api -- \
  curl -s 'http://localhost:8080/v1/qrcodelink?device_name=signal-archiver' -o /tmp/qr.png
kubectl -n signal cp signal-cli-rest-api-<pod>:/tmp/qr.png ./qr.png
```
Then put the linked number in `signal-secret`:
```
kubectl -n signal exec deploy/signal-cli-rest-api -- curl -s localhost:8080/v1/accounts
kubectl -n signal patch secret signal-secret -p '{"stringData":{"SIGNAL_NUMBER":"+44..."}}'
```

## Signal history import
Source is a Signal Android plaintext export (`main.jsonl`, backup-v2 JSONL), not
the encrypted `.backup`. Fetch the groups list so group history lands in the live
threads:
```
NUM=$(curl -s localhost:8080/v1/accounts | sed 's/[][\"]//g')   # inside the rest-api pod
curl -s localhost:8080/v1/groups/$NUM > groups.json
```
Then, with the database port-forwarded:
```
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal \
  SELF_UUID=<your ACI> SELF_PHONE=<your E.164 number> \
  ./tools/import_jsonl.py main.jsonl --groups-json=groups.json [--dry-run] [--limit=N]
```
It dedupes on `(sender_uuid, server_ts)`, so it is safe beside the live feed and
to re-run. Attachment bytes are not imported, only metadata.

A group whose name matches no live group keeps its master-key thread; the
importer warns. `tools/reconcile_groups.py groups.json [--apply]` rekeys threads
from an import run without `--groups-json`.

## Google Chat import
From the decoded archive `~/Code/gchat-archive` produces (a browser capture, not
Takeout). Reactions are counts per emoji, not per person; messages carry Google's
threading. Dedupes on `(group_id, msg_id)`, so it is re-runnable.
```
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal \
  ./tools/import_gchat.py [conversations_dir] [--apply]   # dry-run by default
```

## Telegram
One user session pages back through history and holds the update stream; both
write the same rows. A bot cannot read its owner's chats, so this is a user
client, and `telegram_session` is a credential: re-logging in is rate-limited
for hours. Secret chats are device-local and unreachable.

Log in once, interactively; a code arrives on the phone:
```
DB_HOST=… DB_PORT=… DB_USER=… DB_PASSWORD=… DB_NAME=signal \
  TELEGRAM_API_ID=… TELEGRAM_API_HASH=… \
  cargo run --bin telegram -- login '+31…'
```
Not via `kubectl exec`: the Deployment refuses to start without a session, so
there is nothing to exec into. Run it locally with `signal-db` port-forwarded,
or in a throwaway pod with the same environment. `TELEGRAM_API_ID` and
`TELEGRAM_API_HASH` come from <https://my.telegram.org> and live in
`signal-secret`.

`telegram_read_marks` is the one table that cannot be rebuilt: Telegram keeps
only the current read high-water marks, so a mark not recorded when seen is lost.
`observed_at` is when the archive saw it; Telegram's read updates carry no date.
`outbox` is how far the other side has read mine, `inbox` how far I have read
theirs.

`telegram recapture` re-reads every stored message so columns added later get
filled. Run it, like `telegram probe`, with the feed scaled to zero.

Where Telegram differs from the other origins:

| | Telegram | handled in |
| --- | --- | --- |
| a DM names no sender | `from_id` omitted; `out` says which end | `map::map_message` |
| an edit date is not always an edit to show | `edit_hide` beside `edit_date` | `edit_hidden`, honoured by the viewer |
| an edit mutates the message | same `msg_id`, new text | prior text filed in `telegram_message_edits` first; edits made before the archive held the text have none |
| a deletion names no peer | private chats and basic groups share one id sequence | `Db::mark_telegram_deleted` never reaches a channel |
| a supergroup shares the channel id space | told apart by a flag on the peer | `PeerSpace` from the id, `ConvKind` from the peer |
| reactions | counts per emoji, plus a possibly truncated list of who | `telegram_reactions`, `telegram_reaction_authors` |

Photos are downloaded eagerly to `TELEGRAM_MEDIA_DIR`; larger media when a reader
asks (`telegram_media`).

## Security
The signal-cli data PVC holds linked-device keys; keep its backup encrypted. The
database holds private conversations and the Telegram session. Real content stays
out of git.
