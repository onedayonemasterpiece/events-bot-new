# WAL lane results — 2026-08-15

## Production evidence (read-only)

- At 08:27 UTC `/data` had 1,115,029,504 free bytes; DB was 679,313,408 bytes and WAL 4,190,072 bytes. Earlier retained WAL had already been reclaimed by the next successful hourly checkpoint.
- The 06:42 UTC scheduler collision submitted `event_vector_sync`, `db_wal_checkpoint`, and `db_vacuum` together. The checkpoint timed out after 30.096 s, full `VACUUM` finished after 41.120 s, and the 07:42 checkpoint succeeded with `(0,0,0)`. `VACUUM` rewrote a DB-sized WAL while a concurrent snapshot/read prevented reset.
- Current PRAGMAs before this fix: `journal_mode=wal`, `wal_autocheckpoint=1000`, `journal_size_limit=-1`. No active WAL reader lock was present in the sampled `/proc/locks`; there was no stale claim to kill.
- Local `/dev/shm` reproduction with a 57,794,560-byte DB, pinned reader, and `VACUUM` produced a 58,133,232-byte WAL; concurrent truncate returned `(1,14110,0)`, then `(0,0,0)` after reader close.

## Delivered changes

1. `bc32d312963db0b9f388ff2f415c6e44c34f6efc` independently default-disables periodic full `VACUUM`; only `ENABLE_DB_FULL_VACUUM=1` registers it.
2. Follow-up commit adds:
   - `db_vacuum` to the shared heavy-job policy;
   - preflight/recheck capacity rule `free >= 2 * DB + floor` (default floor 512 MiB);
   - mandatory pre/post truncating checkpoints with structured capacity/size/frame/duration receipts;
   - `DB_WAL_JOURNAL_SIZE_LIMIT_MB=64`, clamped to 4..256 MiB, on init/raw/ORM connections;
   - config examples, canonical cron/runtime docs, and focused regression tests.

## Validation

```text
/home/dev/.codex/venvs/events-bot-new/bin/pytest -q \
  tests/test_db.py tests/test_db_maintenance.py tests/test_scheduling.py
69 passed in 11.76s

/home/dev/.codex/venvs/events-bot-new/bin/python -m compileall -q \
  db.py scheduling.py tests/test_db_maintenance.py
# success

git diff --check
# clean
```

## Residual risk / operational notes

- `journal_size_limit` bounds retained WAL only after SQLite can reset it; it does not cap an active large transaction and does not bypass pinned readers.
- Full `VACUUM` remains unnecessary for ordinary WAL control and should stay disabled in production. The opt-in guard is defense-in-depth for an explicitly approved future run.
- Do not delete live WAL/SHM files. Use the structured checkpoint receipt to identify a busy reader/writer, then allow a supported truncating checkpoint.
- The 120-second scheduler timeout remains; an unusually large future explicitly enabled `VACUUM` may exceed it. Keep the operation disabled unless an operator has reviewed capacity and maintenance timing.
