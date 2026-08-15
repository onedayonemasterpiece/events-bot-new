# INC-2026-08-15 integration receipt

## Integrated lanes

- WAL/vacuum: deployed containment PR #506 at `c655156664edcfe91da11a4b9405d4fa59573f20` / Fly v1975; defense-in-depth commit rebased in this branch.
- Telegram + official parser: truthful carrier buckets, terminal force handling, non-colliding/waiting schedules, all-source day guard and collision-safe exact attachment.
- Smart Update + VK: same-call accepted/product/`FAILED_TECHNICAL`; background worker default-off; VK operator batch has no deferred/retry terminal; OCR blank-success, replay key and grounding fixes.
- StaticSiteBuilder: column-allowlisted immutable projection on `/tmp`, VM read deadline, content-addressed adoption, mounted read-in-place, recursive zero-SQLite output gate.

## Integration corrections after review

- Removed substring/default conversion of verifier diagnostics into product no-event; malformed/uncertain LLM results fail technical.
- Updated ticket/festival/manual/legacy VK callers so technical terminals cannot recreate hidden retry.
- Added three inline terminal-ack attempts and fail-closed ledger terminalization after an accepted-write acknowledgement failure.
- Added column-level static data minimization, sensitive-field exclusions, full optional-export parity and recursive nested SQLite rejection.

## Evidence

- Raw positive/opposite VK replay fixture: `tests/replays/INC-2026-08-15-ingestion-retry-stall-and-wal-growth/vk_location_grounding.json`.
- Production online-backup shadow (ignored artifact): `artifacts/codex/INC-2026-08-15-ingestion-linear/prod-shadow/db.sqlite`.
- Shadow `Database.init()` twice: 0.400 s / 0.194 s; `quick_check=ok`; 195 pre-existing FK findings preserved exactly (156 `event_source_fact`, 37 `eventposter`, 2 `videoannounce_item`), no new FK drift. Receipt: `artifacts/codex/INC-2026-08-15-ingestion-linear/prod-shadow/migration-init2.json`.
- Migration rollback injection preserves both old tables/rows and removes both `_new` tables.
- Approved Opus consultation was attempted but unavailable: `a-opus` required interactive OAuth and Claude alias was not logged in; no lower-class substitute was used. Redacted ignored receipt: `artifacts/codex/INC-2026-08-15-vk-smart/CONSULTANT_STATUS.md`.
- Integrated focused regression suite: **630 passed in 90.01 s**; changed Python modules compile; `git diff --check` is clean.

## Production gates still required

Incident remains open until exact-main CI/merge/deploy, captured legacy Smart/VK terminal drain, one controlled TG catch-up, one full all-source parser catch-up with Sobor settlement, WAL bounded through real workload, and one secret-candidate static canary with `/data`/`/tmp`/Kaggle/Yandex receipts. No root promotion is authorized by this integration.
