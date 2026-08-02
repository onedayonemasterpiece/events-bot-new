# INC-2026-07-03-current-import-vector-vk-publication Current VK import audit: Gemma timeout, vector-gated creates, and VK postponed idempotency drift

Status: open
Severity: sev2
Service: VK auto-import / Smart Update / VK managed publication
Opened: 2026-07-03
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-04-20-club-znakomstv-duplicate-event-cards`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/features/smart-event-update/README.md`

## Summary

During the production import audit on 2026-07-03 04:44–05:14 UTC, the current `vk_auto_import` batch `auto:1783052100` / run `49faaa601f6a4d84b88a5abafa7e1423` finished as `success`, but evidence showed production-impacting defects:

1. VK inbox `9611` (`https://vk.com/wall-95066843_550`) failed draft building with `RuntimeError: event_parse Gemma wall-clock timeout after 240s`, leaving a likely event-like source unimported.
2. Smart Update vector identity gate was available and ran for create decisions, but it allowed creates for the new VK events because nearest vector matches were unrelated/low-confidence.
3. Existing-event updates created or kept duplicate managed postponed VK posts for events `6615` and `6625`, and fresh event `6636` was not present in the managed postponed queue because `vk_sync` failed with VK `214 Access to adding post denied: a post is already scheduled for this time`.

This record is intentionally opened from a read-only audit; no repair was performed in the audit pass.

## User / Business Impact

- One event-like VK source from the current import batch may be missing from the public event inventory.
- Users may see duplicate managed VK scheduled posts for the same canonical event (`6615`, `6625`) if stale postponed entries are not cleaned up before publish time.
- Event `6636` has Telegram/ICS/Telegraph artifacts but its managed VK publication is currently in error state, so VK coverage is incomplete.
- The affected import batch was marked `success`, so the defects are easy to miss without row-level log/DB checks.

## Detection

- Detected by a manual production audit requested by the operator.
- Evidence sources:
  - Fly runtime file mirror `/data/runtime_logs/events-bot.log`;
  - Fly logs live stream started at `2026-07-03T04:44:07Z`;
  - production SQLite `/data/db.sqlite` read-only probes;
  - authenticated VK API `wall.getById` / `wall.get filter=postponed`.
- Artifact directory: `artifacts/codex/current-import-audit-2026-07-03/`.

## Timeline

- 2026-07-03 04:15:00 UTC — `vk_auto_import` scheduled run started, ops_run `3189`.
- 2026-07-03 04:24:39 UTC — `vk_auto_queue` failed `build_event_drafts` for `https://vk.com/wall-95066843_550`.
- 2026-07-03 04:24:40 UTC — traceback ended in `RuntimeError: event_parse Gemma wall-clock timeout after 240s`.
- 2026-07-03 04:28:38 UTC — Smart Update `identity_gate` allowed create for `https://vk.com/wall-138053522_2648`; vector was available, nearest event `4759`, score about `0.792`, no veto.
- 2026-07-03 04:28:50 UTC — event `6637` created.
- 2026-07-03 04:33:47 UTC — source `https://vk.com/wall-152679358_26381` attached to existing event `6615`.
- 2026-07-03 04:37:33 UTC — `vk_auto_import` completed as `success`: processed `9`, imported `3`, rejected `5`, failed `1`, created `2`, updated `2`.
- 2026-07-03 04:39:41 UTC — `joboutbox` `27112` for event `6636` remained `vk_sync error` after VK `214 Access to adding post denied`.
- 2026-07-03 04:44:07 UTC — live Fly log monitoring started.

## Root Cause

Open pending repair investigation:

1. The Gemma event-parse call for `wall-95066843_550` exceeded the 240s wall-clock limit and the row was left `failed` rather than being retried/degraded with a smaller prompt or recovery path.
2. Managed VK postponed publication did not reliably edit/replace the existing managed post for updated canonical events; the queue contained older and newer postponed posts for the same event (`6615`: `5578` and `5643`; `6625`: `5628` and `5640`).
3. VK wall post scheduling collision handling for `6636` treated `214 already scheduled for this time` as a terminal job error without selecting a free slot or reconciling with an existing equivalent postponed post.

## Contributing Factors

- `ops_run.status=success` hides the per-row `inbox_failed=1` and publication-level errors unless details are inspected.
- VK publication conflicts are visible in `joboutbox`, but not summarized as import-run failure.
- The current local checkout is behind production-side incident docs/code history, so runtime evidence must be treated as source of truth for this incident.

## Automation Contract

### Treat as regression guard when

- changing `vk_auto_import`, `vk_auto_queue`, Smart Update identity/vector gate, `vk_sync`, postponed VK scheduling, or managed VK publication idempotency;
- auditing imports where `ops_run.metrics_json` has `inbox_failed > 0`, `events_updated > 0`, or same source/event updates;
- changing Gemma parse timeout/retry handling for VK sources.

### Affected surfaces

- `vk_auto_import` / `vk_auto_queue` row processing and status accounting;
- Smart Update create/merge identity decisions;
- `event_source` attachment for updated events;
- `joboutbox` `vk_sync`;
- VK API `wall.post` postponed queue for owner `-231920894`;
- operator reports for import batch success/partial status.

### Mandatory checks before closure or deploy

- Replay or reprocess `https://vk.com/wall-95066843_550` through the production VK import boundary and Smart Update; verify it is either imported correctly or intentionally rejected with durable reason.
- Verify no duplicate managed postponed VK posts remain for events `6615` and `6625`.
- Verify event `6636` has either a correct managed VK postponed/live post or a documented product decision not to publish it.
- Verify Smart Update vector identity logs remain present for create-path candidates and that vector veto/allow decisions are reflected in `event_identity_decision_log`.
- Run duplicate/false-merge checks for same source URL, same title/date/time/location, and updated existing events.
- Confirm the final fix SHA is reachable from `origin/main` before closure if code changes are needed.

### Required evidence

- `ops_run` row for the repaired/import run.
- Runtime log excerpts for `wall-95066843_550`, `wall-48383763_41188`, `wall-152679358_26381`, and `wall-138053522_2648`.
- Production DB rows for events `6615`, `6625`, `6636`, `6637`, their `event_source`, and `joboutbox`.
- Authenticated VK API evidence for the managed postponed queue before/after cleanup.
- Replay artifacts for the failed source if a code/prompt fix is made.

## Immediate Mitigation

- None performed in the read-only audit pass.

## Corrective Actions

- Open.

## Follow-up Actions

- [x] Investigate and repair/retry failed VK inbox `9611` (`wall-95066843_550`).
- [x] Clean up or reconcile duplicate managed postponed posts for events `6615` and `6625`.
- [x] Repair `vk_sync` for event `6636` and verify managed VK coverage.
- [ ] Decide whether import run status should become `partial` when `inbox_failed > 0` or when immediate publication jobs fail for newly imported/updated rows.
- [ ] Add/adjust tests or replay fixtures for VK parse timeout recovery and postponed idempotency.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Pending repair investigation.

## Mitigation / Repair Log

### 2026-07-03 06:17–06:35 UTC

Performed production data/publication repair with row-level backups under production SQLite tables named `codex_backup_20260703_*` and artifacts in `artifacts/codex/current-import-audit-2026-07-03/`.

- Deleted stale managed VK postponed duplicates:
  - `wall-231920894_5578` for event `6615`.
  - `wall-231920894_5628` for event `6625`.
  - `wall-231920894_5640` became stale after re-syncing event `6625` and was deleted.
- Repaired zoo event fields from source text:
  - `6625`: ticket price corrected to `800..1100`, `ticket_status=available`.
  - `6636`: `event_type=экскурсия`, ticket price `800..1100`, `ticket_status=available`.
- Rebuilt Telegraph pages for `6625` and `6636`.
- Repaired managed VK coverage:
  - `6625` now points to managed VK post `https://vk.com/wall-231920894_5657`.
  - `6636` now points to managed VK post `https://vk.com/wall-231920894_5659`.
- Reprocessed failed VK inbox `9611` / `https://vk.com/wall-95066843_550` using `gemini-3.1-flash-lite` for the event-parse stage after the original Gemma timeout:
  - created `6638` — `Концерт в Арт-резиденции Суходолье`, 2026-07-04 18:00;
  - created `6639` — `Концерт в Арт-резиденции Суходолье`, 2026-07-05 18:00;
  - created `6640` — `Экспедиция в Знаменск`, 2026-07-04 12:00;
  - inbox `9611` marked `imported`, `review_batch=repair:INC-2026-07-03-current-import-vector-vk-publication`.
- Repaired managed VK coverage for recovered events:
  - `6638` -> `https://vk.com/wall-231920894_5660`;
  - `6639` -> `https://vk.com/wall-231920894_5663`;
  - `6640` -> `https://vk.com/wall-231920894_5662`.

Final verification at 2026-07-03 06:34 UTC:

- `vk_sync` jobs for `6625`, `6636`, `6638`, `6639`, `6640` are `done`.
- Authenticated VK postponed queue contains the intended managed posts and no stale `5578` / `5628` / `5640` duplicates.
- Duplicate heuristic for recent active events returned no same-title/date/time duplicates.
- Same-source multi-event rows remain expected splits:
  - `wall-48383763_41188` -> `6625`, `6636`;
  - `wall-95066843_550` -> `6638`, `6639`, `6640`.

Remaining corrective work: fix the root code paths so VK `214` exact `publish_date` collisions are avoided automatically, recovered multi-event posts do not leave `vk_sync` stuck/running, and `vk_auto_import` reports `partial`/action-needed when per-row failures occur.
