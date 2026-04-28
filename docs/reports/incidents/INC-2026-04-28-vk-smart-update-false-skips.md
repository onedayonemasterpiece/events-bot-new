# INC-2026-04-28 VK Smart Update False Skips

Status: closed
Severity: sev2
Service: VK auto-import / Smart Update
Opened: 2026-04-28
Closed: 2026-04-28
Owners: events bot maintainers
Related incidents: —
Related docs: `docs/features/vk-auto-queue/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/festivals/README.md`, `docs/llm/prompts.md`, `docs/operations/release-governance.md`, `docs/operations/runtime-logs.md`

## Summary

On April 28, 2026 the scheduled VK auto-import run rejected two normal offline events:

- `https://vk.com/wall-29891284_13503` — a May 2 masterclass, rejected as `skipped_festival_post`.
- `https://vk.com/wall-168966993_22654` — a May 2 bicycle ride in Gusev, rejected as `skipped_non_event:online_event`.

Both posts had concrete future event anchors and should have created or updated event cards.

## User / Business Impact

- Two valid VK events were absent from the bot inventory.
- Downstream public surfaces that depend on imported events (`/daily`, month/weekend pages, video/event recommendations) could miss these events until a catch-up import is performed.
- Operator-facing VK auto-import reported a green run with `inbox_rejected=2`, so the missed imports were visible only in the per-post Smart Update status lines.

## Detection

- Detected by operator report in Telegram at `2026-04-28 10:14` and `10:15` Europe/Kaliningrad.
- Production `ops_run id=931` confirmed `status=success` with errors:
  - `persist_skipped ... wall-29891284_13503: smart_update returned no event_id: status=skipped_festival_post reason=festival_post`
  - `persist_skipped ... wall-168966993_22654: smart_update returned no event_id: status=skipped_non_event reason=online_event`
- Runtime file mirror was checked on Fly: `ENABLE_RUNTIME_FILE_LOGGING=0`, `RUNTIME_LOG_DIR=/data/runtime_logs`, directory existed but had no active/rotated log files. Fallback evidence came from Fly status, VK API, and production SQLite `ops_run`/`vk_inbox` rows.

## Timeline

- `2026-04-28 07:07:00 UTC` — VK post `wall-29891284_13503` was published.
- `2026-04-28 07:15:51 UTC` — VK post `wall-168966993_22654` was published.
- `2026-04-28 08:15:00 UTC` — scheduled `vk_auto_import` run `ops_run id=931` started.
- `2026-04-28 08:17:00 UTC` — run finished `success`, but processed both posts as rejected/skipped; `events_created=0`, `events_updated=0`.
- `2026-04-28 10:14/10:15 Europe/Kaliningrad` — operator saw the two skip messages and opened this incident.
- `2026-04-28` — root cause localized to festival-context over-classification and broad online-only guard.
- `2026-04-28` — targeted regression tests passed: `pytest -q tests/test_smart_event_update_non_event_guards.py tests/test_festival_context.py tests/test_vk_auto_queue_import.py` (`31 passed`).
- `2026-04-28 09:51 UTC` — hotfix SHA `30b0575a` deployed to Fly as image `deployment-01KQ9QYK28K0SC8SNC20FYH80E`; machine version `1023`, health check passing.
- `2026-04-28 09:52-10:23 UTC` — compensating `vk_auto_import` run `ops_run id=933` imported `wall-29891284_13503` into event `3911`; `wall-168966993_22654` hit the row timeout guard.
- `2026-04-28 10:50-10:51 UTC` — direct incident catch-up `ops_run id=935` imported `wall-168966993_22654` into event `4314`.
- `2026-04-28` — production `vk_inbox` rows `6286` and `6287` verified as `imported`; `/healthz` returned `ok=true`, `ready=true`, no issues.

## Root Cause

1. `_ONLINE_EVENT_RE` matched any standalone `онлайн`, including `онлайн-регистрация`, so an offline bicycle ride with a physical route and start location was treated as online-only.
2. `detect_festival_context()` could keep or create `festival_post` for a single strong event draft when the source/festival payload implied a festival/program context. A bullet list of materials/conditions (`list_lines >= 3`) also counted as `multi_signal`, so a single masterclass could be mistaken for a whole festival program and routed to Festival Queue instead of creating the event.
3. The event-parse prompt did not explicitly require `event_with_festival` for a concrete single event inside a cycle/program and did not clarify that online registration is not an online-only event.

## Contributing Factors

- VK auto-import records skipped Smart Update results as rejected rows, so a scheduled run can be `success` while valid events are missing.
- Existing regression tests covered several non-event guards but not `онлайн-регистрация` vs online-only events.
- Festival detection had a matrix for whole-festival posts, but no targeted guard for a single concrete event inside a festival-like source.

## Automation Contract

### Treat as regression guard when

- Changing `smart_event_update.py` non-event guards, especially `online_event`.
- Changing `festival_queue.detect_festival_context()` or festival queue routing.
- Changing `docs/llm/prompts.md` / event-parse extraction rules for VK/TG.
- Changing `vk_auto_queue` handling of `persist_skipped` / rejected rows.

### Affected surfaces

- `smart_event_update.py`
- `festival_queue.py`
- `docs/llm/prompts.md`
- `vk_auto_queue.py` scheduled/manual VK auto-import
- Production SQLite `vk_inbox` / `ops_run`
- Festival Queue routing

### Mandatory checks before closure or deploy

- Unit tests:
  - `tests/test_smart_event_update_non_event_guards.py`
  - `tests/test_festival_context.py`
- Targeted local replay/contract check for:
  - `wall-29891284_13503` must not resolve to `festival_post`.
  - `wall-168966993_22654` must not match `online_event` solely because of `онлайн-регистрация`.
- Production evidence:
  - Fly runtime health remains passing before and after deploy.
  - Runtime log mirror state checked; fallback evidence captured if disabled.
  - Production `vk_inbox` rows for both posts no longer remain the only terminal evidence after catch-up; imported event IDs or an explicit manual blocker must be recorded.
- Release governance:
  - fix branch starts from `origin/main`;
  - deployed SHA is reachable from `origin/main` before incident closure.

### Required evidence

- Test command output and passing status.
- Git SHA(s) for fix and deploy.
- `ops_run` / `vk_inbox` / event IDs proving catch-up import for both posts, or documented Telegram/VK/API blocker.
- `/healthz` / Fly status evidence after production deploy/catch-up.

## Immediate Mitigation

- Isolated the hotfix in a linked worktree from `origin/main` because the default checkout had unrelated dirty lollipop/Telegram changes.
- Narrowed the online-only guard so online registration/sign-up wording does not classify an offline event as online-only.
- Added festival-context rescue for one strong event draft inside a cycle/program/festival context, including the case where `multi_signal` comes only from bullet-listed materials/conditions rather than multiple dates/times.

## Corrective Actions

- Add regression tests for online-registration and festival single-event routing.
- Tighten the event-parse prompt to require `event_with_festival` for concrete single events inside a festival/cycle and to distinguish online registration from online-only formats.
- Document the regression signal in VK auto-import and Smart Update/Festival docs.

## Follow-up Actions

- [x] Complete production deploy and make the deployed SHA reachable from `origin/main`.
- [x] Perform compensating catch-up for `wall-29891284_13503` and `wall-168966993_22654`.
- [ ] Consider surfacing `persist_skipped` with `events_created=0` as a stronger operator warning when the skipped reason is a known false-skip regression class.
- [ ] Investigate the slow full-pipeline handling of `wall-168966993_22654`: after the false-skip fix, full VK auto-import no longer rejected it as `online_event`, but the OCR/draft path timed out at 1800 seconds before the direct Smart Update catch-up restored the event.

## Release And Closure Evidence

- deployed SHA: `30b0575a` (`fix(vk): rescue single-event festival bullets`), reachable from `origin/main`
- deploy path: `flyctl deploy --app events-bot-new-wngqia`
- deployed image: `events-bot-new-wngqia:deployment-01KQ9QYK28K0SC8SNC20FYH80E`
- Fly status: machine `48e42d5b714228`, version `1023`, state `started`, `1 total, 1 passing`
- regression checks:
  - `pytest -q tests/test_smart_event_update_non_event_guards.py tests/test_festival_context.py` (`4 passed`)
  - local contract check: `wall-29891284_13503` fixture resolves to `event_with_festival`; `wall-168966993_22654` fixture resolves `online_event=False`
  - `pytest -q tests/test_vk_auto_queue_import.py` (`27 passed`)
  - combined targeted run: `pytest -q tests/test_smart_event_update_non_event_guards.py tests/test_festival_context.py tests/test_vk_auto_queue_import.py` (`31 passed`)
  - broader `pytest -q tests/test_vk_auto_queue_import.py tests/test_vkrev_import_flow.py` exposed pre-existing `origin/main` failures in `tests/test_vkrev_import_flow.py` fixtures (missing `location_name` / missing local `GOOGLE_API_KEY`), while `tests/test_vk_auto_queue_import.py` passed; not used as blocking evidence for this hotfix.
- post-deploy verification:
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler/tasks `ok`, `issues=[]`
  - original bad run: `ops_run id=931`, `processed=2`, `rejected=2`, errors were `festival_post` and `online_event`
  - catch-up run: `ops_run id=933`, `processed=2`, `imported=1`, updated event `3911` for `https://vk.com/wall-29891284_13503`
  - direct catch-up: `ops_run id=935`, `status=success`, `inbox_processed=1`, `inbox_imported=1`, created event `4314` for `https://vk.com/wall-168966993_22654`
  - `vk_inbox id=6286`: `status=imported`, `imported_event_id=3911`
  - `vk_inbox id=6287`: `status=imported`, `imported_event_id=4314`
  - `event_source`: both original VK URLs linked to imported event IDs (`3911`, `4314`)

## Prevention

- Regression tests now pin both false-skip classes.
- Canonical prompt/docs now specify the intended distinction between whole-festival posts and single events inside a festival/cycle, and between online-only events and offline events with online registration.
