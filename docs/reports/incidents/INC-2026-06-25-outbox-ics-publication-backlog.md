# INC-2026-06-25 Outbox ICS publication backlog blocks Telegram event posts

Status: open
Severity: sev2
Service: event publication pipeline (`joboutbox`, ICS Supabase Storage, Telegram/VK/Telegraph surfaces)
Opened: 2026-06-25
Closed: —
Owners: Codex / events-bot operations
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-05-19-vk-posts-personal-author`
Related docs: `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`, `docs/features/tg-publishing/README.md`, `docs/features/vk-publishing/README.md`, `docs/features/promo-campaigns/README.md`

## Summary

During the 2026-06-25 production audit, recent events were found with successful Telegraph and VK jobs but no Telegram event publication. The affected rows had `ics_publish` jobs failing repeatedly with `last_error="'message'"`; dependent `tg_ics_post` jobs stayed pending, and dependent `tg_event_publish` jobs were deferred to `2036-06-22 09:44:13` rather than entering the normal Telegram publication queue.

## User / Business Impact

- Users of Telegram event channels could miss newly imported future events even though the same events had been added to the database and, in most cases, published to VK/Telegraph.
- Calendar attachment posts (`tg_ics_post`) were not created for time-bearing events whose Supabase ICS upload failed.
- Operators could see a green health endpoint while publication backlog accumulated behind future-dated outbox dependencies.

## Detection

- Detected by manual incident audit requested on 2026-06-25.
- `/healthz` was healthy, so the issue required DB/log inspection rather than serving health checks.
- Runtime file mirror was available under `/data/runtime_logs` and showed Supabase Storage upload errors bubbling up as `KeyError: 'message'`.

## Timeline

- 2026-06-25 20:33 UTC — production audit captured 39 events added in the previous 24h, healthy `/healthz`, and recent outbox errors.
- 2026-06-25 20:35 UTC — candidate rows showed repeated `ics_publish:error:'message'`, `tg_ics_post:pending`, and `tg_event_publish:pending` with a 2036 retry timestamp.
- 2026-06-25 20:36 UTC — runtime log context showed Supabase Storage upload attempting a `/rest/v1/object/...` URL and raising a `KeyError('message')` while handling a 404 response.
- 2026-06-25 20:39 UTC — publication candidates for repair were enumerated in `artifacts/codex/incident-audit-20260625/requeue_candidates.json`.
- 2026-06-25 20:50 UTC — this canonical incident record was created so the issue can be treated as a regression contract.

## Root Cause

1. `ics_publish` uses the Supabase Python storage client to upload ICS files.
2. In production the failing upload path reached `https://...supabase.co/rest/v1/object/...` instead of the Storage API path `https://...supabase.co/storage/v1/object/...`.
3. The storage client then tried to read `resp["message"]` from the 404 response and raised `KeyError('message')`, which was stored in `joboutbox.last_error`.
4. Because `tg_ics_post` depends on `ics_publish`, and `tg_event_publish` depends on `tg_ics_post`, Telegram event publication remained blocked for affected rows.

## Contributing Factors

- The outbox exposes no compact health signal for “future-deferred publication dependency is blocked by repeated ICS failures”.
- Some candidate rows are event-quality-sensitive; repair must respect the LLM-first policy and must not force questionable event semantics through deterministic shortcuts.
- qTickets parser rows currently build Telegraph/ICS jobs but do not necessarily enqueue the same public event-post fanout as VK/Telegram source imports; this must be audited separately before changing behavior.

## Automation Contract

### Treat as regression guard when

- changing `get_supabase_client`, Supabase Storage upload helpers, `ics_publish`, `tg_ics_post`, `tg_event_publish`, `enqueue_event_pipeline_jobs`, `joboutbox` dependency handling, or publication catch-up tooling;
- changing parser/import paths that decide whether a new event should enqueue Telegram/VK public publication jobs;
- repairing or replaying daily/scheduled publication jobs that include ICS/calendar links.

### Affected surfaces

- `main.py::get_supabase_client`
- `main.py::ics_publish`
- `main.py::tg_ics_post`
- `main.py::enqueue_event_pipeline_jobs`
- `main.py::_run_due_jobs_once_locked`
- Supabase Storage bucket `events-ics`
- Telegram event publication queue and VK/Telegraph publication surfaces
- production runtime logs `/data/runtime_logs`

### Mandatory checks before closure or deploy

- Verify runtime log mirror and collect the relevant Supabase/outbox log lines.
- Verify Supabase Storage upload uses `/storage/v1/object/<bucket>/<path>`, not `/rest/v1/object/...`.
- Run or replay `ics_publish` for at least one affected time-bearing event and confirm `event.ics_url` is populated.
- Confirm dependent `tg_ics_post` is unblocked and produces `event.ics_post_url` or a documented intentional skip.
- Confirm affected eligible future events either have Telegram, Telegraph, and VK publication links or a source-grounded/LLM-reviewed reason for not publishing.
- Confirm community VK posts still pass `INC-2026-05-19-vk-posts-personal-author` (`from_id=-group`, `from_group=1`, unsigned).
- Do not close after deploy alone: if same-day scheduled publication was missed, perform catch-up/requeue and verify post-repair state.

### Required evidence

- artifact path with pre/post DB rows and log excerpts;
- exact production SHA/image checked;
- list of affected event IDs and which surfaces were restored or intentionally left blocked;
- public Telegram/VK links for repaired publications when they become available;
- confirmation that any durable code fix is reachable from `origin/main` before closure.

## Immediate Mitigation

- As of creation, candidate rows were enumerated and repair was limited to event IDs with clear future dates and already-completed VK/Telegraph surfaces. Rows with questionable semantics/media were left for LLM-first review rather than deterministic forcing.

## Corrective Actions

- Pending: make `ics_publish` use a Storage upload path that cannot be confused with the PostgREST base URL, or isolate Supabase clients so Storage and PostgREST sessions do not share a mutable base URL.
- Pending: add an outbox diagnostic/alert for repeated `ics_publish` failure blocking `tg_event_publish`.

## Follow-up Actions

- [ ] Patch and deploy durable Supabase Storage upload fix from a clean `origin/main`-based worktree.
- [ ] Add regression test for Supabase Storage upload URL/options and `KeyError('message')` handling.
- [ ] Audit qTickets parser fanout contract: decide whether qTickets-created events should enqueue public Telegram/VK event posts or only Telegraph/ICS tasks.
- [ ] Add an operator report for events where `tg_event_publish.next_run_at` is years in the future because a dependency failed.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

- Keep this incident as a regression contract for Supabase Storage, ICS publication, and outbox dependency changes.
- Prefer LLM-first event-quality review for semantic repair decisions; deterministic repair scripts may only unblock known publication mechanics for already-accepted event rows.

## 2026-06-25 Mitigation Evidence

- Created production backup tables before mutation:
  - `codex_backup_20260625_outbox_ics_publication_backlog_event_205118`
  - `codex_backup_20260625_outbox_ics_publication_backlog_joboutbox_205118`
  - `codex_backup_20260625_outbox_ics_publication_backlog_notime_event_205406`
  - `codex_backup_20260625_outbox_ics_publication_backlog_notime_joboutbox_205406`
- Uploaded 36 affected ICS files through the Supabase Storage API path `/storage/v1/object/events-ics/...`; all 36 `ics_publish` rows were marked `done` with public `ics_url` values.
- Rebuilt/updated the affected Telegraph pages with the restored ICS links.
- Production worker sent all 36 Telegram calendar asset posts (`tg_ics_post=done`, e.g. `https://t.me/kenigeventscalendar/7013` through `7048`).
- Requeued stuck eligible Telegram event posts:
  - event `6377` immediately published to Telegram: `https://t.me/c/3954607218/1297`;
  - future rows previously parked in 2036 were re-armed and then spacing-deferred by the normal Telegram publish-window guard where applicable;
  - no-time future rows `6378` and `6394` were requeued from the 2036 sentinel with separate backups.
- Post-repair audit at `2026-06-25 20:54 UTC`: no remaining last-24h `joboutbox.last_error="'message'"`, no due pending/running jobs, `/healthz` OK.
- Intentionally left blocked for LLM-first/manual review rather than deterministic forcing:
  - `6375` (`Приезд Хранителей`) has no media and a questionable/prose location, with VK blocked by `vk_sync_missing_media_for_telegram_event`;
  - same-day/past Telegram announcements `6379` and `6383` were not late-posted after their 2026-06-25 19:00 start time;
  - `6382` was already a past/expired 2026-06-24 event.
- Artifacts: `artifacts/codex/incident-audit-20260625/repair_ics_backlog_apply.json`, `post_repair_state_raw.txt`, `final_audit2_raw.txt`, `requeue_no_time_tg_raw.txt`.

### Telegram catch-up window finding

Additional finding from the 2026-06-25 follow-up audit: the Telegram event channel was not merely waiting on normal 10-minute spacing. The ICS outage kept many `tg_event_publish` jobs blocked until the repair completed at about `2026-06-25 20:51 UTC` (`22:51` Europe/Kaliningrad). Production `tg_event_publish` is limited by `TG_EVENT_PUBLISH_START_HOUR=7` / default end hour `23` in local time. After event `6377` published at `20:51 UTC`, the next 10-minute slot would have been after `23:00` local, so `_normalize_tg_event_publish_run_at` moved the remaining catch-up jobs to `2026-06-26 05:00 UTC` (`07:00` local). This means the user-visible “Telegram was quiet and postponed everything to tomorrow” symptom is a combined effect of the ICS dependency outage plus a hard publish-window boundary.

Follow-up: add an operator-controlled incident catch-up mode or policy decision for whether urgent same-day backlogs may temporarily extend the Telegram event publish window / shorten spacing, instead of silently rolling a large accepted-event backlog to the next morning.
