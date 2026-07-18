# INC-2026-07-18 VK captcha publication cadence gap

Status: open
Severity: sev1
Service: managed VK event publication / JobOutbox
Opened: 2026-07-18
Closed: —
Owners: events-bot
Related incidents: `INC-2026-07-03-current-import-vector-vk-publication`, `INC-2026-07-17-vk-auto-provider-quota-false-reject`
Related docs: `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

The managed `klgdevents` VK wall stopped its normal publication cadence after
the last scheduled post at 2026-07-17 18:00 UTC.  By 2026-07-18 06:34 UTC the
authenticated postponed queue was empty even though overnight Telegram and VK
imports had created or updated many future events.

At 2026-07-18 00:31:51 UTC an old `vk_sync` job for event `6732`, already on its
26th retry, received VK API code 14 while editing managed post `6648`.  The
captcha handler persisted every due VK task as `paused` with a next run in 2036.
The VK challenge later cleared on the provider side, but the process had no
automatic probe/resume path and the database pause survived indefinitely.

## User / Business Impact

- The public VK event channel had no new publication for roughly 12 hours.
- The postponed queue was empty, so the usual morning cadence could not recover
  without intervention.
- Overnight `tg_monitoring` imported 64 events (`16` created, `48` merged), but
  most corresponding `vk_sync` jobs were paused and the newly created events
  had no managed VK coverage.
- The scheduled `vk_auto_import` successfully updated events `6476`, `6954`,
  and `6956`; this confirmed import/Smart Update was healthy while publication
  remained broken.

## Detection

- Operator noticed that the last VK wall publication was about 12 hours old and
  the postponed queue was empty.
- Evidence sources:
  - `/data/runtime_logs/events-bot.log*` with 48-hour hourly/size rotation;
  - production `ops_run`, `joboutbox`, `event`, and `vk_inbox` rows;
  - authenticated VK `wall.get` using the production user token;
  - Fly release and health state.
- Incident artifacts: `artifacts/codex/INC-2026-07-18-vk-publication-cadence-gap/`.

## Timeline

- 2026-07-17 18:00 UTC — managed post `7683`, the last public wall item in the
  normal event cadence, was published.
- 2026-07-17 21:40 UTC — scheduled Telegram Monitoring run `ops_run=4042`
  started.
- 2026-07-18 00:31:51 UTC — `wall.edit` for event `6732` / post `6648` returned
  code 14; the job had already reached attempt 26 after repeated edit failures.
- 2026-07-18 00:31:52 UTC — the outbox globally paused VK tasks until 2036.
- 2026-07-18 03:42 UTC — Telegram Monitoring completed successfully: 153
  messages processed, 64 events imported, 16 created, 48 merged.
- 2026-07-18 04:15–04:31 UTC — scheduled VK auto-import `ops_run=4072`
  processed three posts without errors and updated events `6476`, `6954`, and
  `6956`; their VK jobs remained paused by the captcha state.
- 2026-07-18 06:34 UTC — authenticated probe showed `postponed.count=0`; the
  same user token could successfully read the wall, proving the provider-side
  challenge had already cleared.

## Root Cause

1. Permanent/long-lived `wall.edit` failures were still retried by JobOutbox;
   event `6732` reached attempt 26 and eventually triggered a captcha.
2. A single code-14 response paused all VK job kinds with a ten-year
   `next_run_at` sentinel.
3. Captcha recovery depended on a live manual callback.  There was no harmless
   provider probe and no persisted, marker-scoped automatic resume after the
   challenge cleared or the process restarted.
4. The pause was not cohort-scoped: the legacy resume callback would wake every
   historical `paused` VK row, including intentional incident containment.

## Contributing Factors

- Import success and VK publication health are reported independently, so green
  `ops_run` status did not reveal the empty managed VK queue.
- New VK jobs continued reaching the cached captcha state, producing repeated
  `captcha` failures instead of remaining quietly pending.
- Production already contains historical/manual paused VK jobs, which makes a
  broad `status='paused' -> pending` repair unsafe.

## Automation Contract

### Treat as regression guard when

- changing VK captcha handling, JobOutbox pause/resume, `vk_sync`, `wall.edit`,
  or managed postponed scheduling;
- changing Telegram/VK import fanout that creates VK publication jobs;
- deploying code that can restart while a VK captcha pause is persisted.

### Affected surfaces

- `main.py::_run_due_jobs_once_locked` and captcha state;
- `vk_captcha_pause_outbox` and persisted JobOutbox rows;
- `main_part2.py::edit_vk_post`;
- managed VK group `-231920894` wall and postponed queue;
- Telegram Monitoring and VK auto-import publication fanout.

### Mandatory checks before closure or deploy

- A captcha pauses one marker-scoped cohort and does not execute newly enqueued
  VK work into the cached challenge.
- After the cooldown, a harmless authenticated read can resume only that cohort
  with bounded spacing; a process restart must not strand it.
- Historical/manual paused rows outside the marker remain untouched.
- Permanent expired-edit errors stop retrying and do not globally pause VK.
- Production catch-up is limited to the affected overnight cohort and produces
  a non-empty postponed queue without duplicates.
- Event `6956` from the prior VK-auto replay receives correct managed VK and
  image-geometry coverage.
- Final deployed SHA is reachable from `origin/main`; `/healthz`, Fly checks,
  SQLite `quick_check`, runtime logs, and authenticated VK API all pass.

### Required evidence

- Targeted pytest output for captcha pause/resume, permanent edit errors, and VK
  publication regressions.
- Fly release/SHA and health output.
- Before/after production JobOutbox rows for the selected catch-up cohort.
- Authenticated before/after `wall.get filter=postponed` results.
- Runtime excerpts showing the original code 14 and successful paced recovery.
- `ops_run=4072`, event `6956`, and geometry row/visual QA evidence.

## Immediate Mitigation

- Read-only triage completed; broad unpause was explicitly avoided.
- Catch-up will target newly created overnight events `6943..6958` rather than
  all 233 historical paused rows.

## Corrective Actions

- Add persisted captcha cohort markers and marker-scoped resume.
- Probe captcha clearance with a harmless `wall.get` after a bounded cooldown.
- Keep new VK work pending while captcha is active.
- Stagger resumed jobs to avoid another API burst.
- Treat expired/deleted edit targets as terminal for the individual job.

## Follow-up Actions

- [ ] Expose paused VK cohort/postponed-queue health in `/healthz` or an operator
  alert instead of relying on visual detection.
- [ ] Audit historical paused VK jobs separately; do not mix that cleanup with
  this incident catch-up.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Pending implementation and production catch-up verification.
