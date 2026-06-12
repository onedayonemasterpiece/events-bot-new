# INC-2026-06-12 raffle-source publication false skip

Status: open
Severity: sev2
Service: VK auto-import / Smart Update / Telegram+VK event publication
Opened: 2026-06-12
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-10-event-outbox-fanout-deadlock`, `INC-2026-05-05-event-quality-regression`
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`

## Summary

VK auto-import created event `5951` from `https://vk.com/wall-146688375_7432`
(`Путешествие в сказку в деревне Холмогорье`), but the publication scheduler
did not enqueue `vk_sync` or `tg_event_publish`. The source mentioned giveaway
results and tickets, while Smart Update had already extracted a valid fair event
with grounded date, venue, price, posters, Telegraph page, and cleaned event
copy. The raffle fallback guard ran at the scheduling boundary and treated the
event as a third-party ticket giveaway before the cleaned Smart Update output
could reach Telegram/VK publication.

## User / Business Impact

- A valid current event was visible in Telegraph and calendar surfaces but absent
  from both managed public surfaces: `@kldevents` and `vk.com/klgdevents`.
- `afishaengagement` shadow output was also absent because it runs only after a
  normal managed VK event post is created.
- Operator reports were misleading: the Smart Update details showed the original
  VK source link, but no managed VK/TG post existed.

## Detection

- Operator noticed that the import result did not appear in shadow publications,
  then confirmed the event was missing from both VK and Telegram.
- Production runtime file logs on `/data/runtime_logs/events-bot.log` contained
  the decisive line:
  `schedule_event_update_tasks: skip managed VK/TG publication for ticket giveaway event_id=5951; alternative exists`.
- Production DB `joboutbox` for event `5951` contained only `ics_publish`,
  `telegraph_build`, and `tg_ics_post`; no `vk_sync` or `tg_event_publish`.

## Timeline

- 2026-06-12 15:16 UTC — manual `/vk_auto_import 1` started.
- 2026-06-12 15:18 UTC — Smart Update started for
  `https://vk.com/wall-146688375_7432`.
- 2026-06-12 15:19 UTC — event `5951` created with 4 posters.
- 2026-06-12 15:19 UTC — scheduler skipped managed VK/TG publication as ticket
  giveaway fallback.
- 2026-06-12 15:19 UTC — Telegraph, ICS, and calendar Telegram post completed.
- 2026-06-12 — operator reported missing VK/TG/shadow result.

## Root Cause

1. The ticket-giveaway fallback guard was placed at `schedule_event_update_tasks`
   and in the runtime publish jobs, so it could block fanout even after Smart
   Update had produced a valid cleaned event.
2. The guard decided from broad event/source text signals (`розыгрыш` + `билет`)
   instead of first checking whether Smart Update editorial fields already
   contained substantial non-giveaway event copy.
3. The “alternative exists” check was broad: any other non-giveaway queued or
   published event was enough to suppress the current event.

## Contributing Factors

- The original regression test covered prize-only raffle text but not a mixed
  valid event whose source also contained giveaway results.
- Runtime logs were the only source that clearly explained the skip; the bot
  operator report did not show that `vk_sync`/`tg_event_publish` were omitted.

## Automation Contract

### Treat as regression guard when

- changing `schedule_event_update_tasks`, `job_sync_vk_source_post`,
  `job_publish_tg_event_post`, raffle/giveaway publication guards, VK
  auto-import persistence, or Smart Update post-import fanout.

### Affected surfaces

- `main.py` publication scheduling and job-level guards;
- `vk_inbox` / VK auto-import reimport path;
- managed VK event posts in `vk.com/klgdevents`;
- Telegram event posts in `@kldevents`;
- `afishaengagement` shadow path that depends on managed VK post creation;
- runtime log evidence in `/data/runtime_logs`.

### Mandatory checks before closure or deploy

- Regression test: a source containing raffle/ticket text but a cleaned
  Smart Update event body must enqueue both `vk_sync` and `tg_event_publish`
  even when another non-giveaway publication exists.
- Regression test: a prize-only ticket giveaway without cleaned event copy must
  still be skipped when an alternative exists.
- Focused test suite: `tests/test_tg_event_publish.py`.
- Runtime/file-log evidence for event `5951`.
- Production DB verification after mitigation: `wall-146688375_7432` returned to
  pending, reimported, and resulted in managed VK + Telegram publication jobs or
  completed posts.
- Release-governance checks: clean deploy worktree, fix reachable from
  `origin/main`, deployed SHA recorded.

### Required evidence

- test output for focused regression tests;
- deployed SHA and deploy path;
- production log/DB evidence for the `5951` reimport;
- final managed VK URL and Telegram event URL, or queued job evidence if the
  publish slots are intentionally delayed.

## Immediate Mitigation

- Pending: deploy guard fix, then reset the specific VK inbox row
  `group_id=146688375, post_id=7432` from imported back to pending and rerun it.

## Corrective Actions

- Make the raffle fallback guard fail open for valid cleaned Smart Update event
  bodies and keep the fallback only for giveaway/prize-only sources.
- Add regression coverage for a Холмогорье-like event where raw source text
  mentions a ticket raffle but Smart Update editorial fields describe a real
  fair.

## Follow-up Actions

- [ ] Add operator-report visibility when `vk_sync` / `tg_event_publish` is
  intentionally skipped by a policy guard.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /tmp/codex-venvs/events-bot-aeg/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py::test_schedule_event_update_tasks_skips_ticket_giveaway_when_alternative_exists tests/test_tg_event_publish.py::test_schedule_event_update_tasks_publishes_cleaned_event_with_giveaway_source tests/test_tg_event_publish.py::test_schedule_event_update_tasks_allows_ticket_giveaway_without_alternative` -> `3 passed in 1.13s`
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /tmp/codex-venvs/events-bot-aeg/bin/python -m pytest -q -p no:cacheprovider tests/test_tg_event_publish.py` -> `40 passed in 7.00s`
  - `PYTHONDONTWRITEBYTECODE=1 TMPDIR=/tmp /tmp/codex-venvs/events-bot-aeg/bin/python -m pytest -q -p no:cacheprovider tests/test_job_outbox_depends.py tests/test_job_due_filter.py` -> `7 passed in 2.26s`
  - `PYTHONDONTWRITEBYTECODE=1 /tmp/codex-venvs/events-bot-aeg/bin/python -m py_compile main.py tests/test_tg_event_publish.py` -> passed
  - `git diff --check` -> passed
- post-deploy verification:

## Prevention

- Regression coverage must include mixed source posts where giveaway mechanics
  are present in the raw source but Smart Update already produced publishable
  event copy.
