# INC-2026-06-22 Poll Repost Orphan Open Poll

Status: closed
Severity: sev1
Service: Poll to Repost / Telegram `@kenigevents`
Opened: 2026-06-23
Closed: 2026-06-23
Owners: events-bot
Related incidents: `INC-2026-06-15-poll-repost-missing-slots`, `INC-2026-06-13-poll-repost-wrong-date-and-copy`
Related docs: `docs/backlog/features/poll-to-forward/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`, `docs/operations/telegram-link-inspection.md`

## Summary

On 2026-06-22 the production Poll to Repost poll was publicly posted in
`@kenigevents`, but the evening reply and forwarded `@kldevents` recommendation
never appeared. The poll became an orphan: Telegram accepted `send_poll`, then
the follow-up insert into `poll_repost_run` failed with `sqlite3.OperationalError:
database is locked`, so the resolver had no DB row to stop or resolve.

Telethon evidence on 2026-06-23 confirmed `@kenigevents/4124` was still open,
had `22` voters, and no reply/forward existed in the 2026-06-22 result window.

## User / Business Impact

- Subscribers voted in a production poll but did not receive the promised
  evening recommendation.
- The public poll stayed open into the next day, making the mechanic look
  broken and blocking normal operator trust in the daily slot.
- The feature silently lost state even though the public side effect already
  happened.

## Detection

- Detected by operator report on 2026-06-23: “Вчера poll to repost не сработал.
  Был опрос, а ответа на него пересылкой не было.”
- Telethon inspection used the local E2E human session and found the visible
  poll in `@kenigevents`.
- Runtime file mirror was enabled on Fly (`ENABLE_RUNTIME_FILE_LOGGING=1`,
  `/data/runtime_logs/events-bot.log*`, 24 files) and preserved the 2026-06-22
  14:00 UTC failure.
- Production DB `poll_repost_run` had no `prod:2026-06-23` row; the latest prod
  row was `id=62`, `prod:2026-06-21`, `skipped_no_votes`.

## Timeline

- 2026-06-22 14:00:00 UTC — scheduler submitted
  `poll_to_forward_prod_create`, run id `0b5b364fa3e64f4bb3f6255acfc3777b`.
- 2026-06-22 14:00:22 UTC — creation relaxed sparse popularity inventory for
  `prod:2026-06-23`: raw eligible `8`, popularity-qualified `1`, min `5`.
- 2026-06-22 14:00:44 UTC — question reviewer accepted the poll question.
- 2026-06-22 14:00:44 UTC — Telegram poll was posted as `@kenigevents/4124`.
- 2026-06-22 14:00:44 UTC — DB insert in `_insert_run` failed with
  `sqlite3.OperationalError: database is locked` while `source_parsing_day` was
  still running, and the scheduler job ended as `JOB_ERROR`.
- 2026-06-22 17:55 UTC — production resolver had no open `poll_repost_run` row,
  so it could not stop the poll or forward a result.
- 2026-06-23 — operator reported the missing result; Telethon confirmed the
  open orphan poll and absence of reply/forward.

## Root Cause

1. Poll creation performed the irreversible Telegram `send_poll` before the
   durable `poll_repost_run` insert and did not retry transient SQLite writer
   locks. A short lock after the Bot API call created a public poll with no
   durable resolver state.
2. The resolver re-applied strict popularity filtering without the creation-time
   relaxation. If a relaxed poll's winning option contained candidates that were
   not popularity-qualified, the resolver could still drop them and skip the
   recommendation despite a valid audience choice.
3. The scheduler recorded `JOB_ERROR`, but there was no operator-facing alert
   that a public poll had been posted without a DB run row.

## Contributing Factors

- `source_parsing_day` was running concurrently with the 16:00 local poll slot
  and held/contended for SQLite writes long enough to exceed the default busy
  timeout.
- Production has only one create and one resolve slot per day; an orphaned row
  loses the whole daily mechanic unless manually reconciled.
- The state write happened after the Telegram side effect, so ordinary scheduler
  retry/idempotency could not reconstruct the sent poll.

## Automation Contract

### Treat as regression guard when

- changing `poll_to_forward.py` poll creation, DB writes, resolver popularity
  filtering, or production catch-up/manual reconciliation;
- changing SQLite timeout/locking behaviour around scheduled jobs;
- changing scheduler behaviour for `poll_to_forward_prod_create` or
  `poll_to_forward_prod_resolve`.

### Affected surfaces

- `poll_to_forward.py`
- `db.py` SQLite runtime behaviour and concurrent scheduler workload
- `scheduling.py` jobs `poll_to_forward_prod_create` and
  `poll_to_forward_prod_resolve`
- Fly app `events-bot-new-wngqia`
- Telegram channels `@kenigevents` and `@kldevents`

### Mandatory checks before closure or deploy

- Regression test: `_insert_run` retries a transient `sqlite3.OperationalError:
  database is locked` and still persists the public poll row.
- Regression test: Poll to Repost resolve can use the raw eligible event pool
  when creation/resolve popularity coverage is sparse and the winning option's
  candidates were filtered out.
- Full Poll to Repost regression suite:
  `tests/test_poll_to_forward.py` and `tests/test_poll_to_forward_popularity.py`.
- Telethon evidence for `@kenigevents/4124`: poll existence, open/closed state,
  vote count, and whether a reply/forward exists.
- Production DB evidence for `poll_repost_run` state around `prod:2026-06-23`.
- Runtime log evidence for the 2026-06-22 14:00 UTC scheduler failure.
- `/healthz`, Fly status, deployed SHA, and confirmation that the deployed fix is
  reachable from `origin/main`.

### Required evidence

- Logs: `events-bot.log.2026-06-22_13` lines around 14:00:44 UTC show
  `_insert_run` failing with `sqlite3.OperationalError: database is locked` and
  `JOB_ERROR job_id=poll_to_forward_prod_create`.
- Telethon: `@kenigevents/4124`, date `2026-06-22T14:00:44Z`, open poll,
  `total_voters=22`, no reply/forward before `2026-06-22T19:30Z`.
- DB: no `poll_repost_run` row for `prod:2026-06-23` before mitigation.
- Tests/deploy/smoke to be filled before closure.

## Immediate Mitigation

- The orphan poll was identified and preserved for controlled reconciliation.
- After deploy, the orphan poll was stopped and reconciled: the incorrect broad-candidate catch-up messages `@kenigevents/4132` and `@kenigevents/4133` were deleted, then the corrected LLM-first reply `@kenigevents/4134` and forward `@kenigevents/4135` were published for the actual winning option.
- Code mitigation adds bounded SQLite-lock retry around Poll to Repost DB writes
  so a transient writer lock after `send_poll` does not orphan the next public
  poll.
- Resolver mitigation relaxes popularity filtering when the winning option's
  valid candidates were dropped solely by sparse popularity coverage, keeping
  final event choice LLM-first and inside the audience-selected option.

## Corrective Actions

- Add Poll to Repost DB write retry for `_insert_run` and `_update_run`, default
  retry window `POLL_TO_FORWARD_DB_LOCK_RETRY_SEC=180` seconds.
- Keep creation/reply/final-event semantics LLM-first: retry and popularity
  relaxation only preserve durable state/candidate availability; they do not
  generate deterministic topics or deterministic final recommendations.
- Update Poll to Repost docs and regression tests.

## Follow-up Actions

- [ ] Add an operator alert when `poll_to_forward_prod_create` reaches a public
  Telegram side effect but cannot persist the run row.
- [ ] Consider moving the durable run reservation before `send_poll` with a
  `creating` state, so even long DB outages fail closed before public posting.

## Release And Closure Evidence

- deployed SHA: `a96ca61530c1b0d5b26ed21aba1d3abfed856377`, reachable from `origin/main` and `origin/agent/T-000036`.
- deploy path: manual `flyctl deploy --remote-only --app events-bot-new-wngqia`, image `registry.fly.io/events-bot-new-wngqia:deployment-01KVT3TS1TGZ1YPG3NHCJQSEVS`, machine `683961db016e28`, version `1471`, `1 total, 1 passing` check.
- regression checks: `python -m py_compile poll_to_forward.py scheduling.py`; `python -m pytest tests/test_poll_to_forward.py tests/test_poll_to_forward_popularity.py -q` (`53 passed`).
- post-deploy verification: Fly status showed image `deployment-01KVT3TS1TGZ1YPG3NHCJQSEVS`; production DB `pragma quick_check` returned `ok`; Telethon showed `@kenigevents/4124` closed with vote counts `[1,3,1,7,5,4,1]`, wrong catch-up messages `4132`/`4133` deleted, corrected reply `4134` and forward `4135` visible; DB row `poll_repost_run.id=63` is `forwarded`, `chosen_event_id=6244`, `kldevents_message_id=1040`, `reply_message_id=4134`, `forwarded_message_id=4135`.

## Prevention

The prevention guard is mechanical/idempotency-focused: Poll to Repost DB writes
now wait through transient SQLite writer contention, and resolver candidate
loading mirrors creation-time popularity relaxation so sparse metric coverage
cannot erase a valid voted option. Semantic decisions remain with the LLM topic
planner and LLM winner/reply composer.
