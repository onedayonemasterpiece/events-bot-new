# INC-2026-06-28 VK stale event publication after start

Status: mitigated
Severity: sev2
Service: managed `klgdevents` VK wall / Telegram event fanout / VK postponed queue
Opened: 2026-06-28
Closed: —
Owners: events-bot maintainer / Codex
Related incidents: `INC-2026-06-24-vk-past-actuals`, `INC-2026-06-27-vk-prune-starvation`
Related docs: `docs/features/vk-publishing/README.md`, `docs/operations/incident-management.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-28 the managed VK community `klgdevents` showed a fresh post for `День молодёжи 2026` even though the event started on 2026-06-27 at 12:00 Europe/Kaliningrad. Authenticated VK API confirmed live `wall-231920894_4839` posted at 2026-06-28 11:02 KGD with `📅 27 июня 12:00`. The postponed queue also contained stale managed posts for the same event (`wall-231920894_4802`, due 2026-06-28 23:02 KGD), `«Я не понимаю чего хочу»` (`wall-231920894_4651`, due 2026-06-28 19:10 KGD), and `D&D Ваншот: Гребень русалки` (`wall-231920894_4655`, due 2026-06-29 19:10 KGD).

## User / Business Impact

- VK readers saw yesterday's event as a new/current publication.
- Additional stale postponed posts were queued to publish after their event start.
- Telegram event fanout could also publish stale one-day timed events when a guessed future `end_date` was present.

## Detection

- Detected by operator report on 2026-06-28.
- Confirmed by authenticated VK API (`wall.getById`, `wall.get filter=postponed`), production SQLite, and `/data/runtime_logs` file mirror. Local artifacts are under `artifacts/codex/INC-2026-06-28-vk-stale-publication/`.

## Timeline

- 2026-06-27 23:39 UTC — a late Telegram source merge requeued `event_id=6346` after the event had already started.
- 2026-06-27 23:40 UTC — `vk_sync:6346` created managed VK postponed post `wall-231920894_4798` for `День молодёжи 2026`.
- 2026-06-27 23:54 UTC — another merge requeued `event_id=6346`; the previous post was treated as missing and a second postponed post `wall-231920894_4802` was created.
- 2026-06-28 09:02 UTC / 11:02 KGD — VK promoted postponed id `4798` to live wall post `wall-231920894_4839`, after the event day.
- 2026-06-28 09:46 UTC — investigation confirmed the live stale post and stale postponed queue entries.

## Root Cause

1. The same-day start guard trusted any non-empty `event.end_date`. Rows with `end_date_is_inferred=1` were treated as long-running even when their public event line was a one-day timed event. `event_id=6346` had `date=2026-06-27`, `time=12:00`, and a guessed `end_date=2026-07-27`, so `vk_sync` and `tg_event_publish` were allowed after start.
2. VK postponed reservation could schedule a one-day timed event after its start when the queue/spacing pushed the next available slot too far. For `event_id=6457` the later job correctly logged `skip already-started`, but the stale postponed post `wall-231920894_4651` already existed and remained queued.
3. Existing cleanup focused on already-published past live posts and did not treat same-day/yesterday postponed managed posts as a pre-publication safety surface.

## Contributing Factors

- Smart Update can merge late source posts into an active row after the event has started.
- `end_date_is_inferred` is useful for event pages but is not strong enough evidence to bypass public fanout freshness checks.
- The postponed queue is a separate public risk: a post can be created while the event is still upcoming but publish only after the start time.

## Automation Contract

### Treat as regression guard when

- Changing `schedule_event_update_tasks`, `job_sync_vk_source_post`, `job_publish_tg_event_post`, or `post_to_vk` publication freshness logic.
- Changing VK postponed reservation/spacing (`VK_POSTPONED_*`, same-source spacing) for managed event posts.
- Repairing or auditing managed `klgdevents` live/postponed posts for past or already-started events.

### Affected surfaces

- `main.py` public fanout freshness helpers and job handlers.
- `main_part2.py::post_to_vk` / VK postponed reservation.
- Production SQLite: `event`, `event_source`, `joboutbox`.
- VK API: `wall.getById`, `wall.get filter=postponed`, `wall.delete`.

### Mandatory checks before closure or deploy

- Regression tests prove a timed event with `end_date_is_inferred=True` is not enqueued/published after start, while an explicit future `end_date` still allows true long-running events.
- Regression tests prove `post_to_vk` refuses a reserved postponed slot at or after the one-day timed event start deadline.
- Authenticated VK API check proves stale `klgdevents` live/postponed posts from this incident (`4839`, `4802`, `4651`, `4655`) are absent/deleted.
- Runtime log mirror evidence is attached for event ids `6346`, `6457`, and queue post ids.
- If production data is repaired, `PRAGMA quick_check` passes and exact deleted managed URLs are removed from event rows.
- If code is deployed, deployed SHA is reachable from `origin/main` and `/healthz` is ready.

### Required evidence

- VK API before/after artifacts for exact posts and postponed queue.
- Prod DB before/after evidence for `event_id=6346`, `6457`, `6458` and their `joboutbox` rows.
- Runtime log mirror probe/search artifact.
- Test/compile output and deployed SHA.

## Immediate Mitigation

- Deleted verified stale managed VK live/postponed posts with zero comments/reposts: `wall-231920894_4839`, `4802`, `4651`, `4655`, and stale queued `4796`.
- Cleared exact deleted canonical managed URLs from production rows `event_id=6346`, `6302`, `6457`, and `6458`; backup table: `codex_backup_vk_stale_publication_20260628_event`.
- `PRAGMA quick_check` returned `ok`; authenticated VK postponed audit after repair reported `suspicious_stale_managed_queue=[]`.

## Corrective Actions

- Public fanout now treats inferred `end_date` values as untrusted for same-day/timed freshness: a one-day timed event fails closed after its start unless it has a source-grounded explicit multi-day span.
- `post_to_vk` now accepts an event start deadline and refuses a postponed reservation that would publish at or after the start.
- Focused regression tests cover inferred-end stale skips, explicit long-running allowance, and stale postponed reservation refusal.

## Follow-up Actions

- [ ] Add a reusable authenticated VK postponed audit command that maps queued post text back to event ids and flags stale one-day timed events before publication.
- [ ] Review whether obviously wrong inferred `end_date=2026-07-27` rows for one-day events should be corrected at Smart Update merge time.

## Release And Closure Evidence

- deployed SHA: pending deploy
- deploy path: pending deploy from `hotfix/vk-stale-publication-20260628`
- regression checks:
  - `python3 -m py_compile main.py main_part2.py tests/test_tg_event_publish.py` — passed locally.
  - `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 uv run --with-requirements requirements.txt python -m pytest -q -p pytest_asyncio.plugin tests/test_tg_event_publish.py -k 'inferred_end_date or explicit_end_date or reserved_slot_after_start_deadline or same_day_started'` — `4 passed, 54 deselected`.
- post-deploy verification: pending deploy

## Prevention

This incident extends `INC-2026-06-24-vk-past-actuals`: freshness must be checked at enqueue time, execution time, and postponed-slot reservation time. Inferred date ranges are not enough to make a timed event safe for delayed public publication.
