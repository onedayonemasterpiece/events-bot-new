# INC-2026-06-15 Poll Repost Missing Slots

Status: open
Severity: sev1
Service: Poll to Repost / Telegram `@kenigevents` and debug `@keniggpt`
Opened: 2026-06-15
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-13-poll-repost-wrong-date-and-copy`, `INC-2026-06-14-poll-repost-duplicate-wrong-date`
Related docs: `docs/backlog/features/poll-to-forward/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-06-15 no production Poll to Repost poll/result appeared in
`@kenigevents`, and no debug poll/result appeared in `@keniggpt` during the
expected daytime window. The scheduler did run, but the feature repeatedly
self-skipped: production skipped `prod:2026-06-16` at 16:00 local because the
popularity-qualified inventory was below `min_events`, while debug repeatedly
reached a candidate inventory and then dropped to `topic_underfill` because the
LLM topic plan plus strict popularity option filtering produced zero public
options.

## User / Business Impact

- The production audience did not receive the promised daily voting mechanic.
- The debug channel could not validate the feature after the previous incident.
- Operators saw an apparently silent feature, making the rollout feel unstable
  even though scheduler jobs were technically running.

## Detection

- Detected by operator report on 2026-06-15.
- Production DB showed poll runs with terminal skipped statuses instead of
  visible poll messages.
- Runtime file mirror was available on Fly (`ENABLE_RUNTIME_FILE_LOGGING=1`,
  `/data/runtime_logs/events-bot.log*`) and confirmed the skipped scheduler
  paths.

## Timeline

- 2026-06-15 07:00-13:00 UTC: debug runs for target `2026-06-16` skipped as
  `low_popularity_inventory` with only 2 popularity-qualified candidates.
- 2026-06-15 14:00 UTC / 16:00 local: production run `prod:2026-06-16` skipped
  as `skipped_low_popularity_inventory`: raw eligible inventory was 7, but only
  3 candidates survived strict popularity filtering while prod min was 5.
- 2026-06-15 14:00-18:00 UTC: later debug runs had 3-5 eligible/popular events,
  but ended as `skipped_topic_underfill` because topic planning returned no
  valid public options after strict filtering.
- 2026-06-15: operator reported no prod poll/repost and no debug poll/repost.
- 2026-06-18: follow-up log/DB audit found that public debug had stayed blocked
  since 2026-06-15. The scheduler kept running hourly, but every visible debug
  create attempt returned `previous_poll_without_result` because the latest
  visible debug row was the manually invalidated catch-up poll
  `poll_repost_run.id=58`, status `skipped_topic_underfill`,
  `invalidated_reason=overmerged_fallback_topics_after_product_review`, with no
  forwarded result.

## Root Cause

1. Poll creation treated popularity filtering as a hard gate. If raw inventory
   was sufficient but live popularity coverage was sparse, production silently
   skipped instead of using the available event inventory with popularity as a
   ranking signal.
2. Topic planning had no bounded fallback after an LLM planner attempt. When the
   LLM returned empty or one-candidate options and the popularity inventory
   filter removed them, the run became invisible (`topic_underfill`) even with a
   usable inventory.
3. The question guard still allowed phrases like `найду`, `самое крутое` and
   `классное мероприятие`, letting the LLM reviewer accept wording that had
   already been identified as off-tone and misleading.
4. Observability recorded skipped reasons in DB/logs, but there was no public or
   operator-facing guarantee that debug would degrade into a testable poll after
   repeated underfill.
5. The first mitigation over-corrected by using a too-broad deterministic topic
   fallback: it merged unrelated formats and created weak buckets such as
   "evening" to satisfy the option count. That preserved visibility but violated
   the product bar for a friendly editorial poll.
6. The public debug blocker did not distinguish a genuinely unresolved visible
   poll from an operator-invalidated visible poll. Because id=58 remained the
   latest visible debug poll without `forwarded_message_id`, all later hourly
   debug slots self-skipped even though the scheduler was healthy.
7. Production `Другое` feedback was only terminal when `Другое` won outright.
   Non-winning but meaningful `Другое` votes were recorded in `result_json` but
   were not fed into the next production topic-planning prompt, because
   `create_prod_poll_if_due` passed no previous-feedback context.

## Contributing Factors

- The feature had recent fixes that made eligibility safer after wrong-date
  incidents, but the rollout did not add a reliability fallback for smaller
  weekday inventories.
- Production has only one create slot per day, so a transiently sparse
  popularity inventory at 16:00 local loses the whole day.
- Debug hourly runs used the same strict topic planning path, so they were not a
  reliable smoke surface.

## Automation Contract

### Treat as regression guard when

- changing `poll_to_forward.py` poll creation, popularity filtering, topic
  planning, question guardrails, or scheduler integration;
- changing `poll_to_forward_popularity.py` thresholds/baselines;
- changing Fly env around Poll to Repost schedules or min inventory.

### Affected surfaces

- `poll_to_forward.py`
- `poll_to_forward_popularity.py`
- `scheduling.py` jobs `poll_to_forward_debug`, `poll_to_forward_prod_create`,
  `poll_to_forward_prod_resolve`
- Fly app `events-bot-new-wngqia`
- Telegram channels `@kenigevents`, `@keniggpt`

### Mandatory checks before closure or deploy

- Regression test: LLM topic planner underfill with a usable popular inventory
  may produce fallback poll options only when there are enough coherent
  multi-candidate themes.
- Regression test: sparse popular inventory must not be over-merged into broad
  mood/time buckets merely to satisfy the option count.
- Regression test: raw inventory meeting min events must relax strict popularity
  filtering instead of skipping production/debug creation.
- Regression test: full LLM unavailability still skips instead of publishing a
  fully deterministic poll.
- Regression test: question guard rejects `найду`, `самое крутое`, `классное
  мероприятие` and similar off-tone phrases.
- Existing popularity tests: `tests/test_poll_to_forward_popularity.py`.
- Production evidence for 2026-06-15 skipped runs and post-deploy evidence that
  debug can create a visible poll or the next debug slot is no longer blocked by
  underfill for the observed inventory.
- Regression test: operator-invalidated visible Poll to Repost rows with
  `error_json.invalidated_reason` must not block the next slot.
- Regression test: production reads meaningful `Другое` feedback from previous
  results and passes it to the next LLM topic planner.
- Regression test: when `Другое` ties with real options, it is stored as a
  feedback signal but is not sent to the winner-selection LLM as a candidate
  option.
- `/healthz`, Fly status, deployed SHA, and confirmation that deployed fix is
  reachable from `origin/main`.

### Required evidence

- DB: `poll_repost_run.id=52`, `run_key=prod:2026-06-16`,
  `status=skipped_low_popularity_inventory`, `popular_events=3`,
  `eligible_before_popularity=7`.
- Logs: `poll_to_forward.prod_create skipped low_popularity_inventory` at
  2026-06-15 14:00 UTC and repeated `debug_create skipped topic_underfill`.
- Tests/deploy/smoke to be filled before closure.

## Immediate Mitigation

Production 2026-06-15 poll/result is intentionally not backfilled because the
user stated it is already too late for today's public production flow. Debug may
be caught up after the fix if the daytime debug window still allows it.

## Corrective Actions

Deploy mitigation: add LLM-attempt-bounded fallback topic options, relax strict
popularity filtering when raw inventory is sufficient, and tighten question
copy guardrails.

Follow-up correction: constrain fallback topics to conservative coherent
multi-candidate themes and prefer skip/alert over publishing an over-merged poll.

2026-06-18 follow-up correction: disable the long-running public debug loop on
Fly, treat operator-invalidated visible rows as terminal for slot blocking, and
promote production `Другое` votes into the next poll-planning prompt when they
are meaningful but did not win outright.

## Follow-up Actions

- [ ] Add an operator report/alert when production skips a poll slot, including
  raw vs popularity-qualified inventory and whether fallback was available.
- [ ] Decide whether production should publish a short public "no poll today"
  note on skipped days or remain silent.

## Release And Closure Evidence

- deployed SHA: `a943e5afd4689945af3b1dd0f97884f8f8d78054` initially mitigated
  the silent missing-slot issue but produced an over-merged debug fallback poll.
- follow-up SHA: `acb66219` constrains fallback topics to coherent
  multi-candidate themes and prevents over-merged fallback polls.
- deploy path: Fly `events-bot-new-wngqia`, image
  `deployment-01KV683C5MT05YCX3X813Z6BR7` for the initial mitigation.
- follow-up deploy path: Fly `events-bot-new-wngqia`, image
  `deployment-01KV68TJX3BTWEM4M8C5JPCY1P`, machine version `1423`.
- regression checks: `tests/test_poll_to_forward.py`,
  `tests/test_poll_to_forward_popularity.py` (`48 passed`).
- post-deploy verification: `/healthz` returned `ok=true`, Fly machine version
  `1422` was started with `1 passing` check, and debug catch-up created
  `poll_repost_run.id=58` in `@keniggpt` with strategy `fallback_topics`.
  That catch-up is retained as evidence for the over-merged fallback follow-up.
- bad debug catch-up containment: `poll_repost_run.id=58` was not deleted, but
  the Telegram poll was stopped and the DB row moved from `open` to
  `skipped_topic_underfill` with
  `invalidated_reason=overmerged_fallback_topics_after_product_review`, so no
  result/repost will be produced from that weak poll.
- follow-up post-deploy verification: `/healthz` returned `ok=true`, Fly machine
  version `1423` was started with `1 passing` check.
- 2026-06-18 audit evidence before fix: runtime file mirror enabled
  (`ENABLE_RUNTIME_FILE_LOGGING=1`, `/data/runtime_logs/events-bot.log*`, 24
  files); DB quick check `ok`; debug status rollup had 23
  `skipped_topic_underfill`, 6 `skipped_low_popularity_inventory`, 5 `failed`,
  and 18 `forwarded`; recent logs showed repeated
  `poll_to_forward.debug_create skipped previous_poll_without_result ...
  previous_run_id=58 previous_status=skipped_topic_underfill`; production row
  `poll_repost_run.id=59` had 11 votes including 2 for `Другое` (18.2%),
  which was recorded but not used by the old production planner.

## Prevention

Pending.
