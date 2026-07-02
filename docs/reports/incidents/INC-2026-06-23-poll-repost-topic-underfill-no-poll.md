# INC-2026-06-23 Poll Repost Topic Underfill No Poll

Status: monitoring
Severity: sev1
Service: Poll to Repost / Telegram `@kenigevents`
Opened: 2026-06-23
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-22-poll-repost-orphan-open-poll`, `INC-2026-06-15-poll-repost-missing-slots`, `INC-2026-06-13-poll-repost-wrong-date-and-copy`
Related docs: `docs/backlog/features/poll-to-forward/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`, `docs/operations/telegram-link-inspection.md`

## Summary

On 2026-06-23 the production Poll to Repost create slot for target date
2026-06-24 did not publish any poll in `@kenigevents`, so there was also no
same-day result/repost. This is a continuation of the Poll to Repost reliability
thread, but the failure mode is different from the 2026-06-22 orphan poll: the
DB write did not fail; the LLM topic planner returned an over-fragmented plan
that validation reduced to zero options, and no LLM repair pass existed before
falling through to `skipped_topic_underfill`.

Telethon evidence on 2026-06-23 showed no `MessageMediaPoll` in `@kenigevents`
after midnight UTC; Fly runtime logs and production DB showed
`poll_repost_run.id=64`, `run_key=prod:2026-06-24`, status
`skipped_topic_underfill`.

## User / Business Impact

- Subscribers did not receive the promised daily topic poll for 2026-06-24
  events.
- Because no public poll existed, the evening result/repost was also absent.
- Operator trust in the feature was affected for a second consecutive day after
  the orphan-poll repair.

## Detection

- Detected by operator report on 2026-06-23: “Сегодня нет ни опроса, ни
  результата.”
- Telethon inspection with the local E2E human session confirmed the absence of
  a new poll/result in `@kenigevents` on 2026-06-23.
- Runtime file mirror on Fly was enabled (`ENABLE_RUNTIME_FILE_LOGGING=1`,
  `/data/runtime_logs/events-bot.log*`) and preserved the 14:00 UTC create slot.
- Production DB row `poll_repost_run.id=64` recorded a non-exceptional skip:
  `eligible_events=11`, `min_options=6`, `options=0`,
  `strict_popularity_inventory=true`.

## Timeline

- 2026-06-23 14:00:00 UTC — scheduler submitted
  `poll_to_forward_prod_create`, run id `8efdedc4cb28481387d8c99009429085`, for
  target date 2026-06-24.
- 2026-06-23 14:00:27 UTC — Poll to Repost reserved Google AI quota for model
  `gemini-3.1-flash-lite`.
- 2026-06-23 14:00:48 UTC — first `poll_to_forward` LLM call completed.
- 2026-06-23 14:00:53 UTC — question review completed and accepted the product
  question.
- 2026-06-23 14:00:53 UTC — creation skipped with
  `topic_underfill`: popular eligible events `11`, raw eligible events `20`,
  effective minimum content options `6`, valid options after filtering `0`.
- 2026-06-23 19:14 UTC — Telethon confirmed no production poll/result was
  visible in `@kenigevents` for the current day.

## Root Cause

1. The semantic topic planner was LLM-first, but it had only one LLM attempt for
   the topic set. When that attempt produced options that were invalid or too
   fragmented after candidate-id/popularity validation, the system immediately
   moved to deterministic bounded fallback and then skipped.
2. The strict popularity inventory required each public option to keep enough
   popular candidate groups. On a rich day with free-event axis support, the
   effective minimum rose to six options; single-candidate LLM themes can be
   filtered out completely, which is what the production diagnostics recorded as
   `options=0` despite `eligible_events=11`.
3. The deterministic fallback is intentionally conservative and should not
   invent semantic topic cuts. Without an LLM repair pass, a usable event
   inventory could still skip solely because the first LLM topic plan was
   underfilled.

## Contributing Factors

- The previous significant `Другое` feedback signal was passed to the planner,
  increasing prompt complexity for the next topic cut.
- The free-events axis raised the effective minimum option count to six, which
  is product-correct but leaves less room for single-candidate themes.
- There is only one production create slot per day; a clean `skipped_topic_underfill`
  row loses the public daily mechanic unless manually compensated.

## Automation Contract

### Treat as regression guard when

- changing `poll_to_forward.py` topic planning, LLM topic prompts, topic option
  validation, popularity filtering, feedback-other handling, or fallback topic
  generation;
- changing production create/resolve scheduler timing or manual catch-up flows;
- changing Poll to Repost docs or env around minimum option counts.

### Affected surfaces

- `poll_to_forward.py`
- Google AI consumer `poll_to_forward`
- Fly app `events-bot-new-wngqia`
- Production DB table `poll_repost_run`
- Telegram channel `@kenigevents`

### Mandatory checks before closure or deploy

- Regression test: if the first LLM topic plan underfills after validation, a
  second LLM repair planner is called and can publish a valid plan before any
  deterministic fallback.
- Regression test: sparse/fragmented inventories still skip instead of being
  over-merged into weak deterministic topics.
- Full Poll to Repost regression suite:
  `tests/test_poll_to_forward.py` and `tests/test_poll_to_forward_popularity.py`.
- Telethon evidence for `@kenigevents` on 2026-06-23: no poll before mitigation,
  and post-mitigation poll/result state if catch-up is possible.
- Production DB evidence for `poll_repost_run.id=64` / `prod:2026-06-24`.
- Runtime log evidence around 2026-06-23 14:00 UTC.
- `/healthz`, Fly status, deployed SHA, and confirmation that the deployed fix is
  reachable from `origin/main`.

### Required evidence

- Logs: `/data/runtime_logs/events-bot.log.2026-06-23_13` around 14:00 UTC show
  successful LLM calls followed by `poll_to_forward.prod_create skipped
  topic_underfill ... eligible=11 options=0 min=6 strategy=llm_underfilled`.
- DB: `poll_repost_run.id=64`, `run_key=prod:2026-06-24`,
  `status=skipped_topic_underfill`, `error_json.eligible_events=11`,
  `error_json.popularity.eligible_before_popularity=20`, `error_json.min_options=6`.
- Telethon: no `MessageMediaPoll` in `@kenigevents` on 2026-06-23 before
  mitigation.
- Tests/deploy/smoke to be filled before closure.

## Immediate Mitigation

- Incident investigation identified that the failure was semantic topic underfill,
  not the 2026-06-22 SQLite-lock/orphan-poll failure.
- Code fix `b3ffebfea66135b7e2a222f720eec6f4bcc02a1e` was deployed to Fly as
  image `deployment-01KVTZRKA3AEMHKA9WRFSK74PM`, machine version `1472`.
- Same-day catch-up was executed for target date 2026-06-24 with run key
  `prod:2026-06-24-catchup-20260623T1945Z`. It published poll
  `@kenigevents/4136` at 2026-06-23 19:40:49 UTC and resolved it at
  2026-06-23 20:00:32 UTC with reply `@kenigevents/4137`. The poll collected
  six votes, below the production minimum of eleven, so no recommendation was
  forwarded; the public low-votes reply was posted instead of fabricating a
  weak recommendation.

## Corrective Actions

- Add an LLM repair planner for Poll to Repost topic generation. If the first
  LLM topic plan is available but validates below the effective minimum, the
  repair prompt asks the LLM to rebuild the option set using only the same event
  IDs and explicit candidate-count constraints.
- Keep deterministic fallback as a last resort only for conservative,
  multi-candidate themes; it still must not invent broad semantic groupings or
  choose the final event.
- Update Poll to Repost docs, changelog, and regression tests.

## Follow-up Actions

- [ ] Add operator-facing alert/summary for `skipped_topic_underfill` on the
  production create slot, including eligible/raw inventory and validation counts.
- [ ] Consider storing redacted LLM topic-plan diagnostics in `error_json` when a
  plan underfills, so future incidents do not require inference from counts only.

## Release And Closure Evidence

- deployed SHA: `b3ffebfea66135b7e2a222f720eec6f4bcc02a1e`, reachable from
  `origin/main` and `origin/agent/T-000036`.
- deploy path: manual `flyctl deploy --remote-only --app events-bot-new-wngqia`,
  image `registry.fly.io/events-bot-new-wngqia:deployment-01KVTZRKA3AEMHKA9WRFSK74PM`,
  machine `683961db016e28`, version `1472`, `1 total, 1 passing` check.
- regression checks: `python -m py_compile poll_to_forward.py scheduling.py`;
  `python -m pytest tests/test_poll_to_forward.py tests/test_poll_to_forward_popularity.py -q`
  (`54 passed`).
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`,
  `db=ok`; Fly status showed image `deployment-01KVTZRKA3AEMHKA9WRFSK74PM`;
  production DB `poll_repost_run.id=65` is `skipped_no_votes` after catch-up,
  `poll_message_id=4136`, `reply_message_id=4137`, `total_voter_count=6`,
  `min_votes=11`; Telethon showed `@kenigevents/4136` closed and
  `@kenigevents/4137` containing the low-votes public result.

## Prevention

Semantic grouping remains LLM-first: the prevention adds a second LLM pass with
stricter grounding instructions before any deterministic fallback can run. Code
continues to act as validator/guardrail for candidate ids, popularity inventory,
free-only options, and conservative fail-closed behaviour.
