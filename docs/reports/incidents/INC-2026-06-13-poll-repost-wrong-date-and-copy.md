# INC-2026-06-13 Poll To Repost Wrong Date And Copy

Status: mitigated
Severity: sev2
Service: Poll to Repost / Telegram event publishing
Opened: 2026-06-13
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-12-future-event-quality-llm-first-repair`, `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`
Related docs: `docs/backlog/features/poll-to-forward/README.md`, `docs/features/tg-publishing/README.md`, `docs/operations/release-governance.md`

## Summary

Poll to Repost published a debug recommendation "for tomorrow" but forwarded an
`@kldevents` post whose visible infoblock showed the event start date instead
of the target recommendation date/range. The LLM composer also added an
unsupported open-air claim and used awkward template-like phrasing.

## User / Business Impact

- Debug-channel readers could interpret the recommendation as pointing to a
  stale or wrong-date event.
- The reply copy sounded less like a local author and more like generated
  marketing text.
- Unsupported facts such as open-air format reduced trust in the
  recommendation.

## Detection

- Detected by operator screenshots and feedback in `@keniggpt` on 2026-06-13.
- Production SQL confirmed run `debug:2026-06-13T11` targeted `2026-06-14` but
  selected event `5760` with `date=2026-06-12`, `end_date=2026-06-15`, and
  `tg_event_post_id=36`.

## Timeline

- 2026-06-13 09:00 UTC: debug poll `debug:2026-06-13T11` created for
  target event date `2026-06-14`.
- 2026-06-13 09:30 UTC: resolver forwarded event `5760` / `@kldevents` message
  `36`; DB row status became `forwarded`.
- 2026-06-13: operator reported that the forwarded post looked dated from the
  wrong day and that the comment invented unsupported open-air details.
- 2026-06-13: SQL investigation confirmed the DB event had a multi-day range,
  while the selected source post was tied to the start date.

## Root Cause

1. Poll to Repost candidate loading accepted long-running events by
   `date <= target_date <= end_date`, which is correct for event listings but
   too broad for forwarding a concrete already-published `@kldevents` post.
2. `build_tg_event_announcement` displayed only `event.date`, ignoring
   `event.end_date`, so multi-day `@kldevents` posts could visually look like
   one-day start-date announcements.
3. The LLM comment prompt did not sufficiently forbid unsupported format
   inferences such as "под открытым небом" for mixed-format festivals.
4. The renderer accepted template-like neutral-link phrasing such as
   "на этом: {{EVENT_LINK}}".

## Contributing Factors

- The feature operates through forwarding, so the visible source-post infoblock
  is part of the product contract, not just a transport detail.
- Debug mode intentionally runs frequently, making bad copy/date choices more
  visible during iteration.

## Automation Contract

### Treat as regression guard when

- changing `poll_to_forward.py` candidate eligibility, winner resolution,
  topic planning, repost reply composing, or forwarded-source selection;
- changing `main_part2.py` Telegram event infoblock date formatting;
- changing LLM prompts for Poll to Repost topic generation or public comments.

### Affected surfaces

- `poll_to_forward.py`
- `main_part2.py::build_tg_event_announcement`
- `docs/backlog/features/poll-to-forward/README.md`
- `docs/features/tg-publishing/README.md`
- Fly app `events-bot-new-wngqia`
- Telegram channels `@keniggpt`, `@kldevents`

### Mandatory checks before closure or deploy

- `tests/test_poll_to_forward.py`
- `tests/test_tg_event_publish.py`
- A regression that excludes long-running start-date posts from Poll to Repost
  candidates when the target recommendation date is later than `event.date`.
- A regression that rejects unsupported open-air/street claims when the passed
  event context does not support them.
- A regression that rejects `на этом:` / `остановился на этом:` placeholder
  phrasing.
- A regression that renders Telegram event infoblock and calendar button ranges
  when `end_date > date`.
- A regression that keeps a free-events topic as an additional option: when at
  least two free candidates and at least six eligible events exist, five-option
  LLM topic plans must be rejected and a valid poll should carry six or more
  options.
- Production SQL evidence for the original bad run or equivalent smoke.
- Post-deploy `/healthz` and release SHA evidence.

### Required evidence

- deployed SHA: `441c7c8465069e15b6a6167cdc8aaf334e610643`
- tests/smoke:
  - `python3 -m py_compile poll_to_forward.py main_part2.py`
  - `/tmp/events-bot-poll-venv4/bin/python -m pytest tests/test_poll_to_forward.py tests/test_tg_event_publish.py -q` (`61 passed`)
  - Fly `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler `ok`, no issues
  - runtime smoke inside production image:
    `forward_matches_target=False`, `outdoor_claim_filtered=True`,
    `date_label='12–15 июня'`
- production SQL evidence:
  `debug:2026-06-13T11`, target `2026-06-14`, event `5760`,
  `date=2026-06-12`, `end_date=2026-06-15`, `tg_event_post_id=36`.
- confirmation that the fix is reachable from `origin/main`: pushed
  `441c7c8465069e15b6a6167cdc8aaf334e610643` to `origin/main`.

## Immediate Mitigation

- Poll to Repost eligibility now requires `event.date == target_date` for
  repost candidates, so a long-running event's old start-date post is not
  forwarded as tomorrow's recommendation.
- LLM reply rendering now rejects unsupported open-air/street claims and the
  awkward `на этом:` placeholder family.

## Corrective Actions

- Added a date-grounding guard to `load_eligible_events`.
- Added a fact-context validation path for LLM-generated repost replies.
- Tightened the LLM prompt with explicit no-inference rules for mixed-format
  festivals.
- Updated topic planning prompt to consider a playful free-events category when
  enough free candidates exist.
- Tightened topic planning so the free-events option is additive: with enough
  inventory, Poll to Repost requires at least six options and filters
  free-labelled options to `is_free=true` event ids.
- Updated `@kldevents` event-post date rendering to show `date` + `end_date`
  ranges in the infoblock and calendar button.

## Follow-up Actions

- [ ] Decide whether old already-published `@kldevents` multi-day posts should
  be edited/repaired or simply avoided by Poll to Repost.
- [ ] Add long-term reaction monitoring for Poll to Repost reply/forwarded
  messages through `telegram_post_metric`.

## Release And Closure Evidence

- deployed SHA: `441c7c8465069e15b6a6167cdc8aaf334e610643`
- deploy path: manual `fly deploy --remote-only -a events-bot-new-wngqia`
  from clean worktree; Fly release `v1379`, image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KV069Q54ESQ8YTETWYRAYXC1`
- regression checks:
  - `tests/test_poll_to_forward.py`
  - `tests/test_tg_event_publish.py`
  - targeted production SQL for the bad run
  - production runtime smoke for date guard, unsupported open-air filtering,
    and multi-day date label
- post-deploy verification: Fly status machine `48e42d5b714228`, version
  `1379`, state `started`, `1 total, 1 passing`; `/healthz` returned
  `ok=true`, `ready=true`, `db=ok`, scheduler `ok`, `issues=[]`.

## Prevention

- The incident is indexed as an active regression contract.
- Feature docs now state that forwarded source-post date visibility is part of
  Poll to Repost eligibility.
- Telegram publishing docs now require visible date ranges for multi-day event
  posts.
