# INC-2026-06-29 80 Stories Telegram Promo Gap

Status: open
Severity: sev2
Service: Promo campaign / `80 историй о главном` Telegram companion publishing
Opened: 2026-06-29
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-04-80-stories-promo-vk-scheduler-gap`, `INC-2026-06-08-festival-vk-aggregate-regression`, `INC-2026-06-15-tg-promo-markdown-leak`, `INC-2026-06-15-tg-promo-media-drop-and-bullet-copy`, `INC-2026-06-29-tg-fresh-publish-starvation`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/features/tg-publishing/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-29 the operator saw fresh/repeated `80 историй о главном` event posts
on `vk.com/klgdevents`, but the matching Telegram companion behavior was absent
from `@kldevents`. The intended behavior for these campaign posts is: check
whether the same event already has a post in the Telegram event-flow channel,
self-forward it into that channel if it exists, and otherwise publish a new full
Telegram event post.

## User / Business Impact

- VK received visible `80 историй о главном` event refreshes while Telegram did
  not, so campaign surfaces diverged.
- Operators could not tell whether this was an intentional delay, a different
  setting, or a failed scheduler tick.
- The built-in 80 Stories campaign had production Telegram activity rows, but
  those rows were not codified in `ensure_initial_80_stories_campaign`, so drift
  could silently recur after DB edits or reseeding.

## Detection

- Detected manually by the operator on 2026-06-29 after checking VK and Telegram.
- Production DB inspection found campaign `#1` active and VK/TG activities
  enabled. `tg_event_publish` activity `#37` existed only as production config
  created on 2026-06-27, with `max_per_publish=1`, `daily_cap=1`, and a 72-hour
  window.
- Authenticated VK API showed same-day 80 Stories `klgdevents` posts:
  `https://vk.com/wall-231920894_5009`, `https://vk.com/wall-231920894_5012`,
  and `https://vk.com/wall-231920894_5013`.
- Telethon inspection of `@kldevents` last 200 messages found no matching
  current `80 историй` posts for those three events; `@kenigevents` had the
  prior 2026-06-28 forward `https://t.me/kenigevents/4204` for event `5783`.
- Runtime file mirror was available: `ENABLE_RUNTIME_FILE_LOGGING=1`, active
  `/data/runtime_logs/events-bot.log`, hourly rotated files for the incident
  window.

## Timeline

- 2026-06-27 22:19 UTC: production DB activity `#35` (`tg_repost`,
  `@kldevents` -> `@kenigevents`) was created outside the code seeding contract.
- 2026-06-27 22:27 UTC: production DB activity `#37` (`tg_event_publish`,
  `@kldevents`) was created outside the code seeding contract with one daily
  slot and a 72-hour window.
- 2026-06-29 09:30-09:34 UTC: old event-pipeline `vk_sync` jobs refreshed 80
  Stories events `5656`, `5077`, and `4417` and VK assigned live wall ids
  `_5009`, `_5012`, `_5013`.
- 2026-06-29 10:42 UTC: promo runner reconciled stale postponed ids to the live
  VK ids for those events.
- 2026-06-29 10:47 UTC: production DB showed no same-day `tg_event_publish` or
  `tg_repost` exposures for the 80 Stories campaign; only VK story/video rows
  existed for 2026-06-29.
- 2026-06-29 investigation: code/docs updated so the built-in 80 Stories
  campaign seeds and repairs the Telegram activities durably.

## Root Cause

1. The 80 Stories Telegram companion activities were production-only DB
   configuration. `ensure_initial_80_stories_campaign()` seeded VK/story/button
   activities, but did not seed or repair `tg_event_publish`/`tg_repost` rows.
2. The manually created `tg_event_publish` row was under-provisioned for the
   VK contract: one daily Telegram slot versus two daily VK event-post slots,
   plus a 72-hour satisfaction window that could treat older organic posts as
   covering today's slot.
3. Several old 80 Stories event rows still had terminal `tg_event_publish`
   outbox rows from the 2026-06-08 manual containment
   (`last_error=manual_containment_unintended_kraftmarket_reconcile`,
   `next_run_at=2036-01-01`) while their VK sync rows could still refresh. That
   made VK-only refreshes possible unless the promo Telegram companion path
   caught them.

## Contributing Factors

- The scheduler uses midpoint slots for a daily activity. With one Telegram slot
  in a 09:00-21:00 active window, the first due time is 15:00 local, later than
  the first two VK posts observed around local noon.
- The 80 Stories campaign already had several adjacent incidents and manual
  mitigations; without code-owned seeding, production activity rows were easy to
  drift from the documented contract.

## Automation Contract

### Treat as regression guard when

- changing `promo.py::ensure_initial_80_stories_campaign`;
- changing `promo.py::run_promo_vk_activities` for `tg_event_publish`,
  `tg_repost`, `vk_publication`, or `vk_story`;
- changing 80 Stories campaign defaults, targets, daily caps, active windows, or
  source windows;
- repairing old `80 историй о главном` event-pipeline `tg_event_publish` rows.

### Affected surfaces

- `promo.py` campaign seeding and promo runner;
- `promo_activity` rows for campaign `80 историй о главном / summer visibility`;
- `promo_exposure` rows for Telegram/VK campaign evidence;
- Telegram `@kldevents` and `@kenigevents`;
- VK `klgdevents` and `kenigeventsofficial`.

### Mandatory checks before closure or deploy

- Unit test proves the initial 80 Stories campaign seeds `tg_event_publish` and
  `tg_repost` in addition to VK/story surfaces.
- Unit test proves existing production-shaped `tg_event_publish` rows are
  repaired to two daily `@kldevents` slots with a 24-hour window.
- Runtime log mirror checked for the incident window.
- Authenticated VK API confirms the same-day source VK posts.
- Telethon or documented fallback confirms matching Telegram posts after repair.
- Release-governance check: deployed SHA must be reachable from `origin/main`.

### Required evidence

- Test command output.
- Deployed SHA and deploy path.
- `/healthz` after deploy.
- Production DB activity/exposure rows after deploy/repair.
- Public Telegram links for any compensated posts.

## Immediate Mitigation

- Investigation confirmed this was not merely a hidden Telegram setting: the
  Telegram campaign path was under-capped and not yet due for the first manual
  row, while old event outbox rows remained terminal-contained.
- Same-day compensation target is limited to 2026-06-29 80 Stories VK posts that
  are still future events and have no matching `@kldevents` current post.

## Corrective Actions

- Add code-owned `tg_event_publish` seed for the built-in 80 Stories campaign:
  `@kldevents`, `max_per_publish=2`, `daily_cap=2`, 24-hour window,
  09:00-21:00 active window, self-forward existing event post before creating a
  new post.
- Add code-owned `tg_repost` seed for the existing `@kldevents` ->
  `@kenigevents` 80 Stories amplification slot.
- Synchronize existing production-shaped Telegram activities to the code-owned
  caps/config on every `ensure_initial_80_stories_campaign()` run.
- Update promo/VK docs and changelog.

## Follow-up Actions

- [ ] Decide whether old 2026-06-08 `manual_containment_unintended_kraftmarket_reconcile`
  `tg_event_publish` rows for the remaining future 80 Stories inventory should
  be selectively rearmed, or whether the promo companion path is the only
  intended repair mechanism.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - pending
- post-deploy verification: pending

## Prevention

- Keep Telegram campaign activities in the same idempotent built-in seeding path
  as VK activities, so production DB rows cannot remain the only source of truth
  for the expected Telegram companion behavior.
