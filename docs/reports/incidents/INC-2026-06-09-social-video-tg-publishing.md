# INC-2026-06-09-social-video-tg-publishing

Status: open
Severity: sev2
Service: `events-bot-new` social publishing
Opened: 2026-06-09
Closed: —
Owners: events-bot maintainer
Related incidents: `INC-2026-06-07-tg-event-publishing-media-calendar-dedup`, `INC-2026-06-05-vk-story-forward-wall-first`, `INC-2026-05-15-cherryflash-partner-fanout-promo-filter`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/crumple-video/README.md`, `docs/features/promo-campaigns/README.md`, `docs/features/tg-publishing/README.md`, `docs/features/vk-publishing/README.md`

## Summary

On 2026-06-09 several outgoing social surfaces drifted at once:

- CherryFlash published two VK wall video-announcement posts with different captions.
- `guaranteed_any_position` video promo always landed at the end of the CherryFlash clip.
- The public CherryFlash video had VK publication evidence but no normal Telegram channel-body post.
- `@kldevents` Telegram event posts stopped after 08:40 Europe/Kaliningrad while VK event posts continued.

The owner explicitly requested no repair/catch-up publication for this incident. Corrective work must only prevent future recurrence.

## User / Business Impact

- VK followers could see duplicate CherryFlash wall posts.
- Promo sold as "any position" behaved like "last position".
- `@kenigevents` did not receive a normal CherryFlash channel-body video post.
- `@kldevents` looked stopped for the working day even though upstream event publication continued in VK.

## Detection

- Detected by owner report on 2026-06-09.
- Production evidence came from Fly app `events-bot-new-wngqia`, runtime file logs enabled in `/data/runtime_logs`, and read-only SQLite `/data/db.sqlite`.
- Existing alerts did not flag the `@kldevents` day-gap because pending jobs existed; the defect was in slot choice, not handler crash.

## Timeline

- 2026-06-09 07:44 UTC: scheduled `video_popular_review` run started; ops_run `2101`.
- 2026-06-09 09:30 UTC: CherryFlash session `627` reached `PUBLISHED_TEST`.
- 2026-06-09 05:00..06:40 UTC: ten `tg_event_publish` jobs completed for `@kldevents`; last successful event post was job `22537`, event `5682`, `https://t.me/c/3954607218/82`.
- After 2026-06-09 06:40 UTC: no more successful `tg_event_publish` jobs; pending rows were scheduled into the evening or 2026-06-10 morning despite open daytime gaps.
- 2026-06-09 14:45 UTC: scheduled `video_tomorrow` run started; ops_run `2116`.

## Root Cause

1. The CherryFlash `popular_review` story target override included `vk:kenigeventsofficial:wall` with `caption_variant=crumple_official`. That target belongs to CrumpleVideo, not CherryFlash, and caused an extra VK wall video post with a different caption.
2. `_merge_promo_and_fresh_picks()` guaranteed `guaranteed_any_position` by filling organic picks first, removing a tail organic item when full, and then appending the promo. A full organic list therefore always placed the promo at the end.
3. CherryFlash only uploaded/reposted stories through the shared story fanout; it had no normal `telegram_chat` channel-body target for `@kenigevents`.
4. `next_tg_event_publish_run_at()` used the latest/max pending or done anchor in the spacing horizon. Existing pending rows at 2026-06-09 evening anchors, including rows left by the previous repair/requeue operation, caused fresh daytime `tg_event_publish` jobs to be scheduled after the evening cluster or next morning, leaving a gap after 08:40 local time.

## Contributing Factors

- CrumpleVideo and CherryFlash share the Kaggle story-publish helper but need different ordered target ownership.
- The scheduler had regression tests for stale next-day anchors, but not for an open same-day gap before late same-day pending backlog.
- The previous repair/requeue changed many rows into future anchors. That should have been harmless, but the core scheduler treated those anchors as a global tail instead of finding the nearest free slot.
- The queue health signal counted pending rows as work present and did not detect "channel has no due posts for many hours while VK continues".

## Automation Contract

### Treat as regression guard when

- Changing `video_announce/scenario.py` `popular_review` story targets.
- Changing `VIDEO_ANNOUNCE_STORY_TARGETS_JSON` production defaults.
- Changing promo placement in `video_announce/popular_review.py`.
- Changing `next_tg_event_publish_run_at()` or `tg_event_publish` queue rearm behavior.

### Affected surfaces

- `video_announce/scenario.py`
- `video_announce/story_publish.py`
- `video_announce/popular_review.py`
- `main.py` `next_tg_event_publish_run_at`
- `fly.toml` `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`
- `@kenigevents`, `@kldevents`, `vk.com/klgdevents`, `vk.com/kenigeventsofficial`

### Mandatory checks before closure or deploy

- `tests/test_video_announce_story_publish.py`
- `tests/test_promo.py`
- `tests/test_tg_event_publish.py`
- Production config check that CrumpleVideo, not CherryFlash, owns `vk:kenigeventsofficial:wall`.
- Production queue check that a new `tg_event_publish` after the last done anchor can choose the nearest free daytime slot before late backlog.

### Required evidence

- Deployed SHA.
- Focused test output.
- Production Fly deployment output.
- Post-deploy runtime evidence or a dry scheduler evidence query.

## Immediate Mitigation

No repair or catch-up publication was executed, per owner instruction.

## Corrective Actions

- Remove the CrumpleVideo `vk:kenigeventsofficial:wall` target from CherryFlash `popular_review`.
- Add a CherryFlash `telegram_chat` target to post the rendered video into `@kenigevents` channel body shortly after the story upload.
- Move CrumpleVideo's official VK wall fanout into shared production `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`.
- Insert `guaranteed_any_position` promo at a stable daily pseudo-random lower position instead of appending it.
- Change `next_tg_event_publish_run_at()` to find the earliest free publish-window slot around existing anchors instead of scheduling after the latest pending anchor.

## Follow-up Actions

- [ ] Add an observability check for long `@kldevents` no-post gaps while VK managed posts continue.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- Added regression tests for CherryFlash target ownership, CrumpleVideo official VK wall config, random-ish `guaranteed_any_position` placement, and same-day Telegram slot gaps before late backlog.
- Updated feature docs to keep target ownership and scheduler spacing behavior explicit.
