# INC-2026-06-05 VK Story Forward Wall-First

Status: mitigated
Severity: sev2
Service: VK story fanout / promo VK activities
Opened: 2026-06-05
Closed: —
Owners: events-bot
Related incidents: `INC-2026-05-05-80-stories-video-promo-gap`, `INC-2026-05-19-vk-posts-personal-author`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/promo-campaigns/README.md`, `docs/features/vk-publishing/README.md`

## Summary

VK stories for video announcements and `80 историй о главном` promo activity did
not match the desired wall-first product contract. Video announcements were
uploaded directly as separate VK story media, while promo poster stories rendered
event text into a white caption card under the image.

## User / Business Impact

- VK audience saw direct video stories instead of wall-post-linked stories.
- Promo poster stories could include an unreadable/irrelevant white text block.
- The `80 историй о главном` campaign expected clean daily story coverage.

## Detection

Reported by the operator on 2026-06-05 with a mobile screenshot and live check of
`klgdevents` / `kenigeventsofficial` stories.

## Timeline

- 2026-06-05: operator reported direct video story uploads and caption-polluted
  promo story forwarding.
- 2026-06-05: direct video stories `-231920894_456239035` and
  `-231828790_456239038` were deleted.
- 2026-06-05 16:10 UTC: fresh wall-linked photo stories were published:
  `-231920894_456239036` and `-231828790_456239039`, both linked to
  `https://vk.com/wall-231920894_1974`.

## Root Cause

1. The shared CherryFlash story helper treated VK story fanout as an independent
   `stories.getVideoUploadServer` upload, not as a wall-post-linked story.
2. Promo `vk_story` generated a composite story image with event text in a lower
   white panel instead of using the source poster/image directly.

## Contributing Factors

- VK API does not expose a simple `stories.repost(wall...)` method in the public
  schema; wall-post linking must be modeled through story upload parameters.
- The regression suite did not assert that these VK story paths avoid
  `stories.getVideoUploadServer` and avoid rendering a text panel.

## Automation Contract

### Treat as regression guard when

- changing `video_announce/story_publish.py`, `kaggle/CrumpleVideo/story_publish.py`,
  `video_announce/scenario.py`, or partner story targets;
- changing `promo.py` VK story/publication/repost activities;
- changing VK wall author/token behavior that affects community story links.

### Affected surfaces

- CherryFlash/Kaggle story helper target transports: `vk_wall`, `vk_wall_story`,
  legacy `vk_story`;
- `popular_review` VK target order for `kenigeventsofficial` and `klgdevents`;
- КОНБ partner track VK targets for `konb39`;
- Promo VK runner surface `vk_story`;
- VK API calls: `video.save`, `wall.post`, `stories.getPhotoUploadServer`,
  `stories.save`.

### Mandatory checks before closure or deploy

- Unit tests must prove `popular_review` and КОНБ no longer configure direct
  `vk_story` targets for these video paths.
- Unit tests must prove `vk_wall_story` uses `stories.getPhotoUploadServer` with
  `link_url` to the wall post and does not call `stories.getVideoUploadServer`.
- Promo tests must prove the story image builder returns the source wall image /
  poster bytes without rendering a text panel.
- Live VK verification must show current stories in `klgdevents` and
  `kenigeventsofficial` are wall-linked/caption-free.

### Required evidence

- targeted pytest output;
- VK API evidence showing story ids and `link.url`;
- deployed SHA reachable from `origin/main` before closure.

## Immediate Mitigation

Deleted current incorrect direct video stories in `klgdevents` and
`kenigeventsofficial`, then published fresh wall-linked photo stories in both
communities.

## Corrective Actions

- Added `vk_wall_story` target transport for video announcement fanout.
- Changed `popular_review` VK fanout to wall-first story links:
  `kenigeventsofficial` wall clip, same-community story link, then delayed
  `klgdevents` story link or local wall fallback.
- Changed КОНБ prod VK target order to `vk_wall` then `vk_wall_story` for
  `konb39`.
- Changed promo `vk_story` image generation to use the source wall image/poster
  directly instead of drawing title/date/venue into a white panel.

## Follow-up Actions

- [ ] Verify the next scheduled production run's `story_publish_report.json`.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- manual same-day repair evidence:
  `artifacts/codex/INC-2026-06-05-vk-story-forward-wall-first/manual-fresh-story-forward-2026-06-05.json`

## Prevention

VK story fanout changes must keep the wall-first and caption-free story tests in
the release gate for this surface.
