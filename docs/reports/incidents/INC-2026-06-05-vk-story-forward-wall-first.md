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
- 2026-06-05 16:24 UTC: hotfix `d5b8a1dc` was deployed to Fly from clean
  `origin/main` through local `flyctl`; GitHub Actions deploy run
  `27026657785` failed earlier because repository secret `FLY_API_TOKEN` was
  empty.
- 2026-06-05 16:26 UTC: production `/healthz` returned `ok=true`,
  `ready=true`, `db=ok`, and no scheduler/task issues.

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
  poster bytes without rendering a text panel, and that promo story uploads do
  not pass a VK wall `link_url`.
- Live VK verification must show current stories in `klgdevents` and
  `kenigeventsofficial` include the CherryFlash video wall story and
  caption-free promo image stories.

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
- Changed promo `vk_story` upload to omit VK `link_url`, because VK renders wall
  links as a white wall-post/caption card under the story image.

## Follow-up Actions

- [ ] Verify the next scheduled production run's `story_publish_report.json`.

## Release And Closure Evidence

- deployed SHA: `e61fa7362d2afa4bb7493787da5e9a46a1f32658`
  (`origin/main`)
- deploy path: local `flyctl deploy --remote-only --app events-bot-new-wngqia
  --config fly.toml`, image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KTCFH65WKS8M8DY14NAGP8ES`,
  machine `48e42d5b714228` version `1199`
- blocked deploy path: GitHub Actions run
  `https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/27026657785`
  failed at Fly deploy with empty `FLY_API_TOKEN`
- regression checks:
  - `python3 -m py_compile promo.py video_announce/story_publish.py
    video_announce/scenario.py video_announce/partner_tracks.py
    kaggle/CrumpleVideo/story_publish.py`
  - `pytest -q tests/test_promo.py::test_vk_story_image_uses_source_post_photo_without_text_panel
    tests/test_promo.py::test_promo_vk_runner_schedules_publications_and_repost
    tests/test_video_announce_story_publish.py
    tests/test_kaggle_story_publish.py::test_vk_wall_story_links_previous_wall_post_without_video_story_upload
    tests/test_partner_tracks.py::test_konb_track_defaults_to_prod_story_targets`
    passed: `15 passed`
  - `pytest -q tests/test_promo.py` passed: `22 passed`
- postdeploy health: `https://events-bot-new-wngqia.fly.dev/healthz` returned
  `ok=true`, `ready=true`, `issues=[]`
- superseded manual repair evidence (wrong wall post; kept for audit trail):
  `artifacts/codex/INC-2026-06-05-vk-story-forward-wall-first/manual-fresh-story-forward-2026-06-05.json`
- corrective manual repair after operator follow-up:
  - deleted stale/wrong linked promo stories for `wall-231920894_1974` and
    `wall-231920894_2214` from `klgdevents` and `kenigeventsofficial`;
  - republished clean image-only promo stories with `link=null`:
    `-231920894_456239038`, `-231920894_456239039`,
    `-231828790_456239041`, `-231828790_456239043`;
  - published the actual CherryFlash video wall post
    `https://vk.com/wall-231828790_992` into stories:
    `-231920894_456239040` and `-231828790_456239042`, both with
    `link.url=https://vk.com/wall-231828790_992`;
  - evidence:
    `artifacts/codex/INC-2026-06-05-vk-story-forward-wall-first/manual-cherryflash-992-and-clean-promo-2026-06-05.json`
    and
    `artifacts/codex/INC-2026-06-05-vk-story-forward-wall-first/manual-clean-promo-2214-kenig-retry-2026-06-05.json`

## Prevention

VK story fanout changes must keep the wall-first and caption-free story tests in
the release gate for this surface.
