# INC-2026-05-15 CherryFlash Partner Fanout And Promo Filter Regression

Status: mitigated
Severity: sev1
Service: CherryFlash partner story tracks / promo selection
Opened: 2026-05-15
Closed: —
Owners: bot/runtime
Related incidents: `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-05-05-80-stories-video-promo-gap`, `INC-2026-04-26-crumple-story-required-channel-fanout`
Related docs: `docs/features/cherryflash/partner-story-tracks.md`, `docs/features/promo-campaigns/README.md`, `docs/features/telegram-business-stories/README.md`, `docs/operations/release-governance.md`

## Summary

On 2026-05-15 the scheduled `partner_eco_nature_001` CherryFlash run published the eco/nature partner video to the shared Telethon story fanout (`me`, `@kenigevents`, `@lovekenig`) in addition to the intended encrypted Telegram Business target. The same run also admitted unrelated events into the eco track after the Gemma partner classifier returned provider errors, because `manual_review` decisions were allowed in an automatic publication path.

## User / Business Impact

- A channel story boost was spent on `@kenigevents` / `@lovekenig` for content that should have gone only to the Business partner target.
- The east-region partner track used the same story-config path and would have repeated the wrong fanout.
- The eco selection included unrelated items such as `Семинар «Сатья»`, weakening partner trust and editorial quality.
- The `80 историй о главном` promo item correctly entered the video, but its current general promo policy forced the first promo into slot 1 or 2 rather than acting as a guaranteed-any-position boost.

## Detection

The operator saw the 2026-05-15 eco/nature CherryFlash story in `@kenigevents` and `@lovekenig`, then noticed unrelated selected events. Runtime file logging was enabled on production and confirmed the session configuration and selection trace.

## Timeline

- 2026-05-15 10:30 UTC: `video_partner_track_eco` launched session `#304` (`profile_key=popular_review_eco`, `partner_track_id=partner_eco_nature_001`).
- 2026-05-15 10:30 UTC: partner selection logged many `llm_error:ProviderError` decisions and admitted them as `manual_review`.
- 2026-05-15 10:31 UTC: story config for session `#304` logged targets `['me', '@kenigevents', '@lovekenig', 'business:09729df38ffe']`.
- 2026-05-15 11:20 UTC: session `#304` reached `PUBLISHED_TEST`.
- 2026-05-15 11:25 UTC: operator reported wrong channel publication and bad eco selection.
- 2026-05-15 12:10 UTC: follow-up deploy `f8ba897023d4d1f176b4b495fa5128341d24c77c` reached production with the partner/promo fixes plus CherryFlash festival-context video-card rendering. Per operator direction, no compensating CherryFlash rerun/catch-up was started.
- 2026-05-17 07:00 UTC: follow-up production investigation of current
  `popular_review_eco` sessions showed a second promo-path leak: sessions
  `#304`, `#309`, and `#310` contained `promo_campaign_id=1` candidates from
  the base `popular_review` campaign, and those promo candidates were not
  passed through the eco `event_filter`.

## Root Cause

1. Partner track selection params set `story_targets_override=[]` intending “no channel fanout”, but `build_story_publish_config()` treated an empty list as “no override” and fell back to `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`.
2. Partner auto-publish called `build_popular_review_selection(... admit_manual_review=True)`, so LLM provider failures became publishable partner events.
3. The eco classifier had only a single Gemma call path; provider errors had no retry/fallback before the selection layer saw `manual_review`.
4. Promo campaigns had no priority field or separate “guaranteed anywhere” policy, so all video promo behaved as top-slot pressure.
5. The CherryFlash selection builder always resolved video promo through the
   base `popular_review` profile and merged those promo candidates before the
   partner-specific semantic filter. This let a general campaign enter the eco
   partner track even when it was not nature/eco/local-history content.

## Contributing Factors

- Existing channel fanout env is valid for base CherryFlash/Crumple-style stories, but not for Business-only partner tracks.
- `manual_review` is useful for operator review, but scheduled partner runs have no manual approval step.
- Promo UI existed only as commands, making campaign state/priority less visible during operations.

## Automation Contract

### Treat as regression guard when

- Changing `video_announce/story_publish.py` target resolution or `story_targets_override` semantics.
- Changing partner track selection, scheduling, or `run_partner_track_pipeline()`.
- Changing promo campaign resolver ordering, priority, placement policy, or CherryFlash merge order.
- Changing eco partner classifier provider/fallback behavior.

### Affected surfaces

- `video_announce/story_publish.py`
- `video_announce/scenario.py`
- `video_announce/popular_review.py`
- `video_announce/partner_filters.py`
- `promo.py`, `handlers/promo_cmd.py`, `models.py`, `db.py`
- production Fly env `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`
- Telegram Business `postStory` path and base Telethon story fanout

### Mandatory checks before closure or deploy

- `py_compile` for changed runtime modules.
- `tests/test_video_announce_story_publish.py` must prove explicit empty `story_targets_override` blocks global channel fallback while Business target remains.
- `tests/test_partner_tracks.py` must prove eco classifier retries and 4o fallback.
- `tests/test_video_announce_popular_review.py` must prove `guaranteed_any_position` promo lands away from leading slots and stays guaranteed.
- `tests/test_video_announce_popular_review.py` must prove generic partner
  promo candidates pass the partner `event_filter` before publication, and that
  the eco-track off-filter exception admits at most one promo only after three
  profile-matched eco/nature/local-history events are already available.
- `tests/test_promo.py` must prove `80 историй` priority/policy and priority-ordered resolver behavior.
- Production `/healthz` after deploy.
- Post-deploy read-only check that partner story config for a synthetic/DB selection would resolve Business-only targets, not `@kenigevents`/`@lovekenig`.

### Required evidence

- deployed SHA:
- tests:
- production health:
- production config/DB verification:
- fix reachable from `origin/main`:

## Immediate Mitigation

- Code hotfix isolates partner story targets from global channel fallback.
- Partner auto-publish now fail-closes `manual_review` decisions instead of publishing them.
- Eco classifier retries Gemma and falls back to `gpt-4o` for one-off classification before returning `llm_error`.

## Corrective Actions

- Preserve explicit empty selection target overrides as “no Telethon targets”.
- Use Business-only targets for partner tracks.
- Add promo campaign priority (`0..3`) and set `80 историй о главном` to priority `1`.
- Add `guaranteed_any_position` policy for the 80 Stories video activity.
- Add bot UI buttons for promo report/seed/status/priority/status changes.
- Resolve base/global `popular_review` promo for the eco/nature partner track
  only under an explicit bounded policy: if the promo passes the eco filter it
  follows normal promo placement; if it does not pass, admit at most one item
  only after three profile-matched eco/nature/local-history events are already
  available, and downgrade that off-filter item to any-position placement.
- Other partner tracks keep exact-profile promo resolution unless a separate
  documented exception is added.
- Apply the partner `event_filter` to promo candidates as well as organic
  candidates, fail-closing `manual_review` in automatic runs.
- 2026-05-17 local fix evidence is not deployed yet per operator instruction.
  Regression checks: `.venv/bin/python -m py_compile promo.py video_announce/popular_review.py vk_auto_queue.py`
  and `.venv/bin/pytest tests/test_promo.py tests/test_video_announce_popular_review.py tests/test_vk_auto_queue_import.py -q`
  -> `49 passed`.

## Follow-up Actions

- [ ] Add automatic alerting when a partner-track story config contains Telethon channel targets.
- [ ] Add a partner-track dry-run command that prints sanitized target labels and selected event ids before render.

## Release And Closure Evidence

- deployed SHA: `f8ba897023d4d1f176b4b495fa5128341d24c77c`
- deploy path: clean linked worktree `hotfix/INC-2026-05-15-cherryflash-partner-fanout-promo-filter`, pushed to `origin/main`, deployed with `flyctl deploy -a events-bot-new-wngqia`
- regression checks:
  - `python3 -m py_compile scripts/render_cherryflash_full.py scripts/render_mobilefeed_intro_scene1_approval.py video_announce/selection.py video_announce/scenario.py`
  - `/home/dev/projects/events-bot-new/.venv/bin/pytest -q tests/test_cherryflash_full_render.py tests/test_video_announce_selection.py tests/test_video_announce_v_pipeline.py tests/test_partner_tracks.py tests/test_video_announce_story_publish.py tests/test_promo.py tests/test_video_announce_popular_review.py` -> `85 passed`
- post-deploy verification:
  - Fly app image `events-bot-new-wngqia:deployment-01KRNRPJAQPYAFSF3RH0PWAPP8`
  - Fly machine `48e42d5b714228`, version `1096`, state `started`, checks `1 passing`
  - `/healthz` returned `ok=true`, `ready=true`, `db=ok`, scheduler/tasks ok, `issues=[]`
  - webhook check was not available from this worktree because no `TELEGRAM_BOT_TOKEN` was present in local env; no publishing/rerun was attempted

## Prevention

The incident record now acts as a regression contract for partner story target isolation, partner classifier fail-closed behavior, and promo priority / guaranteed-any-position semantics.
