# INC-2026-05-17-konb-cherryflash-test-story-preflight

Status: mitigated
Severity: sev2
Service: CherryFlash partner video announcements
Opened: 2026-05-17
Closed: —
Owners: Codex
Related incidents: `INC-2026-05-15-cherryflash-partner-fanout-promo-filter.md`, `INC-2026-04-24-crumple-story-channel-boosts-required.md`
Related docs: `docs/features/cherryflash/partner-story-tracks.md`, `docs/features/telegram-business-stories/README.md`, `docs/operations/cron.md`

## Summary

The first manual `/v Видеоанонс КОНБ` test run failed in Kaggle before rendering because the КОНБ test target `@keniggpt` was configured as a Telegram story upload. Kaggle ran `CanSendStoryRequest` for that channel and failed with `BOOSTS_REQUIRED`. The same run also inherited the global CherryFlash Telegram Business fanout, even though the КОНБ track should target only its explicit test/prod destinations.

## User / Business Impact

- The operator could not preview the new КОНБ video announcement in the test channel.
- The run failed after Kaggle handoff, consuming a Kaggle version attempt and delaying validation.
- The mounted `story_publish.json` contained unintended Business targets, risking accidental fanout if the story preflight had passed.

## Detection

- Detected by the operator after Kaggle version 98 failed on 2026-05-17.
- Kaggle output showed:
  - `targets=['tg:@keniggpt:test', 'business:d1133add072d', 'business:59ae746c6222', 'business:ac418df51ade']`;
  - `Story preflight failed for tg:@keniggpt:test: ... BOOSTS_REQUIRED`;
  - `RuntimeError: CherryFlash story publish preflight failed`.
- Production DB confirmed session `#320`, `profile_key=popular_review_konb`, status `FAILED`, dataset `zigomaro/cherryflash-session-320-1779042979`.

## Timeline

- 2026-05-17 18:31 UTC: KОНБ partner track code deployed as SHA `6b5179e9`.
- 2026-05-17 18:36 UTC: manual KОНБ run created production session `#320`.
- 2026-05-17 18:36-18:38 UTC: Kaggle version 98 failed during story preflight with `BOOSTS_REQUIRED`.
- 2026-05-17 18:45 UTC: investigation identified the wrong target transport and inherited Business target fallback.
- 2026-05-17 18:50 UTC: fix prepared to use a Telegram channel-post transport for test mode and to suppress Business fanout for КОНБ non-Business modes.

## Root Cause

1. `PARTNER_KONB_LIBRARY.test_story_targets` omitted an explicit transport. The story config parser defaulted that target to `transport=telethon`, which means Telegram Stories via Telethon, not a normal channel post.
2. `_partner_track_selection_params()` did not set `story_business_targets` for non-Business publish modes. Because `mode=popular_review` is Business-enabled, `build_story_publish_config()` fell back to the global `setting.video_announce_story_business_targets`.

## Contributing Factors

- Existing story target transports supported Telegram stories and VK targets, but not a normal Telegram channel-post target.
- The KОНБ regression tests asserted only the peer, not the transport or absence of inherited Business targets.
- The first-run product contract said “test channel” but the implementation reused story terminology too broadly.

## Automation Contract

### Treat as regression guard when

- Changing CherryFlash partner-track story targets, especially KОНБ test/prod modes.
- Changing `story_targets_override`, `story_business_targets`, or `VIDEO_ANNOUNCE_STORY_BUSINESS_MODES` behavior.
- Changing Kaggle `story_publish.py` target transport handling or preflight rules.

### Affected surfaces

- `video_announce/partner_tracks.py`
- `video_announce/scenario.py`
- `video_announce/story_publish.py`
- `kaggle/CrumpleVideo/story_publish.py`
- CherryFlash Kaggle session datasets and `story_publish.json`
- Manual `/v` KОНБ launch path and scheduled `partner_konb_library_001`

### Mandatory checks before closure or deploy

- Unit test that KОНБ test selection params use `transport=telegram_chat` and `story_business_targets=""`.
- Unit test that `build_story_publish_config()` for KОНБ test contains only `@keniggpt` with `telegram_chat`, even when a global Business cache/setting exists.
- Kaggle helper test that `telegram_chat` publish calls `send_file()` and never calls `CanSendStoryRequest`.
- Focused `py_compile` for modified CherryFlash/story modules.
- Post-deploy `/healthz` and production import/config smoke confirming KОНБ defaults.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Test command output.
- Production `/healthz` output.
- Production DB/session evidence for the failed session and, after compensation, successful replacement run evidence.

## Immediate Mitigation

- Changed KОНБ test target to an explicit Telegram channel-post transport.
- Explicitly set `story_business_targets=""` for non-Business partner publish modes, so KОНБ test/prod cannot inherit global Business fanout.

## Corrective Actions

- Added `telegram_chat` transport support to server-side story config parsing and Kaggle story runtime.
- Added regression tests for KОНБ target transport and no inherited Business fanout.
- Updated CherryFlash partner-track docs, Telegram Business docs, cron docs, and changelog.

## Follow-up Actions

- [ ] Add a compact operator-facing note in `/v` status output showing resolved story targets before Kaggle handoff.
- [ ] Consider renaming `story_targets_override` to a broader publish target term in future refactors, because it now covers VK wall/story and Telegram channel posts.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:

## Prevention

- KОНБ test mode is now pinned by tests to ordinary channel-post delivery.
- KОНБ non-Business modes are pinned by tests to no global Business fanout.
- Kaggle helper transport behavior is tested separately from server-side config generation.
