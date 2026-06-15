# INC-2026-06-15 CherryFlash caption metadata missing

Status: open
Severity: sev3
Service: CherryFlash / Telegram channel video post fanout
Opened: 2026-06-15
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-13-kaggle-duplicate-videoannounce`
Related docs: `docs/features/cherryflash/README.md`, `docs/operations/cron.md`

## Summary

The 2026-06-15 CherryFlash public Telegram post in `@kenigevents` was published
with the caption `Видеоанонс` only. Viewer-facing posts must include the release
number and the target date (`Видеоанонс #<session_id> · <D month>`), so the
publication had insufficient metadata even though the video itself was delivered.

## User / Business Impact

- Subscribers saw a generic video caption without release number or date.
- Operators could not visually distinguish daily CherryFlash releases from the
  channel history without opening production session metadata.
- The same static override would affect future scheduled `popular_review`
  Telegram channel posts until fixed.

## Detection

Detected by operator report after the public CherryFlash post was visible.
Production DB showed session `#677` for target date `2026-06-15`, and the public
Telegram channel history showed `@kenigevents/4029` captioned only
`Видеоанонс`.

## Timeline

- 2026-06-15 07:44 UTC: CherryFlash session `#677` created for target date
  `2026-06-15`.
- 2026-06-15 08:12 UTC: public `@kenigevents/4029` Telegram post appeared with
  only `Видеоанонс`.
- 2026-06-15 09:29 UTC: session `#677` reached `PUBLISHED_TEST`.
- 2026-06-15: operator reported the missing number/date metadata.

## Root Cause

1. `VideoAnnounceScenario._popular_review_selection_params()` used static
   story target captions because the session id is not known before the DB row
   is created.
2. `build_story_publish_config()` expands the VK wall placeholder caption into
   a richer VK caption, but the `telegram_chat` target had no corresponding
   expansion layer and therefore posted the literal static caption.

## Contributing Factors

- The caption contract was implicit and not documented in the CherryFlash
  feature spec.
- Unit tests asserted that the base story target override contained the static
  placeholder but did not cover the persisted post-id-aware params used by real
  runs.

## Automation Contract

### Treat as regression guard when

- changing `popular_review` session creation or `selection_params` persistence;
- changing `story_targets_override` for CherryFlash;
- changing `video_announce/story_publish.py` caption expansion for
  `telegram_chat` or `vk_wall` targets.

### Affected surfaces

- `video_announce/scenario.py` CherryFlash session creation;
- `video_announce/story_publish.py` target config generation;
- public Telegram channel `@kenigevents` video posts;
- VK wall CherryFlash caption title.

### Mandatory checks before closure or deploy

- unit tests covering numbered/date caption generation and story config output;
- production post edit for the affected `@kenigevents` message if still editable;
- post-deploy health check and verification that the fix is reachable from
  `origin/main`.

### Required evidence

- deployed SHA;
- focused pytest output;
- edited public post evidence or explicit Bot API failure reason;
- production `/healthz` after deploy.

## Immediate Mitigation

Edited `@kenigevents/4029` with the production bot token from `Видеоанонс` to
`Видеоанонс #677 · 15 июня`. Public `t.me/s/kenigevents` scrape confirmed the
new caption is visible for message `4029`.

## Corrective Actions

- Add a CherryFlash caption builder that formats `Видеоанонс #<session_id> · <D month>`.
- Persist post-id-aware `story_caption` after the session row is flushed and
  before render/story publication starts.
- Fill the `telegram_chat` target caption from the same builder while leaving
  the VK wall placeholder available for the existing rich VK caption expansion.
- Document the caption contract and add focused regression tests.

## Follow-up Actions

- [ ] Consider exposing public Telegram message ids in the session/story report
      so caption repair does not require public channel scraping.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

The feature doc now makes the numbered/date caption mandatory, and focused unit
tests cover both the pure formatter and the story publish config output for
Telegram and VK targets.
