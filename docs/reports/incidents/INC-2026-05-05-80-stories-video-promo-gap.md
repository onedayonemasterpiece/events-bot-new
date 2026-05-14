# INC-2026-05-05 80 Stories Video Promo Gap

Status: closed
Severity: sev3
Service: Video announcements / festival visibility
Opened: 2026-05-05
Closed: 2026-05-14
Owners: events-bot
Related incidents: `INC-2026-05-05-80-stories-source-coverage`, `INC-2026-04-27-cherryflash-missing-photo-urls`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/crumple-video/README.md`, `docs/features/festivals/README.md`

## Summary

Events from `80 историй о главном` did not receive meaningful production video-announcement exposure. Production data showed festival rows in the event base, but no CherryFlash `popular_review` exposure and only failed/test default video-session history.

## User / Business Impact

- A major festival was absent from CherryFlash.
- CrumpleVideo history did not prove public promotion because selected rows belonged to failed/test sessions.
- Upstream source gaps compounded the downstream video gap.

## Detection

- Reported by the user on 2026-05-05.
- Production snapshot query: `event.festival='80 историй о главном'` existed, but joined `videoannounce_item` rows were not public CherryFlash exposure.

## Timeline

- 2026-03..2026-04: several festival rows were selected only in default failed/test sessions.
- 2026-05-05: user reported no festival appearance in video announcements; investigation split this from the source coverage incident because product-level promotion will be handled by future `promo`.
- 2026-05-14 09:05 UTC: scheduled CherryFlash session `#300` published two
  promoted future `80 историй о главном` events to the viewer-facing target
  `-1002210431821`; the legacy session status was `PUBLISHED_TEST`.
- 2026-05-14: promo reporting was fixed to count scheduled viewer-facing
  CherryFlash delivery as production exposure, while keeping failed/manual test
  sessions excluded.

## Root Cause

1. Video selectors were popularity/window driven and did not have a
   named-festival representation contract.
2. The initial promo exposure reporter counted only `PUBLISHED_MAIN`, but the
   scheduled CherryFlash validation target is stored by the legacy status
   machine as `PUBLISHED_TEST` even when it is viewer-facing production output.
3. Source coverage for the festival was initially incomplete, shrinking the
   candidate pool.

## Automation Contract

### Treat as regression guard when

- changing CherryFlash or CrumpleVideo selection;
- introducing the future `promo` feature;
- doing source backfill for important festivals.

### Affected surfaces

- `video_announce/popular_review.py`;
- `video_announce/scenario.py`;
- `videoannounce_session`, `videoannounce_item`;
- future `promo` metadata.

### Mandatory checks before closure or deploy

- Query festival rows joined to `videoannounce_item` by profile/status.
- Do not count `FAILED` sessions or generic manual/test `PUBLISHED_TEST`
  sessions as public exposure.
- Treat the scheduled CherryFlash viewer-facing target as production exposure
  even while the legacy session status is stored as `PUBLISHED_TEST`; evidence
  must include the target chat and selected promoted item positions.
- Once `promo` exists, prove a promoted festival/event can enter the video candidate set.

### Required evidence

- production SQL output for festival/video joins;
- session IDs/statuses used as exposure evidence;
- release SHA and public run evidence if selection behavior changes.

## Immediate Mitigation

Implemented the `promo` feature MVP and seeded the `80 историй о главном`
campaign through 2026-07-18.

## Corrective Actions

- Done: repair source coverage sufficiently for future active festival events
  with renderable posters to exist in the candidate pool.
- Done: implement `promo` as an explicit editorial mechanism rather than
  hardcoding this festival into video selectors.
- Done: record and report scheduled viewer-facing CherryFlash promo delivery.

## Follow-up Actions

- [x] After source backfill, rerun video inventory query for all future festival rows.
- [x] Define `promo` data model and selection contract.
- [x] Add operator-visible diagnostics for promoted/festival absence in CherryFlash.

## Release And Closure Evidence

- deployed SHA: `81bb5096` (`Fix CherryFlash promo reporting and slot order`)
- deploy path: `flyctl deploy --remote-only --app events-bot-new-wngqia`, release
  `v1093`, image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KRJY0FYSDVT4S3CPW4TEZH48`
- regression checks:
  - targeted pytest:
    `tests/test_video_announce_popular_review.py tests/test_promo.py` -> `11 passed`;
  - production query for CherryFlash session `#300`: promoted festival events
    `#4798` at position `1` and `#4604` at position `2`, both future rows, both
    `promo_campaign_id=1`;
  - production selection dry-run after the fix interleaved promo with organic:
    first promo at position `2`, second promo at position `4`.
- post-deploy verification:
  - `/healthz`: `{"ok": true, "ready": true, "db": "ok"}`;
  - backfilled session `#300` through `record_video_promo_exposures`, adding
    two `promo_exposure` rows with `publish_status=PUBLISHED_TEST`,
    `public_target_count=1`, target chat `-1002210431821`;
  - production `/promo report` equivalent now shows:
    `видео-публикаций: 1; промо-показов: 2` and the publication line for
    `14.05.2026 09:05 UTC`, `popular_review session #300`, positions `1, 2`.

## Prevention

Video exposure reports must distinguish selected-in-test/failed history from public delivery.
