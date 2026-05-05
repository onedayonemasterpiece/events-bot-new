# INC-2026-05-05 80 Stories Video Promo Gap

Status: open
Severity: sev3
Service: Video announcements / festival visibility
Opened: 2026-05-05
Closed: —
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

## Root Cause

1. Video selectors are popularity/window driven and do not have a named-festival representation contract.
2. No `promo` concept exists yet for editorially important events/festivals.
3. Source coverage for the festival was incomplete, shrinking the candidate pool.

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
- Do not count `FAILED` or `PUBLISHED_TEST` sessions as public exposure.
- Once `promo` exists, prove a promoted festival/event can enter the video candidate set.

### Required evidence

- production SQL output for festival/video joins;
- session IDs/statuses used as exposure evidence;
- release SHA and public run evidence if selection behavior changes.

## Immediate Mitigation

None. The user explicitly deferred product-level promotion to a future `promo` feature.

## Corrective Actions

- Pending: repair source coverage first.
- Pending: design `promo` as an explicit editorial mechanism rather than hardcoding this festival into video selectors.

## Follow-up Actions

- [ ] After source backfill, rerun video inventory query for all future festival rows.
- [ ] Define `promo` data model and selection contract.
- [ ] Add operator-visible diagnostics for promoted/festival absence in CherryFlash.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: investigation-only
- post-deploy verification: —

## Prevention

Video exposure reports must distinguish selected-in-test/failed history from public delivery.
