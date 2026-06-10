# INC-2026-06-10 CherryFlash Telegram Auth Blocked VK Fanout

Status: open
Severity: sev2
Service: CherryFlash / VK story fanout / scheduled video publication
Opened: 2026-06-10
Closed: —
Owners: Codex
Related incidents: `INC-2026-06-09-social-video-tg-publishing`, `INC-2026-06-04-80-stories-promo-vk-scheduler-gap`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/cherryflash/partner-story-tracks.md`, `docs/operations/cron.md`

## Summary

On 2026-06-10 the scheduled CherryFlash `popular_review` release and the KONB
partner CherryFlash release reached Kaggle handoff but ended as
`videoannounce_session.status='FAILED'` with no VK output. The observed failure
class was `Story publish Telethon client is not authorized`. A Telegram story
authorization failure must not block VK wall/story delivery.

## User / Business Impact

- The daily CherryFlash VK publication did not appear.
- The KONB CherryFlash partner release did not appear.
- `ops_run(kind='video_popular_review')` looked successful because it tracked
  Kaggle handoff, not terminal viewer-facing delivery.
- Runtime health did not expose `video_popular_review`, its watchdog, or
  `promo_vk`, so the missed surfaces were not visible from `/healthz`.

## Detection

- Detected by operator report on 2026-06-10: CherryFlash did not appear in VK,
  and KONB CherryFlash did not appear either.
- Production DB/log inspection showed failed sessions after Kaggle handoff:
  main CherryFlash session `633`, eco session `634`, and KONB session `635`.
- Kaggle output from the reused `zigomaro/cherryflash` kernel showed
  `Story publish Telethon client is not authorized`.

## Timeline

- 2026-06-10 07:44 UTC: main CherryFlash session `633` started and handed off to
  Kaggle dataset `zigomaro/cherryflash-session-633-*`, kernel
  `zigomaro/cherryflash`.
- 2026-06-10 07:47 UTC: session `633` became `FAILED`, `video_url=NULL`,
  `published_at=NULL`; the corresponding `ops_run` still recorded handoff
  success.
- 2026-06-10 10:33 UTC: another CherryFlash render started and held the render
  lane.
- 2026-06-10 10:37 UTC: first KONB scheduled attempt skipped with
  `render_in_progress`.
- 2026-06-10 10:47 UTC: KONB watchdog launched session `635` with
  `partner_track_id=partner_konb_library_001`.
- 2026-06-10 10:50 UTC: session `635` became `FAILED` after Kaggle handoff.

## Root Cause

1. The Kaggle story helper created the Telethon client before per-target fanout.
   When the Telegram session was unauthorized, helper initialization raised
   globally and prevented VK wall/story targets from running.
2. Scheduler duplicate suppression treated any remote Kaggle dataset/kernel
   handoff as sufficient same-day evidence, even when the session later became
   terminal `FAILED`.
3. `/healthz` omitted the affected scheduled surfaces, so the missed daily jobs
   were not visible in the standard runtime health payload.

## Contributing Factors

- `ops_run` success represented Kaggle handoff, not final delivery.
- The same shared kernel slug is reused for main and partner CherryFlash runs,
  so later runs can overwrite the immediately inspectable Kaggle output.
- Parallel scheduled tracks can produce `render_in_progress` skips; retry logic
  must distinguish active remote handoff from failed remote handoff.

## Automation Contract

### Treat as regression guard when

- changing `kaggle/CrumpleVideo/story_publish.py`;
- changing CherryFlash story target configuration;
- changing `scheduling.py` CherryFlash watchdog/startup catch-up logic;
- changing `/healthz` runtime scheduler reporting;
- changing partner CherryFlash story tracks.

### Affected surfaces

- Kaggle story helper target fanout;
- `videoannounce_session` terminal status interpretation;
- `ops_run(kind='video_popular_review')`;
- scheduler jobs `video_popular_review` and `video_popular_review_watchdog`;
- partner track watchdogs such as `partner_konb_library_001`;
- VK wall/story publication targets.

### Mandatory checks before closure or deploy

- Unit test proves VK targets continue when Telethon client creation fails.
- Unit tests prove failed remote CherryFlash handoffs are retryable, while
  active remote handoffs still suppress duplicate reruns.
- Runtime health test proves CherryFlash and promo VK jobs are represented.
- Post-deploy production evidence shows a successful same-day or next-day
  CherryFlash VK delivery and a successful KONB partner delivery.

### Required evidence

- Test command output.
- Deployed SHA and deploy path.
- `/healthz` after deploy with CherryFlash and promo scheduler keys.
- Production `videoannounce_session` rows showing terminal success/publication.
- VK URLs/story ids for the recovered deliveries.

## Immediate Mitigation

- A fresh Telegram auth bundle was copied into production secrets before the
  no-competing-deploy freeze was clarified. No code deploy was performed in this
  branch.

## Corrective Actions

- Story helper now treats Telethon client unavailability as a Telegram-family
  target error and still runs VK wall/story targets.
- CherryFlash watchdog logic now retries terminal `FAILED` sessions even when
  Kaggle dataset/kernel handoff exists.
- Runtime health now includes `video_popular_review`,
  `video_popular_review_watchdog`, and `promo_vk`.

## Follow-up Actions

- [ ] Deploy through the coordinated release window only.
- [ ] Add delivery-level `ops_run` or health evidence that separates handoff
  from final publication.
- [ ] Preserve per-session Kaggle output evidence before a reused kernel run can
  overwrite the previous failed output.

## Release And Closure Evidence

- deployed SHA: —
- deploy path: not deployed from this branch yet
- regression checks:
  - `.venv/bin/python -m py_compile scheduling.py promo.py kaggle/CrumpleVideo/story_publish.py tests/test_scheduling.py tests/test_promo.py tests/test_kaggle_story_publish.py tests/test_video_announce_story_publish.py`
  - `tests/test_kaggle_story_publish.py` -> `15 passed`
  - `tests/test_promo.py` -> `26 passed`
  - `tests/test_video_announce_story_publish.py` -> `12 passed`
  - targeted CherryFlash/promo scheduler regression set -> `8 passed`
- post-deploy verification: —

## Prevention

- Keep Telegram and VK fanout failures isolated per target.
- Treat terminal failed remote handoffs as retryable for same-day watchdogs.
- Keep daily scheduled surfaces present in `/healthz`, not only in logs.
