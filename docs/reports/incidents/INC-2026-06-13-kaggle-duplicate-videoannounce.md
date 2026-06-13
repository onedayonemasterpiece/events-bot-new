# INC-2026-06-13 Kaggle duplicate video announcement

Status: monitoring
Severity: sev2
Service: CherryFlash / Kaggle story-video publication
Opened: 2026-06-13
Closed: —
Owners: events-bot
Related docs: `docs/features/kaggle-status-framework/README.md`, `docs/features/cherryflash/README.md`, `docs/features/crumple-video/README.md`

## Summary

`Видеоанонс` was published twice to `@kenigevents` on 2026-06-13:
`08:11:49 UTC` and `09:47:23 UTC`. The first Kaggle session produced a public
Telegram side effect and later failed final output/report handling, so the
scheduled retry path treated the slot as missing and launched another session.

## User / Business Impact

- Subscribers saw duplicate public video announcements in the same Telegram channel.
- Operators could not tell from the final report alone whether Kaggle was alive,
  rendering, publishing, or already partially successful.

## Detection

Detected by operator report in Telegram channel history. Production DB and public
Telegram history confirmed two posts for the same scheduled slot.

## Timeline

- 2026-06-13 08:11:49 UTC: session `662` published `@kenigevents/3997`.
- 2026-06-13 08:57:10 UTC: session `662` ended as failed after no final output files were downloaded.
- 2026-06-13 09:19 UTC: replacement session `664` started.
- 2026-06-13 09:47:23 UTC: session `664` published `@kenigevents/3998`.

## Root Cause

1. Scheduler remote-handoff checks trusted final session status too strongly.
2. Kaggle story/video publication exposed no live phase/status stream to the server.
3. Kaggle had no live callback/heartbeat stream, so public side effects before final report were opaque.

## Automation Contract

### Treat as regression guard when

- changing CherryFlash/CrumpleVideo/Koenigsberg story publishing;
- changing scheduled Kaggle retry/catch-up logic;
- changing Kaggle final report download or status polling.

### Affected surfaces

- `scheduling.py` duplicate slot checks;
- `video_announce/scenario.py` Kaggle session dataset creation;
- `kaggle/CrumpleVideo/story_publish.py` public Telegram/VK fanout;
- Kaggle status callback endpoint and ledger tables.

### Mandatory checks before closure or deploy

- callback endpoint tests;
- callback/event-ledger tests;
- resource lease tests for `telegram_session:s22`;
- at least one real Kaggle run observed with `kernel_started`, `alive`, and terminal/report events.

## Corrective Actions

- Add a unified Kaggle status framework with callback events, heartbeat, server status ledger,
  and critical resource leases.
- Ship the Kaggle-side helper in every runtime dataset/notebook bundle.
- Create an operational skill/runbook for future Kaggle work.

## Release And Closure Evidence

- deployed SHA: `6671b5f1` on branch
  `hotfix/kaggle-status-framework-main-20260613`, based on `origin/main`
  `904e0aa9`.
- deploy path: Fly app `events-bot-new-wngqia`, machine version `1394`, image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KV1FGSQ70WJV1JR3PCYTRH85`,
  machine `48e42d5b714228`, health check passing.
- regression checks: local focused suite `tests/test_kaggle_status.py`
  and `tests/test_kaggle_notebook_status_instrumentation.py` passes (`10 passed`;
  pytest still hangs during Python shutdown after the green summary); syntax
  checks pass for the status framework, Kaggle client instrumentation,
  Preview3D, KoenigsbergStories, monitor kernels, and script parser kernels.
- production deployment drift found during verification: releases `v1382` and
  `v1383` initially deployed the status framework, and run `videoannounce:669`
  proved callback delivery (`kernel_started`, `resource_acquire`, `cell_started`,
  and `alive` progress through `phase=publish`). A later release `v1385`
  replaced the running image with code that did not contain `kaggle_status.py`,
  `kaggle_status_client.py`, or notebook instrumentation; the 18:10 UTC guide
  run therefore pushed old notebook code without status callbacks.
- post-v1394 production code probe: `/app/kaggle_status.py`,
  `/app/kaggle/kaggle_status_client.py`, guide/Telegram monitor helper loading,
  `video_announce/kaggle_client.py` notebook instrumentation, `preview3d:`
  status wiring, and `kenigsberg:` status wiring are present in the running
  container. `story_publish.py` was not changed by this release.
- post-deploy verification: pending next real scheduled Kaggle run on the v1394
  image (guide light is next scheduled for 2026-06-14 07:05 UTC; video popular
  review is scheduled for 2026-06-14 07:44 UTC; guide full for 2026-06-14
  18:10 UTC; `tg_monitoring` for 2026-06-14 21:40 UTC).
