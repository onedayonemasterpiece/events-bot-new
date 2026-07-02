# INC-2026-06-16 CherryFlash duplicate after bot-send failure

Status: mitigated
Severity: sev2
Service: CherryFlash / Kaggle scheduled video announcement (`@kenigevents`)
Opened: 2026-06-16
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-13-kaggle-duplicate-videoannounce`, `INC-2026-06-14-crumple-vk-transport-drift`, `INC-2026-06-16-tg-event-publish-timeout-duplicate`
Related docs: `docs/features/kaggle-status-framework/README.md`, `docs/features/cherryflash/README.md`, `docs/operations/runtime-logs.md`

## Summary

On 2026-06-16 the scheduled CherryFlash `popular_review` video announcement was
published twice to `@kenigevents`:

- `https://t.me/kenigevents/4038` — caption `Видеоанонс #682 · 16 июня`
- `https://t.me/kenigevents/4039` — caption `Видеоанонс #684 · 16 июня`

The first Kaggle run had already rendered the video, completed its Kaggle ledger,
and produced the public Telegram side effect. After output download, the server
failed while sending the downloaded mp4 to the operator/test chat through the bot
API (`Telegram server says - Bad Gateway`). A broad poller exception handler
misclassified that post-render bot-delivery failure as `kernel output download
failed`, marked session `#682` as `FAILED`, and made the scheduled slot eligible
for startup catch-up/watchdog reruns. The replacement session `#684` rendered and
published another public channel post, burning another Kaggle run without new
product value.

## User / Business Impact

- `@kenigevents` subscribers saw duplicate CherryFlash public video announcements
  for the same scheduled slot/date.
- The duplicate run consumed Kaggle runtime and external publication capacity for
  no new value.
- Operators received misleading state: production DB said `kernel output
  download failed` even though output was downloaded and the actual failure was
  Telegram bot delivery to the test/notify surface.

## Detection

- Detected by operator report with public links `https://t.me/kenigevents/4038`
  and `https://t.me/kenigevents/4039`.
- Production DB confirmed sessions `#682`, `#683`, and `#684` for the same
  `popular_review` scheduled slot.
- Runtime file mirror `/data/runtime_logs/events-bot.log` confirmed output
  download succeeded before the bot-send `Bad Gateway` and before catch-up
  dispatches.
- Kaggle status ledger confirmed `videoannounce:682` was terminal `done` at
  phase `cleanup` before the server marked the session failed.

## Timeline

All times are UTC unless noted. Public channel observations are also shown in Kaliningrad local time (UTC+2).

- 2026-06-16 07:44:00 — scheduled `video_popular_review` started and created
  session `#682`.
- 2026-06-16 07:45:45 — Kaggle dataset
  `zigomaro/cherryflash-session-682-1781595924` was created.
- 2026-06-16 08:16 (10:16 Kaliningrad) — public post
  `https://t.me/kenigevents/4038` appeared with caption `Видеоанонс #682 · 16 июня`.
- 2026-06-16 09:26:44 — Kaggle status ledger `videoannounce:682` reached
  terminal `status=done`, `phase=cleanup`, `progress_percent=100`.
- 2026-06-16 09:29:08 — server poller downloaded output for session `#682`:
  `downloaded 501 files`, including `cherryflash_full_final.mp4`.
- 2026-06-16 09:29:12 — bot delivery to test chat failed with
  `Telegram server says - Bad Gateway`.
- 2026-06-16 09:29:12 — broad poller exception logged
  `failed to download kernel output session=682` and stored session `#682` as
  `status=FAILED`, `error=kernel output download failed`, while
  `video_url=cherryflash_full_final.mp4` remained present.
- 2026-06-16 09:31:35 — startup catch-up dispatched the same missed
  `video_popular_review` slot.
- 2026-06-16 09:36:43 — session `#683` started locally, then failed before
  Kaggle handoff as `runtime restart before Kaggle handoff; rerun required`.
- 2026-06-16 09:38:27 — startup catch-up attempted the same slot again.
- 2026-06-16 09:48:26 — watchdog dispatched the same slot as missing.
- 2026-06-16 09:55:22 — startup catch-up dispatched again and created session
  `#684`.
- 2026-06-16 09:56:37 — Kaggle status ledger `videoannounce:684` was created.
- 2026-06-16 10:23:47 — `videoannounce:684` reported render complete and moved
  to `phase=publish`.
- 2026-06-16 10:24 (12:24 Kaliningrad) — public duplicate
  `https://t.me/kenigevents/4039` appeared with caption `Видеоанонс #684 · 16 июня`.
- 2026-06-16 10:29:34 — production ledger still showed `videoannounce:684` as
  `alive`, `phase=publish`, even though the public channel side effect already
  existed.

## Root Cause

1. The poller combined output download, final session classification, story
   report handling, and bot/test-chat notification in one broad failure domain.
2. A Telegram Bot API `Bad Gateway` during post-download test/notify video send
   escaped into the broad `except Exception` block that labels failures as
   `kernel output download failed` and marks the whole `videoannounce_session`
   as `FAILED`.
3. Scheduled catch-up/watchdog logic treats `FAILED` as a missing slot that may
   need a replacement full Kaggle run.
4. There is no terminal post-render status for "render is complete / video exists
   / public fanout may already have side effects / only narrow delivery or
   operator-notification remains blocked". The only available terminal failure
   state is too coarse.

## Contributing Factors

- `videoannounce_session.status=FAILED` does not distinguish render/no-output
  failures from post-render fanout or bot-notification failures.
- The scheduled slot guard does not consider `video_url` plus terminal Kaggle
  ledger `done/report_written/cleanup` as enough evidence to block a full rerun.
- Public Telegram channel publication happens inside the Kaggle-side social
  fanout, but the server-side final status can still overwrite the session as a
  failed render/output run after a later bot-send problem.
- This repeats the same unsafe invariant from the CrumpleVideo duplicate: once a
  completed render or deterministic fanout outcome exists, another full Kaggle
  render cannot create new value.

## Automation Contract

### Treat as regression guard when

- changing `video_announce/poller.py` around Kaggle output download, final
  session status, test-chat send, or notify-chat send;
- changing `scheduling.py` duplicate slot checks for `video_popular_review` or
  other scheduled video profiles;
- changing `VideoAnnounceSessionStatus` or status transitions in `models.py`;
- changing CherryFlash/CrumpleVideo/Koenigsberg story-video social fanout;
- changing Kaggle status ledger terminal handling or recovery decisions;
- changing Telegram Bot API retry/error classification for video announcement
  sends.

### Affected surfaces

- `video_announce/poller.py` post-Kaggle output handling and broad exception
  boundaries.
- `scheduling.py` scheduled catch-up/watchdog slot eligibility for
  `video_popular_review` and other video profiles.
- `models.py` / `VideoAnnounceSessionStatus` terminal-state vocabulary.
- Kaggle status tables `kaggle_run_ledger` and `kaggle_run_event`.
- Public `@kenigevents` channel posts and operator/test chat delivery.

### Mandatory checks before closure or deploy

- Unit test proving Bot API `Bad Gateway` / server error during test-chat or
  notify-chat send after successful output download does not mark the session as
  `FAILED` and does not make the scheduled slot eligible for full rerender.
- Unit test proving a session with `video_url` and terminal Kaggle ledger
  `status=done` closes the scheduled slot for full rerender even if downstream
  fanout/report/test-send is blocked.
- Unit test proving true pre-render/no-output failures remain eligible for
  bounded recovery.
- Regression test proving `BOOSTS_REQUIRED` / deterministic fanout capability
  failure is terminal for full rerender and may only trigger a narrow
  publish-only/reconcile path if that path can add value.
- Runtime log check after deploy showing no new catch-up full Kaggle run is
  launched for an already-rendered failed/fanout-blocked video slot.

### Required evidence

- Production DB rows for sessions `#682`, `#683`, and `#684`.
- Runtime log lines: `downloaded 501 files`, `failed to send video to test chat`,
  `Telegram server says - Bad Gateway`, `failed to download kernel output
  session=682`, and the subsequent catch-up/watchdog dispatches.
- Kaggle ledger rows for `videoannounce:682` and `videoannounce:684`.
- Public Telegram link evidence for `@kenigevents/4038` and `@kenigevents/4039`.
- Deployed SHA reachable from `origin/main` once corrective code is shipped.

## Immediate Mitigation

- No duplicate cleanup was performed in this incident record yet. If cleanup is
  desired, keep exactly one public post and record which Telegram message is the
  canonical survivor before deleting the other.
- The operational mitigation is to stop treating this as a request for generic
  publication idempotency. The unsafe action is the replacement full Kaggle run;
  retrying the whole notebook cannot add value after the first notebook already
  rendered and publicly posted.

## Corrective Actions

- Introduce a terminal post-render status such as `DELIVERY_BLOCKED` /
  `PUBLISH_BLOCKED` / `FANOUT_BLOCKED` for sessions where render output exists
  but downstream fanout, public-surface capability, or operator/test delivery is
  blocked.
- Reserve `FAILED` for pre-render/render/no-output failures where a replacement
  full Kaggle run can plausibly add value.
- Split poller exception handling into separate phases:
  1. output download / video discovery;
  2. story/public fanout report classification;
  3. bot test/notify delivery.
  Failures in phase 3 must not reclassify phase 1 as failed.
- Make scheduled slot eligibility treat any completed render evidence
  (`video_url`, downloaded final mp4, terminal Kaggle ledger `done`, or durable
  public-side-effect report) as a hard stop for full rerender.
- If retry is needed after post-render failure, retry only the narrow operation
  that can add value: bot notify resend, report reconciliation, or publish-only
  target recovery. Do not rerender.

## Follow-up Actions

- [x] Implement the post-render terminal status and scheduler guard for completed
      Kaggle evidence.
- [x] Refactor `video_announce/poller.py` exception boundaries so post-download
      bot-send errors cannot be logged as output-download failures.
- [x] Add tests for `BOOSTS_REQUIRED`, Bot API `Bad Gateway`, and terminal
      ledger/video-url slot closure.
- [ ] Decide and document cleanup policy for duplicate `@kenigevents` video
      announcements: keep newest, keep first, or leave both when deleting would
      cause more churn.

## Release And Closure Evidence

- deployed SHA: `7c3026f0f260922713a0ff7525e5dca36d9d0600`, reachable from
  `origin/main`.
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia --remote-only`
  from clean worktree `inc/20260616-cherryflash-duplicate`; Fly image
  `registry.fly.io/events-bot-new-wngqia:deployment-01KV80X1E0MG4KNWE1AMM11EG3`,
  machine `683961db016e28` version `1433`, `1 total, 1 passing`.
- regression checks: `python -m py_compile models.py scheduling.py
  video_announce/poller.py`; `python -m pytest -q
  tests/test_video_announce_poller.py tests/test_scheduling.py` reported
  `39 passed, 1 warning in 6.78s` and then hit the known interpreter shutdown
  hang, interrupted after the green summary; `git diff --check` passed.
- post-deploy verification: `/healthz` returned `ok=true`, `ready=true`, DB and
  scheduler `ok`, with `video_popular_review` and watchdog both `ok`; production
  code probe in `/app` confirmed `PUBLISH_BLOCKED`,
  `_video_session_status_closes_scheduled_slot`, and
  `post-render bot delivery failed` are present.
- post-deploy scheduler evidence: after deploy and after the next watchdog tick,
  production DB showed no new `ops_run(kind='video_popular_review')` after
  `2026-06-16 10:45 UTC`; no new video sessions beyond `#684`; runtime logs
  showed startup catch-up skipped with `confirmed Kaggle handoff already exists
  today` instead of launching another full Kaggle run.

## Prevention

The invariant for scheduled Kaggle video jobs must be: after render output or a
public side effect exists, a full replacement Kaggle run is forbidden unless an
operator explicitly overrides it with evidence that rerendering creates new
product value. Scheduler recovery may retry only narrow missing operations and
must use a distinct terminal status for post-render/fanout-blocked sessions.
