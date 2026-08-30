# INC-2026-08-30 stale video lanes and scheduled alert storm

Status: mitigated
Severity: sev2
Service: scheduled `/v tomorrow` and CherryFlash partner tracks
Opened: 2026-08-30
Closed: —
Owners: events-bot
Related incidents: `INC-2026-04-20-video-tomorrow-stuck-rendering.md`,
`INC-2026-06-13-kaggle-duplicate-videoannounce.md`

## Summary

The production database retained two video sessions as `RENDERING` after their
Kaggle kernels had already reached terminal `ERROR`. They occupied both
configured Telegram video lanes even though Kaggle had no active execution or
lease. The `/v tomorrow` watchdog treated `video_lanes_busy` as retryable but
had no retry cooldown, so it dispatched and notified the operator every minute.
Partner-track watchdogs emitted the same notice on their ten-minute ticks.

## Impact

- The 2026-08-30 scheduled `/v tomorrow` could not start at its intended slot.
- Operator Telegram received repeated false "all video lanes are busy" alerts.
- Partner track attempts for KONB and nature/ecology were also blocked.
- Startup catch-up and the watchdog produced two simultaneous attempts after a
  deploy because the watchdog performed its first tick before sleeping.

## Detection And Evidence

- Session `1100`: `RENDERING` since 2026-08-15, lane
  `TELEGRAM_AUTH_BUNDLE_S22_VIDEO1`, no heartbeat, kernel
  `zigomaro/cherryflash-video-lane-1` read back as `ERROR`.
- Session `1163`: `RENDERING` since 2026-08-29, lane
  `TELEGRAM_AUTH_BUNDLE_STORY`, expired lease, stale `running/preflight`
  ledger without heartbeat, kernel `zigomaro/crumple-video` read back as
  `ERROR`.
- Kaggle logs for both kernels showed an early resource-acquire failure; no
  render or publication occurred.
- `ops_run` rows `7958`–`7975` recorded repeated
  `skipped/video_lanes_busy`; rows `7970` and `7971` began in the same second
  from startup catch-up and watchdog paths.
- The production database passed `PRAGMA quick_check` after mitigation.

## Root Cause

1. Lane selection trusted every local `RENDERING` row without reconciling a
   stale/non-terminal ledger against the exact Kaggle provider status.
2. The periodic recovery feature was intentionally disabled in production, so
   restart recovery could not repair stale projections.
3. The tomorrow watchdog excluded skipped busy attempts from its dispatch
   evidence and had no persisted retry cooldown.
4. Lane-busy Telegram notifications had no per-slot deduplication.
5. The watchdog loop ran immediately at startup in parallel with startup
   catch-up.

## Immediate Mitigation

After a fresh provider read confirmed both kernels at `ERROR`, sessions `1100`
and `1163` were changed from `RENDERING` to `FAILED` and their exact
`videoannounce:*` ledgers were closed as terminal failed. A preimage receipt was
stored at `/data/runtime_logs/INC-2026-08-30-video-lane-preimage.json`.

The next watchdog attempt created session `1164`, persisted dataset
`zigomaro/video-afisha-session-1164`, completed confirmed remote handoff and
the exact Kaggle kernel read back as `RUNNING`. No blind public retry was made
for either failed stale session.

## Corrective Actions

- [x] Reconcile expired render rows independently of the optional periodic
  recovery job. Exact current kernel dataset plus terminal failure becomes
  `FAILED`; ambiguous/reused/complete provider state becomes `PUBLISH_BLOCKED`
  only after the absolute timeout.
- [x] Preserve fresh-ledger, active exact-lease and active-poller sessions
  conservatively.
- [x] Add persisted tomorrow, startup catch-up, partner and popular-review
  cooldowns after `video_lanes_busy` or `render_in_progress`.
- [x] Deduplicate lane-busy Telegram notices per scheduled slot for six hours,
  including across restarts through prior `ops_run` receipts.
- [x] Delay the first watchdog tick until startup catch-up has had one interval.
- [x] Resume in-flight video render pollers on startup even when the broad
  multi-product `ENABLE_KAGGLE_RECOVERY` sweep is intentionally disabled.
- [x] Stop Telegram alerts for the five intentionally disabled startup jobs
  observed in this incident; retain informational logs.
- [ ] Deploy exact main and verify two watchdog intervals without a repeat
  alert/dispatch storm.

## Automation Contract

### Treat as regression guard when

- changing video lane selection, Kaggle session/ledger recovery, scheduled
  tomorrow/partner retries, or scheduler startup ordering;
- changing `ENABLE_KAGGLE_RECOVERY` behavior or video watchdog intervals.

### Mandatory checks before closure or deploy

- a stale `RENDERING` session with terminal provider failure becomes `FAILED`
  through a compare-and-set transition;
- a fresh ledger heartbeat remains `RENDERING` and is not provider-probed;
- busy watchdog attempts are deferred during the persisted cooldown;
- repeated same-slot lane notices emit once, while a different slot may notify;
- the watchdog does not tick before its first interval;
- focused scheduler, partner, poller, video pipeline and Kaggle status tests
  pass; production health and SQLite quick check remain green.

### Required evidence

- exact implementation and deployed main SHA;
- provider statuses for affected and replacement kernels;
- session, ledger, lease and `ops_run` readback;
- post-deploy absence of per-minute alerts for at least two watchdog intervals.

## Release And Closure Evidence

- implementation SHA: pending
- deployed SHA: pending
- focused regression tests: `170 passed`; compileall and diff-check passed.
  A repository-wide collection attempt was additionally made but the reused
  incident venv lacks optional NumPy/e2e packages and standalone
  `main_part2.py` imports require boot-injected names, producing 12 pre-test
  collection errors unrelated to this change.
- production mitigation: sessions `1100` and `1163` terminal failed; session
  `1164` reached confirmed remote handoff; quick check `ok`

## Operational Note

A same-volume full SQLite backup attempted during mitigation filled `/data` and
was immediately removed while still partial. This did not cause the original
August 29 lock failure. Future incident backups must preflight free space for
database plus WAL and margin, or stream off-volume rather than copying beside
the live database.
