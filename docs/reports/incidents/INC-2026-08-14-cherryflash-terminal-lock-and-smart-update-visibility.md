# INC-2026-08-14 CherryFlash Terminal Lock And Smart Update Visibility

Status: open
Severity: sev1
Service: CherryFlash scheduler, Smart Update retry worker, event publication outbox
Opened: 2026-08-14
Closed: —
Owners: bot/runtime, Smart Update, Yandex CDN operations
Related incidents: `INC-2026-05-18-konb-cherryflash-render-lock-and-empty-selection`, `INC-2026-08-03-cherryflash-cdn-tls-retry-storm`, `INC-2026-08-10-smart-update-identity-terminal-loss`
Related docs: `docs/features/cherryflash/README.md`, `docs/features/smart-event-update/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

Production kept `videoannounce_session #1083` in `RENDERING` after its durable
Kaggle ledger had already reached terminal `done/cleanup`. The ten-minute
CherryFlash watchdog therefore treated the stale SQLite projection as a live
render lock and repeatedly told the operator to wait. In the same investigation,
Smart Update was proven to have created five events on 2026-08-14, but the
generic durable retry worker had no success notification path. One Telegram
candidate (`event #7618`) became `CREATED` on its 53rd durable attempt without an
operator report. Its downstream media and Telegram publication jobs then failed
strict TLS because `static.kenigevents.ru` had again begun serving the wrong CDN
certificate.

This is a mechanical/idempotency/transport incident. No deterministic rule is
being added to decide event meaning or identity.

## User / Business Impact

- The operator received at least eighteen duplicate render-lock notifications
  for session `#1083` on 2026-08-14.
- The daily CherryFlash slot remained blocked even though remote execution had
  ended more than two days earlier.
- Smart Update success was not consistently visible: the background retry path
  could create or merge an event without notifying the operator.
- Event `#7618` exists in the canonical DB, but its media review and Telegram
  publication remain in retry because the public static CDN TLS identity is
  invalid. A created DB event must not be confused with successful downstream
  publication.

## Detection

- The operator reported the repeated Telegram messages and absence of visible
  Smart Update successes.
- Production SQLite, Kaggle ledger, `ops_run`, JobOutbox, and the runtime file
  mirror under `/data/runtime_logs` were inspected.
- The Telegram Bot API logs show successful VK unified reports for four created
  events. The missing report was isolated to the generic retry worker, not to all
  Smart Update callers.

## Timeline

- 2026-08-12 07:44 UTC: session `#1083` enters `RENDERING` and hands off to
  `zigomaro/cherryflash`.
- 2026-08-12 09:07 UTC: durable ledger `videoannounce:1083` reaches terminal
  `done`, phase `cleanup`, progress `100`; the SQLite session remains
  `RENDERING` with no `finished_at`, error, or verified video receipt.
- 2026-08-14 03:59 UTC: durable Smart Update retry attempt 53 creates event
  `#7618`; no retry-worker success notification is sent.
- 2026-08-14 04:22–10:06 UTC: four VK Smart Update creates are accepted and
  their unified reports are acknowledged by the Telegram Bot API.
- 2026-08-14 09:13–17:53 UTC: the operator receives repeated “session #1083 is
  already rendering” messages on watchdog invocations.
- 2026-08-14: event `#7618` downstream jobs repeatedly fail with a strict TLS
  hostname mismatch for `static.kenigevents.ru`.

## Root Cause

1. The render guard trusted only `videoannounce_session.status=RENDERING`; it did
   not reconcile that projection against a terminal source-render Kaggle ledger.
2. Every watchdog invocation emitted the same render-lock message; there was no
   session/chat notification suppression window.
3. `retry_due_smart_update_candidates()` returned counters only. Its scheduler
   deliberately discarded the bot object, so a durable `CREATED` or `MERGED`
   result had no operator-facing success report.
4. The `static.kenigevents.ru` CDN certificate binding regressed again, blocking
   strict downstream media/publication jobs for a legitimately created event.

## Contributing Factors

- A terminal remote run does not by itself prove that the bot downloaded and
  delivered the video, so blindly marking the session `DONE` or republishing
  would be unsafe.
- The old poller task/recovery state had no durable local delivery receipt to
  distinguish “remote done, delivery completed” from “remote done, projection
  lost”.
- Smart Update durable retry correctly prioritised eventual acceptance, but its
  observability was coupled to interactive import callers that are absent in a
  scheduler replay.
- CDN control-plane readiness was not backed by a continuous public SAN probe.

## Automation Contract

### Treat as regression guard when

- changing CherryFlash render guards, startup recovery, poller tasks, or
  ten-minute watchdog behaviour;
- changing Kaggle video ledger terminal projection;
- changing Smart Update durable claim/retry callbacks or operator reports;
- changing `static.kenigevents.ru` DNS/CDN/certificate configuration or event
  publication retry behaviour.

### Affected surfaces

- `video_announce/poller.py`
- `video_announce/scenario.py`
- `smart_event_update.py`
- `scheduling.py`
- production `videoannounce_session`, `kaggle_run_ledger`, `ops_run`, JobOutbox
- `/data/runtime_logs`
- Yandex CDN `static.kenigevents.ru`

### Mandatory checks before closure or deploy

- an old terminal source-render ledger releases only a still-`RENDERING` DB row;
- a fresh terminal ledger remains within a bounded handoff/delivery grace;
- remote success without verified local delivery becomes `PUBLISH_BLOCKED`, not
  `DONE`, and does not blindly resend or rerender;
- remote failure becomes `FAILED`; publish-only ledgers never change the source
  render session;
- reconciliation is idempotent and cannot overwrite a concurrently terminal
  local status;
- repeated guard checks emit at most one session/chat wait message per bounded
  suppression window;
- retry-worker `CREATED`/`MERGED` results produce one bounded escaped
  superadmin report, while report failure cannot change the durable result;
- existing Smart Update replay/identity and CherryFlash scheduler tests pass;
- strict public TLS serves a SAN for `static.kenigevents.ru`, and a representative
  static ICS/media request succeeds;
- deployed SHA is reachable from exact `origin/main` and production health is
  ready;
- session `#1083` is no longer `RENDERING`, spam stops, and one controlled
  current-day CherryFlash catch-up reaches terminal evidence without duplicates;
- event `#7618` downstream publication retry/catch-up is verified after CDN
  repair.

### Required evidence

- exact pre-fix DB and terminal ledger projections for session `#1083`;
- Smart Update outcome counts and accepted event ids for 2026-08-14;
- focused and full test results on immutable PR head;
- independent exact-head review and GitHub Actions results;
- strict TLS SAN/HTTP evidence after the exact CDN binding repair;
- merged/main/Fly/in-container SHA and post-deploy DB/log checks;
- catch-up session and event-publication receipts without native credentials.

## Immediate Mitigation

- No blind session status write, video resend, rerender, or event recreation was
  performed during investigation.
- The fix reconciles terminal remote evidence only after a bounded grace and
  marks unverified delivery `PUBLISH_BLOCKED`, releasing the lock safely.
- Duplicate wait notifications are suppressed per session/chat for six hours.

## Corrective Actions

- Reconcile old terminal source-render ledgers before startup recovery and every
  render guard check.
- Make the status transition conditional on the DB row still being `RENDERING`
  and cancel a stale local poller after the durable transition.
- Add an accepted-result callback to the durable Smart Update retry worker and a
  bounded superadmin report for `CREATED`/`MERGED` results.
- Re-apply the exact existing Certificate Manager certificate to the exact
  static CDN resource and purge only that CDN cache after operator authentication.

## Follow-up Actions

- [ ] Add an external strict-TLS SAN probe for `static.kenigevents.ru`.
- [ ] Persist an explicit bot-side video download/delivery receipt so terminal
  reconciliation can distinguish delivered from publish-blocked without inference.
- [ ] Monitor retry-worker accepted-report failures as a separate observability
  alert without replaying accepted candidates.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: exact merged `origin/main` through `scripts/deploy_fly_main.sh`
- regression checks: focused pre-release suite passed; full/CI pending
- post-deploy verification: pending

## Prevention

Terminal Kaggle evidence and the local session projection are now explicitly
reconciled rather than trusted independently. Smart Update acceptance and
downstream publication are reported as separate states, so a valid DB create is
visible even when a provider/CDN job remains degraded.
