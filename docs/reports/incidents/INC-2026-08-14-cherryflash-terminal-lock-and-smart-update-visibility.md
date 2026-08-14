# INC-2026-08-14 CherryFlash Terminal Lock And Smart Update Visibility

Status: closed
Severity: sev1
Service: CherryFlash scheduler, Smart Update retry worker, event publication outbox
Opened: 2026-08-14
Closed: 2026-08-14
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
- Event `#7618` initially remained in downstream retry because the public static
  CDN TLS identity was invalid. The incident catch-up repaired the exact CDN
  binding and then completed media review, Telegram publication, and VK sync.
  The created event and its downstream delivery were verified as separate
  durable states.

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
- 2026-08-14 18:41 UTC: exact merged `origin/main` SHA
  `40b78c5dde888a75022832fe2fe1351a456c0775` is deployed through
  `scripts/deploy_fly_main.sh`; Fly reports one healthy machine and the same
  in-container SHA.
- 2026-08-14 18:42 UTC: startup reconciliation moves stale session `#1083`
  from `RENDERING` to `PUBLISH_BLOCKED`, preserving the fact that remote render
  success was not a verified local delivery.
- 2026-08-14 18:45–18:56 UTC: after the exact CDN certificate binding repair,
  event `#7618` media review, VK sync, and Telegram publication complete. A
  compensating accepted-retry notification is sent to the superadmin.
- 2026-08-14 18:55–20:44 UTC: controlled current-day catch-up session `#1097`
  runs on Kaggle, reaches `done/cleanup`, releases its Telegram session lease,
  downloads the final output, and finishes as `PUBLISHED_TEST`. The prior
  automatic recovery attempt `#1096` is truthfully terminal `FAILED` after its
  preflight callback timeout; it is not left as another render lock.

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

## Production Recovery

- Session `#1083` is terminal `PUBLISH_BLOCKED` with `finished_at` populated and
  no false publication receipt. The runtime file mirror contains the one
  reconciliation line and zero repeated `#1083` wait notices after deployment.
- Current-day compensating CherryFlash session `#1097` is `PUBLISHED_TEST` with
  `video_url=cherryflash_full_final.mp4`, a terminal `report_written` and
  `render_done`, and a released
  `telegram_session:env:TELEGRAM_AUTH_BUNDLE_STORY` resource lease.
- Event `#7618` (`Mu_tronic`) has durable `done` rows for media review,
  Telegram publication, VK sync, ICS, Telegraph, and calendar publication.
  Sanitized receipts are `https://t.me/c/3954607218/3243`,
  `https://vk.com/wall-231920894_8967`, and the canonical Telegraph/ICS URLs in
  the event row. The ordinary next-day media-review schedule remains pending by
  design and is not an incident retry.
- The retry-worker notification path sent one compensating success report for
  event `#7618`; the code now emits the same bounded report automatically for
  future durable `CREATED`/`MERGED` retry results.
- SQLite `PRAGMA quick_check` is `ok`; `/healthz` is ready with no issues and
  all critical scheduler/task checks are healthy.
- Twenty consecutive strict HTTPS requests and ten TLS handshakes verified the
  `static.kenigevents.ru` SAN after the exact CDN update. Static ICS retrieval
  returned HTTP 200. No bucket, mail, shared DNS-zone, or unrelated Yandex Cloud
  resource was changed.

## Follow-up Actions

- [ ] Add an external strict-TLS SAN probe for `static.kenigevents.ru`.
- [ ] Persist an explicit bot-side video download/delivery receipt so terminal
  reconciliation can distinguish delivered from publish-blocked without inference.
- [ ] Monitor retry-worker accepted-report failures as a separate observability
  alert without replaying accepted candidates.

## Release And Closure Evidence

- implementation PR: `#502`
- implementation head: `50c5206e22e3aaf86a134697af306cf84737bfa5`
- merge, `origin/main`, deployed, and in-container SHA:
  `40b78c5dde888a75022832fe2fe1351a456c0775`
- deploy path: exact merged `origin/main` through `scripts/deploy_fly_main.sh`
- regression checks: focused suite `134 passed`; stricter changed-surface suite
  `98 passed`; all required GitHub Actions on PR `#502` passed
- post-deploy verification: Fly machine version `1974` is started with `1/1`
  checks passing; `/healthz` reports `ok=true`, `ready=true`, database and
  scheduler/task checks healthy; production SQLite quick-check is `ok`
- incident projections: `#1083=PUBLISH_BLOCKED`, `#1096=FAILED`,
  `#1097=PUBLISHED_TEST`; no post-deploy `#1083` wait spam was found in the
  runtime file mirror
- Smart Update recovery: event `#7618` downstream publication rows are `done`
  and the missing accepted-retry notification was compensated

## Prevention

Terminal Kaggle evidence and the local session projection are now explicitly
reconciled rather than trusted independently. Smart Update acceptance and
downstream publication are reported as separate states, so a valid DB create is
visible even when a provider/CDN job remains degraded.
