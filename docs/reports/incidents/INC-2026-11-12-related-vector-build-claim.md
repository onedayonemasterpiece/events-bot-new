# INC-2026-11-12 Related vectors blocked by a terminal site build

Status: open
Severity: sev2
Service: event-vector-sync / related-event calculation
Opened: 2026-11-12
Closed: —
Registry: inc_95018d0f3af198afabe57c82 (existing October incident)
Related: INC-2026-07-11-event-vector-sidecar-sync-stalled.md

## Impact and evidence

New events lack related vectors; calculation has no current catalog. At
11:17 UTC November 12, the last successful vector receipt was September 4
(ops_run 8216, 587 documents). Supabase was reachable: an old-anchor compact
RPC succeeded, while anchor 10840 returned `anchor event embedding is missing`.
Healthz remained ready and did not detect this freshness failure.

Build state retained job 76545 / September 4 as active although its ledger
and outbox were failed (`npm run check:preview`). Vector job 76235 repeatedly
deferred on `static_build_active`; build job 89186 exhausted attempts waiting
for vector revisions. Full evidence and owner decisions are retained under
`/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check`.

## Root cause and corrective action

Mechanical lifecycle failure: the build finally block preserved a recoverable
dataset even when the remote ledger already recorded failure. The stale claim
then blocked the independent vector owner. Preserve live/unknown/successful
recoverable runs, but finish failed/cancelled remote claims. The exact old
failed claim must be reconciled with `finish_static_site_build_claim` before
catch-up; do not disable the vector freshness barrier.

The owner explicitly deferred website restoration and authorized related
calculation without Astro for a future VK consumer. An additional profile in
the existing runner/kernel returns the same related JSON and cache before
site-only operations; it does not introduce another algorithm or queue.
The original preview-check failure remains outside the website-deferred scope.

## Automation contract

Treat as regression guard when changing build claim completion, vector guards,
related exporter, output import or Kaggle adoption.

Mandatory checks before deploy: related-only kernel returns importable JSON
without Node/Astro/service-share, changed output is rejected, existing handoff
and vector suites pass, Google provider path audit has no debt/unapproved paths.
Before declaring restoration: deployed SHA is in main; catch-up receipt covers
both vector kinds in the current catalog; the existing Kaggle returns fresh
related JSON with usable selected links and no site publication; recurring
Crew control reads actual freshness/coverage/output, rather than healthz alone.

## Release and closure evidence

Pending live verification. No restoration or closure is claimed by this record.
