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

The related-only release reached production and the terminal claim was cleared.
Catch-up persisted 1,016 embedding rows for 622 documents, then stopped on Google
429. The selected key 4 scope reached its 1,000 RPD limit while other registered
scopes remained available. Production already configures a six-key
`GOOGLE_AI_NORMAL_KEY_ENVS` pool, but vector sync's explicit default lane disables
implicit pool inheritance. Pass that existing pool to the shared client; retain
the legacy lane only when no normal pool is configured. No quota is raised or
reset, and every pool reservation remains project/model scoped and atomic.

The BGE-M3 static-related implementation also exists, but actual runtime still
selects `STATIC_SITE_RELATED_MODE=pgvector`. This fix preserves that model space;
it does not assert that the BGE cutover has passed its separate quality gates.

Pending live verification. No restoration or closure is claimed by this record.

Normal vector runs 11994, 11999 and 12002 completed on November 12, but ongoing
Smart Updates repeatedly moved the live revisions ahead of their snapshots,
preventing the related Kaggle from starting. Related-only now consumes the
vector owner's bounded immutable input, and vector completion wakes the same
outbox. New ingestion is handled by the following projection instead of
invalidating the ready one. A concurrent-update regression checks that the
input remains the projected version while the live Event advances. The static
job receives peer priority with vectors in this mode; site profiles retain
their existing policy. Product acceptance still requires fresh related JSON
and then the owner's authorized VK cards/links feature, not vector counters.

November 13: PR906 exact main 4aab83233 was deployed with full 4,478-file
verification and a network-none concurrent-ingest preflight. Health is ready.
The receipt was still the previous snapshot because ordinary vector work waited
behind the media/publication stream. The outbox also applied an implicit FIFO
for the vector job's incidental owner Event: the regression selected media even
with vector priority raised. Vector and static jobs now use the existing
independent-task set and their explicit snapshot/barrier contracts, not the
owner Event's unrelated media FIFO. Related-only computation receives VK peer
priority; no second operator vector writer is introduced. Live JSON remains
required before closure.
