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


### 13 November: live VK consumer acceptance

Calculation repeatedly succeeds in the existing related-only shared Kaggle kernel.
Production cea78b3e2 yields fresh JSON (15:59 UTC), ordinary vector and build jobs
complete. Public14271/14286 have uploaded readable branded similar-event cards,
body lists and one link comment;14226 has a separate other-date card. Postponed
14176 retains two original photos plus a recommendation card. On16:05 UTC all9
postponed items had related lists,8 had images;10790/13973 had no source photos
and exposed a card-upload condition that wrongly required a source poster.

A second mechanical defect was exposed by repeated graph refresh:10936 moved
from postponed14282 to public, but exact-title/date recovery found pre-existing
multiple matches and returned the same value as confirmed absence. The normal
missing-post branch then published again (logs14:40,14:57,15:12 UTC). Ambiguity
must reject the current sync through the existing job error path, never enter
republish. Prefetched refresh must still omit that item and process unrelated
posts. No semantic matching or arbitrary selection is introduced.

Mandatory distinguishing checks: no-source relation card is uploaded; ambiguous
live network lookup raises before republish; prefetched ambiguity skips only the
ambiguous event. Preserve source-gallery/card ordering and one-comment replay.
Live closure pending merged release,13973 card readback and10936 canonical-ID
reconciliation without another post. Existing public duplicates are not deleted
by this change. Six-hour Crew configuration needed inclusion in the existing
service allowlist; corrected without adding a timer or service. Wake delivery
was delayed by host ENOSPC and native dispatch; it is not a punctuality proof.


Owner visual correction: use the mobile Reference4 SVG wordmark with its wide
о, rasterized transparently for Pillow without a new runtime dependency. Reuse
the approved excursion digest date tile and row typography, retaining exact ISO
dates for weekdays and explicit date spans. Visual preview retained centrally;
20 VK/guide renderer and consumer checks pass. The shared text-wrap consumption
counter is corrected to avoid ellipsis on fully fitted two-line titles.

CI blocker evidence: the first PR933 head passed python, guide and static browser
jobs, but identity failed eight media retry tests with normal-pool limiter
unavailable. An isolated clean worktree at exact production/main cea78b3e2
reproduced the same eight failures (6.78s); these tests and event_media.py are
unchanged by this PR. Evidence: retained ci-base-media-proof.log in the canonical
similar-events task directory. This is not a green identity suite. Use the
release-governance explicit local fallback if the final current-head job repeats
these exact baseline failures; do not disable or alter unrelated media policy.


Superseded by direct owner clarification on13 November: a post with no source
image must have no related content. The proposed standalone-card behavior above
is withdrawn. Suppress presentation for such events and remove stale related
text/comment through the normal consumer. Recommendation comments are queued
with one persisted random due time3–20 minutes after actual publication (or
scheduling time for an already-public backfill). Only the existing sequential
vk_calendar_comment handler may send them; it also handles calendar comments,
so comments to different posts cannot race through separate writers. Normal
sync retains a pending comment due time instead of resampling on every refresh.
