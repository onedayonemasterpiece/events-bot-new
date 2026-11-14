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

Public peer regression on13 November: masterclass14271 linked five peers, but
service wall.getById exposed only14151 and14000;14170 was still postponed.
A stored VK URL is not proof of publication. Presentation now uses the existing
public batch read before selecting up to five peers. Canonical other dates and
source galleries stay intact; a provider read error uses the existing job retry.
No new queue, cache, writer or recommendation algorithm is introduced.

Owner clarification13 November: do not edit comments, minimizing CAPTCHA risk.
This supersedes editable recommendation comments. Reuse the existing receipt
to skip already-sent comments; recover a lost receipt by public text identity
without editing or creating a duplicate. Empty recommendations do not delete
an existing comment. New comments retain their random delayed, sequential path.

Owner correction13 November: repeated post edits are a design failure.
Root: related completion replayed bounded backfill on every graph refresh.
Remove that producer loop. Reuse vk_source_hash to wake only uncreated posts;
initial image-bearing publication waits for its anchor in the shared graph.
Existing vector completion schedules related computation, and related completion
wakes the same VK job. No new retry, persisted state, queue or publisher.
Canonical event correction remains separate from graph-only fanout.

One-time carousel/comment consistency: delayed comment previously called fresh
presentation after the random wait, so a newer graph could change the selection
relative to the already-created carousel. Carry the original presentation in
the existing JobOutbox relations payload. The same sole writer and VK guid
remain; old boolean payloads retain their existing read behavior.

Initial wakeup is scoped to an already-requested, completed VK job with no
error/terminal restriction. Graph membership alone must not request publication
of another catalogued event or resume a paused job. Reuse existing outbox state;
no new wait flag or publisher is added.

Simplification of initial waiting supersedes the wakeup producer above. Return
the existing StaticSiteSingleFlightDeferred from the initial VK handler while
its anchor is absent. The ordinary worker keeps the same job pending with its
existing delay and without counting a failed attempt. Delete the graph-completion
VK enqueue callback entirely: no catalogue scan, no unsolicited job creation,
no resumed paused/completed job. Regression checks exercise pending -> ready ->
done on one existing job, plus zero provider calls before readiness.


## Terminal VK jobs resurrected by CAPTCHA recovery, November 13

After the complete-initial-post release939, normal recovery resumed188 jobs
at21:51UTC. Job142439 then attempted to edit old post13322 and triggered
CAPTCHA again. Its failed marker was written by the CAPTCHA finalizer itself;
the independently proven historical terminal row is stopped backfill155563. The pause query included terminal
error rows, replacing their far-future due and diagnostic with the cohort
marker; the resume query then changed them to pending. Three terminal failed
VK jobs were present, including an explicitly stopped related backfill.
The handler also incorrectly marked the temporary CAPTCHA pause as terminal
failed. It now reserves terminal failure for a non-pause outcome. Both existing
cohort queries preserve terminal rows by requiring a null terminal reason. A regression covers an existing terminal error, a terminal row already
inside a paused cohort, and a normal initial publication; only the last resumes.
No new queue, state, recovery path, provider request or CAPTCHA bypass is added.
Fresh related JSON21:49UTC704anchors is verified; normal new-post/comment
acceptance remains blocked by the real VK challenge.

Read-only review caught three terminal rows already resumed by the old release
before the fix:142439,155207,155563. At22:11UTC their exact failed state and
current CAPTCHA marker were rechecked, then the three rows were restored to
terminal error with the existing far-future due; no provider call. Ordinary
initial job156444 remained pending and unchanged. This is a one-time correction
of the already-revived data, not another production recovery mechanism.
CI also crossed local midnight: the unrelated historical price fixture was
rejected as a past event. Its test now explicitly disables past-event rejection,
as neighbouring historical-source tests do, keeping the original source/date
and all seven canonical ticket-price expectations. Seven cases pass locally;
production ingestion policy remains unchanged.


## Graph changes still invalidated source publication, November 13 at23:41UTC

After release940 the normal recovery again stopped on an old wall.edit,
post14231/event10926. The source hash still serialized current relations;
unchanged-post media validation also required the current number of graph
cards. Removing the producer did not remove these independent write triggers.
The source hash now excludes recommendations, and unchanged-source validation
checks original images rather than demanding new graph cards. Initial gallery
and one-time comment creation remain. The existing no-change comment recovery
is retained so a lost initial enqueue does not silently lose its comment.
Real canonical source corrections still invoke the ordinary sync path.

A distinguishing initialized test creates the initial post with two source
photos and its selected relations, changes both peers and card count, then
verifies no second sync and an unchanged source hash; a later description
correction still syncs. Existing create-once comment tests verify no duplicate
provider write. The CAPTCHA cohort reuses the ordinary publication/comment
priority before page jobs, preserving the same30-second cadence.37 focused
checks pass. A one-time guarded cleanup stopped22 unbound existing-post update
rows in the current cohort as superseded, with no provider calls; initial
publication156444 and delayed comment jobs remain untouched. Canonical Event
facts and existing public posts were not changed. This does not claim every
stopped row had been caused by recommendations; it honors the owner's stop
on repeated old-post writes while initial delivery is restored. Evidence:
stop-existing-edit-cohort-20261113T2345.json and source-facts-hash-final-2-focused.log.
Live initial-carousel and delayed-comment acceptance remains open.

Review caught the existing persisted-hash boundary. Exact old hashes can be
reconstructed from the current selection or the original selection already
saved in the related-comment outbox payload. On an exact full-input match,
the existing persistence helper adopts the facts-only hash without upload/edit;
no new marker/state is introduced. Three distinguishing cases cover new format,
legacy current selection, and legacy original selection after graph changes.
All three retain a later genuine source correction. Unmatched historical hashes
are not silently rewritten as synchronized: missing original selection is not
evidence of an unchanged source. No bulk baseline reset is performed.

The peer's next actual read found job155630/event10906 editing managed14213
without a source hash or retained related-comment selection. It was not among
the22 hash-bearing stopped rows. The existing resume query now distinguishes
a genuinely uncreated announcement from a managed URL using the configured
owner and current Event fields, without another state/marker. Its order is
uncreated announcement, one-time comment, managed update, then page jobs.
A regression inserts an older managed row with a null hash before a fresh
announcement, with both a null URL and an external source URL; the new row
still goes first and every gap stays30seconds.40 focused checks pass.

## Reimport erased an existing CAPTCHA schedule, November 14 at 00:49 UTC

PR 941 produced a complete initial postponed carousel (event10946/post14327)
through the ordinary wall.post path at 00:30 UTC. However a concurrent source
and media update re-enqueued existing event10844. The generic enqueue merger
cleared its CAPTCHA marker and could reset its future slot when dependencies
changed; an already paused row could also be reopened through the ordinary
requeue branch. At 00:49 UTC its wall.edit14266 triggered another CAPTCHA.
This bypassed the established recovery ordering, rather than demonstrating a
failure of the facts-only source hash.

Enqueue now merges the latest payload/dependencies while retaining the existing
VK CAPTCHA status, marker, original pause timestamp and scheduled slot. Reimports
do not extend the recovery waiting period. Only the existing CAPTCHA owner
resumes paused rows. No new queue, timer, ledger field, provider request or
manual resume was added. Two distinguishing cases (paused and already resumed
pending) fail on unchanged main and pass with the fix. The two older job-dedup
fixtures for August events also fail on unchanged main in November; they are
separate past-date fixture failures, not caused by this change.

Actual comment timing is still pending: post14327 is scheduled for 04:30 UTC;
its initially selected related comment was due 04:44:11 UTC. A resumed job may
run earlier, but its existing postponed check must defer until publication.

## Early related-comment execution lost its deferred job, November 14

The existing CAPTCHA resume can schedule a related comment before its post is
published. The handler correctly read the future post date, but then enqueued
the same currently-running coalesced job. The generic merger returned merged;
the handler returned False and the worker finalized that sole job as done.
A full-worker regression reproduced the lost comment on unchanged main.

When already running as a deferred comment, return the existing datetime
worker outcome instead. The same job stays pending with zero retry attempts;
after publication it creates exactly one comment and finishes. Initial
scheduling still uses the original enqueue path. No new queue, marker, retry
or manual repair is introduced. This is separate from calendar PR943's known
publication guard and preserves that release.
The same regression covers recovery two minutes after publication at the
maximum random delay: the deadline remains publication +20 minutes, not
execution +20 minutes. The random interval is anchored to actual publication.

## Remote Kaggle polling occupied the shared worker, November 14

After release944, the ordinary related job started at 02:30 UTC and kept the
single outbox worker while the Kaggle CLI polled. Initial VK job156444 (due
02:24) and comments due02:32–02:36 remained pending after their due times.
The older verified related comment had already missed the 20-minute target
(23:37 actual delay). Fair selection cannot serve comments while a selected
handler waits for remote compute.

Related-only submission now uses the CLI's existing --no-wait option and
returns through StaticSiteSingleFlightDeferred after a successful push. The
existing finally retains the exact remote handoff/snapshot, and the existing
adopt-existing path probes once and imports completed output without another
push. Other profiles retain their previous behavior. No new queue, owner,
state field, scheduler or provider limit was added (four production lines).
The regression fails before this change and verifies one push, retained input,
no premature result finalization, then adoption/cleanup without another push.
84 related/handoff/debounce/VK checks pass. Actual VK acceptance remains open.

## Late callbacks reopened a terminal resource holder, November 14

At 03:37 UTC, related-only submission correctly returned the shared worker
after one push, but successive remote runs failed acquiring static_site:builder.
Its holder was run045002: report_written recorded failure at 02:52:13, while
the ledger had subsequently reverted to running/preflight with an earlier
updated_at (02:51:29) and retained terminal_at plus the failure message. The
resource remained active until 05:52. Callback order can differ from kernel
execution order; the status handler applied resource effects before checking
for a terminal ledger and overwrote its outcome unconditionally.

The existing host failure reconciliation confirmed that exact retained failed
report and released only its lease. The normal callback transaction now ignores
late events after terminal_at and releases the exact run's active leases when
accepting its terminal callback. This uses existing ledger/resource fields,
without another recovery loop, queue, timer or writer (11 production lines).
The regression fails on baseline, then verifies failure releases the lease,
late start/alive/acquire cannot reopen it, and a successor keeps ownership.

## Existing same-day updates kept new-publication priority, November 14

At 03:57 UTC, member10967 again selected its canonical same-day anchor10606
and attempted wall.edit13713, receiving CAPTCHA763970275209. Member10967
still had NULL publication URL/hash, despite that existing managed anchor,
because the shared pointer was copied only after successful provider writes.
Normal recovery therefore repeatedly classified this old-post update as a
new announcement, ahead of the paused calendar/recommendation comments.

Persist the already-known managed publication through the existing group-copy
helper before provider work. A failed edit now retains that canonical pointer;
the unchanged resume ordering recognizes it as a managed update and puts new
posts and one-time comments first, at the same existing cadence. This adds
five production lines and no field, queue, retry, manual resume or provider
write. The baseline regression fails on the lost member URL; after the fix it
also checks new announcement, comment, managed-update order and spacing.

Normal recovery at04:08 additionally scheduled comments156657/156658 for05:10,
despite planned publication04:30: the entire uncreated import backlog preceded
comments. Move the existing comment sort criterion first, preserving30-second
spacing and the existing publication deferrals. The regression now requires
comment, new announcement, managed update order. No current cohort slots are
rewritten manually; this rule applies to ordinary subsequent recovery.

## Legacy boolean comments could disagree with the carousel, November 14

Live public14342 published04:20UTC and comment14349 appeared04:34:43 (14:43,
within the requested interval). However, its last two links differed from the
post's five-event list. Actual job156359 retained only relations=true, so the
legacy branch recomputed presentation from the current graph. New jobs already
persist the original carousel selection as a dictionary.

Remove that dynamic legacy fallback. Jobs without a saved selection now finish
without provider calls and log the missing selection; new frozen jobs retain
their existing behavior. Existing posts/comments are neither edited nor
deleted. No selection is guessed from titles or dates, and no new state,
queue, retry or backfill is added (net two production lines, budget five).
The baseline regression returns a write for a boolean job; after the fix it
returns false without recovery, graph computation or comment API calls.

## Owner visual and relevance corrections, 14 November

The owner observed a start date on a multi-day exhibition, a square «Дом китобоя» medallion, uneven weekday/time gaps and an unrelated literature festival beside the women's league match. The existing date tile now centers the ink bounds of the date and metadata with an eight-pixel gap; multi-day related tiles use the known end date and «до». Circular alpha masks preserve the asset contents. Existing short descriptions are optional complete lines in Cygre Regular, omitted when the actual title, venue, medallions and available height leave insufficient space. Post/comment date ranges and their immutable selection remain unchanged.

The owner rejected category-based filtering: related events must be determined by their vectors. The proposed sports-domain patch and its regression test were withdrawn before PR or release. Production evidence is retained in stored-related-vector-evidence-20261114.json: actual related documents include generated category/topic/venue metadata; direct cosine and the raw compact RPC agree for match10990 versus literature9857 at 0.7661417, another match10992 at0.9140409 and KVN10977 at0.8127152. The subsequent graph also uses category/facet conditions to label lower-score pairs pure_related. The full embedding and ranking path is under investigation; the exact influence of metadata is being measured through the existing shared embedding client without projection writes. No category filter is authorized.

### 14 November: measured vector contamination and non-vector promotion

Stored related_v1 vectors and compact RPC agreed: basketball 10990 to literature 9857 cosine 0.7661417, to KVN 10977 cosine 0.8127152, to basketball 10992 cosine 0.9140409. Stored input included generated category, type, topics (including regional labels) and venue. The graph also promoted the literature pair at 0.76 for category equality and mixed facet/category/place/date bonuses into ranking. The proposed sports-category guard was rejected and removed before commit or deployment.

A four-document ablation through the canonical metered Gemini embedding client, with no projection writes, used title once plus summary/description only. Cosines became 0.6890287 (literature), 0.7769320 (KVN), 0.8328616 (basketball). The existing 0.80 vector threshold separates this positive/negative control without category rules. Fix removes metadata from related input and all non-vector promotions from pgvector ranking/reciprocity; query-search facets remain unchanged. Normal vector sync must rebuild changed related hashes before live acceptance. Retained evidence: stored-related-vector-evidence-20261114.json and content-only-vector-ablation-20261114.jsonl in the canonical task artifact directory. No old VK posts/comments are rewritten.
