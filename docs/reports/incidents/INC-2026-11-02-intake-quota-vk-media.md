# INC-2026-11-02 Event intake quota exhaustion and VK media backlog

Status: open
Severity: sev2
Service: RunCoveer Events ingestion and VK event projection
Opened: 2026-11-02
Closed: —
Registry: inc_3d23e0008f860b634306a59b
Related incidents: INC-2026-10-27-vk-import-4o-spend; INC-2026-09-28-lite-fallback-overload; INC-2026-09-24-vk-afisha-publication-recurrence

## Summary and impact

Owner requested an audit after noticing few VK announcements. At approximately
22:00 Europe/Kaliningrad, production was available but event intake was degraded.
This task is an audit, not a production repair. No configuration, data, provider
quota or application release was changed.

## Evidence and timeline

Audit window: local 2026-11-02, UTC+02:00. Queries captured around 19:57–20:01 UTC.
Host: runcoveer-production; container deployment-events-1.
Image: runcoveer/events:guide-private-f9b50fbb9.
Immutable source SHA: f9b50fbb9066e1f85d96d56529a1990256bf2844.
That SHA is an ancestor of fetched origin/main 2b9a653e6727ec780d27fd83b953e19615cb2d02.

- Health ready=true, db=ok, scheduler tick age 14.6s; no reported health issues.
- Root free space 6.5 GiB; SQLite read-only PRAGMA quick_check returned ok.
- File logging enabled at /data/runtime_logs, with active and rotated logs.
- 15 event rows added during the local day: 13 at 00:53–01:19, two at 10:24 and 12:05.
- VK acquisition persisted 226 packets across 82 sources, latest fetched 19:33 UTC.
- Eleven VK auto-import runs processed 132 inbox positions: 106 technical
  failures, six successful imports, 20 rejections; two new events.
  These are processing counts, not a claim of 132 unique valid events.
- Site parse ops_run 11490 processed 261 candidates: zero created, three updated,
  154 unchanged, 104 failed. Earlier ops_run 11464 had 102 failures of 259.
  The runs can contain overlapping candidates and must not be added as unique losses.
- VK terminal job errors: 67 future event projections, all vk_sync_missing_media.
  Sampled event posters were absent or pending review.
- All 15 newly created events had vk_sync status done and stored VK URLs.
  Four publication registry rows were published; the others were missing/ambiguous.
  Stored URLs and done jobs alone are not fresh authenticated wall verification.
- Telegram scanned-message ledger has 13 terminal_error and six
  partial_terminal_error records on the local day; other messages did import.
- Secondary faults: 55 partner KONB video attempts failed selection_empty;
  the scheduled general popular-review and tomorrow-video runs reported success.

VK auto-import history (local days):

| Day | Processed | Technical failures | New events |
| --- | ---: | ---: | ---: |
| Oct 25 | 60 | 14 | 4 |
| Oct 26 | 60 | 17 | 10 |
| Oct 27 | 60 | 19 | 5 |
| Oct 28 | 120 | 87 | 10 |
| Oct 29 | 132 | 117 | 2 |
| Oct 30 | 156 | 138 | 2 |
| Oct 31 | 168 | 150 | 4 |
| Nov 1 | 132 | 104 | 2 |
| Nov 2 | 132 | 106 | 2 |

## Root cause and limits

Of 307 primary provider/schema technical failure tracebacks captured for Nov 2,
302 ended in google_ai.exceptions.RateLimitError: Rate limit exceeded: rpd.
The remaining five ended in provider_429. These are parser invocation counts
across consumers, not unique failed events. Provider logs separately confirm
HTTP 429 RESOURCE_EXHAUSTED on Gemini lanes.

VK attempt ledger includes 102 gemini-2.5-flash TECHNICAL_ERROR attempts with no
completed LLM response. Additional attempts returned malformed structured JSON.
The runtime VK route is gemini-3.1-flash-lite, EVENT_PARSE_LLM unset,
EVENT_PARSE_ENABLE_4O_FALLBACK=0; the provider model chain can choose Flash.
The October 28 routing change is temporally associated with worse throughput,
but the audit does not establish it as the sole cause of exhausted quotas.

The existing VK import path terminalizes an incomplete parser decision as
failed_technical without an automatic retry. A quota reset alone therefore does
not restore those failed rows. This is provider/quota handling, not evidence
that the source posts contained no events.

The separate VK projection backlog is a missing/unapproved media gate.
Its complete media provenance and any publication-registry drift remain unproven.
No authenticated live VK content read succeeded through the connected social
workspace, so no claim about deletion or exact live wall coverage is made.

## Automation contract

### Treat as regression guard when

Changing ingestion model selection, shared quota allocation, source-parse error
classification, VK terminal handling, media approval or VK publication recovery.

### Affected surfaces

VK source packets/inbox/attempts, site parser, Telegram source import,
shared Google limiter, event media review, VK JobOutbox and operational health.

### Mandatory checks before closure or deploy

- Reproduce a quota failure through the existing import boundary without
  semantic non-event conversion; distinguish recoverable provider failure.
- Verify available model/project quota through the shared limiter, never bypass it.
- Replay representative retained source packets through the normal import and
  Smart Update path, with valid-event and non-event controls.
- Reconcile failed rows without duplicate events or public posts.
- Verify exact media provenance for affected future projections and perform
  authenticated wall readback after any authorized publication recovery.
- Meet release governance for any code/config change; prove running-SHA ancestry.
- Verify same-day catch-up and fresh ingestion, not only health HTTP 200.

### Required evidence

Passing replay results, import receipts, provider model/quota receipts, media
and VK readback, release SHA if changed, and remaining backlog counts.

## Immediate mitigation and corrective actions

None: read-only production audit. Incident remains open. No restart or deploy.

## Follow-up actions

- Restore a usable quota-backed parsing route under existing model/budget policy.
- Correct quota-error classification and reconcile affected technical terminals.
- Diagnose and recover the 67 media-blocked future VK projections.
- Investigate publication registry disagreement separately before retrying done jobs.
- Expose actual import success/backlog in operations monitoring.
- Investigate KONB selection_empty repetitions separately from core intake.

## Release and closure evidence

No release performed. Retained local evidence:
 /home/dev/artifacts/events-bot-new/20261102T195715Z-prod-intake-audit-20261102/
Audit is complete; production recovery is not complete.

## Follow-up: quota and routing clarification, 2026-11-02 20:08 UTC

The initial audit's quota explanation was too broad: the keys are NOT all
exhausted. Read-only canonical limiter metadata for Pacific day 2026-11-02 shows:

| Key env | 3.1 Lite used | Registered RPD | Remaining |
| --- | ---: | ---: | ---: |
| GOOGLE_API_KEY | 234 | 500 | 266 |
| GOOGLE_API_KEY2 | 73 | 500 | 427 |
| GOOGLE_API_KEY3 | 16 | 500 | 484 |
| GOOGLE_API_KEY4 | 16 | 500 | 484 |
| GOOGLE_API_KEY5 | 84 | 500 | 416 |
| GOOGLE_API_KEY6 | 80 | 500 | 420 |

These counters are a snapshot, not promises of Google availability. Recent
canonical ledger requests since 18:00 UTC confirm successful 3.1 Lite calls on
keys 1, 2, 5 and 6. Keys 3 and 4 had failed-provider attempts in that window;
their unused registered quota alone must not be described as confirmed health.
No Google diagnostic generation calls were made.

The 302 rpd tracebacks are shared-reservation denials before Google, not 302
provider quota violations. The limiter is blocking exhausted selected routes.
Five other parser tracebacks were provider_429; the exact accounting difference
behind those sparse provider denials is not established.

The central routing defect is now reproduced using pure functions imported
from the actual production container:
- main.py line 10403 replaces the configured Lite model with
  monitoring_text_primary(model), yielding gemini-3.6-flash.
- _generate_once at lines 10315–10329 calls monitoring_text_model_chain again
  with the already replaced model.
- With MONITORING_TEXT_RESERVE_MODELS=gemini-3.6-flash,gemini-2.5-flash,
  the actual chain becomes only [gemini-3.6-flash,gemini-2.5-flash].
- Passing the original configured Lite model would instead preserve
  [gemini-3.6-flash,gemini-2.5-flash,gemini-3.1-flash-lite,gemini-3.5-flash-lite].
- Both selected Flash models have registered RPD 20, while 3.1 Lite has 500.
  The parser loses the configured models with available quota.
- This is not the previously fixed admission/send-budget bug: the deployed
  client already subtracts quota_admission_blocks when enforcing the send cap.

Production still unchanged. Correct the chain construction at its existing
boundary, replay source packets, and recover the affected import backlog under
the existing shared limiter. Never bypass the limiter or raise quota counters
without provider evidence.

## Expanded owner-requested model review packet

[Google catalog, usage, competing hypotheses and minimal recovery choices](../2026-11-02-events-ingestion-model-review.md).

Prepared for external ChatGPT Pro analysis only; no Pro task or production change was launched.

## Continuation, 2026-11-02 20:48 UTC

Owner explicitly requested correction as well as the Pro packet. Root classified
as mechanical model routing. Main parser now constructs the chain once from
the configured caller model, then selects its effective primary. No additional
fallback, worker or queue. Net production lines +6/-5.

Distinguishing caller regression: all three configured models failed before
the fix and pass after. Parser/client/Lite quota suite: 110 passed. Provider
path audit: unapproved=0, allowlisted_debt=0. Registry readback already matches
all finite positive owner-snapshot quotas; numerical quota mutation unnecessary.
The owner's 40/20 is a 28-day peak against current limit, not a verified current
quota-day bypass. Production remains unchanged at this source checkpoint.

## Continuation: regular intake recovery, 2026-11-03

The Pro critique identifies remaining mechanical defects after parser PR #761.
Smart Update now builds each stage's complete model chain before choosing the
effective primary, preserving its configured facts fallback. Smart Update and
topic classification retain a two-provider-send cap independently of emergency
routing configuration; collection adjudication retains its one-send policy.
RunCoveer Compose clears the emergency Flash-first override, restoring Lite-first
routing without changing quota counters or the shared limiter contract.

Explicit RPD denial without Retry-After exits the short inline wait immediately.
The VK slot watchdog recognizes a failed batch as attempted only when its existing
metrics prove every processed row terminal, no unresolved rows and no cancellation.
Crashes, missing metrics and incomplete batches remain eligible for catch-up.
This does not recover failed posts automatically or relax identity decisions.

Distinguishing regressions on the previous source: 10 failed, 4 passed. The
fix introduces no worker, queue, persisted status, identity retry or quota bypass.
Offline Google provider audit: unapproved=0, allowlisted_debt=0; prompt audit passes.
Deployment and finite source-packet catch-up remain pending at this source checkpoint.
The active scheduled Telegram import (ops run 11549) continues on the previous
qualified image and must finish before the Events-only cutover. Follow-up checks
use native Codex wake after 3 hours, then 3 hours, then 5 hours, as requested.

## RunCoveer delivery, 2026-11-03 01:57 UTC

PR [#763](https://github.com/onedayonemasterpiece/events-bot-new/pull/763)
merged with all CI checks green; local affected suites: 284 passed. Production
now runs exact merged main `c6d12178c2eabdb6906c3c239e47c12230632ca4`, image
`runcoveer/events:intake-regularity-20261103`, image ID
`sha256:c2089696d6165bf798271c89fabb7e308836abd5f881848c60a68ddd0e1c56d1`.
All 4,427 tracked source files verified. Isolated preflight HTTP 200; real
negative parser canary through the shared limiter fell back from 3.1 Lite to
3.5 Lite and returned CONFIRMED_NO_EVENT with zero events.

Cutover waited for normal Telegram run 11549 to finish at 01:47:39 UTC:
82 messages, 44 created and 22 merged events, 32 terminal-message errors, partial.
These are previous-release baseline results, not evidence of the new release's
normal intake performance. Events-only Compose preserved Kotopogoda container
`bc2f7488d7e1a59201869be0af0f59841accdabd2ece24c4ad74479fc0e5e15a`.
Actual running SHA, empty emergency reserve override, DEPLOY_PREFLIGHT=0,
SQLite quick_check=ok, readiness and scheduler all verified after cutover.
Previous Events image and original Compose are retained for rollback.

Retained delivery evidence:
`/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/`.
Guarded finite replay of inbox IDs 86941, 86953, 86986 and 87005 uses the existing
source-packet parser, Smart Update and outbox boundary; recovery receipt and
normal scheduled-slot checks remain pending. Incident stays open.

### Finite replay 11553: routing repaired, semantic terminals remain

Replay completed at 02:05:30 UTC: four processed, four technical terminals,
zero created/updated events and zero import links. Positive source parses now
succeeded on 3.1 Lite; there were provider 500/503 errors, not just quota issues.
IDs 86941 and 86953 stopped at `distinct_not_grounded`; 86986 stopped at
`shared_long_event_anchor_requires_retry`; negative-control 87005 remained
`VERIFICATION_TECHNICAL_ERROR`. No negative-control event was created, but this
is not a successful typed no-event decision. Failed recovery is not closure.

The existing `event_identity_decision_log` proves a specific prompt/contract
defect: decisions 18882 and 18886 have confidence 1.0, relation `distinct_event`,
grounded quotes and concrete differences, but raw reason `no_candidate_match`.
The unchanged guard reserves that code for uncertainty. For 86941 the model
correctly distinguished a concert from exhibition 10271, yet selected the
uncertainty code. A six-line clarification in the existing prompt maps proven
different programmes to `distinct_show_keep`, sessions to the existing session
codes and genuine uncertainty to `unknown/no_candidate_match`. No acceptance
guard, threshold, matching algorithm, queue state or retry policy changes.
Read-only live decision replay of the concert now returns FINAL_DISTINCT with
the same evidence and correct code. Identity regression suites: 85 passed.
This prompt correction still requires release and source-to-event acceptance.

### Prompt follow-up delivered and accepted, 2026-11-03 02:28 UTC

PR [#764](https://github.com/onedayonemasterpiece/events-bot-new/pull/764)
passed all CI checks and merged as `343826c80998e84c6c20adda3d57ddd47c6008dd`.
RunCoveer now runs that exact source with image
`runcoveer/events:intake-adjudication-20261103`, image ID
`sha256:3e4b90c13338b348cc453cf4663c81556a68928f06e89b1b680584449bf87cc1`.
Complete source verification, isolated startup/SQLite check, actual runtime SHA,
readiness, database and scheduler passed; Kotopogoda was preserved.

Three real decision-only controls passed through the shared limiter: concert vs
exhibition and networking vs workshop both FINAL_DISTINCT; actual same-event
control FINAL_MATCH. All existing final-result/grounding guards remained active.

Guarded recovery 11554 completed at 02:27:45 UTC with success: both exact carrier
rows imported, no technical failures, new Events 10342 (concert, November 15,
15:00) and 10343 (scrapbooking workshop, November 4, 12:00). Inbox import links
and canonical event_source links match both original VK URLs. Exact stored parse
replay preserved the successful earlier source parses. Child outcomes balance:
CREATED for 86941, CREATED plus CONFIRMED_PRODUCT_EXCLUSION for 86953.

The networking child is not counted as recovered: candidate 25812 ended with
`missing_location` despite a populated venue at entry, requiring separate scoped
location-review analysis. IDs 86986 (long-event anchor) and 87005 (incomplete
typed negative decision) remain technical terminals; no blind replay or guard
relaxation was applied. Follow normal 04:15 UTC VK intake and these residual
failure classes with native wakes at +3h, then +3h, then +5h. Incident remains open.

### First scheduled follow-up, 2026-11-03 05:33–05:36 UTC

Runtime remains 343826c80, readiness/database/scheduler healthy. Normal parser
11555 is still progressing: 51 new events since 02:30, all bound to parser
sources; candidate ledger has 51 CREATED, 2 MERGED, one active attempt.
VK 11556 skipped at 04:15 due to source_parsing heavy lock. Existing watchdog
launched 11557 at 04:47:21, but it is waiting for that lock with no claimed inbox
batch. Normal VK delivery remains pending; do not count its running status as
processed work or attribute the 51 parser events to VK.

Provider telemetry 04:47–05:36: 105 successes, Gemma 42x500/4x503, 3.1 Lite
19x503, seven output-truncated failures each on 3.1 and 3.5 Lite; no structured
quota/block events or provider429 in that window. No blind quota mutation or
live restart during the progressing normal import. Remaining networking
missing_location is an explicit LLM grounding rejection, not a cleared venue
field or quota denial; its correctness needs separate semantic evidence.
Next native check +3h, then +5h; retain the normal Telegram follow-up as needed.


### 2026-11-03: confirmed whole-catalogue starvation

Normal source parser ops11555 held the job-wide shared heavy gate from 02:30
to 07:49:11 UTC (262 events, 89 created, 2 updated, 15 failed). Scheduled VK
04:15 skipped; watchdog11557 waited from04:47 until the parser finished. Actual
VK post claims began07:49, ruling out a permanent deadlock. This is an independent
coordination defect: one catalogue reserves the shared resource for5h19.

The correction moves the existing heavy gate to each source event and uses the
existing PARSE_LOCK for all parser producers. Remove source jobs from the
scheduler whole-job heavy set, including its optional serialization mutex. No
new queue, worker, retry policy, semantic override or concurrency increase.
Regression proves a queued VK operation cannot enter during a source event but
runs before the next event, with either scheduler serialization setting. All six
new/updated regression cases fail against the old production code and pass after.

Normal VK11557 finished partial07:58:50:12 carriers,5 rejected,7 technical failures,
0 created/updated. Provider503, an actual3.1Lite RPD429, unavailable adjudication,
and primary OCR insufficient_quota are separately observed; lock fairness alone
is not import acceptance. Evidence retained under the incident artifact fairness/
subdirectory. Incident remains open pending release and actual successful imports.

OCR-specific mitigation:19 primary fallback calls to3.1Lite returned503 in the
normal VK packet, while3.5Lite text parsing succeeded14times. Existing
POSTER_OCR_GOOGLE_FALLBACK_MODEL set to3.5Lite in the qualified release; actual
synthetic poster OCR canary read CONCERT/15 NOVEMBER2026/15:00 correctly through
the atomic limiter at08:04UTC. No new fallback/retry/send; one-call OCR retained.
