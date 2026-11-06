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


### Fairness release delivered; fallback ordering follow-up

PR #766 merged as 14d38eaf3ae7902e56f019af009fb988326ea0cc; all CI checks passed.
The image passed full verification of 4,427 tracked files, isolated HTTP startup
and SQLite quick_check. The Events-only cutover started at 08:14 UTC after all
imports finished. Runtime source ancestry was verified. Image:
`intake-fairness-20261103`, immutable ID:
`sha256:97db700993faf6e5d7d84bb650b8b10ac9c3c1ad1b8923aea65e39dc453ca80b`.
The empty reserve override remains; OCR fallback uses Gemini 3.5 Lite.
Production readiness, database and scheduler passed. Kotopogoda was unchanged.
Rollback image: `intake-adjudication-20261103`.

The actual old GOOGLE_AI_FALLBACK_MODELS listed 3.1 Lite and Gemma. The sibling
chain contains 3.5 Lite, but Gemma stages consume the two-send budget on Gemma
and 3.1 Lite during the observed outage, before reaching healthy 3.5 Lite.
Reorder this existing setting to 3.5 Lite, 3.1 Lite, Gemma. No additional calls
or recovery path are introduced. Three real decision-only controls at
08:13–08:14 UTC passed: concert versus exhibition and networking versus workshop
returned FINAL_DISTINCT; the same-event control returned FINAL_MATCH.
Gemini 3.5 Lite answered after Gemma failures. No events or queues were changed.

Boot completed at 08:15:00 UTC, crossing the 08:15 cron boundary. Check the
existing watchdog for actual recovery of that slot. Its configured 30-minute
misfire grace plus 120 seconds makes this slot eligible around 08:47 UTC.
Health alone is not proof of normal import acceptance. The incident stays open.


### Normal watchdog run 11565 and remaining latency

The watchdog automatically dispatched run 11565 at 08:47:41 UTC. Actual claims
started immediately. By 09:27, carrier 87068 had imported and several other
carriers had confirmed no-event decisions. Carrier 87482 failed two children
with merge_identity_blocking_conflict; its semantic correctness still needs
source-level analysis and is not counted as a valid exclusion.

Carrier 87457 verified a programme with 12 children, then reached the existing
1,800-second ROW_TIMEOUT. Before cancellation the candidate ledger accepted six
children: one CREATED as Event 10433 and five MERGED as Events 9757, 9758, 9759,
9760 and 9762. All six have canonical event_source links to the actual VK URL.
Carrier-level import links are absent after timeout, so aggregate carrier metrics
understate these durable writes; the remaining children are not recovered yet.

A separate mechanical defect was found in topic classification: it constructed
the shared model chain without the configured fallback list, then explicitly
passed the resulting empty tail to the Google client. Production logs show only
Gemma topic calls despite an available 3.5 Lite reserve. Restore the existing
list at that call site, preserving the two-send limit and unchanged prompts.
The distinguishing native-schema regression fails before and passes after.
The topic, fallback safety and VK suite passed 98 tests.

Captured provider time across the observed runtime window, including background
work, was about 400 seconds for topics and 620 seconds for image geometry on
Gemma. These totals must not all be attributed to the timed-out carrier. A real
3.5 Lite geometry canary passed, but its model is not changed here: geometry
cache identity includes the model, so a global switch would invalidate cached
results and cause additional reclassification. No timeout increase or new retry
worker is introduced. Further recovery and final normal-batch receipts remain
outstanding.


Run 11565 finished partial at 09:29:17 UTC: 12 carriers, 1 imported, 8 confirmed
no-event, 3 technical failures. Its normal completed import created Event 10434
from carrier 87068. The other two failed carriers, 87482 and 87120, have identity
blocking conflicts; 87457 has ROW_TIMEOUT. Six accepted children of 87457 are
additional durable writes outside the final carrier metrics and must remain
separate from the reported one completed import. Two real topic controls on
3.5 Lite returned CONCERTS and LECTURES/SCIENCE_POP correctly through the shared
limiter. This confirms normal automatic delivery is active, with residual
failures still open; it is not full recovery acceptance.


### 10:00 normal slot: retryable 503 exposed a second topic budget issue

Run 11566 started on schedule and was still active at 10:27 UTC. Its source-bound
update to Event 10251 is verified. An actual topic request at 10:22:31 received
503 on Gemma, then its second attempt at 10:23:12 still called Gemma. PR #768
restored the configured model list, but the topic client's default per-model
retry count still consumed both sends before that list could be used.

Set the existing topic client's max_retries to one, matching Smart Update. The
shared client's fallback loop, atomic limiter and total two-send cap stay intact.
A regression using the real topic factory and shared fallback loop with a stubbed
provider 503 fails before and passes after. It asserts exactly two attempts:
Gemma then 3.5 Lite. All 126 topic, shared-client and fallback-safety tests pass.
No new retry mechanism, call budget, topic prompt or schema is introduced.
The actual import continues; no concurrent programme replay has been launched.


### PR 769 delivered and verified, November 3 at 10:53 UTC

Normal 10:00 run 11566 finished partial at 10:29:13: two imported carriers,
seven confirmed no-event and three technical failures. Source-bound receipts
prove Event 10435 was created and Event 10251 updated. The timed-out programme
87457 has not been replayed. Its old 11:20 recovery window was missed.

Qualified merged main fd4da1b81550d4a9633a22ba173311a95a7138bc is now running
as intake-topics-retry-20261103. Actual source, selected model settings and
SQLite quick_check passed explicit runtime checks; ready=true and scheduler
healthy. Kotopogoda stayed unchanged. The topic factory now permits one attempt
per model so retryable 503 can reach Lite within the unchanged two-send cap.

Our initial all-ops deployment guard delayed the release for partner video
selection. Inspection showed ops 11567/11569 had no session, render or
publication yet. Existing startup cleanup and same-day partner watchdog support
a repeat of this pre-session selection. The Events-only switch allowed exactly
those two operations after checking that no video session had appeared and no
other operation was active. Startup marked both crashed as expected. Automatic
selection restart is still to be verified; this deliberately loses the in-memory
selection pass, without interrupting an active source import or remote render.

Production Python optimization disables assert. An earlier retry-release
diagnostic and programme read-only plan therefore did not enforce assertions;
they are retained as invalid acceptance evidence. No programme apply occurred.
New release verification and programme guards use explicit if/raise checks, and
a negative source control rejects under the same optimized interpreter.
Evidence: retained phase2/verification-correction.md,
phase2/live-release-topics-retry-verified.json,
phase2/live-health-topics-retry-verified.json and phase2/video-restart-decision.md.

The next proposed finite replay window is after terminal 11:20 guide work and
the 11:22 promo slot, before the 12:15 source parser. It still requires no active
ops and 30 minutes plus five minutes available, unchanged frozen source and no
existing recovery run. If no idle window exists, defer instead of overlapping
process-local heavy semaphores. Keep the 14:30 verification target, parser/VK
interleaving coverage and normal TG at 21:40. Incident remains open.


At 11:05 UTC, automatic partner restart is verified: watchdog started eco run
11571 at 11:02:30; fresh heartbeats and provider calls confirm active selection.
No new render session exists yet. KONB 11572 ended selection_empty; this is a
separate partner selection result, not VK intake. Runtime remains fd4da1b8 and
health is ready. Evidence: retained phase2/partner-resume-1105.log and
phase2/health-1105.json. Programme recovery is still not launched.


### Normal13:30 UTC slot still skipped at per-event gate, November3

Actual ops11591 is `vk_auto_import / scheduled / skipped` at13:30:00 UTC.
Runtime logs show `job_skip_heavy_busy` against a just-started source-parser
event; parser11581 then progressed from index202 to203. The whole-catalogue gate
fix allows fairness between events, but normal scheduler registration still
used the wrapper's non-blocking skip default. It never entered the waiting
pipeline. Existing watchdog can retry only after the30-minute misfire grace
plus120 seconds, around14:02 UTC; skipped is not normal import acceptance.

Use the already-supported `heavy_guard_mode="wait"` for this registration only.
No new retry, queue or global heavy-mode change. The distinguishing test boots
actual registration with two independent tasks and the real semaphore: baseline
skips, fix waits then runs after event release. Initial test creation accidentally
inherited the parent's reentrant context and is invalid evidence; use retained
vk-slot-wait-before-corrected.log / after-corrected.log. All initial files remain
retained. Production stillfd4da1b8; fix is not deployed, and healthy parsing is
not interrupted. Verify actual watchdog import and source/child receipts rather
than counting the skipped slot as successful. Other owner tasks, spacing fix
and cinema prevention remain active.


## Combined release delivered, 2026-11-03 13:58 UTC

Actual RunCoveer Events runtime is now `7d03b15853d3548299aa334418a9a907ae415312`,
image `intake-guide-promo-spacing-20261103`, immutable
`sha256:96896a2b00717740524948c4b49caedc4b6f2a96e6dba4bf23c5482789ade566`.
PRs #777 and #778 passed all required CI before merge. Source parser completed
261/261 before cutover; the final guard found no active ops. Events alone was
recreated. Kotopogoda container ID remained unchanged. Explicit runtime/model
settings checks, SQLite quick_check, effective cinema reference and identity
function hash passed. Live health reports ready=true, database/scheduler ok,
no issues. The existing promo interval is five minutes.

Evidence is retained in the intake recovery directory under
`promo-spacing-cutover.log`, `promo-spacing-live-verification.log`,
`promo-spacing-live-health.json` and `promo-spacing-startup.log`.
This supersedes earlier not-deployed candidate status; it does not close the
incident or substitute for actual scheduled source receipts.


## Normal automatic VK recovery verified, 2026-11-03 14:55 UTC

Scheduled ops11596 (watchdog recovery of the missed13:30 UTC slot) completed at
14:51:04 UTC with status `partial`: 12 processed, 4 imported carriers, 7 rejected
carriers, 1 technical failure, 12 terminal and 0 unresolved rows. It created
Events10447,10448,10449 and updated7233,9356,9358,9359. Source receipts bind
10447 to wall-30777579_16353,10448 to wall-53460968_12101,7233 to
wall-388318_20692. Programme carrier87644 / wall-148784347_7013 resolved all six
children: two product exclusions, one CREATED10449, three MERGED9356/9358/9359.
Carrier and source-attempt receipts agree. The old9501→6554 July link is not a
new success. Cumulative timing-log ID arrays are not per-row attribution.

This accepts automatic slot recovery and real pipeline progress, not a fully
error-free batch. Carrier87615 / wall-9118984_25569 remains technical with
`identity_final_retry:distinct_not_grounded`. Decision log19054 shows the model
returned create/no_candidate_match/relation unknown, despite explicit date
conflict evidence and confidence1.0. The existing contract intentionally rejects
that combination; do not bypass it or infer a venue-normalization defect.
Candidate25953 is preserved without an accepted owner. Diagnostic model-only
readback does not count as event recovery or source-pipeline acceptance.

Normal promo ticks preserve today's two original URLs and receipts without
reposts. Actual future spacing remains unverified until the next eligible pair.
Runtime7d03b158 and health remain healthy; no restart. Keep normal VK16:30 UTC,
programme's guarded16:35–18:10 candidate interval and normal TG21:40 UTC.
All three incidents stay open. Retained evidence: vk-11596-terminal-1455.json,
vk-11596-finish-runtime.log and identity-87615-decisions.json.


### Residual workshop identity failure: focused prompt qualification

Production decision19054 and a model-only replay both returned
create/no_candidate_match/relation unknown at confidence1.0 despite citing an
explicit November5 workshop against different programmes on September5 and
November4. The final contract correctly kept candidate25953 unaccepted.
The prompt's instruction to return create whenever no recalled event fits was
ambiguous alongside the uncertainty rule. Replace that instruction with the
explicit existing relation/keep-code contract; do not change schema, grounding,
thresholds, model routing or add a worker. Qualification requires the actual
LLM incident replay and a same-event control, plus existing identity regressions.
This is not deployed and the source has not been recovered by this diagnostic.

Project-required Opus consultation was attempted through the installed gateway,
request events-adjudicator-87615-prompt-review-20261103, task
 dvt_f6cf2d950c764178a2272eafd04d4c94. It failed provider_error / INVALID_ARGUMENT;
there is no completed external review. Retained diagnostic uses production
models only as supplementary probes, never as a replacement consultant review.


The real model-only replay now returns FINAL_DISTINCT / distinct_show_keep with
source-grounded date and programme differences. The same-event control returns
FINAL_MATCH to10343, so the change does not make existing events distinct.
The unchanged production prompt reproduced FINAL_RETRY before the edit.
Fifty-nine existing dedup/identity incident regressions pass. Evidence:
`diagnose-identity-87615.log`, `diagnose-identity-87615-after.log` and
`adjudicator-87615-regressions.log`. These are actual LLM decisions, not event
persistence or recovery of carrier87615. Release qualification and delivery
remain outstanding.


## Narrow adjudicator release delivered, 2026-11-03 15:20 UTC

PR781 passed every required CI check after the image pin update and merged as
48fb4e6ca4b8d9246c10fb706809800eece6a424. Actual Events runtime uses image
intake-adjudicator-grounding-20261103, immutable
sha256:1ecca1e8c1f9dec7d1233609d50401122b991791b91d138e06c7dfb2ef4d94e4.
All4433 tracked files, unchanged dependency inputs and running-release ancestry
passed. Isolated initialized SQLite/HTTP preflight and wrong-SHA negative control
passed; the isolated container was removed. Events-only cutover found no active
ops. Live exact SHA, SQLite, model settings, promo interval5, cinema reference,
qualified identity/adjudicator hashes and readiness passed. Kotopogoda unchanged.

Finite replay of carrier87615 ran once as ops11600,
INC-2026-11-03-workshop-catchup. It ended with no technical error but ZERO events:
the earlier location grounding stage returned missing_location, so source and
carrier became confirmed_product_exclusion, candidate25953 REJECTED_PRODUCT_POLICY.
This is NOT event recovery despite the ops success status. Do not replay the
same excluded packet blindly or weaken grounding. The original evidence remains.

The museum's official creativity-center page confirms the Academy of Creativity
at Leninsky83; its weekly November2–8 page confirms this exact workshop on
November5. The original VK post supplies18:00, consistent with official Art
Thursday timing. An explicitly sourced request was queued through the existing
typed event-create parser and full Smart Update, operation
`evt_op_akEwvpGMvW1elcEwv81fSjp_qcWVEmII`, idempotency key
`INC-2026-11-03-workshop-87615-official-source-v2`. Acceptance and publication are
still pending and require readback. This is an official-source recovery attempt,
not proof the normal VK location-grounding issue is fixed.

Official evidence: [weekly programme](https://kaliningradartmuseum.ru/visits/creativity-center/news/master-klassy-nedeli-s-02-11-po-08-11/)
and [venue](https://kaliningradartmuseum.ru/visits/creativity-center/).
Retained evidence: adjudicator-grounding-live-verification.log,
adjudicator-grounding-live-health.json, workshop-catchup-final.json and
workshop-official-operation.json. Keep normal VK16:30, guarded programme recovery,
normal TG21:40 and November4/6 promo checks; all three incidents remain open.


## Accepted workshop and publication-budget evidence, 2026-11-03 15:53 UTC

The official-source operation was accepted at15:32:35 UTC as CREATED10450.
Source13871860 binds the official weekly page; date November5,18:00 and
Leninsky83 are correct. A date/title duplicate check finds one canonical card.
Original excluded VK carrier87615 remains unchanged: this is official-source
recovery, not successful normal VK location-grounding acceptance.

VK initially failed vk_sync_missing_media because the official request had no
poster. The actual saved image from the original VK announcement was staged and
added through typed edit, auto_approved operation
`evt_op_2ANPZa6ApHhjLZw5NrDim3xRRh7455-y`; poster35040 is attached and approved.
Provider readback proves wall-231920894_13320 exists with one image and
post_type=postpone, scheduled November3 20:20 UTC (22:20 Kaliningrad). It is NOT
live yet. Telegraph and calendar had real receipts; newer revisions still need
reconciliation. A cosmetic full-description edit was rejected by the fact gate;
it made no change. A safer canonical-facts-only description then passed the
same typed gate and was accepted as operation
`evt_op_IswWV5uW8NLo2h1aPWHmuMGHnubrZYJN`, revision
8056bc75bf25ff2771e72a92e2b25d54af9fb220330d8f9c91f6be99137e46fd.
Verify that provider projections receive it; accepted is not published.

A separate existing feature budget explains delayed publication: semantic media
role daily limit150 reserves30 calls for blocked future VK cards, leaving120 for
ordinary reviews. The ordinary120 were used by08:22 UTC; primary review100 by
06:25 and image geometry100 by09:13. At15:45,210 posters were pending with
reason daily_budget_exhausted. Event10450's media follow-up is due November4
00:05 UTC, deferring dependent Telegraph/calendar/TG reconciliation. This is
NOT proof all Google project/model RPD keys are exhausted. Media-role routing
uses3.1 Lite and explicitly forbids fallback under its existing contract.
Do not reset counters, raise caps or invent a fallback to force publication.
Investigate prioritization of current publication work versus backlog within
existing budgets; normal VK grounding false exclusions remain a separate issue.

Retained evidence: workshop10450-acceptance-and-jobs.json,
workshop10450-media-publication.log, workshop-vk-owner-readback.log and
media-budget-1545.json. Add a read-only check after20:20 UTC for actual VK release
and after November4 00:05 UTC for media/dependent receipts, alongside the required
normal VK16:30, TG21:40 and November4/6 promo timing checks. No runtime restart
is needed for this report.


### 2026-11-03 16:05 UTC — cached parse drops media evidence; narrow codec correction

- Reader impact: a multi-event programme can spend the bounded row time and Google quota re-processing already accepted children instead of reaching the remaining events.
- Observed failure: the one authorized programme replay is now ops 11601, run `INC-2026-11-03-programme-catchup`, started 15:53:06 UTC after idle ops, MCP intake and scheduler checks. It reused frozen packet 26103 revision 1, but did not return exact no-op for the first accepted child. Old candidate 25917 retained poster hash/OCR; the cached parse receipt omitted all `poster_media`, and reconstructed candidates instead used a volatile VK image URL without hash/OCR. Evidence: retained `phase2/programme-candidate-payloads-1555.json` and `phase2/programme-1550-apply.log`.
- Root: `DraftParseResult.to_receipt_payload` deliberately dropped the entire poster object. The codec now retains its existing non-binary fields and restores `PosterMedia(data=b"")` on replay. No new queue, retry, state, worker, matching rule or quota change. Legacy metadata-free receipts still load; already-lost evidence is not reconstructed.
- Distinguishing JSON round-trip test failed on the baseline (empty media) and passes after the change, including equal real candidate fingerprints. The existing bytes-exclusion test now checks the actual data field instead of discarding all metadata. 117 contract/import regressions pass in 30.88 seconds. Net production Python +8 lines.
- This is a proposed correction, NOT delivered. The active programme runs the unchanged 48fb4e6c release and must finish without interruption; its 30-minute row deadline is around 16:23, before normal VK 16:30. Final six-child preservation and remaining-child receipts are still unaccepted. Do not repeat the run ID or launch the old programme script. The 37-error plan remains unlaunched.


### 2026-11-03 16:25 UTC — receipt codec delivered; finite programme result

PR [#783](https://github.com/onedayonemasterpiece/events-bot-new/pull/783) passed all four required checks and merged. Exact main `215c6a219edec24551e2d38215cda8ccc396091c` is now delivered as `intake-receipt-media-20261103`, immutable `sha256:fa97c6e09aa6634ceda9575569abd9a6568802f7ab73e7bdaf3d1b40a0f03a72`. Running 48fb ancestry, unchanged dependency inputs, all 4,433 tracked files, initialized isolated SQLite, HTTP preflight, wrong-SHA rejection and preserved cinema/adjudicator controls passed. After the programme process exited and ops/MCP intake were idle, only Events was recreated. Explicit runtime/model checks and SQLite quick_check passed; health ready=true, scheduler/database ok, issues empty. Kotopogoda unchanged. Normal VK16:30 and TG21:40 UTC remain scheduled. Initial health read during startup reset the connection; the subsequent live read is ready=true, not a claim that the first probe passed.

The one finite programme replay ran entirely on the preceding release. Ops11601 finished **failed** at16:22:51 UTC in1784.632 seconds: **zero created, six updated**, one terminal carrier, zero unresolved carrier rows. All six original accepted EventSource anchors survived; carrier links now contain9757/9758/9759/9760/9762/10433. Important correction: before-state `programme-accepted-before-1552.json` is actually an empty carrier-link list; the six prior acceptances were EventSource anchors, not six pre-existing carrier links. No current link deletion was demonstrated.

All12 child outcomes were recorded:6MERGED,4FAILED_TECHNICAL identity_final_retry,2 product exclusions missing_location. Frozen packet26103/revision1/parse key are preserved, leases released; carrier and packet remain failed_technical SMART_UPDATE_UNRESOLVED. This was not a timeout and not a successful programme recovery. The local SSH session exited143 near the end, but canonical ops/source receipts and absence of the remote process established terminal outcome; no repeat was launched. Exact parse receipt reuse succeeded, while exact Smart Update no-op failed because older receipt omitted poster evidence. The new codec prevents future metadata loss; it does not reconstruct this already deficient receipt. Do not blindly run the old catch-up again or alter semantic exclusions.

The original hosted poster was downloaded and visually inspected. It explicitly names the venue/address and November14 Melodies of Love; adjudicator compared an existing November13 card without the omitted OCR. Remaining candidates include other events already present in active cards from other sources: November20 IDs9763/9778/9817/9938, November21 IDs9764/9818/9862, November26 IDs9765/9819/9821/9939 and November28 IDs9820/9822/9863. Some same-slot active cards suggest legacy duplicates requiring provenance checks, and some dates/titles contradict the new poster (9775 November13,9815 November14,9779 November27). This calls for bounded provenance reconciliation, not creating six more cards or counting six missing events. No merge/delete/retime was performed in this check.

Observed provider calls in the retained replay log: Lite3.1 27success/4HTTP503, Lite3.5 23success, Gemma16success/17HTTP500/2HTTP503. At least101,563 successful-call tokens were observed; the lost local session means this log may omit its tail. These are not daily totals or money estimates. No captured429/all-key RPD exhaustion supports a quota diagnosis for this run.

Evidence in retained intake directory: `receipt-media-build.log`, `receipt-media-qualified-image.txt`, preflight source/health/negative/cleanup logs, `receipt-media-cutover.log`, `receipt-media-live-verification.log`, `receipt-media-live-health.json`, `phase2/programme-receipts-1624.json`, `phase2/programme-provider-summary.json`, `phase2/programme-existing-statuses-1625.json`, `phase2/programme-original-poster.webp`. All three incidents remain open. Next verify actual ordinary VK16:30 source/child/carrier receipts and normal promo tick; later VK10450 release20:20, normalTG21:40, media reset November4 00:05, daily promo November4 and paired promo November6. No public canary, quota bypass or old37-row recovery.


### 2026-11-03 16:46 UTC — ordinary VK verifier creates a doors/start duplicate

Normal scheduled ops11603 started16:30 without watchdog/manual trigger. New packets26206 and26207 retain poster digest/OCR in receipts, proving the deployed codec on actual normal input. Carrier87696 failed location_grounding_review:llm_repair_value_not_grounded: truncated embedded clip caption contains no explicit venue and OCR only VIBECITY.RU; do not force the group-default location.

Carrier87717/source26207 is a verified new defect. Source wall-149955604_25378 explicitly says one Nov6 Trio Sveta concert, Bar Bastion/Sudostroitelnaya6/1, doors20:00, performance21:00, same ticket270191. Verifier returned two children at20 and21. First MERGED10418; second CREATED10451 as unrelated_titles after the canonical first child had been retimed to21. Two carrier links/source receipts confirm the duplicate. This CREATED outcome is not successful intake acceptance. New duplicate public VK URL13329 and Telegraph exist; public release not yet established.

Local collector replay proves address6/1 is read as a second possible date and triggers MULTIPLE_OCCURRENCES_COLLAPSED occurrences2/events1. The verifier turned an uncertain hint into a split requirement. Semantic root: conditional-verifier instructions omit schedule-anchor roles and over-emphasize splitting. Proposed minimal LLM-first correction replaces the existing generic rule in the shared application builder and Telegram verifier adapter: hint counts are hypotheses, address numbers are not schedule dates, admission/performance are one occurrence, distinct attendable sessions/dates remain separate. No deterministic semantic collapse, new stage, retry or queue. Actual model controls are required before delivery.

Historical regression contracts read: April20 Club Znakomstv doors/start duplicate and May11 Bastion semantic duplicate. Opus provided-only prompt review task dvt_c9649515f8f5472f969017b7a9b26e81 failed provider_error INVALID_ARGUMENT; no external review completed. Bounded existing manifest repair10451->10418 is being prepared; no blind provider delete or source replay. Original three objectives continue. Evidence vk-trio-duplicate-evidence.json, trio-verifier-trigger-reproduced.json, trio-fix-plan.md.


### 2026-11-03 16:55 UTC — Trio mitigation and real verifier controls

Existing manifest-driven repair safely merged10451 into10418, retained canonical sources/media, rewired accepted owner and cancelled obsolete pending jobs. First stale manifest dry-run rejected changed job semantics; no mutation occurred on that plan. A fresh pinned manifest then passed dry-run and applied once. Immutable manifest receipt a9689240adef467075c4594dcea7fe60edf780d876eca30ed46efca09deff2fe is retained under the incident runtime directory. No whole programme replay or normal-import interruption was used.

Duplicate calendar message10364 was removed through existing projection invalidation; Telegraph duplicate now redirects to canonical `Koncert-Mirovye-hity-rok-n-rolla-i-dzhaza-11-03`. Admin readback found duplicate VK13329 postponed to November4 04:40 UTC. Exact owned duplicate deletion returned provider response1, preserving canonical13316; readback verification still pending.

Real Lite3.1 verifier before/after: existing rule reproduced two children (20:00 doors plus21:00 performance); proposed rule returned one child21:00. True separate sessions18:00/21:00 remain two, and two distinct dates November6/7 remain two. These are actual source-parser provider probes, not external consultant review or production Event writes. Real provider controls qualify the identical rule carried by both application and Telegram verifier adapters. 175 source/intake/Telegram regressions,8 prompt-contract checks and2 isolated generated-notebook import checks pass. Generated Telegram notebook was synchronized using the existing builder, also refreshing already-merged script changes in its previously stale checked-in snapshot.

A full isolated VK persist/real Smart Update replay is running against a tiny shadow database seeded with only canonical10418 and its prior sources; current-source acceptance is excluded to avoid a false no-op success. External projection effects are deferred and the shadow has no worker. No production Event changes are made by this replay. Its final outcome is still pending; do not claim full boundary acceptance from the model-only controls. The prompt fix is NOT deployed, current runtime remains215c6a219. Ordinary VK11603 is progressing and must finish before any cutover. Evidence trio-repair-applied.log, trio-cleanup-public.log, trio-delete-vk.log, trio-verifier-before/after/sessions/dates.log, trio-shadow-replay.log.

### 2026-11-03 17:33 UTC — normal slot terminal; verifier prevention delivered

The ordinary 16:30 UTC VK slot completed at 17:25:44 without a watchdog or manual trigger. Ops 11603 is partial: 12 terminal carriers, zero unresolved, three imported carriers, four rejected and five technical. Its raw metrics report nine CREATED and one updated event, but one creation was the already mitigated duplicate 10451. The eight remaining new cards are 10452–10459; 10418 was updated. Candidate, EventSource and carrier receipts agree. The historical carrier link to merged 10451 remains a raw reference and must not be counted as another canonical event.

Festival carrier 87712 / packet 26194 resolved seven children into five CREATED cards 10454–10458 and two technical failures: identity_final_retry and occurrence_scope_review:llm_unavailable. Donor carrier 87716 / packet 26199 accepted CREATED 10459 and a source anchor, but its carrier terminal is EVIDENCE_INCOMPLETE after OCR timeouts. This is partial durable acceptance, not a reason to replay it or a claim that all carriers passed. Other technical carriers are 87696 (ungrounded location), 9508 and 87708 (incomplete typed verification). No timeout, quota or semantic guard was weakened.

The full isolated Trio boundary replay finished successfully: the actual after-model parse went through real VK persistence and Smart Update, MERGED only into 10418 with a new source receipt, zero CREATED. External projections alone were deferred. Duplicate VK 13329 was read back absent; calendar 10364 was read back message_not_found and the duplicate Telegraph page links to canonical. This supersedes the earlier pending statements.

PR [#785](https://github.com/onedayonemasterpiece/events-bot-new/pull/785) passed all four required CI checks and merged. Exact main 78282649624fdcea714c75d2b89ec915ea235fb5 is now running as intake-occurrence-verifier-20261103, immutable sha256:d3ec7e74bd1480a23853342e039e1c92373e92338d0a34e551abcd35acb90415. All 4,433 tracked files, unchanged dependency inputs, running-release ancestry, initialized isolated SQLite/HTTP preflight and wrong-SHA negative control passed before delivery. Cutover waited for normal import completion, idle ops, MCP intake and heavy scheduler work; only Events was recreated. Actual runtime, SQLite quick_check, model settings, empty reserve override, five-minute promo interval, cinema reference and qualified identity/adjudicator hashes passed. Live readiness, DB and scheduler are healthy; Kotopogoda is unchanged. No new build or second cutover is needed.

Evidence in the retained intake directory: vk-11603-progress-1731.json, occurrence-verifier-final-idle.log, occurrence-verifier-cutover.log, occurrence-verifier-live-verification.log, occurrence-verifier-live-health.json and trio-shadow-replay.log. Normal post-fix Telegram acceptance at 21:40 UTC and the next ordinary VK slot remain unverified. Keep Event 10450 public VK check after 20:20 UTC, media review after November 4 at 00:05 UTC, daily promo November 4 and paired timing November 6. All three incidents remain open.

### 2026-11-03 17:40 UTC — remaining default-parser route drops configured reserve

After normal VK terminal and delivery of PR 785, one separate source request for November 14 Melodies of Love was submitted through typed create with the verified organiser poster. Operation evt_op_GCkCPuxEKAshINioeccanoS0HFGm1G1g ended failed / SOURCE_PARSE_RETRY_REQUIRED, zero candidate receipts and zero Event writes. Do not recommit this operation or claim recovery. The November 27 request has not been prepared or committed. Existing programme packet and prior six acceptances were untouched.

Actual parser request at 17:33:45 received Gemma HTTP 500. The default caller built monitoring_text_model_chain(model) without GOOGLE_AI_FALLBACK_MODELS and explicitly passed an empty tail, suppressing the configured Lite reserve. This is the same mechanical omission already corrected for topics, not a semantic extraction failure or all-key RPD exhaustion. OCR separately exhausted three primary OpenAI attempts and its existing Lite fallback timed out; that outcome remains explicit evidence incompleteness.

The proposed correction passes the existing configured list into the same chain builder, exactly as topics does. Net production Python +2 lines, unchanged min(2, len(chain)) physical-send cap, one attempt per model and no hidden SDK retries. No prompt, schema, identity rule, quota, state, worker or queue change. The real factory/shared-client regression reproduces Gemma 500 then Lite 3.5 success in two sends; the negative variant fails after exactly two sends without calling the third model. Both cases failed against the old route. All 104 parser, VK, topic and fallback regressions pass; provider audit reports unapproved=0 and allowlisted_debt=0. Two old assertions were aligned with the already existing canonical model-ID normalization and pass against the qualified baseline function too.

This route change is NOT DELIVERED. Actual production remains 78282649624fdcea714c75d2b89ec915ea235fb5. Evidence: postdeploy-programme-1735.log, parser-fallback-before.log, parser-fallback-final.log and parser-fallback-provider-audit.log. Publication checks, ordinary Telegram and future promo timing remain required.


### 2026-11-03 17:55 UTC — configured parser reserve delivered

PR #787 passed all required CI and contract tests, then merged as 292334719d86fe14a06ce3a6cc8cb8fc970e81c4. Actual runtime now uses that exact source, image intake-parse-reserve-20261103, immutable sha256:6a0555e01949e189278ce0b8d285512495099eeaca6f3771b732f60ac6ea109e. All 4,433 tracked files, unchanged dependency inputs, actual running-release ancestry, initialized isolated SQLite and HTTP preflight passed. Wrong SHA rejected with the expected RuntimeError under optimized Python. The first preflight HTTP read arrived before the listener opened; the later HTTP response is ok with ready=false/DB skipped as expected when jobs are disabled. Preflight container stopped and removed.

Events-only cutover found no active ops or MCP intake; untracked heavy scheduler work was checked. Explicit live runtime, SQLite quick_check, selected models, empty reserve override, promo interval five, cinema reference and qualified identity/adjudicator hashes passed. Live health ready=true, database/scheduler ok, issues empty. Kotopogoda unchanged. Do not repeat the previous cutover. Evidence: parse-reserve-build.log, parse-reserve-qualified-image.txt, preflight source/health/negative/cleanup logs, parse-reserve-cutover.log, parse-reserve-live-verification.log and parse-reserve-live-health.json in the retained intake directory.

The original separate programme request remains terminal failed with zero writes. A new, scoped text request was committed after delivery: evt_op_YF_Y7Jw4kWX08-Iuml8HMNNYwexMYc14, idempotency key INC-2026-11-03-programme-source-melodii-20261114-v2. It quotes just the November 14 event from the visually inspected current organiser poster, without attaching the whole twelve-event programme during single-event parsing. This follows the same existing text-create then typed-media-edit flow used for workshop 10450; no frozen receipt was changed and no identity/grounding decision bypassed. It is queued/processing, not accepted or published yet. Add the original poster only through existing typed edit after acceptance. The November 27 request remains unsubmitted. Do not recommit either key blindly. Continue normal guide 18:10, public VK 20:20, normal TG 21:40 and November 4/6 media/promo checks.

### 2026-11-03 18:00 UTC — VK source query shadows SQL constructor

The live post-release request proves configured failover works: Gemma HTTP 500 at 17:55:09 was followed by successful Lite 3.5 response at 17:55:25, within two sends. Conditional verification also completed and the parser returned one event. Execution then failed before Smart Update at main.add_events_from_text line 18109: the source-string parameter `text` shadows the imported SQLAlchemy constructor `text()`. This affects direct VK-source intake, not the separately implemented normal VK inbox path.

Operation evt_op_YF_Y7Jw4kWX08-Iuml8HMNNYwexMYc14 initially became outcome_unknown. Canonical readback found no operation candidate, no domain receipt and no new matching Event. The actual trace identifies the exception at the source-context SELECT, before the first Smart Update call. A bounded operator reconciliation changed only this exact ledger row to failed / EVENT_CREATE_PREWRITE_FAILURE_VERIFIED, guarded by actual runtime, action digest, expected unknown status, absent domain receipt and absent candidates, with a compare-and-set. No Event or JobOutbox mutation or request retry occurred. Before/after receipt is programme-melodii-v2-reconciliation.json; preserve it and do not recommit either previous key.

The proposed fix replaces only that SQL constructor with the already imported `sql_text` alias, as other queries in the same module do. Net production lines zero, one-token substitution, no prompt/schema/identity/model/quota/retry/queue change. The existing real executor/Smart Update/JobOutbox regression now includes a VK URL: baseline raises the same TypeError while the external-ID case passes; after the alias correction both pass. All 62 create/source-decision/parser assertions pass. This change is NOT DEPLOYED; actual runtime remains 292334719d86fe14a06ce3a6cc8cb8fc970e81c4. November 14 is not recovered, November 27 has not been submitted. Normal guide 18:10, public VK 20:20, normal TG 21:40 and future media/promo checks remain active. Evidence vk-source-sql-before.log, vk-source-sql-after.log, programme-melodii-v2-final.log and programme-melodii-v2-canonical-check.json.


### 2026-11-03 18:59 UTC — ordinary guide remote terminal, local import continues

Scheduled guide ops 11605 remote scan completed at 18:56:54 UTC: all 22 sources visited, 60 posts scanned, 22 occurrences extracted. The report is partial: ruin.keepers posts 9437 and 9432 failed with provider HTTP 503; these are not proof of all-key RPD exhaustion. Post 9437 also had a separate media HTTP 404. Local download completed and ordinary occurrence/profile enrichment is progressing, including successful Gemini 3.5 Lite fallback after Gemma HTTP 500 and TPM rejection. Local ops remains running with no final metrics: remote done is not terminal import or publication acceptance.

Actual production remains healthy on 292334719. Fully qualified PR 789 image 9f14c503f977b467ed6ad963e2de59008bc0a435 / immutable sha256:8b8875452f667264b279f0a10459b92e3556d57b6ec2a02ccebf72590ef51e24 has not been switched. Preserve local import and auto-publication until terminal; then verify source outcomes and perform the prepared Events-only cutover in idle conditions. No new source request, event write or publication mutation this wake. Evidence guide-11605-result-summary-1901.json, guide-11605-state-1857.json and guide-11605-import-1859.log in the retained task directory. Later public VK, normal TG, media reset and November 4/6 promo checks remain required. All incidents remain open.


### 2026-11-03 19:12 UTC — ordinary guide accepted; VK source alias delivered

Scheduled guide ops 11605 completed partial at 19:01:08 UTC: 22 sources, 60 posts, one new excursion occurrence (711) and 21 occurrence updates, 22 templates and 26 profiles touched. The two remote HTTP 503 failures on ruin.keepers posts 9437 and 9432 remain explicit; they were not silently counted as successes. Source reports and occurrence-source anchors are retained. These are excursion records, not new general Event cards. The ordinary post-scan digest hook created issue 327 with status empty and no provider targets; no new evening digest or campaign repost was sent. Today's visual issue 326 and its original repost URLs remain intact.

The healthy guide and its scheduler wrapper finished before delivery. With normal ops and MCP intake idle and untracked scheduler work terminal, the prepared cutover delivered merged PR #789 as exact main 9f14c503f977b467ed6ad963e2de59008bc0a435, image intake-vk-source-query-20261103, immutable sha256:8b8875452f667264b279f0a10459b92e3556d57b6ec2a02ccebf72590ef51e24. Earlier required CI, 62 regression assertions, full 4,433-file qualification, initialized isolated preflight and wrong-SHA control were reused; no rebuild was needed. Runtime, SQLite quick_check, selected models, empty reserve override, promo interval five, cinema reference and identity/adjudicator hashes passed. Startup HTTP probes initially arrived before the listener opened; the subsequent live health passed ready=true, DB/scheduler ok. Kotopogoda container identity is unchanged.

Fresh readback confirmed both prior November 14 source operations are failed with null event_id; existing wrong-programme cards 9815 and 9779 remain unchanged. One bounded November 14 v3 request now uses the normal typed parser and full Smart Update: evt_op_hw6PvnyBvxh_UtG4U2y6FsTOK_U1zhl2, key INC-2026-11-03-programme-source-melodii-20261114-v3. It is processing, not accepted or published. No old key was recommitted and no legacy programme replay was repeated. November 27 remains unsubmitted. Read the durable operation before deciding the next step; do not infer success from processing.

Evidence in the retained task directory: guide-11605-terminal-receipts.json, guide-11605-publication-receipt.json, guide-11605-terminal-1911.log, vk-source-query-final-idle.log, vk-source-query-cutover.log, vk-source-query-live-verification.log and vk-source-query-live-health.json. Continue actual public Event 10450 after 20:20 UTC, normal TG 21:40 UTC, media reset November 4 00:05 UTC, normal VK November 4 04:15 UTC and campaign observations November 4/6. All three incidents remain open.


### 2026-11-03 19:26 UTC — typed source reaches identity guard; no canonical recovery yet

November 14 v3 evt_op_hw6PvnyBvxh_UtG4U2y6FsTOK_U1zhl2 is terminal rejected, event_id null and no publication jobs. Its generic MCP error is EVENT_REQUIRED_FIELDS_MISSING, but the actual candidate 25994 / decision 19099 show identity_final_retry / distinct_not_grounded. The model proposed create / distinct_occurrence while listing the existing November 13 card 9775 versus the November 14 source as a blocking conflict. The guard preserved that uncertainty; no force-create, retiming, guard weakening or blind repeat occurred. Source parsing and SQL alias now reach the real Smart Update boundary, but this is not event recovery.

Canonical 9775 source_text explicitly says November 13 Melodies of Love, while the retained current organiser poster says November 14. Canonical 9815 says November 14 Cinema in the Leading Role, and 9779 says November 27 Give Me Back the Music. Their recorded provenance is derivative Telegram, including our own publications, rather than a newly verified organiser correction. Reconciliation needs stronger schedule evidence; different dates alone must not be converted into an assumed reschedule. The v3 candidate city is Kaliningrad despite explicit Zelenogradsk in the supplied source; this mismatch is recorded without claiming its root cause. Candidate evidence was redacted to omit the private MCP endpoint and restricted to mode 0600.

After terminal v3 and an idle-work readback, the first separate November 27 request was submitted once through the normal typed parser/full Smart Update: evt_op_oMI4xiGzlVrNd502ZMu49M9VoV7c6f-Q, key INC-2026-11-03-programme-source-pesni-20261127. It is processing, not accepted or published. Do not recommit either request. No poster media edit was committed. Production remains healthy on 9f14c503; docs PR 790 merged with all required CI green as main 2b01705165d74df44ba2a73b72b51ef21421ef75, without runtime restart.

Evidence: programme-melodii-v3-terminal.json, programme-melodii-v3-decision.json, programme-existing-exact-source-1925.json, programme-pesni-operation.json and programme-after-v3-health.json in the retained task directory. Preserve normal Telegram, public workshop, media reset and campaign timing checks. All three incidents remain open.


### 2026-11-03 19:40 UTC — November 27 canonical recovery; stale VK edit falsely reported applied

The first November 27 request evt_op_oMI4xiGzlVrNd502ZMu49M9VoV7c6f-Q was accepted MERGED into existing Event 9779. Current title is «Песни о любви», November 27 at 18:00, Solyonaya Vorona, Railway Street 1, Zelenogradsk. One exact-date venue card exists, and EventSource 13915467 links the organiser programme wall-194927034_5002. No duplicate was created, and the date/city were preserved. This is actual full Smart Update acceptance after the SQL alias correction.

Typed media edit evt_op_OtTmrBIzM6ve8t5ehHnkbhDlzUadqiXo accepted the visually checked original programme poster as poster 35057. Media role/pair review remains pending until November 4 00:05 UTC under existing budgets; Telegraph/calendar dependencies defer to that reset, and Telegram card update is due November 3 20:30 UTC. No feature budget was reset or bypassed. Existing text revision reached Telegraph, but the latest media revision is still queued.

Publication readback found a separate mechanical defect: event_publication_status reports VK wall-231920894_12213 up_to_date for the new revision, while authenticated provider readback still shows old title «Верни мне музыку». Logs at 19:34 and 19:38 prove edit_vk_post skipped the old post as uneditable, then sync_vk_source_post returned its URL and the job persisted a changed/success receipt. This supersedes any earlier implication that a successful stored VK receipt proved the corrected text live. Do not create a replacement public post or retry the old post blindly.

Minimal prevention forwards the existing permanent VKAPIError instead of swallowing it as False/None. Existing outbox permanent-error handling already scopes the terminal failure to one job without global captcha pause or retry. The genuine unchanged-content no-op stays False, checked before the edit-availability guard. No new state, queue, provider fallback, public post or semantic rule. Local regression reproduces false job completion for both preflight and provider edit-window rejection. Fix is not deployed yet; actual production remains healthy 9f14c503.

Stronger November 14 provenance remains incomplete. Live organiser search confirms the same concert recurs on multiple dates in September/October, but does not independently establish a November 13-to-14 reschedule. The current November poster visibly places Cinema in the Leading Role on November 13 and Melodies of Love on November 14; do not overwrite old dates or repeat rejected source intake by assumption. Original guide/promo/cinema monitoring remains active.

Evidence programme-pesni-accepted.json, programme-pesni-canonical-1937.json, programme-pesni-media-accepted.json, programme-pesni-media-jobs-1940.json, programme-pesni-publication-before-media.json, programme-pesni-publication-1942.log and vk-permanent-edit-before.log in the retained task directory. Incident remains open.

The final narrow transport patch has net production lines zero (five additions / five deletions in main_part2.py, including moving the existing exact-content no-op before edit-availability rejection). All 18 focused helper/outbox/location-marker controls pass. The broader VK suite had 69 passes and eleven missing-media fixture failures; all eleven reproduce with the unchanged qualified baseline edit helper, so they are retained as existing failures rather than claimed green or repaired in this scope. The three distinguishing permanent-edit assertions failed against baseline. No live public canary or replacement post was sent.


### 2026-11-03 20:07 UTC — permanent-edit fix delivered; disproved receipt reconciled

PR #791 passed all four required CI checks and merged. Production now runs exact main 3bb038338dce53f1e1a74be0441b85fa11dc2be8, image intake-vk-edit-outcome-20261103, immutable sha256:1b9971bb3137fa6053cb8e535861039582278a4a5f01fec80ab69ef30b9c3171. Running-release ancestry, unchanged dependencies, all 4,433 tracked files, initialized isolated SQLite/HTTP preflight and wrong-SHA rejection passed. Runtime/model/empty-reserve/promo-five/cinema/identity/adjudicator checks and live health passed; Kotopogoda unchanged. Preflight container stopped and removed. No rebuild or repeat cutover is needed.

The Events-only restart had no active source imports or MCP intake, but overlapped background media review job 143319 for Event 10137. This was an avoidable gap in the operational check; do not describe the cutover as having all background jobs idle. The existing 180-second stale handling recovered the same job without an operator reset: stale at 20:04:51, normal retry picked at 20:05:24, done at 20:05:56, error cleared. Durable recovery is verified, not assumed. The older BGE claim 143226 predates the prior container start and was not modified.

A read-only live boundary probe used the real new edit helper against VK 12213, locally refusing any provider method except wall.getById. It propagated the expected code 15 / edit time expired, exactly one provider read and zero provider writes. This proves the permanent refusal is no longer swallowed; it does not claim the old post changed.

A bounded operator reconciliation then corrected only the disproved success for 9779. Dry-run and apply required exact runtime, unchanged canonical revision c5e105a11e080dcda1955dbb7b970d472823f950740cf6695f616201783d507e, old hash, job 100936, accepted media operation and receipt 58942. It removed the false applied revision/hash and false last-applied timestamp, preserved the known old post URL, and classified the existing job as a scoped permanent failure without retry. No Event content, source, date, title, outbox fan-out or provider write occurred. This is an explicitly operator-reconciled receipt, not a fabricated successful worker attempt. Do not requeue it or create a replacement wall post blindly.

Post-reconciliation direct DB read confirms job paused / terminal failed and applied_event_revision null. The public MCP publication-status read for 9779 returned INVALID_ARGUMENT/internal error on repeated read, while 10450 publication-status still succeeded. The failed read is retained as a verification limitation; its cause is not established, and SQL evidence must not be presented as a successful MCP read. Do not guess it is a global MCP outage.

Docs PR #792 also passed all four required checks and merged as 40a7dbbb95cd7942e15cd67de5431c9c7f05cdaf, without runtime restart. The Opus audit brief is available in main; coordination and product-requirements threads are mandatory reads. No auditor acknowledgment yet. Owner requires at least one day of production evidence before completion statements in the requirements thread.

Evidence: vk-edit-outcome-build.log, qualified-image.txt, preflight/negative/cleanup logs, cutover.log, live-verification.log, live-health.json, live-negative.log, background-recovery.json, vk-9779-false-success-plan.jsonl, vk-9779-false-success-applied.jsonl and postdeploy-mcp-probe-errors.log in the retained task directory (vk-edit-outcome prefix where applicable). All three incidents remain open. Next actual public 10450 after 20:20 UTC, 9779 TG card after 20:30, normal TG 21:40, media November 4 00:05/00:10 and promo November 4/6. November 14 date conflict remains unresolved; no further replay.


### 2026-11-03 20:31 UTC — actual workshop public post verified after deadline

Read-only authenticated VK checks confirmed the old postponed ID 13320 is absent and the unique exact-title/date public wall post is 13321, with postponed_id 13320. Public URL https://vk.com/wall-231920894_13321 has the workshop title, November 5 at 18:00 and Leninsky Prospekt 83. Four wall.get reads through existing identity recovery helpers, zero provider writes; no replacement publication was created. This proves public availability at 20:29 UTC, not the exact native publication time. Canonical Event 10450 already points to 13321 through existing runtime behavior, while event_publication_status still returns the obsolete 13320 URL and stale applied revision. Thus public-content acceptance and receipt-link acceptance differ; the latter remains incomplete. No manual URL or receipt mutation was performed. Latest media revision remains pending budget reset.

Event 9779 Telegram job 100937 is still pending after its 20:30 due time; its explicit dependencies telegraph_build 100934 and tg_ics_post 100935 are pending until November 4 00:05:01/02 after the media budget reset. Do not count the old Telegram link as current acceptance, and do not bypass those dependencies. Exact VK permanent failure remains paused as reconciled. Ordinary scheduled Telegram at 21:40 UTC is a separate required acceptance.

Docs PR #793 passed all four required checks and merged as main a1ec333c5ddba67301d04e3ab930722347e10781. Production remains healthy on 3bb038338, with no restart or production write this wake. Requirements thread has no new requirement; coordination thread has an owner mention of Ester Show at 1793737347.119059, not recipient acknowledgment or an audit result. Existing constraints and all three open incidents remain.

Evidence in retained intake directory: workshop-live-id-2030.jsonl, public-workshop-2025-probe-v2.jsonl, public-check-health-2029.json and workshop-publication-ledger-2027.json. The first probe had an operator argument TypeError before provider calls, fixed only in the read-only probe; this was not a production application failure.


### 2026-11-03 22:04 UTC — normal Telegram scan progresses; first bounded audit fix prepared

Actual ordinary Telegram ops 11606 started from the 21:40 scheduled slot, run 9fac10bfbaca46a38aeb0b92841e9be0. Durable Kaggle source_done/source_started transitions advanced from nine completed sources at 21:57 to twelve at 22:00, with source 13 of 57 active and five messages scanned. Remote scan remains active: no final child/source/event acceptance yet. Background exact-poster merges into 10137 are unrelated and must not be counted as this scheduled import. No restart, extra source request or replay was run.

Opus audit PR #794 passed all required CI and merged as main 9b504759e9825799459f7ec5164d869ddebc9954. Its first mechanical diagnostic proposal is implemented in source, NOT DELIVERED: the MCP create adapter derives its rejection code from existing candidate receipts. Actual missing_date/title/location keep EVENT_REQUIRED_FIELDS_MISSING; other non-accepted candidates receive SMART_UPDATE_NOT_ACCEPTED with their original reasons still in candidate_receipts. Rejected status, candidate outcomes, Event writes, JobOutbox and identity decisions are untouched. This improves diagnosis, not event recovery or acceptance of the disputed November 14 source.

Valid baseline regression reproduces two wrong-code failures (identity_final_retry and scope_review_failed), while three true missing-field controls pass. After correction all 38 existing create/partner-operation tests pass, including no canonical writes for refused requests and the accepted create path. Net production Python +4 lines, no new state/worker/retry/response field. Evidence mcp-rejection-before-valid.log and mcp-rejection-after.log in retained intake directory. Delivery requires exact merged main qualification and a safe terminal import window. Do not recommit a terminal operation to obtain a live canary. Further Opus proposals, semantic reason-code changes and historical retry drainage are not part of this patch.


### 2026-11-03 23:06 UTC — bounded replay confirms disputed-date risk in proposed guard simplification

Normal Telegram ops 11606 continues its scheduled scan: by 22:59 it had completed 32 of 57 sources with 37 scanned messages, then started signalkld. Fresh notebook heartbeat at 23:04 does not prove local import acceptance. Production remains healthy on 3bb038338; no restart, replay, domain write or new provider call was made. Qualified candidate 5aac76af / immutable ed52cb862a49b11b166b168acafd70275d94abbe9c42a4fd0c1b982b1c5fecc5 is still waiting for terminal ordinary import.

The Opus proposal to remove reason_code from grounded_distinct was checked against saved decision 19099 using the actual function body from merged source 5aac76af47a1c5939452793a7955fa87940c9333. Function SHA-256: 9c8c8895241359fe87eaf09ad7befcde5e848d1192ee5720bfa143fe68b3e890. Inputs were reconstructed from the retained candidate/decision and saved owner source text; this is a bounded function replay, not a complete Smart Update integration or new model verdict. No private operation context was exported.

| Control | Outcome | Evidence |
| --- | --- | --- |
| Original function, raw reason_code distinct_occurrence | FINAL_RETRY / distinct_not_grounded | Literal November 14 candidate and November 13 owner quotes retained |
| Only reason-code membership condition removed in memory | FINAL_DISTINCT | Same quotes and blocking conflict about two dates for the same concert remain |
| Modified function with relation unknown | FINAL_RETRY | Confidence remains 1.0; unknown still refuses create |

Thus removing that single predicate really can authorize a new November 14 card next to disputed existing 9775 on November 13. This is not evidence that either date is correct or that the concerts were rescheduled. Preserve the current fail-closed result until stronger provenance and an appropriate semantic contract distinguish a recurring occurrence from contradictory dates. Adding a new keep code without that distinction would need the same counterexample review. No production guard change, old-row drain, source recommit or additional state/worker was introduced.

Evidence: retained replay-19099-reason-guard.py and replay-19099-reason-guard.json; prior safe organiser/owner provenance in programme-existing-exact-source-1925.json. Public availability of workshop 10450 is already confirmed; its receipt URL discrepancy, the media-reset checks, ordinary Telegram acceptance and future promo timing remain separate unfinished criteria.

### 2026-11-04 07:33 UTC — overnight normal results and duplicate slot dispatch

The native wake due 00:11 UTC was processed only around 07:16 UTC. This is a monitoring delivery gap, not evidence that an unattended check ran at its target time.

Ordinary Telegram ops 11606 reached remote terminal at 00:05:06 and local terminal `partial` at 06:03:53. Durable metrics: 57 sources, 124 messages, 33 events created and 73 merged; 23 messages have terminal technical errors, including partial messages with accepted children. `errors_count=0` does not erase these message-level failures. Exact message receipts are retained in `tg-11606-terminal-receipts-0724.json`. Identity conflicts, unavailable identity LLM and incomplete source evidence remain distinct from product exclusions. Candidate totals over the whole time window also include source parser activity after 02:30; do not attribute that aggregate to Telegram or replay rejected messages indiscriminately.

Morning VK materialized two partial scheduled runs: watchdog 11612 (04:47:21–07:02:53, three created, two imported carriers, three technical rows) and normal 11616 (06:03:53–06:31:31, four created, one imported carrier, two technical rows). Both processed 12 terminal carriers with zero unresolved rows. These are distinct batches, not proof of duplicate Event creation. Source parser 11611 remains active and progressing; preserve it.

Mechanical root cause is reproduced: the normal 04:15 cron job emitted `JOB_SUBMITTED` and waited on the existing heavy gate before its entrypoint could materialize `ops_run`. The watchdog checked only durable rows and dispatched another batch at 04:47:21. It ignored existing `_run_meta` submitted-job accounting maintained by APScheduler events. Production excerpt: retained `vk-double-slot-evidence-20261104.log`. A parameterized regression fails on the original code for a still-submitted slot, while its executed-slot control passes. The minimal proposed fix reuses `_run_meta` before watchdog dispatch; three production lines, no new queue, state, retry or semantic rule. All 72 scheduling tests pass with the existing Telethon dependency available; the initial system interpreter run had one missing-Telethon import failure and 71 passes. Heavy-wait registration regression is checked separately. This fix is not yet delivered at this entry.

Media reset worked for both scoped events: 10450 media 143027 and 9779 media 143291 reached done; Telegraph/calendar jobs also reached done. Actual Telegraph and calendar reads confirm 10450 workshop details. Its Telegram card 143014 is due at 08:00 UTC, so is not accepted yet. For 9779, actual Telegraph title is corrected but its body retains the old concert title; actual Telegram card 4678 also remains old while job 100937 is pending. These are not current publication acceptance. A typed description preparation returned `INVALID_ARGUMENT` without a prepared ledger row or commit. A bounded model-only read-only reproduction returned `PUBLIC_TEXT_FACT_CONFLICT` on locked facts; no Event or job writes occurred and no guard was bypassed. The existing permanent VK failure remains honestly recorded; do not replace the old VK post or repeat reconciliation.

Retained evidence root: `/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/`; relevant files include `media-reset-and-tg-0012.json` (actual capture 07:16, not its historical target in filename), `morning-receipts-0732.json`, `calendar-and-card-live-0722.json`, `telegraph-9779-20261104-live.json`, `pesni-text-gate-readonly-0726.jsonl`, and `vk-submitted-telethon.log`. All three incidents remain open. Candidate 5aac76af remains qualified but undelivered; production remains 3bb038338. New slot-dispatch prevention needs merged-main qualification before delivery. Preserve the upcoming 08:15 ordinary VK slot, guide digest/promo checks, and November 6 paired repost acceptance.

### 2026-11-04 08:12 UTC — workshop Telegram published; accepted rename fact corrected

PR #798 is merged and combined exact main 4571309e24faef35c27a96364995639d15e88817 fully qualified as image intake-vk-submitted-20261104, immutable sha256:aa7c2e88c457622164fb9936ed08a44c575a1868db609d937eb7e29ba9386adc. All four required CI checks, 4435-file source verification, initialized isolated controls for MCP and actual scheduler callback/watchdog, wrong-SHA and HTTP preflight passed. The temporary container was removed. Production remains 3bb038338: parser 11611 still progresses (234/271 at 08:04), so there is no cutover yet. Do not rebuild or switch to the superseded 5aac candidate.

Event 10450 Telegram job 143014 completed at 08:06:50 and published https://t.me/kldevents/5144. The first basic Telethon helper misleadingly exposed empty legacy message/media fields: this is a `rich_message`, not an empty public post. A second exact read traversed its real rich-message blocks and confirms title, November 5 at 18:00, Leninsky83, workshop text, calendar link and photos. This is actual public-content acceptance, not merely a done job. Evidence workshop-rich-live-0812.json and workshop-card-dispatch-0812.log.

For Event 9779, one exact historical title fact 194844 was moved from added to existing conflict, with provenance note 209511 under accepted official source 13915467. The transaction checked the accepted merge, exact old fact, canonical title/date/time/city and all 33 prior fact rows; it preserved every other row and made no Event/outbox/provider/intake write. The unchanged real MCP gate then accepted description preparation under the same stable key, committed once as evt_op_wdKH5AMYswuguIAIsfg80Aye4_lax90C. Revision e82ba58c0d985328e4b8368900be96621410500d57811aece938c9205850dae3; current public projections still require readback. Do not recommit it. Event_get returned a read-budget error, not a global MCP outage; operation and publication-status readbacks succeeded. No identity/date guard was weakened. There was no manual VK requeue or replacement; normal typed-edit reconciliation reached the existing permanent edit-window refusal again.

Opus audit PR #799 passed all required checks and merged as docs main 555c88505; no runtime restart for docs. The auditor withdrew the reason-code relaxation after counterexample #797 and approved #796/#798. A real isolated control found that their suggested note-only use of _record_source_facts replaces the whole source fact batch, losing other facts. This was not applied in production. Full public evidence and prevention limits are in [the bounded reproduction report](../2026-11-04-smart-update-locked-facts-reproduction.md). Generic rename prevention is not claimed by the one-event mitigation. Original normal VK08:15 and guide digest08:30, daily Afisha and November6 paired timing remain open.


### 2026-11-04 09:11 UTC — combined scheduling and diagnostic release delivered

Production now runs merged main `53a7791360ca8308458ce11df313d23c5c7bfd03`,
image `intake-guide-wait-20261104`, immutable
`sha256:b55e338530de0414ad4e1ab4b20c7710e61a2d25c57008d2e677556ce929b65a`.
This includes PR #796 rejection diagnostics, #798 submitted-slot accounting and
#801 daily guide waiting on the existing heavy semaphore. All required CI,
exact 4,436-file source qualification, initialized isolated adapter/scheduler
controls and wrong-SHA control passed. Preflight intentionally disables startup;
its `ready=false` is isolation evidence, not production readiness.

Events-only cutover followed terminal normal imports and idle MCP/background
work. BGE job 144315 was already done at 08:55:13; its generic ledger remained
created and must not override the actual job result. Remote video session 1559
continues rendering `zigomaro/cherryflash` with a durable dataset and notification
target. Existing enabled popular-review watchdog calls `resume_rendering_sessions`
every ten minutes, independently of disabled global Kaggle recovery. That resumes
the same poller without launching another kernel. Release preserved the remote
run; actual resumption after the 09:20 watchdog still needs verification. No
registry reset, recovery flag change or duplicate remote launch was performed.

Explicit live source, SQLite, model settings, cinema reference and semantic
hash checks passed. Health has `ready=true`, DB/scheduler healthy and no issues;
Kotopogoda container and source mount unchanged. No old source operation,
programme replay, 37-row plan or exact false-receipt reconciliation was repeated.

Retained evidence: `guide-wait-release-receipt.json`,
`guide-wait-cutover-0909.log`, `guide-wait-live-verification-0910.log`,
`guide-wait-health-0911.json`, `guide-wait-video-deploy-decision-0909.md` under
the existing intake recovery artifact directory. Incidents remain open: actual
normal import acceptance and residual technical failures are separate criteria.

Ordinary VK 11620 ended partial at 08:32:51: twelve terminal carriers, none
unresolved, zero created/updated Events, eight product rejections and four
technical rows. Exact source/child/carrier receipts are in
`vk11620-terminal-0845.json`. Both Gemini 3.1 Lite and 3.5 Lite returned actual
HTTP 503 high-demand responses, including OCR; this is not proof of all-key RPD
exhaustion. Candidate 26341 / decision 19431 refused distinct creation despite
recalling unrelated September owners. It needs bounded model-only diagnosis
while retaining opposite date-conflict control 19099; no forced creation or
identity/date guard relaxation is authorized by this observation.


### 2026-11-04 09:24 UTC — remote continuation accepted, promo delay preserved

After the Events restart, the same video session 1559 resumed polling and reached
remote COMPLETE and local PUBLISHED_TEST at 09:19:16, without a duplicate remote
launch or recovery flag/registry edits. Output was received and normal cleanup
completed; error is null. Polls were already active before the expected 09:20
watchdog, so that later tick is not claimed as the first-resumption cause. The
ordinary 09:20 watchdog also completed. Evidence: retained
video1559-resumption-0922.log and postdeploy-state-0922.jsonl.

Normal campaign ticks retained Afisha due_at 11:26:38 UTC and Anons cadence
refusal, with no duplicate send. Actual runtime remains healthy 53a779136. Docs
PR 802 merged after all four required CI; docs main 8e04f5205 requires no restart.
Next normal VK slot is 10:00 UTC; this verification does not accept it in advance.


### 2026-11-04 10:35 UTC — ordinary scheduled VK terminal with eight creations

Ops 11623 ran on time 10:00:00–10:28:26 UTC, terminal partial: twelve terminal
carriers, zero unresolved, eight CREATED Events 10513–10520, zero updates. One
carrier fully imported; nine confirmed no-event; two technical carriers retained
seven accepted children and two refused children. Exact EventSource anchors
13937509–13937516 and typed child receipts verified. A failed carrier must not
be interpreted as zero writes or replayed blindly. Watchdog recognized the
existing submitted slot; no duplicate catch-up dispatch observed.

Two residuals are documented for the existing audit in
[normal VK residuals](../2026-11-04-smart-update-normal-vk-residuals.md): candidate
26351 / decision 19442 produces unsupported reason_code distinct_occurrence for
explicit recurring dates; candidate 26349 has mixed-city venue defaults and an
invalid grounding response. Preserve counterexample 19099 and identity/date
guards; no semantic fix or source retry was performed. Actual Gemini Lite OCR
fallback recovered OpenAI 429s. This accepts normal dispatch and exact partial
ingestion, not complete elimination of technical failures.


### 2026-11-04 12:30 UTC — guide/server parser interleaving and remote reserve loss

Ordinary guide light ops 11627 completed remote scanning at 12:21:15 and local
import partial at 12:27:02: 30 sources, 49 posts, five existing occurrences
updated, none created, 17 LLM successes and seven failed posts. Source parser
11629 started 12:15:41 and remained active concurrently; neither slot was
skipped behind the other. Exact source/occurrence receipts are retained in
guide-parser-progress-1230.json; raw errors in guide11627-remote-failures-1224.json
show six provider 500/503 responses and one MAX_TOKENS truncation.

The normal Kaggle payload omits the host's existing GOOGLE_AI_FALLBACK_MODELS,
so the remote shared client cannot use the configured Lite reserve. Minimal
correction forwards that setting, limits each model to one attempt and retains
a three-physical-send inner ceiling across the shared model chain; existing
outer bounded timeout/retry loops unchanged. Baseline actual notebook/shared
client routing controls fail; after the correction 109 guide/client regression
checks pass. Live historical extraction controls are still required before
delivery. No Event/source replay or semantic guard change. Truncation is not
claimed repaired by model availability. Generated notebook snapshot refreshed
with the existing factory; previously merged VK pacing is synchronized too,
already present in the runtime-generated notebook before this fix.

### 2026-11-04 13:07 UTC — remote reserve qualification and bounded live failures

PR 806 merged after all four required CI. Exact source 4fb0cc3614db8c939a91cbab637009b9ea5f4c16 built as immutable c06f6ea53c8f0b616ce20f432484ac0d0da6c930a71b1daf83e9ee8dede8d2b5; 4438 tracked files verified. Initialized isolated SQLite, adapter no-write controls, scheduler submitted-slot control, wrong-SHA refusal and disabled-startup HTTP isolation passed. The existing reserve setting is forwarded by the actual payload builder. Isolated container removed. No production cutover.

Remaining real model-only controls did not pass: profitour 918 screening used Gemini 3.5 Lite, but extraction timed out; the synthetic mixed availability screen returned MAX_TOKENS from Gemini 3.5 Lite and HTTP503 from Gemini 3.1 Lite. No Event or Job writes. This is incomplete semantic qualification, despite the successful process exit. Existing screen output allowance 160 is now a measured truncation observation; no budget or semantic change is claimed. Previously accepted controls were not repeated.

Ordinary parser 11629 continues real source processing: MERGED 10380, later existing ticket receipts 10388/10390/10391/10392 and event 204/271. Transient SQLite locks around 13:00 affected background/on-demand/crawl tasks; health returned ok/ready with DB ok and no issues around 13:04 without a reset or restart. Media 91980 is error/stale under existing recovery, not manually cleared. Terminal import acceptance still pending. Evidence in guide-remote-reserve-preflight.log, guide-remote-remaining-controls.log, parser 11629-source-progress-1306.log and guide-remote-prod-health-1305.json under the retained task directory.

Independent guide-card PR 803 merged as main 79f8b43374dd9b4b531533d7145f4c04974e61b5 and includes PR806. Preserve another operator's eventual newer runtime; never deploy the older 4fb candidate over it. No source replay or identity/date guard weakening.


### 2026-11-04 13:28 UTC — measured guide output ceilings

Real model-only comparison reproduced screen MAX_TOKENS on Gemini 3.5 Lite at 160; the same prompt/schema at 420 returned complete JSON. The existing tier1 allowance 520 truncated a three-occurrence synthetic contrast on both Lite 3.1 and3.5. At 2200 (the existing ask_gemma default), real Lite 3.1 returned all three dates and preserved available Lower Pond versus two sold-out walks. This is a synthetic contrast derived from the edited current 5988 source, not original historical bytes or a source replay. The retained on-demand 918 source produced no dated occurrence, so cannot become ready through existing eligibility checks. No canonical writes.

Minimal prevention changes only the two existing max_output_tokens arguments, zero net handwritten production lines, and regenerates the notebook. No prompt, schema, model chain, retry, worker, state or identity/date guard change. The 109 guide/client regression tests pass; remaining changed-budget historical controls are running. Evidence: guide-screen-budget-comparison.log, guide-remaining-screen420-controls.log, guide-multi-budget2200-control.log and guide-output-budget-tests.log. Not delivered.

Ordinary parser 11629 terminal partial at 13:23:01: three created 10521/10522/10523, two updated 10380/10511, 264 unchanged, two technical Tretyakov failures and a separate Philharmonia kernel error. Actual EventSource anchors 13937517/13937519/13937520 and updated 13937518/13926549 checked; unrelated concurrent Events are not attributed to this run. No blind retries.


### 2026-11-04 13:37 UTC — qualified output-budget release, normal VK active

PR 807 passed all four required CI and merged as exact main b565e271f9d5c2874ae9b16ea0893b6e329927c7. Immutable image 90c3d935864d605509a2c5f5c8b157381085c89a80aead667e73e7ccf7dd36d5 passed full 4439-file source verification, initialized isolated preflight, negative SHA and HTTP isolation. Six real model controls accepted at changed budgets, with explicitly synthetic mixed-availability contrast; no Event/Job writes. All 109 regressions pass. Includes independently merged PNG card quality fix. NOT delivered: ordinary VK 11632 is active from its normal 13:30 slot. Three posts confirmed_no_event, fourth processing at 13:36; no Event success claimed yet. Normal promo tick preserved today's URL without a duplicate. Runtime remains healthy 53a779136. Do not interrupt intake or deliver an older candidate over another operator's newer runtime.

Separate main event_parse truncation remains: January 4 11:00 Tretyakov row exhausted the existing 6000 output allowance on both Lite models (about 5996 output, 8737 input tokens), before Smart Update candidate creation. Raising guide notebook budgets does not repair that surface. Excessive output cause not established; no blind budget increase or source replay. The January 4 13:00 occurrence already exists as 10523. Evidence parser11629-provider-refusal-1313.log and terminal receipts.

Retained guide-output-budget-release-receipt.json records the fully qualified image; cutover and live delivery acceptance remain pending terminal normal import.

### 2026-11-04 14:02 UTC — combined release delivered after normal VK terminal

Normal VK ops 11632 completed partial at 14:00:20: 12 terminal carriers, zero unresolved, zero created, two updated (10474 and 10328), seven rejected and three technical. Exact source anchors 13937521/13937522 and MERGED child receipts checked. Packet 26690 has terminal CONFIRMED_PRODUCT_EXCLUSION and candidate 26360 rejected missing_date; its primary/verification attempt rows are stage receipts rather than final carrier receipts. Preserve accepted sources; no replay.

With ops, MCP intake and background jobs idle and current scheduler heavy work terminal, Events-only cutover delivered exact main b565e271f9d5c2874ae9b16ea0893b6e329927c7 / immutable 90c3d935864d605509a2c5f5c8b157381085c89a80aead667e73e7ccf7dd36d5. Runtime SHA, model settings, output budgets, SQLite, cinema and semantic hashes verified. Live health ok/ready, DB ok, no issues; Kotopogoda unchanged. No rebuild or repeated switch is required.

A separate model-only reconstruction reproduced broad schedule expansion and MAX_TOKENS. Existing operator_extra made the primary answer correct, but subsequent verifier lost that scope, generated a truncated series and terminal adjudicator returned a different date. Fact collection also interpreted phone/duration fragments as impossible calendar values. No experimental fix or canonical writes. Full critical packet: [source parser scope loss](../2026-11-04-source-parser-scope-loss.md). This disproves a prompt-only fix and does not justify weakening date/identity guards. Incident remains open for remaining normal scheduled acceptance and technical refusals.


### 2026-11-04 14:44 UTC — bounded source-scope correction

A transport-only prototype still failed the full model control: its primary date was correct, but verifier returned the full programme and wrong years. The minimal correction now forwards existing operator_extra into existing source_context, passes source_name into the actual source_channel, and states in both existing request rule lists that completeness is checked within operator scope. The structured ticket adapter states the one-card scope using its existing operator instruction. No raw source/OCR is removed, no new state/retry/worker/flag or date/identity guard change. Net handwritten production lines +5, within a 12-line budget; existing Telegram notebook regenerated to embed the changed shared contract.

Valid baseline context transport tests fail twice, after correction they pass. Existing source/parser/Telegram contract suite: 151 passed, using cached missing local BeautifulSoup dependencies rather than installing packages. An earlier broad test command named a nonexistent file, then collection found missing bs4; neither is counted as a test pass or production failure.

Full model-only reconstruction after transport plus request rules returns exactly one 2027-01-04 11:00 occurrence with end_date null through the conditional verifier, versus wrong dates/series before. Exact generic instruction from the source adapter was used. Raw failed OCR remains unreconstructed; zero Event/Job writes. Unscoped multi-event and exhibition-period controls are pending. No new production delivery claimed. Existing b565 runtime remains healthy.


### 2026-11-04 14:56 UTC — source scope correction delivered

PR #809 merged after all four required CI passed. Exact main `4600e9978a7eb9b22234590bfc76258feba0a905`, image `intake-source-scope-20261104`, immutable `sha256:ae711ef4e2c38b996b81b4efe61970f66d4770dcbabaeda32965d86789ddfb7f` delivered Events-only at 14:54:13 UTC. All 4441 tracked files matched, initialized isolated preflight and wrong-SHA control passed, isolated HTTP confirmed no scheduler activation. Live SQLite/settings/cinema/identity and adjudicator hashes, context transport and health passed (`ready=true`, DB ok, no issues). Kotopogoda unchanged.

Real model-only full chain retained January 4, 2027 at 11:00 with no end date. Unscoped two-lecture control preserved both November 20 and November 25; scoped exhibition retained November 10–December 10. Initial 160-second multi-control timeout was below the existing production provider cap; one bounded 270-second harness rerun succeeded without changing runtime timeouts. Reconstruction uses the accepted neighbouring source header, not original failed OCR. Zero Event/Job writes in these controls; 151 local regression tests passed. Phone/duration false-date facts remain independently unresolved; date/identity protections were not relaxed.

Local scheduled video ops 11633 handed off session 1563 before cutover. Only its remote render remained; existing recovery contract reattached the poller after restart to the same `zigomaro/crumple-video` kernel and `zigomaro/video-afisha-session-1563` dataset. Fresh actual RUNNING poll confirmed; terminal video acceptance remains outstanding. No intake replay or new kernel. Next normal VK 16:30 UTC, guide full 18:10 UTC and TG 21:40 UTC still require actual final receipts. Detailed retained receipt: `source-scope-release-receipt.json`.


### 2026-11-04 15:32 UTC — false numeric calendar facts reproduced and corrected

The existing numeric date regex treated telephone `+7 (4012) 99-55-55` as
(2055,55,99) and route duration `25-30 минут` as (2026,30,25). The shared
collector now requires a full four-digit year for a hyphenated numeric date;
dot/slash formats and full ISO/DD-MM-YYYY formats retain existing behaviour.
Short hyphenated ranges/phone fragments remain in raw/OCR semantic evidence
but cannot establish an objective calendar contradiction. No value-validity
filter or semantic verdict bypass. One existing regex changed, zero net
handwritten production lines; Telegram embedded collector regenerated.

Valid source-only before controls fail twice, while five explicit impossible
date controls already pass. Reconstructed source fact comparison removes only
the false IMPOSSIBLE_SCHEMA_VALUE; existing programme MULTIPLE_OCCURRENCES_COLLAPSED
remains, preserving conditional verification. No provider or Event/Job writes.
Retained date-boundary-source-facts-comparison.json contains exact before/after.
This correction is not yet deployed; production remains healthy 4600e9978.


### 2026-11-04 15:43 UTC — date-boundary image qualified, delivery guarded

PR810 merged with all four required CI green. Exact main
`e6bca5539de7feab4cea2e06c3e980aec0e89502`, image
`intake-date-boundary-20261104`, immutable
`sha256:ba65abe3af1c2bd131ef883ff88c01494c78890318ad5dc67a2c465e5c0ef165`
is fully qualified: all 4441 tracked files, initialized isolated preflight,
wrong SHA refusal, adapter/scheduler/context and numeric-boundary controls,
HTTP isolation, cinema and semantic hashes passed. Actual candidate model
chain retained one January4 2027 11:00 event without end date. Event and
JobOutbox row counts remained zero; preflight container removed.

NOT DELIVERED. Guarded cutover attempts stopped before mutation because
normal event_media_review jobs were active. First blocker145343 subsequently
finished done/nochange; work progressed to145359 with two due media jobs.
Do not interrupt this ongoing work or bypass guards. Actual production
remains healthy4600e9978; no new source replay or remote kernel. Video1563
continues on the same renderer. Normal VK16:30 UTC remains required.
Evidence retained in date-boundary-release-receipt.json and cutover logs.


### 2026-11-04 15:59 UTC — date-boundary correction delivered

After normal media work completed, all ops/MCP/background guards passed.
Events-only cutover delivered exact main `e6bca5539de7feab4cea2e06c3e980aec0e89502`
and immutable `sha256:ba65abe3af1c2bd131ef883ff88c01494c78890318ad5dc67a2c465e5c0ef165`
at 15:57:55 UTC. Fresh live SHA/settings/SQLite/cinema/semantic hashes, scope
transport and numeric-date controls passed. Health ready=true, DB ok, issues
empty. Kotopogoda unchanged. The same remote video session 1563 resumed fresh
RUNNING provider polls and ledger updates without another kernel; rendering
is not terminal video acceptance. No source replay or accepted operation repeated.

Earlier guarded attempts refused before mutations while short media jobs
were active. Three activity queries were consolidated into one diagnostic
invocation with the same refusal conditions; no new state, loop, weaker
guard or worker pause. Live media work after boot has fresh post-start claims.
Qualification and real model controls remain as recorded above; no rebuild.
Normal VK 16:30 UTC, guide full 18:10 UTC and TG 21:40 UTC remain required
with exact terminal source/child receipts. Incident stays open.


### 2026-11-04 17:09 UTC — ordinary VK slot accepted without technical refusals

Scheduled VK ops11635 ran16:30:00–16:45:37 UTC, terminal success, run
`bc4b1635afc548488a3b073753dd6086`, batch`auto:1793809800`.
All12 carriers terminal, zero unresolved, zero technical errors: one CREATED
Event10524, ten confirmed_no_event and one product exclusion missing_location.
Carrier88740/source26756/child12098 reports CREATED and EVENTS_RESOLVED;
EventSource13970445 anchors the canonical card to wall-9118984_25576.
No updates or unrelated creations attributed to this run. Retained complete
source/child/carrier receipts: vk11635-terminal-receipts.json. This accepts this
normal slot, not elimination of all historical technical failures or separate
structured parser, guide and TG acceptance. No replay.

Production remains healthy e6bca5539, ready=true, DB ok and no health issues.
Qualified Cherryflash year-label cutover refused before mutation because fresh
normal event_media_review145971 was running. No import interrupted or weaker
guard introduced. Guide18:10 UTC, TG21:40 UTC, November5 media/structured-parser
and November6 promo-pair checks remain outstanding.


### 2026-11-04 17:36 UTC — unchanged media rehydration advances deferred work

After VK11635 terminal, release guards repeatedly found short active media
reviews. Exact job history shows events10428/10431/10432/10328 re-enqueued
seconds after nochange. This is observed repeated work, not evidence that
a finite backlog is simply progressing. Isolated unchanged approved-poster
control reproduced _apply_posters advancing a six-hour pending geometry job
to seven seconds (added0, no provider or production writes). Its unconditional
geometry helper call competes with the existing handler's budget deferral.

Minimal correction arms that helper for new or replaced/reintroduced visual
inputs. Existing pair reviews and handler retries remain; no quota bypass,
new queue, worker, retry or persisted state. Four net production lines.
Regression fails before; 72 media/helper checks passed after, including existing new/reintroduced/replaced-image controls. An additional explicit same-image versus replaced-image due-time contrast passes. Not merged or deployed.
Retained media-unchanged-followup-control.log and media-followup-before.log.
Cherryflash candidate d051 remains qualified but undelivered; do not rebuild
it or confuse current media fix branch with a release. DocsPR813 merged all
four required green, docs main73522f359 requires no restart.


### 2026-11-04 17:58 UTC — combined media and year-label release delivered

PR814 merged after all four required CI passed. Exact main
`44d183696ff30507b151d59b829f15614256d22a`, immutable
`sha256:ca0c948dca380e43f324b017e326a571f086bb5ed2a6d54da4aa772ca6686a25`,
image `intake-media-followup-20261104` delivered Events-only at17:56 UTC.
All4442 tracked files, initialized isolated preflight, actual unchanged versus
replaced-image contrast, year labels, wrong-SHA refusal and HTTP isolation
passed. Own preflight container removed. Live runtime, SQLite, models, promo5,
cinema and semantic hashes passed; health ready=true, DB ok, issues empty.
Kotopogoda unchanged. This supersedes undelivered d051 display-only image.

Normal source ops and MCP intake were idle. Two conservative all-background
guards refused before mutations. The existing media restart contract was then
reviewed:180-second stale claim detection, independent-pipeline backoff and
coalesced successors; previously observed recovery143319. One bounded operator
exception allowed only the four exact nochange-loop event ids and task
event_media_review, still refusing all other background tasks. Active claim
was146259/Event10328. No worker pause, queue mutation or new recovery code.
Immutable media objects and transactional DB inputs remain; source imports
and external publishing tasks were idle. Decision and exact claims retained.

Initial live acceptance: queued reviews146261/146262/146260 completed normally
for10428/10431/10432. Their next jobs146263–146265 preserve Nov5 00:05 UTC
rather than running every seconds. Exact146259 still awaits existing stale
handling, so its recovery and fourth-card loop elimination are not yet accepted.
Normal guide18:10 UTC and TG21:40 UTC remain required, along with structured
parser, budget-reset projections and promo pair. No old source replay.
Retained media-followup-release-receipt.json, live-verification.log,
media-followup-live-progress-1757.json and media-followup-restart-contract.md.


### 2026-11-04 18:00 UTC — exact media restart claim recovered normally

Claim146259/Event10328 was marked stale automatically at17:59:04, scheduled
through existing30-second backoff and finished done/nochange at17:59:42,
last_error cleared. No manual ledger reset, new request or recommit. Its
successor146266 preserves Nov5 00:05 UTC. All four known-loop event ids now
have one future retry each (146263–146266), no active source/MCP/background
work at17:59:47. Other three future jobs retain their17:56 timestamps across
three minutes, versus former seconds-apart duplicates. Detailed receipt
media-followup-live-recovery-final.json. This verifies the bounded existing
recovery contract and immediate four-card loop correction. Longer normal
media/projection acceptance remains at the budget reset; normal guide18:10
and TG21:40 must still complete with actual source receipts.


### 2026-11-04 19:20 UTC — ordinary guide progressing, separate historical media timeout

Normal full guide11637 advanced to15/30 sources and40 scanned posts. Source16
valeravezet began19:12:25; earlier ruin_keepers and ruin.keepers both passed.
Remote/local terminal and source/occurrence acceptance still pending; do not
attribute Events from unrelated imports. Actual44d runtime remains healthy.

The four corrected nochange-loop jobs146263–146266 still retain their original
17:56/17:59 timestamps and Nov5 00:05 due at19:18. A different historical
media job58741/Event7114 is NOT recurrence of that loop: the same ledger job
reached attempt831, timed out at180 seconds19:19:03 and has existing one-hour
backoff until20:19:03. Card «Девчата» date2026-10-10, addedJuly25, has175 poster
rows:5 approved,14 duplicate,152 pending_review,4 rejected. Telegraph/calendar/TG
dependencies remain deferred behind that timeout. last_result=ok is stale
history, not a successful current attempt. This proves a separate persistent
archive-media timeout, not its root cause or a safe replay/retiming decision.

No queue mutation, source replay, budget bypass or worker restart. Preserve
healthy guide11637. Before any correction, isolate the expensive stage and
existing past-event/publication contracts; blindly marking media done could
unblock old publication jobs. Retained media7114-targeted-1919.log and
media7114-state-1920.json. Existing media/projection reset check remains active.


### 2026-11-04 19:56 UTC — normal full guide accepted and audit diagnosis

Ordinary scheduled11637 completed remote scan19:28:34 and local import
success19:33:02:30 sources,75 scanned posts,35 successful LLM post results,
zero deferred/errors. Six new guide occurrence rows712–717 and25 update
operations; these are not25 distinct updated rows (16 distinct updated ids,
20 distinct occurrence ids overall). All31 source/change receipts resolve to
stored rows and exact source anchors; final date/time/status matches the last
change for every row. New715 has no date and is correctly digest-ineligible
(template only); five other new rows have dates. No unrelated canonical Events
attributed to this run. Receipt guide11637-terminal-receipts.json and terminal
summary retained. This accepts the normal full guide slot, not tomorrow's
visual digest or the separate ordinary TG import21:40 UTC.

Opus evening review received and read from PR816. A diagnostic-only correction
is prepared: `last_reason` is interpolated into the validation retry prompt,
so replacing catch-all with a different reason is not behaviour-neutral.
An extra structured log distinguishes repair quotes absent from child scope,
including a parent-quote boolean, while preserving the existing reason, prompt,
rejection and candidate. Valid baseline test fails only on absent diagnostic;
24 grounding/media-helper tests pass after. Initial harness had a wrong fake
signature and then a misspelled test path; those are not valid passes or
production failures. No provider or production writes in controls. Not merged
or delivered. Recurrence and locked-title semantic changes still require
real countercontrols; reason/date/identity guards unchanged.


### 2026-11-04 20:25 UTC — diagnostic-only release delivered

PR 817 merged after all four required CI passed (workflow 41902045872).
Events-only runtime is d148bd509fa851f01beff9e48e44f8addcb817ed, immutable
sha256:dc8ca8dfd2351dd51d23e5bcd252c381d3905e6c43349c72b198f21f8c97b0dd.
Full 4,443-file proof, initialized isolated diagnostic control, unchanged
rejection/retry-prompt/candidate controls, wrong-SHA refusal, HTTP isolation,
container cleanup and live runtime/settings/SQLite/health passed. Kotopogoda
unchanged. Actual container started 20:23:51 UTC. A premature verification
read correctly rejected old SHA during recreation; final new-container read
passed. This logging-only change does not restore a rejected source by itself.
A future natural matching failure is needed to establish the live root cause;
no source replay or identity/date guard relaxation was performed.

Historical media 58741 was allowed to finish before cutover: attempt 832
timed out normally at 20:22:05, next existing retry 21:22:05. The previous
absence of a source_rehydrate completion log does not prove a rehydrate hang:
read-only state has four sources and 157 approved/pending posters; the existing
rehydrate count guard returns early. Materialization then iterates these rows.
Raw-SHA conflicts also occur on the fast exact-v2 path, so 51 conflicts are
not 51 downloads. Exact costly stage and root cause still need evidence.
No job was marked done, replayed, deleted or manually reset. Other historical
media job 67365/Event 6824 was running after normal startup; no claim was
interrupted at cutover. See media7114-stage-correction-2022.md.

The four fixed jobs 146263–146266 retain their original timestamps and
November 5 00:05 UTC due time. Normal TG 21:40 UTC and the media reset, parser,
VK, visual guide and November 6 promo pair checks remain outstanding.
Receipt grounding-diag-release-receipt.json and verification logs are retained
in the existing incident artifact directory. Four incidents remain open.


### 2026-11-04 23:20 UTC — normal TG remote terminal, local import progressing

Normal scheduled TG11638 remote scan finished23:00:49 with57 sources and123
messages. Local ops is still running; remote completion is not acceptance.
Source/candidate anchors confirm new10526/10527 and signalkld12400 children
10528/10529 with EventSource13992397–13992399/13992401. Next message
locostandup4362 started; this is actual progress, not heartbeat inference.
One sampled signalkld12399 child is technical distinct_not_grounded; three
locostandup4361 children are past-event exclusions, not technical failures.
These are bounded samples, not final totals. Whole-run/own-digest outcomes
still require terminal receipts. No competing intake or restart.

PR821 prevention image remains fully qualified and undelivered, no rebuild or
cutover attempt. Actuald148 healthy. Four deferred jobs146263–146266 retain
original timestamps and Nov5 00:05 due, no seconds-loop recurrence. Keep
media00:10/parser02:30/VK04:15/guide08:30 and Nov6 pair checks. Evidence:
tg-normal-progress-2318.json, tg11638-local-2320.json and
tg11638-early-anchors-2320.json in retained recovery directory.


### 2026-11-05 00:15 UTC — media reset observed, targets not yet processed

Budget-reset checkpoint does not accept Event9779 publication: actual media
successor144431 and dependent100934/100935/100937 remain pending, old done
143291 and last_result URLs are stale. Exact four146263–146266 retain original
timestamps and due00:05, no seconds-loop recurrence but no execution receipt.
Worker is actively completing other media jobs; due queue has303 jobs (not
303 missing events). Actual500/503 provider failures continue. No queue reset,
recommit, budget bypass, markdone or public replacement. Repeat exact-target
checks and read current public outputs after actual terminal.

Normal TG11638 local import continues through kldevents5161/5162/5163 with
fresh00:11 transitions, no terminal acceptance yet. Qualified PR821 remains
undelivered to preserve healthy intake. Actuald148 healthy. Retained evidence
media-reset-check-0015.md and its linked JSON/logs. All later normal parser,
VK, visual-guide and promo-pair checks preserved.


### 2026-11-05 00:55 UTC — stale whole-batch snapshot delays midnight targets

Read-only evidence narrows the queue delay beyond provider failures. At00:45
festival_pages10382 timed out after900024 ms; the same worker immediately
picked another old festival_pages10386. Exact media144431 and146263–146266,
due00:05, still had zero attempts at00:50. _run_due_jobs_once_locked selects
one due snapshot and drains it entirely; job_outbox_worker cannot reconsider
newly due jobs until the old batch ends. This is mechanical queue freshness,
not a semantic model/date problem or proof of a dead worker.

Minimal candidate adds optional private max_jobs to the existing drain and
sets it to1 only in the background worker. It finishes the existing claim,
releases the existing lock and reselects by existing priority. Explicit drains
keep their unlimited default. No new worker, persistent state, retry, timer,
quota bypass or changed task priority. Six net functional production lines;
public projection idempotency and all source identity/date rules unchanged.

Real isolated SQLite control enqueues a higher-priority media job during the
first of two festival handlers. Original code incorrectly runs the second
festival; explicit-full-drain contrast still passes. Initial fake handler had
a wrong festival signature and is invalid evidence; corrected baseline has
one distinguishing failure and one passing full-drain contrast. After-fix
checks and release qualification remain pending. No provider/production writes
or import interruption. Actuald148 and qualified-but-undelivered834 candidate
remain unchanged. Evidence media-reset-exact-0050.json, media-tg-worker-0052.log,
outbox-selection-valid-before.log, outbox-fresh-selection-plan.md retained.


After-fix expanded outbox run:29 passed,3 failed. All three failures reproduce
with the exact original834d drain functions restored in memory (test_job_expired,
weekday navigation expectation and running month-page follow-up expectation).
They are not new passes or corrected by this change. Both distinguishing
background-refresh and explicit-full-drain controls pass after; current-fixture
valid baseline has1 failure/1 pass. Provider audit PASS, unapproved0/debt0.
Existing CI now includes the new two-case queue regression. No model call,
Event/source replay or production data write in these controls.


### 2026-11-05 01:14 UTC — combined queue and ordinal candidate qualified

PR822 merged after all four CI checks and contract-tests passed. Clean exact
main682e9031d47769cadfe6c42803dab88a8a5c191b built and qualified as
runcoveer/events:intake-outbox-fresh-20261105, immutable
sha256:7a7658e1c3b9b871d84212f06f13f33acecaf9268b4b77039e81fa0a9ae479e4.
Full4,445-file source proof, actual d148 ancestry, initialized isolated actual
background worker/SQLite refresh control, inherited ordinal/native/date/media
controls, wrong-SHA refusal and final HTTP isolation passed. Early HTTP reset
was startup timing; final check passed. Own preflight container removed.

NOT DELIVERED: ordinary TG11638 local import still progresses at01:12 through
kldevents5178 and ecoklgd3629. Actuald148 remains healthy, no cutover attempt
or interruption. Host exact cutover/verify scripts staged; old undelivered834d
ordinal-only candidate is superseded. Target9779 media/projections and exact
four146263–146266 still pending at01:13; stale URLs not accepted. No queue
reset, source replay, public replacement or budget bypass. Need safe terminal
intake release followed by real worker/public receipts. Receipt
outbox-fresh-qualification-receipt.json and linked logs retained.


### 2026-11-05 02:38 UTC — normal parser slot starts alongside TG intake

The normal source parser02:30 slot started ops11643 at02:30:04, run
`a737808d41d3433193e91ce5ec508d40`. Real Qtickets remote run completed and
returned two files around02:35; theatres remote run was still RUNNING.
TG11638 meanwhile completed kldevents5200 as MERGED10313, then5201 rejected
and meowafisha9088 started. This proves actual source progress and normal
scheduler submission, not final parser/import acceptance. Both local ops still
running; no restart, competing CLI intake or duplicate remote kernel.

Combined682e9031d candidate remains fully qualified and undelivered; actuald148
healthy. At02:36 Event9779 successor144431/projections and exact four
146263–146266 remain pending, with old receipts explicitly not accepted.
The safe-release gate preserves active imports. Need both terminal results,
then Events-only delivery and ordinary worker/publication receipts. Existing
festival restart contract excludes blind interruption. All pending normal
VK04:15, visual guide08:30 and November6 promo-pair checks preserved.
Evidence tg-parser-local-0236.json, parser-slot-0236.log,
media-reset-exact-0237.json and outbox-await-runtime-0238.txt retained.


### 2026-11-05 03:15 UTC — exact Philharmonia source failure identified

Normal parser11643 Philharmonia remote version180 failed before writing any
catalogue JSON: ValueError detail description missing for the official
velikie-shedevry-muzyki-bakh-rekviem-motsarta page. Missing optional status client
was only a warning, not the failure. Live read-only HTTP200 confirms this
page has programme and performer paragraphs but no prose-description block.
The collector incorrectly requires optional prose and aborts the whole
catalogue. This is HTML extraction/coverage, not model/provider or date logic.

Minimal correction reads those existing paragraphs only when prose is absent;
empty page or heading/buttons-only content still raises. Prose remains primary,
no invented text, skipped exception or fake-success partial catalogue. Two net
functional lines, no worker/state/retry/provider change. Distinguishing current
fixture before1fail/8pass, exact error matches remote; after validation pending.
No production Event/queue writes, competing kernel or intake. Existing accepted
source recovery operations remain untouched. Related July27 source-coverage
record remains a required regression contract. Retained remote log and exact
HTML plus philharmonia-program-plan.md provide provenance. The first two local
probe invocations lacked optional bs4 before any HTTP; corrected fetch used
stdlib inspection, production packages untouched. Test-only existing bs4==4.15.0
and soupsieve==2.10 installed into retained artifacts, no project dependency
change or global installation.


After-fix9 Philharmonia controls and2 prior queue contrasts pass (11 total).
A read-only live full catalogue run extracts39/39 listing occurrences, all with
nonempty official detail text; no Event/Job writes or model calls. This accepts
HTML extraction only, not source-to-canonical Smart Update or production
catch-up. Existing fail-closed empty/heading-only page and exact prose-primary
controls pass. New fixture is minimal real performer/programme sections;
existing listing date and output ISO string tests remain unchanged. Prior
local probes lacked test-only bs4; optional package versions from actual
container were installed only under retained artifacts. Required CI now covers
the Philharmonia tests. Delivery and real pipeline acceptance remain pending.


### 2026-11-05 03:24 UTC — combined programme fix qualified and normal TG terminal

PR824 merged all four CI green. Exact main3948742db73a13e859553529b7fb79a99ec8adb9
qualified as intake-philharmonia-program-20261105, immutable
sha256:539b919052c3d2dedd673e7e4ba49ee5ce845d29096dbc61399dcfd0ebfd1356.
Full4446 source proof, actuald148 ancestry, initialized isolated actual
programme-only/empty/prose-primary plus inherited queue/ordinal/native/date/media
controls, wrong-SHA refusal and HTTP isolation passed. Preflight removed.
NOT DELIVERED: source parser11643 healthy active, real dramteatr index130/263.
Old682 candidate superseded, no second build needed. New Philharmonia catch-up
and canonical provenance reconciliation remain outstanding, no competing intake.

Normal TG11638 terminal partial03:11:18:123 exact message receipts,16 created,
56 merge operations,72 accepted source anchors verified over65 unique cards,
zero missing anchors.26 messages have a technical breakdown;29 terminal-error
metrics also include exclusions. No blind replay. Own5477/5478 merge existing,
old nine own-digest cards not repaired. Full exact receipts retained.

Media144431/9779 executedok03:18:43, dependent public jobs stillpending.
Poster35057 rejected/automated_semantic_conflict, no bypass. Two old reviews
146263/146266 completed nochange but created pending near-seven-second followups
146486/146487. Repeated loop not yet proved, sustained post-reset prevention
NOT accepted. Other146264/146265 pending. Earlier future-job stability is not
later reset acceptance. Need producer/input evidence before more code changes;
no markdone/reset/requeue or budget bypass. Evidence in qualification receipt,
tg11638-terminal-summary.json and media-reset-followups-0323.md.


### 2026-11-05 04:10 UTC — combined release delivered after parser terminal

Actual production now runs merged main `3948742db73a13e859553529b7fb79a99ec8adb9`,
image `intake-philharmonia-program-20261105`, immutable
`sha256:539b919052c3d2dedd673e7e4ba49ee5ce845d29096dbc61399dcfd0ebfd1356`.
This includes ordinal classification, fresh background selection and official
programme-only HTML extraction. Prior complete qualification remains valid;
no redundant build or source replay was performed. Explicit live source, SQLite,
model settings, identity/date/cinema/media and HTML controls pass. Health ready,
DB ok, scheduler preserved and Kotopogoda unchanged.

Normal parser 11643 terminated partial at03:57:57:263 occurrence results,
4 created,4 ticket updates,253 unchanged,2 technical failures plus the separate
Philharmonia source failure. Exact result roster and source links for the four
new Events10541–10544 and four updated cards retained. Per-source updated counts
also include unchanged occurrences; do not present them as unique card updates.
No blind replay. Philharmonia compensating import and39-row canonical provenance
reconciliation remain outstanding after this release; extraction39/39 is not
canonical acceptance.

The original cutover refused before changes because exact media claim101711
for Event7117 was running. A recorded one-off decision preserves only that
claim under the existing independent media stale/backoff contract; no source,
MCP, festival or publication task was interrupted. After restart the exact
claim became error/stale with existing next retry05:09:47, without manual reset.
Its eventual terminal/successor outcome remains to verify. No product flag,
worker pause, queue edit or new retry mechanism was introduced. Normal VK04:15
and public projections remain independent acceptance criteria.

Media reviews for all four original loop cards have now executed, but several
near-seven-second successors remain pending. Source rehydrate logs show added=0
and exact-object merges; a repeated execution loop and its exact branch are not
yet established. Sustained post-reset prevention is not accepted. Event9779 media
review done, public Telegraph/calendar pending and TG due07:50. Rejected original
poster remains rejected; no semantic or feature-budget override.

Retained evidence: philharmonia-delivery-receipt.json,
philharmonia-media-claim-restart-0409.md, philharmonia-exact-media-cutover.log,
philharmonia-live-verify-0410.log, parser11643-terminal-0410.json and
media-followup-producer-0350.md in the existing retained incident directory.


### 2026-11-05 04:31 UTC — normal VK progress and media follow-up explanation

Normal automatic VK11644 started04:15:02, run e830bb4dbd0c467e96a3822a3f049ff7,
batch auto:1793852102. First carrier89182/source26841 produced CREATED10545/10546,
child states26659/26660, EventSources14003507/14003508 and both carrier links.
Next accepted source26849/carrier88993 MERGED existing10537, source14003509.
Four no-event/exclusion rows and another active carrier follow. This is progress,
not acceptance of the entire scheduled package; no competing intake was started.
Docs825 merged after all four CI green, no runtime restart.

Media follow-up correlation is now stronger than the earlier rehydrate hypothesis:
all four pending seven-second jobs were created milliseconds after pair reviews
31480/31616/31465/31468 resolved duplicate. Their original approved posters have
geometry error rows from November3, older than the existing20-hour retry horizon.
The existing pair-resolution branch arms another eligible geometry pass. None of
these follow-ups has executed again. Do not infer a rapid loop or suppress this
existing continuation from source-rehydrate added=0 alone. No new producer change
is justified; actual subsequent bounded execution still needs verification.
Evidence media-pair-followup-explanation-0432.md and linked raw JSON retained.

The existing Philharmonia recovery request is pending, attempts0. A bounded
one-source existing-pipeline driver is prepared, not applied; read-only plan
correctly refuses active VK11644. Only after terminal normal VK and safe heavy
idle, with90 minutes plus5 before07:05 guide, may it run once. Stable run ID
INC-2026-11-05-philharmonia-source-catchup. Reconcile unknown outcomes first;
never run the old programme or37-row recovery plan.


### 2026-11-05 04:52 UTC — ordinary VK terminal, bounded Philharmonia catch-up started

Normal automatic VK11644 terminal partial04:46:26:12 terminal posts, zero
unresolved,2 created10545/10546,4 updated8317/9783/9940/10537. All6 accepted
source anchors and carrier links verified. Seven no-event carriers, one product
exclusion and one partial technical carrier88987/source26820; it accepted8317
but refused three children26663–26665 for ungrounded location. Their exact official
ticket cards9122/7179/7180 already exist and were confirmed by today's official
Estrada parser11643. These are additional-source failures, not three missing
canonical Events. No blind partial replay or identity/location bypass.

After terminal normal imports, idle MCP and independently checked heavy work,
read-only Philharmonia plan passed. Applied once through existing run_source_parsing
with only_sources=['philharmonia']; ops11645 started04:49:46, stable run ID
INC-2026-11-05-philharmonia-source-catchup. Remote version181 completed and returned
all39 occurrences. LOCAL processing remains active; first unchanged7519 and new
10547 confirmed in its event-result log. Remote completion is not canonical
acceptance. Do not rerun the driver; reconcile the existing run if delivery of
its result is lost. The pending recovery request is a backstop and can remain
pending until this explicit run settles it. Request status alone does not mean
no recovery is active. No unrelated source/kernel or new product mechanism.

Retained exact input: philharmonia-kaggle181-results.json (160100 bytes), local
apply log and philharmonia-catchup-state-0451.json. Normal VK final receipts and
vk88987-residual-explanation.md retain the distinction between rejected additional
provenance and absent canonical Events. Remaining public projections and all
previous scheduled checks still need verification.


### 2026-11-05 05:25 UTC — ready projection starvation reproduced

Job100934/9779 is still pending after its own latest media144431 completed
at03:18:43. Actual worker logs continue choosing unrelated media/VK jobs;132
media jobs due at05:17:54. Fresh snapshot selection is live, so this is a separate
priority rule: ALL ordinary media precedes ready public projections, despite
existing per-event dependencies already expressing the required media ordering.

A real SQLite distinction fails before: ready Telegraph waits for unrelated
media. Its blocked-own-media contrast passes. Proposed correction swaps only
ordinary media and ordinary projection lanes in the existing priority map:
six scalar values, zero net functional lines, two explanatory comment lines.
VK/fresh Telegram rank-2 and missing-media VK rank-3 remain unchanged; all due,
dependency, lifecycle, budget and public spacing checks remain intact. No new
rank, state, retry, aging mechanism, worker or flag.35 focused controls pass;
six broad Telegram timing failures reproduce on unchanged original worker and
must not be attributed to this patch. No provider or production queue writes
from controls, provider audit clean. This change is NOT built or delivered.

Philharmonia11645 remains active and progressing; normal3D scheduler11646
also started at05:15. It is independent of the source heavy gate and does not
justify interrupting either job. Qualification may proceed in isolation, but
Events cutover must await their safe terminal state. Prepared ready-projections
scripts expect actual394/539b and the Philharmonia-program tag; read before use.
No build, preflight or new image yet. Exact evidence in outbox-ready-projection-plan.md,
outbox-ready-before.log, outbox-ready-focused.log and original-controls.log.


### 2026-11-05 05:38 UTC — ready-projection candidate qualified, not delivered

PR826 merged all required CI and contract tests green. Exact main
`ae5d459ceeab30e86703ac48c63287a7fbdc9b8e` qualified as
`intake-ready-projections-20261105`, immutable
`sha256:f023a4e0f27fce2bb52282893fe982e3217bd56412950bb0cfbb6516ca5a9fb9`.
Full4447 source/actual394 ancestry and unchanged dependency inputs passed.
Initialized isolated inherited controls and actual ready-projection/blocked-own-media
SQLite contrasts passed; wrong SHA refused and HTTP isolation verified. Owned
preflight removed. First probe had an operational NameError from missing main
import; corrected harness reran completely on fresh isolated databases. Image
source was unchanged, so no rebuild. This is NOT DELIVERED while11645 processes
healthy local source data. Do not rebuild or interrupt the import for cutover.

Correction to earlier test characterization: five baseline failures are quiet-time
spacing expectations, the sixth is absent local Telethon, all reproduced using
the unchanged original worker.35 focused and39 broad passing cases are valid;
the six failures are not successful checks. No local package/prod config change.

Exact source181 reconciliation currently finds one active canonical card with
an exact source URL/date/time anchor for every39 official catalogue occurrences,
zero ambiguous anchors. Local source pipeline has not reached terminal, so this
coverage check is not final run acceptance. The exact formerly fatal Bakh/Requiem
source is now CREATED10565 with EventSource14003531, December6 18:00. Its venue
name/address agree with the current official homepage; no speculative venue
profile correction was made. Evidence philharmonia-partial-reconcile-0538.json,
philharmonia-crash-source-receipt-0536.json and official https://filarmonia39.ru/.

A new substantive audit handoff1793856400.415239 was sent in the existing
coordination thread with the Markdown report and PR826. No recipient response
after two bounded reads; status sent_unconfirmed, not accepted. Test limitation
correction followed without a new audit request. No new chat or auditor schedule.
Retained ready-projections-qualification-receipt.json records all evidence and
staged exact host scripts. Actual394 remains healthy; no production switch.


### 2026-11-05 05:49 UTC — PR826 delivered; Philharmonia recovery accepted

PR826 is delivered from merged main ae5d459ceeab30e86703ac48c63287a7fbdc9b8e, immutable image sha256:f023a4e0f27fce2bb52282893fe982e3217bd56412950bb0cfbb6516ca5a9fb9. Original Events-only cutover passed idle guards without exceptions; runtime, SQLite, settings and inherited controls pass, ready=true, Kotopogoda unchanged. Full 4447-file qualification and initialized isolated ready/blocked SQLite controls passed. No new state, worker, retry, budget bypass or semantic change.

The actual normal worker completed Event9779 ICS100933 at05:42:19, Telegraph100934 at05:43:00 and calendar post100935 at05:43:07, after its own media had been done since03:18:43. Public Telegraph read confirms the current title «Песни о любви», November27 18:00, Зеленоградск, Железнодорожная1 and updated description. The existing page URL retains its old slug; that does not mean its body is stale. TG100937 remains legitimately due07:50 UTC. Rejected poster35057 remains rejected, with no override. Calendar public read is tracked separately.

Philharmonia existing-pipeline catch-up11645 completed SUCCESS05:36:49:39 rows,19 CREATED10547–10565,20 unchanged, zero failures. Exact official URL/date/time reconciliation proves39 unique active source anchors over39 unique Events, no ambiguous or missing occurrence. The previously fatal programme-only page now anchors Event10565. Do not run the catch-up again. The result's empty added_event_ids arrays are a reporting limitation: the actual event-result log and exact anchors establish the19 creations.

Operator-script correction: catch-up called the existing settlement helper while its request was still pending; the helper only updates running requests, so it silently left this single request pending. After successful ops and39-source reconciliation, a one-time explicit transaction settled only that exact unchanged pending Philharmonia request to done at05:47:59. Request attempt count remains0 because no scheduled claim occurred; no Event or Job row changed. Read-only plan, guarded script and before/after receipt retained. This is correction of the operator driver, not a new product recovery path or another import.

Evidence retained under the existing incident artifact directory: ready-projections-delivery-receipt.json, ready-projections-live-verify.log, ready-projections-cutover.log, philharmonia-final-reconcile.json, philharmonia-terminal-summary.json, philharmonia-settle-exact-plan.json, philharmonia-settle-exact-applied.json, media-ready-postdeploy-0547.json and9779-telegraph-public-0550.html. Old daily duplicate pairs and nine own-digest cards remain untouched. All five incidents stay open for remaining normal-slot, media, duplicate and promo-pair acceptance.


### November5 08:06 UTC — guide prevention delivered, rich-card repair still pending

Normal guide11648 accepted terminal transport and exact8 source/action anchors:30 sources47posts0 errors,2 creates6 update operations across4 old rows. New718 repeats published716 after a model paraphrase; separate incident INC-2026-11-05-guide-title-drift-duplicate documents confirmed source identity and narrow semantic-routing fix PR828, now delivered544379c8. All required CI, full4448-file proof, isolated/live controls and health pass; no source replay, canonical merge or public canary.

Event9779 public rich message4678 was read through shared Telegram E2E and still shows old «Верни мне музыку». Normal job100937 was repeatedly shifted behind new fresh announcements (07:50→08:03:47→08:13:59) while Telegraph/calendar current content is already verified. Existing public_repair_priority mechanism was applied once to this exact unchanged pending job after read-only plan and identity/dependency checks. It changes only existing payload priority/reason; no new job, Event write, deadline change, spacing bypass or direct provider edit. First operator script had a syntax error before DB access; corrected before plan and apply. Receipt9779-public-repair-priority-applied.json. Actual rich public update is still NOT accepted and must be read after08:13:59 UTC. Do not reapply metadata, recommit old requests or requeue.

Auditor morning report PR829 read. Read-only follow-ups: six due/near-due media jobs on past Events have>=100 attempts (maximum1494), so historical timeout issue is a class, not only58741. No mark done/reset or release of old dependent publications. Own channel61830 touched118 canonical Event IDs in30 days;35 lack another identity_bearing source and31 lack any other source. This does not prove35 unique real-world events, because some may be false duplicate cards. It disproves an immediate zero-loss assumption; source remains enabled pending bounded provenance/author-original reconciliation. Missing one-anchor/different-title #821 control remains follow-up, not evidence of a live false merge. No identity/date guard relaxation.


### November 5 08:50 UTC — normal VK terminal and public rich-card repair

Normal automatic VK11651 (08:15 slot, run fbfd4ff9c5f24b2ead75af9f4d147a28) ended PARTIAL at08:33:46:12 terminal carriers,0 unresolved,1 CREATED10566,1 MERGED8870,5 no-event,2 missing-location product exclusions and3 technical posts. Both accepted children have exact current EventSource14025563/14025564 and carrier links. Technical outcomes are verification failure, distinct_not_grounded and identity LLM unavailable; they were not replayed. Retained vk11651-terminal-summary.json and vk11651-receipts-0845.json distinguish technical failures from exclusions.

Event9779 ordinary rich TG100937 completed08:14:15.751223. Actual shared-Telegram read shows message4678 edited08:16:51 with current title «Песни о любви», November27 18:00 and Зеленоградск, Железнодорожная1. Old description paragraph remains: title/date/address repair is accepted, not every body or media revision. The existing priority metadata was applied once, never repeated. Rejected poster35057 remains rejected. Receipt9779-rich-public-0820.json; no new public post or recommit.

Ordinary visual digest11652 started08:33:46 immediately after VK released heavy work, completing08:34:16, issue330. This proves the normal08:30 slot waited instead of skipping. Actual public VKpost382 (postponed381) is confirmed, source timestamp08:43:59; TG511/530 and MAX URL are provider receipts, with their public acceptance separate. It contains5 excursions and one Sovietsk walk. Existing visual dedup summary:15 raw,13 display,2 suppressed. Visual schedule intentionally differs from new-occurrence digest published-reference suppression; do not treat appearance in a daily schedule after an earlier new-event digest as another duplicate bug. Exact public and coverage records: guide330-public-0848.json, guide330-coverage-idle-0850.json and guide330-dedup-actual.log.


### November 5 10:26 UTC — ordinary VK 10:00 packet reconciled

Ops11654 ended PARTIAL10:17:44, run88627a41ad6740ff958e40e66b748e04, batch auto:1793872800. Twelve terminal posts, zero unresolved. Eight CREATED records10567–10574 and three accepted existing-card outcomes9783/9940/10537; the latter current receipts are NOOP_EXACT_REPLAY, not evidence that three cards changed content. All11 exact child/source/carrier links verified. Earlier anchors for the three no-op cards remain preserved and are not attributed as new source writes.

Carrier89101/source26835 (wall-41284227_9629) accepted six children10568–10573 and refused two states26702/26706 for identity_final_retry / merge_distinct_not_grounded. The whole carrier has failed_technical status but is not six lost events. Seven other terminal non-import posts comprise six semantic no-event and one missing_location exclusion. No blind replay, forced create or identity guard relaxation. Targeted logs show both refused candidates compared against sibling10568 with confidence0.95/0.85; the actual verdict evidence still needs bounded analysis before changing semantics. Retained vk11654-terminal-summary.json, vk11654-receipts-1025.json and vk11654-identity-residuals.log.


### November 5 12:13 UTC — normal guide accepted; transient writer contention reproduced

Ordinary LIGHT11658 succeeded11:48:08 after remote30sources48posts. One new undated guide720,7update operations,8exact source/action anchors over7unique rows verified; no missing sources or final date/time/status mismatch.720 and715 have digest_eligible=0. No replay or publication claim from the import alone.

While local enrichment ran, ordinary outbox/promo/VK continuation writes hit database-is-locked. Health11:48 returned503 with ready=true/db=ok and consecutive outbox OperationalErrors, then recovered fully after terminal without reset/restart. Actual provider progress continued through11:48:08; Lite3.5 failure reached existing Lite3.1 successfully. Source code awaits occurrence/profile enrichment inside the write transaction. An isolated actual-import control now reproduces another writer being locked during that await and committing immediately after import terminal, preserving one exact occurrence/source anchor. Production writes0/provider calls0. This proves the mechanism, not an exclusive live Python task owner. Separate minimal transaction-boundary fix is planned but not implemented; do not add a retry or reset the queue.

PR834 generated parser authority prevention is merged and fully qualified as exact main ba28b44d04781fa358b2a70884b49ba037ea207c, immutable sha256:f18ab57d092cad9f51493c2d12ae29ba931a1acddcc74bfb32cc1df796d70779, image intake-parser-fallback-20261105. All four mandatory CI jobs and collection gates green, full4449source, initialized isolated actual generated/native pipeline controls, wrongSHA and HTTP isolation passed. First probe over-required zero isolated jobs for an accepted positive update while production enables interest_club_relation; explicit fixture-only setting now matches unit environment, no production config changed. Negative-case protection is unchanged. Owned preflight removed. Candidate NOT DELIVERED: qualification completed shortly before parser12:15UTC, so preserve that ordinary slot. Actual remains healthy aa489fde, no cutover attempt. Recheck actual parser then safe idle; no rebuild required.

Retained parser-fallback-qualification-receipt.json, parser-fallback-preflight-fixture-correction.md, guide11658-terminal-summary.json, guide11658-writer-lock-observation.md and guide-import-write-lock-repro.py/.json/.log document exact layers and remaining work. No old accepted operation, programme,37-row plan, Philharmonia driver/settlement or receipt reconciliation repeated.


### November5 12:26 UTC — guide write-lock prevention candidate

The reader-visible failure is delayed ordinary outbox/promo work and transient503 health during a normal guide import. The actual isolated downloaded-result importer now proves the mechanism: another SQLite writer is locked during an existing enrichment await, then succeeds after import terminal. Provider calls0/production writes0. Nine distinguishing controls cover downloaded and local paths, occurrence/profile awaits and the next local scan with preceding pending source writes. All9fail before for database-is-locked, all9pass after. Successful enrichment facts survive; injected provider errors remain existing partial outcomes; reimport twice leaves exactly one occurrence/source anchor. All156guide tests pass.

Candidate adds existing conn.commit before five external boundaries: occurrence/profile enrichment in both paths and local next-source scan. Two concise source/count/transaction log calls identify the release boundary. Seven functional lines plus one comment. Initial four-line plan needed three more functional lines for the discovered local scan boundary and observability; this is explicit scope accounting, not a new retry, lock, worker, state, pool or semantic rule. Completed source rows commit before optional enrichment, which already has per-source failure handling; existing upserts preserve replay idempotency. No production queue reset or source replay.

Not delivered. This candidate includes already-merged parser identity prevention834. Prefer one combined safe release if it qualifies while parser11659 remains active, rather than two restarts. Existing ba28/f18 candidate remains qualified but not delivered until superseded by a fully qualified combined image. Never treat its qualification as proof of this transaction change. Mandatory release controls: latest-head CI, merged exact main and actual-runtime ancestry, full source proof, initialized isolated actual concurrent-writer and parser identity controls, wrongSHA, HTTP isolation, safe idle and preserved models/Kotopogoda. Next normal guide import must show source/occurrence receipts and absence of write contention through enrichment; isolated tests alone do not close the incident.

Evidence: guide-transactions-before.log, guide-transactions-after.log, guide-transactions-regression.log, guide-import-write-lock-repro.py/.json/.log and guide11658-writer-lock-observation.md under retained incident directory. Ordinary source parser11659 remains active on unchanged aa489fde; Qtickets remote completed319.3seconds with two output files, theatres still RUNNING at12:24. Remote completion is not final local import acceptance. No duplicate kernel or competing intake.


### November 5 12:51 UTC — combined guide transaction release qualified, not delivered

PR835 merged after all four required latest-head CI jobs passed, including the new concurrency regression in the existing guide job. Exact main a82c2f6b14892347305814b904bd128edf7c703e, image intake-guide-transactions-20261105, immutable sha256:bd207bfe70d565da6826f2e588dc0d53c41c01ffb44c7b22f17379feb1a95102. Actual aa489fde ancestry, unchanged dependencies and all4450tracked source files proved. Initialized isolated candidate passed nine actual concurrent-writer/failure/reimport controls, three generated/native parser identity controls and inherited source/guide/media/adapter checks. Wrong SHA refused before writes; HTTP ready=false/db=skipped proves isolation. Owned preflight removed; zero provider or production writes.

Candidate is NOT DELIVERED. Current scheduled parser11659 is locally processing the downloaded310rows; real event_result transitions reached dramteatr128 by12:49. Earlier Qtickets remote completion alone was not final acceptance. No restart, CLI intake, kernel duplicate, claim reset or partial replay. Staged exact host cutover/verify scripts require fresh safe idle after terminal plus preserved model settings and Kotopogoda. Actual remains aa489fde. Old ba28/f18 candidate is now superseded by this fully qualified combined image and should not be delivered separately.

A bounded read-only old-duplicate check still finds active canonical pairs10393/10522 and10442/10511 with matching persisted date/time/venue; distinct sector10396 uses ticket52231 and remains a separate record. Stored VK URLs are shared within these groups, including the distinct sector, so deleting a public URL based only on the duplicate card would be unsafe. No canonical/public change. Further managed merge/publication ownership reconciliation remains independent of release acceptance. See old-duplicate-pairs-bounded-plan.md and current snapshot; a stored URL is not a fresh public provider receipt.

Release evidence: guide-transactions-qualification-receipt.json, guide-transactions-build.log, guide-transactions-preflight-probe.log, guide-transactions-wrong-sha.log, guide-transactions-preflight-health.json and guide-transactions-preflight-cleanup.json in retained incident artifacts. Live next normal guide import must still confirm source receipts and absence of write contention through enrichment; do not close from this isolated qualification.


Read-only public follow-up confirms shared VK13415 actually describes sector «Партер», posted November4 08:04:09UTC; protect it during any future canonical duplicate repair. Shared movie13437 is the matching November10 18:00 announcement. Connected authenticated social reader, no publication mutation. Retained old-piano-shared-vk-public.json and old-movie-shared-vk-public.json distinguish live public evidence from earlier stored references. Invalid initial locator arguments were rejected before provider access; known typed locator shape is now recorded in the bounded plan.


### November5 13:39 UTC — normal VK interleaving observed before delivery

Scheduled VK11661 started13:30:02, run273edf2da94f4402a3ea7ca7833f8343, exact batchauto:1793885402. First confirmed MERGED10548 has carrier89516/source26937, child26725, attempt12150, EventSource14036595 and carrier link to wall-26560795_13502. Three no-event carriers, one grounding technical refusal and an active carrier remain; not a terminal package acceptance. Normal parser11659 reached247/310 and VK got actual row processing after the scheduled submission, so no skipped-slot assumption or manual callback. Both healthy imports preserved. Normal3di11660 local success is handoff evidence only, not independently verified remote render/publication. Before release, reconcile any remote heavy work in addition to ops. Actualaa489fde/e887 remains healthy; fully qualified combined a82c image is not delivered, no cutover attempted or rebuild. Evidence actual-ops-1338.json, vk11661-receipts-1340.json, parser11659-transition-1340.log and guide-transactions-await-live-1341.txt in retained incident artifacts.


### November5 14:03 UTC — combined generated-authority and guide-transaction release delivered

PR834/835 delivered as merged main a82c2f6b14892347305814b904bd128edf7c703e, image intake-guide-transactions-20261105, immutable sha256:bd207bfe70d565da6826f2e588dc0d53c41c01ffb44c7b22f17379feb1a95102. Fresh guards passed after normal VK11661 and parser11659 terminal, media147495 finished and active ops/MCP/background empty. Container process inspection showed only ordinary launcher, no RENDERING video sessions; normal3di11660 terminal metrics report1 rendered preview,0 errors, not merely queued handoff. No claim exception or reset. Existing verified source ancestry and full4450-file isolated qualification reused. Events-only cutover, explicit live runtime/SQLite/models/cinema/semantic hashes/scope/date/year/ordinal/queue/guide checks passed; ready=true/dbok/issues empty, Kotopogoda ID unchanged. Initial health probe during startup reset its connection, then normal ready200 succeeded; no second switch.

Normal VK11661 ended partial13:45:00 with12terminal carriers0unresolved, two CREATED10581/10582 and MERGED10548. All three current child/source/carrier anchors verified. Five no-event, two past-event policy exclusions, two technical refusals: ungrounded location and insufficient identity evidence. No blind replay or guard relaxation. Parser11659 then continued and finished partial13:50:16,310 exact logged rows: six created10575–10580, four updates9786/10398/10399/10541,292unchanged,eight failed rows. All10 changed-card parser source anchors present. Eight failed rows are not necessarily eight unique missing events: Estrada repeats the same title/date twice. Some Tretyakov failed-row logs show15:00 for piano sectors whose old canonical time is20:00; this is an observation needing exact source/candidate reconciliation, not permission to retime or create. Existing cards and accepted anchors preserved.

Retained guide-transactions-delivery-receipt.json, guide-transactions-cutover-1403.log, guide-transactions-live-1404.log, parser11659-result-roster.json, parser11659-terminal-1405.json and vk11661-terminal-summary.json. Next normal guide import must still demonstrate receipts and absence of writer contention; live source checks alone do not close that criterion. November6 actual promo pair spacing remains open. No accepted operation, source replay, old driver or settlement repeated.


### November5 16:41 UTC — ordinary video and next VK slot

Scheduled video_tomorrow11662/session1570 completed normally on the delivered runtime without operator compensation. Exact crumple-video version297 remote done16:27:40, local PUBLISHED_TEST16:29:02, errornull. Terminal report all five targets successful, required/blocking/fanout true. Public VKwall-231828790_2323 and Telegramkenigevents5489/story542 verified independently. Story video18.69seconds720x1280H264; no new kernel or resend. Existing cleanup deleted terminal dataset and local output. Sanitized target summary and minimal public receipts retained; unnecessary raw transport references removed from the local report. Static scenes_done0 was not a true per-scene counter, and earlier heartbeat was not counted as acceptance.

Next normal VK11663 started exactly16:30:00, run6522190c05fa4844b1a949110bc1df2c, batchauto:1793896200. First CREATED10583 has current child26737/source26963/EventSource14047630/carrier89578 verified. Package still active in the bounded read, two technical refusals separate from no-event and product exclusions; no full acceptance or replay claimed. Full guide18:10 live writer-contention acceptance and November6 actual paired promo spacing remain outstanding.


### November5 17:03 UTC — scheduled VK terminal receipts reconciled

VK11663 ended partial16:54:40 with12terminal carriers and0unresolved. Two CREATED10583/10584 and seven MERGED10197–10203; all nine current child/source/carrier anchors verified. Programme source26943 has ten children: eight accepted (seven merges and10584 creation), two product exclusions, and no technical child. Preserve its accepted records and do not replay the entire post. Overall six no-event carriers, two product-exclusion carriers and two technical carriers; the two technical posts contain three refused children. Earlier interim count of seven no-event was incorrect; this terminal census supersedes it. Technical reasons are ungrounded location repair/keep and unavailable adjudicator, not proof of nine missing Events or a source-wide outage. Runtime remains healthy on delivered a82c; no retry-state reset or semantic relaxation. Evidence vk11663-terminal-1703.json and vk11663-terminal-summary.json retained. Fullguide18:10 real enrichment/concurrent-writer acceptance, ordinary TG21:40 and November6 actual promo pair remain pending.


### November 5 18:52 UTC — normal guide source receipts and transaction fix accepted live

Ordinary FULL guide11666/run9423c70f282b remote completed18:42:22, local terminal partial18:47:29. All30sources and81posts processed. One new occurrence721 (November19 school career trip) and25 update operations over15unique rows. All26 exact source/action anchors and final date/time/status fields verified, no missing rows or local errors. Remote partial warning is one ProviderError at progulki_s_katey/833, not a source-wide/local import failure; no replay. New-occurrence digest331 has ordinary VK383 and Telegram delivery receipts; public read is separate and not claimed here.

The delivered transaction change now has live symptom evidence: six source-enrichment logs explicitly say transaction=committed. During18:42–18:47:30, ordinary outbox writes and promo tick completed while provider enrichment was active; zero database-is-locked log occurrences. Health ready=true/dbok/issues empty after terminal. This accepts prevention of the previously reproduced long writer lock on this actual normal import, not a claim that all unrelated outbox/provider failures are repaired. Some separate TG publication jobs failed while other queue work continued. No queue reset, lock exception, extra retry or manual guide callback.

Evidence: guide11666-terminal-summary.json, guide11666-receipts-1852.json, guide11666-progress-1850.json, guide11666-writer-live-proof.log and guide11666-health-1854.json in retained incident artifacts. Runtime stays a82c2f6b14892347305814b904bd128edf7c703e; docs do not require restart. Normal TG21:40 and November6 actual promo pair spacing remain mandatory.


## November 5 19:12 UTC — preserve independent queue release in promo correction

Actual runtime was independently updated to PR838 c7b945179 before this release; the candidate ancestry check included it, rather than restoring remembered a82. Qualified merged PR840 runtime9ec3247c585ec27de8e243ea4a9aad0bbcb74627 is now live, immutable751fa08c6afe8db518c08da923e7ba2427512dc65692427afca4e38db576432e. Health ready=true, DBok, model/promo/cinema/identity settings and Kotopogoda unchanged. No intake replay, new kernel or semantic guard change. Normal full guide11666 and concurrent writer acceptance remain as documented above. Promo's real evening send still needs ordinary tick/public receipt acceptance; details in the spacing incident. Preserve normal TG21:40 UTC and November6 actual paired spacing.


## November 5 21:04 UTC — evening audit received and real promo acceptance

Ester Show replied in the existing coordination thread at1793908134.692769. Full PR842 report read and merged after all four required CI green; review document is now published in main. This acknowledges the earlier #834/#835 handoffs: #834 is correct, #835 has no comments; the reviewer withdrew the Lite-default objection and blind own-source-disable suggestion after the supplied counts. This is verified review delivery, not independent SSH verification by the auditor or authority to alter guards.

Read-only follow-up after at least24 hours of #834 production runtime (November6 after14:03 UTC): count parser merge-gate calls and FINAL_RETRY/FINAL_DISTINCT outcomes over a precisely stated window. Do not infer daily rates from tonight's partial exposure or add an early deterministic title exemption. Prior recurrence/locked-fact proposals remain unimplemented and require their real contrasting controls, especially19099 refusal. Past-media expensive-stage root and drain timing remain observations, not permission for reset/mark-done or a guessed worker change.

Actual ordinary evening promo331 is publicly accepted at wall-231920894_13570,117m21s after source384, one matching copy/zero Anons in latest100. No release or source replay this turn. Runtime9ec healthy; preserve normal TG21:40 and November6 paired spacing. Retained auditor842.diff, auditor842-acceptance.json and promo331-acceptance-receipt.json.


## November 5 21:48 UTC — normal Telegram slot started with real transitions

Ordinary scheduled tg_monitoring ops11667 started exactly21:40:00 UTC. Its run ID c47272245b3441f6af07981cfcc9874c matches ledger tg_monitor:c47272245b3441f6af07981cfcc9874c. At21:48:02, eight of57 sources completed and three messages were scanned; source9 dramteatr39 started21:47:49.769376 after domkitoboya completed21:47:45.765865. These are actual source transitions, not notebook percentage. Neither remote nor local terminal is accepted yet.

Runtime9ec and ready=true/DBok/issuesempty are unchanged. No restart, source replay, duplicate kernel or CLI intake. Read-only helper tg-normal-20261105-progress-read.py and receipts progress-2148.json / health-2148.json are retained. Next check real transitions, then remote and local terminal with exact source/child receipts and technical refusals separate from exclusions. Preserve November6 scheduled parser/VK/guide and actual paired promo spacing.


Follow-up 22:55 UTC: the same remote TG run completed22:52:48 after57 sources and115 messages. A frozen identity-only roster confirms115 unique source/message pairs; original report is1,461,951 bytes and run-bound. Local ops11667 remains running; first candidate26748 for agropark39/2467 began22:53:45, no final acceptance yet. Remote completion must not be counted as local success. Helpers tg11667-local-read.py and tg11667-terminal-exact-read.py are prepared read-only; the latter requires terminal ops before it can collect final receipts. No source replay, duplicate kernel or runtime change.


## November 6 03:11 UTC — normal Telegram terminal receipts reconciled

Ordinary TG11667/run c47272245b3441f6af07981cfcc9874c finished local PARTIAL at02:47:14 UTC after remote completion22:52:48. All115 frozen source/message identities have final receipts, missing0 and extra0. There are18 CREATED10585–10602 and48 MERGED operations over65 unique accepted Events; all66 exact EventSource tuples (accepted Event, source URL and occurrence key) exist, missing0. No candidate remains attempt_started in this run. These are canonical/source acceptance results, not public projection acceptance. Interim window aggregates included context attachment and exact replay outside the exact message roster; those counts are superseded by this run-bound census.

Final message statuses:49done,3partial,8partial_terminal_error,22terminal_error,8skipped and25confirmed_no_event. Reason-level evidence identifies24 messages containing technical/identity refusals; a top-level error label alone is insufficient. Candidate refusals comprise19 protected identity retries and12 failed technical children. Source skip breakdown additionally includes two source_evidence_incomplete refusals, distinct from the child-state count. Policy exclusions are79 children:26missing_date,33missing_location,20past_event. Some partial messages preserve accepted children, so none may be replayed wholesale. Runtime metric messages_terminal_errors=30 counts labels and must not be presented as30 technical-only messages. errors_count=0 describes the top-level runner, not absence of these per-message refusals.

Own kenigevents/5487 and/5488 merge existing10333 and10332 with exact anchors14069727/14069793 and earlier independent kozia_gorka/mesto_sily_bar identity sources; no new Event from these two messages. Story-reference5489 is confirmed_no_event. This is bounded current-run evidence, not permission to disable the source or erase old own-digest cards. Created exhibition10594 startsOctober19 but endsNovember16; it is ongoing at import time, not evidence of a past-only import from its start date alone. No retime or queue mutation.

Normal structured parser11672 started automatically02:30:00, run d5fdac55eaef4fff8640870287a1baaa. Its local import progressed to row149/315 by03:08, with actual completed theatre rows; terminal source receipts remain outstanding. Healthy overlap was preserved without another CLI/kernel or restart. Existing runtime9ec remains delivered; no code or production configuration changed. November6 actual promo pair spacing and after14:03 parser-gate metrics remain required.

Retained evidence: tg11667-terminal-0308.json, tg11667-terminal-summary.json, tg11667-exhibition-own-0311.json and parser11672-progress-0309.log under the existing retained intake recovery artifact directory. Raw source payloads were not copied. This accepts complete normal-import accounting while retaining residual technical failures and separate public/media acceptance; the incident stays open.
