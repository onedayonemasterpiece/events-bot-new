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
