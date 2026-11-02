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
