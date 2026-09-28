# INC-2026-09-28 Lite overload and incomplete fallback coverage

Status: open
Severity: sev2
Service: Google AI gateway / source parsing / Smart Update / Telegram publication
Opened: 2026-09-28
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-09-19-tg-monitoring-runtime-starvation`, `INC-2026-09-27-vk-auto-video-reader-publisher-token`
Related docs: `docs/features/llm-gateway/README.md`, `docs/features/vk-auto-queue/README.md`
Registry: `inc_757e6e542a3f64b9a306dbb0`

## Summary and impact

The operator reported Google overload alerts and VK source parses ending in
`FAILED_TECHNICAL` without automatic retry. This is a mechanical availability,
routing and accounting issue, not evidence that event-semantic prompts need
rewriting. Gemini 3.5 was already registered and invoked, but not consistently
available as a fallback. Both Lite endpoints were failing during the incident.

## Evidence and timeline

All times below are UTC (the pasted Telegram display is UTC+2).

- 13:30: VK run `d8ab875361324ce59012035fb9733088` began. Its waiting-heavy notice
  names that same run; it is not by itself proof of a second competing job.
- 13:40: request `df5c01d2-53a3-4669-bde1-344c9a2f54bb` tried 3.1 Lite → 3.5
  Lite → Gemma 4: responses were 503, 503 and 500. Switching was working here.
- 13:40: request `a4127534-765f-4b22-9d9f-3f913f15db87` switched Gemma → 3.1
  Lite, received a successful 3022-token response against an 8821-token
  reservation, then produced a negative-TPM finalize alert for **Gemma**.
- 13:49: live env `GOOGLE_AI_FALLBACK_MODELS` was
  `gemini-3.1-flash-lite,gemma-4-31b-it`; deployed SHA was
  `9a68c377c4ca39cc5792e65bb81ba6079cca202e`.
- Around 13:53: fresh `/healthz` returned ready=true, DB=ok, issues=[],
  approximately 1210 MiB free on `/data`. The bot itself was healthy.
- Active-log sample from 13:00 through capture: 3.1 Lite had 37 HTTP 503s;
  3.5 Lite had 7 HTTP 503s plus one other failure; Gemma had 8 HTTP 500s.
  Successes interleaved: 29 on 3.1 Lite and 4 on Gemma. No successful 3.5 call
  appears in this sample. These are physical calls, not unique lost events.

Retained evidence and test outputs:
`/home/dev/artifacts/events-bot-new/20260928T134855Z-lite-fallback-20260928/`.
The retained directory also contains Fly/Depot-generated temporary build
certificate material, including `depot-key4000317115`; these files are sensitive
and restricted to mode 0600. Do not share the whole directory as a public bundle.
Live source: machine `148eddde9b5778`, `/data/runtime_logs/events-bot.log`.
The mirror was enabled with rotated files present. Initial investigation was
read-only; the authorized compensating replay below uses the normal import path.

## Root cause and contributing factors

1. Provider overload affected multiple models. Model failover helps partial
   failures but cannot guarantee availability when both Lite endpoints fail.
2. `tg_event_publish` cleared every fallback to prohibit Gemma for public copy,
   inadvertently excluding the sibling Lite. Its budgeted 4o fallback remained.
3. Server source parsing explicitly used `allow_model_fallback=False` and a
   one-attempt cap. 3.5 calls in this consumer also represent separate terminal
   adjudication stages, not proof of transport failover for every primary call.
4. Smart Update explicitly included 3.5 only for facts stages; other stages
   inherited the production env chain, which omitted it.
5. The gateway reused one request UID across models, while SQL reserve keeps
   `google_ai_requests.model` immutable (`ON CONFLICT DO NOTHING`). Finalize and
   rolling usage join through that original model. Thus the successful fallback
   can debit the wrong model's bucket. The incident error confirms the negative
   counter symptom; it was not the cause of the preceding Google 503 responses.
6. VK terminal/no-background-retry behavior is an explicit existing owner
   contract against retry storms. The patch preserves it. Technical failure
   must remain distinguishable from an LLM-confirmed non-event.
7. The hook creates a client per call, so instance-local incident cooldown does
   not deduplicate failures across separate publications. Alert aggregation is
   a follow-up, not part of this narrow routing patch.

The library partner message about no eligible events is a separate selection
outcome. This investigation does not establish its cause or equate it with
Google overload. The prior VK publisher-token incident is also distinct.

## Corrective changes delivered

- Opt-in text-only Lite sibling chain in both directions for server parsing,
  Telegram hooks and Smart Update. No global change to Search/Live/tool routing.
- Parsing and hooks use at most two gateway attempts per generation; Gemma
  parsing retains one. Existing row wall-clock and 4o budgets remain.
- Separate ledger request per model with shared `logical_request_uid` in logs
  and incident payloads; total attempt count/cap does not reset. Same-model
  retry retains its UID. This avoids a SQL rollout for new calls and also fixes
  incorrect cross-model rolling quota attribution.
- Existing remote notebook invocations and historical bad ledger rows are not
  repaired by this source patch; subsequent notebook builds embed new gateway.

## Automation contract

### Treat as regression guard when

Changing provider model fallback, request identity, quota finalization, source
parse physical budgets, or the public writer's allowed models.

### Affected surfaces

`google_ai/client.py`, `main.py`, `main_part2.py`, `smart_event_update.py`,
Google quota ledger and generated remote gateway packages.

### Mandatory checks before closure or deploy

- Both Lite directions recover on mocked 503 and TPM admission failure.
- Different models have distinct ledger request IDs but common log correlation.
- Existing hard one-send/opt-out consumers and Gemma parse stay bounded.
- Public hooks exclude Gemma and retain strict 4o fallback.
- Provider-path audit: unapproved=0 and allowlisted_debt=0.
- Exact-main release and live fallback/finalize readback without negative TPM.
- Assess today's terminal carriers and explicitly reconcile any authorized
  catch-up; do not mark an unprocessed technical terminal as a non-event.

### Required evidence / release status

- Focused suite: 231 passed, 17 unrelated publication tests deselected.
- Initial three-file broader suite: 190 passed, 11 failed. All 11 failures were
  reproduced on clean baseline SHA 9a68c377c in the same environment (baseline
  publication file: 95 passed, 11 failed). They involve dated event/promo
  fixtures and typed rich-message fixtures, not the changed hook route.
- Provider audit passed before changes; final audit recorded with the patch.
- PR #697 merged to `main`: `e1af8aaeeaf6990d2a63e0a6e8fe57304967de0d`.
  All required PR checks passed. Manual `scripts/deploy_fly_main.sh --remote-only`
  completed from that clean, exact-main checkout.
- Fly image: `deployment-01M3M8SJ6K0QSGCB8XY5RX6PVV`; machine
  `148eddde9b5778`, version 2108, started at 15:09:16 UTC. Live release marker
  matches the merge SHA. Health: ready=true, DB=ok, issues=[], workers healthy.
- Real source parsing and Telegram publication both attempted 3.1 Lite → 3.5
  Lite after release. Separate per-model request IDs and shared logical ID are
  visible in logs. Both endpoints still returned 503 for some requests.
- Supabase ledger readback confirms distinct model rows finalized
  `failed_provider`, plus successful 3.1 parsing finalized `succeeded` with
  actual usage. No new negative-TPM/finalize error appeared in the sampled
  post-release window. At 15:27:18 UTC, a real 3.5 → 3.1 fallback succeeded:
  initial `028dabfe-e76a-4af6-b2f9-df7b16c0b0b5` finalized `failed_provider`
  under 3.5; fallback `70665cc4-5839-41e6-a9cc-6bd235cd4752` finalized
  `succeeded`, 7038 tokens, under 3.1. This verifies cross-model accounting.
- Compensating replay: `ops_run=9870`, stable run ID
  `INC-2026-09-28-lite-fallback-recovery-9860`, started 15:11:17 UTC. It targets
  only the 20 failed carriers from original run 9860 (`auto:1790602200`). The
  standard import/Smart Update path is retained; guards exclude changed rows
  and prior successes. Source packets, cached parses and existing event links
  remain intact. No blanket queue reset or permanent terminal retry was added.
- The SSH-attached replay process disappeared after the local SSH command
  exited 143. Remote process absence was verified. At 15:42:04, run 9870 was
  marked `error`, and only its orphan locked row 59651 was terminalized with
  `RECOVERY_PROCESS_INTERRUPTED`; cached parses and existing links were kept.
  Eleven completed rows were not replayed. Changed-state guards skipped 59643
  and 59678, which were already pending before selection reached them.
- At 15:42:13, detached remainder run 9873 started with stable run ID
  `INC-2026-09-28-lite-fallback-recovery-9860-remainder`, selecting only
  59651, 59652, 59653, 59659, 59685, 59691 and 59697. It uses the same normal
  import path and per-row expected-state guards, independently of the SSH
  connection. Process stdout is discarded; runtime mirror and ops_run retain
  outcomes. First-run reconciliation evidence is in `remainder-launch.txt`.
- Remainder run 9873 finished at 15:48:54 UTC, status `partial`, 7 terminal
  rows (1 confirmed no-event, 6 technical failures), no created/updated events.
  Combined replay: 18 of the original 20 failed carriers processed, comprising
  3 confirmed no-events, 3 product exclusions and 12 failures. Two changed-state
  rows (59643, 59678) remained pending and were not forcibly reclaimed. All
  current selected rows have clear leases and none is left locked.
- Remaining failures: 6 source-parse technical errors, 3 verification errors,
  and 3 Smart Update identity/occurrence/adjudicator failures. Both Lite models
  still returned 503 in the final sample; this is not full availability recovery.
- Existing event mappings remain 59678 → 9399 and 59698 → 9400. The replay
  created no event mappings or duplicate events. Original source packets and
  previous typed outcomes remain available.
- Final audit initially failed the assumption that all five nonselected
  successful rows would retain their queue statuses: 59623 and 59688 changed
  from `confirmed_no_event` to `pending` with packet links reverting revision
  2 → revision 1. Their old completed packets remain intact. Another revision
  rollback affected skipped 59643; 59678 instead acquired a newer attachment
  revision. Evidence: `packet-revisions.json`, `recovery-final.json`.
  `vk_intake` upsert requeues whenever packet ID changes, including an older
  revision; the actor producing these refreshes was not established. Do not
  attribute these changes to the exact replay selector, which excluded them,
  or claim the whole queue was restored. Revision ordering is an open follow-up.
- Four ordinary Telegram publication jobs ended in error after release in the
  sampled window; normal retries remain scheduled. Health remained ready, but
  publication recovery is incomplete.
- This record remains open: code routing/accounting fixes are delivered and
  live-verified; provider capacity, remaining technical carriers and source
  revision ordering still require follow-up.

## Follow-up actions

- Watch organic provider recovery; cross-model finalization is verified.
  Do not repeatedly probe an overloaded provider.
- Audit historical affected quota attempts before any reconciliation; do not
  blindly clamp negative counters or reset quota.
- Exact authorized replay completed with partial recovery. Reconcile the 12
  remaining failures after capacity/evidence problems are resolved; do not turn
  them into non-events or add an unbounded retry loop. Existing publication
  outbox retries retain their normal schedule.
- Consider shared/batch-scoped incident deduplication separately.
- Use detached, persisted operation tracking for long production replays;
  do not tie their lifetime to a diagnostic SSH session.
- Separately reconcile observed poster upload `403 AccessDenied` and source
  evidence/identity-review failures. These are not fixed by model switching.

- Prevent older source revisions from resetting newer terminal queue decisions;
  retain both source versions and add a revision-order regression check before
  changing this separate ingestion contract.


## Product recovery: verified Flash reserves (2026-09-28)

The earlier Lite failover did not restore ingestion. Real shared-limiter probes
found 2.5 Flash usable on GOOGLE_API_KEY/GOOGLE_API_KEY4, while four other normal
projects return 404. 3.6 Flash succeeded on GOOGLE_API_KEY2/3/6; others returned
503. 3/3.5/3.7/3.8 Flash probes were unavailable. Availability is project-specific;
a model-name-only fallback cannot solve this incident.

Prepared emergency preference 2.5 -> 3.6 for parsing, Smart Update and public
copy, compatible project intersection, two-attempt maximum (collection one),
model-specific thinking settings and fail-closed shared accounting. No quota
increase: Flash remains 20 RPD per project/model. Synthetic JSON and full real
parser probes succeeded, but a no-event parse is not ingestion recovery.
Acceptance remains a normal failed-source replay creating/updating actual future
events, plus negative control and outbox/queue verification after deployment.

Evidence in retained incident directory: gemini-lane-matrix.jsonl,
real-parse-canary-25-scoped.log, reserve-thinking-probe.txt. Focused gateway,
parser and Smart Update tests pass; broader publication suite has nine existing
dated-fixture failures, unrelated to model routing. Live verification pending.


### First reserve release and live canary

PR #700 passed all CI, merged and deployed exact main
`a1540fa07861b4cd1970a2c95f0c25007cb003fb` with ready health at 17:07 UTC.
Image: `deployment-01M3MFJ08H812DNNQ083BWKSBY`. Actual env matches reserve
preference/project map. Run 9882 replays exact failed rows 59871/59828. Parser
and six Smart Update calls succeeded on 2.5; event 9402 was created for the
October 4 play. Final import waited on uncovered Gemma topic classification.
Follow-up includes that stage in the same reserve and coalesces duplicate alerts
at the shared server sink. This is required to remove the observed post-create
stall; it does not bypass LLM event/identity checks.

The deployment terminated old scheduled run 9877; startup marked it crashed.
Reconcile its exact remaining locks before finishing catch-up. No VK authorization
errors were found in the available log window since 16:00 UTC; VK credentials
were not changed. Additional Fly-generated `depot-key3905902939` retained with
0600 permissions; never publish the artifact directory wholesale.


### Quota protection correction

The initial emergency scope included tg_event_publish. Its old backlog consumed
reserve capacity: local 2.5 counters reached 20 on both compatible projects.
The observed rpm/rpd notifications are pre-send shared-limiter admission denials,
not evidence of Google key blocking. No limits were raised or bypassed. At
17:20 UTC, 39 due/soon public-copy jobs were delayed until 17:50:15 with their
payload/state/attempt history preserved; no public-copy job was in flight.
Stop additional manual replays. Correction excludes public copy from emergency
routing and prefers still-admitted 3.6 for monitoring, preserving quota caps.

Canary 9882: success, one created event 9402 and one confirmed non-event.
Poster replay 9883: one created event 9403, one confirmed non-event, one remaining
prose_location failure (59653). Neither run left locks. Public jobs are also
blocked by the independently confirmed CDN TLS mismatch and storage PUT 403;
see INC-2026-09-27-static-cdn-tls-regression. These are not VK-token failures.
