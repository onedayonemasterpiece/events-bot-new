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

Retained, payload-free evidence and test outputs:
`/home/dev/artifacts/events-bot-new/20260928T134855Z-lite-fallback-20260928/`.
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
  post-release window. Successful *cross-model* finalization remains to be
  observed when provider capacity permits it.
- Compensating replay: `ops_run=9870`, stable run ID
  `INC-2026-09-28-lite-fallback-recovery-9860`, started 15:11:17 UTC. It targets
  only the 20 failed carriers from original run 9860 (`auto:1790602200`). The
  standard import/Smart Update path is retained; guards exclude changed rows
  and prior successes. Source packets, cached parses and existing event links
  remain intact. No blanket queue reset or permanent terminal retry was added.
- This record remains open while replay is running and shared provider overload
  persists; deployment alone is not outage closure.

## Follow-up actions

- Verify successful cross-model ledger finalization from organic traffic once
  capacity recovers. Do not repeatedly probe an overloaded provider.
- Audit historical affected quota attempts before any reconciliation; do not
  blindly clamp negative counters or reset quota.
- Finish and record the exact authorized replay; retain technical failures as
  failures if both Lite endpoints remain overloaded. Existing publication
  outbox retries retain their normal schedule.
- Consider shared/batch-scoped incident deduplication separately.
