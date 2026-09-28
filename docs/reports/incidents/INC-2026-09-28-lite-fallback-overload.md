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
The mirror was enabled with rotated files present. No production data changed.

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

## Corrective changes prepared

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
- No merge, production deploy, live provider probe or public catch-up performed.
  This record remains open; a prepared/tested patch is not outage closure.

## Follow-up actions

- Release the reviewed patch and verify organic provider outcomes and per-model
  ledger finalization. Do not repeatedly probe an overloaded provider.
- Audit historical affected quota attempts before any reconciliation; do not
  blindly clamp negative counters or reset quota.
- Reconcile exact failed carriers once capacity recovers, under the existing
  terminal policy. Public publication reruns require explicit operator scope.
- Consider shared/batch-scoped incident deduplication separately.
