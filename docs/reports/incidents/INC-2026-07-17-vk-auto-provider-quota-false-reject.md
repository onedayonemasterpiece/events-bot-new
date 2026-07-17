# INC-2026-07-17 VK auto-import provider quota false reject

Status: open
Severity: sev2
Service: VK auto-import / Smart Update / Google AI gateway
Opened: 2026-07-17
Closed: —
Owners: events bot operator / Smart Update maintainer
Related incidents: `INC-2026-06-03-smart-update-flash-lite-rpd`, `INC-2026-04-28-vk-smart-update-false-skips`
Related docs: `docs/features/llm-gateway/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/vk-auto-queue/README.md`, `docs/operations/runtime-logs.md`

## Summary

The operator ran `/vk_auto_import 1` against production. A concurrent unrelated
Fly release interrupted the first run (`ops_run=4040`), but Telegram redelivery
correctly started recovery run `4041`. The recovery run completed operationally
as `success` yet rejected a clearly grounded future children's event
`wall-221969169_52878` as
`create_bundle_grounding:llm_ungrounded`.

The semantic failure was caused by quota routing, not the source: several Smart
Update `gemini-3.1-flash-lite` stages repeatedly borrowed KEY3 as emergency
overflow. Google returned provider-side `429` while the shared ledger still
showed 25 RPD headroom. The client intentionally failed provider 429 immediately,
and Smart Update's one-attempt bound left the create grounding stage without a
healthy key even though KEY5 and KEY2 retained 881 aggregate Lite RPD headroom.

## User / Business Impact

- One valid future VK event was left absent from canonical event/public surfaces.
- The poster never reached the post-create image-geometry job, so this manual
  geometry acceptance could not produce a fresh bbox row.
- Operator-facing run status was green (`success`, rejected=1) although the
  reject came from provider availability rather than an ungrounded source.

## Detection

- Detected during the requested live `/vk_auto_import 1` acceptance.
- Production runtime mirror was enabled and retained the complete window in
  `/data/runtime_logs/events-bot.log`.
- `ops_run`, `vk_inbox`, provider request UIDs and Supabase limiter counters
  correlated the rejection to KEY3 provider 429s.

## Timeline

- 2026-07-17 20:47:24 UTC — manual run `ops_run=4040` locked `vk_inbox=10337`.
- 2026-07-17 20:47:31 UTC — event parse reserved Gemma 4 and started.
- 2026-07-17 20:47:49 UTC — unrelated Fly release `v1691` cancelled the process.
- 2026-07-17 20:48:03 UTC — startup recovery marked run 4040 crashed and unlocked one row.
- 2026-07-17 20:48:27 UTC — Telegram redelivery started `ops_run=4041` for the same row.
- 2026-07-17 20:49:28 UTC — event parse produced a grounded candidate.
- 2026-07-17 20:49:44–20:51:11 UTC — Lite calls repeatedly selected KEY3 and hit provider 429.
- 2026-07-17 20:51:19 UTC — grounding returned `llm_ungrounded`; run 4041 ended success/rejected=1.
- 2026-07-17 20:55:30 UTC — read-only limiter audit: Lite RPD headroom KEY1=0,
  KEY4=0, KEY3=25, KEY5=431, KEY2=450.

## Root Cause

1. Smart Update used the five registered keys only through scoped-key plus
   emergency overflow, so every fresh stage selected the same lowest-priority
   eligible overflow member instead of round-robin allocation.
2. Provider quota had drifted from the Supabase ledger on KEY3. The provider
   returned 429 even though the ledger had not reached its defensive 450 cap.
3. `GoogleAIClient` treated every provider-side 429 as fail-fast, including for
   consumers that had an explicit normal pool.
4. Smart Update caps ordinary Google client retries at one to control spend, so
   it did not reach an independently healthy key for the grounding stage.

## Contributing Factors

- The run-level status distinguishes transport failures from terminal rejects,
  but does not distinguish a semantic reject produced after provider degradation.
- A deployment overlapped the manual acceptance and made the first run look like
  an importer crash until release evidence was correlated.
- Image geometry already had a true KEY4+KEY5 normal pool, but the parent Smart
  Update pipeline did not use the same allocation primitive.

## Automation Contract

### Treat as regression guard when

- changing `GoogleAIClient.generate_content_async`, normal/overflow pool logic,
  candidate key scoping or provider 429 behavior;
- changing Smart Update Google key configuration or retry bounds;
- changing VK auto-import reject accounting or post-create geometry scheduling.

### Affected surfaces

- `google_ai/client.py`
- `smart_event_update.py::_get_gemma_client`
- `fly.toml` / `SMART_UPDATE_GOOGLE_KEY_ENVS`
- Supabase `google_ai_api_keys`, model limits, usage counters and reserve RPC
- `vk_auto_queue`, `ops_run`, `vk_inbox`, `event_source`, `eventposter`,
  `event_image_geometry` and image-geometry JobOutbox work

### Mandatory checks before closure or deploy

- All five configured env names exist as active registry rows and have
  model-specific headroom evidence.
- Unit tests prove first-allocation normal rotation, ledger-blocked member
  advance, provider-429 member advance, fail-closed missing registry/limiter,
  and no widening for unpooled/explicit scopes.
- Replay `tests/replays/INC-2026-07-17-vk-auto-provider-quota-false-reject`
  through VK auto-import + Smart Update; the real post must create/merge rather
  than `llm_ungrounded`.
- Run the negative retrospective control without creating an event.
- Verify the imported poster schedules and completes image geometry with valid
  normalized boxes/value region.
- Verify `/healthz`, Fly checks, clean main-reachable deploy and no overlapping
  heavy validation job.

### Required evidence

- targeted pytest output;
- pre/post DB diff for inbox/source/event/poster/geometry rows;
- runtime lines showing `reserve_normal_pool_used` and, if encountered,
  `provider_key_rotation` with a different succeeding key;
- deployed SHA reachable from `origin/main`, Fly release/machine and health.

## Immediate Mitigation

- Preserved the exact source/poster replay artifact.
- Confirmed KEY5 and KEY2 retained 881 Lite RPD calls before any replay.
- Did not hammer the rejected row or manually widen the global unscoped pool.

## Corrective Actions

- Declare KEY1–KEY5 as Smart Update's normal pool from the first reservation.
- On provider 429, exclude only the selected member and atomically reserve an
  unused member of that same declared pool without sleep or model fallback.
- Keep emergency-overflow-only and explicit candidate scopes fail-fast; never
  widen them implicitly.
- Add structured `google_ai.provider_key_rotation` evidence.

## Follow-up Actions

- [ ] Consider escalating provider-vs-ledger drift into a temporary shared
  quarantine so other processes also avoid the member during `retry_after_ms`.
- [ ] Make VK auto-import distinguish provider-degraded rejects from genuine
  semantic rejects in run status/Telegram report.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: targeted gateway/Smart Update suite currently `61 passed`;
  broader VK/grounding suite: `80 passed, 7 failed`, with representative failures
  reproduced unchanged on `origin/main` (date-drift and stale mock expectations);
  full production replay pending
- post-deploy verification: pending

## Prevention

Normal capacity is now an explicit allocation contract, not an accidental
fallback chain. The gateway can use another declared key without sleeping while
preserving atomic limit accounting and caller scope.
