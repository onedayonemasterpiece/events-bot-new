# INC-2026-08-27 MCP social voice batch reads required per-job polling

Status: mitigating — bounded batch implementation is in draft review; production/action refresh acceptance pending
Severity: sev2
Service: private eventsBot MCP Social Workspace and audio transcription
Opened: 2026-08-27
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-08-24-mcp-telegram-album-media-ref`, `INC-2026-08-25-chatgpt-frozen-mcp-actions`, `INC-2026-08-26-mcp-telegram-custom-emoji-reaction`
Related docs: `docs/features/audio-transcription/README.md`, `docs/operations/private-events-mcp.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

High-level Telegram reads already created stable, owner-bound durable
transcription jobs, but enriched voice/audio attachments sequentially under one
provider-timeout deadline. A large voice batch could exhaust that local deadline
and report a synthetic terminal timeout while the real job remained queued or
running. The ordinary client path then required one status call and later one
get call per `atr_*` for every refresh round.

## User / Business Impact

- Reading a private Telegram fragment with many voice messages was slow and
  operationally complex in ChatGPT.
- Later attachments could be mislabeled `failed/TRANSCRIPTION_TIMEOUT` even
  though durable processing remained healthy.
- Per-ref polling encouraged provider reconciliation bursts and made it easier
  to violate the serialized Telegram/Kaggle lane or its persisted Retry-After.
- Completed transcripts were not reliably available through one repeat
  high-level read, forcing the user to lose their place or wait through many
  tool calls.

## Detection

The user reported the repeated slow/unsuccessful high-level read after the
stable-media, principal-binding, serialized-dispatch and Retry-After fixes had
already reached production. Code-path tracing and deterministic regressions
confirmed the defect above those preserved layers.

## Timeline

- 2026-08-27: fresh `origin/main` and production Fly v2040 were both confirmed
  at `f5877deb6364a58bb16d0e0fceb4f36cb71bc3df`; health was ready and Fly checks
  were 1/1.
- 2026-08-27: authenticated full-scope production `tools/list` returned 30 tools
  in 149,386 serialized bytes with audio tools still first.
- 2026-08-27: source tracing confirmed the sequential shared-deadline path and
  the per-ref `status()` reconciliation hazard.
- 2026-08-27: deterministic regressions were added for 20 new voices, mixed
  states, repeat reads, owner isolation, response continuation and independent
  timeout budgets; draft PR `#590` was opened.

## Root Cause

1. `SocialWorkspaceRuntime._enrich_telegram_audio` walked voice attachments
   depth-first and awaited start/status/result for each one before visiting the
   next attachment.
2. It derived one enrichment deadline from `social_provider_timeout_seconds`,
   coupling provider transport with transcription waiting.
3. Deadline expiry was projected as terminal `TRANSCRIPTION_TIMEOUT` without
   changing or reading the actual durable terminal state.
4. `AudioTranscriptionService.status()` may reconcile a running backend job;
   looping it per attachment can bypass the monitor-only batch cadence and does
   not itself honor the monitor's persisted provider hold.
5. The public attachment/output contract lacked batch summary, creation/cache
   evidence and explicit inline continuation metadata.

## Contributing Factors

- Earlier incident work correctly prioritized stable identity, no duplicate
  jobs, serialized dispatch and Retry-After; it did not yet redesign the
  higher-level batch consumption surface.
- The tool protocol timeout was sized for two provider budgets, not one provider
  call plus a separately requested whole-batch wait.
- Existing tests covered two voices and per-ref fallback, but not a deterministic
  20-voice registration-before-wait invariant.

## Automation Contract

### Treat as regression guard when

- changing Telegram Social Workspace item/feed/thread reads or voice enrichment;
- changing audio durable job snapshot/reconciliation/dispatch behavior;
- changing ChatGPT-visible social read schemas or transcription output caps;
- changing provider timeout, monitor cadence or persisted Retry-After handling.

### Affected surfaces

- `private_events_mcp/social_workspace.py` read/output contracts;
- `private_events_mcp/social_workspace_tools.py` exact resolver and tool timeout;
- `private_events_mcp/social_workspace_runtime.py` voice batch orchestration;
- `audio_transcription/service.py` owner-bound durable batch snapshot/wait/get;
- ChatGPT action-definition refresh/publication and new-chat acceptance;
- Fly runtime logs, private MCP auth DB and dedicated Telegram/Kaggle lane.

### Mandatory checks before closure or deploy

- 20 new voice attachments are all created/found before exactly one bounded
  wait; elapsed behavior is not 20 times the requested wait;
- mixed ready/queued/running/failed durable states and summary counts project
  correctly, with ready text inline;
- wait expiry never creates terminal `TRANSCRIPTION_TIMEOUT` and preserves the
  real queued/running state plus a safe next refresh delay;
- repeat high-level read creates no duplicate jobs and returns fresh states and
  completed text without per-ref status/get calls;
- one materialization failure remains local, and long text is explicitly
  truncated with a reproducible continuation offset;
- batch snapshot/wait/get rejects a foreign principal and never polls provider
  status or weakens existing public single-job owner binding;
- `transcribe_audio=false`, the three public audio tools, social tool order/count,
  Codex exact-seven catalog, album/media outer refs, serialized dispatch,
  Retry-After, reactions and played/read state do not regress;
- full private MCP/audio tests, compile/diff/lint checks and all required GitHub
  CI jobs pass;
- exact clean `origin/main` deploy, `/healthz`, Fly checks, immutable image SHA,
  authenticated full-scope `tools/list`, log mirror and sanitized audit pass;
- changed actions are refreshed/reviewed/published in the existing ChatGPT app,
  then a new-chat private multi-voice canary passes without N per-ref polls.

### Required evidence

- focused/full test output and green PR checks;
- merged SHA reachable from `origin/main` and exact deployed SHA/machine version;
- authenticated production catalog count/bytes/order without truncation;
- sanitized batch telemetry/canary metrics only: voice count, durations,
  created/cache hits, state counts, high-level refresh count, readiness times,
  duplicate count and provider rate-limit count;
- action refresh/publication receipt and sanitized real ChatGPT call receipt;
- no private links, transcript bodies, opaque/provider/native IDs, paths or
  credentials in logs, docs or committed artifacts.

## Immediate Mitigation

- Existing durable jobs, stable media identity, owner binding, serialized
  dispatch and provider hold were preserved; no jobs were restarted or fanned
  out through a new backend lane.
- The supported fallback remains bounded same-principal status/get, but clients
  must not busy-poll while the batch correction is under review.

## Corrective Actions

- Added explicit `transcription_wait_seconds=0..30`, valid only with explicit
  `transcribe_audio=true`, while keeping provider transport timeout separate.
- Added collect/register/wait/project stages with ingress concurrency capped at
  three and one owner-bound store-only wait.
- Added typed durable `snapshot_many`, `wait_many` and `get_many` service methods
  without a new public MCP tool.
- Added inline text/continuation metadata, aggregate summary and one sanitized
  batch timing/count log.
- Added deterministic coverage for the mandatory checks above.

## Follow-up Actions

- [ ] Merge PR `#590` after full CI and deploy exact clean `origin/main`.
- [ ] Refresh, review and publish the four changed high-level read actions in
  the existing ChatGPT app; start a new chat.
- [ ] Run the sanitized private multi-voice canary, honor returned refresh delay
  and attach only aggregate metrics.
- [ ] Use batch telemetry to establish `time_to_all_ready` before considering
  any separate P1 increase in transcription throughput.

## Release And Closure Evidence

- branch: `fix/mcp-social-voice-batch-read-20260827`
- draft PR: `#590`
- code commit: `3bba47d14`
- test commit: `0fd484d8d`
- deployed SHA: pending
- deploy path: pending exact-main `scripts/deploy_fly_main.sh`
- regression checks: focused local suite passed; full CI/live acceptance pending
- post-deploy verification: pending

## Prevention

The high-level read is now designed as the durable batch refresh surface. The
monitor remains the only normal remote reconciliation loop, wait expiry is a
response property rather than a durable terminal state, and response/canary
contracts make silent text loss, per-ref polling fan-out and provider/native
leakage testable regressions.
