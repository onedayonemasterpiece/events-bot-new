# INC-2026-08-21 Telegram event public writer MAX_TOKENS wave

Status: open
Severity: sev2
Service: Telegram event publishing / Google AI public writer
Opened: 2026-08-21
Closed: —
Owners: events-bot maintainer / incident operator
Related incidents: `INC-2026-07-14-synthetic-thin-source-public-copy.md`
Related docs: `docs/features/tg-publishing/README.md`, `docs/features/llm-gateway/README.md`, `docs/operations/runtime-logs.md`

## Summary

On 21 August 2026 the `tg_event_publish` Gemini Lite public writer produced 24
`MAX_TOKENS` provider failures between 17:16 and 17:39
Europe/Kaliningrad (15:16–15:39 UTC); 23 occurred in the final eleven minutes.
The writer must return structured `sentences[].text + evidence_quote` JSON, but
its provider schema did not bound the sentence array and its output ceilings
remained at 260 tokens for ordinary hooks and 360 for promo hooks.

The strict `gpt-4o` emergency writer absorbed part of the wider invalid-Lite
wave until its persisted 100-request UTC-day budget was exhausted at 15:34 UTC.
Affected publication/edit jobs then failed closed and entered retry rather than
publishing deterministic or ungrounded copy.

The failure continued because the remediation existed only in Draft PR #548
and production was still running Fly machine version 2011, deployed on
17 August. Between 20:33 and 22:11 local time (18:33–20:11 UTC), operators
received another 15 `MAX_TOKENS` alerts. Three `503 UNAVAILABLE` high-demand
alerts in the same interval are a separate transient provider-availability
condition, not evidence against the token/schema root cause.

## User / Business Impact

- The 24 confirmed `MAX_TOKENS` calls affected 24 event rows. At the production
  snapshot taken during investigation, 8 corresponding `tg_event_publish` jobs
  were `done` and 16 were `error` with the strict fallback budget exhausted.
- Six affected events had no Telegram post URL at that snapshot: `7804`,
  `7805`, `7884`, `7985`, `8129`, and `8131`. The other failed jobs were mostly
  edits/reconciliations whose previously published post remained visible.
- By 20:14 UTC, the wider retry backlog contained 62 `tg_event_publish` jobs
  whose latest error was public-writer unavailability; 22 had no public post
  URL/result. This broader count includes invalid Lite/provider failures beyond
  the original 24-call cohort and must not be read as 62 `MAX_TOKENS` calls.
- The fallback wave consumed the full 100-request daily emergency budget, so
  unrelated invalid Lite outputs could no longer be recovered that UTC day.
- The system failed closed as designed: no deterministic narrative or
  lower-class model was substituted for the grounded public writer.

## Detection

- The provider incident notifier surfaced 24 CRITICAL `provider_error`
  signals for `consumer=tg_event_publish`, model
  `gemini-3.1-flash-lite`, code `MAX_TOKENS`.
- The Fly runtime file mirror was verified enabled at
  `/data/runtime_logs/events-bot.log` with 48-hour retention. Structured
  `google_ai.call_error` rows independently confirmed exactly 24 matching
  calls, first at `15:16:38.281Z` and last at `15:39:49.812Z`.
- Production SQLite `quick_check` returned `ok`; `joboutbox`, event public URLs,
  and `llm_daily_request_budget` supplied the impact snapshot.
- A second runtime-mirror check through 20:14 UTC matched all 18 attached
  operator alerts: 15 `MAX_TOKENS` and 3 `503`. The same interval also logged
  seven provider `429` responses that were handled by key rotation before the
  terminal call; this explains alerts showing `attempt=2/1` (`max_retries=1`
  counts local retries, while provider-key rotation advances the request's
  provider attempt cursor).

## Timeline

- 2026-08-21 15:16:38Z (17:16 local) — first confirmed public-writer
  `MAX_TOKENS` response.
- 2026-08-21 15:29:58Z — clustered wave begins; 23 failures follow through
  15:39:49Z.
- 2026-08-21 15:34:43Z — persisted
  `tg_event_public_writer_4o` budget reaches `100/100`.
- 2026-08-21 15:39:49Z (17:39 local) — last call in the reported 24-error
  window.
- 2026-08-21 18:19Z — runtime mirror/config and retained log set verified;
  exact failure distribution and impact evidence captured.
- 2026-08-21 18:24Z — regression tests reproduced the missing schema bounds
  and undersized ceilings before the code fix.
- 2026-08-21 18:33–20:11Z (20:33–22:11 local) — production, still on the
  pre-fix image, emits 15 more `MAX_TOKENS` and three separate `503` alerts.
- 2026-08-21 20:14Z — refreshed DB snapshot shows 62 active public-writer
  unavailable jobs, including 22 without a public post/result; fallback budget
  remains `100/100`.

## Root Cause

1. Since the evidence-grounding rollout, every public sentence is duplicated
   in structured output as both a short public `text` and an exact
   `evidence_quote`; output size is therefore materially larger than the final
   visible intro.
2. `_TG_EVENT_HOOK_RESPONSE_SCHEMA` described `sentences` as an array but did
   not provide `minItems` or `maxItems`. The application parser enforced one to
   three items only after generation, so the provider had no schema-level stop
   at the contract's three-sentence boundary.
3. The Lite and strict 4o lanes still shared output ceilings of 260 ordinary /
   360 promo tokens. Those caps were too close to normal structured-output size
   once exact evidence quotes and JSON framing were included.
4. A concentrated publication/reconciliation wave amplified the per-call
   defect. Invalid or truncated Lite results used the only approved emergency
   lane until its intentional 100/day cap was exhausted; subsequent jobs
   remained retryable errors.

## Contributing Factors

- Prompt text said `1-3 предложения`, but prompt instructions are not a hard
  provider output bound.
- The post-generation parser already rejected more than three items, which
  protected semantics but could not prevent provider work or truncation.
- Output-token regression tests covered the former 260/360 values but did not
  assert that the structured schema itself bounded array cardinality.
- Retry/edit work for already visible posts competed with unpublished event
  announcements after the emergency fallback budget was depleted.
- The fix was intentionally held in a Draft PR for review, so hourly retries on
  the unchanged production image continued to reproduce the incident.

## Automation Contract

### Treat as regression guard when

- changing `build_tg_event_hook_text`, `_TG_EVENT_HOOK_RESPONSE_SCHEMA`, public
  writer output budgets, evidence quotes, model routing, or strict 4o fallback;
- changing `GoogleAIClient` handling of finish reasons or structured JSON;
- rearming or bulk retrying `tg_event_publish` jobs.

### Affected surfaces

- `main_part2.py` Telegram public writer and fallback;
- `google_ai.client.GoogleAIClient` structured generation and typed
  `MAX_TOKENS` handling;
- Fly runtime logging / provider incident notifier;
- SQLite `joboutbox`, `event.tg_event_post_url`, and
  `llm_daily_request_budget`;
- public `@kldevents` announcements and existing-post edits.

### Mandatory checks before closure or deploy

- Schema test proves `sentences.minItems == 1` and `sentences.maxItems == 3`.
- Ordinary and promo Lite calls use the incident-approved 768 / 1024 output
  ceilings; the strict 4o fallback receives the same mode-specific ceiling and
  the same bounded schema.
- All public-hook tests and Google AI client/grounding suites pass. A full
  `tests/test_tg_event_publish.py` run must have no new failures beyond any
  explicitly recorded date-sensitive baseline failures; changed Python files
  compile and `git diff --check` passes.
- Post-deploy controlled retries show successful grounded Lite JSON with no
  `MAX_TOKENS` for the incident cohort.
- Reconcile all 24 affected jobs, prioritizing the six rows with no Telegram
  post URL; verify each eligible current/future event either has a visible
  public post or a documented product-policy exclusion.
- Verify `/healthz`, SQLite `quick_check`, clean release checkout, deployed SHA,
  and reachability from `origin/main`.

### Required evidence

- Runtime mirror probe plus sanitized 24-call window summary under local
  `artifacts/codex/INC-2026-08-21-tg-event-public-writer-max-tokens/`.
- Test output and Draft PR checks.
- Deployed SHA and Fly machine/version after merge.
- Post-deploy log summary and production DB/public-surface catch-up receipt.
- Confirmation that the fix commit is reachable from `origin/main`.

## Immediate Mitigation

- Existing fail-closed behavior prevented truncated or ungrounded output from
  reaching Telegram.
- The fix branch bounds the provider array to one through three items and gives
  the grounded JSON enough completion headroom without changing the approved
  writer models or increasing the 4o daily request cap.
- No production DB rows or fallback-budget counters were mutated during the
  read-only investigation. Catch-up remains mandatory after the Draft PR is
  reviewed, merged, and deployed.

## Corrective Actions

- Added schema-level `minItems=1` and `maxItems=3` to the public sentence array.
- Raised ordinary structured output to 768 tokens and promo structured output
  to 1024 tokens for both Gemini Lite and the strict 4o fallback.
- Added regression assertions for the schema bounds and both mode-specific
  token budgets.
- Documented the structured-output contract and this incident regression gate.

## Follow-up Actions

- [ ] Merge the reviewed fix to `main`, deploy the exact reachable SHA, and
  attach release evidence here.
- [ ] Run a controlled Lite-only retry/catch-up for the 24-event cohort, with
  the unpublished rows first, then reconcile the expanded backlog; record
  public URLs or exclusions.
- [ ] Confirm the next normal publication window has no recurrence of this
  `consumer/model/code` tuple before moving the incident to `closed`.

## Release And Closure Evidence

- branch: `incident/INC-2026-08-21-tg-event-public-writer-max-tokens`
- Draft PR: <https://github.com/onedayonemasterpiece/events-bot-new/pull/548>
- deployed SHA: pending
- deploy path: pending review/merge to `origin/main`
- regression checks:
  - public-hook subset: `6 passed`;
  - `tests/test_llm_source_grounding.py tests/test_google_ai_client.py`:
    `70 passed`;
  - full `tests/test_tg_event_publish.py`: `91 passed`, 9 unrelated failures
    caused by June/July fixtures now being in the past on 2026-08-21;
  - google-genai `2.12.1` config serialization preserved `minItems=1`,
    `maxItems=3`, and `maxOutputTokens=768`;
  - unrelated `smart-update-identity-state-machine` CI failure was traced to a
    fixed 18 August fixture becoming a past event on 21 August; the identity
    regression now explicitly disables the independently tested past-event
    policy (`14 passed` for its source-identity contract file);
  - `py_compile` and `git diff --check`: passed.
- post-deploy verification: pending

## Prevention

- Cardinality that application code depends on must be expressed in the
  provider JSON schema, not only in prompt prose or post-generation parsing.
- Token ceilings for grounded structured writers must budget for both public
  text and repeated evidence payloads; visible character limits are not output
  token budgets.
- Any bulk `tg_event_publish` rearm must review emergency-writer capacity and
  prioritize events without a public post over edits to existing posts.
