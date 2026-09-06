# INC-2026-09-06-voice-search-relevance — vector candidates presented as jazz matches

Status: open
Severity: sev2 (protected preview only)
Service: DevCoveer authenticated conversational Search; no production outage claim
Opened: 2026-09-06
Closed: —
Owners: KenigEvents Search implementation
Related incidents: `INC-2026-09-06-voice-preview-startup.md` (capture/storage regression)
Related docs: `docs/features/static-site-pages/smart-vector-search/voice-search-solution-v1.md`,
`docs/features/unsigned-personalization/authorized-event-search.md`

## Summary / User impact

User said «Джаз на выходных» and received 50 cards presented under a jazz title,
including an exhibition and a rap concert. This is a confirmed semantic precision
defect in the protected preview, not evidence that 50 jazz events exist.
Telegram screenshot: https://t.me/c/4337049383/1457 .

## Detection and timeline (UTC)

- 2026-09-06 13:18:26: interpretation receipt created; input exactly «Джаз на выходных».
- 13:18:29: search receipt created, subsequently completed with 50 items.
- 13:19:43: user's screenshot shows event 7410, opening/exhibition «Эхо той ночи».
- Investigation read the exact two text/search receipts and public candidate
  facts, not unrelated private audio. Frozen user results were not changed.
- One isolated real verifier call on the same candidates/facts returned `ok`:
  20 checked, 1 exact (7422), 15 rejected (including 7410 and 8680), 4 possible;
  remaining 30 were not checked. This is NOT full-catalog precision/recall or a
  claim there is only one jazz event. Provider attempt count 1, shared limiter
  used, no provider fallback. Diagnostic process only; service config unchanged.

## Confirmed root cause

Classification: semantic admission contract / adapter wiring, not ASR failure.

1. `createAssistantDependencies().search` in `event-search/index.ts:2958–2965`
   explicitly sets `use_llm_verifier:false`, with a 60-candidate window.
2. DevCoveer runtime env does not set `EVENT_SEARCH_LLM_ENABLED`; default is off.
3. Merely changing those two switches would STILL not enable it: assistant quota
   admission constructs `llm_reserved:false` (2344–2347), while `llmExecutionAllowed`
   requires `useLlmVerifier && (isCanary || llmQuotaReserved)` (2403–2409). The
   ordinary user's voice request is not canary. Preserve actual shared provider
   reservation when correcting this internal gate; do not fake a quota lease.
4. Shared ordinary Search intentionally fails open: final items are
   `llmResult.used ? llmResult.exact : llmResult.possible` (2575–2577).
   `include_fallback:false` only removes the separate fallback array. It does NOT
   prevent unverified candidates entering primary items. That legacy behavior is
   documented in authorized-event-search.md; reuse did not meet voice's contract.
5. `assistant-handler.ts` then counted all candidates and paired them with the
   interpreted jazz title/summary. The distinction between a recall candidate and
   a verified match was lost. Numeric similarity is not genre confirmation.

Actual receipt: `actual_execution_mode=cold_vector`,
`algorithm_id=pgvector_gemini_embedding_2_possible_only_v1`, verifier
`requested=false,used=false,status=disabled`, `llm_provider_attempts=0`,
`retrieved_count=50`, no result cache hit, new_search with no parent. Thus this
was not old history contamination, a verifier timeout, or a broken microphone.

## Supporting evidence / other findings

- 7422 «Золотые хиты джаза»: similarity 0.7599, 13 September; digest directly
  supports jazz repertoire. Diagnostic classifier accepted it.
- 8680 Pra(Killa'Gramm): similarity 0.7382, 12 September; digest explicitly says
  rap concert, venue is «Калининград Сити Джаз Клуб». Classifier rejected it.
  Venue wording is a plausible contributing retrieval signal, not proof of the
  embedding's exact internal attribution.
- 7410 screenshot exhibition: similarity 0.6916, start 15 August, typed end
  15 September. All 50 candidates overlap the interpreted 12–13 September range
  under current typed interval rules. August start does not alone prove expired
  data. Whether its opening should really last a month requires source repair
  investigation; no event dates were rewritten here.
- New interpretation invented locality kaliningrad although raw question had no
  city and initial base localityIds was empty. Separate grounding defect: do not
  silently narrow region-wide search. Weekend interpretation chose 12–13 Sept
  on Sunday 6 Sept; expected current/next weekend policy needs explicit tests.
- Existing verifier defaults: max20 candidates, fact180 chars, primary2600ms,
  total4300ms. Diagnostic kept the same candidate limit and prompt but allowed
  one12000ms attempt; actual provider7463ms. This is latency evidence, not a
  safe promise that flipping an env flag will solve the whole problem.
- Digest coverage <50%, disabled/quota/timeout all preserve possible candidates
  in legacy Search. Successful verification is required for voice exact claims.

## Automation contract

### Treat as regression guard when

Changing voice→Search adapter, semantic verification, quotas, candidate windows,
result wording, intent grounding or contextual refinement. Existing June29 jazz
example in authorized-event-search.md preferred0exact to false jazz; the new
adapter regressed that product expectation.

### Mandatory checks before closure or deployment of a fix

- New-search and refine/expand paths cannot label vector-only or possible items
  as confirmed matches; disabling/unavailable verifier must yield an explicit
  unverified/error state, not invented exact results or misleading zero matches.
- Same Search/Auth/shared limiter must remain; no owner-only bypass, canary
  impersonation, fake quota lease or fresh paid attempt for an ambiguous receipt.
- Semantic negative controls: rap in a Jazz-named venue, classical music, museum
  exhibition, related music festival; include positive nonliteral jazz evidence.
- Verify query date/locality grounding from raw input/base. Do not implement genre
  selection with keyword regex or arbitrary similarity cutoffs.
- Test complete checked-window accounting, partial/unknown facts and timeout.
  Unchecked items must not become exact, and bounded recall must not be called
  the complete catalog.
- Real authenticated public preview check on exact deployed SHA plus code tests;
  meaningful relevance evidence, not merely HTTP200/card counts.

### Required evidence

Exact frontend/backend SHA, real request stage receipt, checked/accepted/rejected/
unknown counts, public visible result and provider accounting. This is preview
only: no production promotion authorized. If ever promoted, normal main release
rules apply. Startup/storage regression contract remains in force.

## Immediate mitigation / corrective action status

Investigation only: no code/config/runtime or user history changes. Do not claim
this preview is now fixed. Existing semantic verifier is reusable, but voice
needs strict admission, correct quota routing, sufficient fact/time budget,
explicit typed dates in verification and separate uncertain-result presentation.
Do not silently change the legacy ordinary Search fail-open policy globally.

## Evidence and release

Inspected frontend `52bdf4db4450e3a0ad300fb9727428874a6559ad`, backend
`fb28280d7cec103d87313b4e9d7cf78045ec3449`; local analysis HEAD `4885f9470`.
Exact inspected request: `46822c88-9667-437d-810d-a27f274ef91f`.
Local artifact root: `artifacts/codex/voice-relevance-20260906/`:
`recent-search-receipts.json`, `candidate-facts.json`, `telegram-1457.jpg`,
`verifier-probe.json`, `verify-evidence.mjs`, `evidence-check.json`.
Evidence assertions PASS:50typed overlaps, verifier disabled in original,
real20-prefix classification1exact/15rejected/4possible,30unchecked.
The probe is supplementary execution evidence, not external consultant review.
No new worktree, no audio/session/database cleanup, no frontend rebuild/deploy.
