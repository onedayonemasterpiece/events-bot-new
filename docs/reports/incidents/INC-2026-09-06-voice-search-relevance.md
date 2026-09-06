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


## Recall audit and correction integration — 2026-09-06

The preceding investigation-only status is superseded by source correction
integration `c2ac4a8ae` (worker `d720b01c45c5ba45ab8faff0fb70e3e031dd0c81`).
Live delivery remains a separate evidence gate below.

### Confirmed omission, correctly localized

Using the SAME cached query vector as the user's request, read-only live RPC
returned60 rows. Exact cosine ranking with index/bitmap scans disabled returned
all76 eligible indexed rows for12–13September; the ordered top60 was identical.
No approximate-index loss was observed for this query. The direct query plan
used typed date filtering plus primary-key lookup and a sort; no blanket HNSW
health/perfect-recall claim is made for other queries.

Event8580 FOXTROT JAZZ BAND / Paola Meidra in Svetlogorsk ranked **2nd**, similarity
0.7526246687. Event8526 festival entry ranked4th. Both were removed by the
interpreter's unsupported Kaliningrad-only filter AFTER retrieval. Replaying that
city filter gives51 rows; applying the existing reciprocal occurrence-family
collapse gives the user's exact ordered50 (7448 is correctly represented by its
family, not a newly found loss). This reproduces the actual missing result, not
just a hypothesis about top-K.

Authenticated readback of public source https://t.me/kenigevents/4877 confirms
FOXTROT's American jazz programme on12September19:00 in Svetlogorsk. The festival
entry8526 has broader interval/single-opening-program ambiguity: do not count it
as an independently confirmed extra concert solely from its dates.

### Corpus check independent of the original50

- Persisted projection587 documents,587 search_v3 and587 related_v1 vectors,
  model gemini-embedding-2/dim768. All587 search vector text hashes match their
  corresponding projection text_hash; no missing search vector among these docs.
- Query/document construction inspected: `task: search result | query: ...`
  versus `title: ... | text: ...`, same model/dimension; cosine metric. Do not
  confuse a numerically high similarity with confidence of genre identity.
- Full date-window inventory76indexed across all cities, not the original50.
  Canonical Fly comparison adds14 active/unmerged records missing from projection,
  for90 inspected records. Cancelled/merged rows were excluded from that14.
- Separate supplementary classifier pass, not vector-ranked (ID-order batches),
  classified ALL90 with complete ID partitions in5bounded shared-limiter calls:
 2exact (7422,8580),3possible (7410,7689,8526),85rejected. This is not independent
  gold truth or proof that the real world's entire jazz schedule is complete.
- Raw typed intervals are still not certification of source dates; the current/
  next weekend convention and projector date errors remain separate concerns.

### Additional production freshness defect (not silently repaired)

Projection last indexed2026-09-04 21:30UTC. Deployed vector sync guard has been
repeatedly deferring against stale terminal static owner76545, failedSept4
23:02UTC, with remaining claim and retry parked2036. Vector job76235 is pending;
static successor76603 is also pending. The guard merely tests non-null active
job and does not reconcile terminal owner state. This matches existing July11
vector-stall and August12 terminal-static-claim regression families.

Do NOT clear this guard as a voice quick-fix: it could launch the queued full
Kaggle build. Current task forbids full builds/root promotion; production recovery
needs a governed vector-only catch-up that preserves exact static recovery state.
No guard/production DB/job was mutated. Thus global catalog freshness remains
OPEN even after voice precision/unsupported-locality fixes. The14missing records
in this window did not contain a confirmed additional jazz match in the probe;
other kinds of searches are nevertheless affected.

### Source checks and remaining gate

Worker154 assistant/shared-quota/canary/family tests and strict index typecheck
passed. Parent adds90s control timeout (read/media remain60s) with3client tests;
classification total budget45s, individual batch15s. Same durable retry/status
path remains; no automatic repost of ambiguous searches.

Exact evidence under `artifacts/codex/voice-recall-20260906/`:
`rank-comparison.json`, `rank-default-plan.json`, `rank-exact-plan.json`,
`coverage-gaps.json`, `audit-verified.json`, `catalog-classification-summary.json`,
`source-8580.json`, `guard-audit.md`, `guard-{schema,state,runtime}.json`.
Both configured Opus consultant paths were authentication-blocked (`a-opus` and
Claude project Opus). No external review pass claimed; user's ChatGPT Pro audit
is independent and pending. No new credentials requested or lower-class
consultant substituted.

### First published live regression and follow-up correction

Frontend source `882e88f3d5e146334769276ba62d2696e8821b0f` published at
https://kenigevents.ru/preview-voice-relevance-20260906/poisk/ ; all seven selected
public objects match local bytes. Three public landing/Auth/History layout checks
and four topic locator fixtures passed. These are not voice provider acceptance.

The native synthetic-mic browser run used real ASR and Search. ASR was correct,
unsupported city remained empty, but interpreter selected September6–7
(Sunday/Monday), and verifier classified only19/20 first-batch IDs. The strict
admission guard correctly returned an explicit unavailable answer and no cards.
This failed end-to-end acceptance; do not hide it behind passing unit tests.
Receipt: `voice-recall-20260906/final-live-browser-final-receipt.json`.

Correction: voice-only required per-ID enum schema plus unchanged server membership
validation; interpreter calendar grounding supplies actual Saturday/Sunday dates
and asks it to display the resolved interval. Focused47 tests pass. No retries,
shared limiter bypass, speculative relevance thresholds or production changes.
Second live verification is required before declaring this correction delivered.

### Delivered protected-preview acceptance (2026-09-06, second live run)

- Frontend (unchanged): `882e88f3d5e146334769276ba62d2696e8821b0f`.
- Active DevCoveer backend: `adb6e99c84b99b767692049ed4e0ec4dfb3bb91d`.
  Public `/kenig-audio/healthz` reads back that exact SHA; raw media stays on
  DevCoveer. Only private assistant runtime verifier flag enabled; no Supabase
  ordinary Search/production config change and no root/full build promotion.
- Backend generated revision:
  `sha256:9bcf3e1247bc3cc5c4c82cf8375906be56a0595a56bb0d5651f6d7b0f3b3a2b5`.
- Actual browser route:
  https://kenigevents.ru/preview-voice-relevance-20260906/poisk/ .

| Actual live scenario | Evidence / outcome |
| --- | --- |
| Native browser capture of provenance-checked synthetic WAV, silence auto-stop, real ASR | Exact transcript «Я бы хотел сходить на джаз на выходных.»; no confirmation/second mic click |
| Immediate transcript + answer/card skeleton | Both observed at 16806.8 ms browser clock; first cards at 32900.7 ms (~16.1 s after bubble) |
| New search without city | September12–13, localityIds empty; complete58/58 checked in20+20+18 batches; exact7422 and8580;1possible,55rejected |
| Explicit contextual follow-up «Только в Калининграде» | New second section, inherited September12–13; complete48/48 checked in20+20+8 batches; exact7422,4possible,43rejected |
| Prior observed false positives | Rap8680 and exhibition7410 rejected in both requests, not displayed |
| Durable completed searches | `8cd986ee-35ba-4c2e-8e56-127fbfef9787`, `1a4110ac-c1ce-4017-abe1-75e044b8bd8d` |
| Shared topic/mobile-desktop | Current second topic observed at390 and1440; desktop shared menu compacts; zero page errors |
| Auth fixture isolation | Ordinary scoped QA user; protected owner probe PASS, ephemeral fixture cleanup PASS; no product OTP email delivery claimed |

Focused47 tests and strict whole-index/import-graph typecheck pass (local Deno/
remote SDK declaration shim, not an SDK type guarantee). Generated revision and
Git whitespace checks pass. Initial lookup of nonexistent local `.bin/tsc` was
corrected to installed `/usr/local/bin/tsc`; no package reinstall. The first
health request immediately after restart was502; subsequent public readiness
readback and full live browser run passed, no lingering outage claimed.

Evidence directory: `artifacts/codex/voice-recall-20260906/`:
`schema-live-browser-final-receipt.json`, `schema-live-390-{landing,recording,asr-skeleton,results}.png`,
`schema-live-1440-results.png`, `second-integration-tests.log`,
`second-typecheck.log`, `runtime-activation-schema.json`,
`public-readback-clean2.json`, `public-layout.log`, `public-topic.log`.
Rendered screenshots inspected after visible image decoding, not build-only.
Worker patch equivalence verified before removing its completed3.24MB worktree;
all unique worker logs archived in parent artifacts, Git branch refs retained.

**Acceptance boundary:** this reproducer's precision/unsupported-city issue is
fixed in the protected preview; incident remains open for independent audit and
catalog freshness, not a claim of global retrieval completeness. No physical
phone/PWA or mailbox OTP run in this acceptance. Old frozen answers were not
rewritten or auto-replayed. Long mobile topic title visibly clips a third line;
recorded UI follow-up, not claimed visually perfect. Previously reported old
history restoration is also not closed by this new-conversation test.

Telegram delivery verified by exact text/topic readback:
https://t.me/c/4337049383/1458 (topic1030). It explicitly asks for a **new** query,
not recalculation of old history, and separates the open freshness/phone/UI gaps.

### User's subsequent three-turn chain (14:20–14:22 UTC)

Read-only inspection scoped to the known user's text/search receipts; no audio,
other users' histories, user state mutation or automatic replay.

1. `9d228dd8-cdb9-4977-b1ec-8e3b93c0ef6c`: symphonic music / organ next week.
  59checked,3exact7322/7424/7425,3possible. Date interpretation incorrectly used
  September14–20 on SundaySeptember6 (correct next calendar week is7–13).
2. `a5d24ed7-ea1b-4c45-aa94-67fe01e83a40`: “А если через неделю?” →21–27,
 57checked,3*different*exact7362/7427/7428,5possible. No cap3; catalog re-query.
3. `ceb5110e-2331-48ad-9d25-9ddee0fe4619`: “Куда можно пойти с детьми на
  ближайших выходных?” →12–13, children; **music goal correctly removed**.
  Zero candidates reached semantic verification. Confirmed cause: strict
  audience_tags prefilter on a field missing from the projection, not parent
  membership restricting the children query. All220active searchable projection
  rows in7–27September lack audience_tags in their card snapshots.

Source correction defers audience to mandatory verifier, preserves metadata tail,
clarifies union vs conjunction, next-week arithmetic and contextual topic change.
The current pre-search acknowledgement is not editorial: added optional grounded
post-search commentary through the same provider/limiter/checkpoint lifecycle.
Clean UI footer removal delegated in bounded source-only worker; no second shell.

Evidence: `artifacts/codex/voice-dialogue-review-20260906/` scoped
`user-chain.private.json`, public `projection-window.json`, `inventory.log`.
Source/live checks and exact delivery SHA will follow; old receipts remain frozen.
