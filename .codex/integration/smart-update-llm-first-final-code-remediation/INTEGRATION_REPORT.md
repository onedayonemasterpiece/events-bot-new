# P0 final-code remediation — integration report

Date: 2026-08-12 UTC
Repository / PR: `onedayonemasterpiece/events-bot-new`, Draft PR #494
Reviewed base: `5291c42897db8a157f0f9699bc678be42f96a331`
Implementation/docs freeze used by the retained local receipt:
`fbcd782a885fb77f6020e5705ec5999655f7706e`
Final exact HEAD: the pushed Draft PR ref and final PR comment are authoritative;
this report cannot include the SHA of the commit that contains itself.

At intake the local and remote PR heads both matched the reviewed base, so the
intake delta was empty. No new PR or issue was created. No merge, deploy,
production mutation, VK refetch, model replay, or recovery apply was performed.
The incident remains **open P0 / SEV-1** and this report is **not** a deployment
or recovery-apply approval.

## Verdict

The four code-level recall blockers reported against `5291c4289` are corrected
and locally covered. They are ready for exact-head Draft PR review, but the
system is still **not deploy-ready** because the four external release gates at
the end of this report remain open.

| Blocker | Reviewed behavior | Corrected behavior | Principal proof | Code status |
|---|---|---|---|---|
| C — continuation drift | A full page equal to the previous fingerprint became terminal `done/EXACT_PAGE_REPLAY`, which could strand the old tail after head insertion. | Repeated/non-deeper full pages become typed `OFFSET_DRIFT` or `NO_PROGRESS`, store due retry/backoff and deterministic `offset + page_size` rebase; `done` requires a real boundary. Legacy poisoned rows reopen. | Dynamic `<P`, `=P`, `>2P`, ignored-offset, restart, true-boundary, init×2 and collision tests. | **Done** |
| V — conditional verification | Only incomplete evidence and the generic-title defender were reliably produced; five declared contradiction classes had no common production producer. | One pure shared fact collector emits all seven closed reasons. Main/VK/direct/parser and the staged TG producer use the same implementation; facts only request at most one verifier and never decide product semantics. | Unit reason fixtures plus production main, VK and TG wiring tests; technical/uncertain verifier preserves positive children in retry. | **Done** |
| N — negative verdict authority | A complete typed `CONFIRMED_NO_EVENT` could terminate without a reason. | `no_event_reason` is mandatory iff `CONFIRMED_NO_EVENT`, belongs to a seven-value enum, and survives receipts/metrics. Missing, unknown or misplaced values become `RETRY_REQUIRED/SCHEMA_MISMATCH`. | Provider, compatibility, VK receipt/replay, TG album/consumer and direct facade regressions; live prompt static parity. | **Done** |
| R — durable VK packet | Initial crawl flattened the first repost and photo URLs before persistence; outer text, sibling/nested reposts and attachment semantics could be lost before a later failure. | A shared sanitized v1 envelope preserves outer + recursively ordered copy history, raw attachment inventory and semantic media metadata before LLM. Crawl, continuation, fresh fetch and legacy facade derive from the same builder; every fresh-fetch failure may replay only a complete v1 packet. | Recursive envelope/revision/security/user-wall tests and deleted/access/network/API fallback tests. Census/recovery distinguishes lossless, legacy-incomplete and unavailable. | **Done** |

## C — mutable-offset continuation state machine

```text
pending | retry_scheduled | expired running
  -> BEGIN IMMEDIATE claim / exclusive lease / attempts+1
  -> fetch frozen owner, mode, page_size, horizon, original cursor, offset
  -> persist every fetched source revision
  -> derive the deepest observed (date, post_id)
  -> one CAS transition:
       empty page                         -> done / EMPTY_PAGE
       short page                         -> done / SHORT_PAGE
       backfill horizon crossed           -> done / HORIZON_REACHED
       original incremental cursor met    -> done / ORIGINAL_CURSOR_OVERLAP
       full repeated or non-deeper page   -> retry / OFFSET_DRIFT|NO_PROGRESS
                                             offset += page_size, backoff, no cursor move
       full page with deeper progress     -> advance or bounded yield
```

A fingerprint is now loop/drift evidence only. It is never proof of end-of-wall.
The canonical crawl cursor remains owned by the primary crawl. Continuations
retain their immutable original boundary and durable deepest `(date, post_id)`.
A provider that ignores offsets produces bounded retries rather than a hot loop
or false completion.

`Database.init()` idempotently reopens only the historical poison shape
`status='done' AND last_typed_reason='EXACT_PAGE_REPLAY'`; the scheduling path
also reopens that shape defensively. True historical terminal rows are untouched.
Legacy target-offset uniqueness collisions remain due with typed
`OFFSET_DRIFT_COLLISION`, rather than silently becoming stale or done.

### C test mapping

| Case | Exact pytest node |
|---|---|
| A — exact prior full page is nonterminal | `tests/test_vk_crawl_continuation.py::test_exact_full_page_rebases_with_retry_and_never_completes` |
| B/C/D — `<P`, `=P`, `>2P` head insertion drains the full old tail | `tests/test_vk_crawl_continuation.py::test_head_insert_drift_less_equal_and_greater_than_page_drains_tail` |
| E — provider ignores offset | `tests/test_vk_crawl_continuation.py::test_provider_ignoring_offset_is_bounded_retry_not_done` |
| F — legacy poison init×2 and scheduler reopen | `tests/test_vk_crawl_continuation.py::test_legacy_exact_done_reopens_on_initx2_and_schedule` |
| F — uniqueness collision remains retryable | `tests/test_vk_crawl_continuation.py::test_legacy_target_offset_collision_stays_retry_not_stale_or_done` |
| G — proven original-cursor boundary | `tests/test_vk_crawl_continuation.py::test_full_page_original_cursor_is_a_proven_terminal` |
| H — restart after rebase | `tests/test_vk_crawl_continuation.py::test_rebased_offset_survives_restart_and_drains_tail` |
| Retained raw-first safety | `::test_mid_page_persistence_failure_does_not_advance_offset`, `::test_stale_running_lease_is_recovered`, `::test_concurrent_workers_cannot_process_same_row` |

## V — seven closed conditional-verification reasons

The collector receives source text, **all** OCR blocks, stable source metadata,
the primary typed decision and its evidence manifest. It returns only
`ContradictionFact` values. It cannot change a disposition, assign a no-event
reason, delete a child, or persist a product outcome.

| Closed reason | Objective fact produced | Production surfaces | Dynamic proof |
|---|---|---|---|
| `NO_EVENT_WITH_STRONG_SIGNALS` | A primary no-event conflicts with independent invitation, event-type, attendance and date/time signals. | Shared main path (VK, direct/forwarded, official parser/festival) and staged TG path. | `test_v1_strong_no_event_uses_multiple_independent_signal_classes`; `test_v1_vk_production_wiring_auto_verifies_strong_no_event_contradiction`. |
| `EVENT_DATE_CONFLICT` | The sole unambiguous source/OCR date and sole parsed child date differ. | Same shared main and TG collector. | `test_v2_ocr_date_conflict_is_derived_from_unambiguous_dates`; main/TG parameterized wiring tests. |
| `MULTIPLE_OCCURRENCES_COLLAPSED` | Distinct dates/session times, concrete OCR cards, or source occurrence anchors exceed returned children, excluding explicit range/recurrence. | Same shared main and TG collector; parser occurrence metadata can provide anchors. | `test_v3_three_same_slot_ocr_cards_are_distinct_occurrence_anchors`; production main/TG wiring tests. |
| `GENERIC_UNGROUNDED_TITLE` | A positive child title is empty, placeholder, or bare generic type/venue form. | Shared collector plus existing main defender path. | `test_v4_generic_title_fact_does_not_delete_child`; `test_generic_title_uses_conditional_verifier_not_deterministic_veto`. |
| `LIFECYCLE_MIXED_CONTENT_CONFLICT` | A lifecycle-only decision coexists with a future new-event invitation and schedule evidence. | Same shared main and TG collector. | `test_v5_lifecycle_only_plus_new_invitation_is_a_verifier_fact`; main/TG production wiring tests. |
| `IMPOSSIBLE_SCHEMA_VALUE` | Invalid date/time, reversed range, negative/reversed ticket price, or invalid typed boolean. | Same shared main and TG collector. | `test_v6_impossible_date_and_time_are_objective_schema_facts`; main/TG production wiring tests. |
| `INCOMPLETE_EVIDENCE` | Missing/incomplete/truncated manifest or attachment/OCR cardinality gap. | All facades through the common evidence contract. | `test_v7_incomplete_manifest_is_a_fact_and_positive_survives`; VK/TG positive-incomplete durable retry tests. |

The main path performs one primary parse. Zero contradiction facts means no
verifier call. One or more facts are ordered/deduplicated and passed together to
at most one conditional verifier call. Verifier technical failure or
uncertainty returns a typed retry and keeps positive children/actions; it never
turns a deterministic fact into a product reject.

Key exact nodes:

- `tests/test_event_parse_llm_first_contract.py::test_t38_normal_carrier_has_exactly_one_primary_call`
- `::test_t32_t35_only_closed_contradiction_classes_trigger_verifier`
- `::test_v1_v2_v3_v6_production_collector_automatically_invokes_one_verifier`
- `::test_v5_lifecycle_plus_new_event_automatically_invokes_verifier`
- `::test_v10_auto_verifier_timeout_preserves_positive_sibling`
- `tests/test_telegram_monitor_llm_first_producer_contract.py::test_v2_v3_v5_v6_telegram_uses_shared_production_collector`
- `::test_v10_telegram_verifier_timeout_preserves_positive_sibling`

## N — mandatory closed negative reason

`SourceNoEventReason` is exactly:

- `NO_ATTENDABLE_EVENT`
- `GIVEAWAY_ONLY`
- `VAGUE_TEASER`
- `REFERRAL_ONLY`
- `SERVICE_OR_RENTAL`
- `RECAP_ONLY`
- `OUT_OF_SCOPE`

A semantic negative terminal is valid only when all of these are true:

```text
disposition == CONFIRMED_NO_EVENT
events == []
lifecycle_actions == []
evidence_complete == true
no_event_reason in SourceNoEventReason
no unresolved strong contradiction fact
```

| Boundary/input | Result |
|---|---|
| Valid typed complete no-event + closed reason | `CONFIRMED_NO_EVENT`; reason persisted and observable. |
| Reasonless or unknown no-event | `RETRY_REQUIRED/SCHEMA_MISMATCH`; warning/alert, never terminal. |
| `no_event_reason` on positive, lifecycle, mixed or retry disposition | Schema retry; misplaced reason has no authority. |
| Legacy `[]`, `None`, untyped empty/malformed object | Schema retry; never inferred no-event. |
| Old/malformed VK success receipt | Invalidated and reparsed; no exact-replay authority. |
| Receipt decision/draft count or normalized-title mismatch | Invalidated and reparsed. |
| TG album/server message missing/unknown reason or mismatching event arrays | Typed retry; cursor/force state remains due. |
| Positive result with incomplete evidence | Children/actions survive; carrier remains due for enrichment. |

The master, VK overlay and live Telegram prompts require the reason for every
no-event. The audit extracts all four live prompt/schema surfaces, requires enum
parity, rejects an optional/reasonless contract and proves the historical TG
extractor remains unreachable.

Principal nodes:

- `tests/test_source_parse_contract.py::test_n1_reasonless_confirmed_no_event_is_schema_retry`
- `::test_n2_all_closed_no_event_reasons_round_trip`
- `::test_unknown_or_misplaced_no_event_reason_is_schema_retry`
- `tests/test_vk_raw_first_llm_contract.py::test_receipt_a_f_invalid_typed_fields_force_invalidation_and_reparse`
- `::test_n7_vk_terminal_rejection_requires_closed_reason_and_persists_typed_value`
- `tests/test_telegram_monitor_llm_first_producer_contract.py::test_n1_n3_telegram_reasonless_or_unknown_no_event_is_schema_retry`
- `::test_n2_telegram_preserves_every_closed_no_event_reason`
- `::test_n8_album_preserves_unanimous_reason_and_uses_umbrella_for_mixed_reasons`
- `tests/test_ingestion_caller_retry_contract.py::test_zero_extraction_requires_complete_typed_no_event`

## R — durable VK envelope and replay contract

The shared `vk_source_envelope.py` builder consumes a single provider wall item,
not request parameters or exception objects. Its sanitized v1 envelope retains:

```json
{
  "schema": "kenigevents.vk_source_envelope",
  "schema_version": 1,
  "owner_id": 123,
  "owner_type": "group",
  "post_id": 456,
  "raw_item": {"text": "outer announcement", "attachments": [], "copy_history": []},
  "text_segments": [
    {"path": "$", "role": "outer", "text": "outer announcement"},
    {"path": "$.copy_history[0]", "role": "copy", "text": "nested source text"}
  ],
  "text": "outer announcement\n\n[Репост $.copy_history[0]]\nnested source text",
  "attachment_inventory": [
    {"path": "$.attachments[0]", "type": "link", "semantic": {"url": "https://example.invalid/event", "title": "Tickets"}}
  ],
  "all_media_candidates": [],
  "media_candidates": [],
  "omitted_media_candidates": [],
  "unavailable_visual_attachments": [],
  "counts": {"attachment_inventory_count": 1, "text_segment_count": 2},
  "completeness": {"capture_complete": true, "attachment_inventory_complete": true}
}
```

The example is synthetic and contains no production source or secret.
`payload_hash` covers the full canonical envelope. `source_revision_hash` covers
the ordered semantic projection: outer/all recursive text and attachment
content/IDs/URLs/order. Provider counters and JSON key order do not open a new
revision; outer commentary, direct attachment, sibling/nested copy text, and
link/video/doc semantic changes do.

OCR/media caps select processing candidates but do not erase raw inventory;
omitted and unavailable visuals are explicit. Request access tokens,
Authorization/captcha/error payloads and token-like URL parameters are denied or
redacted. An attachment `access_key`, when needed for replay, remains only in the
protected raw/inventory payload and is excluded from semantic hashes and logs.

The same builder feeds primary crawl, continuation, `wall.getById` fresh fetch
and the legacy review facade. User-wall `owner_type='user'` keeps a positive
signed owner ID instead of being coerced to a community ID. Fresh capture is
persisted before semantic parsing and idempotency is decided by packet hashes,
not by a lossy text/photo precomparison.

Every failed fresh fetch (`not_found`, access denied, network error, VK API
error) follows this fallback:

```text
complete durable v1 packet -> parse preserved combined evidence
legacy/incomplete packet   -> RETRY_REQUIRED/EVIDENCE_INCOMPLETE
missing/unusable packet    -> typed SOURCE_FETCH_ERROR or EVIDENCE_UNAVAILABLE retry
```

There is no stale projected-text success path. Production has not yet deployed
the v1 schema, so the current read-only census honestly reports zero lossless
packets rather than treating historical projections as complete.

### R test mapping

| Case | Exact pytest node |
|---|---|
| R1 outer + sibling + nested copies | `tests/test_vk_source_envelope.py::test_builder_preserves_outer_sibling_and_nested_copy_evidence` |
| R2 ordered photo/link/video/doc/nonvisual inventory | `::test_builder_inventory_keeps_semantic_nonvisual_attachments_and_previews` |
| R3 crawl/continuation v1 persistence | `tests/test_vk_crawl_continuation.py::test_continuation_persists_same_v1_envelope_contract_as_primary_crawl`; raw-first envelope assertions |
| R4 semantic revision matrix | `tests/test_vk_source_envelope.py::test_semantic_revision_hash_matrix_ignores_counters_and_key_order`; `tests/test_vk_raw_first_llm_contract.py::test_envelope_revision_matrix_reopens_on_recursive_semantic_edits_only` |
| R5 shared fresh builder and pre-LLM persistence | `tests/test_vk_raw_first_llm_contract.py::test_fresh_fetch_persists_recursive_revision_before_typed_parser` |
| R6 all fresh failures use complete/legacy/missing rails | `::test_any_fetch_failure_replays_complete_envelope_into_typed_parser`, `::test_any_fetch_failure_keeps_legacy_packet_incomplete_without_parser`, `::test_fetch_failure_without_packet_uses_typed_technical_retry_not_stale_text` |
| R7 honest census/recovery replayability | `tests/test_smart_update_loss_census.py::test_packet_replayability_requires_valid_complete_v1_envelope`; recovery planner tests |
| R8 shared facades, user wall and security | `tests/test_vk_source_envelope.py::test_crawl_fresh_and_legacy_facades_share_recursive_derivation`, `::test_user_wall_owner_type_is_positive_and_canonical_across_crawl_and_fresh`, `::test_security_denylist_is_recursive_but_attachment_access_key_is_retained` |

## Static and caller inventory

- `scripts/inspect/audit_source_parse_prompt_contract.py` inventories the live
  master, VK, TG runtime and embedded TG surfaces; requires all seven
  verification reasons, all seven no-event reasons and mandatory reason syntax.
- AST/source gates forbid pre-LLM semantic terminals, hint-based eligibility,
  technical `mark_failed`, free-form reject authority and diagnostic event IDs
  driving accepted side effects.
- Continuation tests prove no repeated-fingerprint path writes terminal `done`.
- Raw-first tests prove packet persistence occurs before semantic parsing and
  recovery never claims missing attachments as restorable.
- TG service tests prove the exact shared contract and collector modules are
  staged into the notebook; the historical untyped extractor is unreachable.
- No new provider/model, unconditional verifier, operator gate, semantic regex
  product verdict, or TPM semantic filter was introduced.

The historical T01–T76 matrix remains the regression index, with T13, T21 and
T32–T38 corrected to the new exact node/invariant evidence. T68 remains
**Partial**: recovery can select the class, but missing historical payloads
cannot be reconstructed or called recovered events.

## Local verification receipt

Retained ignored log:
`artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/p0-final-code-remediation/local-validation/relevant-ci-final.log`

On code/docs freeze `fbcd782a885fb77f6020e5705ec5999655f7706e`:

- exact final focused inventory: **583 passed, 17 warnings in 84.56s**;
- source-prompt audit: **PASS**, four live surfaces, enum parity, legacy TG
  extractor unreachable;
- Google AI provider path audit: **PASS**, 1,141 files, 0 unapproved, 0
  unreadable, 0 allowlisted debt;
- production-module `py_compile`: **PASS**;
- `git diff --check 5291c4289..HEAD`: **PASS**;
- retained log SHA-256:
  `898059019e5c2cb8e5af5bbaec1619cd8b8c71cdd591fdfa339ee409deb14f65`.

Lane receipts are also retained in `.codex/lanes/CONTINUATION-DRIFT/`,
`VERIFICATION-NOEVENT/`, `RAW-PACKET/`, and `DOCS-CI/`. The GitHub Actions
status for the exact final report commit is intentionally not guessed here; the
pushed PR ref and final PR comment are the authoritative external receipt.

## Strict read-only production repeat

Final census/recovery sources were executed in memory on Fly against
`/data/db.sqlite`; no remote script or data file was written. The census and
recovery plan each ran twice for the half-open window
`[2026-08-04T00:00:00Z, 2026-08-12T00:00:00Z)` with batch 10,000 and discovery
misses included.

| Evidence | Result |
|---|---|
| SQLite safety | URI `mode=ro`, `query_only=1`, `quick_check=ok`, write probe rejected, `total_changes=0`. |
| DB identity | Before/after SHA-256 both `e6811b14d0634c6cc1391d3575eb3985753fd65b49c15030c8e0fe907fdbd6b3`; inode/size/mtime unchanged. |
| Census determinism | Two equal report hashes `906279dc8e643a3de75cfb8efcee107d5d6466f5ae7a607a923653337a3b13ba`; inventory hash `5f3e6f262a7b200613a9f24148c1057491b4194f5720c9bf1dc08aa215c47220`. |
| Census units | 676 `carrier_revision` rows; 256 extracted occurrences reported separately; 9 incomplete; 240 unavailable. No extrapolation. |
| Recovery determinism | Two equal report hashes `2624dceb002c51903f9e7c3bea70cf43f13220e31ea1311d3f79d3f5d4d1cdd2`; plan hash `de092562c4892a74b73dc01d70f18945c2b83a95e7c5ccffecc963ff0e92fc5e`. |
| Recovery effect | selected/would-change/changed carriers `125 / 125 / 0`; direct Event insert false. |
| Replayability | `replayable_lossless=0`, `replayable_legacy_incomplete=436`, `unavailable=240`; attachment-restore eligible `0`. |
| Model results | LLM started/completed and model-derived create/merge/noop/retry/lifecycle/no-event totals remain unavailable/zero; they are not inferred. |

Source hashes: census
`867faddb9246a49a41d9684ce567fdaccf2547aa8cd16c9a3eacc8006af921f0`;
recovery
`b0215f813358e59ee472348dc02773ef77cc749d525545bae132cc4b94314a5d`.
The plan only validates replayability and requeues the normal processor; it
never inserts Events directly.

Ignored evidence directory:
`artifacts/codex/INC-2026-08-10-smart-update-identity-terminal-loss/p0-final-code-remediation/production-read-only/`.
Receipt SHA-256:
`a311b818254480f7421f8344298b01b3d24488aed5db1572b872b3b8c5844fb3`;
redaction scan passed; manifest SHA-256:
`901c5483de462bbf54e88b7ca42cbf21b912afd17b8ef286a5003a8dcabd3f5d`.

This is an inventory/requeue plan, **not** a model-derived estimate of recovered
events. It performed no production mutation, refetch, provider call, recovery
apply, deploy, or merge.

## Requirement closure

| Requirement group | Status | Evidence / remaining condition |
|---|---|---|
| C1–C8 continuation | **Done** | Nonterminal drift/rebase, true boundaries, deepest key, legacy reopen, dynamic A–H. |
| V1–V11 verification | **Done** | Shared seven-reason fact collector, main/VK/direct/parser/TG wiring, max one verifier, positive preservation. |
| N1–N9 no-event reason | **Done** | Mandatory closed reason across provider, compatibility, receipts/replay, TG albums/consumer, direct caller, prompts and static audit. |
| R1–R8 raw packet | **Done** | Recursive sanitized envelope, semantic revision hash, all fetch-failure fallback rails, owner parity, honest census/recovery. |
| E1–E10 code/docs/local evidence | **Done** | Canonical docs/CHANGELOG/CI, superseding report, exact local receipts and no-scope audit. |
| E11–E12 production RO evidence | **Done** | Two deterministic census and recovery-plan runs; DB hash/stat unchanged; changed=0. |
| T01–T76 regression matrix | **Partial** | All relevant code gates pass; T68 intentionally remains Partial until historical/model replay exists. |
| GitHub exact-head Actions | **Pending external receipt at report commit time** | Must be green on the exact pushed Draft PR SHA; final PR ref/comment is authoritative. |
| Merge/deploy/recovery apply | **Blocked / forbidden in this task** | Explicitly not performed. |

## Release blockers and next safe sequence

The code remediation does not waive these four independent gates:

1. **Provider entitlement:** verify real provider-side RPM/TPM/RPD, usage tier
   and spend limits for each configured quota scope.
2. **Atomic production snapshot:** rehearse full init/init×2/rollback/read-only
   census on a guaranteed atomic current production SQLite snapshot.
3. **195 existing FK orphans:** obtain an approved, hash-bound repair or
   disposition plan; no repair was applied here.
4. **Model-derived recovery:** after deployment of the typed/raw pipeline, run
   the selected carriers through read-only model-derived replay before any
   approved small-batch recovery apply.

Required sequence remains:

```text
exact-head review + green Draft PR CI
  -> merge only with approval
  -> provider + atomic snapshot + orphan gates
  -> deploy with recovery disabled
  -> health/migration/canary
  -> model-derived read-only replay
  -> separately approved recovery apply in bounded batches
```

Until then Draft PR #494 must remain Draft, the incident remains open, and the
production ingestion/recovery state remains unchanged.
