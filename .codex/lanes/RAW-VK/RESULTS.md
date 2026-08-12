# RAW-VK lane results

## Scope and revisions

- Lane: `RAW-VK`
- Requirements: R04, R08, R09, R10 (VK), R12/R13 (VK), R15, R18 (VK), R21; T01-T20, T31, T39-T55, T71-T75.
- Base PR head: `8614262f2`
- Required semantic-contract dependency: `004d0fc34`
- Implementation commit / head at evidence capture: `0df218d8b`
- Final lane head adds only this results receipt and is reported in the integration handoff (a commit cannot contain its own SHA).

## Delivered

- Added append-only `vk_source_packet` revisions and `vk_source_packet_attempt` funnel receipts, durable `vk_crawl_continuation`, and `vk_inbox.source_packet_id` plus due/quota/retry metadata.
- Persisted every fetched configured-source VK post before semantic selection. Cursor advancement is blocked by packet persistence failure; crawl caps create continuations.
- Changed revisions append/reparse; exact successful packet+prompt+model receipts replay without a provider call. Queue refetch detects edits before receipt lookup and appends the observed revision.
- Removed production prefilter/cancellation semantic bypass and all `reject_reason` authority. Keyword/date/past/far signals are hints only; blank/photo-only carriers remain due for OCR and LLM.
- Complete evidence manifests preserve every available OCR block and explicitly represent unavailable evidence. Incomplete evidence cannot terminalize a negative result; positive children persist while carrier enrichment remains due.
- Consumed typed source dispositions and lifecycle actions. Mixed events/actions resolve independently; lifecycle no-match and typed retry dispositions remain durable.
- Technical/provider/OCR/schema/persist/timeout/429/restart/orphan-lease failures release leases into capped durable backoff forever; no production `mark_failed` API or terminal technical inbox transition remains.
- Added age-fair queue ordering and static AST/text gates for semantic shortcuts.

## Durable schema names

`vk_source_packet`: `payload_hash`, `source_revision_hash`, `evidence_manifest_json`, `parse_result_json`, `successful_parse_key`, `prompt_version`, `model`, `quota_scope`, `provider_retry_after`, `next_attempt_at`, `attempts`, `lease_owner`, `lease_expires_at`, `last_typed_reason`, `terminal_carrier_outcome`.

`vk_source_packet_attempt`: `parse_key`, `payload_hash`, `source_revision_hash`, `evidence_manifest_json`, `llm_started`, `llm_completed`, `structured_response_valid`, `model`, `quota_scope`, `request_id`, `response_id`, `finish_reason`, `provider_retry_after`, `input_tokens`, `output_tokens`, `thought_tokens`, `reserved_tokens`, `primary_disposition`, verification fields, child/action counts, child/carrier outcomes, `next_attempt_at`, `typed_error_reason`.

## Validation

Python: `/home/dev/.venvs/events-bot-region-talk/bin/python`

- `python -m pytest -q tests/test_vk_intake_future.py tests/test_vk_intake_history.py tests/test_vk_intake_poster_budget.py tests/test_vk_auto_queue_import.py tests/test_vk_auto_queue_rate_limit.py tests/test_vk_review.py tests/test_vk_raw_first_llm_contract.py` -> **82 passed**.
- `python -m pytest -q tests/test_source_parse_contract.py tests/test_event_parse_llm_first_contract.py tests/test_vk_raw_first_llm_contract.py tests/test_vk_intake_quality_guardrails.py tests/test_vk_intake_keywords_dates.py::test_build_drafts_library_explicit_free_stays_free tests/test_vk_auto_queue_import.py::test_vk_auto_import_marks_inbox_imported_and_links_multiple_events tests/test_vk_auto_queue_import.py::test_vk_auto_import_keeps_valid_roundup_siblings_after_semantic_rejection tests/test_vk_auto_queue_import.py::test_vk_auto_import_continues_when_first_roundup_draft_is_rejected tests/test_vk_review_lock_retry.py tests/test_vk_review_show_next.py` -> **68 passed**.
- Final bounded VK queue/review/static suite after queue-refetch edit: `tests/test_vk_raw_first_llm_contract.py tests/test_vk_auto_queue_import.py tests/test_vk_auto_queue_rate_limit.py tests/test_vk_review.py` -> **68 passed**.
- `python -m py_compile db.py models.py vk_intake.py vk_auto_queue.py vk_review.py poster_media.py` -> pass.
- `git diff --check` -> pass.
- No live provider, push, production, or deployment calls.

## Changed files

- `db.py`
- `models.py`
- `vk_intake.py`
- `vk_auto_queue.py`
- `vk_review.py`
- `tests/test_vk_auto_queue_import.py`
- `tests/test_vk_auto_queue_rate_limit.py`
- `tests/test_vk_intake_future.py`
- `tests/test_vk_intake_history.py`
- `tests/test_vk_intake_poster_budget.py`
- `tests/test_vk_review.py`
- `tests/test_vk_raw_first_llm_contract.py`
- `.codex/lanes/RAW-VK/RESULTS.md`

## Risks / integration notes

- `main.py` quota reservation/inline-429 behavior is forbidden in this lane and explicitly owned by the integrator.
- Canonical docs and `CHANGELOG.md` were forbidden by the lane map and must be updated by integration/documentation owners.
- SQLite schema is additive and initialized through existing `Database.init`; production migration/deploy was not run.
- Provider usage IDs/tokens are stored when adapters expose them; current VK call boundary may provide only a subset until upstream provider metadata plumbing is integrated.
