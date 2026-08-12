# Lane PROMPT-GATES Results

## Status
committed

## Requirement IDs
- R2: VK typed verdict adapters and durable receipt validation
- R4: live prompt typed-policy migration and CI prompt gate
- PROMPT-GATES: dynamic A-I, receipt/evidence, mutation, and legacy-adapter regressions

## Branch
`agent/smart-update-llm-first-final/prompt-vk-adapters`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/smart-update-final-prompt-gates`

## Base SHA
`e460704bf30d1f770c4e2748650de2a6e69ac657`

## Head SHA
Implementation commit: `8f60c4158`

## Files changed
- `.github/workflows/ci.yaml`
- `docs/llm/prompts.md`
- `kaggle/TelegramMonitor/telegram_monitor.py`
- `scripts/inspect/audit_source_parse_prompt_contract.py`
- `source_parse_contract.py`
- `tests/test_source_parse_contract.py`
- `tests/test_source_parse_prompt_contract.py`
- `tests/test_tg_monitor_gemma4_contract.py`
- `tests/test_vk_auto_queue_import.py`
- `tests/test_vk_raw_first_llm_contract.py`
- `vk_auto_queue.py`
- `vk_intake.py`
- `vk_review.py`

## Commands run
- `python3 scripts/inspect/audit_source_parse_prompt_contract.py --root .`
- `python3 -m py_compile source_parse_contract.py vk_intake.py vk_auto_queue.py vk_review.py kaggle/TelegramMonitor/telegram_monitor.py scripts/inspect/audit_source_parse_prompt_contract.py tests/test_source_parse_contract.py tests/test_source_parse_prompt_contract.py tests/test_tg_monitor_gemma4_contract.py tests/test_vk_raw_first_llm_contract.py tests/test_vk_auto_queue_import.py`
- `uv run --with pytest --with-requirements requirements.txt python -m pytest -q tests/test_source_parse_contract.py tests/test_source_parse_prompt_contract.py tests/test_vk_auto_queue_import.py`
- `uv run --with pytest --with-requirements requirements.txt python -m pytest -q tests/test_event_parse_llm_first_contract.py tests/test_prompt_json.py tests/test_telegram_monitor_llm_first_producer_contract.py tests/test_tg_monitor_gemma4_contract.py`
- `uv run --with pytest --with-requirements requirements.txt python -m pytest -q tests/test_tg_monitor_gemma4_contract.py`
- `uv run --with pytest --with-requirements requirements.txt python -m pytest -q tests/test_vk_raw_first_llm_contract.py -k 'live_prompt_a_i or untyped_empty_none or legacy_positive_list or without_decision or receipt_a_f or receipt_decision or direct_poster or explicit_ocr or queue_legacy or legacy_positive_receipt or invalidates_old'`
- full `tests/test_vk_raw_first_llm_contract.py` assertion run
- `git diff --check`

## Tests / verification
- Prompt audit: PASS, three live surfaces; unreachable TG legacy extractor excluded with call-graph guard.
- Source contract + prompt gate + VK auto queue: **80 passed**.
- Event-parse + prompt JSON + TG live producer + TG Gemma contract: **100 passed**.
- TG Gemma contract standalone: **54 passed**.
- New/owned VK prompt, adapter, receipt, OCR/evidence subset: **31 passed, 12 deselected**.
- Full VK raw-first suite printed **43 passed**. The process then lingered during Python threading shutdown in the shared concurrent test environment and was interrupted after the completed pytest result; the focused owned subset exits cleanly.
- `py_compile`, prompt audit, and `git diff --check`: PASS.

## Requirement closure
- **Done — typed VK adapters:** omitted decisions, bare empty/None/malformed shapes, missing/invalid receipts, unknown dispositions/reasons, and mismatched decision/draft envelopes cannot become terminal no-event or exact replay.
- **Done — positive compatibility:** only the central validated legacy-positive adapter remains; poster objects are excluded from receipts and JSON serialization is covered.
- **Done — evidence behavior:** OCR state is initialized, attachment/OCR cardinality is explicit, incomplete positives survive with enrichment, and incomplete negatives retry.
- **Done — typed no-event reason:** optional closed `SourceNoEventReason` is preserved by core/VK/TG; giveaway-only requires and tests `GIVEAWAY_ONLY`; unknown/misplaced values retry with schema mismatch.
- **Done — prompts:** MASTER, live VK builder, and live TG source prompt use typed dispositions; dynamic A-I covers generic no-event, incomplete cards, incomplete positives, giveaway-only, giveaway+event, teaser, lifecycle-only, location-hint-only, and ticket-hint-only with one provider call.
- **Done — CI gate:** normalized legacy carrier-empty mutations fail; live typed policy requirements and the TG dead-function exclusion/call-graph invariant are enforced in focused CI.

## Risks
- The retained legacy Telegram `extract_events` body still contains old empty-array language by design. CI proves it has no live callers and fails if it becomes reachable.
- No provider/model/call-count change and no deterministic semantic classifier were introduced.
- Full raw-first pytest shutdown linger is environmental/shared-runtime cleanup after all assertions passed; focused owned tests exit zero.

## Merge notes
- Cherry-pick `8f60c4158` and the following RESULTS-only commit.
- No push performed.
