# Lane TYPED-EVIDENCE Results

## Status
committed

## Requirement IDs
- R2.1-R2.6
- R3.1-R3.6
- R7.1-R7.2

## Branch
`agent/smart-update-llm-first-final/typed-evidence-core`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/smart-update-final-typed-evidence`

## Base SHA
`4aea905c35e84766d45e83c3c65d02b5e5acb188`

## Head SHA
Implementation commit: `3fe9476e38c48b6894d1cfb4c926c7dba0a9f6d5`

The final lane tip also contains this results-only record.

## Files changed
- `source_parse_contract.py`
- `main.py`
- `source_parsing/telegram/handlers.py`
- `kaggle/TelegramMonitor/telegram_monitor.py` (typed manifest/decision/combiner functions only; no prompt edits)
- `tests/test_source_parse_contract.py`
- `tests/test_event_parse_llm_first_contract.py`
- `tests/test_add_events_from_text_source_decision.py`
- `tests/test_ingestion_caller_retry_contract.py`
- `tests/test_tg_monitor_reprocess_incomplete_scan.py`
- `tests/test_telegram_monitor_llm_first_producer_contract.py`
- `.codex/lanes/TYPED-EVIDENCE/RESULTS.md`

## Delivered behavior
- Only an explicit, schema-consistent `SourceParseDecision` with a complete producer-owned manifest can terminally return `CONFIRMED_NO_EVENT`; empty inference, `None`, legacy `[]`, malformed mappings, missing manifests, unknown dispositions, and missing/unknown retry reasons become typed `SCHEMA_MISMATCH` retries.
- Legacy positive arrays/objects remain supported only when every event has the minimum shared event envelope (a non-empty string title).
- `EvidenceManifest` now canonicalizes attachment/OCR cardinality. Missing attachment OCR is counted unavailable; available-but-not-included OCR is marked omitted; missing/invalid receipt fields and contradictory counts fail closed. `enrichment_required` now serializes.
- Production normalization no longer manufactures `complete_source("")` when its manifest is absent. Direct `add_events_from_text` supplies its actual poster attachment count, so missing poster OCR cannot authorize a negative terminal.
- Telegram producer manifests apply the same cardinality rule. Album decisions are revalidated through the closed decision validator and a missing child manifest cannot combine into terminal no-event.
- Telegram consumer mappings are canonicalized through the central contract; missing/invalid manifests, source hash/cardinality mismatches, unknown decision fields, and decision-events versus message-events mismatches remain retryable. Positive children from incomplete evidence may still persist while the carrier remains due for enrichment.
- Mandatory verdict A-F and evidence A-E cases are covered in the owned suites; the historical no-manifest Telegram zero-tail terminal test is inverted.

## Commands run

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m py_compile \
  source_parse_contract.py main.py source_parsing/telegram/handlers.py \
  kaggle/TelegramMonitor/telegram_monitor.py \
  tests/test_source_parse_contract.py \
  tests/test_event_parse_llm_first_contract.py \
  tests/test_add_events_from_text_source_decision.py \
  tests/test_ingestion_caller_retry_contract.py \
  tests/test_tg_monitor_reprocess_incomplete_scan.py \
  tests/test_telegram_monitor_llm_first_producer_contract.py

/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_source_parse_contract.py \
  tests/test_event_parse_llm_first_contract.py \
  tests/test_add_events_from_text_source_decision.py \
  tests/test_ingestion_caller_retry_contract.py \
  tests/test_tg_monitor_reprocess_incomplete_scan.py \
  tests/test_telegram_monitor_llm_first_producer_contract.py

git diff --check
```

## Tests / verification
- Owned focused suite: **95 passed**, 8 SQLite adapter deprecation warnings, in 9.71s.
- Python compile: passed.
- `git diff --check`: passed.
- Diff audit: Telegram monitor changes are confined to `_source_evidence_manifest`, `_normalize_source_evidence_manifest`, `_parse_source_decision_response`, and `_combine_source_parse_decisions`; no legacy prompt string was edited.

A broader compatibility probe added `tests/test_vk_raw_first_llm_contract.py` and `tests/test_vk_auto_queue_import.py`: **133 passed, 8 failed**. All eight failures are stale bare-list VK auto-queue mocks whose forbidden `DraftParseResult` fallback still creates an inferred empty decision; MAP-TYPED already assigned that fallback to the integrator/other owner. No owned focused test failed.

## Risks
- `vk_intake.py` / `vk_auto_queue.py` receipt and `DraftParseResult` fixes are intentionally outside this narrowed lane. Until integrated, bare-list VK test doubles (and any equivalent old receipt facade) correctly surface the new fail-closed `SCHEMA_MISMATCH` rather than silently acquiring no-event authority.
- Canonical docs and `CHANGELOG.md` are integration-owned and were forbidden in this lane.
- No production operation, deploy, or push was performed.

## Merge notes
Cherry-pick implementation commit `3fe9476e38c48b6894d1cfb4c926c7dba0a9f6d5`, then the following results-only commit. Integrate the separately owned VK receipt/DraftParseResult changes before the full suite gate.
