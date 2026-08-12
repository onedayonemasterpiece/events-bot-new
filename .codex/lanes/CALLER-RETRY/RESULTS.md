# CALLER-RETRY lane results

## Scope

- Lane: `CALLER-RETRY`
- Requirements: `R17`, `R18`, `T63`, `T74`, `T76`
- Base SHA: `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`
- Implementation head SHA: `7bc65a49a` (`fix(ingestion): keep caller failures automatically retryable`)
- Effort/risk: high; cross-caller retry semantics and acceptance/diagnostic identity boundaries.

## Outcome

- Telegram Monitoring accepts the shared `SourceParseDecision` fields through a duck-typed rolling adapter and only resolves zero extraction for complete typed `CONFIRMED_NO_EVENT`.
- Untyped/unknown empty results, missing/incomplete OCR or evidence, verification failures, poster-bridge uncertainty, and child technical failures retain a durable `TelegramSourceForceMessage`; cursor/force cleanup occurs only after resolution.
- Legacy ad/esoterica/recurring/past detectors are diagnostics only at the caller and no longer terminally skip LLM-produced children.
- Official-parser item technical failures idempotently upsert `source_parser_recovery_request`; legacy two-field `(None, False)` results now mean retry, and retry counts make the run partial.
- Festival `error` rows use bounded exponential backoff, remain in normal due selection, and child/Telegram technical retries cannot be marked queue success.
- Forwarded registered Telegram posts persist a normal monitoring force row on technical/ambiguous empty parse; only complete typed no-event emits the no-event response.
- AST gate now rejects direct/getattr `diagnostic_event_id` assignment, return, storage, or helper pass-through outside direct logging, while retaining acceptance dominance for every direct `result.event_id` side-effect read.

## Changed files

- `source_parsing/telegram/handlers.py`
- `source_parsing/handlers.py`
- `festival_queue.py`
- `main_part2.py`
- `tests/test_ingestion_caller_retry_contract.py`
- `tests/test_smart_update_caller_typed_contract.py`
- `.codex/lanes/CALLER-RETRY/RESULTS.md` (this receipt)

`source_parsing/commands.py` was inspected but required no change: claimed parser recovery requests already bypass the change guard and unresolved requests settle back to `pending`.

## Evidence / commands

### Compile and focused contract suite

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_ingestion_caller_retry_contract.py \
  tests/test_smart_update_caller_typed_contract.py \
  tests/test_smart_update_parser_occurrences.py \
  tests/test_source_parsing_status.py \
  tests/test_source_parsing_commands.py

27 passed, 17 warnings in 13.68s
```

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m py_compile \
  source_parsing/telegram/handlers.py source_parsing/handlers.py \
  source_parsing/commands.py festival_queue.py main_part2.py \
  tests/test_ingestion_caller_retry_contract.py \
  tests/test_smart_update_caller_typed_contract.py

git diff --check
```

Both completed successfully.

### Broader compatibility probes

```text
pytest -q tests/test_source_parsing.py tests/test_source_parsing_commands.py \
  tests/test_source_parsing_existing_parser_attach.py \
  tests/test_source_parsing_status.py tests/test_telegram_monitor_service.py \
  tests/test_festival_queue_web_research.py
```

Result: `60 passed`, with three current-date fixture failures: one parser replay event dated `2026-07-24` is now past, and two festival web-research fixtures dated `2026-08-07..09` are past on `2026-08-11`.

```text
pytest -q tests/test_tg_monitor_reprocess_incomplete_scan.py \
  tests/test_inc_20260731_false_kgd80_festival_link.py \
  tests/test_inc_20260731_poster_candidate_url_telegram_replay.py \
  tests/test_static_collection_ingestion_replay.py \
  tests/test_static_collection_upstream_capture.py
```

Result: `39 passed`; one legacy assertion intentionally conflicts with the new strict contract by expecting an untyped `events=[]` carrier to advance the cursor. Three additional replay failures are current-date fixtures whose July/August 5 candidates are rejected as past by Smart Update on `2026-08-11`.

## Integration dependency / risks

- The semantic-contract lane owns `SourceParseDecision`; this lane intentionally defines no competing type. It consumes exact fields `disposition`, `events`, `lifecycle_actions`, `evidence_manifest`, `evidence_complete`, `parse_version`, and optional `verification`, with the agreed dispositions.
- The legacy zero-tail test must be migrated to a complete typed `CONFIRMED_NO_EVENT` fixture; leaving it untyped must continue to produce `retry_scheduled` by design.
- Canonical docs and `CHANGELOG.md` are integration-owned and were forbidden in this lane.
- No production writes, deploy, push, or recovery apply were performed.
