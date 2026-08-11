# Lane TG-PRODUCER Results

## Status

committed

## Requirement IDs

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Emit the consumer-compatible closed `source_parse_decision` shape and typed retry reasons | Done | `SOURCE_PARSE_DECISION_SCHEMA`, `_parse_source_decision_response`, and every final `scan_source` message payload |
| R02 | Remove production-reachable pre-LLM semantic terminal skips and post-positive deterministic child deletion | Done | `scan_source` now calls `extract_source_parse_decision`; promo/giveaway/date logic is hint-only; no event cap/date rewrite; album merge preserves all children; AST test |
| R03 | Include raw text and all OCR/media coverage in an evidence manifest and forbid incomplete-evidence no-event | Done | `_source_evidence_manifest`; album parsing is deferred until every sibling OCR block is collected; unavailable photo/video evidence is explicit; incomplete no-event becomes `RETRY_REQUIRED/EVIDENCE_INCOMPLETE` |
| R04 | Carry cancellation/postponement/reschedule as typed lifecycle actions, including mixed results | Done | closed lifecycle enum/schema, prompt contract, validator, mixed and album-balance tests |
| R05 | Map empty/malformed/truncated/schema/provider/technical results to durable typed retry | Done | `_source_parse_retry`, response parser, provider/verification exception paths, deterministic tests |
| R06 | Keep one ordinary primary call and only one conditional verification for a closed contradiction | Done | `extract_source_parse_decision`; media albums receive one joint primary call; verification facts are closed and tests prove one normal call / two only on contradiction |
| R07 | Ensure every emitted message has a decision so legacy zero rows do not retry only because producer omitted the verdict | Done | ordinary messages receive a decision immediately; grouped albums receive it after merge and before return; static guard |
| R08 | Add focused deterministic tests and static guards for required Telegram producer cases | Done | `tests/test_telegram_monitor_llm_first_producer_contract.py` (15 tests), plus existing Telegram/consumer compatibility suites |
| R09 | Do not add a model/provider or bypass the shared Google AI gateway | Done | existing `_call_model` / `GoogleAIClient` path retained; only output budget became call-selectable for the typed multi-event result |
| R10 | Respect lane scope and perform no production/network/deploy/push work | Done | only owned producer, focused test, and this receipt changed; no provider/live/production call, deploy, recovery write, or push |

## Branch

`agent/smart-update-llm-first/tg-producer`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/smart-update-tg-producer`

## Base SHA

`af3cafec4af8ff25ec599aa805531599548e546e`

## Implementation SHAs

- `78d9f734dd190ef5ddbbd5abc00455856c3acf0e` — `fix(tg-monitor): emit typed LLM-first source decisions`
- `0c2fbc5ac39b7f225affa66f611ef3872f57299e` — `fix(tg-monitor): parse albums with complete OCR evidence`

## Files changed

- `kaggle/TelegramMonitor/telegram_monitor.py`
- `tests/test_telegram_monitor_llm_first_producer_contract.py`
- `.codex/lanes/TG-PRODUCER/RESULTS.md`

No forbidden application, parser, database, docs, changelog, or production files were changed.

## Commands run

```text
cat AGENTS.md
cat /home/dev/.codex/attachments/688350f8-1040-4fdd-b50e-c14adb5d65f7/pasted-text.txt
cat /home/dev/.agents/skills/feature-fanout/SKILL.md
sed/grep inspection of source_parse_contract.py, source_parsing/telegram/handlers.py,
kaggle/TelegramMonitor/telegram_monitor.py, and focused tests

/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_telegram_monitor_llm_first_producer_contract.py \
  tests/test_tg_monitor_gemma4_contract.py \
  tests/test_telegram_monitor_service.py \
  tests/test_source_parse_contract.py \
  tests/test_ingestion_caller_retry_contract.py

python3 -m py_compile \
  kaggle/TelegramMonitor/telegram_monitor.py \
  tests/test_telegram_monitor_llm_first_producer_contract.py

python3 scripts/inspect/audit_google_ai_provider_paths.py
git diff --check
git diff --check af3cafec4..HEAD
git status --short --branch
git diff --name-only af3cafec4..HEAD
```

## Tests / verification

- Final focused/compatibility suite: **105 passed, 8 warnings in 9.30s**.
  The warnings are existing Python 3.12 `aiosqlite` datetime-adapter deprecations in caller recovery tests.
- `py_compile`: passed.
- `git diff --check` and base-to-head diff check: passed.
- Scope check before this receipt: only the owned producer and focused producer-contract test differed from base.
- Static producer guard proves `scan_source` no longer calls the legacy `extract_events`, strips promo lines before the LLM, applies an event-child cap, or gives free-form rejection authority.

### Google AI path audit blocker evidence

The mandatory offline audit ran but is **not green**:

```text
Google AI provider path audit: FAIL
scanned_files=1137
summary=approved_gateway=10 approved_embedded_gateway=0 approved_dependency_probe=3 allowlisted_debt=0 unapproved=14 unreadable_files=0
```

All 14 findings are in the pre-existing generated notebooks
`kaggle/GuideExcursionsMonitor/guide_excursions_monitor.ipynb` and
`kaggle/TelegramMonitor/telegram_monitor.ipynb`; neither notebook differs from this lane's base and both are outside lane ownership. The changed runtime producer continues to call the existing `GoogleAIClient` gateway. No real Google/model request was made.

## Risks

- The legacy `extract_events` implementation remains in the large runner for compatibility/reference, including old rescue/sanitizer code, but `scan_source` has no call edge to it. A future caller must not reintroduce that path; the AST guard covers the production scan boundary.
- Video frames/transcripts are not inputs to the primary text/OCR semantic call. Video presence is therefore represented as unavailable evidence, which prevents false `CONFIRMED_NO_EVENT` and keeps positive carriers enrichment-retryable, but can increase backlog until a video-evidence stage closes that manifest.
- Provider finish metadata is not exposed by the current `_call_model` return contract. The adapter detects syntactically truncated output, raises the source-result output budget to 4096, validates the complete closed schema, and retries contradictions; a syntactically valid but semantically cut provider result remains a residual provider-boundary risk.
- A media album interrupted by the existing crawl/message cap could still lack an unfetched sibling; downloaded/seen siblings are jointly parsed, while an upstream continuation/crawl-boundary lane remains responsible for proving no album split at the horizon.
- Canonical docs and `CHANGELOG.md` updates are integration-owned and were explicitly forbidden in this lane.

## Merge notes

- Cherry-pick both implementation commits in order, then this results commit.
- The producer deliberately serializes mappings rather than importing `source_parse_contract.py`, because the Kaggle notebook stages this runner as a self-contained script. Field names, dispositions, lifecycle actions, retry reasons, evidence fields, and parse version match the integrated consumer contract.
- The existing notebook path-audit failures must be resolved or regenerated by the integration/notebook owner before any live Google consumer gate; do not treat this lane receipt as a green provider-path audit.
