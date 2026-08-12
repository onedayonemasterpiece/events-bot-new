# SEMANTIC-CONTRACT lane results

## Scope

- Lane: `SEMANTIC-CONTRACT`
- Requirement IDs: `R03`, `R10-R14`, `R16`, `R18`; tests `T21-T38`; contract hooks for `T15-T20`, `T39-T44`.
- Base SHA: `8614262f2c2a5489169cf3c7fa5bf8ab19c83b97`
- Implementation SHA: `076408da1`
- Effort/risk: high; cross-cutting parse response contract, but deliberately dependency-light and backward-compatible at the list boundary.

## Result

Implemented a closed source-level semantic boundary:

- `SourceDisposition`: `EVENTS_FOUND`, `CONFIRMED_NO_EVENT`, `LIFECYCLE_ONLY`, `MIXED`, `RETRY_REQUIRED`;
- typed `LifecycleAction` / `LifecycleActionType`;
- `EvidenceManifest`, including full-text hash/length, attachment/OCR inclusion, omitted/unavailable blocks, and source/provider truncation flags;
- closed seven-class `VerificationReason` set and typed `SourceParseRetryReason`;
- `SourceParseDecision`, exposed through the legacy `ParsedEvents` spelling and preserving list iteration, indexing, length, and `festival` metadata;
- provider-payload adapter accepting the new typed object plus temporary legacy array/single-event shapes;
- empty provider body, malformed JSON after one repair, schema mismatch, provider finish truncation, timeout/rate-limit/provider failure, and verification uncertainty cannot become `CONFIRMED_NO_EVENT`;
- positive children survive incomplete evidence and are marked `enrichment_required`; negative no-event is forbidden unless the manifest and the provider declaration are complete;
- conditional verifier runs zero or one time, only for the seven closed contradictions, and receives full source text, every OCR block, manifest, primary typed result, contradiction facts, current date, publish timestamp, and source context;
- verifier reuses the configured primary provider and either the already configured escalation model or the resolved primary model; no new provider/model or always-on stage was added;
- canonical parser prompt now requires maximum recall, all text/OCR, all siblings/sessions/dates, recap+future and giveaway+event preservation, mixed lifecycle+new-event output, and neutral deterministic hints.

## Changed files

- `source_parse_contract.py` (new)
- `main.py`
- `docs/llm/prompts.md`
- `tests/test_source_parse_contract.py` (new)
- `tests/test_event_parse_llm_first_contract.py` (new)
- `.codex/lanes/SEMANTIC-CONTRACT/RESULTS.md` (this file)

No forbidden file was edited.

## Evidence and commands

Passed:

```text
python3 -m py_compile source_parse_contract.py main.py \
  tests/test_source_parse_contract.py tests/test_event_parse_llm_first_contract.py
# exit 0

git diff --check
# exit 0

/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_source_parse_contract.py \
  tests/test_event_parse_llm_first_contract.py \
  tests/test_prompt_json.py
# 43 passed in 0.70s
```

A broader exploratory run was also made:

```text
/home/dev/.venvs/events-bot-region-talk/bin/python -m pytest -q \
  tests/test_auto_program_url.py tests/test_festival_vk_background.py \
  tests/test_vk_auto_queue_gemma4.py tests/test_vk_default_time.py \
  tests/test_source_parse_contract.py tests/test_event_parse_llm_first_contract.py \
  tests/test_prompt_json.py
# 69 passed, 4 failed
```

Failure classification:

1. `tests/test_vk_auto_queue_gemma4.py::test_event_parse_gemma_default_is_gemma4_without_implicit_4o_fallback` is an expected old-contract assertion: it expects malformed JSON after repair to raise `RuntimeError`; the new required contract returns typed `RETRY_REQUIRED/MALFORMED_JSON`. The test is out of this lane's writable scope and must be inverted during integration.
2. Two festival tests monkeypatch `parse_event_via_4o` while the effective default route is Gemma, causing unintended live-provider attempts and local shared-limiter failure. They are outside this lane and unrelated to the typed contract.
3. One VK default-time test expects `""` while the existing draft builder normalizes it to `None`; no semantic-contract code participates in that conversion.

No live model call was intentionally run; owned tests use deterministic fake providers.

## Integration risks / hooks

- Downstream adapters must branch on `decision.disposition`; they must not interpret an empty list alone. In particular, a retry decision can intentionally retain positive children for later enrichment.
- Existing out-of-scope test expecting `RuntimeError("bad gemma parse response")` must be updated to assert the typed retry outcome.
- VK/raw/caller lanes may pass an `EvidenceManifest` (or mapping) as `evidence_manifest`, and any of the seven enum values through `contradiction_facts`.
- Lifecycle application/target resolution and durable queue state remain downstream responsibilities; this lane only supplies the typed semantic contract and verifier hooks.
- `CHANGELOG.md` was explicitly forbidden for this lane and must be updated by the integration owner.

## Git

- Implementation commit: `076408da1 fix(event-parse): add typed LLM-first source verdict`
- No push performed.
