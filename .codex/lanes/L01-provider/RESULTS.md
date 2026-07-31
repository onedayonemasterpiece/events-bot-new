# L01-provider results

## Scope

- Lane: `L01-provider`
- Requirement IDs: strict Interactions REST provider; public external-call lease facade; explicit Antigravity key-env pool with no local/default/overflow fallback; provider/semantic accounting separation; mocked tests.
- Base SHA: `cf76303d97d665ece2df1cc8afa69121c952f26b`
- Implementation head SHA: `d6da76bc` (feature and test commits before this evidence-only commit)
- Live provider calls: **none**.

## Delivered

- Added async `AntigravityInteractionsClient` for `antigravity-preview-05-2026` with `Api-Revision: 2026-05-20`, mandatory `background=true` + `store=true`, strict response/status validation, and a 100,000 maximum per-interaction token budget.
- Added create, GET polling/wait, continuation with both `previous_interaction_id` and the original `environment_id`, deadline cancellation, current `/cancel` path with 404/405 fallback to preview `:cancel`, and safe environment tar download/extraction.
- Added `ExternalCallLease` and public `GoogleAIClient` reserve/mark/finalize/semantic APIs. The reserve path requires Supabase and a complete explicit `key_envs` pool; it never uses a default key, process-local limiter, direct-RPC fallback, or emergency overflow.
- Each interaction-create/continuation POST uses a distinct UUID in the shared ledger and `X-Request-Id`. Polling, cancellation control calls, and downloads do not reserve another interaction RPD. Continuations stay pinned to the environment's original key/project.
- Provider terminal status is persisted separately from `semantic_status`; `completed` remains `provider_completed` until downstream validation explicitly records `passed`.
- Added JSON-safe lease/interaction checkpoints without API-key values.
- Added additive migration `007_google_ai_interaction_accounting.sql`; existing RPCs and migration 006 safety limits (`54 RPM / 96000 TPM / 90 RPD` per key) remain unchanged.
- Environment snapshot extraction rejects traversal, absolute/backslash paths, links, devices, excessive members, and excessive expanded size. Download writes atomically and redirect handling is HTTPS/Google-host restricted without forwarding the API key across hosts.

## Official contract evidence

- Interactions reference: <https://ai.google.dev/api/interactions-api>
- May 2026 revision migration: <https://ai.google.dev/gemini-api/docs/interactions-breaking-changes-may-2026>
- Antigravity agent and continuation contract: <https://ai.google.dev/gemini-api/docs/antigravity-agent>
- Background execution and statuses: <https://ai.google.dev/gemini-api/docs/background-execution>
- Environment snapshot download: <https://ai.google.dev/gemini-api/docs/agent-environment>

## Commands and validation

```text
/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python -m py_compile google_ai/client.py google_ai/interactions.py
# exit 0

/home/dev/projects/events-bot-new-wt-tg-stale-lease/.venv/bin/python -m pytest -q tests/test_google_ai_client.py tests/test_google_ai_interactions.py tests/test_google_ai_antigravity_limits.py::test_antigravity_limit_migration_uses_safe_caps
# 47 passed in 0.34s

git diff --check
# exit 0
```

An attempted full `tests/test_google_ai_antigravity_limits.py` run produced `47 passed, 1 failed`; the sole failure was `FileNotFoundError` for `docs/features/llm-gateway/README.md`, which is deliberately outside this lane's sparse checkout and forbidden writable scope. Its migration-specific test passed separately as shown above.

## Risks / integration notes

- Migration 007 must be applied before real Interactions POSTs; strict finalization intentionally refuses to fall back to the legacy RPC that conflates provider completion with semantic success.
- No live provider or Supabase migration call was made in this lane. Integration must run a bounded live smoke after applying the migration and configuring registered key-env rows/secrets.
- The client allows a provider budget up to 100,000, while migration 006 intentionally keeps a 96,000 TPM safety cap. A 100,000 reservation therefore fails closed; production prompts should use at most the safe ledger allowance unless the limit policy is deliberately revised later.
- Cancel POSTs are control-plane operations and get distinct request IDs but intentionally do not consume model RPD. The provider's current reference path is tried first.

## Changed files

- `google_ai/client.py`
- `google_ai/interactions.py`
- `google_ai/__init__.py`
- `migrations/007_google_ai_interaction_accounting.sql`
- `tests/test_google_ai_client.py`
- `tests/test_google_ai_interactions.py`
- `.codex/lanes/L01-provider/RESULTS.md`
