# Lane legacy_runtime_google_gateway Results

## Status
committed

## Requirement IDs
- R05

## Branch
`agent/google-gateway/legacy-runtime-google-gateway`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/legacy-runtime-google-gateway`

## Base SHA
`86a0a8382f0dd9cbb644cd02540bf503e012332c`

## Head SHA
Implementation commit: `2ee16894c4b3988ddf4e378eb8d7f9e8545fce8e`

The lane report is committed separately after that implementation commit, so the
branch tip is the commit containing this file.

## Files changed
- `kaggle/UniversalFestivalParser/src/reason.py`
- `kaggle/UniversalFestivalParser/src/enrich.py`
- `kaggle/UniversalFestivalParser/src/rate_limit.py`
- `kaggle/AfishaThumb/scripts/camera_llm.py`
- `kaggle/AfishaThumb/scripts/poster_llm.py`
- `kaggle/AfishaThumb/scripts/scene_llm.py`
- `kaggle/AfishaThumb/scripts/tour_llm.py`
- `tests/test_legacy_runtime_google_gateway.py`
- `.codex/lanes/legacy_runtime_google_gateway/RESULTS.md`

## Implementation evidence
- Removed direct `google.generativeai`, `google.genai`, raw `.env` key loading,
  and provider client construction from all seven owned runtime files.
- All calls now use `GoogleAIClient` and the exact R04 helper API:
  `build_google_ai_limiter_supabase_client(require_configured=True)`.
- No product-data or legacy Supabase fallback factory is supplied.
- Ambient process-local/direct-key escape flags are forcibly disabled for these
  consumers.
- Gateway internal retries are fixed at one and model fallback is disabled, so
  the existing AfishaThumb feature-level maximum of three physical provider
  attempts is not multiplied. Universal Festival Parser calls remain one
  physical provider attempt per feature call.
- Poster vision input is preserved as shared-client multimodal `inline_data`.
- The legacy explicit Festival Parser key parameter remains usable only as the
  selected `GOOGLE_API_KEY` lane's secret after a shared reservation; it cannot
  satisfy a different reserved lane.

## Commands run
- `git worktree add -b agent/google-gateway/legacy-runtime-google-gateway ... 86a0a838`
- `python3 -m py_compile <owned runtime files> tests/test_legacy_runtime_google_gateway.py`
- `rg` static audit for direct Google SDK/client/endpoints across all owned files
- `git diff --check`
- `PYTHONDONTWRITEBYTECODE=1 /home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -p no:cacheprovider -q tests/test_legacy_runtime_google_gateway.py tests/test_universal_festival_parser_utils.py tests/test_festival_context.py`
- `git commit -m "fix(llm): gate legacy Kaggle Google runtimes"`

## Tests / verification
- Focused/offline suite: **14 passed in 2.63s**.
- Static bypass audit: **PASS**, no direct provider SDK or endpoint references in
  the seven owned runtime files.
- Python compile check: **PASS**.
- `git diff --check`: **PASS**.
- No Google/provider, network, Supabase, or key-bearing calls were made.
- Initial test runner discovery found `/usr/bin/python3` without pytest and the
  Region Talk venv without `pytest_asyncio`; the image-geometry venv provided the
  required offline test tooling.
- One earlier combined pytest invocation hit a pytest-cache finalization
  `OSError: [Errno 28] No space left on device`. Re-running with bytecode and
  pytest cache disabled completed the full focused set successfully.

## Incident regression contract
Incident: `INC-2026-07-31-google-ai-parallel-limiter-bypass`.

Completed lane-scoped mandatory checks:
- no direct Google provider bypass remains in the owned legacy runtimes;
- dedicated limiter configuration is mandatory;
- missing helper/config/RPC fails closed before a provider call;
- physical attempt caps are preserved and tested;
- targeted tests and static bypass audit passed.

Release evidence is intentionally not claimed: no push or deploy was permitted,
and the broader incident remains open pending migration 008, Edge/direct-path
closure, project-scope inventory, deployment from `origin/main`, and bounded
post-deploy ledger/provider reconciliation.

## Risks
- Base `86a0a838` does not yet contain `google_ai.limiter_supabase`; integration
  must land R04 before or with this commit. Until then, calls fail closed and
  existing feature fallbacks are used; they cannot reach Google directly.
- Kaggle packaging/cutover must include `google_ai` and the R04 helper alongside
  these scripts. This lane could not edit notebook packaging or launchers.
- Canonical docs and `CHANGELOG.md` updates are integrator-owned/forbidden in
  R05 and remain required before overall delivery.
- The legacy local `GemmaRateLimiter` remains only a conservative feature budget;
  shared provider admission is exclusively owned by `GoogleAIClient`.

## Merge notes
- Dependency: merge/cherry-pick the R04 dedicated Supabase helper before or with
  implementation commit `2ee16894c4b3988ddf4e378eb8d7f9e8545fce8e`.
- No external packages, secrets, migrations, or environment defaults were added.
- No push was performed.
