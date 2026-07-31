# R02 — project_scope_atomic_gate results

## Scope

- Incident regression contract: `INC-2026-07-31-google-ai-parallel-limiter-bypass`
- Base SHA: `86a0a8382f0dd9cbb644cd02540bf503e012332c`
- Implementation head SHA: `d84100d03df27924c2c85d55fe68072900b78250`
- Branch: `codex/project-scope-atomic-gate-20260731`
- Worktree: `/tmp/events-bot-project-scope-atomic-gate`

## Result

- Added `quota_scope` as explicit Google Cloud project metadata and persisted
  the admitted scope on request/attempt audit rows.
- Replaced key/model-only reserve locking with project-scope/model advisory
  locking and RPM/TPM/RPD aggregation across every sibling key, including
  counters belonging to inactive/rotated keys.
- Added versioned contract `google_ai_project_model_atomic_v1` to every reserve
  result plus `google_ai_limiter_capabilities()`.
- Made `GoogleAIClient` reject successful reservations with a missing/wrong
  contract or missing `quota_scope`; usable key metadata is stripped on this
  failure path. The direct REST fallback uses the same parser/gate.
- Preserved strict external/Antigravity leases: their reservations use the same
  gate and raise `ReservationError` instead of using an unversioned ledger.
- Corrected both Gemma 4 model lanes from the obsolete unlimited-TPM/1.5K-RPD
  assumption to conservative `15 RPM / 15,000 TPM / 14,000 RPD` values based on
  the 2026-07-31 provider evidence supplied to the incident.
- Added a self-contained, secret-free Supabase bootstrap migration for the
  accessible personalization project: tables, model seeds, atomic reserve,
  mark/finalize/sweep, strict interaction accounting, capability RPC, RLS, and
  service-role-only grants.

## Changed files

- `google_ai/client.py`
- `tests/test_google_ai_client.py`
- `migrations/009_google_ai_project_scope_atomic_reserve.sql`
- `supabase/migrations/20260731170000_google_ai_canonical_limiter_bootstrap.sql`
- `.codex/lanes/project_scope_atomic_gate/RESULTS.md`

## Commands and evidence

- `command -v supabase` / CLI preflight: Supabase CLI unavailable, so migration
  filenames were created manually following the repository's existing naming
  conventions. No Supabase/provider/network call was made.
- `/usr/bin/python3 -m py_compile google_ai/client.py tests/test_google_ai_client.py`
  — passed.
- `/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q tests/test_google_ai_client.py`
  — `42 passed in 0.40s` on the committed implementation.
- `git diff --check` and `git diff --cached --check` — passed.
- Static SQL checks verified balanced dollar quotes, one capability/reserve
  definition per migration, expected function/grant inventory, and no secret
  patterns.
- `git show --check --oneline --stat d84100d0` — passed.

## Incident mandatory-check status

- Shared client fail-closed: **passed** by existing and new targeted tests.
- Atomic project/model migration prepared: **passed locally/static only**;
  application/capability evidence remains a release blocker.
- Project quota-scope enforcement: **implemented**; the redacted key-to-project
  inventory is not available in this lane and must be populated before cutover.
- Direct-provider path inventory/routing, static repo-wide bypass audit,
  post-deploy reconciliation, deployment, and `origin/main` ancestry: **outside
  R02 / not performed**.

## Cutover requirements and risks

1. Apply the bootstrap to the accessible personalization Supabase project, then
   verify `google_ai_limiter_capabilities()` returns the exact required contract
   before any provider traffic.
2. Populate only key metadata (`env_var_name`, alias, priority, account label,
   stable redacted `quota_scope`); never store key values. Keys from one Cloud
   project must share a scope. The conservative default groups unknown keys and
   may over-throttle until the inventory is complete.
3. Drain/finalize old-ledger leases, especially Antigravity interactions. An
   external lease must finalize against the ledger where it was reserved.
4. Switch all concurrent Fly/Kaggle/Edge/local clients together. Split-ledger
   traffic can still oversubscribe the same provider project.
5. Keep direct Google callers disabled until they use the canonical
   reserve/mark/finalize path.
6. No live PostgreSQL parser/application test was possible without making the
   prohibited external Supabase call; migration application and a
   provider-free transactional reserve smoke remain required release evidence.

No push or deployment was performed.
