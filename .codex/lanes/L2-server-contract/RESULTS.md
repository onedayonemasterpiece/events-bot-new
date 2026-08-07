# L2-server-contract results

## Scope

- Lane: `L2-server-contract`
- Requirement: `R05`
- Base SHA: `ec09c011674eecddf9e9b8e154e3d102f9384b12`
- Implementation SHA: `85b0bccaf`
- Branch: `agent/search-live-automation/server-contract`

## Delivered

- Added closed Search execution-mode contract:
  `cached_vector`, `cold_vector`, `cold_vector_llm`, and
  `degraded_vector_fallback`.
- Explicit execution modes require a database-verified canary principal from
  `auth.users.raw_app_meta_data` or the service-managed allowlist; no
  `raw_user_meta_data` / `user_metadata` authorization is used.
- Added revision-bound result-cache keys and response/receipt fields for
  contract, catalog, corpus, search-document, embedding-policy, LLM-policy,
  and cache-policy versions.
- Added exact embedding-provider, LLM-provider, vector-RPC, result-cache, and
  query-embedding-cache counters. A result-cache hit has zero provider/vector
  attempts.
- Added fresh terminal owner receipts, including sanitized response event IDs,
  for cache hits, successful cold paths, quota rejection, and provider errors.
- Added owner-scoped `get_event_search_receipt_v1(uuid)` while receipt tables
  and internal mutation/revision RPCs remain service-role only.
- Added an atomic, idempotent server-enforced daily canary LLM-attempt budget.
  Each canary provider attempt reserves budget before provider dispatch.
- Vector-only modes cannot enter the LLM verifier. Deterministic verifier
  failure performs zero LLM provider sends and preserves the vector result set.
- Added a separate bounded `search_canary` orchestration quota plan; LLM cost is
  still governed by the stricter daily attempt ledger.
- Added the broker-compatible, PII-free service-only issuance claim RPC
  requested by L1:
  `claim_static_site_auth_session_issue_v1(text,integer,text,text,text,integer)`.

## Security evidence

- All six new `SECURITY DEFINER` functions set
  `search_path = pg_catalog`.
- Internal persona, revision, budget, receipt-mutation, and broker-claim RPCs
  revoke `PUBLIC`, `anon`, and `authenticated`, then grant only `service_role`.
- The only authenticated receipt API filters by
  `r.user_id = (select auth.uid())` and returns no query, token, session, email,
  provider-key name, or raw prompt.
- Every new public table has RLS enabled and no browser table grants.

## Validation

Commands and outcomes:

- `node --test supabase/functions/event-search/*.test.mjs`
  - PASS: 24/24 tests.
- Deno 2.2.7:
  - `deno fmt --check supabase/functions/event-search/index.ts` — PASS.
  - `deno check supabase/functions/event-search/index.ts` — PASS.
- `tests/test_supabase_security_hardening.py` loaded directly and all `test_*`
  functions executed because the repository-wide pytest conftest requires
  unavailable unrelated `aiogram` dependencies:
  - PASS: 8/8 focused security assertions.
- PostgreSQL 17 syntax validation with `pglast`:
  - migration and SQL contract top-level parse — PASS;
  - 4 PL/pgSQL function bodies — PASS;
  - 5 SQL-contract `DO` bodies — PASS.
- PostgreSQL 17-alpine executable contract test:
  - created minimal Supabase-compatible auth/role/search prerequisites;
  - applied `20260807231642_event_search_canary_receipts.sql` with
    `ON_ERROR_STOP=1` — PASS;
  - ran `supabase/tests/event_search_canary_receipts_contract.sql` with
    `ON_ERROR_STOP=1` — PASS and transaction rolled back.
- `git diff --check` / cached diff check — PASS.

## Risks / integration notes

- The executable SQL contract used PostgreSQL 17 with minimal prerequisite
  schemas instead of the full local Supabase stack. This still exercised the
  migration, grants, RLS metadata, functions, owner isolation, atomic budget,
  and broker claim behavior; integration should rerun `supabase test db` after
  all lanes are merged.
- L4 publishes identical `catalog_revision`, `corpus_revision`, and
  `search_document_revision` values into document/embedding metadata. The
  revision RPC prefers those authoritative values and has a deterministic hash
  fallback for pre-publication data.
- No docs, site, workflow, or changelog files were edited in this lane, per the
  ownership map.

## Changed files

- `supabase/functions/event-search/index.ts`
- `supabase/functions/event-search/canary-contract.test.mjs`
- `supabase/migrations/20260807231642_event_search_canary_receipts.sql`
- `supabase/tests/event_search_canary_receipts_contract.sql`
- `tests/test_supabase_security_hardening.py`
- `.codex/lanes/L2-server-contract/RESULTS.md`
