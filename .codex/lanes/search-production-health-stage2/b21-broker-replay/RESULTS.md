# B21 broker replay/SQL identity — results

## Revisions

- Base SHA: `e23001d210bd91384a0162aaf0d8fffc9eb522a0`
- Implementation SHA: `909ce21739f6860f46aac5383481b8ff6f86d3db`
- Branch: `agent/search-stage2-b21-replay`

## Delivered

- Replaced the five-second effective durable-replay polling horizon with a hard 17-second window: three five-second Supabase operation budgets plus a two-second margin.
- Individual polling requests are capped to the remaining deadline; process-local coalesced wait uses the same bounded window.
- A duplicate claimant only polls the claim RPC and can return the owner's encrypted replay; it never calls `generate_link` or completion.
- Widened the physical claim primary key to the exact v2 identity: repository, workflow, run, attempt, platform, persona.
- Preserved v1 rollout compatibility: the v1 function's explicit run/attempt/persona duplicate guard remains conservative and its insert continues through the browser default.
- Hardened disposable PostgreSQL readiness against the official-image temporary-server restart race.

## Regression evidence

Before implementation:

- Slow cross-process owner regression failed after exactly 20 `duplicate_inflight` polls with HTTP-style 409.
- Ephemeral SQL collision regression failed on the second repository identity with the old `(run_id, run_attempt, persona_id)` primary key.

After implementation:

- `python -m pytest -q tests/test_static_site_auth_session_broker.py tests/test_static_site_auth_session_broker_http.py tests/test_static_site_auth_session_broker_sql.py tests/test_supabase_security_hardening.py` — `48 passed`.
- Slow-owner regression obtains durable replay on poll 21 and proves zero duplicate-side `generate_link` calls.
- PostgreSQL 17 regression executes v1 after migration and proves distinct repository, workflow, and platform dimensions each admit separate typed `new` rows rather than physical uniqueness errors.
- `python -m py_compile` passed for the broker and SQL regression.
- `git diff --check` passed.

## Risks / exclusions

- No production/network/deploy operation was performed.
- The widened primary key takes an exclusive table lock while the migration runs; the table is a bounded broker claim ledger and existing rows are already unique under the former narrower key.
- No workflow, journey, mobile adapter, product Search code, CHANGELOG, or unrelated documentation was modified.

## Changed files

- `serverless/static-site-auth-session-broker/index.py`
- `serverless/static-site-auth-session-broker/README.md`
- `supabase/migrations/20260809143602_static_site_auth_broker_platform_claims.sql`
- `tests/test_static_site_auth_session_broker.py`
- `tests/test_static_site_auth_session_broker_sql.py`
- `tests/test_supabase_security_hardening.py`
