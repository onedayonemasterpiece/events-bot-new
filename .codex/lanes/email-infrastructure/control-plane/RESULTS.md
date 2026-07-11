# Lane control-plane Results

## Status
committed

## Requirement IDs
- R04: shared email control plane / consent, capacity, suppression and outbox foundation
- R05: Postbox transactional and NotiSend recommendation adapters
- R06: deterministic control-plane/provider contract tests and production-disabled handoff

## Branch
`agent/email-infrastructure/control-plane`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/email-control-plane`

## Base SHA
`fa3f24b9bf9514fc7d3db5bd8a1f3cfbfecf1086` (R03 architecture already integrated)

## Head SHA
`97389588` (implementation commit; the following handoff-only commit adds this results file)

## Files changed
- `.env.example`
- `CHANGELOG.md`
- `docs/operations/email-delivery.md`
- `docs/routes.yml`
- `email_control/__init__.py`
- `email_control/config.py`
- `email_control/eligibility.py`
- `email_control/models.py`
- `email_control/providers/{base,postbox,notisend,router}.py`
- `email_control/tests/test_control_plane.py`
- `supabase/migrations/20260711203940_email_control_plane_v1.sql`
- `supabase/tests/email_control_plane_contract.sql`
- `.codex/lanes/email-infrastructure/control-plane/RESULTS.md` (handoff metadata only)

## Commands run
- Fast-forwarded this lane to `integration/email-infrastructure-release` after R03 merged.
- Used `npx --yes supabase migration new email_control_plane_v1` with Supabase CLI `2.109.1`; the timestamp was CLI-generated.
- Read-only audited the current personalization Supabase schema before implementation; no live mutation was performed.
- Parsed migration and SQL contract with `pglast`.
- Applied the migration and executed the committed SQL contract in a disposable PostgreSQL 17 container with minimal Supabase role/Auth stubs; stopped the container and removed the image afterwards.
- Ran targeted pytest, Ruff, compileall, YAML parsing, secret-pattern scan and `git diff --check`.

## Tests / verification
- PASS: `PYTHONPATH=/tmp/email-cp-test:$PWD python3 -m pytest -q email_control/tests/test_control_plane.py` — `16 passed`.
- PASS: `/tmp/email-cp-ruff/bin/ruff check email_control`.
- PASS: `python3 -m compileall -q email_control`.
- PASS: `pglast` parses both the CLI-generated migration and committed SQL contract.
- PASS: fresh disposable PostgreSQL 17 apply of the migration.
- PASS: committed SQL contract: 200/201 concurrent-cap invariant, no authenticated raw-table SELECT, two-event publish rejection, exactly-three/published-page acceptance, NotiSend-only recommendation claim, untrusted webhook no-op and verified-event suppression.
- PASS: Postbox adapter requires API `MessageId`, uses raw MIME for `Reply-To`, and never calls network in dry-run.
- PASS: NotiSend adapter uses individual `POST /email/messages`, `payment=subscriber`, real returned `id`, and no Postbox fallback on 402/provider failure.
- PASS: DB and process switches default disabled/dry-run-only; no provider API was called and no email was sent.
- PASS: `docs/routes.yml` parses; `git diff --check` clean.
- Not run: full repository pytest suite. The repository-level `tests/conftest.py` imports runtime dependencies absent in this worktree environment (`aiogram`); the new isolated test package was run directly instead.
- Not run: full local Supabase service stack. Its image pull exceeded the host's safe free-space margin, was stopped and cleaned; the migration was instead executed against disposable PostgreSQL 17 with Supabase-compatible role/Auth stubs.

## Risks
- **Apply blocker:** live objects and `supabase_migrations.schema_migrations` are drifted. Do not run blind `supabase db push`; back up, compare live DDL, reconcile/repair history, and prove local/staging reset first.
- The migration is intentionally unapplied. All runtime/provider switches remain disabled and dry-run-only.
- Public NotiSend docs describe no webhook signature. The implementation records webhook bodies only as unauthenticated/unverified signals; suppression/delivery changes require an authenticated message-status lookup.
- Provider credentials, identities, DNS alignment, Postbox configuration set/Data Streams consumer and NotiSend webhook/status reconciler are infrastructure/deployment gates outside this lane.
- The provider adapters are library foundations; no scheduler/container deployment or live worker orchestration was enabled.
- Favorites, canonical event revalidation, personal-page renderer/publisher, cadence/fatigue and retention decisions remain upstream/downstream release dependencies.
- Provider APIs do not document request idempotency. Ambiguous network outcomes must remain `unknown_delivery` with no automatic resend; only definitive rejection may retry.

## Merge notes
- Cherry-pick `97389588` and the following results-only commit together onto the email integration branch.
- Keep migration unapplied during integration.
- Preserve the fixed DB/runtime route: transactional → Postbox; recommendation → NotiSend; no fallback.
