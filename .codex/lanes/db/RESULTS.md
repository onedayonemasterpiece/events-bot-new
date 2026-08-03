# DB lane results — site launch subscriptions

## Scope

- Requirement: R03, a closed email ledger plus an anonymous-only subscription RPC.
- Target: the separate personalization Supabase/Postgres project selected by `PERSONALIZATION_*` / `PERSONALIZATION_DIRECT_CONNECTION_STRING`; the legacy `SUPABASE_URL` project and Fly SQLite were not used.
- Base: `f44f7fc66ce6f833b7412796de1fb36f53cacec0` (`origin/feature/static-launch-tile-mosaic-20260803`).
- Head: `HEAD` on `agent/static-launch-tile-mosaic-20260803/db` (the commit containing this report).

## Files

- `supabase/migrations/20260803143000_create_site_launch_subscriptions.sql`
- `.codex/lanes/db/RESULTS.md`

## Implemented contract

- `public.site_launch_subscriptions` stores UUID id, normalized unique email, bounded/normalized attribution fields, subscribed/unsubscribed status, saturating positive submission count, and creation/update/last-seen timestamps.
- CHECK constraints enforce the stored normalization, email shape and 254-character limit, source token shape/64-character limit, absolute control-free page path/500-character limit, locale token shape/16-character limit, status domain, counter range, and timestamp ordering.
- RLS is enabled with no browser policies. Direct table privileges are revoked from `PUBLIC`, `anon`, and `authenticated`; explicit operator CRUD remains available to `service_role`.
- `public.subscribe_site_launch_v1(p_email text, p_source text default 'tile-mosaic-launch', p_page_path text default '/lab/launch/tile-mosaic/', p_locale text default 'ru')` is the only browser write surface. It validates and normalizes all input and upserts by normalized email, restoring `subscribed`, refreshing attribution/timestamps, and incrementing `submission_count` without integer overflow.
- The RPC is `SECURITY DEFINER` only because the table is fully closed/RLS-protected. Its search path is locked empty and all referenced relations/catalog functions are schema-qualified. Default/direct execute is revoked from `PUBLIC`, `anon`, `authenticated`, and `service_role`, then granted back only to `anon`.
- The result is deliberately constant and minimal: `(accepted=true, status='subscribed')`. It does not reveal row id, prior existence, count, or stored email.

## Research and commands

- Read the installed `supabase`, `supabase-postgres-best-practices`, and `events-bot-dual-db` skills.
- Read `docs/README.md` and `docs/routes.yml` from the canonical checkout (the lane's sparse worktree excludes `docs/`).
- Fetched and inspected current official sources before implementation:
  - `https://supabase.com/changelog.md`
  - `https://supabase.com/docs/guides/database/postgres/row-level-security.md`
  - `https://supabase.com/docs/guides/database/functions.md`
  - `https://supabase.com/docs/guides/api/securing-your-api.md`
- Relevant current changelog item: 2026-04-28 changed automatic Data/GraphQL exposure defaults; this migration does not rely on defaults and uses explicit grants/revokes.
- Checked `.env` key presence without printing values: all four `PERSONALIZATION_*` keys are set; legacy keys are also present but intentionally unused.
- `supabase` CLI is not installed, so `supabase migration new` and `supabase db advisors` were unavailable. The filename follows the task's prepared canonical migration path/timestamp instead of inventing another name.
- Ran the redacted personalization health helper with a temporary psycopg install under `/tmp`; it connected to the intended project as PostgreSQL 17.6.

## Verification

1. `git diff --check` — passed.
2. PostgreSQL 17 Alpine disposable Docker contract test — passed after fixing one locally detected misuse of schema qualification on SQL's `LEAST` construct:
   - migration applied cleanly;
   - RLS enabled;
   - direct `anon` and `authenticated` table privileges absent and actual anon SELECT denied;
   - only anon had RPC execute; actual authenticated RPC execution denied;
   - mixed-case/space-padded inputs normalized;
   - repeat submission kept one row and incremented count to 2;
   - invalid email/source/page-path/locale each raised SQLSTATE `22023`;
   - safe constant result verified.
3. Intended personalization Supabase/Postgres transactional contract test — passed:
   - verified the table did not preexist;
   - executed the complete migration inside a transaction;
   - verified RLS, `SECURITY DEFINER`, empty `search_path`, ACLs, anonymous call, normalization, repeat upsert/count;
   - explicitly rolled the transaction back and verified `to_regclass('public.site_launch_subscriptions')` returned null afterward.

No production schema or rows were left behind by verification. Applying the committed migration remains an integration/release responsibility.

## Security risks / follow-up

- Anonymous subscription endpoints can be abused to submit many distinct email addresses or inflate a known address's counter. The RPC prevents reads/enumeration and bounds each field/counter, while the site honeypot can reduce basic bots, but there is no trustworthy client identity/IP available inside this RPC for per-client rate limiting. Add edge/WAF rate limiting and abuse monitoring before broad promotion if traffic warrants it.
- Email is PII. Operator access through `service_role` must remain backend-only; never expose the secret key or raw table through the static client.
- PostgreSQL syntax/runtime and live target compatibility were tested, but Supabase DB Advisors could not be run because neither the Supabase CLI nor a callable Supabase MCP advisor was available in this lane.

## Merge notes

- Merge this lane before wiring/deploying the static form so the site can use the exact named RPC arguments and constant result shape above.
- The migration is generic SQL but must be applied only to the personalization project, never the legacy Supabase integration or Fly SQLite.
- No `site/**`, `docs/**`, or `CHANGELOG.md` files were changed in this lane; the owning integration lanes must provide the required canonical documentation and changelog entry.
