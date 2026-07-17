# Lane B — identity, saved occurrences and reminders

Date: 2026-07-17
Branch: `agent/static-release/identity-saved-events`
Base: `d169004376c309dc487fa6b48a7aae4a8ed7dea3`

## Recovery checkpoint

Before continuing this delegated lane, all pre-existing work was inventoried and
preserved without reset/clean/delete. Starting `HEAD` was
`8bc59dc022eb3b8c4c1e722d41ae869342bb6f81` (merge-base/original base
`d169004376c309dc487fa6b48a7aae4a8ed7dea3`). The checkpoint includes the eight
modified foundation/docs/SQL files plus the four untracked lane report, storage
budget, Gherkin and Playwright contract paths shown by `git status`.
Recovery commit: `01a4bef5`. The crash lane was intentionally not rebased after
`origin/main` advanced; integration owns cherry-picking its accepted commits onto
the fresh main-based integration branch.

## Requirement matrix

| ID | Status | Evidence |
|---|---|---|
| ID-1 saved-occurrence schema | **Done (code), Partial (production)** | Private `site_identity`/`saved_events` schemas model account/device identity, durable occurrence saves, separate signals, consent evidence, reminder subscriptions/deliveries and bounded audit/retention state. Production migration is not applied. |
| ID-2 RLS/grants | **Done (code), Partial (production)** | RLS is enabled on every private table; browser roles have no schema/table access; every `SECURITY DEFINER` function has PUBLIC execution revoked and a fixed empty `search_path`; only four owner RPCs are granted to `authenticated`, while service mutations are `service_role`-only. Supabase anonymous Auth tokens fail closed despite using the authenticated DB role. |
| ID-3 idempotent RPCs/merge | **Done (code), Partial (production)** | Save/repeat/undo/count contracts pass. Device materialization requires a random 256-bit proof; merge replay is bound to device/user/consent, cross-device duplicates collapse structurally, authenticated explicit state wins, and credential replacement/account conflict fail closed. |
| ID-4 separate semantics | **Done (code), Partial (final UI)** | Favorite/save, like/not-interested, transactional purpose consent, reminder consent and reminder delivery are separate relations and RPCs. This lane intentionally does not implement final cards/header/detail composition. |
| ID-5 explicit D-1 consent/masked verified email | **Done (code), Partial (production)** | An idempotent consent request requires the synchronized verified `email_control` identity plus matching active transactional purpose consent. Only a masked address snapshot and verification timestamp are stored in the reminder row; plaintext remains in `email_control`. |
| ID-6 exactly-once D-1/lifecycle/Postbox | **Done (foundation), Partial (activation/enriched producer)** | Six-hour catch-up bound, 22:00–08:00 Kaliningrad quiet hours, stale expiry, retry dedupe, pending-reminder replacement after reschedule, no repeat after delivery, cancellation/completed stop, and suppression isolation pass transactionally. Existing Postbox consent/suppression/bounce/complaint/unsubscribe/ambiguous-delivery controls remain authoritative. Generic copy still needs the server-owned canonical Fly enrichment producer; scheduler and delivery switches stay off. |
| ID-7 migration-ledger reconciliation | **Done (read-only/local recovery), Partial (release gates)** | Live ledger row `20260717074903|event_search_age_fields|1` was recovered exactly as `supabase/migrations/20260717074903_event_search_age_fields.sql` from `b01b02ae`; file/live statement SHA-256 is `2b57c2013673eac74b0d391ac3d463c87b83c39e3b1d6a14be1a1f9516ff288b`. Live columns/default/constraints match. New identity migration remains unapplied pending backup, staging and advisors. |
| ID-8 contracts/no final layout | **Done** | Node and rollback SQL/security contracts pass; Playwright/Gherkin contracts cover representative families without implementing/asserting the final header, `/izbrannoe/`, listing cards or event-detail layout. |

No release claim is made for unapplied/deactivated production components.

## Tests and evidence

Passed:

```text
node --test tests/node/site_identity_controller.test.mjs
# 8 passed, 0 failed

migration + supabase/tests/site_identity_saved_occurrence_contract.sql
# executed in one transaction against the current personalization Postgres schema;
# sql_contract_transaction=ok; unconditional ROLLBACK
# follow-up read-only check: live_identity_saved_schemas=0,
# live_identity_migration_ledger_rows=0

python3 scripts/check_authorized_search_readiness.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --probe-auth-config --probe-yandex-provider \
  --probe-yandex-userinfo-adapter --strict
# all checks OK

git diff --check
node --check site/src/lib/site-identity.js
```

The SQL contract covers RLS on every private table; raw-table and PUBLIC-function
denial; authenticated/service RPC grants; supporting FK indexes; anonymous Auth
rejection; merge replay/dedup/credential/account conflict; save/repeat/undo/count;
separate like; idempotent consent and stored masked verified-address evidence;
quiet hours; six-hour producer/dispatcher catch-up bounds; retry dedupe; suppression;
pending and delivered reschedule cases; cancellation; and completed-event no-send.

Written for integration/CI (not executed locally because this lane intentionally did
not install Node dependencies under the low-space worktree):

- `tests/playwright/site_identity_saved_events_contract.spec.ts` — representative
  `/segodnya/`, `/vystavki/`, event-detail and `/poisk/` page families;
- `tests/e2e/features/static_site_identity_saved_events.feature`.

## Production migration/config status

**Not applied/deployed.** Read-only reconciliation found exactly one live ledger row
in the `20260716..20260718` window:
`20260717074903|event_search_age_fields|1`. The exact 48-line migration was recovered
from historical commit `b01b02ae`, which is reachable from
`origin/integration/static-event-v10-system-routing` and related side refs but not
from `origin/main`. Its one live statement has the same SHA-256 as the recovered
file; read-only schema checks confirmed both age columns, the `unknown` default and
the two accepted-value constraints. This closes the missing-file diagnosis, not the
production release gates. `20260717170000` has zero live ledger rows and both new
schemas remain absent after the rollback contract.

Exact activation handoff:

1. create and verify a current backup, then run
   `supabase/migrations/20260717170000_site_identity_saved_occurrence_v1.sql` to the
   staging personalization project and rerun
   `supabase/tests/site_identity_saved_occurrence_contract.sql` transactionally;
2. run Supabase security/performance advisors and confirm no new RLS/function warnings;
3. deploy `supabase/functions/identity-control` with `--no-verify-jwt`; its code
   manually validates bearer tokens and uses only personalization env keys;
4. configure the Supabase Auth email template with both `{{ .Token }}` and
   `{{ .ConfirmationURL }}`, OTP expiry 900 seconds, resend/rate limits, and keep the
   already-probed `custom:yandex` provider/redirect allow-list;
5. schedule service-role `personalization_enqueue_due_reminders_v1` minutely and
   `personalization_retention_cleanup_v1` daily, initially `p_dry_run=true`;
6. keep existing email switches disabled/dry-run, verify dry-run outbox and Postbox
   feedback correlation, then separately approve enabling transactional delivery;
7. inject only publishable personalization URL/key and identity-control URL into the
   static build; never expose the secret key.

The Edge must receive only `PERSONALIZATION_SUPABASE_URL`,
`PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY` and
`PERSONALIZATION_SUPABASE_SECRET_KEY`; generic legacy Supabase variables are not a
fallback. Add an external rate limit for the unauthenticated device-materialization
action before activation.

## Rollback

Before user data: undeploy/disable the Edge and scheduler, revoke RPC EXECUTE, then
reviewed down migration may drop `saved_events`, `site_identity` and the new public
functions. After user data: first disable producers, export/preserve state, and run a
reviewed data-preserving down migration. Static pages and occurrence ICS continue to
work while Supabase is unavailable or disabled.

## Integrator notes

- Add `docs/operations/personalization-storage-budget.md` to `docs/routes.yml`.
- Add the lane behavior summary to `[Unreleased]` in integration-owned `CHANGELOG.md`.
- This lane intentionally does not touch final header/cards, `/izbrannoe/`, listing
  layouts or final event-detail composition.
