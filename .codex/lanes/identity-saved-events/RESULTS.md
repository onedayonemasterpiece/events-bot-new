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

## Requirement matrix

| ID | Status | Evidence |
|---|---|---|
| B1 site-wide Yandex/email/device identity | **Done (code), Partial (production activation)** | Layout-neutral `site/src/lib/site-identity.js`; Yandex readiness probes pass; Node contract covers OTP TTL/cooldown/attempt/replay, remember/forget, reload and account switch. Live email template and new Edge deployment are not changed by this branch. |
| B2 anonymous → auth merge | **Done (code), Partial (production activation)** | 256-bit device proof, consent version, service-only idempotent merge RPC, structural profile/link/save uniqueness, conflict/unlink/purge policy and transactional SQL contract. Production schema is not applied. |
| B3 durable saved occurrence + count/RLS | **Done (code), Partial (production activation)** | Private schemas, idempotent save/undo, separate signal, unique-event count RPC, lifecycle state, RLS/grant assertions and SQL transaction contract. Production schema is not applied. |
| B4 occurrence ICS + D-1/Postbox | **Done (foundation), Partial (canonical producer/activation)** | Occurrence ICS remains static; explicit masked-email consent; quiet hours/catch-up; one D-1 guard; reschedule/cancel/completed transitions; existing Postbox suppression path. Generic messages contain server-owned ids/times; enriched copy still needs a canonical Fly producer. Scheduler/Edge not deployed. |
| B5 storage/retention/growth | **Done** | Live read-only size measurement (36 MB, ~462 MB decimal headroom), conservative growth model, service cleanup RPC and gates in `docs/operations/personalization-storage-budget.md`. |

No release claim is made for unapplied/deactivated production components.

## Tests and evidence

Passed:

```text
node --test tests/node/site_identity_controller.test.mjs
# 7 passed, 0 failed

migration + supabase/tests/site_identity_saved_occurrence_contract.sql
# executed in one transaction against the current personalization Postgres schema;
# sql_contract_transaction=ok; unconditional ROLLBACK

python3 scripts/check_authorized_search_readiness.py \
  --env-file /home/dev/projects/events-bot-new/.env \
  --probe-auth-config --probe-yandex-provider \
  --probe-yandex-userinfo-adapter --strict
# all checks OK

git diff --check
node --check site/src/lib/site-identity.js
```

The SQL contract covers raw-table denial, RPC grants, merge replay/dedup/conflict,
save/repeat/undo/count, separate like, consent evidence/masking, quiet hours,
catch-up, exactly-one D-1 enqueue, Postbox payload schema and cancellation.

Written for integration/CI (not executed locally because this lane intentionally did
not install Node dependencies under the low-space worktree):

- `tests/playwright/site_identity_saved_events_contract.spec.ts` — representative
  `/segodnya/`, `/vystavki/`, event-detail and `/poisk/` page families;
- `tests/e2e/features/static_site_identity_saved_events.feature`.

## Production migration/config status

**Not applied/deployed.** The live personalization database is reachable, but its
migration ledger contains `20260717074903`, which is absent from this checkout.
Applying another production migration before reconciling that drift is unsafe.

Exact activation handoff after reconciliation:

1. register/apply
   `supabase/migrations/20260717170000_site_identity_saved_occurrence_v1.sql` to the
   personalization project and run `supabase/tests/site_identity_saved_occurrence_contract.sql`
   on staging/local;
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
