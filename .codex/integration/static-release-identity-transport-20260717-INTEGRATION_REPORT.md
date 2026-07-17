# Static-site identity + transport integration report

Date: 2026-07-17

Integration branch: `integration/static-release-identity-transport-20260717`

Fresh base: `origin/main@2822a91d6173883fca36ccf135802280ba4ab09d`

Release umbrella source: `origin/docs/static-site-release-plan-20260717@8fecf7da`

## Recovery and accepted commits

Both pre-existing crash worktrees were inventoried and committed before further
edits. Neither lane used reset, clean, deletion or a restart-from-zero.

| Lane | Recovery commit | Pushed worker head | Accepted integration commits | Status |
|---|---|---|---|---|
| Identity / saved occurrences / reminders | `01a4bef5` (after starting committed head `8bc59dc0`) | `bcd1d1184d342d18386e7b0e43a5b27436a10800` | `dab86805`, `53b7edef`, `2fa2734e`, `984b9f67` | **Done as unapplied code foundation** |
| Transport refresh | `206072cf` | `a83704b09037b1059839bb43166ba2ff5d5005e5` | `54d07401`, `06ee3c9a` | **Partial: mechanics foundation accepted; real providers blocked** |

The integrator reviewed the lane diff against `origin/main`. Transport changed no
site UI. Identity changed only layout-independent controller/ICS surfaces; no final
header, `/izbrannoe/`, listing-card, transport-card or event-detail layout was
introduced. During integration the ICS UID was restored to the pre-existing stable
`event-<id>@kenigevents.ru` value while retaining lifecycle/occurrence metadata, so
existing calendar imports are not duplicated.

The recovered full static-site release plan was subsequently merged into this
integration branch. Conflicts were resolved by retaining the complete 233-item
umbrella/208-scenario inventory while updating its identity and transport rows to
the accepted implementation evidence and honest unapplied/blocked status. Open
production-publisher PR `#43` was inspected but not folded in: it is a distinct
four-commit side candidate, 66 commits behind current `main` and conflicting, so it
requires a separate current-main re-port and validation rather than an implicit
merge.

## Requirement closure

### Identity / favorites / reminders

- **Done (code/contracts):** private saved-occurrence schemas; raw-table denial and
  narrow RPC grants; idempotent save/repeat/undo/count; proof-bound device merge;
  separate save, signal, consent and reminder relations; masked verified-email
  evidence; canonical-time-only D-1 authorization; retry/reschedule/cancel/completed,
  quiet-hours and bounded-catch-up contracts; existing Postbox suppression boundary.
- **Done (reconciliation evidence):** missing live ledger migration
  `20260717074903` was recovered exactly, with matching live/file statement SHA-256
  `2b57c2013673eac74b0d391ac3d463c87b83c39e3b1d6a14be1a1f9516ff288b` and
  read-only semantic schema checks.
- **Partial / not applied:** migration `20260717170000`, Edge Function, scheduler and
  mail delivery switches are not deployed or enabled. The generic reminder producer
  still needs server-owned canonical Fly enrichment.

### Transport refresh

- **Done (foundation):** independent KPPK/bus CPU jobs, shared versioned schema,
  validated per-provider attempts and last-known-good state, explicit
  invalid/partial/stale status, immutable combined manifests and safe pointer,
  durable content-hash rebuild intent, unchanged-hash suppression and one coalesced
  follow-up while a build is running.
- **Partial canary:** controlled private Kaggle mechanics runs completed for KPPK
  (`controlled-20260717-kppk-v3`) and bus (`controlled-20260717-bus-v1`). Their
  combined semantic hash was
  `28b103cec10c768ed5c49a962956982cfb159e7248720b3a382bdfda0f4d191a`.
  No publish DB was supplied, so no static build was created.
- **Blocked:** reviewed official KPPK HTML/PDF and bus HTML adapters do not yet
  exist. There is no real-source, production status-ledger/heartbeat/lease, public
  manifest or build evidence. Schedule and production activation remain absent/off.

## Validation

Integration worktree:

```text
node --test tests/node/site_identity_controller.test.mjs
# 8 passed, 0 failed

pytest -q -p no:cacheprovider \
  tests/test_transport_refresh.py tests/test_job_outbox_depends.py
# 18 passed

node --check site/src/lib/site-identity.js
python -m py_compile transport_refresh/*.py <kernels/runners/publisher> main.py
docs/routes.yml parse + referenced integration route targets
stable ICS UID + occurrence metadata source assertions
git diff --check
# all passed
```

Accepted identity worker evidence also includes the final migration plus
`supabase/tests/site_identity_saved_occurrence_contract.sql` in an unconditional
rollback transaction against the current personalization schema, including RLS,
grants and security assertions; the follow-up check found zero new live schemas and
zero new migration-ledger rows. The Gherkin and Playwright contracts were written
but Playwright was not executed locally. Accepted transport worker evidence includes
16 focused tests, the same 18-test adjacent-outbox regression, and a Kaggle-status
suite that printed `19 passed` before hanging at interpreter shutdown and being
terminated; this is not represented as a clean process exit.

## Production truth and activation handoff

**Production applied: no. Production ready: no.**

Identity activation requires, in order: verified backup; staging migration and
transactional SQL/security contracts; Supabase security/performance advisors; Edge
deployment/config review; dry-run reminder scheduler/Postbox correlation; then a
separate delivery approval.

Transport activation requires: implement and review both official-source adapters;
run dated real-source kernels with production-style status callbacks and leases;
review fan-in without a publish DB; run a changed/unchanged publish-DB pair proving
one/zero builds; verify public/static artifacts; then add a default-off schedule and
approve activation separately.
