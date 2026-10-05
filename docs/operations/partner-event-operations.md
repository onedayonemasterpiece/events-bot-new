# Partner event operations

Status as of 2026-10-05: the event/promo partner core is
**DEPLOYED_ACCEPTED** on RunCoveer from canonical main
`e679853bc328aece113163e30667cfd4bbeab70a`. Owner event
create/assets/typed operations plus partner access/event-create and owner/partner
promo gates are enabled. No real partner grant or public provider test
publication was performed during acceptance.

There is one canonical EventsBot database, Smart Update, JobOutbox and promo
engine. The partner OAuth projection does not create a second backend, scheduler,
parser, publication queue or billing model. Telegram registration is not an
identity prerequisite.

## Readiness levels

- **SOURCE_READY — PASS for the event/promo core.** Source, additive SQLite
  initialization, partner OAuth/policy, event/media/review, typed lifecycle,
  promo management/execution readback, restart/recovery and current-main
  compatibility are locally verified.
- **ISOLATED_LIVE_VERIFIED — NOT RUN.** Real provider work must be limited to
  explicitly approved private test destinations after an independent release
  review.
- **DEPLOYED_ACCEPTED — PASS.** `runcoveer/events:e679853bc` is live from
  canonical main. SQLite `quick_check`, additive schema, public health,
  Telegram webhook origin, owner/Codex/partner OAuth metadata and
  unauthenticated rejection passed after staged gate activation. Kotopogoda was
  not recreated and the previous Events image remains the rollback.
- The full v2 TO-BE remains wider than this core. Lifecycle visual badges,
  public old→new history/site lifecycle UX and the isolated-live scenarios stay
  external gates and must not be reported as completed by this source release.

## Access and onboarding

An owner calls `partner_create` with explicit tenant/organization, policy,
portfolio, callback URIs and credential expiry. The returned one-time
`login_secret` is delivered privately. The partner uses authorization-code +
PKCE S256 against the separate `events-partner` resource.

Every request and every mutation boundary rechecks the current server-side
principal/client/audience, grant status, credential epoch, tenant,
organization, policy revision and explicit portfolio. `User.is_partner` and a
Telegram account are not security boundaries.

Owner access lifecycle:

- `partner_get` reads the current server-side binding;
- `partner_access_change` supports suspend, resume, credential rotation,
  policy/portfolio changes and irreversible revoke;
- a grant or portfolio change after prepare invalidates commit/review when the
  frozen policy no longer matches;
- old access/refresh credentials do not survive rotation or revoke.

Several principals may belong to one organization, but every principal still
has an explicit portfolio. Rich organization roles such as
`programme_manager` or `analyst_readonly` remain later roadmap work; this
release uses scopes/actions/portfolio.

## Event workflow

1. Read `partner_workspace_get` and `partner_events_list`. Exact foreign and
   nonexistent event IDs are intentionally indistinguishable.
2. Stage a poster with `event_asset_stage`; the private asset reference is
   principal/digest bound and expiring. `event_asset_get` reads only bounded
   metadata.
3. Call `event_create_prepare` then `event_create_commit`. Smart Update is
   the only canonical create/merge authority. `RETRY`, `FAILED` and a
   diagnostic event ID never authorize publication or promo.
4. When owner review is required, use the owner review tools; an accepted Smart
   Update result yields the canonical event ID and current partner portfolio
   binding.
5. Existing events use exact-ID `event_edit_*`,
   `event_reschedule_*`, `event_postpone_*` and `event_cancel_*`.
   Generic edit cannot change schedule/location/lifecycle. Public text,
   including an optional public lifecycle explanation, passes the same
   fact-consistency gate. The organizer comment remains internal.
6. Read `event_operation_get` and `event_publication_status`. Canonical
   acceptance, queued/running transport, provider publication and current
   applied revision are separate states.

Create and mutation prepare/commit are idempotent. Lost completion after the
canonical boundary is recovered from durable receipts; recovery does not rerun
Smart Update blindly and does not create a second Event or duplicate fan-out.

## Media

Event images use the existing Event Media gate. Partner staging never writes
`Event.photo_urls` directly. Smart Update/materialization/review owns the
approved projection and production CDN invariant. Edit supports add/replace/
remove/reorder through the same approved `EventPoster` projection.

## Promo workflow

Promo is a separate operation after an accepted canonical event ID. Partner-safe
MCP surfaces are only surfaces with existing executable product paths:

- `video_general` through the existing video selection/public exposure path;
- `vk_repost` through the existing `run_promo_vk_activities` runner.

The site placeholder and other installed editorial surfaces are not partner MCP
capabilities.

The partner/owner tools provide:

- `promo_capabilities`, bounded list/get and durable operation readback;
- campaign create/request;
- add activity;
- campaign update;
- pause, resume and archive;
- owner review where current policy does not auto-approve.

Campaign update is revision-CAS protected. Partners may change the title,
campaign end date, total exposure goal and daily cap within server policy. The
end date is clamped to the actual event end. Partner priority changes are
denied; owner priority remains allowed. Archived campaigns cannot be silently
reactivated by a historical replay.

`promo_campaign_get` separates three kinds of evidence:

- current server-state eligibility/cap/window reasons;
- the latest durable per-activity execution outcome from
  `promo_activity_outcome`;
- recent `promo_exposure` publication-accounting rows.

Provider exception text is not returned to partners. Unknown failures collapse
to the closed `provider_error` reason. The readback reports
`recorded_publication`, `non_delivery_recorded` or `not_observed`, while
`publication_state` remains `not_observed` unless independently verified.
Recorded exposure units are not browser impressions.

The scheduler also sanitizes logged promo reasons. Telemetry persistence is
best-effort after provider work: a telemetry write failure cannot turn a
successful public provider mutation into a failed publication.

## Static-site readback

`event_publication_status` includes the existing static-site JobOutbox surface.
It uses the real `event_revisions`, `build_receipt.publication` and
`build_receipt.root_promotion` contracts to distinguish:

- queued/running/failed candidate work;
- a successfully built or secret-published candidate;
- stable-root promotion;
- a stale root whose applied event revision is older than the canonical event.

The private secret-candidate bearer URL is never returned by partner readback.

## Tools and feature gates

| Tools | Owner | Partner | Runtime gate |
|---|---|---|---|
| partner_create/get/access_change | partners:manage | — | `PRIVATE_EVENTS_MCP_PARTNER_ENABLED` |
| partner_workspace_get/events_list | — | partner:events:read | partner gate |
| event_asset_stage/get | events:write | partner:events:propose | `PRIVATE_EVENTS_MCP_EVENT_ASSETS_ENABLED` |
| event_create_prepare/commit | events:write | partner:events:propose | `PRIVATE_EVENTS_MCP_EVENT_CREATE_ENABLED` + partner create gate |
| event_operation_get | operations:read | partner:events:read | create/operations |
| event_edit/reschedule/postpone/cancel prepare/commit | events:write | partner:events:propose | `PRIVATE_EVENTS_MCP_EVENT_OPERATIONS_ENABLED` |
| partner/event operation review tools | partners:manage | — | matching event gate |
| event_publication_status | operations:read | partner:publications:read | event operations |
| promo capabilities/list/get/operation_get | promo:read | partner:promo:read | owner/partner promo gate |
| promo create/activity/update/state prepare/commit | promo:write | partner:promo:request | owner/partner promo gate |
| promo review/decide | partners:manage | — | promo gate |

Partner creation additionally requires
`PRIVATE_EVENTS_MCP_PARTNER_EVENT_CREATE_ENABLED`.
Promo gates are `PRIVATE_EVENTS_MCP_OWNER_PROMO_ENABLED` and
`PRIVATE_EVENTS_MCP_PARTNER_PROMO_ENABLED`. All these gates default off.
Codex retains its read-only seven-tool projection and Social Workspace remains a
separate capability family.

## Review, recovery and publication safety

Preparations freeze actor, provenance, normalized proposal, target revisions,
policy revision, digest and expiry. Owner review cannot edit a frozen payload;
a changed proposal requires a new preparation.

Canonical change/history and the domain receipt commit atomically in a short
SQLite transaction. LLM/network/provider work is outside that transaction.
Fan-out failure after canonical acceptance leaves a recoverable
`reconciliation_pending` state.

Revision-bound pending jobs cannot publish stale event facts. For
rollback-compatible staged rollout, an obsolete job may remain in the existing
enum with `terminal_reason=superseded`; readback exposes the terminal reason
instead of pretending it is a current success. An uncertain provider outcome
uses `outcome_unknown` and is never blindly retried.

Separate lifecycle notices are planned only from authoritative
`first_published_at` evidence:

- strictly more than 24 hours: a separate notice may be planned;
- exactly/less than 24 hours: reconcile the original only;
- unknown age: owner review, no automatic separate notice;
- never published: no notice.

`Event.added_at`, a stored URL or a queue state is never used to invent
publication age. Derived visual badges and public old→new site history remain a
separate release scope and stay off until their prerequisites are accepted.

## Staged release packages

Deploying source and enabling behavior are separate.

1. **R0 — observability.** Keep the existing owner queue snapshot/job fetch
   contract.
2. **R1 — owner create.** Run `Database.init()`; enable only owner canonical
   create and verify accepted-ID/recovery readback.
3. **R1b — assets/media.** Enable event asset ingress and verify Event Media
   projection before partner image intake.
4. **R2 — typed event operations.** Enable edit/reschedule/postpone/cancel and
   revision-bound publication readback; keep partner access off.
5. **R3 — lifecycle/publication prerequisites.** Verify authoritative
   publication receipts and current-revision reconciliation. Do not enable
   separate notices/visual badges/public history merely because code is
   deployed.
6. **R4 — partner product.** Enable partner resource/access, then partner event
   create, then owner promo, then partner promo. Start with two isolated test
   tenants before any real partner.

Every step is reversible by disabling its feature gate. SQLite rollback is
code-first and non-destructive: additive tables/columns stay in place for
forward compatibility.

Production completed R0 through the R4 feature-gate activation on 2026-10-05
without creating partner credentials or issuing a provider mutation.
`ISOLATED_LIVE_VERIFIED` therefore remains deliberately separate: it requires
an existing explicitly private test tenant/destination rather than a public
acceptance post.

## Source acceptance evidence

The source integration baseline was
`b8c9e55ee5553d7df84afa927c63075df9b7dafa`; PR #725 merged to canonical
release main `e679853bc328aece113163e30667cfd4bbeab70a`.

Local Python 3.12 regression on 2026-10-05:

- final combined release regression on the current source: **505 passed**;
- Partner MCP/event/lifecycle/promo/security suite: **342 passed**;
- existing promo engine suite: **91 passed**;
- Telegram partner promo/menu compatibility suite: **34 passed**;
- focused carousel compatibility after telemetry integration: **2 passed**;
- changed modules compile with `py_compile`.

Additive SQLite compatibility was exercised as:

1. current source `Database.init()` twice;
2. create a campaign/activity and `promo_activity_outcome`;
3. exact old binary from current `origin/main` runs its own
   `Database.init()` on the same DB;
4. campaign/activity/outcome remain intact and `PRAGMA quick_check=ok`;
5. the temporary old-binary checkout is removed after the smoke.

The executable local end-to-end protocol smoke is:

```bash
python -m pytest -q \
  tests/test_private_events_mcp_partner_commands_http.py \
  tests/test_private_events_mcp_application_commands.py \
  tests/test_private_events_mcp_event_create_recovery.py
```

It exercises real local OAuth/PKCE → MCP → policy → application service →
SQLite/queue/readback boundaries with provider calls isolated.

## Not part of this source-ready claim

- production deployment or activation;
- real partner credentials/data;
- public-provider live acceptance;
- lifecycle visual badge renderer acceptance;
- public old→new static-site history/lifecycle banner acceptance;
- registrations, NFC/QR check-in and NPS.

Those remain explicit later gates/roadmap and are not hidden blockers for the
current event/promo core.

See also [the provider-neutral application boundary](partner-events-conversational-function-call.md),
the full [v2 scenario registry](../testing/private-events-mcp-event-operations-scenarios.v2.yml),
and the short [Codex release handoff](partner-event-operations-release-handoff.md).
