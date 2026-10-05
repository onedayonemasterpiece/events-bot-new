# Partner event operations

Source implementation is default-off; deployment and activation are separate.
There is one canonical EventsBot database, Smart Update, JobOutbox and promo
engine. No Telegram account is required for a partner.

## Access and workflow

An owner calls `partner_create` with explicit tenant/organization, policy,
portfolio, callback URIs and credential expiry. Deliver `login_secret` privately
once. Partners use authorization-code/S256 PKCE against the separate
`events-partner` resource. Current grant/status/credential epoch are checked on
requests and again at mutation boundaries. Several principals may belong to one
organization but each has an explicit portfolio. Membership roles are a future
layer (`partner_owner`, `programme_manager`, `event_editor`, `checkin_operator`,
`analyst_readonly`); this release uses scopes/actions/portfolio.

1. Read `partner_workspace_get` and search `partner_events_list`; exact
   `event_id` reads one own event. Foreign and nonexistent IDs are indistinguishable.
2. Stage an image using `event_asset_stage`; the ref is private, actor/digest
   bound and expiring. Staging does not publish. `event_asset_get` reads metadata.
3. Prepare/commit create. Commit supplies the same frozen create arguments,
   preparation ref, digest and (partner) policy revision. Owner review may queue
   it. Poll `event_operation_get` for accepted canonical ID.
4. Use exact-ID edit or typed reschedule/postpone/cancel prepare/commit.
   Generic edit supports description and media add/replace/remove/reorder, not
   schedule or lifecycle. Text policies are smart_rewrite (default)/preserve_original/
   replace_exact; all pass the consistency gate. Partner lifecycle requires review.
5. Read `event_publication_status(event_id)`. Transport, target revision and
   applied revision are distinct. `event_publications_get` remains compatible
   with existing accepted-create operation references.
6. Prepare separate promo with current event revision. Partner-safe surfaces
   are those accepted by the existing campaign service (video profiles and VK
   repost); default Telegram button highlighting remains engine-owned. Campaign
   state actions are pause/resume/archive. Read current campaign and recent
   normalized recorded exposures separately from historical operation receipts.

## Tools and feature gates

| Tools | Owner | Partner | Gate |
|---|---|---|---|
| partner_create/get/access_change | partners:manage | — | PARTNER_ENABLED |
| partner_workspace_get/events_list | — | partner:events:read | PARTNER_ENABLED |
| event_asset_stage/get | events:write | partner:events:propose | EVENT_ASSETS_ENABLED |
| event_create_prepare/commit | events:write | partner:events:propose | EVENT_CREATE_ENABLED + partner create gate |
| event_operation_get | operations:read | partner:events:read | event create |
| event_edit/reschedule/postpone/cancel_prepare/commit | events:write | partner:events:propose | EVENT_OPERATIONS_ENABLED + partner create gate |
| partner_event_review_get/image/decide | partners:manage | — | partner create |
| event_operation_review_get/decide | partners:manage | — | event operations |
| event_publication_status | operations:read | partner:publications:read | event operations |
| promo_capabilities/campaigns_list/campaign_get/operation_get | promo:read | partner:promo:read | OWNER_PROMO_ENABLED / PARTNER_PROMO_ENABLED |
| promo_campaign_create/activity_add_prepare/commit | promo:write | partner:promo:request | corresponding promo gate |
| promo_campaign_state_prepare/commit | promo:write | partner:promo:request | corresponding promo gate |
| promo_operation_review_get/decide | partners:manage | — | promo |

Every flag has the `PRIVATE_EVENTS_MCP_` prefix. Existing owner/Codex read and
Social Workspace catalogs remain separate. Codex keeps its seven read tools.
All flags default off; effective workspace capabilities also require current
grant scopes/actions. Configuration should enable canonical create before
partner mutations. Assets are separately gated because they need private
storage.

## Review, recovery and publication

Preparations freeze actor, provenance, normalized changes, revision and policy.
Changes and history commit atomically. A changed grant or event invalidates the
proposal. Owner review cannot edit its payload; a different proposal requires a
new preparation. Create approval queues the existing runtime. Other commands
apply in the same short transaction as approval, current policy and CAS.

Fan-out failures leave `reconciliation_pending`: recovery schedules the current
revision without repeating canonical mutation. Old revision jobs become
`done` with `terminal_reason=superseded`, preserving rollback-compatible enums.
Owner `operations_snapshot` and `fetch(job:...)` expose optional operation/revision/terminal-reason fields without a second queue API. Managed lifecycle jobs use the same Telegram/VK adapters. Uncertain sends pause
with `terminal_reason=outcome_unknown`; no automatic retry of that intent.

Notices are planned per surface from authoritative `first_published_at`:
strictly more than 24 hours → separate notice; at most 24 hours → reconcile
original only; unknown age → `notice_review_required`; never published →
`not_planned`. No timestamp backfill from Event.added_at or URL. Internal
organizer comments never enter public notice text. An optional separate `public_notice` passes the same fact gate against the proposed state; existing public detail links are retained. Derived visual badges and
static public change-history UI remain a separate visual scope; plain lifecycle
text and existing media are used here.

Deployment must run additive `Database.init()` before enabling flags; rollback
keeps added nullable columns/tables. No production campaigns, partner grants or
public test posts are created by the implementation tests. See
[future function-call compatibility](partner-events-conversational-function-call.md).
