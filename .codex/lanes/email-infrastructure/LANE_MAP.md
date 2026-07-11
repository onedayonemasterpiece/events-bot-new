# Email Infrastructure Lane Map

Base: `origin/main@50d4087d5e51e5f27239c97a962a91848475f961`

## Requirements

| ID | Requirement | Primary lane | Depends on | Done when |
|---|---|---|---|---|
| R01 | Isolate work from the stale dirty checkout and preserve release provenance | integrator | — | clean main-based integration branch/worktree exists |
| R02 | Repair Postbox temporary-key hygiene and remove leaked temporary keys safely | provider-ops | R01 | helper is fail-closed and temporary keys are audited/cleaned without touching persistent production credentials |
| R03 | Reconcile canonical architecture: SpaceWeb inbound/human, Postbox transactional, NotiSend recommendations capped at 200, Supabase control plane | architecture-docs | R01 | canonical feature/operations/routes/changelog state agrees |
| R04 | Provision SpaceWeb mailbox/DNS without moving NS or disturbing site records | provider-ops | R02,R03 | mailbox, TLS client access, exact DNS, DKIM and DMARC monitoring verified |
| R05 | Build Yandex Mail Trigger inbound pipeline with private attachments, retries, DLQ and idempotency | inbound | R03,R04 | deployed pipeline and direct/forwarded canary evidence exist |
| R06 | Implement verified-email consent, purpose-specific subscription, suppression, outbox and send guard in personalization Supabase | control-plane | R03 | reviewed migration/API/worker tests and live schema evidence exist |
| R07 | Implement Postbox transactional adapter/events with real provider IDs and no marketing fallback | control-plane | R02,R06 | sender identity, callback ingestion and seed send pass |
| R08 | Implement NotiSend recommendation adapter/webhooks with hard cap 200 | control-plane | R03,R06 | canary group only, consent/suppression and cap enforcement pass |
| R09 | Run controlled E2E from info@kgd80.ru and provider-specific failure/unsubscribe tests | integrator | R04-R08 | redacted evidence matrix and rollback drill pass |
| R10 | Merge to origin/main, deploy only from clean main-reachable SHA, and verify production | integrator | R09 | main reachability, deploy SHA, checks and rollback evidence recorded |

## Dependency graph

`R01 -> R02/R03 -> R04/R06 -> R05/R07/R08 -> R09 -> R10`

## Lanes

```yaml
mode: staged fanout with serial production integration
repo: /home/dev/projects/events-bot-new
base_ref: origin/main@50d4087d5e51e5f27239c97a962a91848475f961
integration_branch: integration/email-infrastructure-release
integration_worktree: /home/dev/.codex/worktrees/events-bot-new/email-infrastructure-release
global_constraints:
  - no tariff changes or paid add-ons
  - NotiSend recommendations are capped at 200 consented users
  - no mass send or production-list import during setup
  - no deploy from the existing dirty checkout
  - no cross-provider fallback
  - origin/main is the only production source of truth
verification_owner: integrator
stop_conditions:
  - any action requires a tariff change or new fixed subscription
  - provider login requires owner 2FA/CAPTCHA
  - DNS values cannot be read from the current provider control plane
  - production data ownership conflicts with the accepted Supabase architecture
lanes:
  - id: architecture-docs
    role: worker
    requirement_ids: [R03]
    execution_mode: parallel
    branch: agent/email-infrastructure/architecture-docs
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-architecture-docs
    writable_files: [docs/, CHANGELOG.md, .codex/lanes/email-infrastructure/architecture-docs/RESULTS.md]
    forbidden_files: [application code, migrations, provider state]
    verification_scope: inspection_only
    status: planned
  - id: inbound
    role: planner_then_worker
    requirement_ids: [R05]
    execution_mode: read_only_until_dependency
    branch: agent/email-infrastructure/inbound
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-inbound
    writable_files: [email_infrastructure/inbound/, tests/email_infrastructure/, deployment manifests, lane RESULTS]
    forbidden_files: [Supabase migrations, provider adapters, canonical docs owned by architecture-docs]
    verification_scope: targeted
    status: planned
  - id: control-plane
    role: planner_then_worker
    requirement_ids: [R06, R07, R08]
    execution_mode: read_only_until_dependency
    branch: agent/email-infrastructure/control-plane
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-control-plane
    writable_files: [email_infrastructure/control_plane/, email_infrastructure/providers/, supabase/, tests/email_infrastructure/, lane RESULTS]
    forbidden_files: [inbound handler, provider control panels, canonical docs owned by architecture-docs]
    verification_scope: targeted
    status: planned
  - id: provider-ops
    role: integrator
    requirement_ids: [R02, R04]
    execution_mode: serial_after_dependency
    branch: integration/email-infrastructure-release
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-infrastructure-release
    writable_files: [provider control planes, Yandex Cloud resources, exact IaC/runbook integration]
    forbidden_files: [unrelated production resources, existing site DNS records, kgd80 persistent credentials]
    verification_scope: live controlled
    status: planned
  - id: integrator
    role: integrator_merge_reviewer
    requirement_ids: [R01, R09, R10]
    execution_mode: serial_after_dependency
    branch: integration/email-infrastructure-release
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-infrastructure-release
    writable_files: [integration conflict resolution, release evidence, final docs/changelog reconciliation]
    forbidden_files: [existing dirty checkout]
    verification_scope: full_local_and_live_canary
    status: in_progress
```
