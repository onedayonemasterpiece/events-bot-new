# Postbox events production release lane map

Base: `origin/main@d9ba3ad03288a923997c6626295b8a9016cf26ed`

## Requirements

| ID | Requirement | Primary owner | Dependencies | Done when |
|---|---|---|---|---|
| R01 | Record owner confirmation that the SpaceWeb canary reached `info@kgd80.ru` | integrator | — | canonical email runbook/evidence no longer says recipient arrival is unknown |
| R02 | Record owner decision to defer NotiSend key reissue | integrator | — | risk acceptance is explicit; recommendation outbound remains disabled |
| R03 | Complete the Postbox event feedback path | integrator | code-audit + infra-audit | consumer, YDS, trigger, destination, Supabase dedup/suppression, E2E, rollback and main provenance pass |

Dependency graph: `code-audit || infra-audit -> serial integrator implementation -> local contracts -> main -> live apply/deploy -> canary/replay/rollback checks`.

```yaml
mode: parallel read-only discovery, serial security-critical implementation and live integration
repo: /home/dev/projects/events-bot-new
base_ref: origin/main@d9ba3ad03288a923997c6626295b8a9016cf26ed
base_branch: main
integration_branch: integration/postbox-events-release
global_constraints:
  - never use or mutate the stale dirty checkout
  - do not enable recommendation outbound or rotate NotiSend while owner defers it
  - do not enable transactional application sending merely because feedback plumbing is deployed
  - no mass send, tariff change, plaintext recipient logs, or cross-provider fallback
  - no provider destination until the tested consumer and rollback path exist
  - live deploy only from a clean main-reachable SHA
verification_owner: integrator
stop_conditions:
  - any action requires a fixed subscription or tariff change
  - provider/DB secret cannot be retrieved without exposing it
  - event schema cannot be correlated without storing plaintext recipients
  - live mutation would touch unrelated site/CDN/core resources
lanes:
  - id: code-audit
    role: planner
    effort: extra-high
    requirement_ids: [R03]
    target: event schema, Supabase RPC, consumer code/test/deploy contract
    depends_on: []
    execution_mode: parallel
    branch: agent/postbox-events-release/code-audit
    worktree: /home/dev/.codex/worktrees/events-bot-new/postbox-events-code-audit
    writable_files: [.codex/lanes/postbox-events-release/code-audit/RESULTS.md]
    forbidden_files: [application code, migrations, provider state]
    expected_output: committed implementation specification and security review
    verification_scope: inspection_only
    status: planned
  - id: infra-audit
    role: planner
    effort: extra-high
    requirement_ids: [R03]
    target: exact YDB/YDS/IAM/Function/trigger/Postbox destination commands and rollback
    depends_on: []
    execution_mode: parallel
    branch: agent/postbox-events-release/infra-audit
    worktree: /home/dev/.codex/worktrees/events-bot-new/postbox-events-infra-audit
    writable_files: [.codex/lanes/postbox-events-release/infra-audit/RESULTS.md]
    forbidden_files: [provider mutations, application code, migrations]
    expected_output: committed redacted live inventory and operator plan
    verification_scope: live read-only
    status: planned
  - id: integrator
    role: worker_merge_reviewer
    effort: extra-high
    requirement_ids: [R01, R02, R03]
    target: implementation, tests, main merge, live infrastructure and closure E2E
    depends_on: [code-audit, infra-audit]
    execution_mode: serial_after_dependency
    branch: integration/postbox-events-release
    worktree: /home/dev/.codex/worktrees/events-bot-new/postbox-events-release
    writable_files: [serverless/email_events/, infra/yandex/email-events/, tests/, supabase/, docs/, CHANGELOG.md, .codex/integration/]
    forbidden_files: [stale dirty checkout, unrelated Fly/core/site resources]
    expected_output: main-reachable implementation and redacted live release evidence
    verification_scope: full_local_and_live
    status: planned
```
