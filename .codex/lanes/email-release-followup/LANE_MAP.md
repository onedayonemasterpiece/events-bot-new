# Email release follow-up lane map

Base: `origin/main@d09948130e26bea9f2294248f0b987940bc5b869`

## Requirements

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| R01 | Determine whether the owner must act for NotiSend API activation | NotiSend provider | — | low | provider-audit | Panel/API state and exact owner/provider action are proven |
| R02 | Explain and close or precisely gate Postbox production readiness | Postbox / deliverability / events | R01 independent | medium | provider-audit | Event destination and Spam-placement state are verified; safe next action is explicit |
| R03 | Send and verify a real message from `info@kenigevents.ru` | SpaceWeb SMTP/webmail | — | high (live send) | smtp-canary | One controlled send reaches the test mailbox with header/auth evidence, or a precise provider blocker is recorded |
| R04 | Reconcile the hanging GitHub CI with the static-site release work | GitHub Actions / release governance | — | high | ci-audit | Current main/workflow/run history is compared; root cause and safe fix/disable state are verified |

Dependency graph: `R01 || R02 || R03 || R04 -> integrator review -> docs/changelog if behavior changes -> main`.

```yaml
mode: parallel discovery and bounded live provider checks, serial integration
repo: /home/dev/projects/events-bot-new
base_ref: origin/main@d09948130e26bea9f2294248f0b987940bc5b869
base_branch: main
integration_branch: integration/email-release-followup
global_constraints:
  - do not use or mutate the stale dirty checkout
  - no mass send, contact import, tariff change, or cross-provider fallback
  - at most one controlled SMTP canary from info@kenigevents.ru
  - never print or commit mailbox/provider credentials
  - do not enable outbound application switches until all production gates pass
verification_owner: integrator
stop_conditions:
  - provider action requires tariff/payment change
  - CAPTCHA or owner 2FA cannot be completed safely
  - a test would send to anyone other than the controlled internal recipient
  - CI fix would weaken unrelated incident regression coverage without evidence
lanes:
  - id: provider-audit
    role: worker
    effort: high
    requirement_ids: [R01, R02]
    target: NotiSend activation and Postbox event/deliverability readiness
    depends_on: []
    execution_mode: parallel
    branch: agent/email-release-followup/provider-audit
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-followup-provider
    writable_files: [.codex/lanes/email-release-followup/provider-audit/RESULTS.md]
    forbidden_files: [application code, Supabase migrations, DNS except an evidence-backed additive event destination change]
    expected_output: committed redacted provider audit with exact action/blocker
    verification_scope: live targeted
    status: planned
  - id: smtp-canary
    role: worker
    effort: extra-high
    requirement_ids: [R03]
    target: SpaceWeb outbound canary
    depends_on: []
    execution_mode: parallel
    branch: agent/email-release-followup/smtp-canary
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-followup-smtp
    writable_files: [.codex/lanes/email-release-followup/smtp-canary/RESULTS.md]
    forbidden_files: [application code, DNS, mailbox settings unrelated to SMTP]
    expected_output: committed redacted send/delivery/header evidence
    verification_scope: live controlled
    status: planned
  - id: ci-audit
    role: worker
    effort: extra-high
    requirement_ids: [R04]
    target: GitHub Actions hanging regression job
    depends_on: []
    execution_mode: parallel
    branch: agent/email-release-followup/ci-audit
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-followup-ci
    writable_files: [.github/workflows/, docs/operations/release-governance.md, CHANGELOG.md, .codex/lanes/email-release-followup/ci-audit/RESULTS.md]
    forbidden_files: [email provider state, application behavior outside the CI root cause]
    expected_output: committed focused CI fix or audit-only blocker handoff
    verification_scope: full local and GitHub Actions
    status: planned
  - id: integrator
    role: merge_reviewer
    effort: extra-high
    requirement_ids: []
    target: conflict resolution, canonical docs, closure audit and release provenance
    depends_on: [provider-audit, smtp-canary, ci-audit]
    execution_mode: serial_after_dependency
    branch: integration/email-release-followup
    worktree: /home/dev/.codex/worktrees/events-bot-new/email-release-followup
    writable_files: [integration-only docs/changelog reconciliation, integration report]
    forbidden_files: [stale dirty checkout]
    expected_output: merged/rejected lanes, full verification, main-reachable changes
    verification_scope: full local and live evidence review
    status: in_progress
```
