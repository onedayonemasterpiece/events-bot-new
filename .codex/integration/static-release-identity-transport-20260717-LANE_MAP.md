# Static release identity + transport lane map

```yaml
mode: worktree_worker_then_serial_integrator
repo: events-bot-new
base_ref: origin/main@2822a91d6173883fca36ccf135802280ba4ab09d
base_branch: origin/main
worker_recovery_base: d169004376c309dc487fa6b48a7aae4a8ed7dea3
integration_branch: integration/static-release-identity-transport-20260717
global_constraints:
  - preserve crash-worktree work before edits
  - no production Supabase migration before ledger reconciliation and security gates
  - transport schedules and activation default off before controlled canary
  - no final identity/header/listing/event-detail or transport-card UI work
verification_owner: root integrator
release_umbrella:
  source: origin/docs/static-site-release-plan-20260717@8fecf7da
  merge_target: integration/static-release-identity-transport-20260717
  status: merged_with_current_foundation_status
  excluded_side_candidate: "PR #43 production publisher; conflicting/current-main re-port required"
stop_conditions:
  - unreconciled production migration 20260717074903
  - failed RLS/grant/security assertion
  - invalid provider output changes accepted last-known-good state
  - dirty or unrelated lane diff
lanes:
  - id: identity-saved-events
    role: worker
    requirement_ids: [ID-1, ID-2, ID-3, ID-4, ID-5, ID-6, ID-7, ID-8]
    target: Supabase identity, saved occurrences, consent and D-1 reminder control plane
    depends_on: []
    execution_mode: parallel
    branch: agent/static-release/identity-saved-events
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-release-identity-saved-events
    writable_files: identity lane SQL/RPC/controller/contracts/canonical docs
    forbidden_files: final header,/izbrannoe/,listing cards,event-detail layout,transport lane
    expected_output: clean pushed commits plus RESULTS.md
    verification_scope: full_local
    effort: extra-high/max
    accepted_worker_head: bcd1d118
    integration_commits: [dab86805, 53b7edef, 2fa2734e, 984b9f67]
    status: merged
  - id: transport-refresh
    role: worker
    requirement_ids: [TR-1, TR-2, TR-3, TR-4, TR-5, TR-6, TR-7, TR-8, TR-9]
    target: independent provider jobs, validated fan-in, immutable manifest and changed-hash rebuild
    depends_on: []
    execution_mode: parallel
    branch: agent/static-release/transport-refresh
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-release-transport-refresh
    writable_files: transport_refresh,kernels,runners,publisher,contracts,canonical docs
    forbidden_files: transport UI cards,listings,event-detail composition,identity lane
    expected_output: clean pushed commits plus RESULTS.md
    verification_scope: full_local
    effort: high
    accepted_worker_head: a83704b0
    integration_commits: [54d07401, 06ee3c9a]
    status: merged
  - id: integration
    role: merge_reviewer
    requirement_ids: [INT-1, INT-2, INT-3, INT-4, INT-5, INT-6, INT-7]
    target: scoped review,cherry-pick,docs/routes/changelog,joint regressions,push
    depends_on: [identity-saved-events, transport-refresh]
    execution_mode: serial_after_dependency
    branch: integration/static-release-identity-transport-20260717
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-release-identity-transport-20260717
    writable_files: integration conflict resolutions,docs/routes.yml,CHANGELOG.md,integration report
    forbidden_files: user dirty root checkout,worker-owned dirty worktrees
    expected_output: clean pushed integration commit and closure matrix
    verification_scope: full_local
    effort: extra-high/max
    integration_report: .codex/integration/static-release-identity-transport-20260717-INTEGRATION_REPORT.md
    status: completed
```

Recovery checkpoints created before further worker edits:

- identity: `01a4bef5` (starting committed head `8bc59dc0`);
- transport: `206072cf` (starting base `d1690043`).
