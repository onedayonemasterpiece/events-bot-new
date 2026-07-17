# Lane map: popular-all-owned-channels

```yaml
mode: serial_integrator_with_parallel_read_only_discovery
repo: events-bot-new
base_ref: origin/main@f2c9c83f
base_branch: main
integration_branch: integration/popular-all-owned-channels
global_constraints:
  - thin Fly bot; provider reads only in Kaggle
  - exact event attribution; no digest metric fanout
  - dedicated Telegram session unchanged
  - compact SQLite buckets and bounded retention
verification_owner: root
stop_conditions:
  - cannot establish exact single-event mapping for second VK community
  - production health/volume/session regression
lanes:
  - id: vk_official_mapping
    role: planner
    requirement_ids: [R01]
    target: existing subagent
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: shared-read-only
    writable_files: []
    forbidden_files: ['*']
    expected_output: exact ledger mapping and safe loader contract
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: vk_resolver_retention
    role: planner
    requirement_ids: [R02, R03, R04]
    target: existing subagent
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: shared-read-only
    writable_files: []
    forbidden_files: ['*']
    expected_output: resolver, single-flight and retention contract
    verification_scope: inspection_only
    effort: high
    status: completed
  - id: integrator
    role: worker
    requirement_ids: [R01, R02, R03, R04]
    target: root
    depends_on: [vk_official_mapping, vk_resolver_retention]
    execution_mode: serial_after_dependency
    branch: integration/popular-all-owned-channels
    worktree: /home/dev/.codex/worktrees/events-bot-new/popular-humanlike-scan
    writable_files: [social_metrics_batch.py, social_metrics_kaggle.py, kaggle/SocialMetricsCollector/, tests/, docs/, CHANGELOG.md, fly.toml, .env.example]
    forbidden_files: [unrelated user work]
    expected_output: merged production implementation and evidence
    verification_scope: full_local
    effort: extra-high
    status: in_progress
```
