# Lane map — INC-2026-07-13 runtime logging and recurring event quality

```yaml
mode: fanout_read_only_then_serial_integrator
repo: events-bot-new
base_ref: origin/main
base_branch: main
integration_branch: integration/incident-20260713-logging-quality
global_constraints:
  - production mutations only by integrator
  - do not repurpose Telegram auth bundles
  - never delete /data/db.sqlite or unknown data without evidence
  - logging must be bounded by rotation and retention
  - semantic fixes must remain LLM-first and vector-first
verification_owner: /root
stop_conditions:
  - uncertain ownership of volume path
  - database backup/quick_check failure
  - E2E Telegram identity cannot be proven
lanes:
  - id: disk-logs-discovery
    role: planner
    requirement_ids: [R01, R02]
    target: read-only Fly volume inventory, current env, July 8 disk incident regression contract, safe cleanup proposal
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: none-read-only
    writable_files: []
    forbidden_files: [production state, repository files]
    expected_output: exact sizes/mtimes/ownership plus delete/keep plan and bounded logging recommendation
    verification_scope: inspection_only
    effort: high
    status: planned
  - id: future-quality-audit
    role: planner
    requirement_ids: [R04]
    target: fresh frozen inventory of all future events, incident regression scan, duplicate/semantic/location/public-surface findings
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: none-read-only
    writable_files: [artifacts/codex/INC-2026-07-13-runtime-logging-quality/**]
    forbidden_files: [production writes, source code, docs]
    expected_output: source-adjudicated findings and candidate root-cause clusters
    verification_scope: full_local
    effort: extra-high
    status: planned
  - id: guide-media-retention
    role: worker
    requirement_ids: [R01]
    target: bounded retention for /data/guide_media based on future-occurrence references, age, total budget and free-space floor
    depends_on: [disk-logs-discovery]
    execution_mode: parallel
    branch: agent/incident-20260713/guide-media-retention
    worktree: .worktrees/incident-20260713-guide-media-retention
    writable_files: [guide_excursions/service.py, tests/test_guide_media_retention.py, docs/features/guide-excursions-monitoring/README.md, .codex/lanes/guide-media-retention/RESULTS.md]
    forbidden_files: [fly.toml, runtime_logging.py, CHANGELOG.md, incident records, production state]
    expected_output: committed retention implementation with dry-run inventory and targeted tests
    verification_scope: targeted
    effort: high
    status: planned
  - id: auth-e2e-integrator
    role: worker
    requirement_ids: [R03]
    target: schema-first minimal production role grant, Telegram UI VK auto-import E2E, log evidence
    depends_on: [disk-logs-discovery]
    execution_mode: serial_after_dependency
    branch: integration/incident-20260713-logging-quality
    worktree: .worktrees/incident-20260713-logging-quality
    writable_files: [production admin role row, E2E docs/tests if needed]
    forbidden_files: [unrelated users, Telegram auth env]
    expected_output: before/after role evidence and live UI E2E terminal result
    verification_scope: full_local
    effort: extra-high
    status: planned
  - id: incident-fix-integrator
    role: worker
    requirement_ids: [R01, R02, R05]
    target: safe cleanup, bounded logging enablement, root-cause fixes, docs/changelog, tests/deploy/catch-up
    depends_on: [disk-logs-discovery, future-quality-audit, auth-e2e-integrator]
    execution_mode: serial_after_dependency
    branch: integration/incident-20260713-logging-quality
    worktree: .worktrees/incident-20260713-logging-quality
    writable_files: [runtime logging/config, quality pipeline, tests, docs, CHANGELOG.md, production repairs]
    forbidden_files: [unrelated dirty root worktree]
    expected_output: commits on origin/main, deployed SHA, health/log rotation/E2E/vector verification
    verification_scope: full_local
    effort: extra-high
    status: planned
```

## Requirements

| ID | Requirement | Dependencies | Done when |
|---|---|---|---|
| R01 | Inventory and safely clean production disk | none | deleted paths are evidenced; DB protected; post-clean free space recorded |
| R02 | Re-enable runtime file logs with bounded retention | R01 | actual env enabled; active log grows; rotation/retention and disk guard verified |
| R03 | Grant E2E Telegram identity minimal admin access and run live VK import E2E | R02 | UI command completes for 1–3 items and is reconciled with logs/DB |
| R04 | Perform a new complete audit of all future events/public surfaces/prior incident families | none | frozen population accounted for and findings source-adjudicated |
| R05 | Fix reproducible root causes LLM-first/vector-first and deliver production verification | R03,R04 | regression replays/tests pass, deployment/catch-up/vector/public evidence recorded |
