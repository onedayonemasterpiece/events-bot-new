# Lane map — INC-2026-07-10 future date quality

```yaml
mode: read_only_parallel_then_serial_integrator
repo: events-bot-new
base_ref: origin/main
base_branch: main
integration_branch: integration/incident-20260710-future-date
global_constraints:
  - incident workflow; source-grounded repair only
  - LLM-first semantics, vector-first retrieval/candidate generation
  - no production/public writes before row backups and source verification
  - do not reuse Telegram role-scoped auth bundles
verification_owner: /root
stop_conditions:
  - ambiguous source truth for destructive repair
  - unhealthy production before/during repair or deploy
lanes:
  - id: prod-event-discovery
    role: investigator
    requirement_ids: [R01, R02, R04]
    target: production DB + VK/TG/Telegraph/source evidence
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: null
    writable_files: [artifacts/codex/INC-2026-07-10-future-date-quality]
    forbidden_files: ['tracked repo files', 'production state']
    expected_output: exact target inventory and correctness classification
    verification_scope: inspection_only
    effort: high
    status: spawned
  - id: incident-code-mapping
    role: investigator
    requirement_ids: [R03, R06]
    target: prior regression contracts and date/range root causes
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: null
    writable_files: []
    forbidden_files: ['tracked repo files', 'production state']
    expected_output: incident family map and code/prompt root-cause map
    verification_scope: inspection_only
    effort: extra-high
    status: spawned
  - id: vector-audit-design
    role: architect
    requirement_ids: [R05, R07]
    target: reusable vector-first retrieval + LLM-first quality audit/prevention
    depends_on: []
    execution_mode: parallel
    branch: null
    worktree: null
    writable_files: []
    forbidden_files: ['tracked repo files', 'production state']
    expected_output: compatible implementation design and reusable components
    verification_scope: inspection_only
    effort: extra-high
    status: spawned
  - id: serial-integrator
    role: integrator
    requirement_ids: [R01, R02, R03, R04, R05, R06, R07, R08]
    target: incident record, implementation, tests, production repair, release closure
    depends_on: [prod-event-discovery, incident-code-mapping, vector-audit-design]
    execution_mode: serial_after_dependency
    branch: integration/incident-20260710-future-date
    worktree: .worktrees/incident-20260710-future-date
    writable_files: ['root-cause code/tests', 'canonical docs', 'CHANGELOG.md']
    forbidden_files: ['unrelated dirty primary-worktree files']
    expected_output: merged prevention and verified all-surface repair
    verification_scope: full_local_and_production
    effort: extra-high
    status: in_progress
```
