# Static-site resilient transport and ecological egress lane map

Integration base refreshed to `origin/main` at `45be3557583aad05898e1767c5526b56efbcf847`.

## Execution matrix

| ID | Requirement | Area | Dependencies | Conflict risk | Primary lane | Done when |
|---|---|---|---|---|---|---|
| R01 | Use the proven resilient Supabase transport on every dynamic static-site surface | browser/Auth/Data | client inventory | high | W1 | no production browser client bypasses the shared transport |
| R02 | Keep Auth and writes safe: no ambiguous duplicate OTP/write, stable session key | Auth/RPC | R01 | high | W1 | Auth E2E and non-idempotent single-send tests pass |
| R03 | Bound abuse, DDoS amplification, retries and storage growth | Gateway/Supabase/client | security inventory, R01 | high | W3 then integrator | rate/cap/RLS/idempotency gates have executable evidence |
| R04 | Reduce Supabase egress and static publication bytes through incremental export/build/upload | builder/publisher | egress inventory | medium | W2 | unchanged builds avoid bulk Supabase reads and redundant uploads |
| R05 | Add a bounded local-first idempotent outbox for strong actions | browser/RPC | R01/R03 | high | W1 | offline action survives reload and reaches Supabase once |
| R06 | Update canonical docs, incident evidence, tests, release and live gates | integration | R01–R05 | high | I1 | build, focused suites, Playwright and deployment checks pass |
| R07 | Make browser storage compact and ecological | browser storage | storage inventory, R05 | high | W1 | versioned budgets, TTL, compaction and migration tests pass |

## Dependency graph

`M1/M2/M3/M4 -> W1/W2/W3`; root-release mapping -> `W4`; all write lanes -> `I1` -> acceptance/release.

```yaml
mode: parallel discovery, isolated write lanes, serial integration
repo: events-bot-new
base_ref: origin/main@45be3557
base_branch: main
integration_branch: integration/static-site-resilient-egress-20260731
global_constraints:
  - preserve thin-client architecture and Supabase ownership
  - never expose service/secret keys
  - never retry ambiguous OTP or other non-idempotent operations across routes
  - do not deploy from a dirty worktree
verification_owner: I1
stop_conditions:
  - a schema or relay change weakens RLS or creates an unbounded public proxy
  - source-of-truth ownership moves from Supabase without an explicit architecture decision
lanes:
  - id: M1
    role: planner
    requirement_ids: [R01, R02, R05, R07]
    target: dynamic-client and browser-storage inventory
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: shared-read-only
    writable_files: []
    forbidden_files: [all]
    expected_output: file-line inventory and integration recommendations
    verification_scope: inspection_only
    status: spawned
  - id: M2
    role: planner
    requirement_ids: [R04]
    target: static egress and publication inventory
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: shared-read-only
    writable_files: []
    forbidden_files: [all]
    expected_output: measured egress root cause and incremental strategy
    verification_scope: inspection_only
    status: spawned
  - id: M3
    role: planner
    requirement_ids: [R03]
    target: thin-client threat model and limits
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: shared-read-only
    writable_files: []
    forbidden_files: [all]
    expected_output: exact hardening changes and tests
    verification_scope: inspection_only
    status: spawned
  - id: M4
    role: planner
    requirement_ids: [R06]
    target: canonical P0/P1 gap audit
    depends_on: []
    execution_mode: parallel
    branch: none-read-only
    worktree: shared-read-only
    writable_files: []
    forbidden_files: [all]
    expected_output: deployed-versus-planned dependency checklist
    verification_scope: inspection_only
    status: spawned
  - id: W1
    role: worker
    requirement_ids: [R01, R02, R05, R07]
    target: shared browser transport, Auth and compact local outbox
    depends_on: [M1, M3]
    execution_mode: serial_after_dependency
    branch: agent/static-site-resilient-egress/W1
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-site-resilient-client
    writable_files: [site/src/lib, site/src/components, site/src/layouts, site/src/pages, site/src/data, site/package.json, site/package-lock.json, site/tests]
    forbidden_files: [kaggle/StaticSiteBuilder, scripts/run_static_site_builder_kaggle.py, infra/yandex]
    expected_output: committed client/outbox implementation and targeted tests
    verification_scope: full_local
    status: planned
  - id: W2
    role: worker
    requirement_ids: [R04]
    target: incremental export/build/upload and egress budgets
    depends_on: [M2]
    execution_mode: serial_after_dependency
    branch: agent/static-site-resilient-egress/W2
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-site-compact-egress
    writable_files: [kaggle/StaticSiteBuilder, scripts/run_static_site_builder_kaggle.py, site/scripts, tests, docs/operations/kaggle-static-site-builder.md]
    forbidden_files: [site/src/lib, site/src/components, infra/yandex]
    expected_output: committed incremental publisher implementation and byte/query evidence
    verification_scope: full_local
    status: planned
  - id: W3
    role: worker
    requirement_ids: [R03]
    target: relay and database abuse controls
    depends_on: [M3]
    execution_mode: serial_after_dependency
    branch: agent/static-site-resilient-egress/W3
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-site-resilient-security
    writable_files: [infra/yandex, supabase/migrations, tests]
    forbidden_files: [site/src/components, kaggle/StaticSiteBuilder]
    expected_output: committed least-privilege limits and regression tests
    verification_scope: targeted
    status: planned
  - id: I1
    role: merge_reviewer
    requirement_ids: [R06]
    target: integrate, reconcile docs/changelog, test and release
    depends_on: [W1, W2, W3]
    execution_mode: serial_after_dependency
    branch: integration/static-site-resilient-egress-20260731
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-site-resilient-egress
    writable_files: [integration conflicts, canonical docs, CHANGELOG.md]
    forbidden_files: []
    expected_output: clean integrated branch, integration report and release evidence
    verification_scope: full_local
    status: in_progress
  - id: W4
    role: worker
    requirement_ids: [R04, R06]
    target: default-off atomic blue/green root publisher and Smart Update handoff
    depends_on: [M2]
    execution_mode: parallel
    branch: agent/static-site-resilient-egress/W4
    worktree: /home/dev/.codex/worktrees/events-bot-new/static-site-atomic-root
    writable_files: [static_site_release.py, main.py release finish path, production publisher, focused tests, release docs]
    forbidden_files: [site/src, site/scripts/export-production-preview-data.py, supabase, infra/yandex/supabase-relay]
    expected_output: committed fail-closed inactive-stage/switch/smoke/rollback state machine and setup runbook
    verification_scope: full_local
    status: spawned
```
