# Autopresenter M0 execution matrix

Base: `981aebd9`; integration branch: `integration/autopresenter-m0`.

## Requirements

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | 20 full cold cycles per candidate: 10 fresh + 10 persistent profiles, new Node/browser each run | runtime | `tools/autopresenter/m0/src/` | candidate layout | medium | runtime | yes | runner emits one result per process cycle |
| R02 | Separate 20/20 local fixture compatibility and 5/5 live `/zavtra/` smoke | runtime | `src/`, `fixture/` | R01 | medium | runtime | yes | two independent metrics and targets |
| R03 | Two exact hermetic candidates across Playwright 1.57 boundary | packaging | `candidates/`, `scripts/` | none | low | packaging | yes | exact versions/build metadata and build scripts |
| R04 | Fail closed: no channel/system browser/download/global cache fallback | packaging | `candidates/`, `scripts/` | R03 | medium | packaging | yes | config/build checks reject fallback paths |
| R05 | Verify clean browser/Node process termination and no orphans | runtime | `src/verify-process-cleanup.js` | R01 | high | runtime | yes | run result contains bounded cleanup evidence |
| R06 | Machine-readable evidence package and schemas | evidence | `schemas/`, report code | runtime result contract | low | evidence | yes | JSON schemas and report aggregator exist |
| R07 | Offline `self-test.cmd`: local fixture/click/files/browser cleanup | runtime | `src/self-test.js` | R01/R02 | medium | runtime | yes | no production/relay/npm/admin/network dependency |
| R08 | Windows launcher, candidate build and path matrix support | packaging | `scripts/*.ps1`, `*.cmd` | R03 | medium | packaging | yes | build/run scripts quote paths and log evidence |
| R09 | Strict PASS/FAIL and winner/no-go selection | evidence | report aggregator/tests | R01–R06 | medium | evidence | yes | 19/20 fails; 20/20+5/5 only pass |
| R10 | Enforce M0-only scope; M1–M3 forbidden | docs | M0 README | none | low | docs | yes | explicit allowed/forbidden inventory |
| R11 | Sync canonical docs/routes/changelog with strict M0 contract | docs | `docs/`, `CHANGELOG.md` | review | medium | docs | yes | cold/live/evidence rules canonical |
| R12 | Honest handoff: target Win10 execution remains required | docs | M0 README/report template | R06 | low | docs | yes | no local Linux result can claim M0 PASS |

## Dependency graph

`R03 → R01/R07/R08`; `R01/R02/R05 → R06/R09`; `R06/R09 → R12`; M1 is blocked until R09 returns PASS on the target Windows 10 laptop.

## Lane map

```yaml
mode: worktree_worker_then_serial_integrator
repo: onedayonemasterpiece/events-bot-new
base_ref: 981aebd9
base_branch: feature/autopresenter-design
integration_branch: integration/autopresenter-m0
global_constraints:
  - implement M0 only
  - no M1 stage, relay, phone UI, recording, final overlays
  - no system browser/channel/download on target
  - no claim of Windows PASS without target evidence
verification_owner: /root
stop_conditions:
  - target Windows 10 unavailable for empirical pass
  - candidate package cannot be built hermetically
lanes:
  - id: runtime
    role: worker
    effort: high
    requirement_ids: [R01, R02, R05, R07]
    execution_mode: parallel
    branch: agent/autopresenter-m0/runtime
    worktree: /home/dev/projects/events-bot-new-autopresenter-m0-runtime
    writable_files: [tools/autopresenter/m0/src, tools/autopresenter/m0/fixture, tools/autopresenter/m0/package.json, tools/autopresenter/m0/package-lock.json, .codex/lanes/runtime/RESULTS.md]
    forbidden_files: [docs, CHANGELOG.md, tools/autopresenter/m0/scripts, tools/autopresenter/m0/schemas]
    verification_scope: full_local
  - id: packaging
    role: worker
    effort: high
    requirement_ids: [R03, R04, R08]
    execution_mode: parallel
    branch: agent/autopresenter-m0/packaging
    worktree: /home/dev/projects/events-bot-new-autopresenter-m0-packaging
    writable_files: [tools/autopresenter/m0/candidates, tools/autopresenter/m0/scripts, tools/autopresenter/release-m0, .codex/lanes/packaging/RESULTS.md]
    forbidden_files: [docs, CHANGELOG.md, tools/autopresenter/m0/src, tools/autopresenter/m0/schemas]
    verification_scope: targeted
  - id: evidence
    role: worker
    effort: high
    requirement_ids: [R06, R09]
    execution_mode: parallel
    branch: agent/autopresenter-m0/evidence
    worktree: /home/dev/projects/events-bot-new-autopresenter-m0-evidence
    writable_files: [tools/autopresenter/m0/schemas, tools/autopresenter/m0/reporting, tools/autopresenter/m0/tests, .codex/lanes/evidence/RESULTS.md]
    forbidden_files: [docs, CHANGELOG.md, tools/autopresenter/m0/src, tools/autopresenter/m0/scripts]
    verification_scope: full_local
  - id: docs
    role: worker
    effort: high
    requirement_ids: [R10, R11, R12]
    execution_mode: parallel
    branch: agent/autopresenter-m0/docs
    worktree: /home/dev/projects/events-bot-new-autopresenter-m0-docs
    writable_files: [tools/autopresenter/m0/README.md, docs/features/static-site-pages/auto-present, docs/features/static-site-pages/README.md, docs/features/README.md, docs/routes.yml, CHANGELOG.md, .codex/lanes/docs/RESULTS.md]
    forbidden_files: [tools/autopresenter/m0/src, tools/autopresenter/m0/scripts, tools/autopresenter/m0/schemas]
    verification_scope: inspection_only
```
