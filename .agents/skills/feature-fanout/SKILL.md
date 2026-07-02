---
name: feature-fanout
description: Use for complex multi-point feature work with 5+ distinct requirements, requests for parallel agents/subagents/background agents, or broad features that need decomposition, worktrees, lane ownership, merge gates, and final closure audit. Do not use for simple one-file fixes.
---

# Feature Fanout

Use this skill when a user gives a feature or bugfix request containing many independent or semi-independent requirements.

Core invariant:

- Preserve every original requirement ID.
- Do not implement a large multi-point request linearly in one long pass.
- Parallelize discovery aggressively.
- Parallelize writes only when lanes have disjoint ownership or separate worktrees.
- Serialize integration and final verification.
- No lane is complete until its work is committed, rejected with evidence, or handed off as a patch artifact.
- A dirty main worktree is never an excuse to do nothing.

## Phase A — Normalize requirements

Before editing, create an execution matrix:

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|

Rules:

1. Preserve original numbering and wording.
2. If the user did not number the list, assign stable IDs: R01, R02, R03.
3. Do not merge requirements silently.
4. Do not drop ambiguous requirements; mark them `needs-interpretation` and choose the safest implementation assumption.
5. Build a dependency graph before any write lane starts.

## Phase B — Classify execution mode

Use these modes:

- `read_only_parallel`: exploration, mapping, logs, tests, risk review.
- `worktree_worker`: implementation in a dedicated branch and worktree.
- `serial_integrator`: changes touching shared architecture, overlapping files, migrations, routing, auth, schemas, or global state.
- `reviewer`: read-only final verification.
- `blocked_with_handoff`: cannot safely proceed, but must return patch/report/status.

Parallel write lanes are allowed only when:

- writable file scopes are disjoint, or
- each lane has its own branch and worktree, and
- the integrator owns the final merge.

Parallel write lanes are forbidden when:

- lanes edit the same file/component without a clear owner;
- lanes change shared schema, auth, routing, migrations, generated code, or global state;
- downstream work depends on upstream code that has not reached a stable committed head.

## Phase C — Lane map

Before spawning workers, write a lane map:

```yaml
mode:
repo:
base_ref:
base_branch:
integration_branch:
global_constraints:
verification_owner:
stop_conditions:
lanes:
  - id:
    role: planner | worker | reviewer | merge_reviewer
    requirement_ids:
    target:
    depends_on:
    execution_mode: parallel | serial_after_dependency | read_only_until_dependency
    branch:
    worktree:
    writable_files:
    forbidden_files:
    expected_output:
    verification_scope: inspection_only | targeted | full_local | ci_only
    status: planned | spawned | committed | merged | rejected | blocked
```

Rules:

- Every requirement ID must appear in exactly one primary lane and may appear in reviewer lanes.
- Every writable lane must have an owner.
- Every writable lane must have a branch name and worktree path before implementation starts.
- The parent/orchestrator must not edit inside worker-owned dirty worktrees.
- If native Codex subagents are unavailable, create the lane map and prompt pack; do not pretend subagents were launched.

## Phase D — Branch and worktree discipline

For every writable worker lane:

Create branch:

```text
agent/<feature-slug>/<lane-id>
```

Create worktree:

```text
.worktrees/<feature-slug>/<lane-id>
```

or another selected ignored/out-of-repo worktree root.

Baseline gate:

- `git status --short --branch` must be captured.
- `git rev-parse HEAD` must be recorded as `base_ref`.
- If the worktree is not clean before work begins, stop that lane and recreate it. Do not continue in a dirty worker worktree.

Worker scope:

- Edit only `writable_files`.
- Do not touch `forbidden_files`.
- Do not run destructive git commands.
- Do not push unless explicitly asked.

Worker handoff:

The worker must produce:

- committed changes, or
- a patch artifact if commit is impossible.

Required output file in the worker branch:

```text
.codex/lanes/<lane-id>/RESULTS.md
```

Required content:

```markdown
# Lane <lane-id> Results

## Status
committed | blocked-with-patch | rejected-by-worker

## Requirement IDs
- Rxx

## Branch
agent/<feature>/<lane-id>

## Base
<base sha>

## Head
<head sha>

## Files changed
- path

## Commands run
- command: result

## Tests / verification
- command: pass/fail/not-run + reason

## Risks
- risk or none

## Merge notes
- anything the integrator must know
```

Completion invariant:

- A worker lane is not complete if `git status --short` shows uncommitted changes, unless it has produced a named patch artifact and marked itself `blocked-with-patch`.

## Phase E — Integration discipline

The orchestrator/integrator owns final code consistency.

Create integration branch:

```text
integration/<feature-slug>
```

Before merging each lane:

- fetch if remote exists;
- confirm integration worktree is clean;
- inspect lane `RESULTS.md`;
- inspect `git diff base..lane_head`;
- reject unrelated changes.

Merge strategy:

- Prefer cherry-picking worker commits when lanes are small.
- Use normal merge only when preserving branch structure is useful.
- Never use bare `git push --force`.
- If force push is explicitly required later, use `--force-with-lease`, but do not push in this setup unless asked.

Conflict policy:

- Resolve conflicts only in the integration worktree.
- Do not edit worker-owned dirty worktrees.
- If a conflict cannot be resolved safely, create `.codex/integration/<lane-id>-conflict.md` and mark the lane blocked.

No lost work:

Every lane must end in one of:

- merged into integration branch;
- rejected with reason and evidence;
- blocked with patch artifact path;
- superseded by another lane, with explicit mapping.

Integration output:

Create or update:

```text
.codex/integration/INTEGRATION_REPORT.md
```

Include:

```markdown
# Integration Report

## Base
## Integration branch
## Lanes
| Lane | Requirement IDs | Branch | Status | Head SHA | Merge commit/cherry-pick | Evidence |
|---|---|---|---|---|---|---|

## Rejected or blocked lanes
| Lane | Reason | Patch/report |
|---|---|---|

## Verification
| Command | Result |
|---|---|

## Dirty worktree audit
| Worktree | Branch | Status |
|---|---|---|
```

## Phase F — Final closure audit

Before final response, run a checklist review against the original requirements:

| ID | Requirement | Status | Evidence | Missing/Risk |
|---|---|---|---|---|

Allowed statuses:

- Done
- Partial
- Missing
- Blocked
- Superseded

Rules:

- Do not claim the feature is complete unless every requirement is Done or explicitly marked otherwise.
- Final verification must be tied to the current integration head SHA.
- The final report must include:
  - changed files;
  - tests/checks run;
  - lanes spawned;
  - lanes merged;
  - lanes rejected/blocked;
  - dirty worktree audit;
  - remaining risks.
- Do not hide skipped tests.
- Do not hide unmerged worker changes.
- Do not abandon a worker branch with uncommitted code.
