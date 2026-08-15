# Lane docs Results

## Status

committed

## Requirement IDs

- R01 — create the initial open sev1 August 15 ingestion/WAL recurrence record.
- R02 — restore the missing August 10, 12 and 14 incident-index contracts.
- R03 — correct stale PR #494 Draft/not-deployed statements without rewriting
  their historical checkpoint context.
- R04 — add the initial `[Unreleased]` incident entry without claiming a fix,
  deploy or recovery.
- R05 — provide this feature-fanout lane receipt.

## Branch

`agent/INC-2026-08-15/docs`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/inc-20260815-docs`

## Base SHA

`b24b435e8d362c93b9c8783e41a7e01993bf3439`

## Head SHA

Documentation implementation commit: `9cc4fa617`.

This receipt is committed separately because a file cannot contain the SHA of
the commit that first introduces that same file. The receipt commit is reported
to the integrator in the lane handoff.

## Files changed

- `CHANGELOG.md`
- `docs/reports/incidents/README.md`
- `docs/reports/incidents/INC-2026-08-10-smart-update-identity-terminal-loss.md`
- `docs/reports/incidents/INC-2026-08-15-ingestion-retry-stall-and-wal-growth.md`
- `.codex/lanes/docs/RESULTS.md`

## Commands run

- structural Python validation for incident status, mandatory headings, linked
  canonical files and incident-index membership;
- `git diff --check`;
- `git diff --cached --check`;
- stale status grep for `Draft PR #494, not deployed`, `must remain Draft`,
  `Production is unchanged` and the old deploy-readiness changelog heading;
- explicit staged-file inspection and commit.

## Tests / verification

- incident docs structural validation: PASS;
- all referenced canonical docs and prior incident records exist: PASS;
- new incident has `Status: open`, `Severity: sev1`, an explicitly provisional
  root cause, full automation contract and pending closure evidence: PASS;
- August 10, 12, 14 and new August 15 records appear in the incident index:
  PASS;
- stale current-state Draft/not-deployed phrases: absent; historical checkpoint
  statements are labelled as historical: PASS;
- `git diff --check` and staged diff check: PASS;
- no code test was applicable to this documentation-only lane.

## Risks

- The new record intentionally contains only operator-reported initial evidence;
  production counts, WAL bytes/readers, retry balances and root cause must be
  replaced or extended with timestamped live receipts by the incident owner.
- The latest main auto-sync had removed current incident-index entries. Future
  generated-doc synchronization must preserve these restored regression
  contracts.
- No final fix, recovery, deployment or incident closure is claimed.

## Merge notes

- Cherry-pick the documentation implementation commit and the following receipt
  commit together.
- Preserve the open/provisional status until live evidence and the mandatory
  regression/closure gates are complete.
- Do not resolve documentation conflicts by re-running the lossy auto-sync over
  the restored incident index.

---

## Retained prior lane receipt from base

The shared `docs` lane path already contained the following Region Talk receipt
at base SHA. It is retained verbatim so this incident handoff does not erase
previous lane evidence.

# Region Talk external-publication documentation lane

## Completed

- Copied the three approved historical JSON payloads and
  `external-publication-research-results.md` from
  `origin/agent/region-talk-external-research-results-20260801` without
  modifying JSON bytes. SHA-256 values match the source registry:
  `59b1d7cc43fff8eabe53f4f8b84b700d1c5ebc60f326b9f3f8c2d208999bc2cf`,
  `c040269f09bd72f16cf74fe2f721d9b8375ede82bd3742ce989e406747384cb0`, and
  `e662b449811a0887dd2fa0ebe33903d8caffed3231323ee9e8fbfc55b027bad7`.
- Read workflow commit `a9c9d43e` and its importer contract. Reproduced dry
  validation against that importer in an isolated `/tmp` copy: the first
  historical file has 5 semantic row rejections, the second has 1, and the
  2026-08-01 input is clean (20 valid, 0 rejected, 63 planned YDB rows).
- Added the canonical guarded-import runbook; updated Region Talk README, YDB
  schema pointer, routes, and `[Unreleased]` changelog.

## Constraints recorded

- The first two immutable historical inputs must not be dispatched. Their
  corrections require new successor request IDs and an explicit reviewed
  workflow-allowlist update; historical payload bytes remain audit evidence.
- Import is YDB staging only and does not publish to Telegram or VK.
