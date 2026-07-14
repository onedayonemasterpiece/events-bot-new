# Lane legal-registry-audit Results

## Status

completed-read-only / integrated

## Requirement IDs

- R02

## Branch

N/A — read-only reviewer lane.

## Worktree

Shared repository inspection only; no files edited.

## Base SHA

`f7abc1c29d2522831e768d8af5d94b94033be210`

## Head SHA

N/A — no commit.

## Files changed

None by reviewer. Findings were integrated by the owning integration lane into code, tests and canonical docs.

## Commands / evidence checked

- Current Minjust registry API, registry id `39b95df9-9a68-6b6d-e1e3-e6388507067e`, `lastModified=2026-07-10T14:27:00Z`.
- Exact filtered current exports for `meduzalive` and `imnotbozhena`.
- Current Minjust extremist-organizations list, updated 2026-07-14.
- Rosfinmonitoring public terrorist/extremist search, edition 2026-07-13.
- Genprokuratura undesirable-organization decision for `SIA Medusa Project`.
- Exact Telegram source surfaces for `@meduzalive` and `@imnotbozhena`.

## Verification

- `@meduzalive`: exact active foreign-agent resource match to entry 219, `SIA Medusa Project`, registration number `40103797863`; exact entity also officially undesirable; no exact extremist-list match established.
- `@imnotbozhena`: no exact foreign-agent/extremist match in the current checked snapshots. It may be blocked only as an explicit manual editorial source; the parody title does not identify a legal person.

## Risks

- Registry status is time-dependent. A static dated rule is an immediate no-spend gate, not a substitute for registry snapshot sync, TTL and active/inactive transitions.
- Fuzzy names can cause serious false legal attribution and must never hard-deny or label a source.

## Merge notes

Accepted. The integration lane implemented exact identity matching, separate legal/editorial reasons, dated evidence fields, no-spend terminal routing and regression tests.
