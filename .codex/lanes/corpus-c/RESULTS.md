# C1/C2 corpus-projection lane

## C2 bounded corrective receipt — generation 19

Status: COMPLETE — payload committed, pushed, and remote-read back.

- Control: `E0_CHATGPT_PRO`, generation 19, `ASTRO_AS_IS_REFERENCE`.
- Branch: `agent/e0-overnight/corpus-c`.
- Worktree: `/home/dev/.codex/worktrees/events-bot-new/e0-corpus-c`.
- Initial SHA: `f280254308a636335de74f8cbdc8df95999a0b90`.
- Payload commit: `99900d0aed343b1fb6474213a1619193f77ff330`.
- Payload remote readback: `99900d0aed343b1fb6474213a1619193f77ff330`.
- Validation: `cd site && node --test ui-conformance/tests/corpus-contract.test.mjs` — 10/10 passing, 0 failed/skipped.
- Diff validation: `git diff --check` and staged `git diff --cached --check` — passing.
- Read-only checklist review: R1–R6 Done; no blocker.

C2 replaces lossy reused-fixture copies with immutable Git references and
canonical source-record hashes. `event.real.4240` is an explicitly named
projection whose exact pinned `venue_name` and `end_date` are enforced by
source-fidelity tests. Executed JSON Schemas, negative cases, source/file hashes,
and a cross-file receipt are part of the narrow contract. No Astro runtime,
free-collection evidence/candidate, foundations tuple, Penpot, deployment, or
main-branch path changed.

Owned paths:

- `site/ui-conformance/**`
- `.codex/lanes/corpus-c/RESULTS.md`

## C1 receipt

Status: COMPLETE — committed, pushed, and remote-read back.

- Commit: `cab613b736e30b1032e937beb8ea3f9bc3e55b58` (initial C1 payload).
- Initial remote readback: `cab613b736e30b1032e937beb8ea3f9bc3e55b58`.
- Validation: `cd site && node --test ui-conformance/tests/corpus-contract.test.mjs` — 4/4 passing.

Delivered an observed 23-archetype census and a dependency-free structural
contract for the C1 priority sequence: date, today, tomorrow, weekend, and
exhibitions. The lane deliberately changed no Astro runtime, free-collection
evidence, foundations, Penpot, or control-plane file.
