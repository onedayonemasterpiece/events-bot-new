# C1 corpus/projection lane

Status: COMPLETE — committed, pushed, and remote-read back.

- Branch: `agent/e0-overnight/corpus-c`
- Commit: `cab613b736e30b1032e937beb8ea3f9bc3e55b58` (initial C1 payload)
- Initial remote readback: `cab613b736e30b1032e937beb8ea3f9bc3e55b58`
- Validation: `cd site && node --test ui-conformance/tests/corpus-contract.test.mjs` — 4/4 passing.

Owned paths:

- `site/ui-conformance/**`
- `.codex/lanes/corpus-c/RESULTS.md`

Delivered an observed 23-archetype census and a dependency-free structural
contract for the C1 priority sequence: date, today, tomorrow, weekend, and
exhibitions. The shared append-only corpus reuses five frozen current-A fixture
entities and makes one justified addition (`event.real.4240`) for the
deterministic today/date state. The lane deliberately changes no Astro runtime,
free-collection evidence, foundations, Penpot, or control-plane file.
