# C1 corpus/projection lane

Status: implemented locally; commit/push/readback pending.

Owned paths:

- `site/ui-conformance/**`
- `.codex/lanes/corpus-c/RESULTS.md`

The lane contains an observed 23-archetype census and executable, dependency-free
structural contract for the C1 priority sequence: date, today, tomorrow, weekend,
and exhibitions. It reuses five frozen current-A fixture entities and makes one
justified append-only fixture addition for the deterministic today/date state.
No Astro runtime, free-collection evidence, foundations, Penpot, or control-plane
file is changed.
