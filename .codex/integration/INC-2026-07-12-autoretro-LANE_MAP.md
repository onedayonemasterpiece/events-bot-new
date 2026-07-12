# Lane map: INC-2026-07-12 Autoretro location/period

- requirement_id: R01
  description: Establish source-grounded one-day date, time, outdoor location and event identity for @kldevents/2297.
  owner: source_truth
  dependencies: []
  validation: Original sources/posters and public text agree on event-local facts.
  status: completed
- requirement_id: R02
  description: Freeze canonical and all managed public projections for the affected event.
  owner: surface_inventory
  dependencies: []
  validation: Event/source/poster/jobs plus Telegram/VK/Telegraph rows and rendered content are mapped.
  status: completed
- requirement_id: R03
  description: Audit the full future catalog and recent public windows for the same and adjacent incident classes.
  owner: future_audit
  dependencies: []
  validation: Vector-first recall plus direct source/public inspection produces classified findings.
  status: completed
- requirement_id: R04
  description: Trace root cause and implement LLM-first/vector-first prevention with replay and negative controls.
  owner: integrator
  dependencies: [R01, R02, R03]
  validation: Production-boundary replay passes through Smart Update and targeted regression tests pass.
  status: completed
- requirement_id: R05
  description: Repair canonical data and every managed public projection, deploy and verify catch-up.
  owner: integrator
  dependencies: [R01, R02, R04]
  validation: Backups, repaired DB, Telegram/VK/Telegraph checks, deployed SHA from origin/main and post-deploy evidence.
  status: in_progress

## Integration order

1. Complete read-only source, surface and future-catalog lanes.
2. Freeze the affected set and incident regression contracts.
3. Implement prevention and production-boundary replay serially.
4. Ship from a clean worktree, then repair canonical/public surfaces.
5. Re-run the future audit and close only with release evidence.
