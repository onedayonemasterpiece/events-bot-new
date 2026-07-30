# Hero Talk focus-link integration — 2026-07-30

| ID | Requirement | Owner | Status | Evidence |
|---|---|---|---|---|
| R1 | Exactly one cursor at the true end | integrator | Done | explicit `[data-home-hero-cursor]`; browser gate asserts one per scene/active scene |
| R2 | More phrases and events | integrator | Done | 24 runtime scenes, 20 distinct current events from the complete current projection |
| R3 | Restore greeting and Kaliningrad voice | integrator | Done | `greeting-day`, `local-keska` donor copy |
| R4 | Restore stretched designer О | integrator | Done | exact `b5f4797d` SVG and donor text-only placement |
| R5 | Deliver to existing focus/Autopresenter URL | integrator | In progress | overwrite `preview-20260729-focus-simple-r15-a5cc0256` after full gates |

Read-only discovery lanes:

- `hero_donor_mapper`: exact source/cursor/copy/glyph audit; no writes.
- `focus_url_mapper`: confirmed stable Autopresenter target and in-place preview deploy; no writes.

All writes are serialized in the existing clean integration worktree. The related-card contrast fix already in this branch is included in the same target build.
