# Hero Talk focus-link integration — 2026-07-30

| ID | Requirement | Owner | Status | Evidence |
|---|---|---|---|---|
| R1 | Exactly one cursor at the true end | integrator | Done | explicit `[data-home-hero-cursor]`; browser gate asserts one per scene/active scene |
| R2 | More phrases and events | integrator | Done | 24 runtime scenes, 20 distinct current events from the complete current projection |
| R3 | Restore greeting and Kaliningrad voice | integrator | Done | `greeting-day`, `local-keska` donor copy |
| R4 | Restore stretched designer О | integrator | Done | exact `b5f4797d` SVG and donor text-only placement |
| R5 | Deliver to existing focus/Autopresenter URL | integrator | Done | rebuilt and overwrote `preview-20260729-focus-simple-r15-a5cc0256`; public verification and browser gates passed |

Read-only discovery lanes:

- `hero_donor_mapper`: exact source/cursor/copy/glyph audit; no writes.
- `focus_url_mapper`: confirmed stable Autopresenter target and in-place preview deploy; no writes.

All writes are serialized in the existing clean integration worktree. The related-card contrast fix already in this branch is included in the same target build.

Release evidence:

- source SHA `ca5b6d41`;
- Astro: 1304 pages;
- preview and unified-output gates passed;
- local and public Hero/date donor Playwright passed;
- local and public desktop related-share contrast Playwright passed;
- public root measured 24 scenes, 20 distinct event scenes, one active cursor,
  one wide-`О` layer and zero horizontal overflow.
