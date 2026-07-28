# Lane result: agent_scenario

## Scope

- Requirements: R03, R06, R07.
- Owned implementation: `tools/autopresenter/agent/**`.
- M0, site, relay, shared docs and changelog were not changed.

## Delivered

- One Node ESM headed Playwright agent for exactly `tomorrow-mobile`.
- Fixed 1920x1080 viewport, DPR=1 logging, and optional completion video plus
  pre-click PNG evidence via `AUTOPRESENTER_ARTIFACT_DIR`.
- Continuous relay long-polling while a run executes, command TTL and idempotent
  acknowledgement handling, heartbeat/state publishing, and the agreed relay API.
- Real `Locator.hover()` and `Locator.click()` flow after target visibility,
  scrolling and `boundingBox()` acquisition. Decorative in-frame cursor and ripple
  use `pointer-events:none`; no DOM activation is used.
- `/zavtra/` completion requires both the URL and stable page-ready marker.
- Stop is cooperative first and escalates after a bounded deadline to context
  replacement. `idle` is emitted only after the agent confirms the run stopped.
- Stage keyboard fallback: Space/Right Arrow run, Escape stop, R reset. The stage
  retains ownership of F/fullscreen.
- Agent states are mirrored into the visible stage via `presenter:status`; reset
  closes extra tabs before reopening the canonical stage.
- Local or centrally installed Playwright loading and SIGINT/SIGTERM cleanup so
  recorded WebM files finalize.

## Verification

Run from `tools/autopresenter/agent`:

```text
node --check agent.mjs                         PASS
npm test                                      PASS (7/7)
git diff --check                              PASS
```

The integrated live smoke is intentionally owned by the integrator because this
lane does not own the stage or relay worktrees.
