# Lane results: interaction_scenario

## Scope

- Lane ID: `interaction_scenario`
- Requirement IDs: `R04`, `R05`
- Base SHA: `e634403db817b3ac4c7fed4e5781f0a13ad0de2b`
- Implementation head SHA: `61de8078c26f5ddeaaa056d97669b6dac9bf5d51`
- Branch: `agent/autopresenter-first-test-ux/interaction-scenario`
- Status: implemented and committed; live harness blocked by host disk exhaustion

## Delivered

- Mobile interaction mode now suppresses the browser cursor in both the stage and embedded page, renders a visible tap circle for activation, and renders a directional swipe trail that distinguishes leftward finger motion from rightward navigation through event content.
- Desktop interaction mode has a separate pressed-key overlay and updates its UI-response label from `presenter:status` or `presenter:desktop-ui-response`; the desktop overlay is never mounted in the mobile scenario.
- `tomorrow-mobile` now:
  1. opens `/zavtra/` through the existing unique presenter hooks;
  2. deterministically selects the visible mobile event with the fewest rail images, then lowest numeric event ID;
  3. horizontally drags the event rail until its `О событии` digest has at least 72% horizontal visibility;
  4. visibly highlights/dwells on that digest for 2.2 seconds;
  5. opens the concrete event detail and visibly highlights/dwells on its mobile description for 2.2 seconds;
  6. only then returns completion evidence and acks with the concrete event ID/title plus both description checkpoints.
- Stop/reset/TTL/idempotent ack/outbound bearer auth paths were preserved and remain covered by agent contract tests.

## DOM inspection and locator choice

The real site has unique presenter hooks only for the initial route transition:

- `[data-presenter-id="nav-tomorrow"]`
- `[data-presenter-id="tomorrow-page-ready"]`

The event continuation uses existing semantic production hooks:

- `[data-mobile-v23-page="tomorrow"] [data-mobile-listing-row][data-event-id]`
- `.rail-window`
- `.event-digest[aria-label="О событии"]`
- `[data-mobile-event-production] .mobile-event-production__prose`

These are substantially safer than visible-text-first locators and let the agent assert event identity and description visibility. There are no dedicated presenter hooks for the event candidate, rail digest, or detail description.

### Exact integration request (non-blocking)

For a stronger long-term presenter contract, the site lane should expose exactly one `data-presenter-id="tomorrow-event-candidate"` on the canonical deterministic mobile row and add stable hooks for `event-description-rail` and `event-detail-description`. Until then, the existing semantic data hooks above are the smallest robust in-scope locator path.

## Verification

### Passing

- `node --check tools/autopresenter/agent/agent.mjs` — PASS
- `npm test` in `tools/autopresenter/agent` — PASS, 10/10 tests
- `git diff --check` — PASS

The tests exercise the pure deterministic candidate selector and assert the semantic order: route ready -> concrete event selection -> horizontal reveal -> rail dwell -> detail open -> detail dwell -> completion. They also assert mobile cursor suppression/tap/swipe visuals, desktop pressed keys/UI response, and the unchanged stop/reset/TTL/auth contracts.

### Live harness blocker evidence

A headed/public-style live run was attempted against the real Astro presenter stage in the sibling `stage-ux` worktree. It could not provide valid scenario evidence because the Astro/Vite runtime repeatedly logged:

`Failed to write to output file: .../site/node_modules/.vite/deps_temp_a69110de/...: no space left on device`

Observed consequences were a Playwright target crash, an externally terminated run (`143`), and finally a timeout waiting for the stage-ready marker after the server's dependency optimizer had failed. `df -h` showed `/dev/vda2` at `100%` with about `83M` available. The dev server was stopped. This is a host/runtime-capacity blocker, not a semantic contract test failure; the integration owner must rerun live E2E after freeing disk.

After two touch-oriented failures, targeted official Playwright research confirmed that touchscreen APIs require a `hasTouch` context and use main-frame CSS coordinates. The implementation therefore retains the already-proven browser action model (real Playwright locator click and horizontal drag) while CSS suppresses the pointer and the scenario renders explicit mobile tap/swipe affordances.

## Risks and merge notes

- Live scenario acceptance remains required after host disk space is restored.
- The agent currently relies on stable non-presenter data hooks for the event continuation; see the exact integration request above.
- Merge after the `stage_ux` lane and rerun the full scenario because that lane changes phone geometry. This implementation uses locator bounding boxes rather than fixed coordinates, so the larger phone should not require code changes.
- No files outside `tools/autopresenter/agent/**` and this lane report were modified.
- No push was performed.

## Changed files

- `tools/autopresenter/agent/README.md`
- `tools/autopresenter/agent/agent.mjs`
- `tools/autopresenter/agent/scenario-contract.mjs`
- `tools/autopresenter/agent/test/static-contract.test.mjs`
- `.codex/lanes/interaction_scenario/RESULTS.md`
