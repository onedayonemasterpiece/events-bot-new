# Autopresenter first-test UX correction integration report

## Execution matrix

| ID | Requirement | Area | Primary lane | Dependencies | Done when |
|---|---|---|---|---|---|
| R01 | First installation and launch complete without pressing Enter | Windows bootstrap | bootstrap_auto | none | happy path has no prompt/read/pause and launches agent automatically |
| R02 | Demonstrator starts in real fullscreen | browser launch | stage_ux + integration | agent launch | browser chrome is absent and stage fills screen |
| R03 | Remove left explanatory UI; presentation must be visual and concise | stage UX | stage_ux | none | no persistent left panel/status/instruction copy in presentation |
| R04 | Desktop shows pressed keys; mobile shows tap circle, not cursor | interaction semantics | interaction_scenario | stage contract | scenario-mode-specific affordance is visible and tested |
| R05 | Tomorrow mobile scenario opens a concrete event and swipes to its description | Playwright scenario | interaction_scenario | real-site hooks | completed state occurs only after event detail/description is shown |
| R06 | Increase phone size | stage UX | stage_ux | none | mobile content is materially larger at 1920×1080 without clipping |
| R07 | Integrate, document, test and redeploy while keeping the same public links | release | integration_release | R01–R06 | exact deployed HEAD passes public E2E and ZIP refreshes |

## Lane status

| Lane | Requirement IDs | Status | Head SHA | Evidence |
|---|---|---|---|---|
| bootstrap_auto | R01 | merged | `a9067b54` | 3/3 bootstrap contract tests; `.codex/lanes/bootstrap_auto/RESULTS.md` |
| stage_ux | R02, R03, R06 | merged | `f9e2e8fa` | Astro build: 465 pages; 1920×1080 and responsive geometry checks; `.codex/lanes/stage_ux/RESULTS.md` |
| interaction_scenario | R04, R05 | merged | `61de8078` | 10/10 lane tests; `.codex/lanes/interaction_scenario/RESULTS.md` |
| integration_release | R07 | completed | `502fcfc7` | integrated tests, local live E2E, exact-HEAD public E2E, Fly deployment and refreshed ZIP |

## Integrated acceptance evidence

- Source candidate: `502fcfc7333aa350fbf45fd61e19fcc698fec3d6`.
- Agent tests: 11/11 passed, including the native fullscreen launch contract.
- Windows bootstrap tests: 3/3 passed.
- Relay tests: 8/8 passed, including the packaged scenario module.
- Static site: 465 pages built successfully.
- M0 compatibility harness: no diff; this release remains
  `FIRST_TEST_NOT_M3` and does not claim Windows compatibility PASS.
- Local 1920×1080 live run completed on event `5296`,
  `Концерт «Фестиваль Pianissimo: Жуан Нету Виейра»`, after the horizontal
  event-rail swipe and visible event-detail description.
- Public exact-HEAD headless run against
  `https://kenigevents-autopresenter.fly.dev` reached `completed` with the same
  event and both checkpoints: digest revealed after horizontal swipe; detail
  description visible.
- Public reset/run/stop lifecycle ended at `idle` with
  `agent confirmed stopped`.
- Fly image:
  `kenigevents-autopresenter:deployment-01KYPHJQY8R2PP7VE35B8JRQP0`;
  machine `2879209fd9e998` is started with 1/1 checks passing.
- Refreshed deterministic Windows archive contains 10 entries, including
  `agent/scenario-contract.mjs`; SHA-256:
  `a8841ceab7d33966a59e4a1f17474bd102eb467453a609507f769ae9097b0427`.

## Requirement closure

| ID | Status | Acceptance evidence |
|---|---|---|
| R01 | Done | Current-console QuickEdit is disabled and restored; PowerShell/npm run non-interactively; successful bootstrap flows directly into agent launch. |
| R02 | Done | Chromium starts with fullscreen flags and the agent requests native fullscreen through CDP, with document fullscreen as fallback. |
| R03 | Done | Persistent narrative/status/step/shortcut UI was removed; only the short presentation phrase remains. |
| R04 | Done | Mobile mode suppresses cursors and renders tap/swipe affordances; the separate desktop contract renders pressed keys plus UI response. |
| R05 | Done | Scenario deterministically swipes event `5296`, reveals `О событии`, opens its detail, and dwells on the real description before completion. |
| R06 | Done | Phone shell is enlarged and centered at the 1920×1080 acceptance viewport without clipping. |
| R07 | Done | Documentation and changelog are synchronized; the exact candidate is deployed and the unchanged public endpoints passed live acceptance. |

## Honest residual check

The automated and Linux-hosted acceptance is complete. The Windows-specific
QuickEdit and native headed-fullscreen behavior is implemented and covered by
contract tests, but the final empirical confirmation must occur on the owner's
Windows 10 laptop using a freshly downloaded archive. This does not upgrade the
separate M0 compatibility verdict.
