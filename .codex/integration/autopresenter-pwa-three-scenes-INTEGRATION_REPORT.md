# Autopresenter PWA and three-scene integration report

## Execution matrix

| ID | Original requirement | Area | Primary lane | Dependencies | Done when |
|---|---|---|---|---|---|
| R01 | Make the phone control an installable PWA named «Пульт презентации» | control/PWA | pwa_control | none | install manifest, offline shell and exact app naming pass |
| R02 | Slow the show, wait for loading and use natural scroll before component targeting | agent motion | scenario_engine | pacing discovery | no swallowed scrolls; visible human-like scroll and explicit settle checkpoints |
| R03 | Add a second scenario that reveals an event through its rail and likes it with a further leftward gesture | agent/site | scenario_engine | like discovery, R02 | real UI like state changes only after the visible gesture |
| R04 | Add a third scenario that reaches and collects the amber artifact through real menu/date transitions | agent/site | scenario_engine | artifact discovery, R02 | deterministic real route and artifact found state complete |
| R05 | Add «Закрыть презентацию» to terminate everything | relay/agent/Windows | integration_release | R01–R04 | confirmed command closes browser and agent and leaves durable closed status |

## Interpretation decisions

- Exact install/display name is `Пульт презентации`; one-word `short_name` is
  `Пульт`.
- “Second” and “third” mean three separately selectable explicit scenarios,
  not one combined script and not a generic scenario DSL.
- “Like by pulling further left” means a visible continued leftward finger
  gesture followed by the real like control/state transition; no DOM-forced
  state.
- The artifact target is the existing amber artifact (“Янтарный космонавт”);
  its actual production route, date and gating condition must be discovered
  from code/data before implementation.
- Shutdown requires a confirmation on the phone because it ends the Windows
  agent and cannot be undone from that same disconnected session.
- The integrator owns the small relay protocol bridge that carries one of the
  three explicit scenario IDs from the PWA to the agent; this remains a fixed
  allowlist and is not a scenario DSL.
- Local live E2E found the existing personalization consent below the fixed
  mobile navigation (`z-index: 30` versus `40`). The integrator owns the narrow
  source/test correction because a real visible consent tap is part of R03;
  no feedback state is forced by the presenter.

## Lane status

| Lane | Requirement IDs | Status | Head SHA | Evidence |
|---|---|---|---|---|
| artifact_discovery | support R04 | completed | read-only | route, gates, selectors and storage contract mapped |
| like_discovery | support R03 | completed | read-only | native edge pull-to-like and durable acceptance mapped |
| pacing_discovery | support R02 | completed | read-only | readiness, natural scroll and bounded timing contract mapped |
| pwa_control | R01 | completed | `fa80be39` | 12 relay/PWA tests and 2 behavioral relaunch tests passed |
| scenario_engine | R02–R04 | completed | `55eccd11` (`5c5dc2d6` implementation) | 17 agent tests, real local runs for all three scenarios, persisted like/artifact assertions |
| integration_release | R05 | completed | `fb30ac6e` | deployed public relay; all three exact-source scenarios and terminal Shutdown passed |

## Local integration evidence

- Full immutable Astro preview built **465 pages** with
  `PUBLIC_SITE_MODE=preview`,
  `PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH=tail`, and
  `PUBLIC_PREVIEW_BUILD_ID=autopresenter-54`.
- `tomorrow-mobile` completed on real event `5296` through rail digest and full
  description.
- `tomorrow-rail-like` changed the native persisted like count from 4 to 5 and
  retained it after reload.
- `weekend-amber-artifact` collected deterministic event `6591`, retained the
  storage/ARIA state after reload, and opened exactly one detail dialog.
- Confirmed Shutdown produced durable relay state `closed`, closed the browser,
  and the agent process exited with code 0 after `remote-command`.
- Regression suites: agent 17/17, relay 13/13, PWA relaunch 2/2, targeted site
  contracts 16/16; syntax, `git diff --check`, and unchanged
  `tools/autopresenter/m0/**` checks passed.
- Bounded evidence is stored under
  `artifacts/codex/autopresenter-pwa-three-scenes/` and is intentionally not
  committed.

## Public owner-test release

- Source SHA: `fb30ac6eb98fbe49e8e653edc4a5aade91e3a44e`.
- Existing Fly app only; no new resources. Machine `2879209fd9e998`, release
  version 8, one `shared-cpu-1x` / 512 MB instance, health check passing.
- Image:
  `kenigevents-autopresenter:deployment-01KYPTPC96EKZMQKPNVVCN6XVT`
  (`sha256:f303487981728dca0eb7e18620988d224c886d5af9079b9f2c7a97b2647b66be`).
- Refreshed 12-entry Windows ZIP SHA-256:
  `fc3f2e76b576af66b8946424a5d75b74563f341e1693e0cdf13ae0efb2fc6265`.
  It contains `pacing.mjs`, all three scenario contracts and the corrected
  non-interactive shared-cache bootstrap.
- Public exact-source E2E completed all three scenarios with the real state
  assertions recorded above. Confirmed Shutdown left `closed`, the agent exited
  0, and agent stderr was empty.
- PWA manifest and public control expose exact names «Пульт презентации» /
  «Пульт», all three selectors and «Закрыть презентацию».
- Physical installation/relaunch on the owner's phone and cache reuse by the
  refreshed ZIP remain owner-device empirical checks, not CI claims.
