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

## Lane status

| Lane | Requirement IDs | Status | Head SHA | Evidence |
|---|---|---|---|---|
| artifact_discovery | support R04 | planned | — | pending |
| like_discovery | support R03 | planned | — | pending |
| pacing_discovery | support R02 | planned | — | pending |
| pwa_control | R01 | planned | pending | pending |
| scenario_engine | R02–R04 | planned | pending | pending |
| integration_release | R05 | planned | pending | pending |
