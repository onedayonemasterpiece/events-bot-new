# Autopresenter visible prototype integration

## Lane reconciliation

| Lane | Requirement IDs | Branch | Status | Lane head | Integration commits | Evidence |
|---|---|---|---|---|---|---|
| `stage_hooks` | R01, R02 | `agent/autopresenter-prototype/stage` | merged | `6d5e2ab5` | `b646e98f`, `3cdcbe91` | Astro build; exact 1920×1080 / 430×932 contract; stable hooks |
| `relay_control` | R04, R05 | `agent/autopresenter-prototype/relay` | merged | `c0bf129a` | `0847d228`, `8a66f7b3` | 6/6 aiohttp tests; manual HTTP/control smoke |
| `agent_scenario` | R03, R06, R07 | `agent/autopresenter-prototype/agent` | merged | `dec5d074` | `373f0ebb` plus integrator recorder/fullscreen fixes | 7/7 contract tests; integrated headed smoke |
| `integration_delivery` | R08, R09, R10 | `integration/autopresenter-prototype` | completed | final delivery commit | one-command launcher, docs, changelog and visual evidence | `tools/autopresenter/prototype/evidence/SMOKE.md` |

No lane was rejected, abandoned or left with an unmerged patch. All three
worker worktrees were clean at handoff.

## Requirement closure

| ID | Status | Closure evidence |
|---|---|---|
| R01 | Done | noindex same-origin 1920×1080 stage; exact 430×932 iframe; headed Chromium log reports viewport 1920×1080 and DPR 1 |
| R02 | Done | exact `nav-tomorrow` and `tomorrow-page-ready` hooks |
| R03 | Done | only `tomorrow-mobile`; target visibility/scroll/bounding box, cursor, real hover, ripple, real click and destination assertion |
| R04 | Done | responsive control page with exact Run/Stop/Reset labels and required state vocabulary |
| R05 | Done | one in-memory aiohttp relay/session/agent, long-poll, IDs, sequence, TTL and idempotency; no DB/Socket.IO |
| R06 | Done | non-awaited runner keeps poll live; separate UI smoke confirms Reset and agent-confirmed Stop; bounded context hard recovery |
| R07 | Done | Space/Right Arrow, Escape, R bridge; stage F fullscreen |
| R08 | Done | no M0 diff; one scenario; prohibited platform/product expansion absent |
| R09 | Done | one command, exact URL, reviewed 1920×1080 PNG, 27 s complete MP4, smoke record and file tree |
| R10 | Done | docs explicitly allow supported dev-OS preview while keeping M3/public demo blocked |

## Integrated acceptance evidence

- Launch: `./tools/autopresenter/prototype/start-dev.sh`
- Control: `http://127.0.0.1:8787/control/`
- Actual control-button run: `idle → running → completed`
- Completion: `tomorrow-mobile: /zavtra/ ready`
- Reset/Stop smoke: `completed → reset/idle → run → stop → agent confirmed stopped/idle`
- Screenshot: 1920×1080, cursor and ripple visually inspected
- MP4: H.264, 1920×1080, 30 fps, 27 s; start, cursor/ripple, click and completed destination frames visually inspected
- Static agent tests: 7/7
- Relay tests: 6/6
- Astro static build: passed
- DOM activation guard: no `element.click()`/`node.click()` in the prototype
- M0 boundary: no file below `tools/autopresenter/m0/` changed

## Remaining gates

- M0 empirical execution on the target Windows 10 laptop is still pending.
- M3 portable ZIP, target-laptop rehearsal and `GO_FOR_PUBLIC_DEMO` remain blocked.
