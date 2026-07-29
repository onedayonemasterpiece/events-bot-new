# Lane pwa_control Results

## Status
committed

## Requirement IDs
- R01 — installable authenticated phone control PWA

## Branch
`agent/autopresenter-pwa-three-scenes/pwa-control`

## Worktree
`/home/dev/projects/events-bot-new-autopresenter-pwa-control`

## Base SHA
`edce9daf9d5e5e6ea9e2041187ae2726db5c62a9`

## Head SHA
`e89ecabb0b41454017e705f5d76fb6486be3fa42` (implementation commit; the committed lane record is a results-only follow-up)

## Files changed
- `tools/autopresenter/relay/control/index.html`
- `tools/autopresenter/relay/control/manifest.webmanifest`
- `tools/autopresenter/relay/control/service-worker.js`
- `tools/autopresenter/relay/control/icons/generate_icons.py`
- `tools/autopresenter/relay/control/icons/icon-source.svg`
- `tools/autopresenter/relay/control/icons/icon-192.png`
- `tools/autopresenter/relay/control/icons/icon-512.png`
- `tools/autopresenter/relay/control/icons/icon-maskable-512.png`
- `tools/autopresenter/relay/server.py`
- `tools/autopresenter/relay/tests/test_server.py`
- `.codex/lanes/pwa_control/RESULTS.md`

## Commands run
- `python3 tools/autopresenter/relay/control/icons/generate_icons.py`
- `python3 tools/autopresenter/relay/control/icons/generate_icons.py --check`
- `file tools/autopresenter/relay/control/icons/*.png`
- `sha256sum tools/autopresenter/relay/control/icons/*.png`
- `uv run --with aiohttp==3.14.1 python -m unittest discover -s tools/autopresenter/relay/tests -v`
- `node --check tools/autopresenter/relay/control/service-worker.js`
- `python3 -m compileall -q tools/autopresenter/relay/server.py tools/autopresenter/relay/tests/test_server.py tools/autopresenter/relay/control/icons/generate_icons.py`
- `python3 -m json.tool tools/autopresenter/relay/control/manifest.webmanifest`
- `git diff --check`

## Tests / verification
- Relay server/package/PWA suite: **11 passed**.
- Manifest contract verifies exact `name` «Пульт презентации», one-word `short_name` «Пульт», `/control/` start/scope, standalone display, matching theme/background colors, and 192/512 any plus 512 maskable PNG icons.
- PWA control contract verifies fragment-token ingestion, `sessionStorage` auth, Authorization header use, fragment removal, and scoped service worker registration remain intact; `localStorage` is not introduced.
- Service worker contract verifies fixed token-free app-shell caching only. `/api/*`, non-GET, cross-origin, and query-variant requests bypass CacheStorage; API responses retain `Cache-Control: no-store`.
- PNG dimensions and signatures verified through served responses. The deterministic generator reproduced the committed bytes under `--check`; the 512 icon was also visually inspected.
- Docker package coverage test confirms the existing recursive relay COPY/.dockerignore rules include all PWA assets.
- No R05 shutdown control was added; the PWA test explicitly guards against «Выключить» UI.

## Risks
- Authentication deliberately remains page-session scoped. A newly opened standalone app session without the original fragment onboarding context shows the existing missing-key state and requires reopening the issued `#token=...` link; no token is persisted in manifest, service-worker caches, or durable browser storage.
- Offline support is intentionally app-shell-only. Commands/state still fail closed without relay connectivity and are never replayed from cache.
- Installability metadata/routes were covered by server tests, but no physical phone/production-HTTPS installation rehearsal was performed in this lane.
- Canonical feature documentation and `CHANGELOG.md` are outside this lane's write ownership and must be updated by the integrator with the combined R01–Rxx behavior.

## Merge notes
- Cherry-pick implementation commit `e89ecabb0b41454017e705f5d76fb6486be3fa42`, then the following results-only commit if lane evidence is desired in the integration branch.
- New public asset URLs are `/control/manifest.webmanifest`, `/control/service-worker.js`, and `/control/icons/{icon-192.png,icon-512.png,icon-maskable-512.png}`. Existing `/control`, `/control/`, `/demonstrator`, `/demonstrator/`, and API URLs are unchanged.
- `cors_middleware` now uses `setdefault` for `Cache-Control`: explicit app-shell caching policies survive, while all API/default/exception responses remain `no-store`.
- The service worker scope is restricted to `/control/`; R05 shutdown UI/API can be integrated separately without modifying this lane's auth or cache contract.
