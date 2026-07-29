# Lane results: autopresenter-pwa-20260729-b

- **Lane:** `autopresenter-pwa-20260729-b`
- **Requirements:** R10, R25
- **Status:** Done
- **Base SHA:** `23e6be76b4e04d79cb3dc1004715790df3cfff92`
- **Implementation head SHA:** `8dd864971e306f7d6608e7ed1fc9edbdf0c70556`

## Delivered

- Added a compact fixed right-side `Экран` rail in the phone PWA with accessible up/down nudge controls.
- Added relay protocol action `scroll` with a strict `up|down` direction and integer amount bounded to 120–1200 px (420 px default/PWA nudge).
- Kept the active scene command, selection, status, and detail intact while a scroll command is issued and acknowledged, so manual scrolling does not replace or end the scene.
- Clarified operator-facing copy for the mobile-menu scenes: the menu is shown with a pause before choosing `Выходные` or `Завтра`.
- Kept the installed PWA name to the requested single word `Пульт` and bumped the shell cache version.

## Changed files

- `tools/autopresenter/relay/control/icons/icon-source.svg`
- `tools/autopresenter/relay/control/index.html`
- `tools/autopresenter/relay/control/manifest.webmanifest`
- `tools/autopresenter/relay/control/service-worker.js`
- `tools/autopresenter/relay/server.py`
- `tools/autopresenter/relay/tests/test_server.py`

## Evidence / commands

```text
uv run --with 'aiohttp>=3.9.5' python -m unittest discover -s tools/autopresenter/relay/tests -p 'test_*.py' -v
# 16 tests: OK

node --test tools/autopresenter/relay/tests/*.test.mjs
# 2 tests: pass

git diff --check
# clean
```

Targeted tests cover accepted/rejected scroll payloads, default/bounds, and preservation of a completed active scene after both issue and acknowledgment. Static control-page assertions cover the rail, accessible labels, menu pacing copy, and one-word PWA name.

## Integration dependency / risk

- The relay now delivers `action: "scroll"`; the integration lane must add the corresponding browser-agent executor. Until that disjoint agent-side change is merged, the relay/PWA contract is intentionally present but the demonstrator cannot move the live page.
- This lane did not edit or test browser-agent, stage, site, docs, changelog, or M0 files by ownership rule.
