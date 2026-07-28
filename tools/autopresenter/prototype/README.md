# Visible Autopresenter prototype

This is the deliberately small M1 + M2-lite development slice. It runs exactly
one scenario, `tomorrow-mobile`, and does not modify or replace the M0
compatibility harness.

## Run

From the repository root:

```bash
./tools/autopresenter/prototype/start-dev.sh
```

Open the printed control URL:

```text
http://127.0.0.1:8787/control/
```

Press **Запустить «Завтра»**. A headed 1920×1080 stage opens the real site in a
430×932 iframe, moves the decorative cursor, performs real Playwright
`locator.hover()` and `locator.click()`, and confirms `/zavtra/`.

The first run installs the lockfile-pinned Node dependencies, the pinned
Playwright-managed browser and a small local Python environment if `aiohttp` is
not already available. Later launches reuse them. Set
`AUTOPRESENTER_SKIP_INSTALL=1` only after that bootstrap.

### Controls

- phone/web control: Run, Stop, Reset;
- local stage: `Space` or `Right Arrow` Run, `Esc` Stop, `R` Reset, `F` fullscreen;
- `Ctrl+C` in the launcher stops site, relay, agent and browser.

To expose the control page on a trusted development LAN, set
`AUTOPRESENTER_RELAY_HOST=0.0.0.0` and open the host's LAN address. This
prototype has no production authentication and must not be exposed publicly.

## Evidence

`AUTOPRESENTER_ARTIFACT_DIR=<absolute-directory>` makes the agent capture a
1920×1080 pre-click PNG with the cursor/ripple and record the stage as WebM.
The bounded reviewed PNG and MP4 shipped in `evidence/` are generated from the
integrated smoke run. Windows 10 portable M3 remains blocked by empirical M0.

- [1920×1080 screenshot](evidence/tomorrow-mobile-1920x1080.png)
- [full-scenario MP4](evidence/tomorrow-mobile.mp4)
- [smoke evidence and hashes](evidence/SMOKE.md)
