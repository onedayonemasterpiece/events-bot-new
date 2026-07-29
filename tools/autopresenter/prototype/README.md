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

Press **Запустить «Завтра»**. A headed fullscreen stage opens the real site in
an enlarged mobile frame. The scenario shows a tap circle (never a mouse
cursor), opens `/zavtra/`, horizontally reveals one concrete event's
**О событии** digest, opens its detail page, and dwells on the real description
before reporting completion.

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

## First Internet test

The owner-facing first test does **not** use the development URL above.
It uses the separately deployed HTTPS service:

```text
PHONE:        https://kenigevents-autopresenter.fly.dev/control/#token=<secret>
DEMONSTRATOR: https://kenigevents-autopresenter.fly.dev/demonstrator/#token=<secret>
```

Open `DEMONSTRATOR` on the Windows x64 laptop, download and extract the ZIP,
then double-click `START-DEMONSTRATOR.cmd`. Open `PHONE` on a phone over mobile
Internet and press **Запустить «Завтра»**. The laptop needs only outbound HTTPS;
the phone and laptop do not share a LAN.

The first successful start downloads portable Node.js, lockfile-pinned
dependencies and the pinned Playwright-managed browser into the persistent
Windows user cache
`%LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1`. Compatible later debug
ZIPs reuse that cache instead of reinstalling the same stack. It changes
QuickEdit only for that console window when the console API is available and
is non-interactive: no Enter confirmation is part of the happy path, and the
fullscreen stage opens automatically. This is an online first-test bootstrap,
not the final hermetic M3 package and not M0 compatibility evidence.

## Evidence

`AUTOPRESENTER_ARTIFACT_DIR=<absolute-directory>` makes the agent capture a
1920×1080 final-description PNG and record the stage as WebM.
The bounded reviewed PNG and MP4 shipped in `evidence/` are generated from the
integrated smoke run. Windows 10 portable M3 remains blocked by empirical M0.

- [1920×1080 screenshot](evidence/tomorrow-mobile-1920x1080.png)
- [full-scenario MP4](evidence/tomorrow-mobile.mp4)
- [smoke evidence and hashes](evidence/SMOKE.md)
