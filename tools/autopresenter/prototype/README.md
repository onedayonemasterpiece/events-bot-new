# Visible Autopresenter prototype

This is the deliberately small M1 + M2-lite development slice. It runs exactly
four fixed scenes in one persistent presentation window and does not modify or replace the M0 compatibility
harness:

- `tomorrow-mobile` — opens Tomorrow, reveals a rail description and event
  detail;
- `tomorrow-rail-like` — reveals a concrete event and continues the real rail
  gesture until its native like state is stored;
- `weekend-amber-artifact` — follows the mobile menu to Weekend and collects
  the existing amber artifact.
- `outro-qr` — shows a fullscreen survey QR from the existing immutable Yandex
  CDN path with strong presentation typography.

There is intentionally no generic scene DSL or editor.

## Run

From the repository root:

```bash
./tools/autopresenter/prototype/start-dev.sh
```

Open the printed control URL:

```text
http://127.0.0.1:8787/control/
```

Choose one of the four scene buttons. A headed fullscreen stage opens the
real site in an enlarged mobile frame. Every scenario shows tap circles and
swipe trails (never a mouse cursor), uses visible natural scrolling and waits
for the interface to settle before the next action.

Starting another scene does not close the presentation window: Run switches
within the same browser/context/page, Stop and Reset are non-terminal, and only
the confirmed Shutdown command closes the environment.

The first run installs the lockfile-pinned Node dependencies, the pinned
Playwright-managed browser and a small local Python environment if `aiohttp` is
not already available. Later launches reuse them. Set
`AUTOPRESENTER_SKIP_INSTALL=1` only after that bootstrap.

### Controls

- phone/web control: four Run buttons, Stop, Reset and confirmed
  **Закрыть презентацию**;
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
Internet and choose a scenario. The control can be installed as the PWA
**«Пульт презентации»** (short name **«Пульт»**). Its access survives an
installed-app relaunch on the same origin and can be removed explicitly with
**«Сбросить доступ на этом устройстве»**. The laptop needs only outbound
HTTPS; the phone and laptop do not share a LAN.

After testing, **«Закрыть презентацию»** asks for confirmation, closes the
fullscreen browser and terminates the Windows agent. A new test then requires
starting `START-DEMONSTRATOR.cmd` again.

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
