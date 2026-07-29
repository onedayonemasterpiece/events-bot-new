# Autopresenter

Two independent tracks live here:

```text
tools/autopresenter/
├── m0/                     Windows 10 compatibility harness (unchanged)
├── agent/                  one headed Playwright agent
│   ├── agent.mjs
│   └── test/
├── relay/                  one-process aiohttp relay
│   ├── server.py
│   ├── control/
│   ├── Dockerfile.internet-test
│   ├── fly.internet-test.toml
│   └── tests/
└── prototype/
    ├── start-dev.sh        one-command development launcher
    ├── first-test/         Windows online-bootstrap templates
    ├── README.md
    └── evidence/
        ├── SMOKE.md
        ├── tomorrow-mobile-1920x1080.png
        └── tomorrow-mobile.mp4
```

The visible slice runs only explicit scenes: `intro-loop`, seven independently
held `lecture-01`…`lecture-07` frames, the service-presentation frames,
`tomorrow-mobile`, `tomorrow-rail-like`, `weekend-amber-artifact`, the
meaning-first full-FHD `weekend-desktop`, and fullscreen `outro-qr`.
They share one browser/context/page/window until terminal Shutdown. It
deliberately has no generic scenario DSL. Start at
[`prototype/README.md`](prototype/README.md).
The M0 target-laptop procedure remains at [`m0/README.md`](m0/README.md).

The first-test Internet path is deliberately separate from the final M3
portable release:

- phone and agent use outbound HTTPS to the single-instance Fly test relay;
- `/control/` is protected by a control bearer token carried in the URL
  fragment, never in the request URL;
- that control is an installable PWA named `Пульт презентации` (`Пульт` as its
  one-word short name); it persists same-origin authorization across installed
  app relaunches and offers an explicit device-access reset;
- agent polling/status uses a different bearer token;
- `/demonstrator/` creates a scoped Windows x64 ZIP with one
  `START-DEMONSTRATOR.cmd`;
- the bootstrap is non-interactive, treats per-console QuickEdit setup as
  best-effort, and forces the browser into fullscreen;
- the stage supports a larger mobile phone, strong fullscreen intro/lecture/
  service/outro typography, and a two-phase FHD Weekend desktop page; mobile
  actions use tap/swipe affordances;
- scenario pacing uses visible natural scroll/drag motion, readiness checks and
  bounded dwell instead of instant component jumps;
- real mobile date menus remain open for 2.2 seconds before the selected item
  is tapped, and the PWA right-hand ↑/↓ strip can nudge the visible page without
  stopping the active scenario;
- current live routes are normalized relative to the pinned focus-preview
  prefix; only the artifact scene uses the explicitly documented R15 candidate
  because the focus build has its artifact feature flag disabled;
- Smart Search requires a one-time login to a separate non-privileged demo
  account, persists browser storage as `browser-state-v1.json` beside the
  shared cache, and accepts success only when real event cards are visible;
- the same headed window survives sequential Run, Stop and Reset commands;
  a new Run cooperatively switches the active scene, and explicit timeout
  policy admits a future scene lasting up to one hour;
- the intro logo/music, seven lecture images, and QR outro use content-addressed
  immutable assets from the existing `static.kenigevents.ru` Yandex CDN;
- the user-provided joke narration and the licensed CC0 error sting use the
  same content-addressed CDN workflow documented by the project
  `autopresenter-audio-cues` skill;
- the confirmed `Закрыть презентацию` action closes the browser, terminates the
  Windows agent and leaves a durable `closed` status;
- the first successful test stores versioned Node, lockfile-keyed dependencies
  and the Playwright-managed browser under
  `%LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1`; compatible later debug
  ZIPs reuse them. It is not hermetic M0/M3 evidence.
