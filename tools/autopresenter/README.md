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

The visible slice runs exactly three explicit scenarios: `tomorrow-mobile`,
`tomorrow-rail-like`, and `weekend-amber-artifact`. It deliberately has no
generic scenario DSL. Start at [`prototype/README.md`](prototype/README.md).
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
- the clean stage centers a larger phone; mobile actions use tap/swipe
  affordances, while the separate desktop contract visualizes pressed keys;
- scenario pacing uses visible natural scroll/drag motion, readiness checks and
  bounded dwell instead of instant component jumps;
- the confirmed `Закрыть презентацию` action closes the browser, terminates the
  Windows agent and leaves a durable `closed` status;
- the first successful test stores versioned Node, lockfile-keyed dependencies
  and the Playwright-managed browser under
  `%LOCALAPPDATA%\KenigEvents\Autopresenter\cache-v1`; compatible later debug
  ZIPs reuse them. It is not hermetic M0/M3 evidence.
