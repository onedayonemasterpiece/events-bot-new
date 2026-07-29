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

The visible slice runs exactly one scenario, `tomorrow-mobile`. Start at
[`prototype/README.md`](prototype/README.md). The M0 target-laptop procedure
remains at [`m0/README.md`](m0/README.md).

The first-test Internet path is deliberately separate from the final M3
portable release:

- phone and agent use outbound HTTPS to the single-instance Fly test relay;
- `/control/` is protected by a control bearer token carried in the URL
  fragment, never in the request URL;
- agent polling/status uses a different bearer token;
- `/demonstrator/` creates a scoped Windows x64 ZIP with one
  `START-DEMONSTRATOR.cmd`;
- the bootstrap is non-interactive and temporarily disables QuickEdit for its
  own console, while the browser is forced into fullscreen;
- the clean stage centers a larger phone; mobile actions use tap/swipe
  affordances, while the separate desktop contract visualizes pressed keys;
- the first test downloads pinned Node dependencies and the
  Playwright-managed browser into the extracted directory. It is not hermetic
  M0/M3 evidence.
