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
│   └── tests/
└── prototype/
    ├── start-dev.sh        one-command development launcher
    ├── README.md
    └── evidence/
        ├── SMOKE.md
        ├── tomorrow-mobile-1920x1080.png
        └── tomorrow-mobile.mp4
```

The visible slice runs exactly one scenario, `tomorrow-mobile`. Start at
[`prototype/README.md`](prototype/README.md). The M0 target-laptop procedure
remains at [`m0/README.md`](m0/README.md).
