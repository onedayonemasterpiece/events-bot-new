# `tomorrow-mobile` integrated smoke

- Date: 2026-07-28 UTC
- Environment: Linux development host, headed Chromium under Xvfb
- Stage viewport: 1920×1080, DPR 1
- Embedded site viewport: 430×932
- Control URL: `http://127.0.0.1:8787/control/`
- Scenario result: `completed`
- Detail: `tomorrow-mobile: /zavtra/ ready`

## Verified flow

1. The control page button **Запустить «Завтра»** issued sequence 1.
2. The continuously polling agent accepted `tomorrow-mobile`.
3. The agent opened/reset the same-origin stage and found the unique
   `[data-presenter-id="nav-tomorrow"]`.
4. It called `scrollIntoViewIfNeeded()`, read `boundingBox()`, animated the
   pointer-events-none cursor, called real Playwright `locator.hover()`, showed
   the ripple and called real Playwright `locator.click()`.
5. Completion required iframe path `/zavtra/` and the unique
   `[data-presenter-id="tomorrow-page-ready"]`.
6. A separate control-page smoke confirmed Reset → `idle` and Run → Stop →
   agent-confirmed `idle`; polling remained active during the run.
7. Relay tests separately verified TTL expiry, monotonically increasing
   sequence, command-ID idempotency, long-poll wake-up and one live agent.

## Visual evidence

| File | Contract | SHA-256 |
|---|---|---|
| `tomorrow-mobile-1920x1080.png` | 1920×1080 pre-click frame with cursor/ripple | `c47eca1a2ec15ecf9e33e5b6662a8cca969147b561808493159addb4c02e9a0a` |
| `tomorrow-mobile.mp4` | 27 s, H.264, 1920×1080, 30 fps; idle → cursor/ripple → real click → completed `/zavtra/` | `2737cad3b94cf98c16317ac2c458aa49e2f985ba0a879fd14248ef2f31b69576` |

This is development evidence only. It is not target Windows 10 M0 evidence,
M3 portable-release evidence or a public-demo approval.
