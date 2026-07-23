# R6 mobile acceptance integration report

Date: 2026-07-23

Branch: `integration/mobile-acceptance-r6-20260723`

Base: `5b237ae6d43e54cfe013cb804b705420f38bd6ff`

Deployed code: `da92ab4a`

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Transparent/glazed desktop tag edges inspected at DPR 1 and 2; no black side rail, stitching and bottom volume retained. |
| R02 | Done | Mobile club cards use the desktop dark full-media overlay hierarchy at 390px without overflow. |
| R03 | Done | Mobile event pages have no breadcrumb/back row; desktop breadcrumbs and structured data remain. |
| R04 | Done | Sticky ticket action hides once the related/footer boundary has passed; the valid earlier primary action remains. |
| R05 | Done | A wide native Share action renders icon plus `Поделиться`. |
| R06 | Done | Event `6667` resolves and renders the dedicated free-admission medallion on mobile without changing fail-closed identity matching. |
| R07 | Done | Mobile menu remains translucent with backdrop blur on light and dark media, plus a local logo halo. |
| R08 | Done | Android install CTA is gated by a real `beforeinstallprompt`, consumes one prompt, and clears on `appinstalled`. |
| R09 | Done | Immutable noindex R6 preview published; complete route catalog delivered and verified in Telegram topic `548`, message `616`. |

## Validation

- Astro export: 389 pages / 288 event pages.
- Static-site suite: 174/174 passed.
- `npm run check:preview`: passed.
- `npm run check:unified-prototype`: passed (18 primary routes, 39 hub links, 373 related cards).
- Public Playwright acceptance: passed at mobile 390px and desktop DPR 1/2; checked routes had no horizontal overflow or broken images.
- Synthetic Android installability smoke: CTA hidden before eligibility, shown after the event, prompt called once, then state cleared.
- External acceptance through agy `gemini-3.1-pro-high`: `GO`, `PASS 8/8`, no required changes.

## Publication and handoff

- Preview: <https://kenigevents.ru/preview-20260723-unified-corrections-r6/>
- Page-type map: <https://kenigevents.ru/preview-20260723-unified-corrections-r6/__preview/>
- Telegram: <https://t.me/c/4337049383/548/616>
- Telegram readback: exact body verified, `reply_to_msg_id=548`.
- Production was not changed.
