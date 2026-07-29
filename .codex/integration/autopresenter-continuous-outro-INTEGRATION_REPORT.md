# Autopresenter continuous environment and QR outro integration report

## Requirement matrix

| ID | Original requirement | Primary lane | Done when |
|---|---|---|---|
| R01 | Diagnose and fix the screenshot error posted in Telegram | integration_release | exact error is recorded and its owning regression passes |
| R02 | Starting another scenario must not close the unified presentation window | persistent_runtime | sequential Run commands reuse one live browser; only Shutdown exits |
| R03 | The environment must support hour-long mixed text/image/stat/site scenes | persistent_runtime | no per-scenario process lifecycle or short global session cap |
| R04 | Presentation media lives in the existing Yandex bucket and is served through CDN | integration_release | QR is uploaded without new infrastructure and public CDN GET passes |
| R05 | Add a beautiful fullscreen survey QR outro | outro_scene | selectable scene renders real CDN QR at presentation scale |
| R06 | Replace non-presentation labels with strong fullscreen animated typography | outro_scene | outro has concise large type and restrained motion, not a dashboard panel |

## Discovery

- Telegram message `820` shows the exact owner-device failure:
  `tomorrow-rail-like exceeded 30000ms`. The unconditional 30-second agent
  ceiling is the root cause; scenario button 01 was also misleadingly styled
  as selected.
- Scenarios 02 and 03 requested `freshContext`, closing the only headed
  BrowserContext/page/window before opening a replacement.
- The supplied QR is 1155×1155 PNG, 7,604 bytes, SHA-256
  `916b6fee58256c4f2111887bf70c502070a55e45a667650dfccdb1495016ccd9`.
- It was uploaded to the existing `kenigevents.ru` bucket and existing
  `static.kenigevents.ru` CDN under an immutable content-addressed key. No
  bucket, CDN, machine or other infrastructure was created.

## Lane status

| Lane | Requirement IDs | Status | Head | Evidence |
|---|---|---|---|---|
| telegram_error_discovery | support R01 | completed | read-only | message 820 screenshot and exact timeout captured |
| runtime_discovery | support R02/R03 | completed | read-only | freshContext/window and timeout lifecycle mapped |
| media_discovery | support R04 | completed | read-only | existing bucket/CDN/key and cache contract mapped |
| persistent_runtime | R02/R03 | merged | `12de70bc` (`ad7bab77` implementation) | agent 21/21, relay 13/13, PWA 2/2, bootstrap 4/4 |
| outro_scene | R05/R06 | merged | `036c587c` (`16652bf0` implementation) | focused 6/6, agent 20/20, CDN hash/dimensions |
| integration_release | R01/R04 | completed | `dddc98fc` | existing Fly app + existing Yandex CDN; four-scene public E2E and Telegram 827 |

## Integrated local verification

- Runtime and control expose exactly four explicit scenes; no DSL/editor.
- Sequential Run regression keeps one context generation and one page; Stop
  and Reset remain non-terminal, Shutdown remains the only terminal action.
- Explicit timeout policy is 120/120/120/30 seconds and validates a future
  explicit timeout up to one hour.
- Agent `25/25`, relay `13/13`, control auth `2/2`, Windows bootstrap `4/4`,
  presenter-stage `3/3`.
- Astro immutable preview build: 465 pages, including presenter stage.
- Visual QA: 1920×1080, zero overflow/errors, strong fullscreen type, loaded
  504×504 QR from the exact 1155×1155 immutable CDN asset.

## Release evidence

- Public branch:
  `https://github.com/onedayonemasterpiece/events-bot-new/tree/feature/autopresenter-design`
- Fly release 10:
  `deployment-01KYPYAHH0EF1JMXNVN1KRKQW9`, one shared CPU, 512 MB, 1/1 checks.
- Windows ZIP: 13 entries, SHA-256
  `ecb0b467ce0b72b068ab2d27b6612b1302544c06af63a55a998ac63ef871e252`.
- Exact-source public run: active scenario switch passed; all four scenes
  completed and captured; one context generation; zero stderr; Shutdown exit 0
  and durable `closed`.
- Telegram: verified reply `827` to message `803` contains PHONE,
  DEMONSTRATOR and GitHub branch links.
- Incidents remain open only for owner Windows/cache-reuse evidence and
  `origin/main` reachability; public demo/M3 verdict remains NO-GO.
