# Autopresenter service scenes — integration report

- Branch: `integration/autopresenter-service-scenes`
- Handoff commit: `6b50cc73`
- Existing Fly app: `kenigevents-autopresenter`
- Fly machine version: `14` (`shared-cpu-1x`, `512 MB`, `ams`)
- Telegram handoff: chat `4337049383`, reply to `803`, delivered message `859`
- Public branch: <https://github.com/onedayonemasterpiece/events-bot-new/tree/integration/autopresenter-service-scenes>

No new Fly application, database, queue, bucket or CDN resource was created. Existing
Yandex CDN assets and the existing single Fly machine are reused. Protected control and
demonstrator fragments are intentionally not stored in Git.

## Requirement closure

| ID | Status | Result |
|---|---|---|
| R01 | Done | Telegram remarks 803–850 are consolidated at the bottom of the author scenario with message traceability. The text above the appended section is the user's updated source copied from the active worktree, not an integrator rewrite. |
| R02 | Done | Accepted scene IDs are declared in `SCENE_ACCEPTANCE_CONTRACT`; direct accepted method/markup SHA-256 drift fails `accepted-scene-freeze.test.mjs`. Draft verification stays targeted; intentional changes require reopening acceptance. |
| R03 | Done | Intro ports the historical Hero Talk semantic-fragment reveal, active-fragment cursor, two-line composition and short transition rhythm. |
| R04 | Done | Focus-preview base and exact invitation fragment are pinned; readiness proves the fragment was accepted into participation state. |
| R05 | Done | Plain «Анонсы» transitions into the exact vector; the exact branded «о» over-stretches and ease-in-out settles to its final vector size. |
| R06 | Done | Find/share/calendar beats and animated product icons are explicit. |
| R07 | Done | Meaning-first desktop and mobile medallion scenes use a real focus-preview event; desktop shows horizontal hero, top medallion and naturally scrolls to two inline medallions. |
| R08 | Done | First joke line types, then pauses exactly seven seconds, then reveals the follow-up. |
| R09 | Done | PWA supplies a bounded query; the phone scene visibly types it. Submission occurs when the focus-preview session is signed in; the real sign-in gate is preserved otherwise. |
| R10 | Done | Disruption, taste, feedback, focus-group, NPS and future-celebrity beats are explicit held scenes. |
| R11 | Done | One browser/context/page/window persists across Run/Stop/Reset; only Shutdown closes it. Fresh Windows ZIP contains the current contracts. |
| R12 | Partial | Targeted suites, focused live E2E, remote full Astro build, deploy, public smoke and fresh ZIP integrity pass. Full all-scenes rehearsal and target Windows owner-laptop acceptance remain intentionally deferred until the author document is final. |
| R13 | Done | PWA status/detail shelf and sticky timer use stable reserved geometry. |
| R14 | Done | Seven lecture frames are seven separately controlled held scenes; no autoplay. |
| R15 | Done | Sticky elapsed/countdown timer, intro start time and periodic countdown copy are implemented. |
| R16 | Done | Lecture layouts/themes vary; horizontal media is contained and the Znanie mark is transparent and contrast-aware. |
| R17 | Done | Service frames retain strong meaning-first typography; Weekend and desktop medallion demos are two-phase. |

## Verification evidence

- Node agent/stage/PWA suites: `38/38` passed.
- Relay Python suite: `14/14` passed.
- Accepted-scene freeze gate: passed.
- Targeted live 1920×1080 E2E: wordmark, joke, desktop/mobile medallions,
  focus invitation and sequential same-page switching passed.
- Remote Docker full Astro build: `465` pages, completed in `1m 33s`.
- Fly deploy image: `deployment-01KYQJ1EC8ZA2W4TC5R86A85P3`; health check passing.
- Public `/healthz`, control shell, demonstrator shell and stage: HTTP 200.
- Public PWA smoke: title/manifest, `27` explicit scene buttons, sticky timer and Shutdown present.
- Fresh Windows ZIP: `14` files, archive integrity passed, deployed agent contract verified.
- Protected links delivered to Telegram message `859` (reply to `803`).

## Release boundary

The deployed owner-only Internet test is ready for the next Windows/phone iteration.
Public demonstration remains `NO-GO` until the target Windows M0 evidence and a final
continuous rehearsal are completed.
