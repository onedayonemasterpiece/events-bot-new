# Autopresenter iteration 2026-07-29 B — integration report

## Delivery

- Branch: `integration/autopresenter-iteration-20260729-b`
- Public source: <https://github.com/onedayonemasterpiece/events-bot-new/tree/integration/autopresenter-iteration-20260729-b>
- Runtime: the existing `kenigevents-autopresenter` Fly app; no new Fly/Yandex resources.
- Scope rule: accepted scenes were left unchanged unless R01–R25 required a correction.
- Fly release: machine version `15`, image
  `deployment-01KYQSQHZVJ6T6DKYV1B121F8J`, health check passing.
- Windows owner-test ZIP SHA-256:
  `151be93e67ac577f762171f5f904b68a16f3a638ea8122e34e2000aaf96c22c4`.

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Hero Talk uses bounded, varied character/word pauses and a slower readable rhythm. |
| R02 | Done | Intro fragments preserve explicit spaces; stage contract test covers both lines. |
| R03 | Done | Countdown copy is exactly «Вот-вот / начинаем». |
| R04 | Done | Lecture media has no decorative stretch fields, rings, or image-frame shadows. |
| R05 | Done | All seven 1920×1080 lecture frames were screenshot-reviewed. |
| R06 | Done | Lecture statement types first; media then enters. |
| R07 | Done | «Видеть главное» reserves clearance for the «Знание» mark. |
| R08 | Done | Final lecture scheme occupies about half the FHD frame at readable scale. |
| R09 | Done | Search/share/calendar use the static-site icon assets/components. |
| R10 | Done | Mobile menu is opened, held visibly, then its exact item is tapped. |
| R11 | Done | Artifact journey performs menu navigation, real rail drags, collection, storage/reload and dialog verification on the current checked R15 candidate. |
| R12 | Done | Sixteen real medallions enter large, then settle into a 4×4 grid. |
| R13 | Done | Supplied joke audio is on content-addressed Yandex CDN and plays with the second typed line; service words are absent. |
| R14 | Done | R01–R25 are appended as a consolidated clarification section in the canonical scenario document. |
| R15 | Done | Presentation frames use a slowly animated soft halo with reduced-motion fallback. |
| R16 | Blocked | Real search submission/results are implemented, but require one-time sign-in to the separate demo account. No credentials/session were supplied; the scene now fails explicitly instead of pretending to search. |
| R17 | Done | A provenance-first reusable audio-cue skill was added; the error scene plays a licensed CC0 cue from Yandex CDN. |
| R18 | Done | Live journeys use the exact current focus build; the artifact-only exception is documented because that build ships the feature disabled. |
| R19 | Done | Focus-group scene shows a large QR for the exact onboarding invitation. |
| R20 | Done | The real current Today page is activated and naturally scrolled to its visible 0–10 focus rating block. |
| R21 | Partial | Verified KGD80 people and real participant-like UI are shown and live-smoked. Current static data has no canonical event-to-person relation, so the second step uses the explicit event-participants lab surface rather than inventing production relations. |
| R22 | Done | Current Tomorrow event 5297 journey live-smoked successfully. |
| R23 | Done | Rail description and rail-like journeys live-smoked; consent, exact +1 count, storage and reload persistence are verified. |
| R24 | Done | Current Weekend 1–2 August desktop page fills FHD and naturally scrolls. |
| R25 | Done | PWA includes a right-side vertical ↑/↓ manual-scroll strip without closing or resetting the active scene. |

## Validation

- Agent: `30/30` Node tests passed.
- Relay: `16/16` Python tests passed.
- PWA authorization: `2/2` Node tests passed.
- Presenter stage: `7/7` Node tests passed.
- Full Astro static build: `465` pages passed.
- Audio skill validation: passed.
- Live targeted smokes passed:
  - `tomorrow-mobile`;
  - `tomorrow-rail-like` (`5297`, count `2→3`, storage/reload verified);
  - `weekend-amber-artifact` (`7164`, collection/reload/dialog verified);
  - `service-focus-group`;
  - `service-nps`;
  - `service-future-celebrity`;
  - `service-medallions`;
  - `service-joke`;
  - `weekend-desktop`.
- Search smoke produced the expected explicit one-time-login blocker.
- Actual Windows x64 launch remains an owner-machine smoke; Linux validation covers packaging/contracts only.
- Post-deploy checks: `/healthz`, PWA HTML/manifest, presenter stage, authenticated
  ZIP download and all 14 archive entries passed.

## Release gate

- Owner test: **GO**, except authenticated search until the demo session is prepared.
- Public demo: **NO-GO** until the one-time search login and a full uninterrupted owner rehearsal are completed.
