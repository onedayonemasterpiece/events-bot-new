# Autopresenter iteration 2026-07-29 B — execution matrix

| ID | Requirement | Area / likely files | Dependencies | Risk | Primary lane | Parallel? | Done when |
|---|---|---|---|---|---|---|---|
| R01 | Human-like Hero Talk compromise: slower and livelier | stage CSS/JS, stage tests | historical behavior | medium | INT | after discovery | deterministic human-like rhythm is visible and tested |
| R02 | Restore spaces in intro | stage markup/CSS/JS | R01 | low | INT | no | rendered text preserves spaces |
| R03 | Replace zero-minute copy with “Вот-вот начинаем” | stage JS/tests | none | low | INT | exact copy appears at the threshold |
| R04 | Remove expanding fields around lecture images | stage CSS | visual audit | medium | INT | images have no parasitic containers/fields |
| R05 | Screenshot and inspect lecture edges/outlines | screenshots + stage | R04 | low | INT | FHD evidence reviewed visually |
| R06 | Type lecture text, then reveal image | stage JS/CSS/tests | lecture hold contract | medium | INT | typed statement completes before media reveal |
| R07 | Prevent “Видеть главное” overlap with Znanie | stage CSS | visual audit | low | INT | FHD screenshot shows safe logo clearance |
| R08 | Last lecture scheme uses about half screen and readable enlargement | stage CSS/data | visual audit | low | INT | final scheme is maximized without clipping |
| R09 | Use real static-site icons for find/share/calendar | stage markup/assets | media discovery | low | INT | icons originate from current site assets/components |
| R10 | Mobile menu remains visible long enough to understand selection | agent pacing + PWA copy | live selectors | medium | PWA | yes | dwell and action are explicit and observable |
| R11 | Artifact scene performs artifact discovery actions after Weekend | agent runtime/tests | latest preview hooks | high | INT | live smoke reaches visible artifact result |
| R12 | Medallion scene animates real recognizable medallions into 4×4 grid | stage/assets/tests | media discovery | medium | INT | large-to-grid sequence contains real assets |
| R13 | Joke scene plays supplied Telegram audio on second line, no service copy | stage/audio/CDN | Telegram media | high | INT | audio is fetched, hosted and triggered at the correct beat |
| R14 | Append consolidated corrections to base scenario | scenario doc | all requirements | low | INT | bottom section contains R01–R25 traceability |
| R15 | Add slowly moving soft halo background | stage CSS | visual QA | low | INT | motion is subtle and does not obscure content |
| R16 | Authenticated Smart Search submits and shows results | agent/auth/docs/tests | auth discovery | high | INT | owner-test account/session returns result cards |
| R17 | Add licensed error sting and reusable sourcing skill | media/skill/stage | web research/license | medium | INT | sourced file, provenance and validated skill exist |
| R18 | Use latest focus preview routes in every live scene | contracts/agent/tests | route discovery | high | INT | no stale live route remains |
| R19 | Focus-group scene shows QR for exact onboarding URL | stage/QR/CDN/tests | exact URL | low | INT | scannable QR is visible |
| R20 | NPS scene shows real page and actual NPS block | agent/live page | latest preview selector | high | INT | mobile page scrolls to real NPS block |
| R21 | Correct people data and show real event people/like UI in phase two | stage/agent/assets | KGD80 + live preview | high | INT | names/data correct and interface phase is visible |
| R22 | Restore Tomorrow scenario | agent/contracts/tests | latest preview | high | INT | targeted live smoke completes |
| R23 | Restore rail-right scenario | agent/contracts/tests | R22 | high | INT | rail visibly shifts right and completes |
| R24 | Use current Weekend page and scroll | agent/contracts/tests | latest preview | high | INT | current dates are shown and desktop scroll occurs |
| R25 | Add manual vertical scroll strip to PWA | relay PWA/server + agent | protocol integration | medium | PWA | yes | up/down controls scroll the active live page without ending it |

## Dependency graph

- DISC-INTRO → R01–R03.
- DISC-LECTURE → R04–R08, R15.
- DISC-MEDIA → R09, R12, R13, R17, R21.
- DISC-LIVE → R10–R11, R16, R18–R24.
- PWA implements R10/R25 independently; INT adds the matching agent command and integrates it.
- INT appends R14 only after the implementation decisions are stable.
- AUDIT runs only after targeted tests, full final regression, packaging and deploy evidence.
