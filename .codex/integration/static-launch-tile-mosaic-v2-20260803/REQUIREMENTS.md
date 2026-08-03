# Tile mosaic v2 requirement matrix

| ID | Requirement | Area | Dependencies | Primary lane | Done when |
|---|---|---|---|---|---|
| R01 | Desktop tiles are square and sized from one sixth of viewport height; twelve columns may overflow right | geometry | none | ui-v2 | measured square at all desktop fixtures |
| R02 | Mosaic starts at top and about 36–37vw, intruding into the copy zone | composition | R01 | ui-v2 | top=0 and left boundary verified |
| R03 | Gaps are opaque nearly black seams and never reveal the projected image | material | R01 | ui-v2 | pixel/DOM contract confirms dark lattice |
| R04 | Default PWA projection hides the light outer square; generic cover stays independent | projection | none | ui-v2 | brand and generic modes both work |
| R05 | Brand squircle is bounded and positioned around 14–80svh | projection | R04 | ui-v2 | full form visible on reference desktop |
| R06 | Left logo uses the square PWA icon | content | R04 | ui-v2 | image replaces text lockup |
| R07 | H1 is “Полюбить / Калининград / Анонсы” | content | none | ui-v2 | exact DOM text/line structure |
| R08 | Remove eyebrow; show exact orange date and “СКОРО ЗАПУСК • 1 СЕНТЯБРЯ” status | content | none | ui-v2 | exact visible copy |
| R09 | Use exact four-line description and visually hidden accessible email label | content/a11y | none | ui-v2 | exact text and accessible label |
| R10 | Reference form proportions, dark terracotta material, envelope, glass input | form | R09 | ui-v2 | desktop geometry and focus states pass |
| R11 | Remove carbon hatching; strengthen irregular leather/metal roughness and left ambience | material | none | ui-v2 | no repeating diagonal pattern |
| R12 | Static state mix is expressive and revealed tiles retain texture/clarity | motion/material | R03 | ui-v2 | distribution within specified bands |
| R13 | Desktop ≥1024×760 is fullscreen without vertical scrolling; small/mobile can scroll | layout | R01 | ui-v2 | scrollHeight==innerHeight for desktop fixtures |
| R14 | Responsive mobile version remains usable | mobile | R01-R10 | ui-v2 | 320–430 fixtures have no overflow/overlap |
| R15 | Slow tile dynamics, pointer lighting and reduced-motion static mode remain correct | motion/a11y | R12 | ui-v2 | temporal and reduced-motion assertions pass |
| R16 | Same engine supports brand mask, cathedral/photo cover, landscape cover plus focal point | projection | R04 | ui-v2 | runtime image-mode API/query passes |
| R17 | Success, duplicate, invalid, honeypot and network error form states remain correct | form/transport | R10 | qa-v2 | browser assertions cover each state |
| R18 | Desktop acceptance: 1366×768, 1440×900, 1536×864, 1672×941, 1920×1080 | QA | R01-R13 | qa-v2 | terminal L1 report/screenshots |
| R19 | Mobile acceptance: 320×700, 360×800, 390×844, 430×932 | QA | R14 | qa-v2 | terminal L1 report/screenshots |
| R20 | Capture animation at 0, 5 and 10 seconds | QA | R15 | qa-v2 | three timestamped frames |
| R21 | Required handoff screenshots at 1672×941, 1920×1080 and 390×844 | QA | R18-R20 | qa-v2 | artifacts exist and are inspected |
| R22 | Capture PWA and arbitrary photo through one projection engine | QA | R16 | qa-v2 | both modes in evidence report |
| R23 | Update canonical feature documentation | docs | final contract | docs-v2 | docs match implementation and acceptance |
| R24 | Update changelog, integrate, validate exact SHA, publish noindex preview and send updated link to Telegram thread | release | all | integration-v2 | public HTTP 200 + Telegram receipt |
