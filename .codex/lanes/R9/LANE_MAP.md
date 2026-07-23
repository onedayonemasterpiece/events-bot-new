# R9 mobile acceptance lane map

Base: `15106c51` (`integration/mobile-v23-rails-menu-revert-r8-20260723`).

| Requirement | Scope | Dependency | Owner lane | Integration order |
|---|---|---|---|---|
| R1 | Mobile event leather tag | accepted shared leather asset/component | EVENT | 1 |
| R2 | Rail media loading skeleton | accepted v23/v28 rail donor | RAIL | 2 |
| R3 | Single high no-OCR media crops to landscape 4:5; multi-card may use portrait 4:5 | rail group cardinality + OCR contract | RAIL | 2 |
| R4 | Exact accepted weekend calendar | accepted v23/v28 donor | RAIL | 2 |
| R5 | Easter egg exact mechanism and discoverability | accepted easter-egg donor/canonical docs | RAIL | 2 |
| R6 | Sticky page subheader and shelves | accepted v23/v28 donor | RAIL | 2 |
| R7 | Mobile exhibition medallion scale | accepted exhibitions v12 donor | EXHIBITIONS | 3 |
| R8 | Search CTA as accepted progress button | accepted mobile search donor | SEARCH | 4 |
| R9 | Telegram: normalize event-detail medallion sizes | screenshot message 624/625 | EVENT | 1 |
| R10 | Telegram: straighten rail right arrow | screenshot message 626/627 | RAIL | 2 |

Every lane must name exact donor branch/SHA/files, preserve unrelated R8 work, add regression coverage, docs and CHANGELOG only where lane-owned; integrator reconciles shared docs/CHANGELOG.
