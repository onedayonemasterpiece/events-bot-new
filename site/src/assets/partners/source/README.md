# Information partner logo source assets

Runtime partner logos live in `site/public/assets/partners/`. The grid may use different logo aspect ratios; do not flatten all marks into one square lockup.

| Partner | Runtime asset | Source/provenance | Format rule |
| --- | --- | --- | --- |
| АО «КППК» / Калининградская пригородная пассажирская компания | `/assets/partners/kppk-rzd-red.svg`; original parked header asset `/assets/partners/kppk-rzd.svg` | Official site `https://www.kppk39.ru/` uses `/bitrix/templates/rzd/img/logo.svg`; contacts page gives full name `Акционерное общество «Калининградская пригородная пассажирская компания»` and short name `АО «КППК»`. The runtime SVG is the official header mark recolored to RZD red for a transparent flat-board surface; public label is `АО «КППК»`, not `КППК / РЖД`. | SVG primary |
| Российское общество «Знание» | `/assets/partners/znanie-russia.svg` | `/home/dev/projects/kdg80/site/public/shared-assets/logo-znanie-main.svg` | SVG primary |
| «80 историй о главном» | `/assets/partners/kgd80.svg` | `/home/dev/projects/kdg80/site/public/shared-assets/logo-80-istorii-hero.svg` | SVG primary, vertical hero lockup |
| «Кантата» | `/assets/partners/kantata-education.webp` + PNG fallback | `Kantata_logo_Black_R.png` captured from `kantatafest.ru` Tilda page | WebP primary because source is raster PNG |
| Театр «Акт Опус» | `/assets/partners/act-opus.webp` + PNG fallback | `https://actop.us/plays` Next image `logo_new_black.3136802c.png` | WebP primary because source is raster PNG |
| ИЦАЭ Калининграда | `/assets/partners/icae-kaliningrad.svg` | Byte-faithful copy of the official horizontal footer mark `https://klgd.myatom.ru/wp-content/themes/icao2/image/logo-footer-h.svg`, captured 2026-07-23; source and runtime SHA-256 `e59541c9ffa5c4865d87c1273068b2440ebf89bc794de6d5d18387cc9a0f3797` | SVG primary, unmodified 288×50 official wide lockup |

No OpenAI image generation/editing was used.
