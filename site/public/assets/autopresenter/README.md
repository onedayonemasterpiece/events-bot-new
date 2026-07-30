# Autopresenter third-party visual assets

These assets are used only in the internal presentation stage.

## Competitor marks

Marks were copied from the corresponding official public websites on 2026-07-29 for nominative comparison in the market-research slides:

- `competitors/klops-afisha.svg` — https://source.klops.ru/afisha/images/klops_logo_web_afisha.svg
- `competitors/visit-kaliningrad.png` — https://visit-kaliningrad.ru/assets/logotype.png; paired with the official text label because the source mark is an icon.
- `competitors/new-kaliningrad.gif` — https://www.newkaliningrad.ru/images/logo.gif
- `competitors/yandex-afisha.svg` — https://yastatic-net.ru/s3/afisha-frontend/static/_/edd95d686dc740fa5c07.svg

No endorsement is implied. Preserve the marks without redrawing.

## Lecture image

- `lecture/desire-path-ludwell-cc-by-sa-2.jpg`
- Author: David Smith / Geograph Britain and Ireland
- Source: https://commons.wikimedia.org/wiki/File:Desire_paths_across_the_grass,_Ludwell_Valley_Park,_Exeter_-_geograph.org.uk_-_6690600.jpg
- License: CC BY-SA 2.0 — https://creativecommons.org/licenses/by-sa/2.0/
- Adaptation: presentation crop and overlaid labels only; the source pixels are otherwise unchanged.

## Lecture UI reference screenshots

The owner supplied three exact reference screenshots in the presentation
review thread. They are stored unchanged under content-addressed keys on the
existing Yandex CDN and are not duplicated in this directory:

- T-Bank — Telegram message `890`, SHA-256
  `181b3a4b299696ccad0736b0cf8007f4c94649eb89a6bf9c6ed4f6f9cfad6c15`;
- Yandex Go — message `891`, SHA-256
  `55639003f7a89671a7f1b0c6264092b3b9e1a7100a04ace1b2d1e0375bb48a67`;
- Gosuslugi — message `893`, SHA-256
  `165c2e26ea0d3a717acfb8835d12154c4e65b74c05c5d94523d0ed4ca01fdd8b`.

The exact URLs and Telegram provenance are pinned in
`tools/autopresenter/agent/presentation-contract.mjs`.

## SVG Repo icons

The presentation reuses CC0 SVG Repo icons from the shared local library and two direct CC0 downloads. Files retain the SVG Repo numeric ID in their filename. The train and shield downloads include adjacent metadata JSON files. Presentation CSS may recolor them as monochrome masks; that is an adaptation, not a new source icon.

## Friends Club video

The video itself is not duplicated in this directory. It was supplied by the
owner in Telegram message `871`, hashed and uploaded unchanged to the existing
Yandex CDN as
`friends-club-darya-7cb34fb872eb528a4938f4e7af3cd8d2ebf1850246cb0cf9e2b44e7b17b05ac6.mp4`.
The source message is recorded in `presentation-contract.mjs`.
