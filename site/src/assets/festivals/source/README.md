# Festival medallion source assets

Current Telegram RichMessage slice: `kgd80-80-stories`.

| Slug | Source page | Source URL | Runtime assets | Notes |
| --- | --- | --- | --- | --- |
| `kgd80-80-stories` | https://kgd80.ru/ | https://kgd80.ru/shared-assets/logo-80-istorii-hero.svg | `/assets/festivals/kgd80-80-stories.svg` + `.png` Telegram fallback | Official festival SVG, fitted into the accepted circular medallion. |

The `512×512` transparent PNG fallback was deterministically rasterized from
the committed runtime SVG with local headless Chromium
(`--default-background-color=00000000`). The static site may use SVG; the
Python Telegram strip renderer consumes the Pillow-readable PNG.

No OpenAI image generation/editing was used.

## Venue-brand source reused for listing overlays

| Slug | Category | Brand | Source | Runtime | Note |
| --- | --- | --- | --- | --- | --- |
| `kaup` | `venue_brand` | Кауп | https://static.tildacdn.com/tild3166-3161-4133-a638-363932633936/Logo_wh_main.svg | `/assets/festivals/kaup.svg` | Official Kaup mark reused from `origin/feature/static-site-venue-medallions-20260703`; listing overlay is allowed only for `image_text_mode=visual_only`. |
