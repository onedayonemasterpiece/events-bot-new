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
