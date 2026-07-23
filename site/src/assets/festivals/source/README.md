# Festival medallion source assets

Raster final assets are WebP-primary with PNG fallback after the 2026-07-04 medallion contract pass.
Retrieved/updated: 2026-07-03; complete reachable-history inventory restored and audited 2026-07-23. Runtime medallion assets live in `site/public/assets/festivals/`.

| Slug | Type | Festival/brand | Source page | Source URL | Runtime asset | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `kgd80-80-stories` | festival | 80 историй о главном | https://kgd80.ru/ | https://kgd80.ru/shared-assets/logo-80-istorii-hero.svg | `/assets/festivals/kgd80-80-stories.svg` + `.png` fallback | Официальный SVG с сайта фестиваля; в круг вписан знак/композиция hero-логотипа. |
| `kaliningrad-city-jazz` | festival | Kaliningrad City Jazz | https://t.me/jazzfestivalru | https://t.me/jazzfestivalru (public Telegram avatar, retrieved 2026-07-03) | `/assets/festivals/kaliningrad-city-jazz.webp` | Заменён прежний полный SVG на публичный Telegram-аватар фестиваля: надпись City Jazz 2026 крупнее и читается лучше в круглом медальоне. |
| `kaliningrad-street-food` | festival | Городской пикник Kaliningrad Street Food | https://streetfoodfestival.ru/ | https://static.tildacdn.com/tild6634-6663-4633-b138-333363653339/LOGO_black_main.svg | `/assets/festivals/kaliningrad-street-food.svg` | Официальный SVG Kaliningrad Street Food; знак широковат, поэтому используется как крупная brand-плашка внутри круга. |
| `grozd-festival` | festival | Гроздь | https://xn--80adeebpd0a2atfmx3jf.xn--p1ai/ | https://static.tildacdn.com/tild3438-3966-4661-b131-613566643434/Layer_13.svg | `/assets/festivals/grozd-festival.svg` | Официальный SVG-слой с сайта фестиваля; белый wordmark сохранён на тёмном фиолетовом фоне. |
| `koroche` | festival | Короче | https://korochekino.ru/ | https://static.tildacdn.com/tild3861-3461-4363-b136-666532343734/__2023-07-19__012939.png | `/assets/festivals/koroche.webp` | Официальный PNG-знак с сайта фестиваля; Telegram-аватар тоже проверен, но для медальона выбран более чистый site logo. |
| `ostrova` | festival | Семейно-музейный фестиваль «Острова» | https://detivmuzee.ru/ | https://static.tildacdn.com/tild6330-6433-4135-b532-343738366262/__2.png | `/assets/festivals/ostrova.webp` | Официальный логотип с сайта detivmuzee.ru; Telegram-аватар проверен, но site logo даёт читаемый полный wordmark с подписью «музейно-семейный фестиваль». |
| `more-vnutri` | festival | Море внутри | https://sea-inside.ru/ | https://sea-inside.ru/assets/logo.svg | `/assets/festivals/more-vnutri.svg` | Официальный SVG-логотип сайта фестиваля; использован как чистый знак вместо афишной обложки Telegram. |
| `simfoniya-vetra` | festival | Симфония ветра | https://xn--80awafglm0a6dza.xn--p1ai/ | https://xn--80awafglm0a6dza.xn--p1ai/bitrix/templates/yh/assets/images/bkf.svg | `/assets/festivals/simfoniya-vetra.webp` | Официальный SVG-знак БКФ/«Симфония ветра» с сайта Янтарь-холла; афиша события проверена как подтверждение связки. |
| `bahosluzhenie` | festival | Бахослужение | https://filarmonia39.ru/festivali/mezhdunarodnyy-muzykalnyy-festival-bakhosluzhenie-4/ | https://filarmonia39.ru/upload/iblock/aae/6dk15dbws94827t8588xmjl2jnvmkd42.png | `/assets/festivals/bahosluzhenie.webp` | Официальный широкий логотип со страницы фестиваля филармонии; для читаемости в круге локально скомпонованы его Bach/organ icon и основной wordmark без AI-генерации. |
| `tolkin-fest` | festival | Толкин Фест | https://tolkinfest.ru/tolkinfest2025 | https://static.tildacdn.com/tild3132-6135-4666-a466-613662666530/photo.png | `/assets/festivals/tolkin-fest.webp` | Официальный квадратный знак с архивной страницы Tolkin Fest; отдельный чистый логотип 2026 в открытых результатах не найден. |
| `kaup` | venue_brand | Кауп | https://www.kaup39.ru/ | https://static.tildacdn.com/tild3166-3161-4133-a638-363932633936/Logo_wh_main.svg | `/assets/festivals/kaup.svg` | Официальный знак поселения викингов «Кауп». Система хранит Кауп как локацию (`Поселение викингов Кауп`); в фестивальной лаборатории этот знак оставлен как площадочный бренд для событий Каупа без отдельного чистого логотипа. |

Social avatar review notes: Telegram avatars were checked for City Jazz, Короче, Острова and Море внутри; VK public/mobile pages often did not expose a clean festival-specific `og:image`. ВитаЛики/marafonbards yielded only a generic «Марафон авторской песни» organizer mark, so it is not included as a festival medallion.

No OpenAI image generation/editing was used. SVG assets are local wrappers or official SVGs; raster assets are source-faithful local square compositions made with Sharp from official/public assets for visual QA.

Telegram/Pillow fallback audit (2026-07-23): the accepted SVG runtime assets
`kaup`, `kaliningrad-street-food`, `grozd-festival` and `more-vnutri` received
deterministic 512×512 RGBA same-stem PNG renders from the checked-in SVGs.
Existing `kgd80-80-stories.png` remains its same-stem fallback. The browser
continues to use SVG primary assets; the PNGs exist so the deterministic Pillow
Telegram renderer can consume the same catalog without an SVG engine. The local
conversion used CairoSVG 2.9.0 with explicit 512×512 output and no generative
image tooling.

## Venue-brand source reused for listing overlays

| Slug | Category | Brand | Source | Runtime | Note |
| --- | --- | --- | --- | --- | --- |
| `kaup` | `venue_brand` | Кауп | https://static.tildacdn.com/tild3166-3161-4133-a638-363932633936/Logo_wh_main.svg | `/assets/festivals/kaup.svg` | Official Kaup mark reused from `origin/feature/static-site-venue-medallions-20260703`; listing overlay is allowed only for `image_text_mode=visual_only`. |

V13 records `kaup` as `listing_ready` + `venue` and
`kgd80-80-stories` as `listing_ready` + `festival`. The R10 rail regression
also records `more-vnutri` as `listing_ready` + `festival`, resolving real event
`4211` only from its structured festival field. These are structured
bindings, not evidence that an image is crop-safe; `visual_only` remains the
mandatory overlay gate and crop evidence remains asset-specific.

The 2026-07-23 recovery restores the complete 10-festival plus one venue-brand source set from the accepted inventory. SVG assets are local wrappers or official SVGs; raster assets are source-faithful local square compositions made with Sharp from official/public assets for visual QA. No OpenAI image generation/editing was used.
