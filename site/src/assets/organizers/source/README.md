# Organizer medallion source assets

Retrieved: 2026-06-29; `dom-kitoboya` social avatar rechecked 2026-07-01; SVG runtime pass checked 2026-07-02.

These files are official/source-faithful inputs for the first event-page organizer medallions. Final runtime assets live in `site/public/assets/organizers/`.

| Slug | Organization | Source page | Source URL | Final asset | Notes |
| --- | --- | --- | --- | --- | --- |
| `world-ocean-museum` | Музей Мирового океана | https://www.world-ocean.ru/ | https://www.world-ocean.ru/images/main/logo_new_mobile_v2.svg | `/assets/organizers/world-ocean-museum.svg` + PNG/WebP fallback | Из официального мобильного SVG-логотипа использован крупный знак ММО без мелкой подписи; runtime SVG построен из простых векторных примитивов. |
| `history-art-museum` | Историко-художественный музей | https://koihm.ru/ | https://koihm.ru/wp-content/uploads/2026/03/logo_koihm_white.png | `/assets/organizers/history-art-museum.svg` + PNG/WebP fallback | Public SVG candidates for KOIHM returned 404 on 2026-07-02; the geometric building/`КОИХМ` medallion was locally vectorized from the accepted PNG. |
| `kaliningrad-philharmonic` | Калининградская филармония | https://filarmonia39.ru/ | https://filarmonia39.ru/local/templates/filarmonia/images/logo_black.svg | `/assets/organizers/kaliningrad-philharmonic.svg` + PNG/WebP fallback | Официальный чёрный SVG-знак вписан в жёлтый круг #FAB534, как в текущем аватаре Telegram-канала https://t.me/filarmonia_39; знак не растрируется в runtime. |
| `kant-island` | Остров Канта | https://sobor39.ru/ | https://sobor39.ru/images/logo.svg | `/assets/organizers/kant-island.svg` + PNG/WebP fallback | Из официального горизонтального SVG-логотипа взят точный path знака Кафедрального собора — самая узнаваемая часть для круглого медальона. |
| `dom-kitoboya` | Дом китобоя | https://domkitoboya.ru/ + https://t.me/domkitoboya | source logo snapshot `dom-kitoboya.logo.webp` + Telegram avatar snapshot `dom-kitoboya.telegram-avatar-20260701.jpg` | `/assets/organizers/dom-kitoboya-stacked.webp` / `/assets/organizers/dom-kitoboya-stacked.png` | Медальон собирается из фирменного логотипа без верхней мелочи; public `logo.svg` candidates returned 404 on 2026-07-02 while `logo.webp`/`logo.png` exist, so the medallion remains raster until a source SVG is found. |
| `tretyakovka-kaliningrad` | Филиал Третьяковской галереи | https://t.me/tretyakovka_kaliningrad | Telegram avatar snapshot `tretyakovka-kaliningrad.telegram-avatar-20260701.jpg` | `/assets/organizers/tretyakovka-kaliningrad.svg` + PNG/WebP fallback | Простая геометричная золотая буква `Т` реконструирована как SVG-примитивы на тёплом светлом фоне. |
| `konb` | Калининградская областная научная библиотека | local docs/reference/лого КОНБ (1)(1).png | `konb.logo.png` | `/assets/organizers/konb.webp` / `/assets/organizers/konb.png` | Explicit raster exception for the 2026-07-02 SVG pass; this task intentionally does not convert КОНБ. |

No OpenAI image generation/editing was used. The medallions were produced by local SVG rendering/vectorization, source cropping, and alpha-preserving PNG/WebP fallback export.
