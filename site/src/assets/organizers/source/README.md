# Organizer medallion source assets

Retrieved: 2026-06-29; `dom-kitoboya` social avatar rechecked 2026-07-01; SVG/WebP runtime pass checked 2026-07-02.

These files are official/source-faithful inputs for the first event-page organizer medallions. Final runtime assets live in `site/public/assets/organizers/`.

| Slug | Organization | Source page | Source URL | Final asset | Notes |
| --- | --- | --- | --- | --- | --- |
| `world-ocean-museum` | Музей Мирового океана | https://www.world-ocean.ru/ | https://www.world-ocean.ru/images/main/logo_new_mobile_v2.svg | `/assets/organizers/world-ocean-museum.svg` + PNG/WebP fallback | Из официального мобильного SVG-логотипа использован крупный знак ММО без мелкой подписи; runtime SVG построен из простых векторных примитивов. |
| `history-art-museum` | Историко-художественный музей | https://koihm.ru/ | https://koihm.ru/wp-content/uploads/2026/03/logo_koihm_white.png | `/assets/organizers/history-art-museum.svg` + PNG/WebP fallback | Public SVG candidates for KOIHM returned 404 on 2026-07-02; the geometric building/`КОИХМ` medallion was locally vectorized from the accepted PNG. |
| `kaliningrad-philharmonic` | Калининградская филармония | https://filarmonia39.ru/ | https://filarmonia39.ru/local/templates/filarmonia/images/logo_black.svg | `/assets/organizers/kaliningrad-philharmonic.svg` + PNG/WebP fallback | Официальный чёрный SVG-знак вписан в жёлтый круг #FAB534, как в текущем аватаре Telegram-канала https://t.me/filarmonia_39; знак не растрируется в runtime. |
| `kant-island` | Остров Канта | https://sobor39.ru/ | https://sobor39.ru/images/logo.svg | `/assets/organizers/kant-island.svg` + PNG/WebP fallback | Из официального горизонтального SVG-логотипа взят точный path знака Кафедрального собора — самая узнаваемая часть для круглого медальона. |
| `dom-kitoboya` | Дом китобоя | https://domkitoboya.ru/ + https://t.me/domkitoboya | source logo snapshot `dom-kitoboya.logo.webp` + Telegram avatar snapshot `dom-kitoboya.telegram-avatar-20260701.jpg` | `/assets/organizers/dom-kitoboya-stacked.webp` (`.png` fallback) | Медальон собирается из фирменного логотипа без верхней мелочи; public `logo.svg` candidates returned 404 on 2026-07-02 while `logo.webp`/`logo.png` exist, so the medallion remains WebP-first raster until a source SVG is found. |
| `tretyakovka-kaliningrad` | Филиал Третьяковской галереи | https://t.me/tretyakovka_kaliningrad | Telegram avatar snapshot `tretyakovka-kaliningrad.telegram-avatar-20260701.jpg` | `/assets/organizers/tretyakovka-kaliningrad.svg` + PNG/WebP fallback | Простая геометричная золотая буква `Т` реконструирована как SVG-примитивы на тёплом светлом фоне. |
| `kldzoo` | Калининградский зоопарк | https://kldzoo.ru/ | https://kldzoo.ru/local/templates/s1/img/logo.png | `/assets/organizers/kldzoo.webp` (`.png` fallback) | Официальный квадратный PNG-логотип, получен 2026-07-03 и локально конвертирован без перерисовки; source сохранён как `kldzoo.logo.png`. |
| `konb` | Калининградская областная научная библиотека | local docs/reference/лого КОНБ (1)(1).png | `konb.logo.png` | `/assets/organizers/konb.webp` / `/assets/organizers/konb.png` | Explicit raster exception for the 2026-07-02 SVG pass; this task intentionally does not convert КОНБ. |
| `act-opus` | Театр «Акт Опус» | https://actop.us/plays | Next image PNG `logo_new_black.3136802c.png` | `/assets/organizers/act-opus.svg` (`.png` fallback) | Осьминог заменён на стековую надпись из официального wordmark: блок `АКТ` и слово `ОПУС` сохранены из source PNG и размещены друг над другом внутри круга; `АКТ` уменьшен/опущен с безопасным inset. SVG self-contained, без OpenAI image generation. |
| `znanie-russia` | Российское общество «Знание» | https://znanierussia.ru/ + local kgd80 shared assets | official site CSS primary `#0501D0`; `logo-znanie-festival.svg` + `logo-znanie-main.svg` | `/assets/organizers/znanie-russia.svg` (`.png` fallback) | Полный круг залит официальным синим `#0501D0`, внутренний знак `З` оставлен белым, увеличен, оптически выровнен через root-clipped SVG group и клипуется нижним краем круга. Для `event.festival=80 историй о главном` показывается по curated policy даже без явного упоминания Знания в тексте. |
| `kgd80` | Фестиваль «80 историй о главном» | https://kgd80.ru/ | `kgd80.logo-80-istorii-hero.svg` from the KGD80 hero logo | `/assets/organizers/kgd80.svg` (`.png` fallback) | Новый медальон фестиваля из hero lockup; tighter viewBox увеличивает знак внутри круга с безопасными отступами, а весь lockup оптически опущен на несколько пикселей. Для событий `80 историй о главном` рендерится вместе с медальоном Российского общества «Знание». |
| `kantata-festival` | Фестиваль «Кантата» | https://kantatafest.ru/obrazovatelnaya-programma | `Kantata_logo_Black_R.png` | `/assets/organizers/kantata-festival.webp` (`.png` fallback) | Используется официальный wordmark «КАНТАТА»; source raster, поэтому runtime WebP-first. |
| `dramteatr39` | Калининградский драматический театр | https://dramteatr39.ru/ | https://dramteatr39.ru/img/logo.svg?v=2 | `/assets/organizers/dramteatr39.svg` | Официальный горизонтальный SVG масштабирован и обрезан кругом до левого театрального знака; фон сделан фирменно-тёмным для белого исходника. |
| `kaup` | Поселение викингов «Кауп» | https://www.kaup39.ru/ | https://static.tildacdn.com/tild3166-3161-4133-a638-363932633936/Logo_wh_main.svg | `/assets/festivals/kaup.svg` | Официальный SVG-знак возвращён как `venue_brand`; runtime совпадение ограничено нормализованными алиасами площадки/источника. |

No OpenAI image generation/editing was used. The medallions were produced by local SVG rendering/vectorization, source cropping/recomposition, embedded-source SVG where needed, and PNG fallback export. If no SVG source/vector-safe source exists, browser-facing runtime assets should be WebP-first, with PNG only as fallback/QA.

## Listing overlay reuse

V13 does not maintain a hand-written three-venue allow-list. Every existing
manifest item has an explicit `listingStatus` and `listingBinding`; only
`listing_ready` items with an exact/bounded structured venue or festival match
participate. The selected asset must still be `image_text_mode=visual_only`, at
most one overlay is rendered, and runtime priority is venue before festival.
`znanie-russia` stays `blocked_missing_binding` until the static event contract
exports a structured organizer field; the duplicate `kgd80` identity remains
`detail_only`. This listing behavior reuses the source-faithful assets introduced
by commits `00b9bfd6`, `4c249a8e`, `849aaeaa` and `fa367ea3`; no artwork was
redrawn for V13.
