# Organizer medallion source assets

Retrieved: 2026-06-29; source-faithful venue repair pass checked 2026-07-04; complete reachable-history inventory and Greza Khutor recovery audited 2026-07-23.

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

| `yantar-hall` | Янтарь холл | https://янтарьхолл.рф/ | https://янтарьхолл.рф/bitrix/templates/yh/yh_ya_logo.svg | `/assets/organizers/yantar-hall.svg` | Официальный квадратный SVG-знак YH выбран вместо горизонтального wordmark для читаемости в круглом медальоне; площадка имеет много будущих событий и alias `Янтарь холл, Ленина 11, Светлогорск`. |
| `muzteatr39` | Калининградский музыкальный театр | https://muzteatr39.ru/ | https://muzteatr39.ru/wp-content/uploads/image_theme/logo2.png | `/assets/organizers/muzteatr39.webp` (`.png` fallback) | Официальный квадратный PNG пересобран как WebP-primary медальон без трассировки; фон затемнён для контраста белого знака. |
| `dom-iskusstv` | Калининградский театр эстрады / Дом искусств | https://домискусств39.рф/ | https://static.tildacdn.com/tild3965-6438-4135-a233-383865633034/svg.svg | `/assets/organizers/dom-iskusstv.svg` | Официальный Tilda SVG wordmark Театра эстрады/Дома искусств встроен в круглый медальон. |
| `city-jazz-club` | Калининград Сити Джаз Клуб | https://londonpub.ru/cityjazz/events | https://static.tildacdn.com/tild6331-6539-4430-a531-343262333939/logojazz.png (`recheck-20260704/city-jazz-club.official-logojazz.png`) | `/assets/organizers/city-jazz-club.webp` (`.png` fallback) | Ручная SVG-перерисовка отклонена как исказившая реальный логотип клуба; runtime WebP-first сохраняет официальный PNG с круглым знаком и фирменной подписью без invented `CITY JAZZ CLUB` lockup. |
| `rostec-arena` | Ростех Арена | https://www.rostec-arena.ru/ | https://www.rostec-arena.ru/theme/src/logo.svg (`recheck-20260704/rostec-arena.official-logo.svg`) | `/assets/organizers/rostec-arena.svg` (+ PNG QA fallback) | Официальный SVG-логотип сайта встроен без ручной перерисовки: сохранены фирменные голубые полосы и белый wordmark на синем градиентном круге. |
| `bar-bastion` | Бар Бастион | https://vk.com/bar_bastion | public VK avatar `bar-bastion.vk-avatar-20260704.jpg` | `/assets/organizers/bar-bastion.webp` (`.png` fallback) | VK-аватар маскируется во внутренний круг, а золотая бренд-обводка рисуется последней; разрывы окружности от квадратного overlay устранены. |
| `signal` | Сигнал | https://t.me/signalkld + https://signalcommunity.timepad.ru/ | Telegram avatar `recheck-20260704/signal.telegram-avatar-20260704.jpg`; Timepad `logo_org_265887.jpg` | `/assets/organizers/signal.webp` (`.png` fallback) | SVG-наборный вариант отклонён: он потерял фирменный овал и подменил шрифт. Runtime WebP-first сохраняет официальный овальный знак, двойные oval-strokes и исходное положение нижней подписи. |
| `mumod` | Музей курортной моды | https://mumod.ru/ | https://mumod.ru/wp-content/themes/mumod/images/logo.svg | `/assets/organizers/mumod.svg` | Официальный SVG-логотип с сайта музея встроен в светлый круглый медальон. |
| `kldzoo` | Калининградский зоопарк | https://kldzoo.ru/ | https://kldzoo.ru/local/templates/s1/img/logo.png | `/assets/organizers/kldzoo.webp` (`.png` fallback) | Официальный квадратный PNG увеличен и отдаётся WebP-primary; внешняя салатовая обводка заменена на нейтральную. |
| `locostandup` | Стендап-клуб «Локация» | https://locostandup.ru/ | https://img3.creatium.ru/disk2/ee/53/e9/5338346aed3353ce0b7a71e35231aeda9e/lokaciya.svg (`recheck-20260704/locostandup.official-logo.svg`) | `/assets/organizers/locostandup.svg` (+ PNG/WebP QA fallback) | Официальный site SVG wordmark сохранён в круглом медальоне; добавленная ранее подпись `СТЕНДАП-КЛУБ` удалена, а кольцо сделано нейтральным серым в тон wordmark, чтобы контекстный акцент не выглядел частью логотипа. |
| `kaliningrad-art-museum` | Калининградский музей изобразительных искусств | https://www.kaliningradartmuseum.ru/ | homepage inline SVG `recheck-20260704/kaliningrad-art-museum.official-inline-logo.svg`; footer PNG still kept as source | `/assets/organizers/kaliningrad-art-museum.svg` (+ PNG/WebP QA fallback) | Фон — официальный бордовый `#871B30`; аббревиатура `ИЗО` удалена, под официальным знаком размещена крупная читаемая подпись `МУЗЕЙ ИСКУССТВ`. |
| `brachert` | Дом-музей Германа Брахерта | https://hbrachert.ru/ | https://hbrachert.ru/bitrix/templates/brachert/images/logo-br.png | `/assets/organizers/brachert.webp` (`.png` fallback) | Официальный PNG-логотип hbrachert.ru отдаётся WebP-primary. |
| `ruin-keepers` | Хранители руин | https://ruin-keepers.ru/ | official 1-bit PNG logo `recheck-20260704/ruin-keepers.official-logo.png` | `/assets/organizers/ruin-keepers.webp` (`.png` fallback) | Ручной filled-tower SVG отклонён как не похожий на знак; runtime WebP-first сохраняет официальный outline tower/heart и wordmark `Хранители руин` из 1-bit PNG. |
| `profitur` | Профи-тур | https://t.me/excursions_profitour | public Telegram profile avatar `r14-20260727/profitur.telegram-avatar-20260727.jpg`, retrieved 2026-07-27 | `/assets/organizers/profitur.webp` (`.png` fallback) | Публичный source-faithful avatar с надписью «ПРОФИ-ТУР» сохранён без перерисовки; runtime — локальный resize 512×512 и круглая alpha-маска. |
| `greza-khutor` | Грёза Хутор | https://vk.com/wall-231920894_5687 | `docs/reference/greza-khutor/987234 (4).png` | `/assets/organizers/greza-khutor.webp` / `/assets/organizers/greza-khutor.png` | Вариант 04 выбран по 4 явным голосам в VK; исходник скопирован без смыслового редизайна, локальные crop/resize, без AI. |

No OpenAI image generation/editing was used. The medallions were produced by local SVG rendering/vectorization, source cropping/recomposition, embedded-source SVG where needed, and PNG fallback export. If no SVG source/vector-safe source exists, browser-facing runtime assets should be WebP-first, with PNG only as fallback/QA.

## Listing overlay reuse

V13 does not maintain a hand-written three-venue allow-list. Every existing
manifest item has an explicit `listingStatus` and `listingBinding`; only
`listing_ready` items with an exact/bounded structured venue or festival match
participate. The selected asset must still be `image_text_mode=visual_only`, at
most one overlay is rendered, and runtime priority is venue before festival.
`znanie-russia` stays `blocked_missing_binding` until an event carries grounded
organizer evidence for it; the duplicate `kgd80` identity remains `detail_only`.
Organizer bindings consume `PreviewEvent.organizer_names`, which Smart Update
persists from quoted event-local evidence or an explicit curated self-publisher
binding. They never infer organizer identity from venue/title/description prose.
This listing behavior reuses the source-faithful assets introduced
by commits `00b9bfd6`, `4c249a8e`, `849aaeaa` and `fa367ea3`; no artwork was
redrawn for V13.

Telegram/Pillow fallback audit (2026-07-23): the accepted SVG runtime assets
`dramteatr39`, `yantar-hall`, `dom-iskusstv` and `mumod` received deterministic
512×512 RGBA same-stem PNG renders from the checked-in SVGs. `kaup` received
the equivalent fallback in the festival runtime tree. These files are transport
fallbacks only; the browser continues to use the source-faithful SVG primary.
The local conversion used CairoSVG 2.9.0 with explicit 512×512 output and no
generative image tooling.
