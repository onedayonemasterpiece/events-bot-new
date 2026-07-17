# Городской обзор на главной: минимальный prototype gate

> **Status:** clean-home lab with an expanded narrative queue, finite automatic chains and optional desktop media; immutable review build is recorded below after each verified publication.
> **Implementation:** dedicated one-route build for `/lab/briefing/`; no production homepage integration.
> **Production effect:** none; the lab is not linked from production navigation and is published only under an immutable preview prefix.
> **Decision:** `GO_TO_PROTOTYPE_ONLY`.
> **Product desirability:** unvalidated.
> **Validated by users:** false.
> **Validated by metrics:** false.
> **Owner:** unassigned.
> **Review after:** production canary + baseline listing funnel + lab usability/telemetry evidence.

## Решение после внешнего аудита

Первое исследование подробно ответило на вопрос «как могла бы работать полноценная briefing-система», но не доказало, что она нужна пользователям. Два прохода одного Gemini Pro — полезное итеративное design review, а не две независимые консультации и не product evidence.

Поэтому прежний `Conditional Go` отменён:

```text
research: accept with corrected provenance/status
minimal isolated prototype: approved
production implementation: not approved
Gemini Lite: deferred
personalization overlay: deferred
extended scenario platform: deferred
```

Качественный отзыв о слишком быстрых видеоанонсах подтверждает общий риск фиксированного темпа и потери «что, где, когда», но относится к другому media format. Он не доказывает desirability текстового briefing и не определяет его оптимальную высоту или скорость.

## Место в roadmap

Typed briefing не разблокирует текущий production static-site rollout и не является P0/P1 до появления базовой воронки. Правильная последовательность:

```text
production export
→ production canary
→ baseline listing funnel
→ выявление конкретной потери discovery
→ minimal static briefing experiment
→ только после выигрыша motion/personalization/platform work
```

Lab-прототип допустим сейчас только как дешёвый research artifact: он не меняет production routes, публикуется только в изолированном versioned preview prefix с `noindex` и не претендует на приоритет над data quality, export/canary или telemetry write path.

## Проверяемая гипотеза

**Showcase question:** считывается ли короткая подача как дружелюбный нарратив, вызывающий любопытство или объясняющий полезное целевое действие; заметна ли смысловая «печать», не мешает ли она чтению и остаются ли категории сразу доступными?

Это визуально-поведенческая проверка исходной концепции. Она не заменяет последующий product experiment `A: no briefing` vs `B: static`, если showcase пройдёт визуальную приёмку.

**Не проверяется в V0:**

- серверная персонализация и междевайсовая память;
- «с прошлого визита»;
- Gemini Lite или другой LLM;
- runtime API/manifest fetch;
- runtime-генерация новых сценариев;
- production-анимация wordmark; lab использует отдельный SVG только с точными двумя контурами широкой «О» из master wordmark, а не текстовую имитацию или crop всего слова;
- production homepage integration.

## Изолированный lab

Route:

```text
/lab/briefing/
```

### Clean-home/media iteration — текущий source contract

Immutable review builds:

- [current corrective conditional-motion build](https://kenigevents.ru/preview-20260717t1346-briefing-lab-85f5a9ee/lab/briefing/?variant=c&review=media&scenario=media_review_writing_kaliningrad&pace=slow&replay=1);
- [superseded: 14-scenario deck that retained terminal media by removing entry/exit motion](https://kenigevents.ru/preview-20260717t1237-briefing-lab-139d9809/lab/briefing/?variant=c&review=media&scenario=media_review_writing_kaliningrad&pace=slow&replay=1);
- [superseded: manual 12-image deck without sequence/portrait contracts](https://kenigevents.ru/preview-20260717t1049-briefing-lab-38425f28/lab/briefing/?variant=c&review=media&scenario=media_review_planet_ocean&pace=slow&replay=1);
- [current stripe/OCR/source-quality corrective candidate](https://kenigevents.ru/preview-20260717t0951-briefing-lab-21ca7a49/lab/briefing/?variant=c&scenario=weather_water_demo&pace=slow&replay=1);
- [superseded: dramatic mosaic with opaque/overlapping stripe, low-resolution sources and insufficient OCR admission](https://kenigevents.ru/preview-20260717t0754-briefing-lab-902829dd/lab/briefing/?variant=c&scenario=anticipated_person_named&pace=slow&replay=1);
- [superseded: rejected `12×4` checkerboard/distorted crop](https://kenigevents.ru/preview-20260716t0544-briefing-lab-4c2caa60/lab/briefing/?variant=c&scenario=anticipated_person_named&pace=slow&replay=1);
- [desktop `8×3` mosaic + text-only mobile](https://kenigevents.ru/preview-20260715t2306-briefing-lab-7c2b2a30/lab/briefing/?variant=c&scenario=live_meeting_mosaic&pace=slow&replay=1);
- [conditional storm → future lecture + pending cursor](https://kenigevents.ru/preview-20260715t2212-briefing-lab-53c3021d/lab/briefing/?variant=c&scenario=storm_weekend_demo&pace=slow&replay=1);
- [конкретный forwarding signal](https://kenigevents.ru/preview-20260715t2212-briefing-lab-53c3021d/lab/briefing/?variant=c&scenario=frequently_forwarded&pace=slow&replay=1);
- [конкретный Pianissimo](https://kenigevents.ru/preview-20260715t2212-briefing-lab-53c3021d/lab/briefing/?variant=c&scenario=festival_demo&pace=slow&replay=1);
- [full-viewport wide-media easter egg](https://kenigevents.ru/preview-20260715t2212-briefing-lab-53c3021d/lab/briefing/?variant=c&scenario=rare_event&pace=slow&replay=1).

Эта итерация отвечает на продуктовую, а не лабораторную критику предыдущего
экрана:

- selector раскрывает **19**, а не 8 сценариев; count/progress/play-all
  вычисляются из deck, а не захардкожены;
- рядом с curiosity-teaser есть отдельный named-person формат с Татьяной
  Куртуковой и ссылкой на canonical fixture event `6020`;
- добавлены `Добрый день!`, local phrase с `«кеска»`, smart-search education,
  grounded rare-event scene и явные `DEMO-СИГНАЛ` для погоды, фестиваля и
  необычного формата;
- верх страницы выглядит как обычная главная: hero → категории → начало ленты;
  A/B/C, selector, replay, play-all, debug memory/telemetry вынесены в компактный
  collapsed `LAB · управление` внизу;
- meta/progress, pause и pace полностью удалены из public hero и существуют
  только внутри закрытого нижнего LAB-dock;
- Variant C сам проигрывает конечную цепочку максимум из трёх связанных сцен с
  readable hold; публичная кнопка `Показать следующее` скрыта во время цепочки и
  появляется только в terminal state;
- предыдущая цель `13/19` media-сцен **superseded безопасностью источника**.
  Текущий admission оставляет mosaic только у `3/19` сценариев: картинка
  должна одновременно иметь ручное `ocrSafe`, exporter metadata
  `image_text_mode=visual_only`, `image_kind=photo`, `safe_crop=true`,
  `recommended_hero_fit=cover`, ширину не менее `1000px` и площадь не менее
  `1MP`. Если любой сигнал отсутствует, lab показывает полноценный text-only
  narrative вместо случайного постера или нерелевантной подмены;
- обычная очередь `3/19` не является visual sample. Для сравнения механики
  существует отдельный `?review=media`: **14** review-only сценариев, 14 разных
  event objects и 16 exact sources (три источника входят в один collage-plan).
  Они не попадают в cooldown, automatic queue или трёхэкранную цепочку. После
  reveal каждый экран по умолчанию останавливается и сохраняет финальную
  картинку; под hero всегда видны прямые
  кнопки `1–14`, `Назад`, `Повтор`, `По очереди`, `Далее`. `По очереди`
  запускает последовательный просмотр разных нарративов, manual default
  позволяет вернуть любую картинку без ожидания. Review mode сохраняет exact source order: если именно
  вручную проверенный asset исчез или перестал проходить admission, resolver
  abstain, а не подбирает другой raster того же события. Клетки, которые
  геометрически пересекают fragment line boxes, получают alpha cap `.24`:
  защита следует grid, а не создаёт новую бумажную плашку. Mobile показывает те
  же тексты и review controls без raster URL; rail `1–14` переносится полностью
  и не прячет последние состояния;
- image lifetime равен narrative lifetime. Каждая новая mosaic проходит
  irregular tile-entry. Terminal и manual state не запускают exit вообще и
  сохраняют картинку. Только реальный automatic successor сначала
  preloads/decodes exact next media-plan, затем запускает irregular exit
  текущего кадра; после exit вместе коммитятся `{copy, CTA, media-plan}`, а
  следующая mosaic снова проходит entry. Поэтому нет ни пустого hero, ни новой
  copy поверх orphan-картинки. Pause инкрементирует transition token, отменяет
  pending entry/exit и фиксирует текущий кадр в `is-present`;
- crop correction точечный, а не массовый: сцены `02` Ivana Kupala и `08`
  «День валяния в сене» возвращены к принятому baseline `cover 52/50` и
  `cover 55/50`; `07` и `12` также возвращены к `52/45` и `55/50`.
  Единственные сохранённые portrait-led head-safety exceptions — `04`
  «Пишу из Калининграда» (`75vw`, 16 колонок, `56/6`) и `06` Верти́нский
  (`38/20`). Scene `04` не становится detached portrait stamp: 16 меняет
  высоту square-cell поля, но его ширина остаётся `75vw` до правого viewport
  edge. Это временные human overrides до production metadata producer;
- review states 13/14 проверяют два отдельных portrait contracts. Один
  вертикальный source занимает непрерывный правый `5×5` cluster без
  растяжения. Три вертикальных OCR-safe источника одного события распределяют
  **все** активные колонки между тремя соседними contiguous cover-панелями
  (`7/7/6` при 20 колонках): каждая клетка заполнена ровно одним источником,
  letterbox/half-cell и чередования источников нет. План fail closed целиком,
  если отсутствует хотя бы один exact asset;
- build `preview-20260717t1237-briefing-lab-139d9809` сохраняется только как
  regression evidence: он правильно удерживал terminal raster, но удалял
  entry/exit целиком и массово менял crop. Corrective local candidate прошёл
  Playwright `17/17` плюс focused `2/2`; exact screenshots/WebM подтверждают
  irregular entry, conditional exit→next-entry, terminal persistence,
  восстановленные сцены `02/08` и заполненный three-source collage. Gemini 3.1
  Pro High gate сохранён как честная последовательность `FAIL` → correction →
  `PASS`, а не как перенос старого approval;
- immutable corrective build
  `preview-20260717t1346-briefing-lab-85f5a9ee` построен из implementation
  source `85f5a9ee`: isolated allowlist `6 files`, public verification и exact
  noindex passed. Public captures повторили четыре desktop finals; WebM
  фиксируют `entry → hold → exit → next entry` и отдельный terminal state,
  который остаётся с media после `6.5s`. Mobile `320×568`/`390×844` имеет
  `bodyWidth == innerWidth`, `14` review buttons, `mediaMode=none` и `src=null`.
  URL, четыре desktop PNG, два WebM и два mobile PNG отправлены в Telegram
  topic `6` как сообщения `191–199`; delivery verified, повторное чтение до
  `top_message=199` не обнаружило новых входящих комментариев;
- актуальная mosaic — адаптивная CSS-grid `16×5` / `18×5` / `20×5` для
  `1024–1535` / `1536–1791` / `≥1792px`. Она занимает правые `75vw` на любой
  desktop review-width, поэтому больше не сжимается до половины экрана из-за
  `88px` cell-cap. Square cells, `3px` vertical gutters и один cached raster сохранены; горизонтальные gutters удалены;
- final alpha выбирается из семи заметных bands через scenario-seeded,
  deterministic cell/cluster field. Последние два столбца всегда `1`, внутри
  есть длинные острова, локальные reversals, резкие провалы и вспышки; запрещены
  parity-gap/checkerboard и требование «каждый сосед обязан отличаться», которое
  и породило предыдущую шахматную доску;
- текст остаётся на той же shared-shell позиции, что и text-only hero:
  mosaic не меняет stage/message width, font или alignment. Старые `96%`
  background-плашки, `0.08em` padding и четыре расширяющих box-shadow удалены:
  fragment/CTA получают один тонкий `28%` paper wash высотой лишь `12%` строки,
  `1px` glyph halo и нулевой box-shadow; inline event link сохраняет
  один underline, отделённый от wash. Горизонтальный `row-gap` убран совсем,
  чтобы paper gutter не мог выглядеть вторым underline; вертикальный `3px`
  `column-gap` сохраняет mosaic rhythm, а высота пересчитывается по фактической
  ширине cell, поэтому клетки остаются квадратными. Viewport-bound media shell начинается
  ровно около `25vw` (`x=360` при 1440, `x=480` при 1920) и заканчивается на
  физическом правом pixel без horizontal scroll;
- каждая клетка клипует один общий виртуальный image-layer с `background-size:
  cover`; исходный aspect ratio больше не приводится к grid ratio. Для каждого
  lab media указана ручная focal position, а runtime фиксирует natural и cover
  dimensions для regression-проверки равного X/Y scale. До reveal runtime
  вычисляет фактический cover-upscale и скрывает mosaic при значении `>1.10`,
  поэтому ошибочная exporter metadata не может легализовать размытый raster;
- поверх scenario-seeded поля добавлены редкие независимые contrast accents:
  отдельные клетки принудительно уходят в `.03` (washed) или `.96` (bright),
  не меняя непрозрачный двухколоночный right anchor и не создавая parity pattern;
- mosaic разрешена только при `(min-width:1024px) and (min-height:650px)`.
  На mobile и коротком desktop она превращается в обычную text-only сцену и
  URL изображения вообще не назначается. Variant B/reduced-motion показывают
  итоговую матрицу сразу; Variant C использует три irregular entry-beat:
  `40..700ms` delay и индивидуальные `360..759ms` durations. Exit строится из
  независимого, а не reverse-entry seed: `0..279ms` delay и `180..359ms`
  duration. Runtime randomness и saturation не используются; один
  scenario/viewport воспроизводится одинаково. Hover/focus/pause фиксируют
  полностью проявленную композицию;
- во время reveal работает scenario-driven bar/underscore; после законченной
  фразы horizontal underscore остаётся только при реально запланированном
  timed continuation, а в terminal state делает три blink и исчезает;
  media-сценарии weather/forwarding используют underscore уже во время
  fragment reveal, поэтому горизонтальный pending-сигнал не теряется между
  строками и затем непрерывно переходит в ожидание следующего сценария;
- декоративная «О» — отдельный source-faithful asset; составной path всего
  `Анонсы` больше не crop-ится внутри hero.

Предыдущий mosaic baseline immutable prefix
`preview-20260715t2306-briefing-lab-7c2b2a30` построен из source
`7c2b2a30`: isolated build/check pass; Playwright `15/15` pass плюс повтор
pointer-interrupt `3/3`. Public verification на `1440×900`, `1366×768`,
`390×844`, `320×568` вернул HTTP 200, exact noindex, нулевые page errors/bad
responses и `bodyWidth == innerWidth`. Desktop делает ровно один request
выбранного raster; mobile — ноль и `data-media-mode=none`. На 1440 stage
`x=0..1440`, mosaic `x=572..1468`, на 1366 — `x=584.39..1391.2`; таким образом
последний столбец реально crop-ится физическим viewport, а не shared shell.
Gemini 3.1 Pro (High) exact screenshots/video gate: `MOSAIC LAB GATE: PASS`,
`PUBLISH FOR USER REVIEW: YES`; это не production approval.

Предыдущий follow-up Gemini 3.1 Pro (High) verdict `MOSAIC FOLLOW-UP GATE:
PASS` признан **невалидным и superseded**. Prompt заранее сообщил модели
ошибочные success-метрики; ответ повторил их и не заметил, что все 11
горизонтальных переходов каждой строки чередовались, parity means отличались на
`.2367`, правый край содержал `.76/.81/.82/.86`, а `background-size:1200%
400%` растягивал портрет `360×450` в grid ratio `3:1`. Этот ответ нельзя
использовать как visual acceptance evidence.

Immutable prefix `preview-20260716t0544-briefing-lab-4c2caa60` построен из
source `4c2caa60`: isolated build/check pass, Playwright `15/15`. Exact public
captures на `1440×900`, `1366×768`, `390×844`, `320×568` вернули HTTP 200,
нулевые page errors и `bodyWidth == innerWidth`; public `20.5s` WebM показывает
три последовательных mosaic-сценария с reveal/hold/exit, а не один пример.
Build сохраняется только как rejected regression evidence и больше не является
рекомендуемым URL для пользовательского просмотра.

Исправленный immutable prefix
`preview-20260717t0754-briefing-lab-902829dd` построен из source `902829dd`.
Isolated build/check pass; Playwright `15/15` и повтор playback-control
regression `3/3`. Exact public captures на `1920×900`, `1440×900`,
`1366×768`, `390×844`, `320×568` вернули HTTP 200, нулевые page/console errors
и `bodyWidth == innerWidth`. На desktop media начинается ровно с `25vw` и
заканчивается на правом pixel; последние два столбца fully opaque. Natural и
cover dimensions доказывают одинаковый X/Y scale для portrait `360×450` и
horizontal `478×317`; mobile остаётся `data-media-mode=none` без source.
Public `20.5s` WebM показывает три последовательных scenario-seeded поля.

Blind-first Gemini 3.1 Pro (High) exact gate сравнил rejected/candidate pixels,
шесть entry/exit фаз и WebM без заранее сообщённых success-метрик: `MOSAIC
DRAMATIC CORRECTION: PASS`, все шесть пользовательских требований `PASS`,
`PUBLISH FOR USER REVIEW: YES`. Scope/URL опубликован в Telegram topic `6` как
`#113`; desktop finals `#114–116`, entry/exit `#117–118`, mobile `#119–120`,
exact WebM `#121`. Все receipts verified; post-send inspect вернул
`top_message=121` без нового пользовательского комментария.

Этот `PASS` также признан **невалидным и superseded** после следующего
пользовательского visual review. Reviewer не сделал hard-fail на три дефекта,
видимые в переданных pixels: перекрывающиеся непрозрачные fragment slabs и
двойной горизонтальный ритм рядом с underline, растягивание низкоразрешённых
`360×450`/`478×317` источников и конфликт hero copy с текстом внутри poster.
Новый gate не имеет права наследовать этот verdict: stripe occlusion, OCR и
фактический upscale являются отдельными publish blockers.

Финальный corrective prefix
`preview-20260717t0951-briefing-lab-21ca7a49` построен из implementation source
`21ca7a49`. Isolated build/check pass; Playwright `16/16` pass. Exact captures
на `1920×900`, `1440×900`, `1366×768` и `390×844` подтвердили: `row-gap=0`,
только вертикальный `column-gap=3px`, media заканчивается на physical right
pixel, natural source `2560×1707`, cover-upscale `.422/.563`, `bodyWidth ==
innerWidth`; mobile остаётся без raster URL. Named/rare/storm преднамеренно
abstain и показывают text-only состояние. Gemini 3.1 Pro (High) blind-first
gate `2026-07-17 09:48:55–09:50:39 UTC`, status `0`, empty stderr: R01–R04 и
mobile/motion `PASS`, overall `PASS`, `PUBLISH FOR USER REVIEW: YES`, blockers
none. Telegram topic `6`: scope/URL `#144`, desktop `#145–146`, mobile `#147`,
WebM `#148`; receipts verified, post-send top message `148`, новых входящих
комментариев после публикации не было. Это acceptance только isolated lab, не
production homepage.

Отдельный manual-review prefix
`preview-20260717t1049-briefing-lab-38425f28` построен из implementation source
`38425f28`. Он показывает 12 разных event objects и 12 разных exact-source
photos через `review=media`; автоматический переход отключён, прямые `1–12`,
`Назад`, `Повтор`, `Далее` остаются под hero. Isolated build/check прошёл
allowlist из 6 файлов; полный Playwright regression прошёл `17/17`. Первый
Gemini 3.1 Pro (High) gate честно вернул `OVERALL: FAIL` из-за контраста
04/05/07 и обрезанных mobile 10–12; после alpha cap `.24` только у клеток,
пересекающих line boxes, и двухстрочного mobile rail corrective gate вернул
все критерии `PASS`, `PUBLISH FOR USER REVIEW: YES`, blockers none. Telegram
topic `6`: URL `#159`, contact sheet `#160`, сложные desktop states `#161–163`,
mobile `#164`, WebM `#165`; все receipts verified, post-send top message `165`,
новых входящих комментариев после отправки не было. Production не изменён.

Реальная погода, promo overlay, runtime writer и video не включены. Weather
scenes маркируются `DEMO-СИГНАЛ`; production-формулировки требуют свежего
forecast window, совместимых по времени событий и fail-closed expiry. Границы описаны в
[platform backlog](../../backlog/features/static-typed-briefing/README.md).

### Visual harmony + artist/unusual foundation — следующий candidate

Два exact desktop-состояния из пользовательских скриншотов не были частью
предыдущего wide/mobile acceptance и при отдельной проверке Gemini 3.1 Pro
получили `FAIL`: small-media выглядело оторванной карточкой, solid Next
перебивал смысловой CTA, named typography была слишком слабой, а deployed lab
не содержал внешний SVG `Анонсы` и показывал только endorsement. Candidate
исправляет именно эти дефекты:

- isolated build allowlist теперь физически включает и проверяет полный
  `announcements-wordmark-ui.svg`, поэтому утверждённый `240×88` desktop
  lockup не деградирует в один текстовый tier;
- small media живёт во второй колонке одной hero-grid, сохраняет исходное
  `4:5`, не имеет frame/radius/shadow и остаётся небольшим editorial poster;
- named H1 использует bounded desktop scale `56–64px`; weather-сцена остаётся
  в 2–3 строках; terminal Next — ghost/muted и легче основного CTA;
- 1366×768, 1440×900 и 1920×900 проверены отдельно, включая длинное русское
  имя, отсутствие overlap/overflow и source-faithful poster crop.

External gate прошёл последовательность `FAIL` → `PASS WITH CONDITIONS` →
`PUBLISH PASS`; committed prompts/answers находятся в
[consultation evidence](../../reports/static-typed-briefing-consultation-2026-07-15/README.md).
Candidate прошёл isolated build/check, `11/11` Playwright и `4/4` registry
converter tests; опубликован как immutable build
`preview-20260715t2005-briefing-lab-f7d99384` из source `f7d99384`. Telegram
delivery/evidence verified в topic `6`, messages `#77–82`.

Supplied workbook нормализован в
[artist visit registry](../../reference/artist-visit-registry.md), но все 1 235
seed entities намеренно имеют `locality.status=unknown`: присутствие или
отсутствие имени не доказывает неместность. Автоматический 14-day digest
приездов, participant/locality evidence gates и dedupe описаны в
[product contract](../../backlog/features/static-typed-briefing/artist-arrivals-and-unusual-events.md).
Выявление действительно необычных будущих событий вынесено в отдельный
[LLM-first contract](../../llm/unusual-event-detection.md): semantic signature,
season-aware baseline, grounded adjudication и fail-closed public writer;
исторические примеры вроде дня клубники, цветения маков или прогулки с
фонарщиком — только типовые иллюстрации, не текущие факты.

Техническая приёмка immutable build
`preview-20260715t2005-briefing-lab-f7d99384` (`source f7d99384`): isolated
build/check pass; Playwright `11/11` pass; geometry покрывает `17 scenes
including fallback × B/C × 4 viewports = 136`, а named/weather acceptance
отдельно покрывает `1366×768`, `1440×900`, `1920×900` и длинное русское имя.
Mobile media хранит только `data-src` и не делает image request; desktop
small/wide media, source-faithful 4:5 crop, bounded exit,
static/reduced-motion state и нулевой сдвиг categories при exit проверены
отдельно. Public capture вернул HTTP 200 для page/wordmark/wide-O,
`noindex,nofollow,noarchive`, 1–3 строки, `bodyWidth == viewportWidth`, нулевые
page errors/bad responses и видимые категории/начало ленты на `320×568`,
`390×844`, `1366×768`, `1440×900`.

Mobile review gate выполнен в Telegram topic `6`: прежние URL/screenshots/motion
были доставлены как `#51–54`; пользовательские сообщения `#55–57` отвергли
public meta/pause/pace, отсутствие automatic continuation и desktop
frame-inside-frame. Комментарии и аннотированный screenshot были прочитаны до
правки; receipt `#58` подтвердил Gemini gate и scope коррекции. Immutable URL
доставлен как `#66`, `320×568`/`390×844` phase/terminal screenshots — `#67–69`,
desktop wide acceptance — `#70`, slow-chain WebM — `#71`; все send receipts
верифицированы. Visual-harmony build и scope отправлены как `#77`, mobile
`320×568`/`390×844` screenshots и slow-motion — `#78–80`, desktop
1366 crop/1440 weather — `#81–82`; повторное чтение до `#82` не обнаружило
новых входящих комментариев.

### Text-state continuity and grounding follow-up

Пользовательский review показал, что предыдущий `PUBLISH PASS` нельзя было
переносить с named-person/poster crop на непроверенные text-only states.
Отдельные terminal captures `weekend_count`, `weather_water_demo`,
`frequently_forwarded` и `festival_demo` на 1366×768 и 1440×900 получили у
Gemini 3.1 Pro High четыре `FAIL` и overall `FAIL`: градиент заканчивался на
границе 1180px shell и создавал очевидный inner frame. Это publish blocker, а
не вкусовщина.

Follow-up contract:

- background wash идёт edge-to-edge на `100vw`, но текст и действия остаются
  в общей 1180px grid;
- text-only scenes используют единый bottom anchor: actions находятся примерно
  в 64px от нижней границы hero; blanket `translateY` запрещён, потому что
  трёхстрочные сообщения имеют другую высоту;
- `Обещают ясные выходные. Махнём на море?` заменяет искусственные
  `Допустим`/`на воду`;
- singular social narrative прямо называет и открывает grounded event `6466`,
  а не общий `/populyarnoe/`;
- festival scene называет Pianissimo и Максима Милославского и ведёт на event
  `5294`, а не generic search;
- добавлена двухэкранная условная DEMO-цепочка `storm_weekend_demo` →
  `storm_lecture_science_demo`: premise не утверждает наличие текущего прогноза,
  а linked recommendation ведёт на будущую шоу-лекцию `5803` 24 июля. Две
  прежние лекции с прошедшими display dates исключены после grounding review;
  broad `end_date` больше не трактуется как будущий occurrence. В production
  weather premise допускается только при свежем storm/wind forecast и
  date-compatible events; при одном прошедшем gate цепочка остаётся двухэкранной.
- horizontal cursor возвращён как state signal: он остаётся на finished scene,
  пока действительно поставлен таймер следующего сообщения; terminal scene
  делает три blink и убирает cursor, pause/reduced/static скрывают его сразу.

Исходный ответ agy ошибочно назвал уже существующий muted transparent Next
solid/accent и не распознал ссылку `24 идеи`; corrective continuation фиксирует
эти факты. Каноничны выводы про full-bleed seam, bottom anchor и соответствие
narrative→destination, а не ошибочная первая трактовка CTA.

Финальный immutable prefix
`preview-20260715t2212-briefing-lab-53c3021d` построен из source
`53c3021d`: isolated build/check pass, Playwright `14/14` pass. Public captures
на `320×568`, `390×844`, `1366×768` вернули HTTP 200, точный noindex, нулевые
console/request errors, `bodyWidth == viewportWidth`, hero `≤42vh`, видимые
categories/feed и pending cursor. Exact mobile WebM заканчивается на step 2 с
текстом `Шоу-лекция: Суперспособности.`, terminal state `stopped` и bounded
cursor retirement. Gemini focused gate честно прошёл через stale-video `FAIL`,
затем exact corrected-video `LAB PUBLISH PASS`; merge reviewer после grounding
fix не нашёл blockers. Первый созданный до этого audit prefix
`preview-20260715t2150-briefing-lab-1ed2a29c` superseded и пользователю не
отправлялся.

Telegram mobile gate: перед отправкой последний входящий message оставался
`#82`; final scope/URL, `320×568`, `390×844`, exact WebM и desktop `1366×768`
доставлены в topic `6` как `#92–96`. Receipts verified повторным чтением; новых
входящих комментариев после `#96` на момент closure не было.
Mosaic scope/URL, public `320×568`, `390×844`, desktop `1440×900` и full slow
WebM отправлены как `#97–101`; все receipts verified. Повторное чтение topic с
`top_message=101` не обнаружило новых пользовательских комментариев после
отправки.
Follow-up scope/URL отправлен как `#105`; три final desktop-состояния —
`#106–108`, partial reveal — `#109`, public mobile `320×568`/`390×844` —
`#110–111`, exact `20.5s` desktop WebM — `#112`. Все receipts verified;
post-send inspect вернул `top_message=112` без нового пользовательского
комментария.

### Предыдущая curiosity/action-итерация — regression evidence

Основной immutable review URL начинает с curiosity-сцены и принудит выбранный в текущем ревью темп `pace=slow`, но не делает его глобальным default:

<https://kenigevents.ru/preview-20260715t1526-briefing-lab-07143d5e/lab/briefing/?variant=c&scenario=frequently_forwarded&pace=slow&replay=1>

Автоматическая локальная очередь без QA-фиксации сценария:

<https://kenigevents.ru/preview-20260715t1526-briefing-lab-07143d5e/lab/briefing/?variant=c&pace=slow&replay=1>

Приёмка начинается с любопытства и социального сигнала, затем в selector проверяются три обучающих сценария. URL без `scenario` показывает автоматическую локальную очередь: после квалифицированного показа reload выберет следующий eligible-нарратив. URL с `scenario` — ручной QA bypass и не меняет память показов.

На странице оцениваются только: сильная типографика в 1–3 строки, любопытство без манипуляции, поэтапная «печать», вертикальный/горизонтальный курсор, обучающие share/like/«Не интересно» сцены, контроли и быстрый переход в категории. Блок ниже `Дальше начинается лента` — только масштабный контекст, не предмет приёмки.

Все числа помечены `DEMO-ДАННЫЕ`, а социальные/комментарные сцены — `DEMO-СИГНАЛ`. Они не выдаются за production-факты.

Strict build/check и `8/8` Playwright-проверок прошли, включая матрицу `9 scenarios × 2 briefing variants × 4 viewports = 72` и отдельный post-exposure action contract. Public acceptance на `320×568`, `390×844` и `1440×900` подтвердила HTTP 200, noindex, `≤50vh`, категории/начало feed в initial viewport, нулевой horizontal overflow, нулевые page errors и нулевые non-GET requests.

Предыдущий communication-first prefix `preview-20260715t1407-briefing-lab-9c8c9a62` остаётся regression evidence и больше не является текущим concept-review target.

Variants:

| Variant | Contract | Purpose |
|---|---|---|
| `A · control` | briefing отсутствует; сразу категории и feed | baseline first-event discovery |
| `B · static` | весь короткий editorial briefing виден сразу | проверить ценность содержания |
| `C · motion` | тот же текст проявляется 2–5 смысловыми фрагментами; bar/underscore сопровождает reveal, а horizontal underscore удерживает ожидание только при scheduled continuation | проверить исходную коммуникационную механику |

Все варианты находятся на одной QA-странице и используют одинаковые сценарии, категории и feed-context. Public surface содержит только narrative, его смысловой CTA и terminal-only `Показать следующее`. Collapsed LAB-dock после ленты содержит A/B/C, selector всех 19 сцен, progress/demo/status, pace, Pause, `Повторить`, конечный `Показать все 19` и previous/debug actions. Нельзя менять copy между B/C: иначе невозможно отделить motion от content value. Лента подписана как черновой пространственный контекст и не является предметом дизайн-приёмки.

## Минимальная очередь нарративов

Lab содержит 19 видимых scenario IDs и universal fallback. Это смешанная очередь: ориентиры, приветствия, обучение целевым действиям, social curiosity, named-event media и явно маркированные future-signal demos. Все доступны в selector; production eligibility и расширенная библиотека остаются backlog.

| ID | Family / eligibility | Копирайт в lab | Cooldown |
|---|---|---|---|
| `today_count` | count/demo | `Сегодня — 18 идей. Какая зацепит вас?` | 1 день |
| `tomorrow_count` | count/demo | `Завтра — 12 поводов выйти из дома.` | 1 день |
| `weekend_count` | count/demo | `24 идеи на выходные. С чего начнём?` | 1 день |
| `greeting_day` | polite welcome/demo | `Добрый день! Что сегодня вас удивит?` | 1 день |
| `local_keska` | human-approved local tone | `Мы говорим по-калининградски. И скажем «кеска».` | 60 дней, не более 3 в год |
| `smart_search_education` | route education | `Можно просто спросить: «Куда с ребёнком?»` | 30 дней, не более 3 в год |
| `share_education` | action education | `Есть с кем пойти? Нажмите «Поделиться».` | 30 дней, не более 3 в год |
| `like_education` | action education | `Событие понравилось? Отметьте сердцем.` | 30 дней, не более 3 в год |
| `not_interested_education` | action education | `Не ваше? Нажмите «Не интересно».` | 30 дней, не более 3 в год |
| `frequently_forwarded` | bounded social signal, currently demo | `Часто пересылают «Планету Океан». Заглянем?` | 14 дней |
| `anticipated_person` | grounded public-comment signal, currently demo | `В комментариях ждут гостя. Угадаете кого?` | 30 дней |
| `anticipated_person_named` | grounded fixture person/event | `В Светлогорск едет Татьяна Куртукова. Пойдём?` | 30 дней |
| `live_meeting_mosaic` | grounded future event + desktop mosaic | `Живая встреча. Алексей Мышкин. 13 августа.` | 30 дней |
| `rare_event` | grounded fixture event, lab editorial framing | `Редкий формат: Вертинский. Идём?` | 30 дней |
| `weather_water_demo` | fixed future-signal demo; no provider data | `Обещают ясные выходные. Махнём на море?` | 7 дней |
| `storm_weekend_demo` | conditional storm premise demo; no live forecast claim | `Если прогнозируют шторм — может, в уют?` | 7 дней |
| `storm_lecture_science_demo` | one genuinely future linked lecture | `Шоу-лекция: Суперспособности.` | до 24 июля |
| `festival_demo` | fixed concrete-festival demo | `Pianissimo. Максим Милославский.` | 7 дней |
| `unusual_format_demo` | fixed format demo | `Иногда лучший план — паблик-ток. Послушаем город?` | 14 дней |
| `neutral_fallback` | нет eligible-сцен | `Город не ждёт. Что удивит сегодня?` | не запоминается |

Для education-сцен один подтверждённый успех целевого действия **после квалифицированного показа этого нарратива** подавляет повтор на 90 дней. Более старый лайк/share/dislike не считается ответом на новый показ; обычный клик по CTA тоже не считается успехом.

### Локальная память и пределы

- `ke-briefing-memory-v1` хранит только ограниченные timestamps показов и успешных action kinds; `ke-briefing-lab-prefs-v1` хранит только `slow|normal|fast`.
- Не хранятся тексты, user/event IDs, авторы комментариев, auth/profile и история просмотров событий.
- Автоматический exposure пишется только после `≥50%` видимости в течение 250 ms. Query `scenario`, selector, replay и play-all ничего не записывают.
- Реальный action success синхронизируется из уже верифицированного `ke_event_feedback_log_v1` или event `ke:event-action-success`; он удовлетворяет нарратив только если его timestamp новее exposure timestamp. Briefing-клок не выдаёт намерение за успех.
- Кнопка «Сбросить локальную память и темп» удаляет оба ключа.
- Приоритет темпа: QA query `pace` > сохранённый явный выбор > `normal`. Query-параметр не перезаписывает preference.

### Grounding social/comment сигналов

Production-фраза «часто пересылают» допустима только из ограниченного метрикой окна успешных share-действий, а не кликов по кнопке. «В комментариях ждут гостя» требует публичных комментариев, нескольких уникальных авторов, явного ожидания, высокой confidence связи person/entity с этим событием и expiry. Имена авторов и цитаты в briefing не переносятся. Текущий lab не имеет этого data contract, поэтому оба сигнала явно `DEMO-СИГНАЛ`.

В [platform backlog](../../backlog/features/static-typed-briefing/README.md) заданы future-gated families: редкое и благотворительное событие, smart-search education, named/visiting artists, festival window, unusual format, bounded discussion signal, polite/local tone и weather-event chain. Lab включает только безопасные fixed-copy демонстрации некоторых форматов. `anticipated_person_named` и `rare_event` ссылаются на реальные fixture events; weather/festival/unusual остаются явными `DEMO-СИГНАЛ`, не production facts. Благотворительность, происхождение артиста, live comment counts и юмористический факт не симулируются без provenance.

### Minimal manifest shape

```json
{
  "schema_version": "briefing-lab-v0",
  "id": "today_count",
  "chain_id": null,
  "next_scenario_id": null,
  "priority": 100,
  "eligible": true,
  "facts": {"count": 42},
  "headline_template": "Сегодня в городе — {{count}} событий.",
  "supporting_text": "Выберите выставку, концерт или встречу.",
  "cta_label": "Смотреть события",
  "cta_token": "route:today",
  "fact_source": "static_event_export",
  "generated_at": "2026-07-15T08:00:00Z",
  "expires_at": "2026-07-16T00:00:00Z"
}
```

Contract:

- deterministic and build-time validated;
- facts and copy stored separately;
- `generated_at` and `expires_at` required;
- allowlisted route token, not raw model-generated URL;
- no network request in lab render;
- stale, missing or invalid fact selects `neutral_fallback`;
- no Gemini-generated output is admitted automatically.
- optional `next_scenario_id` резолвится только в другой compiled eligible node;
- optional media содержит canonical `event_id`, validated derivative и
  `small|wide|mosaic` mode; mosaic дополнительно требует explicit OCR-safe
  curation, photo/visual-only metadata, safe cover crop, `≥1000px`, `≥1MP` и
  runtime upscale `≤1.10`; отсутствие/ошибка media не инвалидирует text/link;
- `next_scenario_id` задаёт automatic edge; runtime останавливает chain после
  третьей сцены даже при циклическом графе;
- public `Показать следующее` доступен только после terminal stop и начинает
  новую bounded chain; это переход по compiled edge/queue, а не успех CTA/action.

## Layout gate

Исправленный lab проверяет коммуникацию, а не дизайн ещё не спроектированной главной ленты:

- hero занимает `min(42svh, 250px)` на mobile и `min(42svh, 360px)` на desktop, всегда `≤50svh`;
- сообщение имеет 1–3 визуальные строки во всех 19 сценариях на `320×568`, `375×667`, `390×844`, `1440×900`;
- mobile typography примерно `27–34px`, desktop `48–72px`, с отдельными терракотовыми фактами/ссылками;
- пять категорий переносятся на две строки без скрытого горизонтального жеста и остаются видимыми сразу под hero;
- начало контекстной ленты видно, но карточки не являются предметом оценки;
- production `EventLayout` и `EventListItem` оставлены только как масштабный фон, без уменьшения их размеров.
- media существует только на desktop `≥1024px`, не меняет фиксированную
  высоту stage/позицию categories/feed и не получает `src` на mobile;
- mosaic дополнительно требует высоту `≥650px`: CSS-grid содержит `16×5`,
  `18×5` или `20×5` квадратных cells с `3px` paper-gutters и занимает правые
  `75vw` при `1366`, `1440`, `1600` и `1920px`;
  text остаётся на общей desktop-позиции, media пересекает его только визуально
  и читается за счёт узкого полупрозрачного wash/glyph halo, а не сплошных
  paper blocks. Viewport shell заканчивается точно на правом pixel,
  body width не увеличивается. Каждая cell клипует общий source-faithful
  `cover` layer с ручным focal point; один raster URL загружается один раз,
  decode/404 скрывает mosaic без пустого frame.

Автоматическая матрица проверяет все 19 сценариев плюс fallback в B/C: hero и message не переполняются, `scrollHeight <= clientHeight`, строк не более трёх, категории и начало feed находятся в initial viewport. Отдельные проверки фиксируют отсутствие media request на mobile, отсутствие CLS при enter/exit и text-only degradation.

## Reproducible shareable-lab workflow

The lab uses a separate Astro `srcDir` and output root, so it does not run or publish the full catalog build:

```bash
cd site
PREVIEW_BUILD_ID=briefing-lab-$(git rev-parse --short=12 HEAD) npm run lab
```

That single command builds, checks and serves the lab. Local URL для просмотра механики: `http://127.0.0.1:4177/<build-id>/lab/briefing/?variant=c&replay=1`; A/B остаются сравнительными вариантами; выбранный сценарий задаётся через `scenario=<id>`. The build fails closed unless the artifact contains only the lab HTML, hashed Astro CSS, manifest, favicon and exact standalone wide-O asset.

Public publication is a distinct command and accepts only `preview-YYYYMMDDtHHMM-briefing-lab-<sha8>`. It performs recursive copy only into that new prefix, never `sync`, delete, cache purge, root write, stable ICS mutation or production navigation change. Local telemetry remains capped at 24 records in `window.__briefingTelemetry` and can be downloaded from the on-page debug panel; no beacon, XHR, POST, Supabase or analytics transport is created.

### Superseded preview evidence — 2026-07-15

Первый immutable preview `0e94a440` оставлен только как regression evidence и **не подходит для оценки исходной идеи**:

- [A · control](https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=a)
- [B · static](https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=b)
- [C · reveal](https://kenigevents.ru/preview-20260715t1241-briefing-lab-0e94a440/lab/briefing/?variant=c)

Техническая приёмка первого preview прошла: one-route build and five-file allowlist passed; local Playwright passed `3/3`; all eight scenarios plus fallback passed B/C overflow and production-geometry checks at the four required viewports; the public A/B/C × viewport screenshot matrix returned `12/12` HTTP 200 with exact noindex and zero POST/XHR/fetch/beacon/Supabase/analytics/telemetry requests. Evidence is stored locally under `artifacts/codex/static-typed-briefing-shareable-20260715/` and intentionally not committed.

The visual result also preserves the negative finding: at `320×568` the title/decision region is visible, but the unshrunk production card extends below the viewport. Этот shell-centric preview заменён communication-first версией и больше не должен показываться участникам исследования.

### ENOSPC boundary

The earlier full static build failed because the host filesystem was at `99–100%` capacity; this was block-space exhaustion, not a lab defect. Space has since been reclaimed (`7.8 GiB` free, `87%` block use, `12%` inode use at the correction start). This does not change either published artifact, but removes the active capacity blocker for rebuilds. The corrected isolated lab is rebuilt independently; an ordinary full-catalog production build is outside this research acceptance.

## Motion and interaction gate for Variant C

- complete useful text exists in DOM; screen readers do not receive per-character updates;
- 4–5 semantic fragments appear with deterministic ease-out, not one whole-card wipe and not slow literal typing;
- first fragment is visible immediately; total formation is about `0.9–1.5s` depending on `Медленнее / Обычно / Быстрее`;
- terracotta cursor имеет два scenario-driven вида во время reveal; после
  completion горизонтальный underscore продолжает мигать, пока уже
  запланирован следующий timed scene, в terminal state исчезает после трёх
  циклов, а при pause/reduced/static скрыт;
- `Повторить` intentionally restarts the current bounded chain in lab without reload;
- `Показать все 19` в LAB-dock remains a separate QA mode that plays each representative scenario once and stops;
- обычный Variant C на desktop и mobile автоматически продолжает только
  2–3 compiled scenes, затем останавливается; public `Показать следующее`
  появляется лишь после stop;
- Pause/Continue, pace, progress `N из 19`, DEMO/status, Previous/Replay и
  Play-all являются LAB-only и отсутствуют в hero;
- hover, focus, blank-area pointer interaction, hidden tab and BFCache finish the complete sentence and pause; links remain stable and activate on the first tap. Если scroll подводит stage под уже неподвижный pointer и успевает создать hover-pause, следующий blank tap фиксирует explicit pause, а не неожиданно возобновляет chain;
- `prefers-reduced-motion` shows the complete current scene with manual controls and no auto-advance;
- B has the exact same current message fully static; A removes only the communication surface.

The large decorative `о` is a separate `aria-hidden` image whose SVG contains only the exact two-contour O subpath from the master announcement wordmark (`viewBox="2571 410 1600 1104"`). `<use>` compound path всего слова и текстовая подделка удалены.

## Phrase-shaped mosaic light, face safety and kinetic accents — 2026-07-17

Последняя desktop-mosaic итерация больше не строит осветление от фиксированной
левой границы. После формирования фактической фразы браузер собирает
`getClientRects()` всех `[data-reveal-fragment]`, объединяет их в визуальные
строки и для каждой строки ставит начало light field сразу после её реального
правого края. Поэтому короткая строка открывает фотографию раньше длинной, а
контур прозрачности следует текущему сгенерированному тексту, включая переносы.
Геометрия пересчитывается через один coalesced `requestAnimationFrame` после:

- замены/мутации текста;
- изменения размеров message или mosaic root;
- загрузки шрифтов;
- resize viewport.

Диагностический контракт lab: `data-copy-line-count`,
`data-copy-line-rights`, `data-copy-geometry-revision` на mosaic root и
`data-copy-line`, `data-illumination-origin-x`, `data-field-progress` на
клетках. Эти атрибуты являются QA hooks, а не production analytics.

Face safety применяется после copy shield и после точного расчёта
`cover`/`contain` геометрии каждого source panel. Нормализованный bbox
проецируется только в панель своего `source_order`, расширяется небольшим
padding и помечает все пересекающиеся клетки. Их итоговая opacity не может быть
ниже `0.50`, даже если клетка одновременно находится под текстом: paper stripe
сохраняет читаемость текста, а лицо не распадается на пустые отверстия. В
portrait collage metadata источников не смешивается. Текущие четыре набора
ручных bbox нужны только как явно помеченный `lab-fixture`; когда массив
`image_assets[].face_boxes` приходит из exporter, он имеет приоритет.
Канонический будущий producer/exporter contract: [face bbox contract](./typed-briefing-face-bbox-contract.md).

Основная tile-кинематика остаётся управляемой symmetric ease-in-out
`cubic-bezier(.65,0,.35,1)`. После фильтрации text/face cells детерминированно
выбираются 2–3 разнесённые source-backed клетки для позднего design beat:
примерно `900–980ms` delay и `380–500ms` duration, не позднее общего
`1560ms` entry budget. Если двух безопасных клеток нет, эффект не создаётся за
счёт лица или текста. Exit использует ту же кривую, отдельные более короткие
тайминги и запускается только перед реальным автоматическим successor.
`prefers-reduced-motion` остаётся мгновенным, mobile/короткий desktop не
запрашивают narrative raster.

Acceptance evidence:

- isolated build/allowlist: PASS;
- Playwright: `18/18`, включая реальную мутацию длины второй строки,
  per-source face floor, collage isolation, deterministic accents, computed
  easing, resize/font/mutation recalculation и mobile silence;
- Gemini 3.1 Pro (High), strict pixel/code gate: `PASS`, blockers `0`;
- локальные screenshots/WebM и redacted diagnostics:
  `artifacts/codex/typed-briefing-dynamic-shield-face-kinetics-20260717/`.

Это по-прежнему isolated lab, а не разрешение на rollout homepage. Значение
`0.50` нельзя автоматически понижать ради большей «драмы»: такое изменение
требует нового face-legibility A/B review и не должно возвращать уже отвергнутые
пустые клетки на лицах.

## Experiment sequence

Do not launch A/B/C/D simultaneously without traffic/MDE evidence.

### Experiment 1

```text
A: no briefing
B: static editorial briefing
```

Only if B wins without harming first-event visibility/performance:

### Experiment 2

```text
B: static briefing
C: one short reveal
```

Only after a general briefing wins:

### Experiment 3

```text
general deterministic briefing
vs
coarse/personalized briefing
```

Before any experiment, define baseline eligible sessions, event-open rate, unit of randomization, MDE/sample size, duration, returning-user assignment, deep-link/search exclusions and multiple-comparison policy.

## Metric and telemetry contract

**Primary production outcome (future experiment):**

```text
event_detail_open_rate =
  unique eligible_listing_sessions with >=1 destination-confirmed event_detail_open
  / unique eligible_listing_sessions
```

An `eligible_listing_session` starts on the eligible home/general listing surface, has a stable experiment assignment, becomes visible and has at least one active event target. The denominator is not conditioned on briefing impression: otherwise control sessions and failed treatment delivery disappear from the comparison.

The isolated lab cannot prove `event_detail_open`: a source-page click shows activation, not successful destination rendering. V0 therefore uses only an in-memory debug sink and records `event_detail_activate`. A future public experiment may call it `event_detail_open` only after the destination event page confirms a visible load through a consented, server-accepted, bot-filtered and deduplicated path.

### Logical events

```text
eligible_session
briefing_impression
briefing_complete {completion_kind: static|natural|interrupt|reduced_motion}
briefing_interrupt {reason: pointerdown|focusin|scroll|visibility_hidden}
first_event_visible
event_detail_activate       # lab/source-side proxy only
event_detail_open           # future destination-confirmed outcome
ticket_click
calendar_add
share
```

Delivery semantics:

- `eligible_session`: once per experiment/session for A, B and C; also the intention-to-treat denominator;
- `briefing_impression`: B/C only after at least 50% of the block is visible for 250 continuous visible milliseconds;
- `briefing_complete`: B after qualified static impression; C after natural end or forced completion;
- `briefing_interrupt`: first causal interrupt only; reduced motion is a delivery state, not an interrupt;
- `first_event_visible`: stable first-card title/decision region at least 90% visible for 250 continuous visible milliseconds;
- `event_detail_activate`: source link activation; never silently promoted to the primary outcome;
- `ticket_click`: requested ticket/register/source/phone action, not purchase proof;
- `calendar_add`: ICS request, not proof of calendar import;
- `share`: only after native share or clipboard copy succeeds.

The lab sink is bounded and local:

```text
window.__briefingTelemetry
```

It makes UI/test semantics observable but sends no network request and creates no production telemetry write path. QA query assignments are labeled `assignment_source=qa_query` and excluded from causal aggregates.

Bounded payload: schema version, ephemeral session/page-view IDs, experiment ID, variant, assignment source, scenario ID, copy/build version, viewport class, reduced-motion flag, active-visible time rounded to 100ms, event ID/rank and event-specific reason/outcome. Never store or emit rendered free text, auth token, raw profile, email, exact geolocation, search history or full URL/referrer.

### Session and dedupe

- experimental unit is a session; V0 may use an ephemeral `sessionStorage` UUID;
- assignment remains stable through reload/BFCache inside the session;
- `eligible_session`, impression, complete and first-event-visible emit at most once per session/experiment;
- BFCache restore does not replay reveal or duplicate events;
- hidden time is excluded from visibility timers;
- telemetry failure never delays navigation or other actions.

### Guardrails

- `initial_first_event_visible_rate` including non-viewers in the denominator;
- `time_to_first_event_visible` p50/p75 plus non-view rate;
- mobile binary gate at `320×568` and `375×667`;
- listing scroll depth;
- ticket/calendar/share session rates;
- `briefing_interrupt_rate`;
- LCP, INP, CLS and JS/fallback errors;
- return-7d only when identity/consent and sample size make it valid.

A public experiment still needs baseline eligible sessions/open rate, unit of randomization, MDE/sample size, duration, consent basis, destination instrumentation, bot/monitor exclusion, server dedupe and persistent aggregates. Until then the primary metric is a contract, not a measured result.

## Eight P0 prototype blockers

1. Without JavaScript, B/C show the full useful briefing.
2. On mobile, hero is `≤50svh`, all five category actions and the beginning of the explicitly secondary feed are visible initially.
3. `prefers-reduced-motion` disables reveal completely.
4. Hover, focus and pointer interaction complete the sentence and expose a stable paused state; Replay works intentionally.
5. BFCache restore does not replay the reveal.
6. Missing/stale/invalid facts select the neutral fallback.
7. B/C retain identical scenario copy; C exposes distinct fragment states, a
   maximum-three-node automatic chain and terminal-only public Next; finite
   play-all, Pause, progress and three discrete pace controls stay in LAB-dock.
8. Eligible session, impression, completion/interruption, first-event visibility and source-side event activation are distinguishable in the local lab sink; no activation is mislabeled as destination `event_detail_open`.

P1/P2 platform checks, the extended scenario platform, personalization, Gemini writer boundaries and the extended risk register are preserved only in the [post-validation backlog](../../backlog/features/static-typed-briefing/README.md).

## Consultation provenance

Committed evidence:

- [evidence README and decision trace](../../reports/static-typed-briefing-consultation-2026-07-15/README.md);
- [prompt v1](../../reports/static-typed-briefing-consultation-2026-07-15/prompt-v1.md);
- [Gemini Part I](../../reports/static-typed-briefing-consultation-2026-07-15/gemini-part1.md);
- [corrective prompt v2](../../reports/static-typed-briefing-consultation-2026-07-15/prompt-v2.md);
- [Gemini Part II](../../reports/static-typed-briefing-consultation-2026-07-15/gemini-part2.md);
- [focused rejected-media prompt and Gemini FAIL](../../reports/static-typed-briefing-consultation-2026-07-15/media-correction-gemini.md);
- [post-change Gemini acceptance](../../reports/static-typed-briefing-consultation-2026-07-15/media-acceptance-gemini.md).

The model was Antigravity/agy `Gemini 3.1 Pro (High)`, run twice as one correlated consultation thread from input commit `926dad8a91fc7f1070126d32a05281aa92ff1666`. Checksums and accepted/corrected/deferred decisions are in the evidence README.

The later screenshot-specific correction gate used the same display model on
2026-07-15 at 18:24–18:45 UTC. It rejected the nested-container version,
confirmed the full-viewport replacement as `PASS WITH CONDITIONS`, and its two
remaining implementation conditions (continuous paper protection and wrapped
terminal actions) were applied. This is a visual prototype gate, not user or
metric validation.

## External-audit traceability

| Requirement | Resolution |
|---|---|
| R01 status/evidence | `GO_TO_PROTOTYPE_ONLY`; user/metric validation false |
| R02 no Gemini/personalization MVP | excluded from V0; retained only in appendix |
| R03 initial ≤8 scenarios + fallback | первоначально выполнено как 8+fallback; последующий явный user-review superseded этот showcase-limit, поэтому selector расширен до 19+fallback без изменения production gate |
| R04 isolated A/B/C lab | `/lab/briefing/` |
| R05 mobile visibility | hero `≤50svh`; categories and beginning of secondary feed visible; message remains 1–3 lines |
| R06 no production/deploy | explicit lab-only scope |
| R07 telemetry contract | bounded local/debug events above |
| R08 one primary metric | `event_detail_open / eligible_listing_session` |
| R09 eight P0 checks | eight blockers above; remainder appendix |
| R10 consultation provenance | committed prompts, outputs, hashes, SHA and decision trace |

## Next decision

The branch may be reviewed as a **minimal lab prototype**, not as an approved homepage feature. Production work remains blocked until:

1. production static-site baseline funnel exists;
2. lab demonstrates first-event visibility and accessibility;
3. Experiment 1 has a feasible sample-size plan;
4. B shows downstream event discovery value without performance or feed-discovery harm.

If B does not win, remove the feature rather than expanding scenarios, motion or personalization.
