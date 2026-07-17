# Mobile event UI lab — 2026-07-15

Статус: **preview-эксперимент, не production contract**.

Решение владельца после проверки на реальном Android: **open prose принят**,
graphite action dock принят как направление. Следующий `accepted-v2` candidate
исправляет три обнаруженных на телефонных скриншотах дефекта, не переписывая
исходную матрицу 2×2. `accepted-v3` сохраняет v2 как точку сравнения и отдельно
отвечает на следующий Android feedback по лайку, подписям, дню недели и OCR gap.
`accepted-v4` сохраняет принятую дату/время и исправляет переусложнение v3:
возвращает poster-parallax, убирает ring/check у лайка и чистит ритм между
информационными поверхностями. Проверка владельца показала, что v4 развернул
OCR-parallax не в ту сторону, а один только gap не устранил оптический разрыв.
`accepted-v5` поэтому инвертирует движение и вводит единое gradient-продолжение
для medallions + description вместо ещё одной границы между блоками.
Проверка v5 на Android обнаружила, что собственный gradient-background wrapper
всё ещё начинался жёсткой горизонтальной линией в нижних скруглениях decision.
`accepted-v6` переносит градиент на прозрачный псевдослой, поднятый до фото, и
добавляет одинаковое состояние скачанного ICS для mobile и desktop. Проверка
движения выявила, что OCR-постер всё ещё шёл заметно медленнее photo hero:
его прогресс делился на полную высоту вертикальной афиши. `accepted-v7`
нормализует скорость относительно обычного mobile photo hero, не добавляя
масштаб и не меняя безопасный диапазон `-travel → 0`. Проверка владельца
показала, что после достижения `0` V7 заметно останавливается ещё до ухода hero.
`accepted-v8` поэтому продолжает движение через `0` к `+travel` с той же
скоростью, что photo hero. Для безопасного движения в обе стороны используется
симметричный crop по `10%` сверху/снизу текущего кадра без масштабирования.

## Решения после повторного аудита

Предыдущая формулировка про «перекрывающий контент lockup» снята: фиксированная
терракотовая бирка — намеренный брендовый приём. Также не считаются дефектами и
не меняются в этом эксперименте:

- инверсия светлых desktop и графитовых mobile discovery cards;
- различная media policy для OCR-постеров и фотографий;
- терракотовая декоративная линия у decision block.

Проверяются только два подтверждённых расхождения:

1. большой rounded container вокруг editorial prose уменьшает полезную ширину;
2. calendar/share/like выглядят как разрозненные secondary actions.

## Матрица 2×2

| Вариант | Description | Actions | Что изолирует |
|---|---|---|---|
| `control` | текущая card | текущие | точка сравнения |
| `open-prose` | открытый молочный canvas | текущие | только экономию ширины текста |
| `action-dock` | текущая card | графитовый dock | только группировку действий |
| `open-prose-action-dock` | открытый canvas | графитовый dock | совместный результат |
| `accepted-v2` | открытый canvas | адаптивный графитовый dock | принятые решения + mobile corrections |
| `accepted-v3` | открытый canvas | container-aware графитовый dock | feedback по лайку, labels, weekday и OCR gap |
| `accepted-v4` | открытый canvas | container-aware графитовый dock | owner correction: параллакс, простой active like, vertical rhythm |
| `accepted-v5` | единая gradient continuation surface + open prose | container-aware графитовый dock | owner correction: reverse parallax и seamless decision→context composition |
| `accepted-v6` | gradient rise из-под photo + open prose | тот же dock + shared calendar state | без paint-edge у скруглений; `Добавлено` одинаково на mobile/desktop |
| `accepted-v7` | тот же seamless/open surface | тот же dock + shared calendar state | одинаковая воспринимаемая скорость OCR/photo parallax без OCR zoom |
| `accepted-v8` | тот же seamless/open surface | тот же dock + shared calendar state | непрерывный OCR parallax всё время видимости hero, без остановки на `0` |

Open prose убирает только фон, border, radius, shadow и лишний inner padding у
описания. Source gate остаётся отдельным компактным объектом. Action dock
сохраняет текстовые labels для calendar/share и touch targets не менее `44px`;
icon-only вариант не принимается из-за неоднозначности и accessibility.

## Реальные stress-cases

- event `5658`: фотография, ticket CTA, calendar/share/like, длинный prose;
- event `5761`: visual/free-like и другой набор действий;
- event `5878`: OCR poster в `contain`, без унификации с photo hero.

Индекс lab публикует все 33 комбинации в `390×844` iframe и даёт отдельную
ссылку каждой комбинации: `/lab/event-mobile/`.

Preview builds:

- original matrix: `preview-20260715t-mobile-ui-variants-v1`;
- accepted candidate: `preview-20260715t-mobile-ui-accepted-v2`.
- feedback candidate: `preview-20260715t-mobile-ui-accepted-v3`.
- owner-correction candidate: `preview-20260715t-mobile-ui-accepted-v4`.
- reverse/continuation candidate: `preview-20260715t-mobile-ui-accepted-v5`.
- seamless/calendar-state candidate: `preview-20260715t-mobile-ui-accepted-v6`.
- matched-parallax-velocity candidate: `preview-20260715t-mobile-ui-accepted-v7`.
- continuous OCR-parallax candidate: `preview-20260715t-mobile-ui-accepted-v8`.

## Accepted v2 corrections

Телефонные screenshots подтвердили:

1. hero был шириной `100vw`, но начинался от левого padding родительского
   `page-shell`, поэтому выходил вправо на `12px`; v2 центрирует full-bleed
   относительно viewport через симметричные `calc(50% - 50vw)` margins;
2. дата/время терялись после массивного action dock; v2 выделяет `when` и
   `where` отдельными semantic spans и усиливает `when` цветом, размером и весом;
3. квадратный текстовый poster event `5761` ошибочно имел fixture status
   `visual_only` и получал photo cover/parallax zoom; v2 явным review override
   проверяет `poster-stage + poster-billboard`, не меняя framing настоящей photo.

На `<380px` accepted v2 оставляет label ровно у одного secondary action, а
остальные сохраняет как `48px` icon controls. Выбор выполняется при статическом
рендере без hydration shift: ticket label имеет приоритет; при calendar+share
чётные event id показывают calendar, нечётные — share; если calendar отсутствует,
label получает share. На `390px+` видны обе подписи.

## Accepted v3 corrections

Следующий телефонный feedback показал, что v2 исправил геометрию, но не закрыл
четыре interaction/visual дефекта:

1. выбранный лайк почти не отличался от обычного состояния;
2. viewport breakpoint `<380px` не описывал фактическую внутреннюю ширину dock
   на Android, поэтому обе подписи могли оставаться видимыми;
3. усиление одной терракотовой строки date/time оставалось визуально слабым и не
   показывало день недели;
4. poster-parallax сдвигал весь OCR visual через transform, не меняя его layout
   box, и создавал пустую полосу перед decision block.

V3 сохраняет open prose и no-zoom `poster-stage`, но:

- делает active like терракотовым solid-control с контрастным ring, solid-heart,
  отдельным check badge и `aria-pressed="true"`; состояние различается не только
  цветом;
- увеличивает secondary icons с `1.08rem` до `1.28rem`, оставляя touch targets
  не меньше `52px` в v3 dock;
- использует CSS container query по фактической внутренней ширине action dock:
  до `400px` виден ровно один label, остальные действия остаются icon controls с
  полным `aria-label`; на более широком контейнере возвращаются обе подписи;
- разделяет метаданные на weekday chip, дату, табличное время и отдельную строку
  места на плоской divider-surface вместо ещё одной вложенной beige card;
- отключает translate-parallax у v3 `poster-stage`, сохраняя intrinsic ratio,
  `object-fit: contain`, нижний OCR-текст и стандартный decision overlap.

V3 — новый изолированный noindex preview, а не production promotion.

## Accepted v4 corrections

Владелец принял новую date/time hierarchy, но отклонил два излишних решения v3:
отмену poster-parallax и ring + check badge у active like. Дополнительно он указал на
грязный переход между первой context surface и основным prose/info block.

V4:

- не меняет weekday/date/time и container-aware labels;
- показывает active like только плотной терракотовой заливкой и белым
  solid heart; внешний ring, check badge, lift и shadow отсутствуют;
- оставляет poster image в `contain`/natural-width без `scale` и `cover`, но двигает
  image внутри клиппируемого visual viewport на `39.6–47.3px` для ширин
  `360–430px`; negative bottom margin резервирует этот travel в layout, поэтому ни в одной
  scroll-position не открывается пустая полоса;
- задаёт не менее `12px` между hero decision и context/medallion surface и не менее
  `18px` между context surface и основным description block.

V4 остаётся изолированным noindex preview; общий production hero-parallax не меняется.

## Accepted v5 corrections

Владелец отклонил направление v4 parallax и отметил на Android screenshot
«ложный подвал» между rounded decision card и первым medallion. V5 не лечит это
ещё одним отступом или новой карточкой:

- poster image остаётся `contain`/natural-width без `scale` и `cover`; на старте
  он находится в `-travel`, а при scroll движется к `0` по формуле
  `-maxOffset + progress * maxOffset` — то есть в сторону, обратную v4;
- travel остаётся ограниченным `36–48px`, а clip viewport всегда покрыт image
  bounds сверху и снизу;
- crumbs, medallions и open description объединены одним DOM-wrapper
  `.mobile-event-review__continuation`; для старых вариантов wrapper имеет
  `display: contents`, поэтому их геометрия не меняется;
- только v5 делает continuation full-bleed parent surface: `margin-top:-24px`
  заводит её под rounded decision, компенсирующий top padding возвращает контент
  в поток, а gradient от тёплого milk/taupe к transparent создаёт плавное
  продолжение;
- у continuation нет border, radius и shadow; у decision border прозрачен и
  shadow отключён, но нижние rounded corners остаются видимы за счёт мягкого
  тонального контраста parent gradient;
- отдельная card surface вокруг prose не возвращается; medallions и description
  воспринимаются содержимым одного родителя.

V5 также остаётся noindex preview и не меняет production mobile event template.

## Accepted v6 corrections

У v5 первый непрозрачный stop принадлежал прямоугольному paint-box continuation,
который начинался на `24px` под decision. Центр перекрывался самой карточкой, но
в её нижних rounded corners фон открывался как горизонтальная полка; на 390px
pixel audit показал скачок до `26` RGB-уровней за одну строку.

V6 сохраняет DOM, OCR framing и reverse parallax v5, но:

- у continuation теперь `background:none`, поэтому на его top-coordinate нет
  собственной линии отрисовки;
- градиент рисует `::before`, поднятый на `clamp(28rem,110vw,32rem)` — прозрачная
  часть начинается внутри фото, а тон набирается постепенно за decision;
- у decision настоящий `border:0`, у обеих поверхностей нет shadow/radius/border;
- medallions и prose остаются содержимым одного semantic continuation wrapper;
- календарная ссылка после успешного fetch + запуска ICS download показывает
  `Добавлено`; состояние синхронизируется на всех mobile/desktop controls этого
  event id и автоматически удаляется на следующий день после события.

V6 — отдельный noindex preview; production mobile template не продвигается.

## Visual QA

- проверены все `4 × 3` cases на ширинах `360`, `390`, `430` и `768px`;
- у action dock все targets имеют высоту `48–52px`; на `360px` три secondary
  actions остаются в одной строке, а при двух actions share занимает остаток;
- open prose даёт тексту на `390px` дополнительные `26px` полезной ширины;
- фото и OCR poster продолжают использовать исходные разные композиции;
- у текущего mobile hero сохранён существующий `12px` full-bleed overhang — это
  общий control invariant, а не эффект какого-либо варианта.

Для `accepted-v3` отдельно проверены `3` real-event scenarios на ширинах `360`,
`390`, `430` и `768px`:

- на phone нет horizontal overflow, hero rect совпадает с viewport;
- все secondary targets не меньше `52px`;
- на `360–430px` container query оставляет ровно одну доступную подпись, на
  широком dock показывает все существующие labels;
- у `5761` и `5878` на phone `object-fit: contain`, image и visual заканчиваются
  на одной координате, decision block перекрывает media без transform-gap;
- weekday присутствует во всех трёх сценариях;
- like меняет `aria-pressed`, count, opaque fill, border и outline→solid heart,
  сохраняется после reload и корректно снимается повторным tap.

Машиночитаемые metrics и `13` screenshots сохранены в локальном artifact
`artifacts/codex/mobile-ui-v3-qa-20260715/`. Полный Astro preview build (`454`
pages) и `check-preview.mjs` завершились успешно.

Для `accepted-v4` повторена та же матрица `360/390/430/768 × 3`:

- horizontal overflow и hero viewport offset отсутствуют;
- poster остаётся `contain`, image transform фактически меняется после
  scroll, а image bounds покрывают visual viewport до и после сдвига;
- вертикальные gaps не падают ниже `12px` и `18px`;
- active like имеет opaque terracotta fill, transparent border, white solid heart, без
  pseudo-element badge и box-shadow; tap/reload/toggle-off persistence пройден.

Артефакты: `artifacts/codex/mobile-ui-v4-qa-20260715/`.
Полный preview build собрал `457` страниц; обновлённый `check-preview.mjs` прошёл.

Для `accepted-v5` проверены `360/390/430/768 × 3` scenarios:

- на телефонах hero и continuation совпадают с viewport, horizontal overflow нет;
- у OCR/poster на `360/390/430` initial transform равен
  `-39.6/-42.9/-47.3px`, после scroll `180px` —
  `-17.4/-20.7/-25.1px`: движение идёт к нулю и визуально вниз;
- до и после scroll `image.top <= visual.top`, а
  `image.bottom >= visual.bottom`, поэтому reverse parallax не открывает фон;
- continuation начинается на `24px` за rounded decision, не имеет
  border/radius/shadow; на phone medallions идут через `26.1–28px`, details —
  через `17.6–19.8px` после medallions;
- weekday/date/time, touch targets `>=52px`, container-aware labels и простой
  terracotta active-like с reload/toggle-off persistence сохранены.

Артефакты: `artifacts/codex/mobile-ui-v5-qa-20260715/`.
Полный preview build собрал `460` страниц; обновлённый `check-preview.mjs` прошёл.

Для `accepted-v6` проверены `360/390/430 × 3` scenarios:

- continuation имеет `background-image:none`, а его `::before` поднимается на
  `448–473px` внутрь hero/photo и содержит единственный gradient;
- в бывшей точке top-edge на обоих viewport gutters максимальный one-row RGB
  delta равен `1` (у v5 было до `26`), то есть жёсткая линия устранена;
- decision и continuation имеют нулевые borders, без radius/shadow у
  continuation; horizontal overflow отсутствует, targets остаются `>=52px`;
- OCR сохраняет `contain`, покрытие clip viewport и принятое движение
  `-travel → 0`: `-39.6/-42.9/-47.3px` при старте и
  `-17.4/-20.7/-25.1px` после scroll `180px`;
- реальный browser download event дал `kenigevents-event-5658.ics`; компактное
  состояние пережило mobile reload, отобразилось `Добавлено` на desktop той же
  страницы, повторный click не создал дубль;
- cleanup удалил expired/corrupt values и ограничил синтетические `300` записей
  до `256` ближайших будущих.

Артефакты: `artifacts/codex/mobile-ui-v6-calendar-qa-20260715/`.
Полный Astro build собрал `463` страницы.

Для `accepted-v7` Playwright сравнил контрольный V6 OCR, V7 OCR и обычный
photo hero при `360/390/430 × 844` и scroll `0/60/120/180px`:

- за первые `120px` scroll V6 OCR проходил только `14.8px`, тогда как photo —
  `28.4px`; это подтверждает замеченное владельцем замедление;
- V7 OCR проходит те же `28.4px`, что photo, на всех трёх ширинах; relative
  velocity error равен `0`;
- OCR остаётся `object-fit:contain`, `scale=1`; на всех samples изображение
  полностью перекрывает clip viewport и не открывает фон;
- движение сохраняет направление `-travel → 0`, horizontal overflow нет;
- при `prefers-reduced-motion:reduce` transform остаётся `0 → 0`.

Артефакты: `artifacts/codex/mobile-ui-v7-parallax-qa-20260715/`.
Полный Astro build собрал `466` страниц; `check-preview.mjs` прошёл.

Для `accepted-v8` Playwright сравнил V7 OCR, V8 OCR и photo hero при
`360/390/430 × 844` и scroll `0/60/120/180/240/300px`:

- V7 после `180–240px` достигает `0` и остаётся неподвижным, хотя OCR visual
  ещё виден; замечание владельца воспроизведено;
- V8 на каждом шаге продолжает движение примерно на `14.2px`, как photo hero:
  например, на `390px` значения идут
  `-39/-24.8/-10.6/3.7/17.9/32.1px` без плато на `0`;
- clipped visual имеет высоту `288/312/344px`, а расчётный конец движения
  наступает позже его ухода из viewport, поэтому остановка не видна;
- OCR остаётся `contain`, `scale=1`, суммарный crop ограничен `20%`, image
  покрывает visual на всех samples, horizontal overflow отсутствует;
- при `prefers-reduced-motion:reduce` transform остаётся `0 → 0`.

Артефакты: `artifacts/codex/mobile-ui-v8-parallax-qa-20260715/`.
Полный Astro build собрал `469` страниц; `check-preview.mjs` прошёл.

## External consultation

Консультация выполнена через `agy` моделью `Gemini 3.1 Pro (High)`. Gemini
предложил control, seamless prose и grouped actions. Для более честного решения
предложение преобразовано в факторную матрицу 2×2: так можно отдельно принять
open prose, отдельно action dock или только их комбинацию. Raw response хранится
в локальном, некоммитимом artifact
`artifacts/codex/static-mobile-ui-variants-20260715/gemini-3.1-pro-high-variant-review.raw.md`.

Повторный screenshot audit той же Pro-моделью подтвердил все три дефекта и
рекомендовал viewport-centred full-bleed, контрастную semi-bold date/time строку,
contain-oriented OCR policy и детерминированную server-rendered label priority.
Первый расширенный вызов истёк по времени; успешный узкий повтор сохранён в
`artifacts/codex/mobile-ui-telegram-review-20260715/gemini-3.1-pro-high-screenshot-audit-retry.raw.md`.

V3 acceptance review выполнен через `agy` моделью `Gemini 3.1 Pro (High)` по
реальным 390px screenshots. Все четыре feedback-пункта получили `PASS`, P0/P1
визуальных регрессий не найдено; reviewer признал v3 готовым к отправке владельцу
как отдельный prototype. Raw response:
`artifacts/codex/mobile-ui-v3-qa-20260715/gemini-3.1-pro-high-review.raw.md`.

V7 motion review через `Gemini 3.1 Pro (High)` проверил только активную скорость
на первых `120px` и поэтому не заметил последующее плато на `0`; после owner
feedback этот review не считается финальным acceptance. Raw response сохранён
для аудита ошибки:
`artifacts/codex/mobile-ui-v7-parallax-qa-20260715/gemini-review.raw.md`.

V8 acceptance review выполнен той же `Gemini 3.1 Pro (High)`, но уже по всему
видимому диапазону `0–300px`. Консультант подтвердил, что расчётная остановка
наступает после ухода clipped visual из viewport, bounded `20%` crop оправдан
запретом zoom, P0/P1 нет; итоговый verdict `PASS`. Raw response:
`artifacts/codex/mobile-ui-v8-parallax-qa-20260715/gemini-review.raw.md`.

Поправка v4 также обсуждена с `Gemini 3.1 Pro (High)`. Два первых print-mode
вызова завершились пустым stdout; после проверки CLI log и точечного
исследования известного silent-empty print-mode поведения успешный узкий повтор
выдал ответ. Консультант верно рекомендовал двигать image внутри
клиппируемого container и убрать ring/check, но ошибочно прочитал `360–430`
как диапазон gap, а не ширину viewport. Эта часть совета отклонена; реальные gaps
зафиксированы QA на `≥12px` и `≥18px`.
После уточнения фактической геометрии финальный узкий gate-review той же
Pro-модели дал `PASS` для parallax, like и vertical rhythm; P0/P1 не найдены. Raw
response: `artifacts/codex/mobile-ui-v4-qa-20260715/gemini-v4-acceptance-review.raw.md`.

Для v5 выполнены screenshot diagnosis и финальный gate через `agy` моделью
`Gemini 3.1 Pro (High)`. Диагноз `false floor` принят: нижняя граница/shadow и
мертвая молочная полоса заменены overlap + gradient continuation. Совет
консультанта об общей bordered/shadowed outer card и `object-fit: cover`
отклонён как противоречащий прямому feedback владельца, open-prose решению и
OCR no-zoom contract. Финальный review дал `PASS` направлению reverse parallax,
gap-safety и composition; P0/P1 blockers не найдено. Raw response:
`artifacts/codex/mobile-ui-v5-qa-20260715/gemini-v5-acceptance-review.raw.md`.

V6 финально проверен через `agy` моделью `Gemini 3.1 Pro (High)` по реальным
mobile/desktop screenshots, pixel/geometric measurements и browser calendar
E2E. Reviewer дал `PASS` seamless gradient, OCR regression safety и честному
compact calendar acknowledgement; P0/P1 blockers для отдельного preview не
найдено. Raw response:
`artifacts/codex/mobile-ui-v6-calendar-qa-20260715/gemini-v6-acceptance-review.raw.md`.

## Task-channel workflow

Telegram topic, назначенный пользователем каналом задачи/приёмки, не является
progress-log. Actionable feedback запускает полную итерацию
`feedback → implementation → QA → preview publish → один acceptance handoff`;
receipt-only сообщения и обещания прислать результат в следующем запуске
запрещены проектным `AGENTS.md`.

## Acceptance gate

До переноса в основную event page требуется принять либо отклонить v6 seamless
gradient rise и shared mobile/desktop calendar acknowledgement. Reverse clipped
no-zoom poster parallax уже принят владельцем и остаётся regression contract.
Унаследованные simplified active-like, icons/container-aware labels/date-time
также остаются видны в preview, но этой итерацией не переутверждаются. Discovery
и brand tag не меняются.

## Production integration v3 — accepted V8 is the generated mobile contract

This section supersedes the earlier laboratory-only acceptance gate. The exact
accepted V8 source line from commit `fd8766b1` is now integrated into the shared
production event route rather than reimplemented “по мотивам”. Desktop and
mobile remain separate responsive surfaces: below `1024px` only
`[data-production-mobile-event]` is visible; at desktop widths only the accepted
Continuous Editorial/Split surface is visible.

Every one of the `282` future/ongoing generated event routes now carries these
hard markers and behaviors:

- `data-mobile-review-variant="accepted-v8"`;
- `data-mobile-review-revision="v4"`;
- `data-mobile-parallax-profile="photo-continuous-crop"`;
- the accepted V8 weekday/date/time panel, compact action dock, continuation
  gradient, open prose and calendar-added state;
- photo continuous parallax and no-zoom clipped OCR motion from the accepted V8
  implementation;
- medallions, transport, description and discovery stay in the same long mobile
  reading flow; transport is inserted after compact facts, not as a second event
  layout.

The production gate is generated-page evidence, not a consultant opinion:

- Playwright loaded all `282` routes and asserted the exact V8 markers, hidden
  desktop surface and zero horizontal overflow;
- real `390×844` photo, OCR and rail routes showed continuous transform changes;
- the rail route decoded and displayed the Lastochka illustration;
- clicking a real `Смотрите дальше` card navigated to the generated related
  event route;
- `prefers-reduced-motion` behavior remains inherited from the accepted V8
  runtime.

Future mobile changes must start from this shared production contract. A lab
prototype may not replace it until the accepted source and its browser checks
are deliberately promoted together.
