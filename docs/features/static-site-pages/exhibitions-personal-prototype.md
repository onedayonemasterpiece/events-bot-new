# Прототип персональной страницы «Выставки»

> **Статус:** presentation contract promoted to the noindex integrated review
> `/vystavki/`; production root is not promoted.
> **Маршрут:** `/lab/exhibitions-personal/` в обычной Astro-сборке.
> **Источник визуального подхода:** `docs/reference/Выставки UI UX.png`.
> **Последний immutable preview:** `preview-20260720-exhibitions-personal-v12-465c2bc5`.

## Зачем нужен отдельный прототип

Лабораторная страница проверила другой продуктовый цикл: пользователь быстро
разбирает новые выставки, явными действиями формирует интересы, затем получает
персонально упорядоченную подборку и только при необходимости раскрывает
длинный хвост. В integrated noindex candidate эта presentation/interaction
механика используется и на `/vystavki/`, но данные больше не являются
донорским July fixture.

Public-review route сначала получает кандидатов через
`getOngoingExhibitionEvents()`, сохраняя date filtering и occurrence collapse,
а затем строит bucket-проекцию. Неактивные, истёкшие, невыставочные, duplicate
id и точные нормализованные title repeats отбрасываются fail-closed. Донорская
страница остаётся визуальным contract, а не источником production данных.

Прототип не добавлен в sitemap и не является обещанием готовой серверной
персонализации. Его localStorage-ключ `ke_exhibitions_prototype_v1` намеренно не
совпадает с production-профилем.

## Продуктовый цикл

1. Красный счётчик в навигации приводит пользователя к новым выставкам.
2. `Новое для вас` hard-pin-ится выше любого обычного ranking.
3. Лайк добавляет темы выставки в локальные интересы; `Не интересно` оставляет
   устойчивую серую заглушку с `Отменить`, чтобы карточки не прыгали под рукой.
4. После действия, открытия страницы события/галереи или явного снятия
   отметки `Новое` соответствующая новинка перестаёт входить в навигационный
   счётчик. Групповое действие не означает реальный просмотр карточек, не
   меняет интересы и не убирает выставки из ленты.
5. После inbox идут выставки, объяснимо поднятые из-за недавнего открытия,
   общего интереса/обсуждений или близкого закрытия.
6. Старые long-running позиции доступны под `Показать ещё`, но не получают
   верхнюю позицию только потому, что всё ещё идут. Хвост строится из всех
   exhibition-событий committed export, начавшихся не менее 21 дня назад,
   исключает уже показанные id и нормализованные повторы заголовков.

`Для меня` всегда расположен перед `Все`. При cold start он не пуст: показывает
новое и общее главное с честной подсказкой `Начните с 2–3 оценок`. Положительное
действие не является отрицательным фильтром: один лайк больше не скрывает
несвязанные выставки. После трёх текущих лайков совпадающие темы устойчиво
поднимаются внутри `Стоит увидеть` и длинного хвоста, но набор не сокращается.
Только explicit `Не интересно` скрывает конкретное событие в `Для меня`; `Все`
сохраняет полный курируемый набор и серую заглушку с undo.

## Информационная архитектура

Порядок прототипа фиксирован:

1. заголовок и короткая сводка;
2. `Для меня / Все`, объяснение cold start и тематические фильтры;
3. `Новое для вас` — отдельная triage-очередь;
4. `Стоит увидеть` — единый объяснимый mix `recent + popular + ending soon`;
5. `Давно идут` — progressive disclosure длинного хвоста.

Вместо нескольких одинаково громких секций карточка показывает причины
позиции: `Открылась 4 дня назад`, `Популярно в источниках`,
`Заканчивается сегодня`. Raw score пользователю не показывается; like/share
counts не дублируются в reason chips.

## Ranking contract для следующей production-итерации

Прототип использует явный курируемый fixture, а не выдаёт presentation-order за
готовый production ranker. Целевая модель:

```text
if newly_detected && unseen && relevant_for_current_mode:
    bucket = NEW_INBOX                 # hard pin до meaningful review
else:
    score = 0.34 * personal_affinity
          + 0.24 * normalized_source_interest
          + 0.16 * verified_shares_and_discussion_signal
          + 0.14 * recently_opened_boost
          + 0.12 * verified_ending_soon_boost
          - fatigue_penalty
          - old_long_running_penalty
```

- `normalized_source_interest` использует экспортированные `likes_count`,
  `shares_count` и качественные reason codes, а не выдуманные numeric
  discussions/mentions;
- `recently_opened` затухает после 14–21 дня;
- `ending_soon` применяется только к source-grounded `end_date`, не к
  inferred/сомнительной дате;
- fatigue по повторным показам понижает, но не удаляет выставку;
- old-tail penalty не действует в режиме явного просмотра полного хвоста;
- до трёх положительных отметок порядок остаётся курируемым; на третьей и
  последующих отметках стабильная сортировка поднимает совпадения только внутри
  своего bucket и проходит через View Transition, не скрывая карточки.

## Состояния навигационного индикатора

| Условие | Индикатор |
|---|---|
| Есть unseen newly-detected | красный `N новых` независимо от liked tags |
| Лайк/открытие конкретной новинки | только она выходит из unseen-счётчика |
| Есть explicit negative для события | оно не входит в счётчик |
| Новинок нет, но частый посетитель ещё не открывал раздел | мягкий серый `загляните`, не ложное `для вас` |
| Все релевантные новинки meaningfully reviewed | индикатор скрыт |

Счётчик не очищается от prefetch, hover или одного `DOMContentLoaded`. В
прототипе meaningful review — like/reject, открытие title/gallery либо явная
кнопка завершения triage.

## Keyboard и accessibility contract

- отдельная skip-link ведёт к результатам;
- `Для меня / Все` — завершённый `radiogroup`: `←/→`, `Home/End`, click и
  `Space/Enter` работают через native buttons;
- обычный Tab сохраняется; `↑/↓` из страницы, media, title или row-action
  выбирают предыдущую/следующую видимую выставку, переносят roving focus на её
  native title link и подсвечивают всю surface карточки. Поэтому следующий
  `Enter` нативно открывает detail без отдельной JS-эмуляции ссылки;
- `↓` на последней видимой карточке `Стоит увидеть` синхронно раскрывает
  `Давно идут` и переносит фокус на первую подходящую уникальную карточку;
  обратный `↑` возвращает к последней основной карточке, не схлопывая хвост;
- `Enter` открывает страницу выставки, `G` — её фотографии, `L` — like,
  `X` — `Не интересно`, `F/A` — режимы;
- shortcuts не перехватываются внутри `input`, `textarea`, `select`, `button`
  или editable content; после перехода Tab в общий footer стрелки остаются у
  footer-ссылки и не возвращают фокус в список;
- все действия имеют target не меньше `44×44`, `aria-pressed` и polite live
  feedback;
- gallery — native modal `dialog`, поддерживает `←/→`, `Esc` и возвращает фокус
  на media deck. Deck остаётся честной ссылкой на detail без отдельной
  нарисованной кнопки: обычный desktop click/Enter открывает фотографии,
  modified click сохраняет native link, а до `820px` tap/Enter сразу открывает
  событие и не вызывает gallery;
- цветовые точки всегда сопровождаются текстовым статусом;
- `prefers-reduced-motion` отключает декоративные transition.
- до `820px` скрыты все визуальные keyboard affordances: `kbd`, help-trigger и
  help-panel; desktop shortcuts при этом не удаляются.

## Визуальная система

Из референса перенесены dark museum-at-night, компактный timeline, тонкие
границы, фотоколоды, orange/gray/blue lifecycle-сигналы и yellow discussion
marker. Адаптированы:

- страница использует каноническую шапку `EventLayout` с
  `headerCurrent="exhibitions"` и штатным `heroChrome="immersive"` для общего
  mobile discovery drawer; отдельная реконструкция brand/nav и локальные
  правила, скрывающие пункты общей навигации, запрещены;
  общий header получил опциональный base-aware badge, который не меняет другие
  страницы и синхронизируется с тремя состояниями индикатора прототипа;
- `EventLayout` также остаётся единственным владельцем общего production
  `SiteFooter` (`data-site-footer="service-v1"`): prototype больше не скрывает
  уже отрендеренный footer и не создаёт локальную копию его ссылок/стилей;
- мелкий secondary text поднят до читаемого размера/контраста;
- hover имеет равнозначный `:focus-within`, но не меняет геометрию row, media,
  текста или actions: нет подъёма, scale, rotate и раскрытия стопки; меняются
  только opacity halo, edge-light, border и shadow;
- photo deck следует `smart-image-crop`: сначала выбирается asset, затем
  геометрия. Только classified `visual_only event_photo` с `safe_crop`, focal
  evidence и достаточным разрешением входит в именованные `P/S/W/L` cover-
  токены; для landscape discovery приоритетен широкий `L 3:2`. OCR, poster,
  unknown и unsafe asset не получают скрытый center-crop: они остаются
  отдельными edge-to-edge natural document cards, а исходный порядок доступен
  в полноразмерной gallery;
- deck не содержит image `contain`, letterbox, четырёхстороннего padding или
  blur-fill. Desktop grid резервирует одну общую адаптивную media-column
  `420..680px`, поэтому начало всех title/body образует устойчивую вертикаль и
  `+N` не двигает текст. Сам viewport прозрачный и не изображает пустое место
  рамкой/заливкой: только реальные edge-to-edge frames имеют границу. Одиночные
  natural documents и полностью поместившиеся группы центрируются внутри
  колонки без изменения их пропорций; если natural card шире mobile viewport,
  одновременно уменьшаются её width и height, а не обрезается содержимое;
- колода появляется только при реальном переполнении текущей полосы. Полностью
  поместившиеся карточки остаются видимы; следующие карточки укладываются
  вправо с offset `13px` desktop / `9px` mobile и убывающим z-index, а их
  правые края привязаны к общей границе media-column. Каждый следующий слой
  физически ниже и уже предыдущего, темнее, чуть контрастнее и насыщеннее:
  перспектива существует постоянно, а не возникает на hover. После пяти
  реальных previews шестой слой — нейтральная серая плоскость без `<img>`.
  Направленные тени и светлый rim отделяют соседние листы; `+N` честно означает
  все media, которые не показаны полностью;
- на desktop deck не имеет фиксированной высоты: он растягивается до высоты
  content-row, но сохраняет общую ширину media-column. Поэтому длинный title
  делает выше и фотографии, вмещает меньше previews, однако не двигает начало
  текста в соседних строках. `ResizeObserver` и завершение загрузки webfont
  повторяют расчёт колоды без ручного resize;
- discussion-marker вынесен в фиксированную правую aside над действиями и не
  отнимает строку у title. Timeline rail, date/status и светящаяся lifecycle-dot
  визуально находятся слева от bordered surface; короткий цветной connector
  доводит линию до карточки, как в исходном референсе;
- timeline connector теперь физически заканчивается до date copy, а dot
  центрируется по первой строке даты; desktop/tablet rejected stub начинается
  только от bordered exhibition surface и не перекрывает rail. На mobile
  padding карточки отдельно компенсируется в offsets dot/connector: центр dot
  совпадает с центром vertical spine; цветной glow усилен;
- deck и полноэкранная gallery имеют стабильные shimmer-skeleton states. Для
  cached image отдельно проверяется `complete/naturalWidth`; success и error
  всегда снимают skeleton, а ошибка оставляет устойчивую серую поверхность без
  broken-image icon и без изменения геометрии;
- безопасные photos выбирают ближайший именованный token по геометрическим
  midpoint-границам. В частности, исходные `4:3` больше не форсируются в `L
  3:2`: они получают точный `W 4:3` без прежней потери около `11%` кадра;
- edge-light адаптирует локальный приём `farmapers` (`HoverGlow` / glass-card):
  отдельные неинтерактивные halo и border-ring слои, но без cyan-палитры,
  pointer tracking и бесконечного idle sweep на плотном списке;
- единый motion rhythm использует `cubic-bezier(.16,1,.3,1)`, `240ms` для
  press/action и `420ms` для row/deck/dialog; движутся только
  `transform/opacity/filter`, metadata больше не анимирует grid rows;
- filter/mode changes используют progressive View Transitions, gallery имеет
  разные enter/exit длительности, а keyboard scroll переключается на `auto`
  при `prefers-reduced-motion`;
- reduced-motion отключает transition/animation, но не обнуляет blanket-правилом
  позиционирующие transform у skip-link, live-region, timeline dots и deck
  layers; интерактивный подъём row/icon подавляется отдельными селекторами;
- на ширине до `820px` row превращается в вертикальную карточку без
  горизонтального overflow, фильтр остаётся sticky, а стандартный мобильный
  discovery drawer заменяет самодельную нижнюю навигацию;
- на `768px` остаётся компактный timeline row, на `1440px` используется полная
  rail/deck/title/action-композиция.

## Данные, аналитика и ограничения

Numeric `обсуждения/упоминания` удалены: таких public полей в `PreviewEvent` и
export нет, прежние числа были неподтверждёнными prototype literals. В UI
остались только честные build-time данные: `likes_count` один раз внутри
heart-button и optional `shares_count > 0` с share icon и подписью/tooltip
`Пересылки и репосты исходных публикаций`. Это сумма доступных Telegram
forwards, VK reposts и generic source shares после exporter dedupe; она не
выдаётся за число уникальных людей. `Обсуждают` может появиться без числа только
при exporter reason code `popularity_reason_codes.includes('discussed')`.
Локальная персональная отметка меняет `aria-pressed`/заливку сердца, но
намеренно не прибавляет `+1` к агрегату: browser-only предпочтение ещё не
является сохранённым общим лайком. Visible likes не делятся в UI на
source/service.

Основные продуктовые метрики следующего эксперимента:

- доля визитов, где triage новых завершён;
- like / not-interested / undo rate по новым и обычным позициям;
- доля переходов в detail/gallery после exposure;
- сокращение времени до первого осмысленного действия;
- engagement `Для меня` против `Все` после 3+ оценок;
- guardrails: empty-personal rate, undo-after-negative, diversity первых 10,
  concentration показов, keyboard completion и layout-shift после действий.

Прототип ничего не отправляет в Supabase и не заявляет production analytics.
Local state нужен только для interaction review.

Перед smart-crop итерацией выполнен свежий read-only production media audit на
2026-07-19: `280` public active events, `590` canonical approved assets,
`190` событий с одним image, `88` с несколькими, `113` all-OCR и только `42`
события со strict sales-photo evidence. Поэтому blanket `cover` отклонён:
широкий browse crop применяется только к доказанным фотографиям, а документы
остаются полноформатными карточками без полей и без потери текста.

## Incident regression contract

`INC-2026-07-02-exhibition-duplicates-static-site` остаётся обязательным guard.
Prototype fixture fail-fast проверяет:

- уникальность event id;
- наличие каждого события в committed export;
- точный `event_type=выставка` вместо широкого keyword rescue.

Это не заменяет production replay/DB/public-surface checks инцидента. Так как
production `/vystavki/`, import, Smart Update и read-side selection не менялись,
в этой задаче выполняется только prototype-scope regression: curated identities,
валидная Astro-сборка и отсутствие дублированных DOM rows.

## Внешняя критика

Перед реализацией получен read-only review через `agy`, модель
`Gemini 3.1 Pro (High)`. Приняты core loop, hard-pin new inbox, cold-start
fallback, fatigue/old-tail demotion, no-layout-shift undo и измеримые keyboard
assertions. Не приняты буквально: generic 3–4-column desktop grid (сохранён
timeline из исходного PNG) и очистка badge на одном `DOMContentLoaded`
(заменена meaningful review).

После browser screenshots выполнен второй acceptance-review той же Pro-линии.
Его конкретные блокеры применены: мобильный bottom nav снова показывает все
пять разделов, первое фото лежит поверх колоды, roving `tabindex=0` перемещается
вместе с фокусом, а свернутая hover-мета скрыта от accessibility tree. Совет
поднимать общие хиты выше new inbox на cold start не принят: он противоречит
явному продукт-contract этой страницы; вместо этого cold-start качество новых
позиций должно проверяться метриками и отдельным экспериментом, не менять
базовый порядок прототипа.

Финальный повторный gate Gemini 3.1 Pro (High) после этих исправлений: `Accept`,
P0 не осталось. Отдельно сохранён продуктовый риск аналитики: длину серых undo
stubs нельзя напрямую сравнивать со следующими production-сеансами, где
отклонённые карточки уже не будут выданы.

В итерации shared-header / deck-depth повторный pre-design review той же
`Gemini 3.1 Pro (High)` линии подтвердил проблемы исходной `object-fit:cover`
геометрии, слабых границ листов и `grid-template-rows` motion. Его предложение
перевести весь prototype на светлую scroll-snap ленту не принято буквально:
оно ломало заданные dark-reference и метафору колоды. Приняты shared header,
aspect-aware contain, reflow-free metadata, cinematic easing и reduced-motion;
отдельный public screenshot gate выполняется после интеграции.

Финальный acceptance-review через `agy` (`Gemini 3.1 Pro (High)`) оценил
halo/edges, aspect-aware multi-photo deck, границы колоды и motion как `Pass`,
но нашёл P0 в page-local responsive overrides общей шапки: на промежуточной
ширине они оставляли только текущий раздел. Prototype теперь использует штатный
immersive drawer на mobile, badge-extension отражает `headerCurrent`, а
prototype-local диапазон `760..1020px` получил fluid gap/padding вместо скрытия
навигации; базовая геометрия shared layout для production pages не менялась.

Перед smart-crop / real-overflow итерацией отдельный критический review той же
`Gemini 3.1 Pro (High)` линии подтвердил P1-дефекты: blanket `contain`, inset
`7px 64px 7px 7px`, фиктивный stack при двух фотографиях и одновременные
row/frame/image/metadata transforms на hover. Его предложение включить blanket
`cover` для каждого poster не принято: новый `smart-image-crop` contract требует
fail-closed OCR. Принята синтезированная схема: широкий `L/W` cover для
evidence-safe photos, natural edge-to-edge document cards, реальный правый
overflow stack и полностью световой hover.
Рекомендация удалять mobile drawer вместе
с overrides не применялась буквально: browser gate подтвердил, что drawer —
канонический общий компонент, а не самодельная навигация.

В итерации fixed-column / stronger-deck отдельный pre-design review через
`agy`, `Gemini 3.1 Pro (High)`, правильно локализовал две причины обратной
связи: content-sized `auto` media-column двигала начало текста, а порог `1.20`
почти не оставлял шанса токену `W` и резал обычные `4:3` photos до `L`. Приняты
фиксированная адаптивная колонка, midpoint token selection, направленные тени и
усиленный `+N`. Предложенные консультантом hover fan-out и `scale(1.04)` явно
отклонены: они противоречат повторному требованию владельца о неподвижной
геометрии; hover остаётся только light/halo state.

Перед итерацией adaptive-height / perspective / loading выполнен новый
критический разбор через `agy`, `Gemini 3.1 Pro (High)`, а также три независимых
read-only lane-а по layout/title, reference/depth и loading/QA. Приняты:
растяжение deck вслед за высоким desktop row при неизменной ширине колонки,
`ResizeObserver`, закреплённая у даты timeline-dot, уменьшающиеся depth planes,
нейтральный шестой слой и cache-safe skeleton lifecycle. Совет Gemini оставить
`Обсуждают` inline после title отклонён: он противоречит наблюдаемой проблеме
длинного заголовка; marker вынесен в уже зарезервированную правую aside.
Финальный screenshot/code/QA gate той же Pro-линии дал `ACCEPT` по R1–R5 без
P0/P1. Единственное P2-замечание о сохранении высоты rejected row не принято
как дефект: это намеренный documented undo-stub contract, предотвращающий
прыжок списка под рукой пользователя.

Независимый checklist-review дополнительно поймал два P1, пропущенных первым
source-string gate: blanket `transform:none!important` ломал позиционирование в
reduced-motion, а single-image deck сохранял пустой reserve правого edge-stack.
Оба устранены отдельными targeted reduced-motion states и
`.ex-deck__images--1`; browser interaction gate после исправления обязателен.

Перед итерацией timeline / keyboard / engagement выполнены критический
pre-design review через `agy`, `Gemini 3.1 Pro (High)`, и три независимых
read-only lane-а по keyboard/reject, timeline/mobile и social/product. Приняты
физически короткий connector, глобальный roving focus с whole-row halo,
surface-confined reject stub, усиленный mobile glow, прямой media-link и единый
heart + aggregate count. Совет удалить presentation-only обсуждения и
упоминания не применён буквально: по явному продуктовому вопросу владельца они
сохранены как пассивные сигналы открытых источников с иконками comment/@ и
точными aria-label, а не выдаются за production telemetry. Финальный
screenshot/code/Playwright gate той же Pro-линии дал `ACCEPT` по R1–R7, без
P0/P1/P2. Это решение позже superseded data-truth итерацией: числа оказались
не связанными с exporter fields и полностью удалены.

Перед data-truth / mobile-axis коррекцией выполнены три read-only аудита и
критический review через `agy`, `Gemini 3.1 Pro (High)`. Консультант дал
`ACCEPT`: убрать нарисованную gallery-icon, удалить invented numeric
discussions/mentions, показывать только exported `shares_count` и qualitative
`discussed`, а mobile dot/connector сдвинуть на точную величину row padding.
Playwright после реализации подтвердил на `375`, `390` и `768px`: dot/spine
delta `0`, connector rounding не больше `0.01px`, zero overflow; desktop media
открывает gallery без отдельной иконки, mobile media — detail. Финальный
повторный gate той же Pro-линии дал `ACCEPT` по R1–R3 без P0/P1/P2.

### V8: физические клавиши и разбор колоды

Команды оценки теперь определяются прежде всего по физическому
`KeyboardEvent.code`: `KeyL`, `KeyX`, `KeyG`, `KeyF`, `KeyA`. Поэтому `L/X`
работают и при русской раскладке (`д/ч` в `event.key`), а латинский
`event.key` оставлен только как fallback. Команды не выполняются при
модификаторах, автоповторе, IME composition и внутри editable controls;
`↑/↓` остаются layout-independent навигацией списка.

Правый overflow больше не рассчитывается через консервативный резерв всех
sliver-ов. Полные фото набираются до компактного badge-reserve, после чего
первая карта стопки обязана соприкасаться с последней полной карточкой с
нахлёстом не менее `8px` desktop / `6px` mobile. Полные фотографии лежат выше
стопки по z-axis, поэтому дополнительная полная карточка остаётся читаемой, а
из-под неё справа выходят последовательно уменьшающиеся края колоды. `+N`
привязан к фактическому последнему краю и всегда равен числу фотографий после
текущей полностью раскрытой группы. Ширина media-column и hover geometry не
меняются.

На desktop `←/→` с roving-title выбранной выставки или с фокусом на её
media-link экспериментально листают фотографии группами. Текущая полная группа
за `190ms` ускоряется за левый/правый край, затем следующие карточки за
`470ms` с stagger `36ms` раскладываются из стопки через FLIP/WAAPI и
`cubic-bezier(.16,1,.3,1)`. Анимируются только `transform` и `opacity`; deck,
row и текстовая колонка остаются неподвижны. Состояние хранится отдельно для
каждой колоды (`cursor/history/phase/queued`), конец не зацикливается, быстрые
нажатия сводятся к одной ожидающей команде, resize сначала отменяет активные
WAAPI animations. При `prefers-reduced-motion: reduce` страница переключается
мгновенно без motion objects. На `≤820px` стрелки фото не перехватываются, а
media-link по-прежнему ведёт прямо на страницу события.

Последняя страница колоды теперь не обязана быть неполным разрозненным suffix.
Если оставшиеся новые фотографии занимают слишком мало места, pager ищет
самое раннее помещающееся terminal-окно строго правее предыдущего cursor и
оставляет в нём нужные фотографии прошлого батча. Поэтому последний блок
начинается от левого края, обязательно показывает хотя бы одну новую карточку,
не зацикливается и возвращается `←` к точному предыдущему cursor. Retained
frames не проигрывают exit-анимацию: они FLIP-переезжают из прежней позиции, в
том числе из stack в full, поэтому переход не создаёт кратковременную пустоту.

Чтобы длинный список не создавал DOM по числу всех фотографий, каждая строка
имеет не больше семи переиспользуемых frame-shells. Полный сортированный
manifest хранит реальные размеры, srcset, crop evidence и source index;
логические фотографии rebound-ятся в shells текущей группы и ближайшей
стопки. Пятый depth-level остаётся нейтрально-серым, но получает настоящее
изображение и skeleton, когда доходит до раскрытой группы.

Предварительный Gemini 3.1 Pro (High) review дал `REVISE`: потребовал
ограничить DOM вместо рендера всех media, добавить stagger, локализовать
перехват стрелок и отменять WAAPI перед resize. Все четыре замечания включены
в реализацию; глобального перехвата `←/→`, 3D rotate, spring и анимации layout
properties нет. Финальный review той же Pro-линии дал `ACCEPT` по R1–R3 без
P0/P1. Единственный P2 о fractional zoom дополнительно закрыт Playwright
проверкой `100/125/150%` на `1020/1440px`: межгрупповой зазор не появился,
frames остались внутри deck, `+N` не изменился.

### V9: мягкая персонализация, terminal-window, полный хвост и footer

Новая предреализационная критика `Gemini 3.1 Pro (High)` оценила исчезновение
`Окон времени` после лайка `Диалогов` как продуктовую ошибку: положительный
сигнал не должен превращаться в hard exclusion при неполной таксономии.
Рекомендация принята полностью: liked tags после трёх отметок влияют только на
стабильный порядок внутри bucket, а unseen badge не фильтруется по ним;
explicit negative по-прежнему сильнее ranking. `likedTags` пересобираются из
текущих likes после unlike и reject, поэтому удалённая отметка не оставляет
устаревший интерес.

Тот же review потребовал overlapping terminal-window, синхронное раскрытие
long-running tail по `↓`, неизменное состояние при `↑` и защиту общего footer
от глобального list-shortcut. Shared footer уже находился в production
`EventLayout`; исправление удаляет только prototype-local `display:none`.
Отдельный техдолг остаётся вне этой итерации: production check пока не
дублирует footer service-v1 assertions secret-candidate gate, а canonical
компонент всё ещё носит legacy CSS-имена `site-footer-prototype__*`.

## Проверка

```bash
cd site
npm run build
node scripts/check-exhibitions-personal-prototype.mjs
python3 -m http.server 4321 --directory dist
# затем открыть /lab/exhibitions-personal/
```

Browser QA должен покрывать `375×812`, `768×1024`, `900×900` и `1440×1000`: zero
horizontal overflow, 22 уникальных exhibition rows в текущем committed export,
3 new rows, no console
errors, одинаковую desktop media-column/body vertical alignment, высокий deck
у event `4913`, честные right-edge stack/count, строго убывающую высоту depth
planes, серый tail без image, неподвижный hover, keyboard movement и gallery.
Loading gate задерживает первый image: skeleton виден до ответа, исчезает после
load, а deck geometry имеет delta `0`. Последний source/build gate: Astro build
`381` pages и prototype contract `56/56`; production route/data/ranking не
менялись. V6 Playwright дополнительно проверяет: short timeline connector,
единственный roving target, Up/Down с body/media/actions, нативный Enter,
центровку rejected stub по surface, отсутствие keyboard/gallery affordances на
mobile и прямую media navigation. V7 gate дополнительно проверяет отсутствие
painted gallery icon, real share values, отсутствие numeric mentions/comments,
qualitative `Обсуждают`, desktop gallery/media-link fallback и совпадение осей
dot/spine/connector на трёх mobile widths. V8 gate дополнительно проверяет
русские `key=д/code=KeyL` и `key=ч/code=KeyX`, игнорирование
modifier/repeat/composition, отсутствие межгруппового зазора на `375, 768, 820,
821, 900, 1020, 1021, 1045, 1100, 1440px`, точную формулу `+N`, forward/back
history до дна колоды, bounded rapid input, неизменность deck/row rect,
мгновенный reduced-motion switch и нулевой mobile cursor после `←/→`.
V9 gate дополнительно проверяет: один лайк не скрывает `Окна времени`, после
трёх likes меняется только порядок, terminal batch удерживает карточку прошлого
батча и начинается с `left=0`, вся история точно обратима, `↓` раскрывает полный
уникальный long-running хвост, shared footer виден без overlap/overflow, а
`ArrowDown` внутри footer не уводит фокус обратно в список. Актуальный source
contract: `56/56`.

### V10: непрерывная связка `лайк → фото`, явный хвост и общие footer-hotkeys

Пользовательская последовательность `↓ → L → →` выявила не ошибку раскладки,
а потерю DOM-focus: `syncPersonalOrder()` переустанавливал каждый row через
`append()` даже при неизменном порядке. Фокус уходил в `body`, поэтому
глобальный `↑` мог восстановить выбранную строку, а локальный `→` до этого не
доходил до deck. Теперь порядок сравнивается до мутации; при настоящем rerank
сохраняется тот же title-node и roving owner. Like-sync не запускает
document-wide View Transition и не планирует геометрический deck-relayout,
поэтому немедленный `→` начинает собственную кинематическую раздачу без гонки
двух animation timelines. Это действует и для `key=д/code=KeyL`.

История footer подтвердила: отдельной версии `SiteFooter.astro` с хоткеями не
существовало. Рабочие `P/S` раньше добавлял event-page keyboard navigator
(`d0027a53`, затем production extraction `11cbef17`). V10 переносит эту узкую
ответственность в общий `ServiceShareAction`: только при desktop `>=1024px` и
только когда фокус уже внутри footer share-root физический `KeyP` нажимает
реальную кнопку копирования карточки, а `KeyS` — текста/ссылки. На mobile нет
ни keycaps, ни `aria-keyshortcuts`. Shared handler проверяет
`event.defaultPrevented`, а сам предотвращает обработку, поэтому при будущем
слиянии с новым event navigator не возникает двойного click. Остаточный
техдолг после rebase на `origin/main`: удалить старую footer-инъекцию из
event-only navigator, оставив единственного владельца в `ServiceShareAction`.

Длинный хвост больше не раскрывается неявно одним `ArrowDown`. Последняя
featured-строка передаёт фокус native disclosure-кнопке; `Enter/Space`
раскрывает первые четыре позиции, фокус остаётся на кнопке, следующий `↓`
входит в первую карточку, а `↑` возвращается симметрично. Дальнейшие позиции
добавляются явными батчами по четыре; scroll сам ничего не загружает. Выбран
накопительный вариант, а не замена окна: он сохраняет `Ctrl+F`, скролл-контекст
и уже просмотренные карточки. При этом hidden rows не получают image `src`, а
`IntersectionObserver` снимает media вне viewport; в каждом видимом tail-deck
не больше трёх реальных image planes, остальные depth-слои остаются
нейтральными. Так полный набор из 13 уникальных tail-позиций достижим, но
первое раскрытие остаётся малым и визуально заполненные карточки не превращаются
в пустые рамки ради слишком жёсткого глобального лимита.

Предпроектный `Gemini 3.1 Pro (High)` review ранжировал потерю фокуса как P0,
bulk-раскрытие хвоста как P1 и раздельное владение footer-hotkeys как P2.
Приняты отказ от no-op DOM reorder, общий root-scoped footer controller,
explicit disclosure и cumulative batch size `4`. Совет ставить animation
semaphore/queue поверх потерянного фокуса не использован: исправлена причина,
а существующий deck queue сохранён. Предложенный жёсткий лимит 12 картинок
скорректирован после screenshot QA: он оставлял видимые строки пустыми;
вместо этого ограничены реальные planes каждого viewport-active deck.

V10 browser gate проверяет stable и настоящий third-like rerank, немедленный
`→` после Latin/Russian physical L, ноль child-list мутаций при стабильном
порядке, disclosure `↓ → Space → ↓ → ↑`, все 13 tail id ровно по одному,
viewport media budget, footer `P/S` positive/negative scope и отсутствие всего
keyboard chrome на `375px`. Результат: source contract `59/59`, desktop/mobile
Playwright — без console/page errors и horizontal overflow.

Финальный gate того же `Gemini 3.1 Pro (High)` по публичному immutable preview:
`ACCEPT`, P0/P1 блокеров нет. Консультант отдельно подтвердил cumulative batch
`4`, сохранение focus на disclosure после `Enter/Space` и per-visible-deck
media cap; P2 — измерять частоту последовательных раскрытий и только по данным
решать, нужен ли адаптивный размер батча.

### V11: честные карточки хвоста и снятие notification debt

Пустые рамки в четвёртой строке раскрытого хвоста оказались не ошибкой
загрузки. V10 искусственно разрешал только три real-media plane на каждый
видимый tail-deck; поэтому четвёртая полная карточка и ближние карты колоды
получали серую заглушку, хотя для них был настоящий asset. Лимит удалён.
Теперь каждая полностью раскрытая фотография и первые четыре depth-level
всегда привязаны к реальному изображению, а нейтральным остаётся только пятый
дальний слой. Чтобы этот слой не исчезал на широком desktop, где помещаются до
четырёх полных фотографий, bounded pool расширен с семи до девяти shell:
`4 full + 4 media-depth + 1 neutral-depth`. Производительность по-прежнему
ограничивают cumulative batch по четыре строки, девять переиспользуемых shell
на строку и viewport-based
`IntersectionObserver`, снимающий `src` у ушедших далеко за экран колод.

Product review через `agy`, `Gemini 3.1 Pro (High)`, дал техническому исправлению
`ACCEPT` / P1 и предложил не удалять массовое действие, а исправить его смысл.
`Отметить просмотренными` создавало ложное обещание экспозиции: пользователь не
просмотрел все карточки, а лишь хочет погасить накопившийся красный индикатор.
Поэтому действие сохранено как escape hatch от notification debt, понижено до
вторичной text/ghost-команды и переименовано в `Снять «новое» у всех`.

Его контракт узкий и обратимый:

- снимаются красный header badge и визуальные плашки `Новое`;
- карточки остаются на тех же местах, likes/rejects и профиль интересов не
  меняются;
- feedback прямо говорит: `Отметки «новое» сняты. Карточки остались в ленте.`;
- `Отменить` восстанавливает точный предыдущий seen-state и счётчик;
- команда скрыта, пока новых непросмотренных карточек нет.

Автоматически считать карточку просмотренной только по scroll не принято:
scroll слишком легко погасит уведомление случайно. Удалять bulk-action совсем
тоже не принято — без него редкий пользователь с накопившимся inbox вынужден
делать однотипные действия по каждой карточке. Для production нужно измерять
долю использований, долю последующих undo и возвраты к карточкам после снятия
`Новое`; если действие почти не используется, его можно убрать без потери
основного triage-цикла.

V11 source contract: `60/60`. Local Playwright на desktop подтвердил у четвёртой
строки `С чего начинается Родина` семь честных media-frames, один явный серый
depth-5 plane и отсутствие необоснованных `depth-tail`, сохранение трёх
карточек и порядка после bulk reset,
неизменность likes/rejects/interests, точный undo и отсутствие media binding у
скрытого хвоста. Desktop и `375px` прошли без console/page errors и horizontal
overflow.

Финальный повторный gate `Gemini 3.1 Pro (High)` после расширения bounded pool
и демонстрации фактических focus assertions дал `ACCEPT`: P0/P1/P2 замечаний
не осталось.

### V12: институциональная печать, семантика дат и тихий photo pager

Отдельный product review через `agy`, `Gemini 3.1 Pro (High)`, подтвердил, что
выставочный список нуждается в связи с общей системой медальонов, но не в
detail-page ряду из крупных `90–112px` токенов. Рассмотрены четыре варианта:
отдельный ряд ломает вертикальный ритм, inline-аватар у площадки слишком
утилитарен, а логотип как фотография фатально искажает `+N`, pager и gallery.
Принят один compact pattern: `44px` desktop / `36px` mobile institutional seal
в левом верхнем углу photo deck.

Seal использует существующий fail-closed `resolveEventMedallions`: показывается
не больше одного curated `venue_brand` или primary `organizer`. Он является
стабильным sibling photo-frames, а не членом `deckMedia`, поэтому не уезжает с
раздаваемыми фотографиями и не входит в photo count/gallery. Элемент
неинтерактивен, `aria-hidden`, лежит выше фото и ниже `+N`, использует manifest
ring/background и контрастную тень. Ошибка изображения скрывает seal; при
отсутствии curated identity карточка остаётся без круга. Neutral initials не
используются: неполное покрытие лучше честной чистой карточки, чем системной
заглушки, похожей на broken image.

Timeline теперь сообщает смысл границы, а не голую календарную дату:

- недавно открывшиеся — `с 15 июля`;
- upcoming — `с 8 августа`;
- ending/popular/long с известным окончанием — `до 19 июля`;
- граница в другом календарном году включает год: `до 28 марта 2027`, чтобы
  март не воспринимался как уже прошедший;
- derived long-running без end date — `с <дата начала>`; `Постоянная` допустима
  только при отдельном source-grounded признаке постоянной экспозиции.

Визуальный fixed toast после каждого `←/→` удалён: `+N` и движение колоды уже
дают достаточную зрительную обратную связь, а вспышка внизу конкурировала с
действием. Photo page озвучивается отдельным visually-hidden `aria-live` с
debounce `250ms` и мгновенным reduced-motion режимом. Toast сохранён для
глобальных действий и undo; его появление теперь использует slide/fade с
кинематическим easing, а смена сообщения — crossfade без предварительного
очищения текста.

Acceptance прошёл source-contract `65/65` на 22 уникальных выставках и
одинаковую Playwright-проверку local/public desktop/mobile: одна
curated-печать находится вне photo semantics, имеет `44/36px`, не перекрывает
`+N`, корректно скрывается при ошибке загрузки и не создаёт horizontal
overflow. Проверены также тихое листание с delayed screen-reader status,
transition у action toast и межгодовая дата. Публичный immutable preview
ответил `HTTP 200`. Финальный gate `Gemini 3.1 Pro (High)` дал `ACCEPT` без
P0/P1/P2 замечаний.

### R9 owner correction: mobile seal scale, 2026-07-23

Последующая приёмка цельной мобильной сборки признала `36px` визуально слишком
мелкими. Публичная и лабораторная выставочные поверхности поэтому используют
`44×44px` institutional seal и на desktop, и на mobile. Семантика V12 не
меняется: это один неинтерактивный fail-closed sibling photo deck, выше фото и
ниже `+N`, вне pager/gallery/photo count. Проверка на `320/390/430px` обязана
подтверждать отсутствие перекрытия счётчика, horizontal overflow и broken
image.
