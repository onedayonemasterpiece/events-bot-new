# Прототип персональной страницы «Выставки»

> **Статус:** отдельный интерактивный product/UI prototype; production `/vystavki/` не изменён.
> **Маршрут:** `/lab/exhibitions-personal/` в обычной Astro-сборке.
> **Источник визуального подхода:** `docs/reference/Выставки UI UX.png`.

## Зачем нужен отдельный прототип

Текущая `/vystavki/` остаётся production-like статическим списком. Лабораторная
страница проверяет другой продуктовый цикл: пользователь быстро разбирает новые
выставки, явными действиями формирует интересы, затем получает более короткую
персональную подборку и только при необходимости раскрывает длинный хвост.

Прототип не добавлен в sitemap и не является обещанием готовой серверной
персонализации. Его localStorage-ключ `ke_exhibitions_prototype_v1` намеренно не
совпадает с production-профилем.

## Продуктовый цикл

1. Красный счётчик в навигации приводит пользователя к новым выставкам.
2. `Новое для вас` hard-pin-ится выше любого обычного ranking.
3. Лайк добавляет темы выставки в локальные интересы; `Не интересно` оставляет
   устойчивую серую заглушку с `Отменить`, чтобы карточки не прыгали под рукой.
4. После действия, открытия страницы события/галереи или явного
   `Отметить просмотренными` соответствующая новинка перестаёт входить в
   навигационный счётчик.
5. После inbox идут выставки, объяснимо поднятые из-за недавнего открытия,
   общего интереса/обсуждений или близкого закрытия.
6. Старые long-running позиции доступны под `Показать ещё`, но не получают
   верхнюю позицию только потому, что всё ещё идут.

`Для меня` всегда расположен перед `Все`. При cold start он не пуст: показывает
новое и общее главное с честной подсказкой `Начните с 2–3 оценок`. После первого
лайка остаются совместимые темы и уже понравившиеся выставки. `Все` возвращает
полный курируемый набор; отвергнутая карточка остаётся серой заглушкой с undo.

## Информационная архитектура

Порядок прототипа фиксирован:

1. заголовок и короткая сводка;
2. `Для меня / Все`, объяснение cold start и тематические фильтры;
3. `Новое для вас` — отдельная triage-очередь;
4. `Стоит увидеть` — единый объяснимый mix `recent + popular + ending soon`;
5. `Давно идут` — progressive disclosure длинного хвоста.

Вместо нескольких одинаково громких секций карточка показывает причины
позиции: `Открылась 4 дня назад`, `373 отметки`, `Часто обсуждают`,
`Заканчивается сегодня`. Raw score пользователю не показывается.

## Ranking contract для следующей production-итерации

Прототип использует явный курируемый fixture, а не выдаёт presentation-order за
готовый production ranker. Целевая модель:

```text
if newly_detected && unseen && relevant_for_current_mode:
    bucket = NEW_INBOX                 # hard pin до meaningful review
else:
    score = 0.34 * personal_affinity
          + 0.24 * normalized_source_interest
          + 0.16 * discussion_and_mentions
          + 0.14 * recently_opened_boost
          + 0.12 * verified_ending_soon_boost
          - fatigue_penalty
          - old_long_running_penalty
```

- `normalized_source_interest` использует агрегированные честные отметки,
  обсуждения и упоминания, а не один канал;
- `recently_opened` затухает после 14–21 дня;
- `ending_soon` применяется только к source-grounded `end_date`, не к
  inferred/сомнительной дате;
- fatigue по повторным показам понижает, но не удаляет выставку;
- old-tail penalty не действует в режиме явного просмотра полного хвоста;
- порядок замораживается на текущий сеанс, чтобы лайк не пересортировал список
  прямо под фокусом пользователя.

## Состояния навигационного индикатора

| Условие | Индикатор |
|---|---|
| Профиля ещё нет, есть unseen newly-detected | красный `N новых` |
| Есть положительные интересы | красный счётчик только unseen новых с совпадающими темами |
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
- обычный Tab сохраняется; `↑/↓` перемещают только между видимыми title links,
  когда фокус уже находится внутри списка;
- `Enter` открывает страницу выставки, `G` — её фотографии, `L` — like,
  `X` — `Не интересно`, `F/A` — режимы;
- shortcuts не перехватываются внутри `input`, `textarea`, `select`, `button`
  или editable content;
- все действия имеют target не меньше `44×44`, `aria-pressed` и polite live
  feedback;
- gallery — native modal `dialog`, поддерживает `←/→`, `Esc`, возвращает фокус
  на исходную фотоколоду;
- цветовые точки всегда сопровождаются текстовым статусом;
- `prefers-reduced-motion` отключает декоративные transition.

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
  поместившиеся карточки остаются видимы; до трёх следующих реальных images
  укладываются вправо с последовательным offset `13px` desktop / `9px` mobile
  и убывающим z-index, а их правые края привязаны к общей границе media-column.
  Верхние fully-visible cards лежат над началом stack: это убирает зазор и
  оставляет справа настоящие последовательные корешки. Направленные вправо
  shadow и светлый rim отделяют листы даже на тёмных фотографиях.
  Счётчик `+N` означает все media, которые не показаны полностью; декоративных
  пустых `i`-карт больше нет;
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

Сводные `обсуждения/упоминания` в prototype fixture — presentation-only
сценарии для проверки иерархии. Production UI должен получать их из
версионированной агрегированной проекции. Visible likes не делятся в UI на
source/service и не должны выдаваться за текущий пользовательский like.

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

Независимый checklist-review дополнительно поймал два P1, пропущенных первым
source-string gate: blanket `transform:none!important` ломал позиционирование в
reduced-motion, а single-image deck сохранял пустой reserve правого edge-stack.
Оба устранены отдельными targeted reduced-motion states и
`.ex-deck__images--1`; browser interaction gate после исправления обязателен.

## Проверка

```bash
cd site
npm run build
node scripts/check-exhibitions-personal-prototype.mjs
python3 -m http.server 4321 --directory dist
# затем открыть /lab/exhibitions-personal/
```

Browser QA должен покрывать `375×812`, `768×1024` и `1440×1000`: zero
horizontal overflow, 12 уникальных exhibition rows, 3 new rows, no console
errors, одинаковую desktop media/body vertical alignment, честные right-edge
stack/count, keyboard movement, like/reject/undo, input shortcut guard, gallery and
badge clearing. Последний gate: Astro build `381` pages, prototype contract
`31/31`, ArrowDown/gallery/Escape/reject/undo/like/mobile drawer — pass без
console errors; reduced-motion сохраняет позиционирующие transforms при `0s`
transition и `auto` scroll. Отдельный regression-smoke `/vystavki/` подтвердил
`51/51` уникальную listing row, прежний H1, все пять desktop destinations,
mobile drawer и отсутствие horizontal overflow; route/data/ranking не менялись.
