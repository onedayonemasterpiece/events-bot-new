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

- мелкий secondary text поднят до читаемого размера/контраста;
- hover имеет равнозначный `:focus-within`;
- photo deck — лёгкое наложение кадров, не тяжёлая 3D-анимация;
- на ширине до `740px` row превращается в вертикальную карточку без
  горизонтального overflow, фильтр остаётся sticky, активный раздел уходит в
  нижнюю навигацию;
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
errors, keyboard movement, like/reject/undo, input shortcut guard, gallery and
badge clearing.
