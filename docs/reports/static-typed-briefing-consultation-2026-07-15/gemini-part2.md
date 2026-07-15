# Консультационное заключение: KenigEvents Live City Briefing — Часть II (Глубокая спецификация)

Данный документ является развитием и детализацией первой фазы исследования. Исправлены исходные допущения: архитектура базируется на **Astro SSG (Static Site Generation)** со статичным глобальным манифестом в Yandex Object Storage. Исключены спекулятивные метрики дефицита билетов, переработана модель доступности (a11y) и сенсорных взаимодействий (`pointerdown`). 

Документ содержит исчерпывающие контракты, без которых невозможен переход к прототипированию.

---

## A. Контракт сценариев и редакционная библиотека (Scenario Contract & Copy Library)

*Алгоритм сборки, исключений и tie-breakers:*
1.  **Multi-tab & Session:** `last_visit` фиксируется только при закрытии/скрытии вкладки (`visibilitychange: hidden`) или при неактивности > 30 минут. Это предотвращает проблему «пустых обновлений» (Ghost New) при одновременном открытии двух вкладок.
2.  **Viewed vs Dismissed:** `viewed` (был во viewport > 2 сек) снижает вес события на 80%. `dismissed` (явно скрыто крестиком, если применимо) — жесткий фильтр (вес = 0).
3.  **Tie-breaker:** Если два сценария имеют равный вес, побеждает тот, который не показывался пользователю дольше всего (Cooldown tracking).
4.  **Filter Bubble Control:** В каждую выборку из 3-4 сцен принудительно вставляется один слот диверсификации (Serendipity или Verified Popular), не зависящий от профиля пользователя.

### Таблица сценариев (18+ семейств, >45 вариантов сообщений)
*Все ссылки используют машиночитаемый токен `{{link:type:id|Текст}}` для безопасной гидратации. `[Текст]` внутри шаблона означает подставляемую ссылку.*

| ID | Visitor State | Условия (Facts & Provenance) | Приоритет / Исключения | Safe Wording Rule | Forbidden Claim / Антипаттерн | Варианты текста (2-4 на сценарий) | Target / Deep Link | Placement / Ограничение частоты |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `first_anon` | First Anonymous | Нет данных в `localStorage`. База SSG. | **P4.** Исключает все персональные. | Фактическое описание каталога. | «Я подобрал для вас...» | 1. Сегодня в городе {{event_count}} событий. Начать можно с {{link:route:/vyhodnye/|главного на выходные}}.<br>2. Собрали актуальную афишу. От {{link:cat:family|семейных прогулок}} до {{link:cat:music|вечерних концертов}}.<br>3. Вы на главной странице городских событий. Посмотрите {{link:route:/popular/|что популярно прямо сейчас}}. | Разводящие / Популярное | Scene 1. Cooldown: до появления last_visit. |
| `return_anon` | Returning Anonymous | `last_visit` > 4 часов, есть `new_events`. | **P1.** Базовый сценарий возврата. | Констатация дельты времени. | «Вы ждали, и мы добавили...» | 1. Пока вас не было, появилось {{count}} новых событий.<br>2. С вашего прошлого визита мы добавили {{count}} событий.<br>3. В афише новые события. Одно из них — {{link:event:top_new_id|top_new_title}}.<br>4. Обновили каталог: {{count}} новых вариантов на выходные. | /new | Scene 1. 1 раз за сессию. |
| `auth_home` | Authenticated | Наличие JWT / Session. | **P2.** Исключает first_anon. | Использование сохраненного профиля. | «Мы знаем, что вы любите...» | 1. Вы вошли в профиль. В ваших {{link:route:/saved/|сохранениях}} без изменений.<br>2. В ваших {{link:route:/saved/|избранных событиях}} появилось {{count}} обновлений. | /saved | Scene 1. 1 раз в сутки. |
| `high_freq_zero` | High-frequency (Zero changes) | `last_visit` < 2 часов, новых событий 0. | **P1.** Перебивает `return_anon`. | Честное признание отсутствия новинок. | «У нас всегда есть что-то новое!» | 1. С прошлого визита новых событий пока нет.<br>2. Каталог не изменился за последние два часа. Зато сейчас популярно {{link:event:top_id|top_title}}.<br>3. Новых событий пока нет. Можно изучить {{link:route:/editorial/|выбор редакции}}. | Editorial / Popular | Scene 1. |
| `many_changes` | Returning (Many changes) | `new_events` > 15 с `last_visit`. | **P1.** | Агрегация, не пугать числом. | Перечисление десятков событий. | 1. С прошлого визита добавилось более пятнадцати событий.<br>2. Каталог сильно обновился. Посмотрите {{link:route:/new/|всю новую афишу}}.<br>3. Появилось много нового. Особенно в разделе {{link:cat:top_new_cat_id|top_new_cat_title}}. | /new | Scene 1. |
| `fav_cat_explicit` | Authenticated / Explicit Pref | Пользователь явно отметил категорию X. | **P2.** | Ссылка на явный выбор. | «Ваша любимая категория...» | 1. В разделе {{link:cat:id|title}}, за которым вы следите, новое событие.<br>2. Обновление в выбранной категории: {{link:event:new_id|event_title}}.<br>3. Добавлено {{count}} события в раздел {{link:cat:id|title}}. | /category/:id | Scene 2. |
| `inferred_affinity`| Inferred Affinity | > 3 кликов в категории X без явной подписки. | **P3.** | Мягкое предположение. | «Вы любите концерты.» | 1. Часто ищете выставки? Обратите внимание на {{link:event:new_id|event_title}}.<br>2. Возможно, вас заинтересует новое событие в разделе {{link:cat:id|title}}.<br>3. Из похожего на то, что вы смотрели: {{link:event:sim_id|sim_title}}. | /category/:id | Scene 3. Cooldown: 2 дня. |
| `saved_org` | Saved Organizer | Подписка на организатора/площадку. | **P1.** Высший приоритет для новинок. | Акцент на конкретную площадку. | — | 1. У площадки {{link:org:id|org_name}} анонсировано новое событие.<br>2. Новое в афише {{link:org:id|org_name}}: {{link:event:event_id|event_title}}.<br>3. Сохраненный организатор {{link:org:id|org_name}} добавил {{count}} события. | /org/:id | Scene 1 или 2. |
| `saved_approaching`| Saved Event | Сохраненное событие завтра или сегодня. | **P1.** Экстренный. | Напоминание времени старта. | «Билеты заканчиваются» (если нет факта из БД). | 1. Напоминаем: {{link:event:id|title}} состоится уже завтра.<br>2. Ваше сохраненное событие {{link:event:id|title}} начнется сегодня в {{time}}.<br>3. Запланировано на сегодня: {{link:event:id|title}}. | /event/:id | Scene 1. |
| `today_tonight` | Context: Today | Текущее локальное время < 16:00. | **P2.** Зависит от объема афиши на сегодня. | Фокус на вечер. | — | 1. На сегодня в афише {{count}} событий. Начать можно с {{link:route:/today/|главного}}.<br>2. Сегодня вечером: {{link:event:id1|title1}} и еще {{count}} вариантов.<br>3. Для сегодняшнего вечера популярно {{link:event:id|title}}. | /today | Scene 1 (для новых) или Scene 3. |
| `tomorrow_weekend` | Context: Weekend | Четверг/Пятница. | **P2.** | Смещение фокуса на выходные. | «Уже придумали, чем заняться?» | 1. Близится конец недели. Мы собрали {{link:route:/vyhodnye/|главное на выходные}}.<br>2. Впереди выходные: {{count}} событий в городе.<br>3. На субботу и воскресенье запланировано {{count}} вариантов. Выделяем {{link:event:id|title}}. | /vyhodnye | Scene 2. |
| `family_kids` | Category Focus | Пользователь с детьми / Выходные утро. | **P3.** | Мягкая подача возраста. | «Для ваших детей...» | 1. На выходных много {{link:cat:family|семейных событий}}.<br>2. Если планируете отдых с детьми, обратите внимание на {{link:event:id|title}}.<br>3. Сегодня {{count}} событий с рейтингом 0+. | /family | Scene 3. |
| `exhibitions` | Category Focus | Сценарий подкачки выставок. | **P3.** | — | — | 1. Из долгих форматов: открылась выставка {{link:event:id|title}}.<br>2. В разделе {{link:cat:exhibitions|Выставки}} сейчас {{count}} активных событий. | /exhibitions | Scene 3. |
| `concerts` | Category Focus | Фокус на музыку/концерты. | **P3.** | — | — | 1. На этой неделе {{count}} {{link:cat:music|живых концертов}}.<br>2. Из громкого: в пятницу выступает {{link:event:id|title}}. | /music | Scene 2. |
| `theatre` | Category Focus | Фокус на театр. | **P3.** | — | — | 1. Театральная афиша пополнилась. Ближайший спектакль — {{link:event:id|title}}.<br>2. В разделе {{link:cat:theatre|Театр}} на этой неделе {{count}} постановок. | /theatre | Scene 3. |
| `lectures_free` | Category Focus | Лекции / Бесплатно. | **P3.** | Честное указание «Бесплатно». | — | 1. Образовательная афиша: {{link:event:id|title}}.<br>2. На этой неделе {{count}} {{link:route:/free/|бесплатных лекций}}.<br>3. Вход свободный: сегодня проходит {{link:event:id|title}}. | /free | Scene 4 (Serendipity). |
| `pushkin_card` | Segment | Только если `pushkin_card: true` в манифесте. | **P3.** | Точный маркер факта. | — | 1. {{count}} событий недели доступны по {{link:route:/pushkin/|Пушкинской карте}}.<br>2. По Пушкинской карте можно посетить {{link:event:id|title}}.<br>3. Добавлено {{count}} новых событий по Пушкинской карте. | /pushkin | Scene 3. |
| `verified_popular` | Serendipity | `popularity_score` > 90%. | **P4.** | Социальное доказательство без навязывания. | «Все уже идут на...» | 1. Чаще всего сейчас открывают {{link:event:id|title}}.<br>2. Самое популярное событие сегодня — {{link:event:id|title}}.<br>3. Лидеры просмотров этой недели собраны {{link:route:/popular/|здесь}}. | /popular | Scene 4. 1 раз за сессию. |
| `human_editorial` | Editorial | Ручной редакторский флаг в манифесте. | **P2.** | Фокус на "Выбор редакции". | — | 1. Выбор редакции на сегодня: {{link:event:id|title}}.<br>2. Мы собрали {{link:route:/editorial/|события, которые советуем не пропускать}}.<br>3. Редакционный выбор выходных — {{link:event:id|title}}. | /editorial | Scene 2. |
| `stale_manifest` | Stale/Offline | Timestamp глобального манифеста > 24ч. | **P1.** | Нейтральная констатация. | «Сайт сломался.» | 1. Показываем сохраненную афишу. Актуально на {{date}}.<br>2. Сеть недоступна. Можно изучить {{link:route:/popular/|сохраненные события}}.<br>3. Вы в офлайн-режиме. Показываем загруженную ранее афишу. | / | Scene 1. |
| `sparse_catalog` | Sparse Catalog | Менее 5 активных событий в БД. | **P1.** Блокирует все контексты дат. | Фокус на качество, не количество. | «У нас почти ничего нет.» | 1. В афише сейчас всего несколько событий. Рекомендуем {{link:event:id|title}}.<br>2. Затишье в городе. Но проходит {{link:event:id|title}}.<br>3. В ближайшие дни событий немного. | / | Scene 1. |
| `already_viewed` | History | Событие из манифеста есть в `viewed`. | **Penalty (0.2x)** | Не подавать как новинку. | «Вы еще не видели...» | 1. Вы уже смотрели {{link:event:id|title}}. Оно пройдет в эту субботу.<br>2. Возвращаясь к просмотренному: {{link:event:id|title}} начинается в {{time}}. | /event/:id | Manual (после автоплея). |

---

## B. Точная компоновка и Wireframes (Pixel Budget)

**Определение "Hero ≤ 50%":** Глобальный хедер (Header) **НЕ** входит в высоту компонента Briefing, но сумма `Header + Briefing + Category Chips` **не должна превышать 55svh** (с допуском), чтобы пользователь всегда видел начало первой карточки ленты событий на экране без скролла.

### Таблица бюджетов по Viewports (в пикселях CSS)

| Viewport | Header | Category Chips | Briefing Height | Сумма верхней части | Остаток от 100vh | Видимость первой карточки (Feed) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1440 × 900** | 72 | 48 | **260** (28%) | 380 | 520 (57%) | Отличная (полностью видно ~1.5 карточки). |
| **1366 × 768** | 64 | 48 | **220** (28%) | 332 | 436 (56%) | Отличная. |
| **390 × 844** | 56 | 40 | **200** (23%) | 296 | 548 (64%) | Идеальная. |
| **360 × 800** | 56 | 40 | **180** (22%) | 276 | 524 (65%) | Идеальная. |
| **320 × 568** | 48 | 40 | **140** (24%) | 228 | 340 (59%) | Достаточная (видно заголовок и картинку первой карточки). |

### Текстовые Wireframes и Лимиты

**Desktop (1440 / 1366):**
```text
[Header: Логотип              Калининград ▼             Поиск]

[ Briefing Container: padding: 24px; min-height: 220px; ]
(Моноширинный мелкий) КАЛИНИНГРАД · ОБНОВЛЕНО ТОЛЬКО ЧТО       Сцена 1/4   [⏸ Пауза]
 
(H1 / Гротеск 48px, line-height 1.1)
С вашего прошлого визита появилось
12 новых событий. Три из них — 
в разделе [Живая музыка].

[Category Chips: Сегодня | Завтра | Выходные | Бесплатно | Театр ]
[Feed: Карточка 1] [Feed: Карточка 2] [Feed: Карточка 3]
```
*Условия Desktop:* Line-clamp: 3. Font-size: `clamp(32px, 3.5vw, 56px)`. 

**320×568 (Критичный Fallback):**
```text
[Header]
[ Briefing Container: min-height: 140px; padding: 12px; ]
(Мелкий) ОБНОВЛЕНО ТОЛЬКО ЧТО     1/4 [⏸]
 
(H1 22px, line-height 1.2)
Пока вас не было, появилось 12 
новых событий. Три из них — 
в разделе [Живая музыка].

[Chips]
[Feed]
```
*Условия Fallback:* Line-clamp: 3 (строго). Font-size: `22px`. Если текст не влезает в 3 строки — обрезается через `text-overflow: ellipsis`, что является сигналом редактору: тексты для 320px должны быть жестко сжаты.

---

## C. Критика визуального дизайна и Спецификации

### 1. Editorial Poster (Теплая бумага / Графит / Терракота)
*   **Типографика:** Базовый текст — плотный современный гротеск (например, Inter, Golos или свой фирменный), цвет Graphite (`#2D2D2D`). Максимальная длина строки — 40-45 `ch`.
*   **Бренд:** Размещается на фирменном фоне Warm Paper (`#F9F6F0`). Акцентные ссылки — Terracotta (`#CC5533`).
*   **Буква «О»:** Буква «О» в слове «АнОнсы» остается элементом логотипа. Внутри самого брифинга она не растягивается в словах, чтобы не ломать Accessibility (Screen readers не умеют читать искаженные слова). Максимум — при смене сцены весь текстовый блок маскируется (Clip-path) формой вытянутой буквы О.
*   **Поверхность:** Полный Flat (Full-bleed), никаких рамок, теней и карточек. Сливается со страницей.
*   **Оценка:** Brand fit (5/5), Readability (5/5), Novelty durability (4/5 — стареет медленнее, так как это просто хорошая типографика), Banner-blindness risk (Low).

### 2. Ambient Dashboard (Панель приборов)
*   **Спецификация:** Темная подложка, моноширинный шрифт (JetBrains Mono или Roboto Mono), цвет текста — зеленый или оранжевый на темно-сером. 
*   **Оценка:** Brand fit (1/5 — рушит «Культуру Калининграда»), Readability (3/5), Banner-blindness risk (Medium). Отвергнуто.

### 3. Magazine Insert (Журнальная врезка)
*   **Спецификация:** Разделение экрана по вертикали. Крупная цифра (100vh) слева, мелкий текст справа.
*   **Оценка:** Идеально для десктопа, но абсолютно не реализуемо (0/5) для `320x568` без превращения в другой дизайн. Отвергнуто.

**Выбор:** **Editorial Poster**. Он дает минимальный риск (Implementation risk) и 100% поддержку Astro SSG, так как не требует сложных CSS-сеток, подверженных поломкам при гидратации.

---

## D. Стейт-машина моушна и Accessible DOM Model

### Модель Доступности (Accessibility) и DOM
Проблема с `aria-live` и дублированием решается стандартом W3C для Auto-Rotating Carousels.
**HTML-структура (Без дублирования скрытых нод):**
```html
<section aria-roledescription="carousel" aria-label="Ваш городской обзор" class="briefing-container">
  <div class="controls">
    <button aria-label="Остановить автоматическую смену" aria-pressed="false" class="pause-btn">
      <span aria-hidden="true" class="icon-pause"></span> Пауза
    </button>
  </div>
  <div aria-live="off" id="briefing-track"> <!-- aria-live включается в polite ТОЛЬКО при паузе -->
    <!-- Сцена 1 -->
    <div role="group" aria-roledescription="slide" aria-label="Сцена 1 из 4" class="slide active">
      <p>Пока вас не было...</p>
    </div>
  </div>
</section>
```

### Таблица переходов (State Machine)
*Rule of thumb:* При `pointerdown` прерывание завершается немедленно (jump-to-end), а не плавно за 100-150ms. Это гарантирует стабильный хитбокс ссылки в момент отпускания пальца (`pointerup` -> `click`).

| State | Entry Trigger | Timer/Behavior | Visual/DOM | Allowed Exits | Accessible Announcement |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `static_ssg` | Request / No JS | None | Рендер Сцены 1 без opacity. Ссылки кликабельны. | `hydrating` (если JS) | Screen reader читает сразу как обычный текст. |
| `hydrating` | JS Loads | Sync | Чтение LocalStorage. Пересчет `last_visit`. | `entering` | — |
| `entering` | От SSG или смена сцены | ~250ms CSS Transition (`opacity`, `transform`) | Фрагменты сцены всплывают (`stagger` 80ms). Хитбоксы ссылок УЖЕ в конечном размере. | `reading`, `paused_user` (по interrupt) | `aria-live="off"` (Silent). |
| `reading` | Окончание `entering` | `setTimeout` (3000-5000ms в зависимости от `ch` текста) | Текст полностью статичен. | `entering` (Scene+1), `paused_user`, `exhausted` | `aria-live="off"` (Silent). |
| `paused_auto` | `visibilitychange: hidden` | Заморожен | Анимация замирает. | `reading` (при `visibility: visible`) | — |
| `paused_user` | `pointerdown`, `focusin`, `hover` на контейнере | Остановлен **навсегда** | Если поймали на `entering` — немедленный `jump-to-end` (< 0ms). Активация кнопки `aria-pressed="true"`. | `manual` | `aria-live="polite"` включается. SR объявляет текст. |
| `manual` | Клик [Дальше] / Свайп | Остановлен | Смена сцен без стаггера (Fade In 150ms). | `paused_user` | Сразу читается SR. |
| `exhausted` | Показано 3 авто-сцены | Остановлен | Автоплей завершен. | `manual` | — |
| `reduced_motion`| `(prefers-reduced-motion: reduce)` | Остановлен | Переход из `hydrating` сразу сюда. Никаких авто-смен. | `manual` | Читается как обычный абзац. |

### Предотвращение CLS (Reserved Geometry)
Контейнеру жестко задается `min-height`, равная высоте 3 строк H1 на данном брейкпоинте плюс контролы. Использование `line-clamp: 3` гарантирует, что 4-я строка не появится. CLS равен строго `0.0`.

---

## E. Gemini Lite "Lollipop" Prompt Family

Пайплайн использует Gemini не для принятия решений, а как стилизатор/агрегатор (Compression & Phrasing) на стороне сборки статики (или Background Worker'а).

### Versioned I/O Schema (v1.2)
**Вход от бекенда (Immutable Deterministic Facts):**
```json
{
  "schema_version": "1.2",
  "scenario_id": "return_anon",
  "locale": "ru-RU",
  "constraints": {
    "max_chars": 120,
    "max_sentences": 3
  },
  "facts": {
    "new_events_count": 12,
    "top_category_name": "Живая музыка",
    "top_category_id": "cat_18"
  }
}
```

**Lollipop Prompt (Stage 1: Phrasing):**
> Ты — дружелюбный редактор городских афиш Калининграда. У тебя есть факт: добавлено `{{new_events_count}}` новых событий, самое популярное направление — `{{top_category_name}}`. 
> Напиши 3 варианта короткого (до 120 символов) приветствия для вернувшегося на сайт читателя. 
> ПРАВИЛА:
> 1. Не используй местоимения «Я», «Мой».
> 2. Не придумывай названия событий, которых нет в фактах.
> 3. Вместо реальных ссылок используй строго маркер `{{link:cat:cat_18|Текст ссылки}}`.
> 4. Выведи ответ в строгом JSON массиве `variants`, где каждый элемент — строка.

**Hard Validators (Fail-closed Logic):**
Если пайплайн валидации на сервере фиксирует что-либо из нижеперечисленного, ответ Gemini **отбрасывается**, и отдается `deterministic_fallback` ("Новых событий: 12. Смотреть раздел [Живая музыка]").
1. Наличие тегов `<a href...` (Только маркеры).
2. Длина любой строки > 120 `chars`.
3. Наличие галлюцинаций цифр (RegEx ищет цифры в строке и проверяет на равенство фактам).

**Cache Keys & Invalidation:** 
Ключ кеша: `hash(scenario_id + facts)`. Инвалидация происходит только при изменении счетчика событий на порядок (например, 12 -> 20), чтобы не гонять LLM на каждое изменение с 12 до 13.

---

## F. План Эксперимента (Beyond Hero CTR)

Hero CTR (клики по самому текстовому блоку) — обманчивая метрика (Vanity metric). Если блок не кликают, но читают и затем уверенно используют фильтры, он работает. 

**Варианты A/B/C/D:**
*   **A (Control):** Без брифинга. Классический каталог (Header -> Categories -> Feed).
*   **B (Static):** Брифинг есть, но статичный, без моушна. (Проверка ценности текстов).
*   **C (Semantic-fragment):** Наша спецификация (Осмысленная анимация).
*   **D (Literal typewriter):** Диагностический ад (Печать по 1 символу). Ожидаем падения метрик.

### Метрики
*   **Primary (Главная):** *Discovery Transition Rate* — доля сессий на главной странице, завершившихся переходом на страницу конкретного события (Event Detail Page) **любым путем** (через брифинг, через поиск, через ленту). Блок должен разогреть интерес к каталогу в целом.
*   **Secondary:** Время до первого осмысленного клика (Time to first category/event click). Ожидаем снижения (пользователь быстрее принимает решение).
*   **Guardrail:** Homepage Bounce Rate. Если увеличивается на > 5% (относительно), стоп теста. 

### Segmentation & Novelty-Decay
Нельзя оценивать эксперимент в первые 3 дня (Novelty Effect — кликают, потому что блестит). Оценка идет на окнах Day 7, Day 14. 
Отдельная сегментация по `New Users` (насколько понятно) и `Heavy Users` (насколько не раздражает со временем).

### Acceptance Checks (Пре-лонч)
- [ ] Отработка на слабом Android (Chrome, 3G throttling, CPU x4 slowdown) — нет лагов (Jank).
- [ ] Отключение JS в браузере (Fallback показывает читаемый статический текст Сцены 1).
- [ ] Симуляция порчи данных: запись в `localStorage` строки `"{broken_json_!@#}"` не крашит страницу (обработка `try-catch`).
- [ ] `prefers-reduced-motion` включает моментальный показ текста.

---

## G. Red-Team Risk Register (20 векторов)

| # | Риск (Угроза) | Оценка (Sev/Lik/Det) | Mitigation (Смягчение) | Владелец | Blocker Phase |
| :- | :--- | :--- | :--- | :--- | :--- |
| **P0 (Блокируют Prototype/Rollout)** | | | | |
| 1 | **Banner Blindness:** Блок выглядит чужеродно и игнорируется (Evidence: heatmap). | High / High / High | Полностью бесшовный flat-дизайн, совпадение шрифта и фона с лентой. | Design | Rollout |
| 2 | **CLS > 0:** Прыжки ленты при смене сцен из-за разницы строк (Hypothesis). | High / Med / High | Жесткий `min-height`, `line-clamp: 3`. | Frontend | Prototype |
| 3 | **Pointer Hitbox Shifting:** Юзер кликает по ссылке, а она смещается (Hypothesis). | High / Med / Easy | Ссылка существует в DOM с нужными размерами еще до начала `opacity` перехода. | Frontend | Prototype |
| 4 | **Hydration Mismatch:** Astro SSR отдает один текст, клиентский JS меняет его на другой, вызывая мерцание (Flash of Content). | High / High / Easy | Клиентский компонент использует CSS `opacity: 0` до момента гидратации стейта, либо SSR/SSG уже содержит Skeleton. | Frontend | Prototype |
| 5 | **SR Spam (A11y):** Screen reader зачитывает текст каждую секунду при `aria-live`. | Crit / High / Easy | Использовать `aria-live="off"` для автоплея, `polite` только для ручного `paused_user` стейта. | Frontend | Rollout |
| **P1 (Высокие риски Эксперимента)** | | | | |
| 6 | **Ghost New:** Юзер открыл 3 вкладки. Первая обновила last_visit, остальные считают, что новинок нет. | Med / High / Med | `last_visit` обновляется только на `visibilitychange: hidden` или `beforeunload`. | Frontend | Experiment |
| 7 | **LLM Hallucination:** Gemini придумывает цены или время. | Crit / Low / High | Офлайн-пайплайн с Hard Validators. Никакого реалтайм промптинга. | Backend | Experiment |
| 8 | **Local Storage Quota/Clear:** Safari удаляет LS через 7 дней у ITP пользователей (Evidence). | Med / High / Easy | Принятие факта. Сценарий тихо откатывается до `first_anon`. Без крашей. | Product | N/A |
| 9 | **Infinite Loop Strobing:** Анимация мелькает на краю зрения при чтении ленты ниже. | High / High / Med | Жесткий лимит `exhausted` = 3 ивентам. Затем пауза. | Design | Prototype |
| 10| **Over-indexing (Filter Bubble):** Показываем только выставки юзеру, нажавшему туда случайно. | Med / High / Med | Алгоритм Tie-breaker (Serendipity слот) разбавляет выборку. | Backend | Experiment |
| **P2 (Операционные/Сетевые)** | | | | |
| 11| **Manifest Bloat:** Файл global.json растет > 250kb (Evidence). | High / High / Easy | Манифест содержит только 50-100 `top` и `new` событий, а не весь каталог (Pagination). | Backend | Rollout |
| 12| **Stale Cache Node:** Yandex Object Storage отдает CDN ноде старый манифест. | Med / Low / High | ETag, `Cache-Control: s-maxage=300`. Принятие лага в 5 минут. | DevOps | Experiment |
| 13| **Repetition Fatigue:** Пользователь устает от формата через неделю. | High / Med / Hard | Анализ метрики Day-14 Retention в группе C. Снижение агрессивности показа. | Product | Rollout |
| 14| **Font Loading (FOUT):** Брифинг использует H1, который появляется после загрузки веб-шрифта, меняя размеры. | Med / High / Easy | `font-display: swap` + `size-adjust` в CSS (Font metrics override). | Frontend | Prototype |
| 15| **Mobile Header Collision:** На 320px высоты не хватает для 3 строк. | Med / High / Easy | Fallback CSS: обрезка до 2 строк или уменьшение кегля до 20px. | Design | Prototype |
| 16| **Hover State on Touch:** iPad симулирует `hover`, ломая стейт-машину (Evidence). | Med / High / Med | Отключение hover-слушателей при срабатывании `touchstart` (Touch-first detection). | Frontend | Prototype |
| 17| **PII Leakage:** Хранение токенов в `localStorage` (Evidence). | Crit / Low / Med | Строгий code-review: в LS только таймстемпы и массивы ID категорий. | Sec | Rollout |
| 18| **Back/Forward Cache (BFCache):** Юзер жмет "Назад", скрипт не отрабатывает гидратацию, видим старый Last Visit. | Med / Med / Hard | Использование события `pageshow` с флагом `event.persisted` для рефреша стейта. | Frontend | Rollout |
| 19| **Network Offline (Service Worker):** Манифест устарел. | Med / Med / Easy | Сценарий `stale_manifest` корректно обрабатывает `fetch()` ошибки (try/catch). | Frontend | Rollout |
| 20| **Wrong Locale Time:** Браузер юзера в таймзоне UTC+3, события в UTC+2 (Калининград). | High / High / Med | Клиентский код должен принудительно рассчитывать 'сегодня/завтра' в `Europe/Kaliningrad`. | Frontend | Prototype |

---

### Приоритетный список открытых решений и Артефакты перед имплементацией

**Открытые решения к согласованию:**
1. Подтверждение бюджета высоты на 320x568px (Допускаем ли мы 140px высоты, жертвуя обрезкой текста до 3 строк?).
2. Подтверждение отказа от `prefers-reduced-motion` автоплея (полная статика для этого сегмента).
3. Принятие риска `stale-while-revalidate` задержки манифеста (юзер может не увидеть событие, добавленное 1 минуту назад).

**Необходимые артефакты для перехода к разработке:**
1. **Static DOM Sandbox:** CodePen/Figma прототип, демонстрирующий ТОЛЬКО компоновку текста (1440 и 320) без JS. Проверка CLS.
2. **Motion CSS Playground:** Изолированный компонент, где `pointerdown` прерывает `opacity` CSS Transition. Проверка стабильности хитбоксов ссылок.
3. **JSON Manifest Schema:** Зафиксированная схема `global.json`, согласованная с бэкенд/data инженерами (до байта).
