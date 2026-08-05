# Клавиатурная навигация V8: Power Navigation

> **Статус:** owner-corrected product model и evidence plan; runtime V7 пока не
> переписывается этим документом.  
> **Дата:** 2026-08-04.  
> **Baseline:** `main@0d1848bc324ef8c44df146ec2a7126a116a94bf4`.  
> **Текущая реализация:** V7 router в
> `site/src/lib/keyboardEventNavigation.mjs`, mounted через
> `KeyboardEventNavigation.astro`.  
> **Основной принцип:** сохранить сильную power-user систему V7, устранить
> доказанные тупики и достроить сквозной маршрут чтения, восстановления и
> переходов между страницами.

## 1. Решение владельца продукта

Клавиатурная навигация — не вспомогательный accessibility-only слой и не
эксперимент, который следует заменить обычным `Tab`. Это самостоятельная
продуктовая возможность для небольшой, но ценной группы опытных пользователей.

Её задача:

```text
быстро войти в управление из любого видимого состояния страницы
→ быстро прочитать основную информацию о событии
→ быстро выполнить частое действие
→ быстро посмотреть изображения
→ быстро пройти похожие события как визуальную двумерную сетку
→ Enter открыть выбранное событие
→ продолжить тот же маршрут на следующей странице
→ вернуться назад без потери позиции и логического владельца
```

Критическая метрика качества:

> **На desktop-странице события пользователь клавиатурой всегда может начать
> или восстановить навигацию из любого видимого состояния страницы.**

Это измеряется как `keyboard_start_reliability`: доля проверенных состояний, в
которых один документированный ввод переводит систему к видимому логическому
владельцу или выполняет одно однозначное действие без мыши, зависания и
дублированного side effect.

## 2. Что в V7 сохраняется

Следующие решения считаются сильной продуктовой и технической основой и не
подлежат удалению без отдельного доказательства:

1. **Визуальный граф похожих карточек.** Стрелки выбирают соседей по фактической
   геометрии после CSS-оптимизации, а не по случайной DOM adjacency.
2. **Двумерная навигация.** `←/→` ведут к соседним визуальным карточкам,
   `↑/↓` — к ближайшему центру в строке выше/ниже.
3. **Переход между зонами.** Related и динамическая continuation образуют один
   сквозной граф.
4. **`Enter` на корне карточки.** Открывает выбранное событие.
5. **Контекстные действия.** `L`, `K`, `S`, а для текущего события также `C` и
   `P`, делегируют работу существующим кнопкам, а не создают второй backend.
6. **Галерея.** Есть modal ownership, стрелки, `Escape`, возврат логического
   владельца и защита от протекания held key на скрытую страницу.
7. **Rerender recovery.** При замене карточек персонализацией владелец
   восстанавливается по event ID или детерминированному fallback.
8. **Layout-independent physical keys.** Реализация использует
   `KeyboardEvent.code`, поэтому физический shortcut не ломается при смене
   русской и латинской раскладки.
9. **Lifecycle cleanup.** Listener, observer, timer и managed attributes имеют
   явный `destroy()`.
10. **Минимальная telemetry-модель.** Уже существует local daily set успешных
    action codes без raw key stream, URL, title и точного времени.

Нельзя интерпретировать критику глобального ownership как основание удалить
arrow navigation карточек. Проблема находится в неполном графе состояний и
неоднозначном re-entry, а не в самой двумерной модели.

## 3. Что исследования действительно доказывают

Два внешних результата имеют разное качество.

- Второй отчёт корректно совпадает с baseline SHA, признаёт отсутствие нового
  live replay и в основном точно описывает существующий router, fixtures,
  Playwright contract и сильные стороны focus recovery.
- Первый отчёт содержит неподтверждённые URL, SHA, имена функций, величины
  scroll-step и действия клавиш, которые расходятся с фактическим кодом. Он
  используется только как набор гипотез, но не как release evidence.

Поэтому решения V8 строятся в следующем порядке:

```text
owner requirements
→ текущий source и существующие тесты
→ свежий GitHub Actions browser evidence
→ подтверждённые выводы исследований
→ общие accessibility рекомендации
```

Ни один тезис о реальной поломке конкретного page family не считается
подтверждённым, пока его не воспроизвёл новый evidence workflow на точном SHA.

## 4. Две канонические desktop-семьи

Runtime уже маршрутизирует события в две component families.

### 4.1. Editorial / горизонтальное изображение

Условия в текущем presentation resolver:

- качественный `visual_only` landscape;
- ориентир не меньше `1280×720` и ratio не меньше `1.25`;
- либо подтверждённая landscape event photo рядом с identity poster/document.

Визуальная структура:

```text
┌─────────────────────────────────────────────────────────────────────┐
│ sticky header                                                        │
├─────────────────────────────────────────────────────────────────────┤
│                         горизонтальный hero                          │
│                                                                     │
│  [title / facts / place]                       [CTA + media rail]    │
│                                                                     │
├──────────────────────────── reading surface ────────────────────────┤
│ О событии → Главное → Транспорт                                      │
├─────────────────────────────────────────────────────────────────────┤
│ Смотрите дальше: визуальная сетка карточек                           │
└─────────────────────────────────────────────────────────────────────┘
```

Контракт клавиш:

| Контекст | Клавиша | Действие |
|---|---|---|
| event owner | `←/→` | предыдущее/следующее hero-изображение; safe no-op при одном |
| event owner | `↑` | открыть gallery с текущего изображения |
| event owner | `↓` | войти в reading route с «О событии» |
| event owner | `Enter` | основной CTA |
| event owner | `L/K/S/C/P` | like / calendar / copy link / copy description / copy poster, только если команда доступна |
| reading route | `↑/↓` | предыдущий/следующий read stop или semantic section |
| related card | arrows | существующий visual graph |
| related card | `Enter` | открыть событие |

### 4.2. Split / вертикальная афиша или constrained media

Типичные причины:

- portrait или square visual;
- OCR/document media;
- landscape ниже accepted resolution;
- low-resolution portrait fallback;
- grouped portrait viewer.

Визуальная структура:

```text
┌─────────────────────────────────────────────────────────────────────┐
│ sticky header                                                        │
├──────────────────────────────┬──────────────────────────────────────┤
│ вертикальная афиша / viewer   │ title / facts / place               │
│ sticky media                 │ CTA                                  │
│                              │ О событии                            │
│                              │ Главное                              │
│                              │ Транспорт                            │
├──────────────────────────────┴──────────────────────────────────────┤
│ Смотрите дальше: визуальная сетка карточек                           │
└─────────────────────────────────────────────────────────────────────┘
```

Контракт идентичен по смыслу. Разница только в том, какой media owner открывает
fullscreen gallery/efficient viewer и как рассчитывается визуальный scroll.
Нельзя поддерживать две разные грамматики shortcuts для двух шаблонов.

## 5. V8 graph: не «document mode против application mode», а явные владельцы

V8 состоит из пяти зон:

```text
EVENT
→ READING
→ RELATED
→ CONTINUATION
→ NEXT EVENT
```

Overlay zones:

```text
GALLERY
HELP
CONSENT / AUTH
```

Каждая зона имеет один логический owner. Обычная mouse-навигация остаётся
неизменной. Keyboard router включается после keyboard intent и не требует
автофокуса при загрузке.

### 5.1. EVENT owner

Это видимая action surface текущего события.

- `Enter` — primary CTA;
- `L` — like;
- `K` — calendar;
- `S` — copy title + canonical URL;
- `C` — copy rendered description + URL;
- `P` — copy canonical poster;
- `←/→` — closed hero media;
- `↑` — gallery;
- `↓` — first reading stop.

Если конкретного действия нет, help показывает его disabled и объясняет причину.
Нажатие disabled shortcut не создаёт side effect и даёт короткий status:
`«Календарь недоступен для этого события»`.

### 5.2. READING owner

Существующая V7 почти не моделирует чтение. V8 добавляет два уровня.

#### Semantic sections

```text
О событии
→ Всё главное в одном месте
→ Как добраться / транспорт, если существует
→ Смотрите дальше
```

#### Read stops внутри длинного описания

Длинное описание не перескакивается целиком. После layout router строит
нефокусируемые read stops по границам абзацев:

- следующий абзац выбирается примерно на `55–70%` высоты viewport ниже текущей
  опорной точки;
- строка текста не режется посередине;
- focus остаётся на semantic reading owner, а viewport сдвигается к следующему
  абзацу;
- когда описание исчерпано, следующий `↓` переводит focus на heading следующей
  semantic section;
- `↑` симметрично возвращает предыдущий read stop;
- `PageDown`, `PageUp`, `Space`, `Shift+Space` остаются дополнительными нативными
  путями чтения.

Это заменяет фиксированный микро-scroll `40–72px` на читаемый адаптивный шаг и
не требует десятков focusable anchors в Tab order.

### 5.3. RELATED и CONTINUATION

Существующий визуальный graph сохраняется.

- root каждой managed card — один owner стрелочной навигации;
- `←/→` — visual neighbor;
- `↑/↓` — nearest center в соседней visual row;
- переход с последней related row в continuation остаётся детерминированным;
- `Enter` открывает canonical event link;
- `L/K/S` делегируются выбранной карточке;
- `Escape` из внутренней кнопки возвращает card root;
- `Home/End` ограничиваются текущей managed zone или явно описанным общим
  graph, но не меняют смысл вне карточек.

DOM order не переписывается только ради теоретического паритета. Сначала новый
workflow обязан доказать реальное расхождение visual/Tab/AT order на текущей
сборке. Оптимизированная геометрия карточек является принятой частью продукта.

### 5.4. NEXT EVENT и back route

`Enter` на карточке начинает multi-page route.

Перед navigation сохраняется краткоживущий local handoff:

```json
{
  "version": 2,
  "source_event_id": 7001,
  "target_event_id": 7002,
  "source_zone": "related",
  "source_card_id": 7002,
  "source_visual_index": 4,
  "expires_at": 0
}
```

В handoff нет аналитики, title, URL history или profile data. `expires_at`
ограничен несколькими минутами.

На следующей странице:

- keyboard navigation снова доступна без клика мыши;
- первый directional intent входит в EVENT owner новой страницы;
- help доступен сразу.

При Back / `Alt+Left` / browser history / bfcache:

- восстанавливается исходная card по event ID;
- если card исчезла после rerender — ближайший surviving visual neighbor;
- scroll восстанавливается так, чтобы owner был виден;
- held keys и double-press state не переносятся между документами.

`Backspace` проверяется как advisory platform behavior: современные браузеры не
обязаны использовать его для history navigation. Product contract опирается на
browser Back и `Alt+Left`, но evidence фиксирует фактическое поведение Backspace.

## 6. Universal re-entry: ключевая доработка

Глобальный listener появился не случайно: после load, blur, pointer interaction,
rerender, gallery, browser history и иных переходов `activeElement` может стать
`body`. Удалять recovery до воспроизведения этих случаев нельзя.

Целевая политика:

### 6.1. Глобально разрешены

- `?` — открыть contextual help из любого non-editor состояния;
- directional arrows — восстановить ближайшего владельца и выполнить максимум
  один directional step;
- `Escape` — закрыть верхний overlay либо вернуть nested action к owner.

### 6.2. Глобально не выполняются автоматически

- `L/K/S/C/P/Enter` не должны создавать side effect из произвольного `body` без
  подтверждённого logical owner;
- после recovery сначала восстанавливается owner; действие допустимо только в
  его контексте и должно быть показано help/shortcut badge.

### 6.3. Алгоритм re-entry

При arrow key на `body`:

1. Если сохранённый logical owner существует, подключён к DOM и видим — вернуть
   его.
2. Иначе определить зону по viewport intersection:
   - hero/action visible → EVENT;
   - description/practical/transport visible → ближайший READING stop;
   - related grid visible → ближайшая card к центру viewport;
   - continuation visible → ближайшая continuation card;
   - page-end/footer visible → ближайшая semantic section, а не скрытый owner.
3. Выполнить один step в направлении нажатой стрелки.
4. Показать visible focus и обновить help context.
5. Никогда не выполнять два перехода из одного keydown/repeat.

Это и есть техническое содержание метрики `keyboard_start_reliability`.

## 7. Контекстный help

Help — не длинная документация и не отдельная страница. Это modal/popover,
который открывается кликом по `?` рядом с action surface или физической клавишей
`?`.

### 7.1. Содержимое

```text
Клавиатура · сейчас: Похожие события

[←] Предыдущее событие       доступно
[→] Следующее событие        доступно
[↑] Строка выше              доступно
[↓] Строка ниже              доступно
[Enter] Открыть событие      доступно
[L] Нравится                 доступно
[K] В календарь              недоступно — нет точной даты
[S] Скопировать ссылку       доступно
[C] Описание                 недоступно в карточке
[P] Афиша                    недоступно в карточке

[Esc] Закрыть · ? Открыть снова
```

Требования:

- показывает все commands registry, а не только available;
- доступные — active, недоступные — disabled с причиной;
- labels зависят от текущего owner и page family;
- не обещает media navigation при одном изображении;
- отражает состояние calendar/like/copy support;
- `Tab` ходит по help controls, `Escape` закрывает;
- focus возвращается прежнему owner;
- help не запускает действие и не считается mastery;
- click и `?` используют один компонент.

### 7.2. Keycaps

Существующие badges на CTA и карточках сохраняются. Help не заменяет их, а
объясняет целую грамматику. После устойчивого использования конкретный badge
может скрываться, но `?` остаётся всегда.

## 8. Обучение без обязательного тура

### 8.1. Контекстный popover — рекомендуемый первый опыт

После первого осмысленного keyboard intent на event page рядом с action surface
показывается один раз:

```text
Стрелки ведут по событию и похожим карточкам.
Enter открывает выбранное событие.  ? — все клавиши.
```

Почему не только `«Нажмите ↓, чтобы прочитать событие»`:

- пользователь сразу понимает всю модель, а не одну кнопку;
- `?` становится устойчивой точкой восстановления знаний;
- `Enter` объясняет основной cross-page route;
- текст не обещает одинаковое действие `↓` во всех контекстах.

Popover:

- noindex experiment до owner acceptance;
- не блокирует content/CTA;
- закрывается;
- имеет cooldown;
- не показывается после mastery;
- не появляется во время modal/auth/error.

### 8.2. Hero-talk / home_hero

После нескольких event-page visits без keyboard usage допустима цепочка:

```text
События можно смотреть и без мыши.
→ На странице события нажмите ? — покажем все клавиши.
```

### 8.3. Hero-talk / event_page_end

Если keyboard route не использован:

```text
Эту страницу можно пройти клавишами.
→ Стрелки ведут по разделам и похожим событиям.
→ ? покажет команды.
```

Если route использован:

```text
Вы прошли событие клавишами.
→ Enter откроет выбранную карточку, а Back вернёт к ней.
```

### 8.4. Pointer onboarding

Hero-talk может отдельно объяснить существующий pointer shortcut:

```text
Двойной щелчок по карточке — нравится.
```

Он не смешивается в одной сцене с полной keyboard map: одна новая возможность
за chain.

## 9. Артефакт-пасхалка внутри keyboard route

Речь идёт не о новом «клавиатурном артефакте». Используется обычный collectible
из утверждённой коллекции **«Знаки Янтарного края»**.

Текущий registry draft содержит `8` IDs:

```text
amber-cosmonaut
prussian-brick
ships-bell
migratory-bird-ring
satellite
baltiysk-lighthouse
marzipan-heart
queen-louise-bridge
```

`amber-cosmonaut` уже является first onboarding specimen и не
переназначается. Для keyboard-only placement рекомендуется
**`migratory-bird-ring` / «Кольцо перелётной птицы»**:

- стрелочный маршрут визуально напоминает путь по карте;
- находка естественно связывается с переходом между card zones;
- artifact уже входит в fixed first collection;
- planned difficulty=`standard`, а не onboarding;
- keyboard-only find не блокирует threshold `5 из 8`, даже если человек не
  использует этот способ.

### 9.1. Рекомендуемое место

**Bridge между последней related row и первой continuation row.**

```text
related cards
→ ArrowDown на последней строке
→ вместо мгновенного перехода появляется отдельный artifact node
→ Enter / Space: найти «Кольцо перелётной птицы»
→ ArrowDown: продолжить в continuation
```

Преимущества:

- одинаково работает в Editorial и Split;
- не меняет visual order карточек;
- не маскируется под event card;
- не зависит от количества изображений;
- путь существует только внутри arrow graph;
- placement стабилен при reload/reorder;
- после находки остаётся спокойный marker «Найдено — открыть историю»;
- при отсутствии continuation equivalent bridge создаётся после последней
  related row до page-end.

### 9.2. Другие допустимые места для проектирования

| Placement | Механика | Плюсы | Риск / решение |
|---|---|---|---|
| gallery extra stop | появляется после полного keyboard traversal изображений, перед CTA-slide | кинематографично, хорошо для media-rich страниц | не работает на single-image; только secondary placement |
| reading margin stop | появляется между description и practical после прохождения read stops | связывает находку с чтением | не должен выглядеть как event fact |
| return-from-next-event | после Enter → next event → Back marker появляется рядом с восстановленной card | награждает multi-page mastery | сложнее history/bfcache, нужен strict idempotency |
| help hidden clue | после открытия help показывает clue, но не сам find | хорошо обучает | help exposure не равен find |
| hero wrap stop | после keyboard wrap последнего hero image | заметно | зависит от multi-image, нельзя делать universal |
| page-end completion | после полного EVENT→READING→RELATED route | просто тестировать | слишком предсказуемо, слабее discovery |
| geometric sequence | скрытая комбинация стрелок по grid | игровая редкость | отвергнуть для первой коллекции: непрозрачно и похоже на secret code |

### 9.3. Accessibility и fairness boundary

Owner decision допускает находку только через keyboard route. Чтобы это не
блокировало collection programme:

- этот collectible не является единственным путём к threshold `5 из 8`;
- screen-reader keyboard path получает тот же artifact node и accessible name;
- pointer/touch не раскрывают node;
- shortcut help может дать hint, но не автоматический find;
- отсутствие keyboard usage не влияет на event access, club membership или odds;
- find не становится taste signal;
- exact placement не публикуется в public registry;
- после find receipt идемпотентен и переносится при identity merge.

Это требует явной owner-корректировки прежнего правила, по которому каждый
обязательный placement имел одинаковый pointer/keyboard path: keyboard-only
collectible становится optional bonus внутри коллекции, а не обязательным
условием eligibility.

## 10. Экологичная аналитика

Нужен граф количества пользователей, реально использовавших keyboard navigation
в конкретный день. Собирать raw keydown нельзя.

### 10.1. Daily facts

Одна server row на pseudonymous subject и день `Europe/Kaliningrad`:

```json
{
  "schema_version": "keyboard_usage_daily_v1",
  "subject_hmac": "server-derived",
  "day": "2026-08-04",
  "used_navigation": true,
  "used_reading": true,
  "used_card_graph": true,
  "used_gallery": false,
  "used_action_shortcut": true,
  "opened_help": true,
  "completed_cross_page_route": false,
  "found_keyboard_artifact": false
}
```

Set semantics / `ON CONFLICT` merge: значения только переходят `false→true`.

Запрещено хранить:

- raw key sequence;
- точные timestamp/interval;
- URL, event ID, title;
- clipboard content;
- focus trail и scrollY;
- browser fingerprint;
- ошибочные/случайные keydown до successful command.

### 10.2. KPI

Главный граф:

```text
distinct subjects with any used_* = true by day
```

Диагностические показатели:

- `keyboard_start_reliability` — browser evidence, не пользовательская слежка;
- `keyboard_active_users_daily`;
- `keyboard_help_users_daily`;
- `keyboard_cross_page_users_daily`;
- `keyboard_artifact_finders_daily`;
- доля keyboard users, использовавших reading/card/gallery/action categories;
- `keyboard_dead_end_rate` только из bounded QA/focus cohort evidence, без raw
  production key logs.

Local facts до consent не retro-upload. Reset/delete follows personalization
privacy contract.

## 11. Governing requirements

| ID | Требование |
|---|---|
| KN-01 | Существующий visual card graph сохраняется, пока fresh evidence не докажет дефект |
| KN-02 | `Enter` на managed card всегда открывает именно выбранное событие |
| KN-03 | Из любого видимого non-editor состояния один documented input восстанавливает keyboard owner |
| KN-04 | Editorial-horizontal и Split-vertical проходят одну command grammar |
| KN-05 | `↓/↑` дают сквозной EVENT→READING→PRACTICAL→TRANSPORT→RELATED route |
| KN-06 | Длинное описание имеет paragraph-bound read stops без новых Tab stops |
| KN-07 | `←/→/↑/↓` внутри cards продолжают визуальную 2D-навигацию |
| KN-08 | Related↔continuation bridge выдерживает async hydration/rerender |
| KN-09 | `L/K/S/C/P` выполняются только у подтверждённого current owner и делегируют existing controls |
| KN-10 | Недоступная команда даёт status и не вызывает side effect |
| KN-11 | `?` и clickable question control открывают один contextual help |
| KN-12 | Help показывает active/disabled commands и причины в текущем состоянии |
| KN-13 | Keycaps остаются рядом с частыми действиями и адаптивно скрываются после mastery |
| KN-14 | Первый learning popover объясняет arrows, Enter и `?`, а не только одну `↓` |
| KN-15 | Hero-talk и event page-end имеют separate feature-discovery chains |
| KN-16 | Hero-talk может отдельно объяснить double-click like |
| KN-17 | Artifact — collectible из fixed first collection, не новая сущность |
| KN-18 | Рекомендуемый artifact ID — `migratory-bird-ring`, bridge related→continuation |
| KN-19 | Keyboard-only artifact не блокирует threshold/eligibility и доступен screen-reader keyboard path |
| KN-20 | Multi-page Enter route и Back восстанавливают source card owner |
| KN-21 | Hard reload, soft reload, bfcache, blur, visibility, DOM reorder и resize не создают dead end |
| KN-22 | Held/repeat key выполняет максимум один semantic step до matching keyup |
| KN-23 | Modal/help/consent являются topmost owner и возвращают focus |
| KN-24 | Global re-entry проверяется тестами до удаления или расширения |
| KN-25 | Production analytics хранит только successful daily booleans |
| KN-26 | Dashboard показывает distinct keyboard users по дням |
| KN-27 | Не навешивать shortcuts на редкие специфические кнопки без доказанного сценария |
| KN-28 | Все новые визуальные hints сначала выпускаются как immutable noindex experiment |
| KN-29 | Любой runtime rollout привязан к exact SHA и GitHub Actions evidence |
| KN-30 | Текущая документация, feature flag и фактическое поведение не расходятся |

## 12. Многоэтапные acceptance routes

### Route A — прочитать и выбрать

```text
load Editorial
→ ArrowDown enters EVENT/READING
→ несколько Down проходят description read stops
→ Down practical
→ Down transport
→ Down related first card
→ arrows select card
→ Enter next event
```

### Route B — media-first

```text
load Split
→ directional re-entry EVENT
→ ArrowUp gallery/viewer
→ Left/Right images
→ Escape owner restore
→ C copy description
→ P copy poster when supported
→ Down reading
```

### Route C — card actions

```text
related card owner
→ L like exactly once
→ K calendar exactly once or disabled status
→ S copy selected event
→ ArrowRight next visual card
→ Enter open
```

### Route D — cross-page and back

```text
select related card 5
→ Enter event B
→ keyboard navigation available immediately
→ browser Back / Alt+Left
→ card 5 restored and visible
→ ArrowRight continues from card 5
```

### Route E — recovery

```text
mid-description
→ pointer click inert text / window blur / focus body / resize / rerender
→ one ArrowDown
→ nearest reading owner restored
→ exactly one step
```

### Route F — artifact

```text
last related row
→ ArrowDown
→ keyboard-only artifact bridge appears
→ Enter find receipt
→ story/collection accessible
→ ArrowDown continuation
→ reload marker persists
```

## 13. Evidence before implementation

Первый GitHub Actions workflow должен быть characterization-first:

- автоматически выбрать по built HTML одну Editorial и одну Split page;
- сохранить route, presentation reason, hero ratio и exact SHA;
- пройти state/recovery matrix;
- проверить существующий card graph и Enter routes;
- измерить `keyboard_start_reliability`;
- отдельно отметить target gaps reading/help/artifact;
- сохранить Playwright trace, screenshots, JSON report, console errors и current
  activeElement/owner after each step;
- не выполнять реальные email, OTP, payment или external side effects;
- current-contract regressions могут блокировать PR;
- target gaps сначала advisory, затем переводятся в blocking по мере реализации.

## 14. Этапы

### K0 — evidence

- owner requirements;
- machine-readable scenario matrix;
- GitHub Actions characterization on both page families;
- fresh findings без изменения V7 runtime.

### K1 — reading and re-entry

- semantic sections;
- paragraph read stops;
- viewport-aware re-entry;
- multi-page handoff/back restore;
- tests become blocking.

### K2 — help and onboarding

- contextual `?` help;
- key availability registry;
- one-time noindex popover;
- Hero-talk/page-end capability model.

### K3 — artifact

- approved `migratory-bird-ring` placement bundle;
- related→continuation bridge;
- stable find receipt and collection handoff;
- keyboard/screen-reader evidence.

### K4 — analytics and rollout

- daily boolean ledger;
- distinct-users-by-day graph;
- exact-SHA release gate;
- bounded focus-group/noindex canary before public rollout.

## 15. Non-goals

- не превращать каждую кнопку сайта в shortcut;
- не заменять мышь для обычного пользователя;
- не удалять card geometry optimizer без evidence;
- не создавать обязательный tutorial;
- не создавать новую сущность «keyboard artifact»;
- не собирать поток клавиш;
- не считать shortcut exposure освоением;
- не выдавать документацию или workflow skeleton за подтверждённый runtime PASS.
