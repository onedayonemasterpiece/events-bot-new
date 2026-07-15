# Живой городской обзор на главной: исследование текстового hero-блока

> **Статус:** research / prototype gate, 2026-07-15.
> **Решение:** `Conditional Go` только для изолированного прототипа; production-интеграция, Gemini-пайплайн и персональные данные пока не разрешены.
> **Поверхность:** главная статического KenigEvents, перед категориями и лентой.
> **Рабочее имя компонента:** **«Городской обзор»**. Это не рекламный hero и не чат-ассистент, а компактный навигационный брифинг.

## Короткий вывод

Идея жизнеспособна, если ценность создаёт не «печатная машинка», а быстрый ответ на вопрос пользователя: **что изменилось, что актуально сейчас и куда перейти одним нажатием**. Наиболее сильный сценарий — подтверждённая дельта «с прошлого визита». Наиболее опасный — анимация, которая навязывает скорость чтения и превращает полезный блок в баннер.

Для прототипа принимается направление **Editorial briefing**:

- тёплый бумажный фон, графитовая типографика и терракотовые ссылки;
- плоская композиция без терминальной чёрной карточки, CRT, scanlines и «ИИ-аватара»;
- одна полезная сцена доступна в статическом HTML сразу;
- на desktop последующие сцены могут появляться смысловыми фрагментами;
- на mobile и при `prefers-reduced-motion` — ручная смена без автоплея;
- блок заметно меньше 50% экрана, чтобы категории и начало ленты оставались видны;
- production-тексты в будущем может варьировать Gemini Lite, но факты, выбор событий, claims и ссылки формируются детерминированно и проходят fail-closed валидацию.

## Источники и внешняя консультация

Исследование опирается на:

1. пользовательскую идею и приложенный анализ текстового «живого обзора»;
2. приложенный разбор кинетики: semantic fragments + быстрый ease-out;
3. текущий Astro SSG, реальные routes и static-first personalization contract;
4. [канонический design system](design-system/README.md) и [brand lockups](design-system/brand-lockups.md);
5. [anonymous personalization requirements](../unsigned-personalization/requirements.md) и [event-detail personalization contract](../unsigned-personalization/event-detail-related.md);
6. свежую критическую консультацию через Antigravity/agy, модель **`Gemini 3.1 Pro (High)`**, запуски 2026-07-15 08:18–08:22 UTC, оба `status=0`.

Raw prompt, Part I и исправляющий Part II сохранены в игнорируемом каталоге:

```text
artifacts/codex/static-typed-intro-consultation-20260715/
```

Gemini подтвердил `Conditional Go`, предложил трактовать механику как **Dynamic Navigational Briefing**, выбрал Editorial Poster, отверг буквальный typewriter и рекомендовал static-first fallback. Part II был запрошен отдельно, потому что первый ответ не выполнил требуемую глубину сценарной библиотеки и ошибочно упомянул SSR/Cloudflare/Vercel. Во втором ответе модель исправила основу на Astro SSG + Yandex Object Storage/CDN.

### Что из консультации принято

- продуктовая роль «навигационный брифинг», а не баннер;
- главный JTBD: сокращение времени до осмысленного перехода;
- плоская editorial-композиция;
- semantic fragments вместо долгой посимвольной печати;
- no-JS/reduced-motion статический fallback;
- детерминированный подбор + LLM только как офлайн-редактор;
- отдельное тестирование ценности текста и ценности движения;
- downstream discovery metric вместо оптимизации только CTR hero-блока.

### Что в ответе Gemini скорректировано этой спецификацией

- текущий проект использует **SSG**, а не SSR; manifest — статический same-origin artifact, не `/api`;
- route tokens должны резолвиться в реальные `/segodnya/`, `/zavtra/`, `/vyhodnye/`, `/vystavki/`, `/populyarnoe/`, `/poisk/`, а не в придуманные `/today`, `/popular`, `/free`;
- `line-clamp` не может молча скрывать интерактивную ссылку: copy validator обязан гарантировать полное размещение, иначе выбирается короткий fallback;
- нельзя обновлять `last_visit` на каждом `visibilitychange:hidden`: несколько вкладок и системное сворачивание создадут ложное «ничего нового»;
- включение `aria-live` после смены атрибута само по себе не гарантирует корректное объявление; ручная навигация получает отдельный короткий status contract;
- approved wide-`о` wordmark нельзя анимировать: допустим только отдельный декоративный мотив, не являющийся логотипом, и лишь в prototype lab;
- не подтверждённые текущим data contract «сохранённый организатор», «заканчиваются билеты», рейтинг просмотров и некоторые категории считаются future-gated сценариями, а не доступными фактами.

## 1. Продуктовая роль

### Пользовательская работа

Компонент должен помочь выполнить одну из работ:

1. **Возвращение:** «Что изменилось с прошлого раза?»
2. **Ориентация:** «Что актуально сегодня, завтра или на выходных?»
3. **Сужение выбора:** «С чего начать среди большого каталога?»
4. **Продолжение интереса:** «Есть ли новое в явно выбранной теме?»
5. **Безопасное расширение:** «Покажите один небанальный путь вне привычного».

Он не должен:

- изображать человека или «думающий ИИ»;
- подменять поиск, категории и ленту;
- сообщать неподтверждённую срочность, популярность или персональную близость;
- заставлять ждать завершения анимации;
- становиться единственным способом найти событие;
- бесконечно вращать сообщения.

### Сравнение с альтернативами

| Подход | Сильная сторона | Риск | Решение |
|---|---|---|---|
| Видео-hero | эмоциональный образ | тяжёлый, пассивный, плохо связывает с событиями | не использовать как baseline |
| Статичный обзор | быстро читается, прост | слабее ощущение обновляемости | обязательный Variant B |
| Semantic motion | подчёркивает обновление | может замедлить чтение | Variant C после static gate |
| Буквальный typewriter | заметная метафора | раздражение, moving links, novelty decay | только diagnostic Variant D |
| Categories-first | максимальная управляемость | пользователь сам анализирует каталог | обязательный control Variant A |
| Search-first | сильный явный intent | не помогает без готового запроса | оставить доступным рядом |

### Решение `Conditional Go`

Переход к коду допустим только после согласования:

- desktop/mobile wireframes с реальными header/category/feed размерами;
- статической библиотеки минимум из 24 сценариев;
- claim provenance и route-token schema;
- V0 без Gemini и без персонального backend read;
- motion state machine и accessibility contract;
- плана эксперимента, отделяющего copy value от novelty motion.

## 2. Размещение в пределах первого экрана

### Что означает «не более 50%»

Глобальный header не считается частью компонента. Acceptance-инварианты:

```text
briefing_height <= 0.50 * small_viewport_height
header + briefing + primary_categories <= 0.72 * small_viewport_height
visible_feed_preview >= min(96px, 0.16 * small_viewport_height)
```

`50%` — верхний запрет, не целевой размер. Рабочая цель — `22–32svh`, а на коротких телефонах `20–25svh`. Используется `svh` для первоначальной стабильной компоновки; `dvh` не должен заставлять компонент прыгать при появлении/скрытии browser chrome.

### Бюджет по контрольным viewport

| Viewport | Header | Briefing target / max | Categories + gap | Остаток для начала ленты | Комментарий |
|---|---:|---:|---:|---:|---|
| 1440×900 | 88 | 260 / 360 | 72 | 480 | типографика до 52px, лента видна существенно |
| 1366×768 | 88 | 220 / 300 | 64 | 396 | не более 3 строк основного текста |
| 390×844 | 96 | 196 / 300 | 64 | 488 | первая сцена статична, далее manual |
| 360×800 | 96 | 180 / 280 | 64 | 460 | контролы одной строкой |
| 320×568 | 96 | 136 / 220 | 48 | 288 | короткий copy variant, 2–3 строки, без status-перегруза |

Значения — prototype targets, а не новые design-system tokens. Точный header берётся из approved `AnnouncementsLockup`, а не уменьшается ради hero.

### Иерархия

**Внутри обзора:**

1. короткий видимый label `Городской обзор`;
2. источник/основание (`Сегодня`, `С прошлого визита`, `Выбор редакции`) без технического шума;
3. 1–3 строки, одна мысль, максимум две ссылки;
4. `1 из N`, `Назад`, `Дальше`; `Пауза` только когда автоплей действительно включён;
5. `Смотреть всю афишу` после последней сцены.

**Снаружи и постоянно доступны:**

- поиск;
- текущий город/контекст;
- `Сегодня`, `Завтра`, `Выходные`, `Выставки`, `Популярное`;
- начало общей ленты.

### Wireframe: desktop

```text
┌ approved header / lockup ────────────── navigation + search ┐
│ Городской обзор                         обновлено 8 минут назад │
│                                                               │
│ Пока вас не было, появилось                                  │
│ 12 новых событий. Три — в разделе Живая музыка.              │
│                                                               │
│ Назад   1 из 3   Дальше                        Пауза           │
└───────────────────────────────────────────────────────────────┘
[ Сегодня ] [ Завтра ] [ Выходные ] [ Выставки ] [ Популярное ]

Новые события
┌ event card ┐ ┌ event card ┐ ...
```

- flat/full-width inside shared content container;
- no border, shadow or dark terminal panel;
- main copy `clamp(32px, 3.2vw, 52px)`, line-height `1.02–1.12`, max `28–38ch`;
- links are terracotta, underline remains visible independently of color;
- controls are secondary, each at least `44×44px`.

### Wireframe: mobile

```text
┌ approved mobile brand handle ┐

Городской обзор                     1 из 3
Сегодня в Калининграде 18 событий.
Начните с событий на выходные.

[Назад] [Дальше]
[Сегодня] [Завтра] [Выходные] [ещё]

Новые события
┌ first event card begins ...
```

- first scene is immediately readable;
- no autoplay after scene 1;
- font `24–30px`, at 320px `21–23px`;
- status metadata may be omitted at 320px before reducing copy below 21px;
- no truncation of a link. If a scene fails the 320px fit validator, use its short variant.

## 3. Визуальные направления

| Направление | Механика | Brand fit | Читаемость | Долговечность | Риск баннера | Решение |
|---|---|---:|---:|---:|---:|---|
| **A. Editorial briefing** | крупный текст прямо на paper surface, links как редакционные акценты | 5/5 | 5/5 | 4/5 | низкий | **выбрано** |
| B. Editorial split | desktop: число/факт слева, объяснение справа; mobile складывается в один поток | 4/5 | 4/5 | 4/5 | средний | исследовать как desktop variation |
| C. Calm status rail | компактный label/временной контекст + более спокойный текст | 4/5 | 5/5 | 5/5 | низкий | accessibility/control variation |
| D. Dark terminal/dashboard | mono, тёмный фон, cursor/scanlines | 1/5 | 3/5 | 1/5 | высокий | отвергнуто |

### Выбранный визуальный контракт

- surface совпадает с фоном страницы;
- graphite — основной текст, terracotta — действие, muted warm gray — metadata;
- основная гарнитура — текущий брендовый/system grotesk; mono только для необязательного короткого времени/счётчика;
- одна крупная смысловая доминанта на сцену: число **или** название/категория, но не всё сразу;
- ссылки выглядят ссылками до hover;
- никаких декоративных emoji вместо icons;
- никаких glass/gradient/neon/CRT effects;
- никаких аватаров, speech bubbles и фразы «я нашёл».

### Wide-«о»

[Approved wordmark](design-system/brand-lockups.md) не анимируется и не деформируется. Для prototype lab можно отдельно проверить `aria-hidden`-мотив, геометрически вдохновлённый wide-`о`:

- как маску перехода между сценами;
- как статичную рамку важного числа;
- не внутри слова;
- не как копию/замену логотипа;
- только `transform/opacity/clip-path`, без влияния на layout;
- при reduced motion мотив статичен или отсутствует.

Если мотив принимается, это отдельное изменение design-system governance; исследование не даёт production-разрешения.

## 4. Кинетика и взаимодействие

### Принцип

**Пользователь читает готовый текст; motion сообщает о смене контекста.** Движение не является способом доставки букв.

### Рекомендуемые параметры для Variant C

| Уровень | Desktop | Mobile | Reduced motion |
|---|---|---|---|
| initial useful fragment | видим сразу | видим сразу | видим сразу |
| secondary fragment enter | 180–260 ms | 0–160 ms, только первая сцена | 0 ms |
| stagger | 60–100 ms | 0–60 ms | 0 |
| translate | 4–6px | 0–3px | 0 |
| blur | по умолчанию 0; экспериментально ≤1px | 0 | 0 |
| reading hold | 4–7 s по длине | manual | manual |
| scene exit | 140–220 ms | manual fade ≤120 ms | 0 |
| auto transitions | максимум 2 после initial | 0 | 0 |

`cubic-bezier(0.16, 1, 0.3, 1)` допустим для entrance. Character typing допустим лишь для необязательного status label длиной до 24 символов и не входит в V0/V1.

### State machine

| State | Вход | Видимое состояние | Таймер | Следующие состояния |
|---|---|---|---|---|
| `static_ssg` | HTML loaded | полная безопасная scene 1 | нет | `hydrating`, остаётся static |
| `hydrating` | JS available | тот же текст, без layout change | нет | `ready_static`, `error` |
| `ready_static` | manifest/profile resolved | scene 1 готова | desktop may schedule | `entering_next`, `paused_user`, `manual` |
| `entering_next` | desktop auto/manual next | новая сцена в зарезервированной геометрии | entrance only | `reading`, interrupt |
| `reading` | entrance complete | полностью статичный текст | desktop hold | `entering_next`, `exhausted`, `paused_user` |
| `paused_pointer` | hover/pointer proximity | текущая сцена завершена | остановлен на время hover | `paused_user` or explicit resume |
| `paused_user` | focusin, pointerdown, Pause | полный текст, ручные controls | нет | `manual` only until Continue |
| `manual` | Next/Previous | выбранная сцена | нет | `manual`, `exhausted` |
| `exhausted` | max auto reached | последняя сцена + CTA | нет | `manual` |
| `document_hidden` | visibility hidden | полный текст, no animation | нет | visible → remains paused |
| `stale` | manifest outside safe TTL | non-time-sensitive fallback | нет | manual/static |
| `error` | parse/fetch/profile error | SSG fallback | нет | static |
| `reduced_motion` | media query | full static scene | нет | manual |

### Interrupt contract

- `pointerdown`: `jump-to-end` в тот же frame, stop timer; исходный click по ссылке продолжается.
- `focusin`: полный текст максимум за 100 ms, autoplay выключается до явного `Продолжить`.
- `hover` на fine pointer: завершение до 100 ms, pause; уход мыши сам по себе не включает autoplay после keyboard/user pause.
- `scroll`: если обзор покинул viewport более чем на 50%, autoplay прекращается до конца session view.
- `visibilitychange:hidden`: завершить текущую сцену и pause; не обновлять автоматически visit watermark.
- `pageshow` с BFCache: восстановить текущую scene и manual/paused state, не проигрывать вступление заново.
- `resize/orientationchange`: сохранить scene, пересчитать fit; если copy не помещается, переключить заранее подготовленный short variant без обрезки ссылки.
- link activation: немедленно записать только bounded navigation evidence; animation must not delay navigation.

### Controls и accessibility

- visible buttons: `Назад`, `Дальше`, `Пауза` / `Продолжить`;
- visible progress: `1 из 3`, не dots-only;
- минимум `44×44px`, visible focus ring;
- для auto mode region may use carousel semantics; в manual/static mode обычный `section` проще и надёжнее;
- текущий DOM содержит один настоящий readable text tree, а не визуальную и скрытую дублирующую копию;
- во время auto mode `aria-live="off"`;
- на нажатии `Дальше/Назад` короткий отдельный `role="status"` может объявить `Сцена 2 из 3`, а новая сцена остаётся обычным текстом в DOM;
- focus не переносится автоматически в текст;
- анимируемые декоративные spans получают `aria-hidden`, но link/text semantics остаются на одном статичном родителе;
- порядок DOM не меняется во время fragment reveal.

## 5. Сценарная модель

### Машинные tokens

Copy не содержит raw URL/HTML. Допустимы:

```text
{{link:event:E123|Название события}}
{{link:route:today|афиша на сегодня}}
{{link:route:tomorrow|афиша на завтра}}
{{link:route:weekend|события выходных}}
{{link:route:exhibitions|выставки}}
{{link:route:popular|популярное}}
{{link:route:search:q_family|семейные события}}
{{link:organizer:O42|Название организатора}}
```

Renderer резолвит ID через allowlisted manifest. Не существующий route/event/organizer делает вариант невалидным.

### Общие редакционные правила

- одна сцена = одна мысль;
- 1–3 строки, максимум 2 links;
- факт/время/число берутся только из manifest;
- explicit preference можно назвать выбранной; inferred preference — только осторожное предположение;
- golden personas используются для eval coverage, но не присваиваются человеку как пользовательский label;
- «популярно» только при документированном aggregated popularity signal;
- «выбор редакции» только при human editorial flag;
- viewed downrank, не hide; dismissed hard-exclude на ограниченный TTL;
- не использовать «лучшее», «обязательно понравится», «специально для вас»;
- если безопасной рекомендации нет, показывается честная навигация, а не выдуманная близость.

### Библиотека сценариев

| ID | Eligibility / provenance | Placement / cooldown | Safe vs forbidden | Friendly variants |
|---|---|---|---|---|
| `first_generic` | нет profile/visit facts; static build timestamp valid | scene 1; до появления visit state | описывать каталог; нельзя «для вас» | 1) «Собрали актуальную афишу Калининграда. Начните с {{link:route:today|событий на сегодня}}.» 2) «Здесь события без лишнего шума: {{link:route:weekend|выходные}}, выставки и городские встречи.» |
| `first_today` | active-today count > 0 from Fly export | scene 1; 1/session | точный count; нельзя «самые интересные» | 1) «Сегодня в Калининграде {{count}} событий. Откройте {{link:route:today|афишу на сегодня}}.» 2) «На сегодня есть из чего выбрать: {{count}} событий и {{link:route:exhibitions|выставки, которые идут дольше одного дня}}.» |
| `authenticated_generic` | authenticated session valid, but no eligible explicit preference/delta claim | scene 1; 1/session | можно сказать, что профиль доступен; нельзя симулировать знание вкусов | 1) «Профиль подключён, а афиша доступна целиком. Начните с {{link:route:today|событий на сегодня}}.» 2) «Для начала можно открыть {{link:route:popular|популярное}} или воспользоваться поиском.» |
| `profile_unavailable` | auth/profile overlay timeout, invalid schema or unavailable Supabase; SSG manifest valid | scene 1 static; until overlay recovers | не показывать персональный claim; не сообщать технические детали | 1) «Пока показываем общий городской обзор. {{link:route:today|События на сегодня}} уже доступны.» 2) «Персональный обзор не загрузился, но вся {{link:route:search:q_all|афиша}} работает как обычно.» |
| `return_delta` | coordinated previous watermark; new eligible ids since it | scene 1; once per delta watermark | «с прошлого визита» только при valid watermark | 1) «Пока вас не было, появилось {{count}} новых событий. {{link:route:search:q_new|Посмотреть новое}}.» 2) «Афиша обновилась с прошлого визита: {{count}} новых событий, включая {{link:event:top_new_id|top_new_title}}.» |
| `return_zero` | valid watermark; eligible delta = 0 | scene 1; max 1/4h | честно о нуле; нельзя создавать «новость» | 1) «С прошлого визита новых событий пока нет. Можно открыть {{link:route:popular|популярное сейчас}}.» 2) «Афиша не изменилась. Посмотрите {{link:route:weekend|планы на выходные}}, если ещё не выбирали.» |
| `many_new` | delta ≥ configured threshold | scene 1; 1/delta | агрегировать; не перечислять всё | 1) «Афиша заметно обновилась: {{count}} новых событий. {{link:route:search:q_new|Смотреть по порядку}}.» 2) «Появилось много нового. Больше всего — в разделе {{link:route:search:q_top_new_category|top_new_category}}.» |
| `explicit_category` | user explicitly selected/followed facet; source=`explicit` | scene 2; 1/day/category | «вы выбрали/следите»; не «мы поняли» | 1) «В выбранной категории {{link:route:search:q_category|category_name}} появилось {{count}} событий.» 2) «Обновление в разделе, за которым вы следите: {{link:event:event_id|event_title}}.» |
| `inferred_affinity` | bounded recent actions meet threshold; confidence ≥ gate | scene 3/manual; 1/2 days | «возможно/похоже»; не «любимая категория» | 1) «Похоже на то, что вы смотрели: {{link:event:event_id|event_title}}.» 2) «Возможно, пригодится подборка {{link:route:search:q_category|category_name}}.» |
| `saved_organizer` | future-gated: explicit organizer follow + new canonical event | scene 1/2; 1/new organizer delta | назвать explicit follow; не использовать при inferred venue clicks | 1) «У {{link:organizer:org_id|org_name}} появилось новое событие — {{link:event:event_id|event_title}}.» 2) «Новая дата у организатора, за которым вы следите: {{link:event:event_id|event_title}}.» |
| `saved_event_today` | explicit save; event active and starts today; grounded time | scene 1; once/event/day | точные date/time; no scarcity claim | 1) «Сохранённое событие {{link:event:event_id|event_title}} начнётся сегодня в {{time}}.» 2) «На сегодня у вас сохранено {{link:event:event_id|event_title}}. Время начала — {{time}}.» |
| `saved_event_tomorrow` | explicit save; event starts tomorrow in Europe/Kaliningrad | scene 1/2; once/event/day | точная day boundary | 1) «Напоминание на завтра: {{link:event:event_id|event_title}}.» 2) «Завтра состоится сохранённое событие {{link:event:event_id|event_title}} — начало в {{time}}.» |
| `today_context` | active today, safe current Kaliningrad date | scene 1/2; 1/session | no invented editorial quality | 1) «На сегодня запланировано {{count}} событий. {{link:route:today|Открыть всю афишу}}.» 2) «Сегодня можно начать с {{link:event:event_id|event_title}}, а затем посмотреть всю афишу.» |
| `tonight_context` | current local time + events later today with exact starts | scene 1/2; only before useful cutoff | «сегодня вечером» only when time window matches | 1) «На вечер осталось {{count}} событий. Ближайшее — {{link:event:event_id|event_title}} в {{time}}.» 2) «Если ищете план на вечер, откройте {{link:route:today|оставшиеся события сегодня}}.» |
| `tomorrow_context` | tomorrow list non-empty | scene 2; 1/session | exact date boundary | 1) «На завтра в афише {{count}} событий. {{link:route:tomorrow|Посмотреть все}}.» 2) «Завтра можно начать с {{link:event:event_id|event_title}}.» |
| `weekend_context` | Thu–Sun or explicit weekend intent; list non-empty | scene 1/2; 1/session | no «вдвое больше» without computed comparison | 1) «На выходные собрано {{count}} событий. {{link:route:weekend|Посмотреть субботу и воскресенье}}.» 2) «Планы на выходные уже в афише. Один из вариантов — {{link:event:event_id|event_title}}.» |
| `family` | grounded audience/age/family facets; eligible events | later/manual; 1/day | no assumption user has children | 1) «Для отдыха с детьми есть {{count}} вариантов. {{link:route:search:q_family|Открыть семейную подборку}}.» 2) «Семейный вариант на выходные — {{link:event:event_id|event_title}}.» |
| `exhibitions` | current `/vystavki/` eligible events | later; 1/session | use actual route | 1) «Сейчас идут {{count}} выставок. {{link:route:exhibitions|Посмотреть экспозиции}}.» 2) «Из форматов без спешки — {{link:event:event_id|event_title}}.» |
| `music` | grounded category/tags; safe search route | later; 1/session | no genre inference without facts | 1) «В живой музыке на этой неделе {{count}} событий. {{link:route:search:q_music|Открыть подборку}}.» 2) «Из музыкального нового — {{link:event:event_id|event_title}}.» |
| `theatre` | grounded theatre category | later; 1/session | no invented premiere | 1) «Театральная афиша пополнилась: {{link:event:event_id|event_title}}.» 2) «На этой неделе {{count}} спектаклей. {{link:route:search:q_theatre|Выбрать постановку}}.» |
| `lectures` | grounded lecture category | later/manual; 1/session | no «образовательный» unless format grounded | 1) «В разделе лекций новое событие — {{link:event:event_id|event_title}}.» 2) «На неделе запланировано {{count}} лекций. {{link:route:search:q_lectures|Посмотреть темы}}.» |
| `free` | `is_free=true` or canonical free admission fact | later/manual; 1/session | no price inference from absent ticket | 1) «Вход свободный на {{count}} событий. {{link:route:search:q_free|Посмотреть бесплатное}}.» 2) «Бесплатный вариант на сегодня — {{link:event:event_id|event_title}}.» |
| `pushkin_card` | explicit Pushkin-card fact/medallion | later/manual; 1/day | exact availability only | 1) «По Пушкинской карте доступны {{count}} событий. {{link:route:search:q_pushkin|Посмотреть список}}.» 2) «{{link:event:event_id|event_title}} можно посетить по Пушкинской карте.» |
| `verified_popular` | documented aggregate window + minimum sample/privacy threshold | later/diversity; 1/session | name metric window; no «все идут» | 1) «Чаще всего сегодня открывают {{link:event:event_id|event_title}}.» 2) «В {{link:route:popular|популярном сейчас}} — события, которые чаще смотрят за последние сутки.» |
| `human_editorial` | explicit editor flag + review timestamp | scene 1/2; 1/day | call it editorial | 1) «Выбор редакции на сегодня — {{link:event:event_id|event_title}}.» 2) «Редакция отметила {{link:event:event_id|event_title}} среди событий выходных.» |
| `newly_added` | `created_at` in current manifest window; active/public | later; 1/session | «добавлено», not «new premiere» | 1) «Недавно добавили {{link:event:event_id|event_title}}.» 2) «В свежих анонсах {{count}} событий. {{link:route:search:q_new|Открыть новые}}.» |
| `serendipity` | safe event outside top positive facets; not negative/dismissed | last/manual; 1/session | transparent exploration, not personalization | 1) «Если хочется выйти за привычный выбор: {{link:event:event_id|event_title}}.» 2) «Неожиданный маршрут по афише — {{link:route:search:q_diverse|другая тема}}.» |
| `sparse_catalog` | eligible pool below threshold | scene 1; until pool recovers | honest, no apology drama | 1) «В ближайшие дни событий немного. Один из вариантов — {{link:event:event_id|event_title}}.» 2) «Афиша сейчас компактная. {{link:route:search:q_all|Посмотреть всё доступное}}.» |
| `stale_manifest` | age beyond time-sensitive TTL but cached data still safe | scene 1 static | disclose timestamp; no “today/now” | 1) «Показываем сохранённую афишу, обновлённую {{updated_at}}.» 2) «Новые данные пока не загрузились. Можно открыть {{link:route:search:q_all|сохранённую афишу}}.» |
| `offline` | network unavailable; cached manifest present | scene 1 static | honest offline state | 1) «Сейчас без сети — показываем ранее загруженную афишу.» 2) «Офлайн-режим. Сохранённые события остаются доступными.» |
| `no_safe_recommendation` | candidates all fail gates | scene 1/manual | navigation fallback | 1) «Не будем гадать. Выберите дату: {{link:route:today|сегодня}} или {{link:route:weekend|выходные}}.» 2) «Начните с {{link:route:search:q_all|поиска по всей афише}}.» |
| `already_viewed` | event detail explicitly opened or strong viewed event; still active | manual only; 1/event/day | acknowledge prior view; no “new” | 1) «Вы уже открывали {{link:event:event_id|event_title}}. Событие состоится {{date_text}}.» 2) «Вернуться к просмотренному: {{link:event:event_id|event_title}}.» |
| `dismissed` | explicit “not interested” action, TTL active | never recommend same event | hard exclusion; no copy about hidden item | 1) «Скрытые события не попадут в обзор до окончания выбранного периода.» 2) «Можно продолжить с {{link:route:search:q_all|остальной афишей}}.» |

### Deterministic assembly

1. Filter canonical active/future events and allowed routes.
2. Reject candidates missing claim provenance, link target or viewport-fit variant.
3. Build scenario candidates without LLM:
   - P0: saved event date (future-gated) / safety fallback;
   - P1: valid return delta;
   - P2: current date context or explicit preference;
   - P3: verified popular/editorial/category;
   - P4: one diversity candidate.
4. Hard exclusions: dismissed, past/inactive, unsafe time boundary, stale time-sensitive claim, duplicate event/link.
5. Penalties: explicitly viewed `−80%`; shown scenario within cooldown `−100%`; same category as previous scene `−40%`.
6. Select maximum 4 total scenes; desktop autoplays at most first 3 including initial, mobile autoplays none.
7. Tie-breakers: higher provenance confidence → fewer previous impressions → fresher build → stable scenario ID.
8. Never show both `return_zero` and `return_delta`, or `first_*` and returning personalization, in one session.
9. Record exposure only after scene remained visible long enough to read; do not treat auto-created hidden scene as shown.

## 6. Manifest и personal overlay

### Proposed global artifact

```json
{
  "schema_version": "briefing-manifest-v1",
  "build_id": "<static build id>",
  "generated_at": "<ISO timestamp>",
  "timezone": "Europe/Kaliningrad",
  "safe_until": "<ISO timestamp>",
  "routes": {
    "today": "/segodnya/",
    "tomorrow": "/zavtra/",
    "weekend": "/vyhodnye/",
    "exhibitions": "/vystavki/",
    "popular": "/populyarnoe/",
    "search": "/poisk/"
  },
  "facts": [],
  "scenarios": [],
  "copy_pack_version": "briefing-copy-ru-v1"
}
```

Recommended location is a versioned same-origin static path such as:

```text
/data/briefing/<build_id>.json
/data/briefing/current.json
```

This is a build artifact for Yandex Object Storage/CDN, not a required runtime API.

### Minimal local state

Allowed, bounded and versioned:

```json
{
  "schema_version": "briefing-local-v1",
  "visit_watermark": "...",
  "recent_scenario_ids": ["..."],
  "opened_event_ids": [123],
  "dismissed_event_ids": [{"id": 456, "expires_at": "..."}],
  "explicit_facets": ["music"]
}
```

Do not store email, auth token, exact geolocation, raw search history, demographic guess, long unbounded event history or a human-readable «persona» label in localStorage. Existing consent and personalization profile rules remain authoritative.

### Visit and multi-tab semantics

- one browser profile owns a stable `visit_session_id` and `visit_watermark`;
- tabs coordinate through `BroadcastChannel` with `storage` event fallback;
- current session reads one immutable previous watermark;
- a new watermark commits only after a meaningful session (for example, active view or navigation), and only the coordinator tab writes it;
- backgrounding a tab alone is not a new visit;
- BFCache restore does not create another visit;
- on corrupted state: discard the local overlay and keep the SSG scene.

### Cache and stale behavior

- `current.json`: short CDN TTL plus `stale-while-revalidate`; exact values must follow the existing static publisher contract;
- build-id manifest: immutable long cache;
- after `safe_until`, disable `today`, `tonight`, counts and delta claims;
- after a longer stale limit, render only timeless SSG navigation copy;
- slow Supabase/user overlay never delays initial text;
- V0/V1 require no IndexedDB. Add it only if bounded localStorage state proves insufficient.

## 7. Gemini Lite offline copy pipeline

### Boundary

Deterministic code owns:

- eligibility;
- event/category selection;
- counts, dates, time windows and provenance;
- personalization evidence type;
- ranking and exclusions;
- route/event tokens;
- scenario priority;
- viewport limits.

Gemini Lite may own only:

- Russian phrasing within an approved scenario family;
- grammatical agreement for supplied facts;
- short/normal variants;
- tone variation without semantic expansion.

No runtime LLM call occurs in page view/hydration.

### Versioned input

```json
{
  "schema_version": "briefing-writer-input-v1",
  "scenario_id": "return_delta",
  "locale": "ru-RU",
  "timezone": "Europe/Kaliningrad",
  "facts": [
    {
      "fact_id": "delta_count",
      "value": 12,
      "provenance": "static_export:new_since_watermark",
      "as_of": "2026-07-15T08:00:00+02:00",
      "claims": ["new_since_visit"]
    }
  ],
  "links": [
    {"token": "L1", "kind": "route", "target_id": "search:q_new", "label_facts": ["new_events"]}
  ],
  "personalization": {"evidence": "visit_watermark", "explicit": false},
  "limits": {"normal_chars": 118, "short_chars": 78, "max_links": 2, "max_lines": 3}
}
```

### Output

```json
{
  "schema_version": "briefing-writer-output-v1",
  "scenario_id": "return_delta",
  "variants": [
    {
      "variant_id": "return_delta.normal.01",
      "viewport": "normal",
      "fragments": [
        {"kind": "text", "value": "Пока вас не было, появилось 12 новых событий. "},
        {"kind": "link", "token": "L1", "label": "Посмотреть новое"}
      ],
      "used_fact_ids": ["delta_count"],
      "used_link_tokens": ["L1"]
    }
  ]
}
```

### Lollipop stages

1. `briefing_phrase_v1`: 3 grounded normal variants for one scenario.
2. `briefing_compact_320_v1`: compress an already valid variant without deleting facts/link tokens.
3. `briefing_style_audit_v1`: classify fake intimacy, advertising tone, repeated openings and forbidden claims.
4. Deterministic validators; a model never performs the final acceptance gate.

Example phrase prompt:

```text
Сформулируй 3 коротких варианта только для scenario_id=return_delta.
Используй ровно переданные FACT и LINK tokens. Не добавляй числа, даты,
названия, оценочные claims, HTML или URL. Не используй «я», «мы знаем»,
«специально для вас», «обязательно». Верни JSON по schema.
```

### Hard validators

- output JSON schema exact;
- all digits/date/time/entity claims map to supplied facts;
- every link token is supplied and used at most once unless allowed;
- no raw URL, HTML, Markdown link or unknown event ID;
- maximum 2 links, 3 sentences, length limits by viewport;
- required fact IDs preserved in short variant;
- forbidden phrases/claim classes absent;
- all named event/organizer/category labels canonical or inflected only by an approved safe rule;
- rendered copy fits 320/360/390 and desktop lab without link truncation;
- failure returns deterministic hand-written template.

Cache key includes writer prompt version, model ID, scenario ID, locale, normalized facts, link registry and constraints. Invalidate on any semantic fact/link change, prompt/schema change or editorial override. A count change `12→13` is semantic and cannot silently reuse text saying `12`.

## 8. Исследование и эксперимент

### Phases

| Phase | Scope | Explicit cuts | Gate |
|---|---|---|---|
| V0 documentation | this spec, copy pack, wireframes, schema | no code, no Gemini runtime | product/design review |
| V1 static prototype | Astro lab page, mobile/desktop, categories + feed | no animation, no personal data | layout/a11y/content value |
| V2 motion prototype | semantic fragments + manual controls | hardcoded/fixture manifest | interruption/CLS/reduced motion |
| V3 data-connected prototype | static manifest + bounded local overlay | no production rollout | correctness/perf/privacy |
| V4 experiment | A/B/C/D assignment and analytics | no automatic full rollout | decision thresholds |
| V5 writer pilot | offline Gemini Lite + validators | no client LLM | editorial acceptance |

### Variants and hypotheses

| Variant | Hypothesis |
|---|---|
| A categories-first | fastest control; no briefing value |
| B static briefing | grounded summary improves event-detail discovery without motion cost |
| C semantic motion | adds attention/return signal without degrading downstream discovery |
| D literal typewriter | diagnostic: expected to raise wait/misclick and novelty fatigue |

Personalized vs generic content should be a separate factor or later experiment; otherwise copy relevance and motion cannot be disentangled.

### Metrics

**Primary:** `Discovery Transition Rate` — share of eligible homepage sessions reaching a concrete event detail through any path within the session.

**Secondary:**

- time to first category/event action;
- event details per session;
- category/search/feed path mix;
- useful briefing link activation;
- return-session discovery rate;
- saves/calendar actions downstream when those surfaces are enabled.

**Guardrails:**

- immediate exit / bounce proxy under one common definition;
- scroll depth and first-feed visibility;
- pause/continue/next/back use;
- misclick/rage-click proxy;
- auto-scene exposure vs actual read opportunity;
- CLS, INP, LCP, long tasks and JS errors;
- reduced-motion parity;
- stale/error fallback rate;
- dismissed-topic leakage;
- Day 7/14 novelty decay for returning users.

Do not ship on hero CTR alone. No numeric MDE or sample size is fixed until real baseline traffic and conversion are known.

### Event taxonomy

```text
briefing_impression
briefing_scene_exposed
briefing_scene_completed
briefing_pause
briefing_resume
briefing_next
briefing_previous
briefing_link_activate
briefing_fallback
briefing_error
homepage_category_activate
homepage_feed_event_activate
event_detail_reached
```

Payload is bounded: experiment variant, scenario ID, scene index, link kind/target ID, manifest version, provenance class, viewport class, reduced-motion flag and session-local timestamp. Never send rendered free-form personalized text, auth token or raw profile.

### Qualitative test tasks

1. «Найдите событие на сегодня, не пользуясь поиском».
2. «Поставьте обзор на паузу и откройте ссылку во второй сцене».
3. «Вернитесь назад и продолжите с того же места».
4. «На телефоне перейдите к выходным одним касанием».
5. «Объясните, почему система показала эту рекомендацию».
6. «Найдите категории и первую карточку, не дожидаясь анимации».
7. Screen reader + keyboard: прочитать текущую сцену, перейти по ссылке, сменить сцену.

## 9. Риски

| Риск | Severity | Detectability | Mitigation | Blocker |
|---|---|---|---|---|
| Motion delays reading | high | high | static B gate, ≤600ms composition | experiment |
| Moving link hitbox | critical | high | final geometry from first frame, pointerdown jump | prototype |
| CLS from line count/font | critical | high | reserved height, fit variants, font metrics | prototype |
| Banner blindness | high | medium | flat editorial continuity, downstream metric | rollout |
| Mobile hero crowds feed | high | high | viewport budgets and 320px gate | prototype |
| SR auto-announcement spam | critical | high | aria-live off for auto, manual status only | prototype |
| Keyboard cannot stop motion | critical | high | focusin permanent pause until explicit resume | prototype |
| Tap needs two attempts | critical | high | first pointerdown pauses, same click navigates | prototype |
| Reduced-motion still autoplays | critical | high | static/manual state | prototype |
| Gemini invents facts | critical | medium | deterministic facts + fail-closed validators | writer pilot |
| Fake intimacy | high | medium | evidence-aware language rules | content gate |
| “Popular” without evidence | high | high | metric provenance and sample threshold | content gate |
| Stale manifest says “today” | high | high | safe_until disables temporal scenarios | prototype |
| Multi-tab corrupts delta | high | medium | coordinator + immutable session watermark | data prototype |
| BFCache restarts onboarding | medium | high | pageshow restore | data prototype |
| Dismissed item reappears | high | medium | hard exclusion + contract test | experiment |
| Filter bubble | medium | low | diversity slot outside negative facets | rollout |
| Personal data in localStorage | critical | medium | strict schema/code review/consent boundary | data prototype |
| Manifest bloat | high | high | compact scenario facts, gzip budget | data prototype |
| Slow user overlay blocks UI | high | high | SSG first scene, async enhancement | prototype |
| Route token is broken | high | high | build-time allowlist/link checker | prototype |
| 320px copy truncates link | high | high | short variant; never interactive line-clamp | content gate |
| Wide-«о» mutates approved logo | high | high | separate motif, design-system review | visual gate |
| Repetition fatigue | high | low | cooldowns, Day 7/14 analysis, max scenes | rollout |
| Analytics rewards hero only | medium | high | discovery-level primary metric | experiment |

## 10. Prototype acceptance checklist

### Product

- [ ] The block is described as navigation/briefing, not an AI assistant.
- [ ] Scene 1 provides value without personalization.
- [ ] Maximum 4 scenes/session; no infinite loop.
- [ ] Categories/search/feed remain independently usable.

### Content/data

- [ ] Every claim has fact ID, provenance and safe-until.
- [ ] Every link token resolves to an existing route/entity.
- [ ] Explicit and inferred preferences use different language.
- [ ] Viewed is downranked; dismissed is excluded.
- [ ] Missing facts select a deterministic fallback.
- [ ] 320px short copy is validated, not ellipsized over links.

### Visual/layout

- [ ] Briefing height ≤50svh and hits the stricter viewport targets.
- [ ] Header + briefing + categories leaves at least 96px of feed visible.
- [ ] Desktop 1440×900 and 1366×768 pass.
- [ ] Mobile 390×844, 360×800 and 320×568 pass.
- [ ] Approved lockup is unchanged.
- [ ] Decorative wide-«о» motif, if present, is separate and `aria-hidden`.

### Motion/accessibility

- [ ] No-JS and reduced-motion show full readable scene.
- [ ] `pointerdown` completes and pauses without cancelling link click.
- [ ] `focusin` pauses until explicit resume.
- [ ] Autoplay does not resume when the component leaves viewport or tab hides.
- [ ] Links have stable hitboxes from first frame.
- [ ] Controls are labeled, keyboard reachable and at least 44×44px.
- [ ] Auto changes do not create screen-reader live-region spam.
- [ ] CLS is 0 during scene changes.

### Performance/privacy

- [ ] SSG text renders without manifest/user-overlay fetch.
- [ ] Corrupt/missing local state cannot break the page.
- [ ] Manifest and local payload have explicit byte budgets.
- [ ] No PII/auth token/raw profile text is stored or emitted.
- [ ] Slow/offline/stale paths are tested.

### Experiment

- [ ] A/B/C/D isolate categories-first, static copy, semantic motion and literal typewriter.
- [ ] Primary metric is downstream event discovery.
- [ ] Day 7/14 returning-user decay is inspected.
- [ ] Low-end Android, keyboard, screen reader, reduced-motion and JS-off checks pass.
- [ ] Rollout stop conditions are agreed before exposure.

## Open decisions before code

1. Is the desktop auto-sequence necessary, or should manual navigation be the baseline on every device?
2. Which facts are production-ready today: last visit, explicit facets, favorites, organizer follows, popularity and editorial flags?
3. What exact activity makes a visit watermark meaningful?
4. What byte budget and freshness SLA can the existing static publisher guarantee for briefing manifests?
5. Is the separate wide-«о» transition motif worth a design-system exception, or should V1 stay purely typographic?
6. Which analytics consent state permits experiment events?
7. What first-session generic copy wins editorial approval before any generated variants are considered?

До ответов на эти вопросы корректный следующий шаг — **V1 static content prototype**, а не data-connected personalized motion.
