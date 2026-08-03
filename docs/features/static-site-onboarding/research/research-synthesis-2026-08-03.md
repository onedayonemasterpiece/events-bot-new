# Консолидация исследований онбоардинга KenigEvents

> **Статус:** принятое product-research решение; не implementation approval.  
> **Дата:** 2026-08-03.  
> **Проверенный репозиторный срез:** `main@09fcde9012b30d0c3b4a30d35f45e3c9858b096c`.  
> **Исходники:** [Gemini](gemini-deep-research-2026-08-03.txt), [ChatGPT](chatgpt-deep-research-2026-08-03.txt).  
> **Итог:** [каноническая стратегия](../README.md), [варианты](../strategy-options.md).

## 1. Решение

Оба исследования независимо приходят к одному сильному выводу: обычному пользователю KenigEvents не нужен обязательный tour, tutorial wall, checklist или программа «освоения сайта». Нужна небольшая система контекстных сообщений поверх уже полезного static-first интерфейса.

**Основным источником для стратегии выбрано исследование ChatGPT.** Оно точнее отделяет фактический `main`, open PR, target architecture и research prototype; не сводит активацию к одному неоднородному событию; лучше учитывает capability-specific observability, low-traffic uncertainty, accessibility, legal/data boundaries и возможность того, что дополнительная подсказка вообще не нужна.

**Исследование Gemini сохранено как дополнительный источник.** Из него приняты компактная матрица `proceed | narrow | defer | reject`, акцент на action-result copy, отрицательное отношение к checklist и ранним permission prompts, а также значительная часть сценариев и copy candidates. Его технические выводы и численные рекомендации не используются автоматически.

Выбранная стратегия: **вариант A — сдержанный utility-first contextual onboarding**. Динамический narrative Hero и культурные артефакты остаются challenger/post-release слоями, а не основой MVP.

## 2. Где исследования совпадают

Конвергенция достаточно сильна, чтобы не создавать две равноправные взаимоисключающие стратегии.

| Решение | Gemini | ChatGPT | Итог |
|---|---|---|---|
| Обязательный линейный тур | Reject | Reject | Запрещён для standard user |
| Checklist/progress | Reject | Reject | Не используется как продуктовая механика или KPI |
| Ценность до входа и настройки | Proceed | Proceed | Жёсткий инвариант |
| Contextual/JIT guidance | Proceed | Proceed | Основной принцип |
| Action echo после действия | Proceed | Proceed | Первый implementation slice |
| Inline recovery | Proceed | Proceed | Предпочтительнее overlay/coachmark |
| Dynamic Hero | Narrow | Narrow | Cold/unknown Hero защищён; динамика только как редкий challenger |
| Page-end Talk | Proceed | Proceed | Основной post-value placement, но не по одному scroll depth |
| Login/PWA/permissions | Defer/Narrow | Narrow after value | Только после antecedent value и release/platform eligibility |
| Utility и promo purpose | Разделить | Разделить | Разные consent, state, metrics и suppression |
| Artifact-led onboarding | Narrow | Defer | Не входит в onboarding MVP; отдельный культурный track |
| Focus-group incentives | Изолировать | Изолировать | Нельзя переносить missions/prize/progress |
| Onboarding completion | Не KPI | Не KPI | Не оптимизируется |
| Low-traffic validation | Mixed methods | Mixed methods + A/A/SRM/MDE | Не объявлять победителей underpowered A/B |
| Доступность | Обязательна | Обязательна | Release blocker, не средняя метрика |

## 3. Ключевые расхождения и их разрешение

### 3.1. Одна activation proxy или staged outcomes

Gemini предлагает `first explicit event decision` как главную activation proxy. Это лучше page view, времени на странице и регистрации, но всё ещё объединяет разные по смыслу и наблюдаемости исходы: like, hide, share intent, ICS download и ticket outbound.

ChatGPT предлагает ступени:

```text
FV1 — qualified_event_understanding
FV2 — explicit_event_decision
C1  — capability_success
C2  — continuity_value
R1  — qualified_return
```

**Решение:** принять staged-модель. `explicit_event_decision` остаётся главным оперативным FV2-показателем, но не называется единственной активацией и всегда публикуется вместе с компонентами. Comprehension проверяется исследованиями, а не выводится из 15 секунд dwell.

### 3.2. Роль Hero Talk

Gemini верно предупреждает о banner blindness, но в собственном resolver допускает ранний editorial takeover и в одном journey предлагает блокировать Hero до первого сохранения. Это создаёт противоречивую роль сильной зоны.

ChatGPT предлагает защищать статическое service promise для cold/unknown state, а динамику включать редко и только после доказанного value/return.

**Решение:** первый и неизвестный визит всегда получают стабильную ориентацию. Hero не становится последовательным «учителем следующей функции». Редкая editorial/contextual динамика — отдельный challenger после baseline и non-inferiority проверки.

### 3.3. Page-end Talk: конец прокрутки или post-value

Gemini связывает placement с концом ленты и `IntersectionObserver`. Но человек может доскроллить до конца, ничего не поняв и не выбрав; это delivery event, а не antecedent value.

ChatGPT требует содержательного условия: explicit decision, successful action, saved state, завершённый recovery или явный запрос помощи.

**Решение:** Page-end может физически находиться после контента, но eligibility не выводится из scroll depth. Не более одного proactive message на page journey.

### 3.4. Артефакты

Gemini допускает первую точную подсказку после двух core successes. ChatGPT предлагает вообще не использовать артефакт для обучения core-функциям и вынести его в post-release cultural pilot.

**Решение:** артефакты не входят в onboarding MVP. Позднее возможны три варианта: отдельная трёхобъектная мини-коллекция; статический редакционный маршрут без hidden progress; after-action декоративный artifact после доказательства non-interference. Основной вариант — отдельный культурный слой.

### 3.5. Численные caps и thresholds

Gemini предлагает конкретные `1–2` показа, `14/30 дней`, `>2 возвратов`, `>15% denial`, `>5% CTR`, `>6 месяцев`, `5 секунд undo` и другие пороги. Некоторые полезны как иллюстрации, но в исходнике нет достаточного основания считать их нормами KenigEvents.

ChatGPT корректно маркирует числа как priors/configurable hypotheses.

**Решение:** в канонической стратегии численные caps остаются конфигурацией эксперимента. Без baseline применяется минимальное давление: один proactive exposure на capability/version и отсутствие повторов после permanent dismissal. Остальные значения утверждаются experiment charter.

## 4. Проверка репозиторных утверждений

Исследования не равнозначны по точности repo audit. Ниже зафиксированы наиболее важные проверки на `main@09fcde9…`.

### 4.1. Hero не имеет уже реализованного dismissal

`site/src/components/HomeHeroTalk.astro` — статический Astro-компонент с service promise, двумя CTA и одной event-card-like ссылкой. В нём нет CSS/client dismissal, localStorage state, dynamic registry или exposure transport.

Следовательно, утверждение Gemini «скрытие реализовано на клиенте через CSS» отклонено. Dismissal — будущий contract, а не текущая возможность.

### 4.2. Персонализация не требует Edge SSR

`HomeColdStartFeed.astro` отдаёт полезную статическую ленту и локально переставляет карточки по `ke_personalization_profile`. Целевой `personalization-to-be.md` прямо описывает static-first hybrid: materialized profile projection, local zero-network rerank и периодическое ETag-обновление.

Следовательно, утверждение Gemini, что PR #270 «требует отказа от SSG» и что следует внедрить Edge SSR для Home, отклонено. Возможная island hydration уже совместима со static-first; SSR не является необходимым следствием onboarding strategy.

### 4.3. Доступный toast/live-region уже существует

`site/src/components/MobileToastRegion.astro` содержит отдельные `role=status`/`aria-live=polite` и `role=alert`/`aria-live=assertive`, persistent behavior для action/error, pause при pointer/focus/touch/visibility и reduced-motion fallback.

Следовательно, утверждение Gemini об отсутствии явного aria-live в продукте в целом неверно. Реальная задача — проверить, какие action handlers используют существующий компонент, не создают ли duplicate announcements, и где нужен inline echo вместо global toast.

### 4.4. Favorites/calendar не просто planned

Текущий canonical document фиксирует `R15 candidate implemented`, deployed owner-only Supabase schema, static noindex saved page и calendar-first merge; остаётся browser acceptance через реальный Yandex session. При этом документ одновременно говорит об одном durable saved-event и отдельных `calendar_saved`/`favorite_saved` source flags.

Следовательно, формулировка Gemini «Favorites/Calendar (PR #235) planned» слишком груба. ChatGPT точнее: частично реализованный candidate с незакрытыми release и product-semantics gates.

### 4.5. Search имеет point-of-intent auth gate

Анонимный пользователь видит и заполняет поле `/poisk/`, но current anonymous quota равна `0`; submit сохраняет intent и запускает Yandex PKCE. Production UX требует fresh immutable-candidate real round-trip acceptance.

Следовательно, onboarding может учить формулировать запрос, но не имеет права говорить, что полноценный умный поиск работает без входа или уже production-complete.

## 5. Оценка качества двух исследований

| Критерий | Gemini | ChatGPT | Решение |
|---|---|---|---|
| Точность repo baseline | Средняя; несколько ошибочных claims | Высокая; SHA-pinned и release-state aware | ChatGPT primary |
| Разделение implemented/target/open PR | Непоследовательно | Явно и подробно | ChatGPT |
| Evidence discipline | Много vendor/secondary источников, отсутствует strength grading | Primary/official hierarchy, strength/limitations | ChatGPT |
| Работа с неопределённостью | Числа часто поданы как нормы | Числа как priors/MDE examples | ChatGPT |
| Activation design | Один хороший proxy | Staged outcomes + proxy components | ChatGPT, Gemini FV2 retained |
| Message architecture | Полезный простой resolver | Полный registry/purpose/arbitration contract | ChatGPT |
| Copy examples | 36 широких specimens, часть переобещает текущий release | Меньше рекламности, точнее scope/unknown outcomes | Объединить после fact check |
| Accessibility | Правильное направление, ошибочный repo gap | Нормативные и route-specific gates | ChatGPT |
| Experiment design | Mixed methods, A/A/SRM, но произвольные kill thresholds | MDE, underpowered template, one-mechanism experiments | ChatGPT |
| Простота executive framing | Сильная | Более объёмная | Gemini как summary layer |

## 6. Что принять из Gemini

1. Компактную верхнеуровневую классификацию:
   - `Proceed`: static value, inline recovery, action confirmations;
   - `Narrow`: dynamic Hero, coachmarks, reintroduction;
   - `Reject`: standard checklist/progress;
   - `Defer`: permissions и artifacts до value/release gates.
2. `First explicit event decision` как измеримый FV2 proxy.
3. Copy rule `действие → реальный результат`.
4. Нейтральное отношение к dismissal/permission denial.
5. Большой исходный набор copy candidates как редакционный backlog.
6. Явный запрет на confirmshaming, FOMO, fake urgency и bundled consent.
7. Акцент на изоляции artifact metrics от event activation.

## 7. Что принять из ChatGPT

1. Staged outcomes FV1/FV2/C1/C2/R1.
2. Release-truth inventory и запрет обучать planned capabilities.
3. Protected static Hero для cold/unknown state.
4. Page-end eligibility только после antecedent value.
5. Capability-specific success evidence и `unknown_external_outcome`.
6. Расширенный competency state:
   `dismissed_until`, `dismissed_permanently`, `failed_recoverable`,
   `blocked_dependency`, `deprecated`, versioned reintroduction.
7. Один renderer/control plane, но отдельные system/onboarding, promo и artifact registries/purposes.
8. Exposure как delivery diagnostic, никогда не taste signal и не mastery.
9. Existing live-region reuse вместо параллельного toast engine.
10. Low-traffic protocol: task research → instrumentation A/A → isolated canary → causal claim только при достаточной мощности.
11. Accessibility/legal/privacy как обязательные gates.
12. Cultural artifact как отдельный post-release track.

## 8. Что изменить или отклонить

### Изменить

- `mastered` не определяется универсальным числом кликов. Default-гипотеза — два отдельных успеха без help/recovery либо явный permanent dismiss; capability owner может задать другое evidence.
- Permission denial не всегда равен permanent dismissal всей функции: он блокирует автоматический повтор системного prompt, но настройки могут оставаться доступными по запросу.
- Retention не получает историю onboarding dismissals как CRM target. Но минимальный purpose-local suppression state должен сохраняться, иначе другой renderer начнёт nagging.
- Action echo не всегда toast: локальное reversible действие предпочтительно подтверждать рядом с control; global toast — только когда результат относится ко всей странице или нет устойчивого inline места.

### Отклонить

- обязательный Edge SSR для персонализированного Home;
- scroll до третьего экрана как success evidence;
- dwell `>15 секунд` как comprehension;
- автоматическое предложение reminder сразу после share/save до выпуска reminder contour;
- утверждение, что PWA обязательно «работает без интернета»;
- фиксированные denial/CTR/cooldown thresholds без baseline;
- `7±2` как точный современный capacity contract интерфейса;
- claim о CSS/localStorage dismissal текущего Hero;
- claim об отсутствии live regions в текущем shell;
- медицинский disclaimer в конце Gemini-результата как нерелевантный artifact генерации.

## 9. Вариативность стратегии

Серьёзная вариативность остаётся только в интенсивности коммуникационного слоя.

### Вариант A — сдержанный utility-first contextual onboarding

**Выбран.** Static Hero; persistent IA; inline recovery; action echoes; один Page-end message после value; без dynamic feature-selling Hero и без artifacts в MVP.

Плюсы: минимальный риск CTA cannibalization, проще static/no-JS, меньше state и telemetry, быстрее доказать полезность.

### Вариант B — редакционно-повествовательный contextual onboarding

Hero может показывать редкие route-aware editorial/return sequences для доказанно возвращающихся пользователей; Page-end Talk строит связные narrative chains; после mastery допустим первый точный artifact hint.

Плюсы: сильнее бренд и потенциал связать Hero Talk с редакционным стилем. Минусы: сложнее arbitration, выше banner blindness и pressure risk. Рассматривается только как challenger после стабильного варианта A.

### Слой C — культурное исследование

Не отдельная базовая стратегия. Это post-release extension: bounded collection либо статический культурный маршрут, отдельные ledger/caps/holdout и non-inferiority core CTA.

Полное сравнение: [strategy-options.md](../strategy-options.md).

## 10. Принятые решения

1. Нет стандартного тура, checklist или общего onboarding completion.
2. Первая ориентация статична и полезна без состояния.
3. Никакая proactive подсказка не появляется до core content.
4. Outcome model staged; `explicit_event_decision` — FV2, не универсальная North Star.
5. Не более одного proactive learning message на page journey.
6. Action confirmation и recovery выше feature promotion.
7. Hero и Page-end — placements; intent/source/objective хранятся отдельно.
8. Onboarding не является promo campaign; возможен общий renderer, но отдельный registry/purpose.
9. Login/PWA/reminder/Push только после antecedent value, фактического release и platform eligibility.
10. Utility и promo permissions независимы.
11. Artifacts не обучают core capabilities и не входят в MVP.
12. Focus-group missions/incentives не переносятся.
13. Exposure не является preference, success или mastery.
14. Copy едина семантически; Golden personas не меняют urgency, CTA, права или pressure.
15. Low-traffic неопределённость показывается честно; underpowered test не выбирает rollout.

## 11. Оставшиеся BLOCKED решения

- точная пользовательская семантика `like`, `favorite_saved`, `calendar_saved`, ICS и названия `Моё`/`Мои события`;
- production acceptance Search;
- production-complete anonymous→authorized identity merge;
- фактические personalization factors для `Почему это`;
- utility reminder delivery и lifecycle;
- promo Push consent/delivery;
- применимость и финальный комплект документов по рекомендательным технологиям и персональным данным;
- baseline traffic/conversions и MDE feasibility;
- права/provenance и owner культурного artifact track.

До закрытия соответствующего BLOCKED решения registry обязан считать capability `blocked_dependency`, а copy не может обещать её результат.

## 12. Рекомендуемая последовательность

1. Зафиксировать выбранную стратегию и source hierarchy.
2. Провести task/a11y baseline текущего UI и определить, где достаточно исправить label/IA без подсказки.
3. Решить calendar/save semantics.
4. Описать capability registry, competency state и message arbitration.
5. Первый implementation slice — один уже работающий reversible action: точный echo + Undo + existing live region + no-JS/core fallback.
6. Зарегистрировать fixtures, A/A, SRM и data-quality gates.
7. После evidence — один Page-end continuity experiment.
8. Только после release dependencies — Search recovery, identity sync, PWA и personalization explanation.
9. Отдельно решить, нужен ли культурный artifact pilot вообще.
