# Agent-assisted event discovery — product, architecture and experiment plan

> **Статус:** продуктовый контракт и техническая спецификация для проверки перед реализацией. Это не runtime/production PASS и не разрешение включить провайдера.
> **Обновлено:** 5 сентября 2026 года, включая восстановление связей «Плана релиза».
> **Техническое решение:** [voice-search-solution-v1.md](voice-search-solution-v1.md).
> **Сквозные зависимости:** [release-integration.md](../../static-personal-announcements/release-integration.md).
> **Постановка по островам всего сайта:** [20260905-floating-islands-system-design.md](../design-system/window-prompts/20260905-floating-islands-system-design.md).

## 0. Owner correction and authority

Этот документ владеет продуктовым поведением разговорного поиска; дочерний v1 — техническими API, данными, состояниями и проверками. Общесистемные правила не копируются в новый независимый поиск. [Предыдущий отчёт](../../../reports/voice-assistant-product-technical-vision-20260905.md) остаётся историческим обоснованием.

| Ограничительная или неполная трактовка | Принятое направление |
|---|---|
| Голос наследует небольшой лимит обычного поиска | Собственная щедрая динамическая allowance внутри общего provider ledger; не запрещать полезную работу при свободной разрешённой ёмкости. |
| Четвёртый Floating Island нежелателен | Несколько управляемых ролей: часть шапки, полки/контекстные заголовки, нижняя навигация, composer. |
| Новый ответ заменяет прежнюю выдачу | Завершённые ответы остаются листаемыми разделами с названием, вопросом, пояснением и карточками. |
| Ответ модели всегда одна короткая фраза | Полезный компактный ответ по умолчанию, раскрываемые детали; factual адрес/транспорт допустимы без карточек. |
| Пока достаточно обсуждать идеи | Требуются полноценная документация решения и отдельное комплексное проектирование общесайтовой системы. |
| Голос и острова можно развить отдельно от июльского релиза | Обязательны связи с надёжным транспортом, сбором и интерпретацией статистики, действующей персонализацией и F1–F17. |

Auth eligibility, provider/privacy/localization, существующий Search, общие владельцы и release gates сохраняются. Численные настройки v1 — инженерные предложения, не уже применённые квоты или измеренные SLA.

Единственные владельцы: [Search](README.md); [transport](../../unsigned-personalization/production-integration.md); [Yandex resilience](../../../operations/yandex-dependency-resilience.md); [analytics](../analytics/README.md); [personalization blueprint](../personalizaion/personalization-to-be.md) и его ручные требования; [data ownership](../../../architecture/personalization-data-ownership.md); [product-model](../../../product-model/README.md); [release umbrella](../../static-personal-announcements/README.md); [autotest strategy](../../../operations/static-site-autotest-strategy.md) и [реестр](../../../testing/static-site-autotest-scenarios.v1.yml).

Продуктовый смысл находится в events-bot-new, общие visual/component contracts — в lovekgd-design-system, Penpot — связанная проекция. Existing pattern `pattern.detached-chrome-control-islands` в [DS #47](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/47) не переименовывается во второй паттерн. [#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621) сохраняет владение нормализацией и одним Kaggle published-preview path. Документальная работа разрешена сейчас; внедрение и A=S=P claims требуют baseline целевых consumers, не выдуманного общего PASS.

## 1. Product hypothesis

Разговорный поиск помогает выразить желание без знания фильтров: «вечером, с ребёнком, бесплатно, не концерт» → подходящие события → уточнение без повторения всей фразы. Это дополнительный discovery-режим по тому же Event Corpus, не универсальный чат и не замена полезной афиши.

Пользовательский outcome — быстро найти подходящее событие и достаточно сведений для решения. Outcome владельца — уменьшение пустых сессий и возвращение пользователей за новым выбором при приемлемой стоимости. Количество реплик с ИИ не является успехом.

Риски: утомительное уточнение, выдуманные факты, потеря речи/контекста, бесполезные расходы, перекрытые CTA, растущая история и невоспроизводимая статистика. Их закрывают конкретные контракты, а не произвольное ограничение функции.

## 2. Initial rollout decision: rescue first, voice is an input mode

Не заменять основной поиск без доказательства пользы. Начальный разговорный эксперимент помогает после нулевого/неудачного результата, повторных переформулировок либо явного «Помочь подобрать». Добровольный голосовой ввод не требует сначала потерпеть неудачу: modality и eligibility эксперимента — разные настройки.

Гость не запускает cost-bearing поиск, вход не делает autosubmit. Анонимная Auth-сессия фокус-группы не является автоматически подтверждённой identity для любых функций. Classic listing/filter fallback остаётся доступным в пределах фактических возможностей системы.

## 3. Architecture decision

Сайт использует свои ограниченные domain operations через существующие Auth и транспорт. MCP — внешний адаптер, не обязательная зависимость пользовательского пути. Выбран небольшой voice service с durable receipts, упорядоченной обработкой и общей Search domain; Devstand означает обычный сервис, не вызов кодового агента. Fly остаётся thin.

Восстановлены два независимых направления: product/Auth/Search → Supabase напрямую или через Yandex relay; optional analytics → Yandex ingest напрямую или через Supabase Edge blind bridge. Это не резервные копии БД. Proxy 200 не доказывает downstream commit, второй маршрут не лечит тот же упавший upstream, ошибка telemetry не портит здоровье product route.

Voice control и media отдельно регистрируются в общем каталоге с проверенными маршрутами, caps и retry semantics. Старые 180 символов и новые большие audio blobs не проталкиваются в существующий endpoint молча. Предел gateway не означает предел человеческой мысли: технические части аудио имеют общую identity и не считаются новыми поисками. Точная реализация — в v1; новый транспорт, индекс, профиль или limiter не создаётся параллельно.

## 4. Model role and elastic resource policy

Разрешённая Lite policy определяется текущим registry/capability check, без скрытого Flash/Pro/Gemma escalation. Embedding — отдельная существующая Search policy и отдельный фактический расход.

Один Lite-вызов на простой ход — ориентир, не безусловный предел. После понимания аудио и получения данных может потребоваться bounded presenter. Модель не описывает результаты, которых ещё не видела. Заголовок не требует отдельного вызова; existing verifier/presenter не дублируется.

Разделены provider hard limits/spend, динамическая voice allowance и технические limits тела/очереди. Каждая реальная попытка проходит reserve→mark_sent→provider→finalize. Свободная ёмкость допускает bursts/borrowing, pressure сначала сокращает лишнюю работу и допустимые пакетные тесты. Fair queue и hysteresis сохраняют вход новым пользователям и устойчивый UX. Принятые реплики не исчезают при смене policy. Sent/unknown отмена не считается unsent refund. Цель — стоимость полезного результата, а не минимальное число обращений любой ценой.

## 5. Search-first, clarify-second interaction

По известным условиям искать сразу, не начинать обязательный длинный опрос. Первая порция карточек — presentation default, не размер всей выборки. Пагинация принадлежит своему answer section.

Спрашивать только о существенной неоднозначности. Hard constraints не ослабляются молча; unknown price не free; альтернативы явно названы. Подстановка примера и переход в готовую подборку различаются. Обычная выборка «Бесплатно» не превращается в набор произвольных служебных блоков.

## 6. Compact conversation state and answer history

Structured intent, временные anchors, исключения и небольшой хвост речи отделены от видимой истории и постоянного профиля. «С ребёнком сегодня» не становится пожизненным свойством пользователя. Новая беседа не создаёт новую analytics session или profile автоматически.

Capture продолжается при ожидающей сети. Durable ordered реплики и атомарные версии защищают от пропусков/дублей. Догон до section commit может уточнить один draft; после commit создаётся новый раздел. Подавляется устаревшая выдача, не исходная речь. Client abort не доказывает отмену провайдера, unknown dispatch не переигрывается вслепую.

Просмотр, база уточнения и pending draft — разные состояния. Прокрутка не меняет target; «Уточнить эту подборку» выбирает старый раздел и создаёт новый в конце с parent. «Второе» привязано к referenced result set. «Из них» относится ко всей логической выборке, не первому экрану. Расширение/обновление истёкшего snapshot обозначается отдельно.

Неизменяемая история решения не возвращает скрытые события. Global exact hide/undo и актуальные lifecycle/facts применяются и к прошлым sections. Видимый префикс/текущая карточка не прыгают при arrival новой profile revision; изменение профиля не переписывает прошлую прозу. Пересчёт интересов и выдача новой проекции принадлежат общему materializer.

Сроки v1 и очистка — отдельные проверяемые настройки. Истечение implicit context не стирает видимую историю тайно; чтение старого ответа не реактивирует истёкшие ограничения. Logout/reset/delete и identity isolation описаны в v1 без смешивания их смыслов.

## 7. Typed response and grounded content

Host проверяет закрытые semantic blocks, IDs и фактические поля. Title/date/price/image/address/action URL/social proof/medallions — из canonical projections. Текст проходит те же factual boundaries, что карточки; JSON schema сама по себе не truth proof.

Название отражает проверенные effective constraints: «Бесплатные события» → «Бесплатные события на побережье». Исходный вопрос остаётся отдельно. Не использовать arbitrary HTML, исполняемый Markdown и model-authored action URLs. Невалидная генерация даёт честный degraded fallback, а не придуманный ответ.

## 8. UI composition: scrollable answers and a coordinated island system

```text
Бесплатные события
  Вопрос → полезное пояснение → общие карточки → Показать ещё

Бесплатные события на побережье
  Вопрос → уточнённый ответ → общие карточки → Показать ещё

Как добраться до выбранной площадки?
  Вопрос → factual explanation / map actions, без фиктивной пустой grid
```

Обычно достаточно нескольких полезных предложений и раскрытия деталей. Важная оговорка не прячется ради короткого текста; длинный вопрос не обрезается молча и не становится нечитаемым мелким шрифтом.

Заголовок идёт в потоке, закрепляется в пределах своего раздела и уступает место следующему. Обратная прокрутка возвращает предыдущий. Не складывать историю заголовков в верхний стек и не закреплять весь ответ. Semantic heading один.

Explicit submit показывает новый heading один раз. Если пользователь затем читает историю, поздний ответ не перехватывает прокрутку; есть «Новый ответ ↓». Изображения, раскрытия, старая пагинация и Back сохраняют anchor. Native text editing не ломается отдельным перехватом стрелок.

Островов несколько; один shared layout owner управляет occupied rectangles, gaps, safe areas, keyboard, слоями и раскрытием. Surface primitive, composition, control semantics и runtime раздельны. Composer не заменяет общий shell; без magic z-index, full-screen invisible hit plane, уезжающего stop и закрытого последнего CTA. Existing drawer/modal/toast имеют согласованный приоритет.

Та же геометрия нужна не только красоте: для below-viewport rerank, точной card exposure и сверки actual served list. Flow→sticky одной шапки не новый page view. Повтор карточки в разных sections не новый уникальный event в общей метрике. Shared layout сообщает геометрию, а не собирает raw pointer/keystroke поток.

Capture и processing независимы. Status соответствует реально известному этапу; JSON indeterminate progress допустим, fake percent нет. Availability manifest не personal admission и не причина постоянного direct polling. Outage не стирает active composer.

A=S=P использует один corpus, sanitized synthetic profile/activation/consent/state, viewport и versioned components. Private profiles не экспортируются в Penpot. Статический board не доказывает network race или microphone quality. Текущий #621 сохраняет один Kaggle published-preview путь; local mock не получает owner-review credit.

## 9. Location, address and transport answers

Пояснение без карточек — полноценный ответ. Достоверный адрес или существующее описание проезда не ждут идеального геокаталога. Location directory остаётся владельцем identities/coordinates/map actions; interim projection не становится второй БД.

Расстояние по прямой, маршрут, расписание и длительность пути различаются. Неизвестные значения не выводятся из общих знаний модели. «Рядом со мной» имеет отдельный permission/privacy flow; unrestricted web search не включён. Старые июльские timetable fixtures не считаются текущим расписанием.

## 10. Social proof, medallions and source recommendations

Initial experiment сохраняет сравнимые карточки/корпус/факты. Compact cards, медальоны, editorial evidence, social proof и карты — отдельные последующие изменения.

«Что рекомендует Культурная чайка?» — evidence-backed source filter, не имитация вкусов. Mention, advertisement и endorsement не тождественны. Отсутствующие подтверждения не выдумываются, новый ручной каталог одобрений не создаётся.

## 11. Product experiment sequence

A — rescue неудачного Search; B — добровольный hybrid; C — проверка основного presentation после доказательства пользы; D — отдельно location/identity/compact enhancements. Лента ответов — принятая форма conversational mode, не эксперимент с максимизацией числа реплик.

Eligible population, exposure, corpus/profile/model/policy versions, denominator и guardrails определяются до сравнения. Добровольный выбор голоса или «Для меня» создаёт selection bias; простое сравнение со всеми не доказывает uplift. Адаптивные ограничения и отказанные opportunities видны в readout, а не исчезают из denominator для улучшения отчёта.

## 12. Measurement

Общие `event_value_reached_rate`, `event_intent_action_rate`, cards/time-to-first-value и определения actors/sessions наследуются из Analytics. Они не переименовываются и не переопределяются внутри voice.

Обязательные вопросы: помогает ли голос найти событие; полезны ли уточнения; не мешают ли острова CTA/чтению; улучшает ли профиль результат; не теряются ли действие и его статистическое подтверждение. На каждый нужен зарегистрированный факт, источник, consent/purpose, denominator, воспроизводимый query/readout, ограничения и конкретное решение владельца. Детальная матрица — в v1 §12 и release-integration, а не второй реестр метрик.

Путь измерения: реальное событие → правильный класс данных → primary/sink receipt → агрегат → проверенный отчёт → finding/decision/follow-up. Browser intent, primary commit и analytics projection — разные состояния. Proxy HTTP 200 и `/general_stats` не доказывают закрытия этой цепочки.

Strong actions считаются по authoritative store; weak visibility/depth/hints — consented bounded summaries; reliability/usage — отдельный operational evidence. Общие budgets/outbox/deduplication сохраняются. Без optional analytics consent продукт и допустимая персонализация работают; analytics не активирует и не обучает профиль произвольным join. Raw query/audio/LLM response/profile/tokens/precise coordinates не уходят в общий поток.

Repeated sections не умножают unique-card depth; rendered не равно seen; фокус и перекрытия учитываются. Save/calendar один durable state, ICS export не внешний import, click не покупка/посещение. Test/preview/synthetic actors исключены. Missing data — coverage gap/INSUFFICIENT_DATA, не ноль. Product Atlas получает reviewed aggregate evidence, не raw stream. Optional action map сохраняет настоящий zero-cost OFF.

## 13. Acceptance thresholds

Численные quality/latency/load thresholds определяются по измеренному baseline. Цель — лучшее обнаружение/намерение, сопоставимый результат с меньшими временем/просмотром либо meaningful rescue. Сохраняются diversity, accessibility, privacy и ресурсные guards.

В тестах недопустимы выдуманные IDs/actions, потеря принятых реплик, применение устаревшей выдачи, дубли primary effects и возврат exact hides. Synthetic-only корпус не доказывает человеческую usability. Пропущенный live test не quality PASS; красивые screenshots не полный release verdict.

## 14. MCP and storage boundaries

External MCP использует те же ограниченные domain services. Никаких arbitrary SQL/network/provider tools. Durable actions и изменения профиля используют прежние Auth/activation/consent/idempotency paths. Не создавать новый profile/email control plane, history vector index, raw analytics в Supabase или второй Google ledger.

По accepted personalization blueprint активация — осмысленное разрешённое действие, не generic checkbox и не простой voice query. Purpose consents независимы. Immediate explicit state и materialized profile разделены; shared store и localization gate не меняются молча из-за нового интерфейса. Analytics actor не определяется беседой или сырым профилем.

## 15. Explicit non-goals

Не строить general chatbot, autonomous purchases, бесконечную raw history, гарантированное background listening, unrestricted research, multi-agent planner или hidden provider escalation. Не изобретать coordinates/route times/popularity. Не включать always-on action map, не использовать raw analytics как скрытый источник профиля.

Эти ограничения не запрещают полезный развёрнутый ответ, несколько островов, щедрый разговор и доступную историю в её retention policy. Надёжность не достигается постоянным урезанием функции, но и не доказывается обходом лимита через другой маршрут.

## 16. Delivery and executable-test plan

V1 §14–16 задаёт пакеты A–E, 32 предложенных voice-сценария и их связь с общими seam tests из release-integration. Пока это проект проверок, не число выполненных тестов. Existing scenario registry/CI остаются единственными исполнителями.

ChatGPT делает product/schema/state/policy/fixture и measurement-oracle работу; кодовый агент интегрирует реальные endpoints, DB/CAS/receipts, транспорт, limiter, capture и browser/fault tests. Общие UI/SoT/Penpot принадлежат действующим family owners #621. Pure voice core может работать inline и развиваться параллельно, не создавая временный второй floating shell.

Первый полезный вертикальный срез: поиск бесплатно → уточнение побережья → hide → другая подборка без возврата скрытого → primary receipt и разрешённая статистика до воспроизводимого readout. Аналитика выключена или YDB недоступна — product path остаётся корректным. Расширение не отменяет F1–F17, source freshness, primary actions и release guards.

GitHub-hosted PR lane использует provider mocks, настоящий browser capture, HTTP fault server и test DB; protected live — реальный разрешённый ASR через общий limiter. Qwen CPU заранее создаёт immutable audio fixtures с независимой разметкой и exact hashes. Не генерировать корпус на каждый PR, не загрязнять им product stats. L2/L3 не заменяются viewport emulation; self-hosted runners не добавляются.

## 17. Closure criterion

Документальная готовность означает, что исполнитель понимает совместимые данные, API, states, transport, profile, measurement и критерии без нового концептуального аудита. Runtime готовность требует реализации, фактических bindings и нужных тестов; deployed и measured состояния подтверждаются отдельно.

Итоговый продукт сохраняет canonical facts/actions, общий Auth/transport/ledger, последовательный догон, историю, явную базу уточнения, глобальные скрытия и неподвижный видимый контент. Общая система островов не мешает чтению, статистика объясняет результат, а подтверждённые действия участвуют в существующей персонализации. Markdown, commit, schema check, screenshot и model response не заменяют всю эту цепочку.
