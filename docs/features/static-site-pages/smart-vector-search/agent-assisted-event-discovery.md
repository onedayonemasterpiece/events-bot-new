# Agent-assisted event discovery — product, architecture and experiment plan

> **Status:** продуктовая концепция и owner corrections зафиксированы; техническая спецификация v1 подготовлена для implementation review. Это не production/runtime PASS и не разрешение включить провайдера.  
> **Updated:** 5 September 2026.  
> **Implementation specification:** [voice-search-solution-v1.md](voice-search-solution-v1.md).  
> **Новое окно ChatGPT по сквозной системе островов:** [20260905-floating-islands-system-design.md](../design-system/window-prompts/20260905-floating-islands-system-design.md).

## 0. Owner correction and authority

Этот документ — владелец продуктового поведения разговорного поиска. Технические контракты API, состояния, конкурентность, данные, recovery, числовые defaults и тест-план описаны один раз в дочерней [спецификации v1](voice-search-solution-v1.md), а не в новых хронологических отчётах. Предыдущий [аналитический отчёт](../../../reports/voice-assistant-product-technical-vision-20260905.md) остаётся обоснованием, не текущим нормативным дублем. Детализация редакции `bce0a4a` консолидирована в v1 без отмены принятых ниже правил.

| Было в ограничительной трактовке | Принято по уточнению владельца |
|---|---|
| Голос наследует небольшой лимит обычного поиска | Отдельная щедрая load-adaptive product allowance внутри единого provider ledger. Не блокировать полезное использование при свободном разрешённом ресурсе. |
| На странице не нужен четвёртый остров | Нужна управляемая система: часть шапки, подходящие полки/контекстные заголовки, нижняя навигация, голосовой composer. Нет запрета на несколько островов. |
| Уточнение заменяет текущую выдачу | Завершённые ответы остаются самостоятельными листаемыми разделами: название, вопрос, полезный ответ, карточки. |
| Ответ модели всегда одна крошечная фраза | Кратко по умолчанию, раскрываемо при необходимости; фактические ответы про адрес/транспорт полноценны даже без карточек. |
| Продолжать только обсуждение гипотезы | Теперь нужна документальная проработка решения и отдельное комплексное проектирование островов для всего сайта. |

Auth-only, provider eligibility/privacy, текущий production Search, shared ownership и release gates не отменены. Числовые defaults из технического документа — инженерные предложения для review, не применённые квоты или уже подтверждённые SLA.

[Canonical Search](README.md) владеет действующим поиском; [personalization ownership](../../../architecture/personalization-data-ownership.md) — identity/profile/consent; [mobile shell](../mobile-shell.md) и LoveKGD Design System — общей оболочкой; [autotest strategy](../../../operations/static-site-autotest-strategy.md) и [реестр сценариев](../../../testing/static-site-autotest-scenarios.v1.yml) — тестовой инфраструктурой. Продуктовый смысл принадлежит events-bot-new, общие visual/component contracts — lovekgd-design-system, Penpot — связанная проекция.

Система островов имеет существующий ID `pattern.detached-chrome-control-islands` и [DS PR #47](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/47). Новый handoff адресно разделяет проектирование сейчас и последующее baseline-gated внедрение; старое ожидание полного AS-IS не должно запрещать документальную работу. [#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621) остаётся текущим владельцем нормализации/интеграции и одного опубликованного Kaggle Preview. Ни этот документ, ни handoff не объявляют нормализацию завершённой и не разрешают незаметный redesign проверяемого AS-IS.

## 1. Product hypothesis

Разговорный поиск помогает выразить желание без знания категорий и фильтров: «вечером, с ребёнком, бесплатно, не концерт» → подходящие события → уточнение без повторения всего запроса. Это дополнительный способ поиска по тому же Event Corpus, не универсальный чат и не замена полезной ленты.

Ценность: multi-constraint input, постепенное уточнение, понятное объяснение совпадений, спасение неуспешного поиска, возврат к прежним вариантам, factual location/transport answers. Риски: утомительные вопросы, выдуманные факты, бесполезные модельные вызовы, потеря контекста, неограниченно растущий DOM и перекрытый островами контент. Их закрывают конкретные контракты v1, не произвольное урезание функции.

## 2. Initial rollout decision: rescue first, voice is an input mode

Не заменять основной Search без доказательства полезности. Начальный assistant experiment — нулевая/неудачная выдача, повторные переформулировки или явное «Помочь подобрать». При этом добровольный голосовой ввод в поиске не требует обязательно сначала потерпеть неудачу: modality и eligibility разговорного эксперимента — разные настройки.

Гость не запускает cost-bearing операции; вход не делает autosubmit. Обычная лента/фильтры и возврат из assistant всегда доступны в пределах фактических backend capabilities.

## 3. Architecture decision

Сайт вызывает свои типизированные domain operations через существующие Auth/transport semantics. MCP — будущий внешний адаптер, не обязательный hot path. Выбран небольшой самостоятельный first-party voice service с durable receipts, ordered processing, общей Search domain и Supabase. Размещение Devstand не означает вызов coding agent на каждый запрос; Fly остаётся thin.

Новые HTTP interfaces и места source integration описаны в [v1 §2–6](voice-search-solution-v1.md#2-прочитанная-база-и-точные-места-интеграции). Нельзя просто вставить длинный transcript в legacy поле 180 символов, копировать retrieval, профили, карточки или Google ledger. Существующий Search wire contract сохраняется. Admin/Smart Update endpoints публичному ассистенту не выдаются.

## 4. Model role and elastic resource policy

Используется owner-approved Lite policy; конкретную модель разрешает текущий registry/capability check. Не наследовать скрыто Flash/Pro/Gemma fallback. Embedding остаётся существующей отдельной Search policy и учитывается отдельно.

Один Lite call на простой ход — цель эффективности, не обязательный предел. Когда нужно сначала понять аудио, потом получить события и объяснить найденное, допустим bounded presenter после retrieval. Модель до retrieval не может описывать неизвестные результаты. Не дублировать existing verifier/presenter ещё одним writer; title сам по себе не требует отдельной генерации.

Разделять provider hard limits/spend, динамическую voice product allowance и технические bytes/duration/queue ceilings. Всё реальное проходит reserve→mark_sent→provider→finalize. Свободный ресурс допускает bursts/borrowing; pressure сначала уменьшает ненужную работу и допустимый discretionary batch. Fair queue сохраняет доступ новым пользователям, hysteresis предотвращает мигание статуса. Accepted utterances не пропадают при смене политики; sent/unknown отмена не превращается в unsent refund. Дубликаты/транспортные части не являются новыми пользовательскими вопросами.

Политика, metering, telemetry и absence-of-limiter behavior конкретизированы в [v1 §11–12](voice-search-solution-v1.md#11-ресурсная-политика-и-egress). Публичный CDN status не раскрывает personal allowance и не даёт права обойти server admission.

## 5. Search-first, clarify-second interaction

Искать по уже известным условиям и показывать полезные кандидаты до длинного опросника. Небольшая первая страница карточек — presentation default, не ограничение всей выборки. «Показать ещё» принадлежит конкретному разделу.

Уточнение обязательно только при существенной неоднозначности. Hard constraints не ослабляются молча, unknown price не считается free, альтернативы явно обозначены. Подстановка поискового примера и переход в готовую подборку различаются; типовая фильтрованная выборка не превращается в набор случайных служебных блоков.

## 6. Compact conversation state and answer history

Хранить structured intent, происхождение условий, правильные временные anchors, исключения и небольшой хвост речи; не бесконечный transcript в prompt. Отдельно существуют active task, листаемая история и постоянные предпочтения. «С ребёнком сегодня» не становится пожизненным свойством профиля.

Capture не блокируется ожидающей сетью. Реплики durable/ordered/idempotent; догон до публикации может уточнять один draft. Atomic section commit — чёткая граница: уже завершённый ответ immutable, следующая реплика создаёт новый раздел. Поздний ответ не перезаписывает свежий, но исходная речь не выбрасывается вместе с отменой retrieval.

Просматриваемый раздел, выбранная база уточнения и pending draft — разные поля. Scroll не меняет target. Явное «Уточнить эту подборку» выбирает старый section; новый ответ добавляется в конец с parent-ссылкой. «Второе событие» связано с реально referenced result set. «Из них» работает по логической parent selection, а не только первому экрану; расширение запроса и refresh устаревшего snapshot обозначаются отдельно.

В [v1 §4–7 и §10](voice-search-solution-v1.md#4-доменная-модель) заданы поля, race/crash semantics, ownership, membership, TTL и deletion. Forgetting implicit context не означает незаметного стирания видимой истории; отображение истории не реактивирует expired условия. Logout/delete изолируют identity и late callbacks. Сроки 30 минут/7 суток — proposed configurable defaults, не уже утверждённые ограничения.

## 7. Typed response and grounded content

Модель возвращает закрытые semantic blocks; host проверяет ID, источник и структуру. Event title/date/price/image/address/action URL/social proof/medallions берутся из canonical projections. Prose проходит те же factual boundaries, что карточка; валидный JSON сам по себе не доказывает правду.

Название выборки происходит из effective constraints: «Бесплатные события» → «Бесплатные события на побережье». Исходный вопрос остаётся отдельно. Без arbitrary HTML, executable Markdown и model-authored navigation URLs. Допустим безопасный текст с абзацами/выделением/короткими списками. Ошибочная генерация даёт честный degraded fallback, не invented facts.

## 8. UI composition: scrollable answers and a coordinated island system

Разговор — вертикальная лента самостоятельных результатов:

```text
Бесплатные события
  исходный вопрос → полезное пояснение → EventCards → Показать ещё

Бесплатные события на побережье
  исходный вопрос → уточнённый ответ → EventCards → Показать ещё

Как добраться до выбранной площадки?
  вопрос → factual explanation/map actions, без фиктивной пустой grid
```

Обычный ответ — несколько полезных предложений; детали раскрываются, а не прячутся нечитаемым шрифтом. Важная оговорка/неизвестность не скрывается ради короткого текста. Длинный user query раскрывается без truncation.

Section heading идёт в потоке, затем становится компактным sticky island в границах своего раздела; следующий заголовок заменяет его, прокрутка вверх возвращает предыдущий. Не накапливать исторические шапки, не дублировать доступный heading и не pin весь ответ.

Явный submit показывает новую границу/заголовок один раз. Если пользователь затем читает историю, поздний ответ не перехватывает scroll: «Новый ответ ↓». Back, поздние изображения, раскрытие текста и пагинация старого ответа сохраняют anchor. Не перехватывать стрелки поверх native text editing.

Островов несколько: partial header, single-shelf/answer context, bottom navigation, composer. Appearance не равен fixed positioning. Один shared layout owner задаёт measured occupied space, gaps/safe areas/keyboard/layers/expanded policy; никаких независимых top/bottom/magic z-index. Различать surface primitive, composition, control semantics и runtime behavior; не создавать universal pill/window manager или вторую оболочку. Drawer/modal/toast/focus precedence согласованы с существующими consumers. Stop-recording и последний CTA доступны, touch target не уезжает, невидимый screen-sized hit plane запрещён.

Полная сквозная система проектируется в отдельном [окне Floating Island](../design-system/window-prompts/20260905-floating-islands-system-design.md). Конкретный Search→Shell adapter и capture/status/scroll implementation — [v1 §8–9](voice-search-solution-v1.md#8-capture-и-пользовательские-состояния). Voice не захватывает владение общей оболочкой сайта.

Capture и processing независимы, статусы соответствуют реально известной стадии. JSON indeterminate progress допустим; fake percent/притворная ASR-транскрипция нет. Expiring CDN manifest не создаёт per-tab backend ping; outage не удаляет уже активный composer/контекст.

A=S=P означает общий corpus, versioned dependencies и UI-state/viewport fixtures для Astro, Git SoT и native Penpot. Статический board доказывает заявленное визуальное состояние, не network race или физический микрофон. Owner review и опубликованный Preview сохраняют единый Kaggle path #621; локальный mock не получает их credit.

## 9. Location, address and transport answers

`explanation` и `mixed` — полноценные результаты. Достоверный адрес или существующее описание проезда можно дать без ожидания идеального геокаталога. Location directory остаётся единственным владельцем canonical identities/coordinates/map actions; trusted interim projection не становится второй БД.

Различать расстояние от места, straight-line, маршрут и длительность пути. Нет invented timetable/route time из общих знаний модели; неизвестность явно сообщается. «Рядом со мной» требует отдельного permission/privacy flow. Произвольный интернет-поиск не включён в обычный Search.

## 10. Social proof, medallions and source recommendations

Initial comparison сохраняет ту же карточку/корпус/факты, чтобы проверить разговорный слой. Compact cards, organizer/venue medallions, editorial evidence, behavioral proof и richer maps — отдельные позднейшие эксперименты, а не одновременно новый дизайн всего продукта.

Запрос «Что рекомендует Культурная чайка?» — будущий подтверждённый source filter, не имитация вкусов человека. Mention/advertisement/endorsement различаются; отсутствующие рекомендации не выдумываются. Canonical source/medallion evidence переиспользуется, новый ручной список одобрений не создаётся.

## 11. Product experiment sequence

A: rescue неуспешного Search с прежним corpus, classic fallback, matched/missed outcome и общей динамической policy. B: добровольный hybrid с ясностью, что оба режима ищут по одному каталогу. C: randomized primary presentation после доказательства пользы с сопоставимыми retrieval/card/actions. D: отдельно location/identity/compact-card enhancements.

Лента ответов — принятая форма разговорного режима. Эксперименты измеряют её пользу, а не считают каждую новую ИИ-реплику успехом. Маленький искусственный cap не подменяет cost guardrails.

## 12. Measurement

Основные existing outcomes: event_value_reached_rate, event_intent_action_rate, cards_to_first_event_value, time_to_first_event_value, matched/missed, abandonment. Дополнительные: rescue success, turns to value, clarification-without-result, accepted refinement, возврат/уточнение старой выборки, assistant→classic switch, lost/duplicate utterances и stale applied results, невольный scroll jump, schema/factual fallback, p50/p95 latency/queue, calls/tokens/audio seconds/egress на успешный выбор.

Adaptive policy измеряет также зря простаивающую разрешённую ёмкость при заблокированном пользователе, oscillation и starvation новых пользователей. Не логировать raw queries/audio/identity в общей или публичной аналитике. Не оптимизировать низкий расход сам по себе ценой деградации поиска.

## 13. Acceptance thresholds

Числовые quality/latency/load thresholds фиксируются после измеренного baseline. Требуется рост discovery/intent success, либо сопоставимый успех с меньшими временем/просмотром, либо содержательное rescue failed sessions. Guardrails factuality/privacy/quota/accessibility обязательны.

В acceptance нет допустимых выдуманных Event/location/map actions, незаметного применения устаревшего результата или потери принятых реплик. Synthetic-only corpus не доказывает human usability. Пропуск live testing из-за бюджета/политики не quality PASS.

## 14. MCP and storage boundaries

External MCP adapter использует те же domain services, canonical IDs/revisions и bounded results; никаких arbitrary SQL/network/provider tools. Feedback/save/calendar/profile/subscription — отдельные typed commands с existing Auth/consent/idempotency. Никакого нового profile/email control plane, history vector index, event copy или quota ledger. `my-data-hub.operation.get` не считается доступным site-user receipt без отдельного contract.

## 15. Explicit non-goals

Не строить general chatbot, autonomous purchase, бессрочную raw history, гарантированное background listening, unrestricted research, multi-agent planner или hidden provider escalation. Не выдумывать coordinates/route time/popularity. Эти границы не запрещают полезный подробный ответ, совместные острова, щедрую беседу или доступную историю в её retention policy.

## 16. Delivery and executable-test plan

Исполняемые пакеты A–E, точные места интеграции и 32 stable proposed сценария Given/When/Then находятся в [v1 §14–16](voice-search-solution-v1.md#14-пакеты-реализации-и-границы-записи). До реализации это test design, не пройденные автотесты.

ChatGPT выполняет product/schema/state/policy/fixture работу лично. Кодовый агент интегрирует real endpoints, SQL/CAS/recovery, Media capture, actual shared-shell APIs и прогоняет DB/browser/race/mobile tests. Текущие #621 family owners ведут общесистемный UI/SoT/Penpot; source-only voice core может идти параллельно и работать inline без канонизации временного острова.

Расширяется existing scenario registry/CI/Search health, не второй framework. PR lane — provider mocks, real browser capture и изолированная test DB. Protected live lane проверяет настоящий разрешённый ASR/retrieval через общий limiter. L2/L3 не подменяются viewport emulation; self-hosted runners не добавляются.

Qwen CPU baseline переиспользуется для заранее созданного versioned acoustic corpus. Expected meaning размечается независимо; сохраняются hashes, notebook/source/model versions, consent и event corpus. No TTS generation per PR, no реальный голос в public artifact без разрешения. Deep canary, холодный ASR и cache smoke различаются.

## 17. Closure criterion

Документальная проработка завершена, когда независимый исполнитель может реализовать поведение из конкретных контрактов без повторного концептуального аудита. Runtime готов к ограниченному пользовательскому эксперименту только после реализации, актуальных bindings и требуемых тестов: canonical facts/actions, existing Auth/transport/ledger, сохранённый последовательный догон, листаемая история, явная база уточнения, общий island layout, полезные factual answers и наблюдаемый discovery outcome. Markdown/commit/schema check не равны реализации, live acceptance или owner visual approval.
