# Floating Islands — владелец сквозной системы

Дата: 2026-09-05. Pattern: `pattern.detached-chrome-control-islands`.

**Документальное проектирование выполнено; реализация новой сквозной версии, native P и production adoption не заявлены.** Это короткий cross-repository routing, не копия требований.

Владелец системы — существующий [lovekgd-design-system#47](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/47). Зафиксированная версия пакета: `f56b29b25dcd043d07088eb5ef120693418113ad`.

| Документ | Содержание |
|---|---|
| [Спецификация системы](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/f56b29b25dcd043d07088eb5ef120693418113ad/docs/research/floating-control-islands-2026-08/system-design-v1.md) | Правила FI-01–FI-20, shared ownership, роли/состояния/геометрия/scroll/keyboard/layers, Search adapter и A=S=P. |
| [Матрица потребителей](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/f56b29b25dcd043d07088eb5ef120693418113ad/docs/research/floating-control-islands-2026-08/consumer-matrix-v1.md) | Все 17 registry archetypes и C1–C6; Search и Free не подменяют весь сайт. |
| [Первый implementation пакет FI-P1](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/f56b29b25dcd043d07088eb5ef120693418113ad/docs/research/floating-control-islands-2026-08/implementation-package-1.md) | Shared geometry + section context на Popular, Today/Free/event-detail compatibility, 32 предложенных тест-сценария, конкретные source boundaries и критерии поставки. |
| [Прочитанные источники и evidence](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/f56b29b25dcd043d07088eb5ef120693418113ad/docs/research/floating-control-islands-2026-08/sources-and-decisions-v1.md) | Source/public pins, browser observations, unresolved P и ограничения выполненной проверки. |

Для актуализации читать current PR #47 и его owning README; не копировать текст системных правил в отдельный prompt или voice spec. Полный site-wide AS-IS/P не блокирует документальный дизайн. Baseline затронутого consumer, owner review и active conformance gates для последующего изменения не отменены.

[Search product](../smart-vector-search/agent-assisted-event-discovery.md) и [voice technical v1](../smart-vector-search/voice-search-solution-v1.md) остаются владельцами диалога, capture, API, Auth, privacy, ledger и allowance. Shared adapter сопоставлен с FI-17. Предложенный FI-16 capture/overlay handshake нужно согласованно реализовать, а не считать существующей серверной возможностью.

Runtime integration остаётся в [events-bot-new#621](https://github.com/onedayonemasterpiece/events-bot-new/issues/621) и executable trunk `agent/static-site-single-kaggle-contract`. Не запускать отдельный оркестратор/конвейер, не менять production/shared foundations/STATUS из этой documentary lane. Penpot read в окне проектирования был заблокирован safety check; native writes и A=S=P PASS отсутствуют.
