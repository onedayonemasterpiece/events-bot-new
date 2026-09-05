# Floating Islands — владелец сквозной системы

Дата: 2026-09-05. Pattern: `pattern.detached-chrome-control-islands`.

**Документальное проектирование выполнено; новая runtime версия, native P и production adoption не заявлены.** Это короткий routing, не копия требований.

Владелец — [существующий lovekgd-design-system#47](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/47). Полный проверяемый пакет закреплён на **`4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0`**. Начать с [единой точки входа README](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0/docs/research/floating-control-islands-2026-08/README.md).

| Документ | Содержание |
|---|---|
| [Core specification](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0/docs/research/floating-control-islands-2026-08/system-design-v1.md) | FI-01–FI-20: roles/states/geometry/scroll/keyboard/layers, типизированный Search adapter, A=S=P. |
| [Обязательные release bindings](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0/docs/research/floating-control-islands-2026-08/release-bindings-v1.md) | RB-01–03: receipt states, occlusion→exposure/served-list, visible-prefix freeze/global hides/undo, MeasurementQuestions. Учтено current #587 release дополнение на `c048ebe…`. |
| [Матрица потребителей](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0/docs/research/floating-control-islands-2026-08/consumer-matrix-v1.md) | Все 17 archetypes прочитанного actual registry и C1–C6; current release manifest проверяется перед миграцией. |
| [FI-P1](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0/docs/research/floating-control-islands-2026-08/implementation-package-1.md) | Shared geometry + section context Popular; Today/Free/event-detail compatibility; 32 основных + 5 binding сценариев, пока не реализованных/не выполненных. |
| [Источники и evidence](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/4af505fea7d2ca4351db9c6d9bb8bd241bdc31c0/docs/research/floating-control-islands-2026-08/sources-and-decisions-v1.md) | Source/public pins, ограниченные личные browser observations, Penpot block и отсутствие непроверенных PASS. Late source read — отдельно в release-bindings. |

Для последующих изменений читать current PR #47/owning README; не пересказывать FI/RB правила в новый prompt. Полный site-wide AS-IS/P не блокирует документальный дизайн. Target baseline, visual owner review и active conformance/release gates сохраняются.

[Search product](../smart-vector-search/agent-assisted-event-discovery.md), [voice v1](../smart-vector-search/voice-search-solution-v1.md) и [release integration](../../static-personal-announcements/release-integration.md) остаются upstream владельцами. Shell не создаёт transport/profile/analytics pipeline; FI-16 capture/overlay handshake и FI-17 adapter требуют согласованной реализации, не считаются уже существующими endpoints.

#621 и executable trunk `agent/static-site-single-kaggle-contract` сохраняют runtime integration. Из этой documentary lane не изменять production/shared foundations/STATUS. Penpot read был заблокирован safety check; native writes и A=S=P PASS в этом окне отсутствуют.
