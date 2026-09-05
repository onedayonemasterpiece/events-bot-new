# Floating Islands — владелец сквозной системы

Дата: 2026-09-05, уточнение верхней композиции v1.1. Pattern: `pattern.detached-chrome-control-islands`.

**Документальное проектирование; новая runtime версия, native P и production adoption не заявлены.** Это короткий routing, не копия требований.

Владелец — [существующий lovekgd-design-system#47](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/47). Текущий пакет закреплён на **`eb3309591be368d729ea52c90b6ef99d1acbad6b`**: [единая точка входа README](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/README.md).

| Документ | Содержание |
|---|---|
| [Top-row v1.1](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/top-row-composition-v1.1.md) | Одна верхняя строка, independent views и icon+label→label→icon, whitespace, meaningful menu/medallion, stable target, readable flow fallback; исследование и конкретная интеграция. |
| [Core specification](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/system-design-v1.md) | FI-01–FI-20 с адресными поправками FI-02/05/09/11: semantic headings в документе, locator общей строки без дублирующего control tree. |
| [Release bindings](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/release-bindings-v1.md) | RB-01–03: receipts, occlusion→served/exposure, frozen prefix/global hides/undo, optional analytics OFF и MeasurementQuestions. Upstream authority не копируется в shell. |
| [Матрица](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/consumer-matrix-v1.md) | 17 actual registry IDs/C1–C6, one-row applicability; current release manifest проверяется перед migration. |
| [FI-P1](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/implementation-package-1.md) | Первый видимый результат — one-row Popular, не дополнительный sticky этаж; shared regression, конкретные source owners, тесты/приёмка/откат. |
| [Источники v1](https://github.com/onedayonemasterpiece/lovekgd-design-system/blob/eb3309591be368d729ea52c90b6ef99d1acbad6b/docs/research/floating-control-islands-2026-08/sources-and-decisions-v1.md) | Предыдущий evidence с его границами; current research/tool/model evidence отдельно в top-row v1.1. |

**Адресная совместимость с Search:** прежнее «section heading становится sticky» сохраняет смысл видимого текущего контекста, но migrated top-row вариант не переносит/не клонирует H2 в header. H2 остаётся в section, общей строке принадлежит non-heading locator либо отдельное действие выбора раздела. Viewed section/refinement base/pending draft по-прежнему независимы. Это явное presentation amendment, не новый Search API или второй controller.

14 offline reference-model tests выполнены на искусственных размерах, не на Astro. 32 core+5 binding runtime scenarios остаются planned; 12 top-row categories расширяют affected cases. Новый browser/Penpot read в уточнении вернул FORBIDDEN; нет новых native objects/screenshots/A=S=P PASS. Точные assets/views/geometry материализуются существующим exporter, не похожими иконками.

[Search product](../smart-vector-search/agent-assisted-event-discovery.md), [voice v1](../smart-vector-search/voice-search-solution-v1.md) и [release integration](../../static-personal-announcements/release-integration.md) остаются upstream owners. FI-16 capture/overlay handshake и FI-17 adapter требуют реализации и проверки. Последующие amendments global-product-decisions сохраняют собственную authority; этот routing не переписывает их.

#621/executable trunk `agent/static-site-single-kaggle-contract` владеют интеграцией. Documentary design разрешён сейчас; honest target baseline, owner review, native conformance и release gates сохранены. Production/shared foundations/STATUS из этой lane не изменяются; нового оркестратора/конвейера нет.
