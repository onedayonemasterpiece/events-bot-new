# First-party карта действий: исследования сигналов

Два пользовательских исследовательских документа о минимальной first-party
телеметрии для карты действий сайта событий. Материалы сохранены как evidence и
**не являются автоматически принятым implementation contract**.

- [Архитектура и семантика сигналов взаимодействия](first-party-action-map-signal-architecture.md) — развёрнутое исследование классов сигналов, privacy/стоимости и семантической привязки к версии, странице и компоненту.
- [Минимально достаточные сигналы](minimum-sufficient-first-party-action-signals.md) — краткий вывод и политика `ALWAYS` / `CAMPAIGN_ONLY` / `DO_NOT_COLLECT`.

Ключевая позиция материалов: измерять не пиксельную heatmap или session replay,
а версионированную наблюдаемость компонента: exposure → attempt → expected/
observed effect → retry → performance/layout context.

Статус: research input; перед реализацией нужны отдельные product, privacy,
retention и schema decisions.
