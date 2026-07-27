# External product gate — 2026-07-27

> **Статус:** blocked, не является completed external consultant review.

Для текущего решения о нейминге, публичном реестре и первой коллекции был
подготовлен отдельный критический brief. Запрошенная модель:
`agy --model "Gemini 3.1 Pro (High)"`, то есть approved Gemini Pro class
`gemini-3.1-pro-preview`.

Fresh запуск 2026-07-27 завершился до обращения к модели:

```text
Eligibility check failed: Your current account is not eligible for Antigravity,
because it is not currently available in your location.
```

Exit code: `1`; response body пуст. Разрешённый fallback `a-opus` остановился на
том же Antigravity eligibility gate. Эти попытки **не** представлены как Gemini
или Opus review и не заменены Flash/Lite-моделью.

Redacted receipt хранится только в ignored artifact lane:
`artifacts/codex/artifact-registry-20260727/`.

Текущая проработка использует как вход ранее завершённые Gemini Pro консультации
от 2026-07-21:

- [первая критическая консультация](gemini-consultation-2026-07-21.md);
- [KPI/state консультация](gemini-kpi-state-consultation-2026-07-21.md).

Новые параметры `8/5`, exact membership и public registry требуют fresh Pro gate
после восстановления eligibility; до него они остаются `draft` и требуют owner
acceptance.
