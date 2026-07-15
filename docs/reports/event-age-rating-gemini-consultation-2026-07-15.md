# Gemini Pro consultation: event age rating

Дата: 2026-07-15. Консультант вызван через локальный agy wrapper `a-gemini`;
provider route сообщил Gemini 3.1 Pro (High). Полные prompt/response и stderr
сохранены в ignored artifact
`artifacts/codex/event-age-rating-audit-2026-07-15/`.

## Входные ограничения

- Kaggle только CPU;
- BGE pipeline уже проверен в Region Talk;
- launch/status/heartbeat contracts уже проверены Telegram Monitoring и
  CherryFlash;
- LLM-запросы строго лимитированы, запросов на событие должно быть минимум;
- regex не решает семантику; конфликт не сворачивается в максимальный возраст;
- assessed отделён от declared и не публикуется по умолчанию.

## Принято

- hybrid pipeline: source-native structured fact → semantic context check →
  local vector/model shadow → bounded fallback;
- versioned hashes для input, rubric, model и prototype bank;
- abstention как штатный результат;
- batch caps, runtime guard, partial checkpoint и heartbeat;
- отдельные метрики over-permissive error и coverage.

## Уточнено/отклонено интегратором

Первый ответ предлагал broad regex semantics, nearest-prototype thresholds и
выбор максимума при конфликте. Это противоречит контракту задачи. В follow-up
консультант принял ограничения: regex только candidate retrieval, unresolved
conflict → `NULL`, CPU-only reuse проверенных project paths, one-call upper
bound. Даже после уточнения proposal оставался слишком уверенным в direct
prototype labels, поэтому в реализации nearest BGE prototype — только retrieval
evidence. Итоговый assessed candidate требует approved calibrated dual-head
artifact; иначе worker abstains.

## Итоговое решение

Дополнительные online LLM calls/event = 0: age JSON piggybacked в существующий
Smart Update call. Source-native values вообще не используют LLM. CPU Kaggle BGE
запускается отдельным bounded batch и не пишет declared value. Публичный default
— `declared_only`.

