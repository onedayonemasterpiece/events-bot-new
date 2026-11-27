# INC-2026-11-27 VK flood между ожидающими запросами

Status: open
Severity: sev2
Service: shared VK GET/POST transport, initial event publication and comments
Opened: 2026-11-27
Owners: KenigEvents root
Related incidents: INC-2026-09-13-guide-vk-monitoring-user-token-flood, INC-2026-09-24-vk-afisha-publication-recurrence
Related docs: ../../features/vk-publishing/README.md

## Summary and impact

Пост события 11502 вышел с готовой каруселью в 06:10 UTC. Два комментария
отправлены автоматически по одному в 07:06:48/52 вместо штатных 3–20 минут.
Настоящие ответы VK code9 получены `utils.getShortLink` в 06:00:05.657/.967;
они открыли user credential cooldown до 07:00:05.968. Поздние `wall.get`
code9 были локальными отказами circuit, а не новыми ответами провайдера.
Причина первого внешнего flood и точный shortener caller не установлены.

## Root cause

Механическая гонка, без изменения семантики событий: оба общих транспорта
проверяли cooldown до ожидания `VK_SEMAPHORE`. Уже ожидающий запрос поэтому
мог обратиться к тому же credential после code9 предыдущего запроса. В POST
code9 также отмечался после освобождения semaphore. Два реальных отказа с
разницей 310 мс согласуются с гонкой, но сами по себе не доказывают её участие
в этом эпизоде. Офлайн concurrency control воспроизводит дефект отдельно.
Production использует `VK_CONCURRENCY=1` по умолчанию.

## Corrective actions

Перенести существующую проверку внутрь существующего semaphore после throttle,
на каждую HTTP-попытку; отмечать code9 до release. Другие credential сохраняют
прежний fallback. Не добавляются state, locks, retries или обход квот.
Не менять shortener, cooldown, jitter, старые посты и frozen комментарии.
Исправление не гарантирует задержку 3–20 минут при внешнем flood и не устраняет
первый отказ VK. При concurrency >1 уже отправленные запросы не отменяются.

## Automation contract

### Treat as regression guard when

Меняются `vk_api`, `_vk_api`, общий semaphore/throttle или actor flood circuit.

### Affected surfaces

`main.py`, GET/POST admission; existing service/publication credential isolation.

### Mandatory checks before closure or deploy

- Два concurrent same-user запроса с первым code9 дают один mock HTTP,
  один circuit mark, прежний expiry; следующий комментарий отклоняется локально.
- GET и POST; другой credential работает, прежний fallback и private logging
  проходят существующие тесты.
- Exact main CI, source review и image preflight; обычный Events-only cutover.
- После запуска штатные импорты/related продвигаются, новый пост и комментарии
  проверены по durable receipts. Не индуцировать flood и не replay старые jobs.

### Required evidence

Retained `/home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check/`:
`flood-allowlist-20261127.json`, `flood-before-tests.log` (2 FAIL),
`flood-after-tests.log` (2 PASS), `flood-source-verdict-20261127.md`,
`vk16216-live-acceptance-20261127.md`. Release SHA/image и live проверку
добавить после фактического выполнения; сейчас выпуск не запускался.

## Follow-up and closure

Root: выполнить регрессию, ревью и штатный выпуск; затем проверить обычную
новую публикацию и последовательные единичные комментарии. Первоначальный
provider flood не приписывать гонке без дополнительной атрибуции. Инцидент открыт.
