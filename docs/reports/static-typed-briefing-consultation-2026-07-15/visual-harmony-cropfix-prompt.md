# Narrow final acceptance — named small-media crop fix

Ты — внешний арт-директор, проводящий узкий финальный gate. Предыдущий post-fix review дал `PASS WITH CONDITIONS`: единственный publication blocker — named-сцена на 1366×768, потому что широкий контейнер 314×266 жестко срезал имя артиста сверху и подпись афиши снизу.

Исправление: small media теперь сохраняет исходную вертикальную пропорцию 4:5 вместо принудительного широкого crop. На 1366×768 постер 213.25×266.56; на 1440/1920 — 257.61×322.02. Это намеренно не крупная картинка, как и просил пользователь. Она по-прежнему плоская, без рамки/тени, выровнена по правому краю общей hero-grid.

Открой:
- `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/postfix-cropfix/named-1366x768.png`
- `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/postfix-cropfix/named-1440x900.png`
- `/home/dev/projects/events-bot-new-typed-briefing-artist-unusual-20260715-integration/artifacts/codex/static-typed-briefing-artist-unusual/postfix-cropfix/named-1920x900.png`
- и `metrics.json` в этой же папке.

Ответь кратко и строго:
1. 1366 blocker: `CLOSED` или `OPEN`, одна конкретная причина.
2. Отдельно PASS/FAIL на 1366, 1440, 1920.
3. Является ли 213×267 на 1366 визуально намеренным «небольшим editorial poster», или он стал слишком мал/оторван? Учти декоративную O и баланс с H1.
4. Есть ли новый P0/P1 regression? Если нет, прямо `none`.
5. Финальный verdict: `PUBLISH PASS` или `FAIL`.
6. Максимум два неблокирующих наблюдения для ручной проверки анимации.

Не оценивай ленту, не предлагай redesign, новые изображения или возвращение к wide crop. Пиши по-русски.
