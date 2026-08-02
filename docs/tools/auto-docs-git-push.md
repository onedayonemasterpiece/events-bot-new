# Автопубликация документации в GitHub

`events-bot-docs-autopush.service` отслеживает текстовые файлы в `docs/` и после сохранения автоматически создаёт commit и отправляет его в `origin/main`.

- Интервал проверки — 10 секунд.
- Новые и изменённые файлы с расширениями `.md`, `.mdx`, `.rst`, `.txt`, `.csv`, `.json`, `.yml` и `.yaml` отправляются автоматически.
- Медиа, архивы и `docs/reference/visitors-30072026.md` не отправляются.
- При первом запуске сервис сохраняет текущий локальный снимок как baseline и не публикует старые незакоммиченные документы. Последующие изменения публикуются автоматически.

Статус и журнал:

```bash
systemctl --user status events-bot-docs-autopush.service
journalctl --user -u events-bot-docs-autopush.service -f
```
