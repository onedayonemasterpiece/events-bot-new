# Документация

Этот каталог устроен **feature‑ориентированно**: у каждой фичи есть свой “дом” в `docs/features/`.

## Быстрый роутинг (для агентов)

- Машиночитаемая карта: `docs/routes.yml`
- Список фич: `docs/features/README.md`

## Канонические разделы

- Архитектура: `docs/architecture/overview.md`
- Personalization data ownership (Fly/Supabase/YDB/Object Storage): `docs/architecture/personalization-data-ownership.md`
- Эксплуатация: `docs/operations/` (как запускать/поддерживать)
- Workflow репозитория и worktree/branch policy: `docs/operations/repository-workflow.md`
- Incident management / closure gate: `docs/operations/incident-management.md`
- Event-quality release monitoring: `docs/operations/event-quality-release-monitoring.md`
- Email delivery/deliverability: `docs/operations/email-delivery.md`
- Personalization Supabase 500 MB storage/compaction: `docs/operations/personalization-storage-budget.md`
- Kaggle static-site builder / publisher protocol: `docs/operations/kaggle-static-site-builder.md`
- Расписание / cron / scheduler routing: `docs/operations/cron.md`
  - дефолты APScheduler: `scheduling.py`
  - production overrides: `fly.toml` (`[env]`)
  - локальный/env шаблон: `.env.example`
- Telegram link inspection через Telethon: `docs/operations/telegram-link-inspection.md`
- LLM: `docs/llm/` (промпты, формат запросов, классификатор тем)
- Справочники: `docs/reference/` (локации, праздники, шаблоны)
- Пайплайны/парсеры (каноника): `docs/features/source-parsing/` (в `docs/pipelines/` остаются redirect‑stub’ы для старых ссылок)
- Фичи: `docs/features/`
- Static personal announcements release map: `docs/features/static-personal-announcements/README.md`
- 3D previews (`/3di`): `docs/features/preview-3d/README.md`
- Бэклог (не реализовано): `docs/backlog/`
- Отчёты и планы: `docs/reports/` (incident index: `docs/reports/incidents/README.md`, template: `docs/reports/incidents/TEMPLATE.md`)
- Инструменты: `docs/tools/`

## Правила добавления/обновления доков

1. Новая фича → добавь `docs/features/<feature>/README.md` и внеси её в `docs/routes.yml`.
2. Не создавай новый документ, если можно расширить существующий канонический.
3. Если нужно сохранить старый путь (ссылки/история) — оставь короткий redirect‑stub без копирования текста.
4. Новый production incident → добавь канонический record в `docs/reports/incidents/` по `TEMPLATE.md`, внеси его в incident index и убедись, что он пригоден для автоматического regression-check по ID.
