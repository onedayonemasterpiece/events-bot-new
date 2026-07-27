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
- Event-media automatic gate: `docs/features/event-media/README.md`
- Event age rating (declared/assessed, Smart Update, CPU BGE, backfill): `docs/features/event-age-rating/README.md`
- Read-only duplicate-image audit: `docs/operations/event-image-duplicate-audit.md`
- Email delivery/deliverability: `docs/operations/email-delivery.md`
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
- Interest clubs implementation RC, identity ADR and gated release plan: `docs/features/interest-clubs/README.md`
- Static personal announcements release map: `docs/features/static-personal-announcements/README.md`
- Static event-page production plan, 10-day Telegraph cutover and tests: `docs/features/static-site-pages/release-plan.md`, `docs/features/static-site-pages/test-scenarios.md`
- Official static-site presentation gate and central UI-debt register: `docs/features/static-site-pages/presentation-release-checklist.md`
- 30.07.2026 static-site focus-group design, implementation gap audit and
  handoff prompt: `docs/backlog/features/static-site-focus-group/README.md`
- Unified mobile header/bottom-nav/toast shell: `docs/features/static-site-pages/mobile-shell.md`
- Selective desktop/mobile breadcrumb product contract: `docs/features/static-site-pages/breadcrumbs.md`
- Free + children search/admission/audience contract: `docs/features/unsigned-personalization/audience-admission-discovery.md`

- Static-site service share: `docs/features/static-site-pages/service-sharing.md`
- Static-site Easter eggs product discovery, analytics and external-research brief: `docs/features/static-site-easter-eggs/README.md`
- Service-share preview runbook: `docs/operations/service-sharing-preview.md`
- 3D previews (`/3di`): `docs/features/preview-3d/README.md`
- Бэклог (не реализовано): `docs/backlog/`
- Отчёты и планы: `docs/reports/` (incident index: `docs/reports/incidents/README.md`, template: `docs/reports/incidents/TEMPLATE.md`)
- Инструменты: `docs/tools/`

## Правила добавления/обновления доков

1. Новая фича → добавь `docs/features/<feature>/README.md` и внеси её в `docs/routes.yml`.
2. Не создавай новый документ, если можно расширить существующий канонический.
3. Если нужно сохранить старый путь (ссылки/история) — оставь короткий redirect‑stub без копирования текста.
4. Новый production incident → добавь канонический record в `docs/reports/incidents/` по `TEMPLATE.md`, внеси его в incident index и убедись, что он пригоден для автоматического regression-check по ID.
