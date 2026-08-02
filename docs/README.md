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
- Telegram vertical-video quality, SHA cache, Yandex CDN and ranking:
  `docs/features/telegram-monitoring/video-quality.md`
- Event age rating (declared/assessed, Smart Update, CPU BGE, backfill): `docs/features/event-age-rating/README.md`
- Event participants/headliners UI and static projection: `docs/features/event-people/README.md`
- Read-only duplicate-image audit: `docs/operations/event-image-duplicate-audit.md`
- Email delivery/deliverability: `docs/operations/email-delivery.md`
- External focus-group mailbox OTP E2E: `docs/testing/external-focus-email-otp.md`
- Static-site deterministic direct/relay fault profiles:
  `docs/testing/transport-fault-profiles.v1.yml`
- **Стратегия автотестирования статического сайта и данных, Android/iOS gates,
  запуск из ChatGPT, scenario registry и Codex handoff:**
  `docs/operations/static-site-autotest-strategy.md`,
  `docs/features/static-site-pages/release-autotest-gates.md`,
  `docs/operations/static-site-qa-chatgpt-control-plane.md`,
  `docs/testing/static-site-autotest-scenarios.v1.yml`,
  `docs/testing/static-site-autotest-codex-prompt.md`
- Kaggle static-site builder / publisher protocol: `docs/operations/kaggle-static-site-builder.md`
- Default-off two-bucket/ALB atomic root publisher and setup/rollback runbook:
  `docs/operations/static-site-atomic-root.md`
- Расписание / cron / scheduler routing: `docs/operations/cron.md`
  - дефолты APScheduler: `scheduling.py`
  - production overrides: `fly.toml` (`[env]`)
  - локальный/env шаблон: `.env.example`
- Region Talk / «О Калининграде говорят»: `docs/features/region-talk-channel/README.md`
- Telegram link inspection через Telethon: `docs/operations/telegram-link-inspection.md`
- LLM: `docs/llm/` (промпты, формат запросов, классификатор тем)
- Справочники: `docs/reference/` (локации, праздники, шаблоны)
- Пайплайны/парсеры (каноника): `docs/features/source-parsing/` (в `docs/pipelines/` остаются redirect‑stub’ы для старых ссылок)
- Фичи: `docs/features/`
- Interest clubs implementation RC, identity ADR and gated release plan: `docs/features/interest-clubs/README.md`
- Static personal announcements release map: `docs/features/static-personal-announcements/README.md`
- Static event-page production plan, 10-day Telegraph cutover and tests: `docs/features/static-site-pages/release-plan.md`, `docs/features/static-site-pages/test-scenarios.md`
- Official static-site presentation gate and central UI-debt register: `docs/features/static-site-pages/presentation-release-checklist.md`
- Unified mobile header/bottom-nav/toast shell: `docs/features/static-site-pages/mobile-shell.md`
- Selective desktop/mobile breadcrumb product contract: `docs/features/static-site-pages/breadcrumbs.md`
- Free + children search/admission/audience contract: `docs/features/unsigned-personalization/audience-admission-discovery.md`
- Unusual-events static feed, shared BGE/scoring, concept notifications and
  rollout gates: `docs/features/unusual-events/README.md`

- Static-site service share: `docs/features/static-site-pages/service-sharing.md`
- Static-site collections: original requirements in `docs/features/static-site-pages/podborki.md`,
  extraction/product/venue/egress plan in `docs/features/static-site-pages/podborki-to-be.md`
- Static-site Easter eggs product discovery, analytics and external-research brief: `docs/features/static-site-easter-eggs/README.md`
- Service-share preview runbook: `docs/operations/service-sharing-preview.md`
- 3D previews (`/3di`): `docs/features/preview-3d/README.md`
- Бэклог (не реализовано): `docs/backlog/`
  - Фокус-группа статического сайта: product/page prototype и production
    boundary — `docs/features/static-site-focus-group/README.md`
- Отчёты и планы: `docs/reports/` (incident index: `docs/reports/incidents/README.md`, template: `docs/reports/incidents/TEMPLATE.md`)
- Инструменты: `docs/tools/`

## Правила добавления/обновления доков

1. Новая фича → добавь `docs/features/<feature>/README.md` и внеси её в `docs/routes.yml`.
2. Не создавай новый документ, если можно расширить существующий канонический.
3. Если нужно сохранить старый путь (ссылки/история) — оставь короткий redirect‑stub без копирования текста.
4. Новый production incident → добавь канонический record в `docs/reports/incidents/` по `TEMPLATE.md`, внеси его в incident index и убедись, что он пригоден для автоматического regression-check по ID.
