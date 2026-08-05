# Документация

Этот каталог устроен **feature‑ориентированно**: у каждой фичи есть свой “дом” в `docs/features/`.

## Быстрый роутинг (для агентов)

- Машиночитаемая карта: `docs/routes.yml`
- Статический сайт: `docs/features/static-site-pages/README.md`
  - аналитика и продуктовая статистика: `analytics/README.md`;
  - единый runtime сбора/доставки/консолидации: `analytics/unified-statistics-runtime-architecture.md`;
  - машиночитаемые события и migration inventory: `analytics/statistics-event-catalog.v1.yml`, `analytics/statistics-migration-inventory.v1.yml`;
  - хранение/retention/архив аналитики: `analytics/storage-retention-architecture.md`;
  - карточки, чтение, CTA, Hero Talk и освоение сервиса: `analytics/product-measurement-extension.md`;
  - закрывающий реестр TO-BE из веток: `to-be-documentation-consolidation.md`, `to-be-branch-disposition-ledger.md`;
  - подборки и гастрономия: `podborki.md`, `gastronomy-collection.md`;
  - персонализация: `personalizaion/personalization-to-be.md`, transport ecology, longitudinal E2E и golden personas;
  - погода: `weather-calendar.md`;
  - фокус-группа и обычный онбординг — отдельные контракты.
- Автотесты статического сайта: `docs/operations/static-site-autotest-strategy.md` и `docs/testing/static-site-auth-session-fixture.md`.
- Список фич: `docs/features/README.md`

## Канонические разделы

- Архитектура: `docs/architecture/overview.md`
- Personalization data ownership (Fly/Supabase/YDB/Object Storage): `docs/architecture/personalization-data-ownership.md`
- Сквозная аналитика и продуктовая статистика статического сайта:
  `docs/features/static-site-pages/analytics/README.md`
- Единый runtime статистики, reliable direct/relay delivery и migration inventory:
  `docs/features/static-site-pages/analytics/unified-statistics-runtime-architecture.md`
- Экологичное хранение, TTL и архив статистики:
  `docs/features/static-site-pages/analytics/storage-retention-architecture.md`
- Консолидация TO-BE-документации и resolved branch ledger:
  `docs/features/static-site-pages/to-be-documentation-consolidation.md`,
  `docs/features/static-site-pages/to-be-branch-disposition-ledger.md`
- Hero Talk: `docs/features/hero-talk/README.md`
- Волонтёрские заявки: `docs/features/static-site-pages/volunteer-recruitment/README.md`
- Эксплуатация: `docs/operations/` (как запускать/поддерживать)
- Workflow репозитория и worktree/branch policy: `docs/operations/repository-workflow.md`
- Incident management / closure gate: `docs/operations/incident-management.md`
- Event-quality release monitoring: `docs/operations/event-quality-release-monitoring.md`
- Event-media automatic gate: `docs/features/event-media/README.md`
- Telegram vertical-video quality, SHA cache, Yandex CDN and ranking:
  `docs/features/telegram-monitoring/video-quality.md`
- Event age rating (declared/assessed, Smart Update, CPU BGE, backfill): `docs/features/event-age-rating/README.md`
- Event-age public-surface parity release gate: `docs/features/static-site-pages/event-age-rating.md`
- Event participants/headliners UI and static projection: `docs/features/event-people/README.md`
- Consolidated source + KenigEvents event engagement: `docs/features/post-metrics/consolidated-event-engagement.md`
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
- Resolved branch audit TO-BE-документации:
  `scripts/audit_to_be_documentation.py`,
  `scripts/reconcile_to_be_documentation_audit.py`,
  `docs/features/static-site-pages/to-be-branch-disposition-ledger.manifest.json`,
  `.github/workflows/to-be-documentation-audit.yml`
- Минимальный продуктовый мониторинг статических подборок — health,
  living semantic examples и stability baseline:
  `docs/testing/static-collections-product-quality-autotests.md`
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
- Unusual-event grounded semantic contract: `docs/llm/unusual-event-detection.md`
- Справочники: `docs/reference/` (локации, праздники, шаблоны)
- Редакционные review-наборы и source receipts: `docs/review-data/` (не production truth)
- Пайплайны/парсеры (каноника): `docs/features/source-parsing/` (в `docs/pipelines/` остаются redirect‑stub’ы для старых ссылок)
- Фичи: `docs/features/`
- Interest clubs implementation RC, identity ADR and gated release plan: `docs/features/interest-clubs/README.md`
- Static personal announcements release map: `docs/features/static-personal-announcements/README.md`
- Static event-page production plan, 10-day Telegraph cutover and tests: `docs/features/static-site-pages/release-plan.md`, `docs/features/static-site-pages/test-scenarios.md`
- Official static-site presentation gate and central UI-debt register: `docs/features/static-site-pages/presentation-release-checklist.md`
- Unified mobile header/bottom-nav/toast shell: `docs/features/static-site-pages/mobile-shell.md`
- Adaptive desktop/mobile navigation target: `docs/features/static-site-pages/responsive-navigation.md`
- Selective desktop/mobile breadcrumb product contract: `docs/features/static-site-pages/breadcrumbs.md`
- Free + children search/admission/audience contract: `docs/features/unsigned-personalization/audience-admission-discovery.md`
- Unusual-events static feed, shared BGE/scoring, concept notifications and
  rollout gates: `docs/features/unusual-events/README.md`
- Final SEO/GEO immutable release gate: `docs/features/static-site-pages/seo-geo-release-optimization.md`
- Exhaustive medallion visual QA: `docs/features/static-site-pages/medallion-visual-qa.md`
- Autopresenter status/routing: `docs/features/static-site-pages/auto-present/README.md`

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
5. Принятое TO-BE-решение должно оперативно доехать до канонического документа в `main`, даже если runtime ещё не реализован. Документы в feature-ветке, PR body, комментарии и preview остаются WIP/evidence и не могут быть единственным источником требования.
6. Общие определения аналитики, пользователей, сессий, consent, хранения, метрик и resource budgets не дублируются по фичам: feature-документы ссылаются на `docs/features/static-site-pages/analytics/README.md` и добавляют только feature-specific контракт.
7. Нельзя выбирать канонику только по дате commit. Сверка owner decision, статуса документа, current runtime и конфликтов фиксируется в `docs/features/static-site-pages/to-be-documentation-consolidation.md` и branch/path ledger.
8. Weekly `TO-BE documentation branch audit` — fail-closed guard: новая requirement-like branch/path без reviewed disposition блокирует resolved audit.
