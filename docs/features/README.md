# Фичи

Этот раздел содержит **канонические** описания реализованного поведения по фичам.

## Реализовано

- `docs/features/digests/README.md` — дайджесты (подборки/превью/публикация)
- `docs/features/crumple-video/README.md` — видео-анонсы `/v` (CrumpleVideo)
- `docs/features/cherryflash/README.md` — CherryFlash / Popular Video Afisha (story-first daily popularity-driven video announce)
- `docs/features/kenigsberg-stories/README.md` — генератор исторических Telegram Stories `Мост в Кёнигсберг` (MVP spec)
- `docs/features/source-parsing/README.md` — извлечение/парсинг событий из внешних источников (в т.ч. `/parse`)
- `docs/features/tourist-label/README.md` — туристическая метка (ручная разметка + экспорт)
- `docs/features/llm-gateway/README.md` — управление лимитами Google AI (Gemma/Gemini) и ретраи
- `docs/features/smart-event-update/README.md` — единый Smart Update (match/merge, лог фактов, Telegraph)
- `docs/features/event-media/README.md` — единый автоматический Smart Update gate для event images и approved public projection
- `docs/features/event-age-rating/README.md` — declared/assessed возраст, provenance, Smart Update, CPU BGE и backfill
- `docs/features/linked-events/README.md` — единый contract связанных событий: другие даты, strict similarity, broad discovery и персональная выдача; requirements, branch inventory и screenshots
- `docs/features/vk-auto-queue/README.md` — авторазбор очереди VK постов
- `docs/features/vk-publishing/README.md` — исходящие VK-публикации: событийные посты с медиагруппами и компактный daily
- `docs/features/vk-location-marker/README.md` — безопасная VK location marker-метка для событийных wall-постов
- `docs/features/vk-dynamic-cover/README.md` — динамическая обложка VK-сообщества: генерация wide/mobile assets и `/cover`
- `docs/features/tg-publishing/README.md` — исходящие Telegram-публикации отдельных событий после Smart Update
- `docs/features/tg-premium-emojis-update/README.md` — Telethon-редактор premium/custom emoji для ежедневных Telegram-анонсов
- `docs/features/exhibitions-smart-update/README.md` — поддержка выставок через Smart Update
- `docs/features/festivals/README.md` — фестивали: серии/выпуски, очередь и связь с событиями
- `docs/features/post-metrics/README.md` — статистика постов и популярность (TG/VK): медианы, уровни ⭐/👍, основа для ранжирования
- `docs/features/ticket-sites-queue/README.md` — очередь обогащения событий по ссылкам на ticket‑сайты (pyramida/dom/qtickets)
- `docs/features/admin-action-assistant/README.md` — ассистент админ‑команд: запрос «простыми словами» → команда + подтверждение
- `docs/features/recent-imports/README.md` — отчёт `/recent_imports`: список событий, недавно созданных/обновлённых из Telegram, VK и `/parse`
- `docs/features/promo-campaigns/README.md` — промо-кампании для будущих событий/фестивалей: `/promo`, `/a`, CherryFlash boost и мягкое `✨`-выделение
- `docs/features/telegraph-cache-sanitizer/README.md` — прогрев/проверка Telegram web preview (Instant View) для Telegraph страниц + очередь на пересборку “битых”
- `docs/features/preview-3d/README.md` — генерация 3D-превью (`/3di`) через Kaggle/Blender с выгрузкой в Supabase Storage
- `docs/features/guide-excursions-monitoring/README.md` — мониторинг экскурсионных анонсов гидов, отдельный digest track и публикация в тестовый канал
- `docs/features/telegram-business-stories/README.md` — Telegram Business webhook contract, encrypted connection cache и публикация сторис через Bot API
- `docs/features/tg-monitoring-on-demand/README.md` — fast-path запуск Telegram Monitoring по новым постам в allowlisted Telegram-каналах
- `docs/features/kaggle-status-framework/README.md` — единый callback/heartbeat/status framework для Kaggle runtime/notebooks без скрытой дедупликации публикаций
- `docs/features/region-talk-channel/README.md` — автономный discovery внешних Telegram/VK/web-публикаций о Калининградской области, YDB funnel и доставка подтверждённых кандидатов в операторский чат

## Черновики требований

- `docs/features/subscriber-acquisition/requirements.md` — требования к social acquisition workflow для роста подписчиков через Telegram/VK-рекомендации.

## Research / rollout

- `docs/features/interest-clubs/README.md` — implementation RC раздела клубов по интересам: evidence-first identity, static projection, ADR и отдельный production release gate

- `docs/features/static-personal-announcements/README.md` — umbrella release map для static-site персональных анонсов: F1–F17, owners, stages, global decisions и канонические feature homes
- `docs/features/static-site-pages/README.md` — static-first event pages на `kenigevents.ru`; production promotion пока blocked, текущий план и 10-дневный Telegraph cutover — `release-plan.md`, acceptance inventory — `test-scenarios.md`
- `docs/features/event-people/README.md` — отдельный mobile/desktop блок участников, fail-closed static export и person-like contract
- `docs/features/unsigned-personalization/README.md` — mixed personalization family: local anonymous preview, pgvector related/search canary, remote telemetry/profile pending
- `docs/features/site-user-identity/README.md` — Yandex/verified-email identity, sessions and anonymous→authenticated profile linking; post-release VK message-code extension: `docs/features/site-user-identity/vk-message-link.md`
- `docs/features/event-favorites-calendar/README.md` — durable favorite/calendar state and lifecycle semantics
- `docs/features/personal-email-announcements/README.md` — opt-in recommendation email with exactly three events plus a personal page
- `docs/features/event-email-notifications/README.md` — transactional followed-event confirmation/reminder/cancellation/reschedule email
- `docs/features/event-transport/README.md` — source-backed rail/bus guidance and transport ICS for eligible event pages
- `docs/features/event-comment-feedback/README.md` — aggregated discussion signals from public source comments
- `docs/features/event-issue-reporting/README.md` — admin event-problem report and ArtKodex repair/history workflow
- `docs/features/info-partners/README.md` — страница информационных партнёров на static-first сайте: логотипы организаций, nofollow-ссылки и связь со статусом/промо-кампаниями
- `docs/features/static-site-easter-eggs/README.md` — post-release discovery механики региональных коллекционных пасхалок: promo activity, автоматическая режиссура, обратная связь/partner intake, аналитика и release gates без production implementation
- `docs/features/unusual-events/README.md` — R15 implementation candidate:
  `/neobychnoe/`, 15-family taxonomy, one shared BGE vector contract,
  concept-level unread state and fail-closed Kaggle rollout
- `docs/features/editorial-collections/README.md` — продуктовое исследование редакционных подборок: frozen real-event pilot «Необычное», временная архитектура evergreen hub + dated editions, evidence выбора периода, benchmark-контент, reusable prompts и noindex Astro-lab с шестью сравнимыми композициями

## Как добавлять новую фичу

1. Создай `docs/features/<feature>/README.md`.
2. Если у фичи есть “поток задач” — заведи `docs/features/<feature>/tasks/README.md` и храни в `tasks/` ссылки на backlog items/PRs/отчёты (без дублирования текста).
3. Если у фичи есть диаграммы/скриншоты — храни в `docs/features/<feature>/assets/`.
4. Добавь запись в `docs/routes.yml`.
5. Если есть протокол/спека, но фича ещё не реализована — клади её в `docs/backlog/` (а не в `docs/features/`).
