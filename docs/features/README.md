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
- `docs/features/linked-events/README.md` — связанные события (серии “то же событие в другие даты” + UI «Другие даты»)
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

## Черновики требований

- `docs/features/subscriber-acquisition/requirements.md` — требования к social acquisition workflow для роста подписчиков через Telegram/VK-рекомендации.

## Research / rollout

- `docs/features/static-site-pages/README.md` — проектируемые static-first event pages на `kenigevents.ru` с SEO/GEO, sitemap, JSON-LD и временным Telegraph dual-run
- `docs/features/event-comment-feedback/README.md` — **Сигналы обсуждения**: docs-first дочерняя фича static-site pages для агрегированной смысловой сводки открытых комментариев к источникам события
- `docs/features/unsigned-personalization/README.md` — anonymous personalization для static event pages: MVP-0 `event_detail_related`, localStorage + Supabase telemetry/profile без auth
- `docs/features/info-partners/README.md` — страница информационных партнёров на static-first сайте: логотипы организаций, nofollow-ссылки и связь со статусом/промо-кампаниями

## Как добавлять новую фичу

1. Создай `docs/features/<feature>/README.md`.
2. Если у фичи есть “поток задач” — заведи `docs/features/<feature>/tasks/README.md` и храни в `tasks/` ссылки на backlog items/PRs/отчёты (без дублирования текста).
3. Если у фичи есть диаграммы/скриншоты — храни в `docs/features/<feature>/assets/`.
4. Добавь запись в `docs/routes.yml`.
5. Если есть протокол/спека, но фича ещё не реализована — клади её в `docs/backlog/` (а не в `docs/features/`).
