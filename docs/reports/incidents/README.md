# Инциденты

Канонический индекс production incidents и post-incident разборов. Эти записи должны использоваться как обязательный regression-check перед любыми новыми изменениями в затронутых prod-поверхностях.

## Автоматический запуск incident workflow

- Достаточно указать конкретный incident ID (`INC-*`), чтобы агент автоматически:
  - открыл `docs/operations/incident-management.md`;
  - открыл этот индекс;
  - открыл канонический incident record по ID;
  - использовал его как regression contract до closure/deploy.
- Если изменения затрагивают surface из incident record, агент обязан поднять этот record как regression-check даже без явного указания пользователя.
- Если канонического record ещё нет, его нужно создать из `TEMPLATE.md` до завершения задачи.

## Канонический шаблон

- `TEMPLATE.md` — шаблон для новых incident records.

## Активные regression contracts

- `INC-2026-05-05-kitoboya-garage-date.md`
  - Scope: Telegram Monitoring / VK auto-import / Smart Update exhibition date grounding, teaser handling, `course_promo` skip guard, long-running inferred-range correction, and production cleanup for the `Куплю гараж. Калининград` duplicate/date regression.
  - Must not regress: exhibition/fair teasers without an exact day/range/end date must not materialize as first-of-month or message-date event cards; later exact announcements with `кураторские экскурсии` must not be skipped as course promos; and a later source-grounded opening date must be able to correct an inferred legacy long-run exhibition row through a real import + Smart Update replay.
- `INC-2026-05-05-event-quality-regression.md`
  - Scope: Telegram Monitoring / VK auto-import / Smart Update free/location/duplicate invariants, source/default venue fallback, same-ticket/same-slot matching, rental/non-event guards, production event inventory cleanup.
  - Must not regress: zero or missing ticket price must not imply `is_free=true`; ticket giveaways or included-in-entry-ticket wording must not mark the event free; prose/unsupported locations must not be replaced by unrelated `default_location`; same real event must not survive as multiple cards when a specific ticket URL/date/place or near-identical same-slot source text proves identity.
- `INC-2026-05-05-cherryflash-disk-full.md`
  - Scope: `guide_excursions/kaggle_service.py`, `/data/guide_monitoring_results`, Fly `/data` volume, SQLite `/data/db.sqlite`, scheduled CherryFlash `popular_review`, production catch-up health checks.
  - Must not regress: Guide monitoring result bundles must not fill the production SQLite volume; `database or disk is full` / `Errno 28` must trigger disk evidence collection and same-day CherryFlash catch-up if the local slot was missed.
- `INC-2026-05-02-pre-daily-event-quality.md`
  - Scope: Telegram Monitoring / VK auto-import event-local venue grounding, literal field-placeholder cleanup, canonical ticket/program titles, and pre-daily future duplicate/location audit.
  - Must not regress: active today/future event cards must not borrow unrelated source/default venues when the event-local block names a different venue; field-name literals like `location_address` must not become public data; and one real event must not survive as multiple active cards before daily surfaces.
- `INC-2026-05-01-future-event-quality-audit.md`
  - Scope: Telegram Monitoring / VK auto-import future active event rows, prose-like `location_name`, source/default venue recovery, Smart Update duplicate merge guards, Bar Bastion future imports, `/daily`/Telegraph/month/day/video-announcement surfaces.
  - Must not regress: future active event cards must not expose prose/schedule fragments as venues, and one real future event must not survive as multiple active public cards when source posts differ only by repost, ticket URL, title wording, or doors/start time.
- `INC-2026-05-01-daily-location-drift.md`
  - Scope: Telegram Monitoring Gemma venue extraction/review, Telegram candidate grounding, Smart Update weak/default time duplicate matching, VK source default-location repair, and May 1 daily catch-up.
  - Must not regress: arbitrary prose/schedule fragments must not survive as public `location_name`; semantic venue repair must stay LLM-first rather than a growing phrase dictionary; unsupported extracted times must be weak anchors; known VK sources must not default unrelated events to `Калининград Сити Джаз Клуб`.
- `INC-2026-04-30-tg-monitoring-event-quality-regressions.md`
  - Scope: Telegram Monitoring Gemma extraction prompts/schema, schedule-rescue prompt, free/ticket semantics, Smart Update duplicate matching, production cleanup for false-free/work-hours/duplicate event rows from the 2026-04-30 batch.
  - Must not regress: missing ticket price must not mean free; ticket links/status/sale wording must not produce `is_free=true` without explicit free-entry evidence; institution work-hours/holiday-opening notices must not be imported as events by schedule rescue; same real event must not survive as multiple public cards because title/location wording drifted.
- `INC-2026-04-30-tg-monitoring-work-schedule-false-skips.md`
  - Scope: `smart_event_update.py` deterministic non-event guard `work_schedule`, Telegram Monitoring server import, `telegram_scanned_message` skip diagnostics, `/daily` recently-added inventory, production catch-up for `@kenigatom/496` and `@kraftmarket39/199`.
  - Must not regress: concrete future Telegram events at a museum/library venue or an address containing `Музейная` must not be skipped as `skipped_non_event:work_schedule` unless the source is actually a work-hours notice.
- `INC-2026-04-28-vk-smart-update-false-skips.md`
  - Scope: `smart_event_update.py` online-only guard, `festival_queue.py` festival-context routing, `docs/llm/prompts.md`, VK auto-import `persist_skipped` handling, production `vk_inbox`/`ops_run` catch-up evidence.
  - Must not regress: a concrete offline VK event must not be skipped only because it has online registration, and a single masterclass/lecture/show/ride inside a festival/cycle/program context must create/update an event instead of being routed as a whole `festival_post`.
- `INC-2026-04-27-cherryflash-missing-photo-urls.md`
  - Scope: `video_announce/popular_review.py`, `video_announce/scenario.py`, scheduled CherryFlash `popular_review`, prod sqlite event/session rows, Kaggle handoff/story publish evidence.
  - Must not regress: CherryFlash must not pick events whose persisted rows still have empty renderable `photo_urls`; source-post poster rehydration must be persisted before session items and render payload are built; older `cherryflash-session-*` datasets must not remain attached to the shared Kaggle kernel; and a missed same-day CherryFlash slot must be repaired with a compensating rerun.
- `INC-2026-04-27-prod-unresponsive-during-cherryflash-recovery.md`
  - Scope: Fly production runtime, `/healthz`, `/webhook`, `/start`, CherryFlash live/catch-up runner, and long-running production validation.
  - Must not regress: CherryFlash recovery must not continue while the serving bot is unhealthy; runtime file mirror or fallback evidence must be checked; `/healthz` and webhook readiness must be restored before same-day catch-up evidence is accepted.
- `INC-2026-04-27-tg-monitoring-sticky-skipped-post.md`
  - Scope: `source_parsing/telegram/handlers.py`, `telegram_scanned_message` idempotency, Telegram Monitoring import-only/recovery, Smart Update skipped results, `/daily` recently-added inventory, video announcement input pool.
  - Must not regress: a Telegram post with `events_extracted > events_imported` must not become permanently metrics-only just because an earlier server import marked it `skipped` without diagnostics; valid future event payloads must be retryable, while intentional/permanent skips must persist a reason/breakdown.
- `INC-2026-04-26-crumple-story-required-channel-fanout.md`
  - Scope: `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, `fly.toml`, `.env.example`, `video_announce/story_publish.py`, `kaggle/CrumpleVideo/story_publish.py`, embedded `crumple_video.ipynb`, scheduled `video_tomorrow` story status.
  - Must not regress: `me` remains the first blocking render-gate target, but production channel fanout (`@kenigevents`, `@lovekenig`) must be marked required so a missing channel story cannot finish as green `Story publish status: OK`.
- `INC-2026-04-26-prod-slow-during-vk-daily-catchup.md`
  - Scope: Fly production runtime, `/healthz`, `/webhook`, manual production catch-up/smoke commands, VK daily recovery procedure.
  - Must not regress: manual production catch-up must not run heavy full-bot workflows in a way that starves the serving machine; health/webhook degradation during validation must stop validation immediately and restore serving before continuing.
- `INC-2026-04-26-vk-daily-message-limit.md`
  - Scope: `main_part2.py::build_daily_sections_vk`, `send_daily_announcement_vk`, `post_to_vk`, `vk_scheduler`, `fly.toml` / `VK_DAILY_POST_MAX_CHARS`, VK daily publication state.
  - Must not regress: VK daily must split oversized sections before `wall.post`, must preserve event cards when possible, and must not mark the daily VK slot sent unless every chunk returns a VK post URL.
- `INC-2026-04-26-daily-location-fragments.md`
  - Scope: `source_parsing/telegram/handlers.py`, `location_reference.py`, `docs/reference/locations.md`, `docs/reference/location-aliases.md`, `main_part2.py::build_daily_posts`, Telegram Monitoring Gemma 4 location extraction/import, `/daily` publication.
  - Must not regress: prose/schedule/bio fragments must not survive as public `location_name`, known venues must recover address/city from the reference layer, and one `/daily` event card must not be split between two Telegram posts.
- `INC-2026-04-25-prod-bot-unresponsive-after-tg-monitoring-smoke.md`
  - Scope: Fly production runtime, Telegram webhook and `/start`, Telegram Monitoring post-deploy smoke/recovery/import path, scheduler heavy jobs, runtime health, and runtime evidence collection.
  - Must not regress: production smoke/validation must not make the serving bot unresponsive; `/healthz` and `/webhook` failures must trigger incident workflow immediately; runtime log mirror/rotated files must be checked before falling back to Fly logs/Kaggle/DB evidence.
- `INC-2026-04-24-crumple-story-channel-boosts-required.md`
  - Scope: `fly.toml`, CrumpleVideo story target order, `video_announce/story_publish.py`, `kaggle/CrumpleVideo/story_publish.py`, scheduled `video_tomorrow` catch-up, Telegram channel story boosts.
  - Must not regress: production CrumpleVideo must keep a Premium self-account story target (`me`) as the first blocking upload target, channel `BOOSTS_REQUIRED` must remain visible without blocking render delivery, required channel fanout must not finish green when missed, and a missed same-day scheduled slot must be repaired or explicitly blocked by Telegram capability evidence.
- `INC-2026-04-23-cherryflash-pre-handoff-loss.md`
  - Scope: `video_announce/scenario.py`, `scheduling.py`, `video_announce/poller.py`, CherryFlash scheduled `popular_review`, prod sqlite `ops_run`/`videoannounce_session`, Kaggle CherryFlash handoff evidence.
  - Must not regress: scheduled CherryFlash must not mark `ops_run` success before a real non-local Kaggle dataset/kernel handoff is persisted, local-only failed sessions for today's slot must trigger same-day catch-up, and existing remote handoffs must suppress duplicate reruns even if local status is misleading.
- `INC-2026-04-23-guide-digest-extraction-loss.md`
  - Scope: `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`, guide Gemma 4 screen/extract/enrich prompts, multi-date occurrence extraction, guide digest eligibility/catch-up.
  - Must not regress: multi-date posts must preserve each available dated excursion as digest-ready, sold-out/no-date/non-excursion controls must not become subscriber-visible digest cards, and a missed daily guide window must be repaired with production-equivalent monitor/catch-up evidence.
- `INC-2026-04-22-cherryflash-service-notifications-routed-to-channel.md`
  - Scope: `video_announce/poller.py`, `video_announce/scenario.py`, CherryFlash/admin notify routing, Telegram publish-vs-service destination split.
  - Must not regress: restart/service diagnostics must never leak into `test`/`main` publish channels by fallback; they must stay in operator/superadmin DM unless an explicit notify target says otherwise.
- `INC-2026-04-22-cherryflash-false-failed-after-successful-story-publish.md`
  - Scope: `video_announce/poller.py`, `video_announce/scenario.py`, CherryFlash scheduled recovery/handoff state, prod sqlite `videoannounce_session`, Kaggle CherryFlash completion evidence.
  - Must not regress: a fresh CherryFlash run must not remain locally `FAILED` after the same dataset already reached successful Kaggle/story completion, while truly stale `local:*` sessions must still fail closed instead of hanging forever.
- `INC-2026-04-21-guide-gemma4-partial-monitoring.md`
  - Scope: `kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py`, `google_ai/client.py`, `guide_excursions/service.py`, scheduled guide monitoring and auto-publish, `/guide_report` observability.
  - Must not regress: Gemma 4 guide schemas must stay provider-compatible, individual post-level LLM/provider failures must remain visible with source/post IDs, and a run-level `partial` marker must not suppress digest publication when fresh eligible guide material was successfully imported.
- `INC-2026-04-20-video-tomorrow-stuck-rendering.md`
  - Scope: scheduled `/v tomorrow`, `video_announce/scenario.py`, `video_announce/poller.py`, `scheduling.py`, runtime supervision, prod sqlite state.
  - Must not regress: scheduled `video_tomorrow` must not crash while leaving the live session orphaned in `RENDERING`, and restart recovery must never poll Kaggle against repo-local `local:*` refs.
- `INC-2026-04-20-club-znakomstv-duplicate-event-cards.md`
  - Scope: `smart_event_update.py`, `vk_intake.py`, cross-source repost merge guards, event-page rebuild path, prod event rows for the same day/venue cluster.
  - Must not regress: one real event must not survive as multiple active cards when reposts vary only by ticket URL/button wording or when one extractor takes `doors` time and another takes `start` time from the same source text.
- `INC-2026-04-19-cherryflash-story-media-invalid.md`
  - Scope: `kaggle/CrumpleVideo/story_publish.py`, `kaggle/CrumpleVideo/crumple_video.ipynb`, CherryFlash story bundle, Telegram `SendStoryRequest` media profile.
  - Must not regress: CherryFlash story publish must not pass preflight but then fail with opaque `MEDIA_FILE_INVALID`; the exact uploaded story file must be the one-pass final `720x1280 H.265/AAC` CherryFlash render, and `story_publish_report.json` must contain media diagnostics for that uploaded file without a default helper re-transcode.
- `INC-2026-04-16-prod-disk-pressure-runtime-logs.md`
  - Scope: `fly.toml`, `runtime_logging.py`, Fly prod volume hygiene, `/data` artifact retention.
  - Must not regress: the production bot must not become unavailable because `/data` filled up with runtime logs/backups and startup logging hit `Errno 28`.
- `INC-2026-04-16-cherryflash-kaggle-save-kernel-drift.md`
  - Scope: `video_announce/kaggle_client.py`, CherryFlash Kaggle launch path, `kaggle/CherryFlash/`, Kaggle `SaveKernel` response handling.
  - Must not regress: CherryFlash must not log a successful deploy when Kaggle `SaveKernel` returned an error, and fresh `cherryflash-session-*` datasets must be retried as bind-lag instead of being silently accepted as stale launch state.
- `INC-2026-04-15-gate-location-and-linked-facts-drift.md`
  - Scope: `source_parsing/telegram/handlers.py`, `smart_event_update.py`, `docs/reference/locations.md`, Telegraph event rebuild path.
  - Must not regress: gate-family venues (`Закхаймские` / `Фридландские` / `Железнодорожные`) не должны схлопываться по слову `ворота`, а linked-source sensitive facts не должны попадать в canonical fact log без подтверждения source text / OCR.
- `INC-2026-04-14-daily-delay-vk-auto-queue-lock-storm.md`
  - Scope: `ops_run.py`, `vk_review.py`, `vk_auto_queue.py`, `main.py::_vk_api`, Fly prod recovery, `/daily`, `/start`.
  - Must not regress: transient SQLite locks не должны системно останавливать scheduler recovery, проблемный VK post не должен бесконечно всплывать после rate-limit, а `/daily` shortlink failures не должны растягивать ежедневный анонс на повторные bad-token попытки.
- `INC-2026-04-10-crumple-story-prod-drift.md`
  - Scope: `/v`, `video_announce/`, `kaggle/CrumpleVideo/`, `fly.toml`, story-related env и release drift.
  - Must not regress: story publish не должен silently деградировать в mp4-only режим.
- `INC-2026-04-10-crumple-audio-source-drift.md`
  - Scope: `/v`, `video_announce/scenario.py`, Kaggle dataset assembly, audio assets и final render contract.
  - Must not regress: финальный production asset должен использовать только `The_xx_-_Intro.mp3`.
- `INC-2026-04-10-tg-monitoring-festival-bool.md`
  - Scope: `tg_monitoring`, `source_parsing/telegram/`, `smart_event_update.py`, Kaggle payload normalization.
  - Must not regress: malformed optional payload fields не должны переводить импорт в `partial` из-за типового `.strip()`/diagnostic crash.

## Правила ведения incident records

1. Один customer-visible production event — один канонический incident record.
2. Если похожий сбой повторился в другой день или в другой волне, создавай новый `INC-*` record и ссылайся на предыдущий в `Related incidents`, а не переписывай историю поверх старого.
3. Каждый incident record должен содержать automation contract:
   - affected surfaces;
   - mandatory checks before closure/deploy;
   - required evidence;
   - follow-up actions.
4. Инцидент не считается дисциплинированно закрытым, пока fix не в проде, не достижим из `origin/main`, не покрыт regression evidence и не заведены follow-up actions.
5. Для source-import / Smart Update quality incidents regression evidence обязано включать replay сырых offending source artifacts через production import path + Smart Update на prod snapshot/shadow DB. Prompt diff, unit tests или ручной SQL-аудит без такого replay не являются достаточным closure.
