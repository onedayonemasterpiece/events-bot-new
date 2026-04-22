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
