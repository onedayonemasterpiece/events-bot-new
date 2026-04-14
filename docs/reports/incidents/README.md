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
