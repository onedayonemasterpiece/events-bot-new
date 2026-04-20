# CrumpleVideo Tasks

## Status

- Каноническая фича: [README.md](/workspaces/events-bot-new/docs/features/crumple-video/README.md).
- Машиночитаемый роутинг: `docs/routes.yml -> features.crumple_video`.
- Быстрые кнопки `/v` уже переведены на ручной preflight: сначала `INPUT/SELECTED`, потом явный запуск рендера.
- Для автоматического `/v` теперь есть отдельный optional scheduler job:
  - канонический env route: `ENABLE_V_TOMORROW_SCHEDULED` / `V_TOMORROW_*`;
  - legacy `ENABLE_V_TEST_TOMORROW_SCHEDULED` / `V_TEST_TOMORROW_*` остаётся только alias-совместимостью;
  - по умолчанию slot production-first, а `V_TOMORROW_TEST_MODE=1` возвращает его в legacy test-render flow.
- Автопубликация stories теперь живёт внутри `Kaggle` notebook:
  - после успешного рендера notebook сам вызывает Telegram stories API через Telethon;
  - auth доставляется через encrypted split-datasets, а target config приходит отдельным `story_publish.json`;
  - перед тяжёлым рендером notebook делает story preflight и падает сразу, только если blocking primary target не может принять story;
  - downstream fanout targets считаются best-effort: их `BOOSTS_REQUIRED`/repost ошибки должны попасть в report, но не отменять render и не останавливать попытку публикации в следующий target;
  - production target-order нужно фиксировать через `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`, а не через скрытый `main`-канал профиля;
  - для story-video preview frame жёстко привязан к первому кадру CrumpleVideo;
  - для быстрых проверок без полного рендера есть `kaggle/execute_crumple_story_smoke.py`.

## Production Decisions

- Лимит нужно задавать не только по числу событий, а по итоговой длительности ролика.
- Текущая сборка notebook даёт примерно `3.17s + 2.5s * N`, где `N` — число афиш без intro/outro.
- При текущих таймингах:
  - `12` афиш дают около `33.2s`;
  - `16` афиш дают около `43.2s`;
  - технический потолок при лимите `55s` сейчас около `20` афиш, но без запаса.
- Рекомендация для продового видения: soft cap `16` афиш и hard cap по итоговой длительности `<= 55s`.
- Если слегка увеличить время показа одной афиши, безопасный диапазон сейчас выглядит как `+2..+4` кадра на афишу при `24 FPS` (примерно `+0.08..0.17s` на сцену). Это сохраняет динамику и оставляет запас при `16` афишах.
- Так как ролик про события на завтра, production draft лучше собирать ближе к вечеру того же дня, а не утром: это даёт более свежие post-metrics и ближе к реальному decision window аудитории.
- Порядок сцен должен учитывать не только editorial relevance, но и подтверждённую популярность постов-источников: самые сильные события лучше ставить ближе к началу ролика.

## Current Gaps

- Telegram-story onboarding for `hold to pause` / seek is implemented, but still needs manual validation on a real published story:
  - current runtime uses the approved 3-step CTA sequence with lightweight `Cygre` styling and a subtle touch cue;
  - rollout keeps total duration and shot composition unchanged, and uses only the folded-paper intervals between scenes;
  - remaining validation: confirm in a live Telegram story that the CTA stays secondary, readable, and non-annoying on repeat views.
- Notebook всё ещё жёстко режет сцены до `12` через `scenes[:12]`, поэтому продовый cap `16` сейчас не поддержан фактическим рендером.
- Если у профиля настроен `main`-канал, готовое видео автоматически уходит не только в test, но и в main. Для ручного продового контроля это риск.
- Тестовый режим сейчас выставляет `allow_empty_ocr=True`, поэтому тестовый прогон не полностью повторяет продовое качество отбора.
- Story-safe `1080x1920` upload path требует отдельного ручного подтверждения после следующего live prod run: внутренние тесты могут подтвердить только подготовку canvas, но не финальный Telegram-side visual result.
- После live preflight `2026-04-07` текущий риск для prod-stories лежит уже не в старом `BOOSTS_REQUIRED`, а в branch/config drift: неправильный deploy может снова тихо выключить story-path даже при живом render.
- Вручную можно посмотреть итоговый список и JSON, но нет отдельного UI-отчёта по схлопнутым дублям: dedupe сейчас сводит похожие события по `title + location_name + city`, агрегируя расписание, и оператор не видит состав группы явно.
- Сигнал популярности постов уже существует как отдельная feature/data layer, но текущий `/v` ещё не использует его в собственном ranking/order pipeline.

## Manual-First Rollout

- Использовать текущий quick-preflight flow как основной продовый режим:
  - открыть `/v`;
  - выбрать быстрый запуск на завтра или профиль;
  - проверить `INPUT`-список кандидатов;
  - развернуть `Все кандидаты`, сверить спорные группы и дубли;
  - подтвердить/снять события через `READY/SKIPPED`;
  - посмотреть `payload.json` перед рендером.
- Пока не готов явный approval flow, не назначать `main`-канал для production-профиля или добавить отдельный publish step до включения такого профиля.
- Добавить в UI отдельный отчёт по dedupe-группам:
  - representative event;
  - список event_id, которые схлопнулись в группу;
  - причина схлопывания;
  - aggregated schedule.

## Scheduled Generation

- Автоматический слот `/v` должен считаться production-first, а не “только тестом”:
  - основной env route: `ENABLE_V_TOMORROW_SCHEDULED=1`;
  - legacy `ENABLE_V_TEST_TOMORROW_SCHEDULED` / `V_TEST_TOMORROW_*` оставить как совместимость, но не как канонический naming.
- Целевая витрина для аудитории: к `21:00 Europe/Kaliningrad` уже увидеть опубликованные stories в обоих target-каналах.
- Канонический story-order для prod:
  - сначала `@kenigevents`;
  - затем `@lovekenig` через `10` минут (`600` секунд).
- Рекомендованное production окно запуска: `16:00 Europe/Kaliningrad`.
- Обоснование:
  - при `VIDEO_KAGGLE_TIMEOUT_MINUTES=225` worst-case completion приходится на `19:45`;
  - второй story target с задержкой `600` секунд тогда укладывается примерно в `19:55`, то есть остаётся буфер до `21:00`;
  - slot не пересекается с `guide_excursions_full` в `20:10`, поэтому Kaggle-side story publish не должен конкурировать за тот же remote auth bundle.
- Временный legacy-флаг:
  - `V_TOMORROW_TEST_MODE=1` возвращает этот же slot в test-render path, если нужно короткое диагностическое окно без production rollout.
- Для prod story fanout порядок не должен зависеть от скрытого `main`-канала профиля:
  - использовать explicit `VIDEO_ANNOUNCE_STORY_TARGETS_JSON`;
  - `main` + `VIDEO_ANNOUNCE_STORY_EXTRA_TARGETS_JSON` считать только fallback-механикой.
- Продовая защита от silent downgrade:
  - `VIDEO_ANNOUNCE_STORY_REQUIRED=1`;
  - `/healthz` должен краснеть, если story publish выключен или явно сломан по env-конфигу.

## Popularity Priority

- Источник сигнала: [Post Metrics & Popularity](/workspaces/events-bot-new/docs/features/post-metrics/README.md).
- Базовое правило для `/v`:
  - popularity не должна быть единственным фильтром на попадание;
  - popularity должна работать как boost в ranking и как tie-breaker для первых позиций.
- Практический порядок приоритета:
  - сначала события с strongest popularity signal (`⭐/👍`, особенно `оба`);
  - затем editorial/manual promo;
  - затем остальные релевантные события.
- Для первого production шага достаточно soft-boost:
  - не ломать ручной выбор оператора;
  - не вытеснять хорошие события только из-за одного шумного источника;
  - использовать popularity прежде всего для порядка первых `3-5` сцен.
- Следующий шаг реализации:
  - собрать per-event popularity aggregate из TG/VK post metrics;
  - сохранить его в selection payload/trace;
  - показывать оператору, почему событие поднято в начало ролика.

## Future TODO

- Вынести лимит сцен в payload/notebook и считать его от `target_max_duration_sec`, а не только от `selected_max`.
- Держать regression-check на аудио-контракт: один трек (`The_xx_-_Intro.mp3`), один стартовый offset (`1:17`) и никакой server-side подмены на legacy cue в test/prod dataset path.
- Сделать test mode максимально близким к production selection, включая OCR-ограничения.
- Подключить popularity signal из TG/VK post metrics к ranking/order для `/v`, начиная с soft-boost первых сцен.
- Добавить отдельный publish step после test-рендера:
  - ручной publish в `main` для тех профилей, где автоматический `main` всё ещё считается слишком рискованным.
