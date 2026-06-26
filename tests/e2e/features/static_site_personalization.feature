# language: ru
@draft @static_site @personalization @event_detail_related @playwright_contract
Функция: MVP-0 personalization for event_detail_related on static event pages
  Чтобы пользователь быстрее находил похожее интересное событие на kenigevents.ru
  Как посетитель без авторизации
  Я хочу сразу видеть статический блок "Похожие события", а после consent — локально переупорядоченный блок
  без online LLM, без server-side profile read по anon_id и без зависимости от Supabase для CTA.

  Предыстория:
    Дано опубликован static export будущих событий из Fly SQLite
    И страница события доступна как static HTML
    И блок "Похожие события" доступен как same-origin static manifest
    И Supabase personalization project используется только для compact telemetry/profile aggregation, но не является source of truth событий

  Сценарий: Без consent страница события показывает static event_detail_related fallback
    Дано я открыл страницу события "/sobytiya/<slug>/" на мобильном viewport 375px
    Когда consent banner ещё не подтверждён
    Тогда HTML события и CTA доступны без ожидания Supabase
    И блок "Похожие события" показывает static_related_v1 порядок
    И текущие, отменённые и просроченные события не показаны в related block
    И localStorage не содержит персонального профиля
    И персональная telemetry не отправлена

  Сценарий: Mobile после consent локально переупорядочивает related block
    Дано я открыл страницу джазового события на viewport 375px
    И я нажал "ОК" в consent banner
    И мой localStorage profile содержит положительный интерес "jazz/live_music" и отрицательный интерес "kids"
    Когда client island применяет local_related_rerank_v1
    Тогда surface равен "event_detail_related"
    И layout_mode равен "module"
    И presentation_mode равен "vertical_related"
    И джазовые/live_music кандидаты подняты выше детских событий
    И явно скрытое событие не возвращается в related block
    И telemetry содержит served_list_summary с current_event_id, rank, score и reason_codes

  Сценарий: Desktop использует related module/grid, а не мобильную бесконечную ленту
    Дано я открыл страницу события на desktop viewport 1440px
    И мой профиль содержит долгосрочный интерес "theatre"
    Когда персонализация применена после consent
    Тогда layout_mode равен "module"
    И presentation_mode равен "grid_related"
    И блок остаётся контекстно связанным с текущим событием, а не превращается в персональную главную
    И hover/focus не является сильным сигналом персонализации
    И уже видимый контент страницы не переставляется резко

  Сценарий: Supabase/API timeout не ломает блок и CTA
    Дано telemetry endpoint или Supabase Data API недоступен
    Когда пользователь открыл static event page и подтвердил consent
    Тогда HTML, CTA и static related fallback остаются доступны
    И local_related_rerank_v1_fallback может работать только из localStorage и manifest
    И telemetry содержит recommendation_fallback_used только после consent
    И page_view не требует online LLM вызова

  Сценарий: Offline enrichment создаёт признаки, но не участвует в каждом page view
    Дано есть batch будущих событий из production SQLite с концертами, спектаклями, лекциями, кинопоказами и экскурсиями
    Когда offline enrichment или deterministic probe строит event feature snapshot
    Тогда результат соответствует controlled taxonomy/schema
    И каждый event feature содержит normalized_tags, audience_exclusion_tags, feature_schema_version и warnings
    И online event_detail_related использует готовый snapshot/manifest, а не LLM prompt

  Сценарий: Personalization не меняет решения Smart Update по dedup/facts
    Дано Smart Update создал или обновил canonical Event из источников
    Когда static export и personalization feature snapshot читают это событие
    Тогда personalization может добавить только presentation/ranking признаки
    И telemetry пользователей не участвует в dedup, fact extraction, date/time/location repair или source trust
    И исправления title/date/venue/status по-прежнему проходят через Smart Update LLM-first pipeline и incident regression checks
