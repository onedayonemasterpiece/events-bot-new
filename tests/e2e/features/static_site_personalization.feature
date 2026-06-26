# language: ru
@draft @static_site @personalization @event_detail_related @playwright_contract
Функция: MVP-0 персонализация блока похожих событий на static event page
  Чтобы пользователь быстрее находил следующее интересное событие
  Как посетитель страницы конкретного события без авторизации
  Я хочу сразу видеть статический блок "Похожие события", а после consent — локально переупорядоченный блок,
  который дешёвый, устойчивый к отсутствию backend и отличается на mobile/desktop только презентацией, не смыслом контракта.

  Предыстория:
    Дано опубликована static event page "/sobytiya/<slug>/" из Fly SQLite
    И при build/export подготовлен static manifest related_static для текущего события
    И Supabase personalization не является source of truth событий
    И online LLM не вызывается при page_view

  Сценарий: Без consent страница показывает static related fallback и не пишет персональную telemetry
    Дано я открыл страницу события на mobile viewport 375px
    Когда consent banner ещё не подтверждён
    Тогда HTML и CTA события доступны сразу
    И блок "Похожие события" показывает static_related_v1
    И текущий event, cancelled events и другие даты того же события не показаны в related
    И localStorage не содержит персонального профиля
    И compact personalization telemetry не отправлена

  Сценарий: После consent local profile переупорядочивает related block, но контекст текущего события доминирует
    Дано я открыл страницу джазового концерта
    И мой локальный профиль любит театр и немного джаз
    И мой профиль содержит negative_interest_tags для детских мастер-классов
    Когда я нажал "ОК" в consent banner
    Тогда surface равен "event_detail_related"
    И layout_mode равен "module"
    И algorithm_id равен "local_related_rerank_v1"
    И джазовое/музыкальное похожее событие остаётся выше театральной подборки
    И детские мастер-классы не доминируют в top related module
    И compact served_list_summary содержит current_event_id, shown ranks, reason_codes и algorithm_id

  Сценарий: Явно скрытое событие не возвращается в блок
    Дано блок "Похожие события" персонализирован после consent
    Когда я нажимаю "Не интересно" на related card
    Тогда event_id добавлен в hidden_event_ids локального профиля
    И этот event_id не появляется в следующих render блока

  Сценарий: Desktop использует module/grid/right rail, не мобильную бесконечную ленту
    Дано я открыл ту же страницу события на desktop viewport 1440px
    Когда персонализация применена после consent
    Тогда viewport_class равен "desktop"
    И presentation равен "desktop_related"
    И layout_mode остаётся "module"
    И блок может быть right rail или grid ниже описания
    И hover/focus не является сильным сигналом обучения

  Сценарий: Backend/Supabase недоступен, но fallback и CTA не ломаются
    Дано Supabase Data API или RPC недоступен или отвечает медленно
    Когда пользователь подтвердил consent
    Тогда блок использует local rerank или static fallback без блокирующего спиннера
    И CTA события остаётся доступной
    И telemetry содержит recommendation_fallback_used только после consent
