# Профиль пользователя статического сайта

> **Статус:** продуктово-технический contract для согласования.  
> **Дата среза:** 2026-08-04.  
> **Связано:** [`personalizaion/transport-ecology-profile-architecture.md`](personalizaion/transport-ecology-profile-architecture.md), [`focus-group.md`](focus-group.md), [`personalizaion/personalization-implementation-contract.md`](personalizaion/personalization-implementation-contract.md).

## 1. Источники и границы решения

Этот документ определяет только страницу профиля и **не переопределяет** существующие пользовательские коллекции.

Обязательные источники:

1. [`schedule-user-requirements.md`](schedule-user-requirements.md) — исходное требование: календарные сохранения показываются в разделе `Избранное`;
2. [`../event-favorites-calendar/README.md`](../event-favorites-calendar/README.md) — фактический R15-контракт и текущая реализация `/izbrannoe/`;
3. [PR #235](https://github.com/onedayonemasterpiece/events-bot-new/pull/235) — открытая, ещё не слитая целевая доработка `Избранного` в две последовательные зоны `Мой календарь` и `Понравилось`;
4. [`personalizaion/requirements.md`](personalizaion/requirements.md) — исходное требование по скрытым событиям: `Подборки → Помечены «не интересует»`;
5. [`personalizaion/personalization-implementation-contract.md`](personalizaion/personalization-implementation-contract.md) — exact-hide, undo и отдельная recovery-collection скрытых событий.

Следовательно:

- `Избранное` относится к `calendar_saved` и `favorite_saved`;
- `Не интересует` — отдельный exact-hide state и отдельная recovery-collection;
- like, favorite/calendar save и `Не интересует` остаются разными сигналами;
- профиль не переносит к себе ни одну из этих коллекций.

## 2. Решение

Ввести отдельную пользовательскую страницу профиля как единую точку для:

- входа и выхода;
- просмотра состояния аккаунта и сессии;
- просмотра и настройки интересов;
- сброса персонализации;
- копирования диагностической информации;
- будущих privacy/account actions.

Рекомендуемый route для обсуждения:

```text
/profil/
```

Страница профиля — utility surface, не SEO surface. Она должна быть `noindex,nofollow,noarchive`.

## 3. Что профиль не делает

Профиль не показывает и не управляет:

- календарными сохранениями;
- понравившимися событиями;
- карточками `Избранного`;
- списком событий, помеченных `Не интересует`;
- восстановлением скрытого события.

Эти функции остаются у существующих продуктовых поверхностей:

```text
/izbrannoe/
  current main: future saved set, calendar-first merge
  target in open PR #235: Мой календарь + Понравилось

Подборки → Помечены «не интересует»
  exact-hide recovery + restore
```

Этот документ не выбирает вместо PR #235 финальную визуальную реализацию `Избранного` и не объединяет hidden-state с Favorites.

## 4. Почему профиль нужен

Сейчас функции аккаунта и персонализации распределены по интерфейсу:

- кнопка `Выйти` под мобильной биркой конкурирует с навигацией;
- пользователю негде увидеть, работает ли персонализация локально или синхронизирована;
- нет понятного места для reset;
- диагностическую информацию приходится собирать отдельной страницей;
- будущие raffle/PII/privacy flows потребуют управляемой поверхности.

Профиль решает это без превращения сайта в тяжёлое приложение: страница остаётся статической оболочкой с client hydration.

## 5. Mobile navigation

В мобильном account-блоке показывать не отдельную кнопку `Выйти`, а:

```text
Профиль
```

Логика:

- signed-out: `Профиль` ведёт на страницу с предложением войти и объяснением локального режима;
- signed-in: `Профиль` ведёт на страницу статуса аккаунта и персонализации;
- logout находится внутри профиля;
- существующий пункт `Избранное` остаётся отдельным;
- hidden recovery остаётся внутри `Подборки → Помечены «не интересует»`;
- ни `Избранное`, ни hidden recovery не вкладываются в профиль.

## 6. Desktop navigation

Desktop допускает компактный account popover только как shortcut:

- статус входа;
- `Открыть профиль`;
- возможно `Выйти`, если это не создаёт второй полноценный центр управления.

Интересы, reset, diagnostics и privacy actions находятся на полной странице профиля.

`Избранное` и hidden recovery сохраняют собственные маршруты/места в навигации; desktop popover не переопределяет их.

## 7. Содержание страницы

### 7.1 Состояние аккаунта

Показывает:

- signed-in / signed-out / checking / degraded;
- способ входа: email/Yandex/alias, без раскрытия лишних данных;
- состояние сессии: актуальна, требует обновления, offline local mode;
- timestamp последней успешной синхронизации персонализации.

YDB не участвует в обычной проверке и поддержании Supabase session.

### 7.2 Персонализация

Показывает:

- `не начата` / `активна` / `сброшена`;
- источник данных: `синхронизировано`, `на этом устройстве`, `сохранённая копия`;
- основные интересы top-K, если projection совместима;
- объяснение без чувствительных или навязчивых выводов;
- CTA `Настроить интересы`;
- CTA `Сбросить персональные рекомендации`.

### 7.3 Диагностика

Кнопка:

```text
Скопировать диагностическую информацию
```

Redacted bundle:

```text
build/release id
route/surface id
auth health class
personalization transport health class
profile projection revision/age class
outbox counts and oldest pending age bucket
storage budget used
service worker version
browser mode: web/app
last error classes
```

Запрещено включать:

- email / ФИО;
- JWT / refresh token / OTP;
- raw user agent;
- raw feedback text;
- screenshot body;
- bearer preview URL;
- точный IP или сетевые идентификаторы.

### 7.4 Privacy/account actions

Ближайший этап:

- `Выйти на этом устройстве`;
- `Сбросить персональные рекомендации`.

Позже, после отдельного product/legal contract:

- `Удалить данные персонализации`;
- `Экспортировать данные`;
- `Удалить аккаунт`;
- просмотр raffle data только при реальной необходимости.

Logout, reset и delete — разные операции.

## 8. Data source states

Профиль сначала рисует локальное состояние.

| State | UI copy | Backend behavior |
|---|---|---|
| `local-fresh` | `Показываем актуальные данные на этом устройстве` | Немедленный backend не нужен. |
| `synced` | `Синхронизировано` | Последний ACK/refresh успешен. |
| `local-stale` | `Показываем сохранённую копию; обновим позже` | Допустим bounded background refresh. |
| `static-only` | `Персонализация пока не настроена` | Projection отсутствует или несовместима. |
| `pending-actions` | `Есть действия, ожидающие отправки` | Показывается честное состояние outbox. |
| `reset-pending` | `Сброс применён на устройстве; подтверждаем на сервере` | Старый epoch больше не может воскресить профиль. |

Эти состояния описывают аккаунт и профиль интересов, а не содержимое `Избранного` или hidden collection.

## 9. Storage ecology

Profile page читает общий компактный personalization envelope и не создаёт собственное хранилище коллекций.

Правила:

- no durable profile-specific debug log;
- no full event manifest in profile storage;
- no copy of calendar/liked/hidden event lists;
- no stored diagnostic history, кроме опциональной последней redacted-копии с коротким TTL;
- no raw PII in localStorage;
- aggregate budget KenigEvents-owned keys остаётся `64 KiB`;
- disposable hints удаляются раньше strong/current state.

## 10. Focus-group integration

При отправке feedback, page score, NPS, structured event error или screenshot клиент автоматически прикладывает redacted diagnostics object.

Component receipts:

```text
feedback_text
feedback_score
feedback_screenshot
feedback_diagnostics
```

Допустимые outcomes:

```text
text committed, screenshot pending, diagnostics attached
text committed, screenshot failed, diagnostics attached
text failed, screenshot skipped, diagnostics attached
```

Diagnostics не подтверждает доставку самого feedback и не должна блокировать отправку текста/оценки при собственной ошибке.

## 11. Accessibility and keyboard

Профиль поддерживает:

- keyboard navigation всех account actions;
- visible focus;
- `role=status` / `role=alert` для состояний;
- screen-reader-readable labels источника данных;
- no hover-only controls;
- no hidden logout under visual-only affordance.

Доступность `Избранного` и hidden recovery проверяется их собственными контрактами, а не профилем.

## 12. Autotests

```text
profile.mobile_menu_links_to_profile_not_logout
profile.desktop_popover_opens_profile
profile.signed_out_local_mode
profile.signed_in_synced_mode
profile.local_stale_ydb_down
profile.copy_diagnostics_redacted
profile.reset_marks_epoch_and_blocks_late_replay
profile.logout_does_not_delete_profile
profile.storage_budget_enforced
profile.no_backend_on_read_when_cached
profile.does_not_render_event_collections

favorites.calendar_and_like_signals_remain_distinct
favorites.current_contract_not_redefined_by_profile
favorites.two_zone_target_remains_owned_by_pr235
hidden.collection_remains_under_mobile_collections
hidden.restore_flow_not_moved_to_profile_or_favorites
signals.like_favorite_calendar_hide_are_distinct

focus.feedback_attaches_diagnostics_bundle
focus.feedback_component_receipts_are_truthful
```

## 13. Non-goals

- Full admin interface для анализа feedback.
- Public profile page.
- Social network profile import.
- Просмотр или управление event collections внутри профиля.
- Изменение IA `Избранного` в этом документе.
- Перенос hidden recovery из `Подборки`.
- Supabase profile cache.
- Remote profile writes до localization/legal gate.
