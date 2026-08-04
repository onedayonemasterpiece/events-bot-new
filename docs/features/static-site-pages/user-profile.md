# Профиль пользователя статического сайта

> **Статус:** продуктово-технический contract для согласования.  
> **Дата среза:** 2026-08-04.  
> **Связано:** [`personalizaion/transport-ecology-profile-architecture.md`](personalizaion/transport-ecology-profile-architecture.md), [`focus-group.md`](focus-group.md), [`personalizaion/personalization-implementation-contract.md`](personalizaion/personalization-implementation-contract.md).

## 1. Решение

Ввести отдельную пользовательскую страницу профиля как единую точку для:

- входа/выхода;
- просмотра состояния персонализации;
- управления интересами и reset;
- копирования диагностической информации;
- future privacy/account actions.

Сохранённые и скрытые события **не входят в профиль**. Они остаются в отдельной пользовательской поверхности `Избранное`, которая является единственным местом их просмотра, восстановления и удаления.

Рекомендуемый route для обсуждения:

```text
/profil/
```

Страница профиля — utility surface, не SEO surface. Она должна быть `noindex,nofollow,noarchive`.

## 2. Почему профиль нужен

Сейчас функции размазаны по интерфейсу: авторизация, выход, персонализация, диагностика и фокус-группа живут как отдельные элементы. Это плохо масштабируется:

- кнопка `Выйти` под мобильной биркой конкурирует с навигацией;
- пользователю негде увидеть, что персонализация работает локально или синхронизирована;
- нет понятного места для reset;
- диагностическую информацию приходится собирать отдельной страницей;
- будущие raffle/PII/privacy flows потребуют поверхности управления.

Профиль решает это без превращения всего сайта в приложение: страница остаётся статической оболочкой с client hydration.

`Избранное` при этом сохраняет самостоятельную продуктовую роль и не растворяется в account settings.

## 3. Mobile navigation

В мобильном меню под биркой показывать не `Выйти`, а:

```text
Профиль
```

Логика:

- signed-out: `Профиль` ведёт на страницу с предложением войти и объяснением локального режима;
- signed-in: `Профиль` ведёт на страницу статуса аккаунта и персонализации;
- quick logout из основного меню убрать, чтобы не смешивать navigation и account management;
- `Избранное` остаётся отдельным пунктом/разделом навигации и не переносится внутрь профиля.

## 4. Desktop navigation

Desktop допускает маленький account popover, но только как shortcut:

- статус входа;
- `Открыть профиль`;
- возможно `Выйти`, если не перегружает UX.

Все сложные действия — интересы, reset, diagnostics и privacy — находятся на полной странице профиля. Popover не должен становиться вторым полноценным центром управления.

`Избранное` остаётся отдельной полноэкранной utility surface, одинаково доступной на desktop и mobile.

## 5. Содержание страницы

### 5.1 Header/status

Показывает:

- signed-in / signed-out / checking / degraded;
- способ входа: email/Yandex/alias, без раскрытия лишних данных;
- состояние сессии: актуальна, требует обновления, offline local mode;
- timestamp последней успешной синхронизации.

YDB не участвует в обычной проверке Supabase session.

### 5.2 Персонализация

Показывает:

- включена / не начата / сброшена;
- источник данных: `синхронизировано`, `на этом устройстве`, `сохранённая копия`;
- основные интересы top-K, если projection совместима;
- объяснение без creepy inference: `Рекомендуем по отмеченным темам`, а не скрытые выводы о личности;
- CTA `Настроить интересы`;
- CTA `Сбросить персональные рекомендации`.

### 5.3 Граница с «Избранным»

Профиль не показывает карточки, счётчики, списки или controls сохранённых и скрытых событий.

Отдельный раздел `Избранное` владеет двумя пользовательскими коллекциями:

- `Сохранённые`;
- `Скрытые / Не интересно` с восстановлением события.

Обе коллекции могут использовать общий local/server current state, но не дублируются в профиле. Это сохраняет простую информационную архитектуру:

```text
Профиль   = аккаунт, интересы, синхронизация, диагностика, privacy
Избранное = сохранённые и скрытые события
```

### 5.4 Диагностика

Кнопка:

```text
Скопировать диагностическую информацию
```

Содержимое redacted bundle:

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
- bearer preview URL.

### 5.5 Privacy/account actions

Минимум на ближайший этап:

- `Выйти на этом устройстве`;
- `Сбросить персональные рекомендации`.

Позже:

- `Удалить данные персонализации`;
- `Экспортировать данные`;
- `Удалить аккаунт`;
- raffle data view only when product/legal flow is approved.

Эти действия не должны быть одним и тем же. Logout не удаляет профиль; reset не удаляет аккаунт; delete не является logout-only.

## 6. Data source states

Профиль всегда сначала рисует локальное состояние.

| State | UI copy | Backend behavior |
|---|---|---|
| `local-fresh` | `Показываем актуальные данные на этом устройстве` | No immediate backend required. |
| `synced` | `Синхронизировано` | Last ACK/projection refresh succeeded. |
| `local-stale` | `Показываем сохранённую копию; обновим позже` | Background refresh allowed by policy. |
| `static-only` | `Персонализация пока не настроена` | No projection or incompatible schema. |
| `pending-actions` | `Есть действия, ожидающие отправки` | Outbox state shown honestly. |
| `reset-pending` | `Сброс применён на устройстве; подтверждаем на сервере` | Late replay blocked by reset epoch. |

Эти состояния описывают аккаунт и профиль интересов. Состояние коллекций `Избранного` отображается на самой странице `Избранное`, а не в профиле.

## 7. Storage ecology

Profile page must not introduce large local storage. It may read the shared personalization envelope and display a summarized view.

Rules:

- no durable profile-specific debug log;
- no full event manifest in profile storage;
- no local duplicate of saved/hidden collections for rendering profile page;
- no stored diagnostic history except optional one latest redacted copy with short TTL;
- no raw PII in localStorage;
- localStorage aggregate budget remains 64 KiB for KenigEvents-owned keys;
- if quota is exceeded, disposable hints are removed before user state.

## 8. Focus-group integration

When the user submits feedback, page score, NPS, structured event error or screenshot, the client attaches a redacted diagnostics object automatically.

Required component receipts:

```text
feedback_text
feedback_score
feedback_screenshot
feedback_diagnostics
```

Valid outcomes:

```text
text committed, screenshot pending, diagnostics attached
text committed, screenshot failed, diagnostics attached
text failed, screenshot skipped, diagnostics attached
```

Invalid UX:

- `Всё отправлено`, если screenshot pending/failed;
- `Ничего не сохранилось`, если text/score committed;
- diagnostic bundle with PII/tokens/raw body.

## 9. Accessibility and keyboard

Profile must support:

- keyboard navigation through all account actions;
- visible focus;
- clear `role=status` / `role=alert` messages;
- screen-reader-readable source labels (`на этом устройстве`, `синхронизировано`);
- no hover-only controls;
- no hidden logout under visual-only affordance.

`Избранное` отдельно должно обеспечивать keyboard-accessible переключение между сохранёнными и скрытыми событиями и доступное восстановление скрытого события.

## 10. Autotests

Minimum scenarios:

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
profile.does_not_render_saved_or_hidden_collections
favorites.owns_saved_and_hidden_collections
favorites.hidden_restore_flow
focus.feedback_attaches_diagnostics_bundle
focus.feedback_component_receipts_are_truthful
```

Evidence:

- DOM snapshot;
- accessibility tree;
- localStorage/IndexedDB redacted summary;
- network request count;
- route health summary;
- screenshot for mobile menu/profile/favorites states.

## 11. Non-goals

- Full admin interface for feedback analysis.
- Public profile page.
- Social network profile import.
- Browsing or managing saved/hidden events inside the profile.
- Editing PII or raffle data before legal/product flow is approved.
- Supabase profile cache.
- Remote profile writes before localization/legal gate.
