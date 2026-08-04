# Dual-plane персонализация, транспортная надёжность и экологичный профиль

> **Статус:** промежуточное архитектурное решение для согласования.  
> **Дата среза:** 2026-08-04.  
> **База:** `main@0d1848bc324ef8c44df146ec2a7126a116a94bf4`.  
> **Scope:** статический сайт, персонализация, social actions, профиль пользователя, диагностика, фокус-группа, Supabase/YDB/Yandex transport, автотесты.  
> **Не является:** финальным юридическим заключением по 152-ФЗ, production migration, runtime implementation или разрешением на включение remote personalization writes.

## 0. Source precedence и неизменяемые продуктовые границы

Этот документ не переопределяет существующие event collections.

Обязательные источники:

1. [`../schedule-user-requirements.md`](../schedule-user-requirements.md) — календарные сохранения находятся в `Избранном`;
2. [`../../../event-favorites-calendar/README.md`](../../../event-favorites-calendar/README.md) — текущий R15-контракт `/izbrannoe/`;
3. [PR #235](https://github.com/onedayonemasterpiece/events-bot-new/pull/235) — открытый, ещё не слитый target двухзонного `Избранного`: `Мой календарь` + `Понравилось`;
4. [`requirements.md`](requirements.md) — скрытые события находятся в `Подборки → Помечены «не интересует»`;
5. [`personalization-implementation-contract.md`](personalization-implementation-contract.md) — exact-hide, undo и отдельная hidden recovery collection.

Инварианты:

```text
Избранное:
  calendar_saved + favorite_saved
  current main: calendar-first merged future set
  target PR #235: Мой календарь + Понравилось

Hidden recovery:
  Подборки → Помечены «не интересует»

Профиль:
  account + interests + diagnostics + privacy
  event collections не содержит
```

Like, calendar save/favorite и `Не интересует` остаются разными сигналами.

## 1. Короткое решение

Целевая модель строится не вокруг Supabase-cache профиля, а вокруг **zero-backend navigation**:

```text
Static page / CDN event manifest
        ↓
Browser local profile projection
        ↓
Local rerank / exact hide / saved-state overlay
        ↓
Обычное листание календаря и подборок: 0 запросов к YDB и 0 запросов к Supabase
```

YDB используется только при явной необходимости:

1. activation / reset / delete / identity link;
2. coalesced batch сильных действий (`like`, `hide`, `save`, настройки);
3. редкий refresh `profile_projection` по ETag/revision;
4. materialization и public counters jobs;
5. PII / raffle / consent operations.

Supabase используется для:

1. Auth/session plane;
2. event/search projection plane;
3. optional blind bridge до YDB, когда пользовательский путь к Yandex недоступен;
4. **не** для долговременного profile cache по умолчанию.

## 2. Почему не Supabase profile cache по умолчанию

| Риск | Следствие |
|---|---|
| Юридический | Encrypted projection остаётся псевдонимным профилем, если оператор может связать её с человеком. |
| Две версии правды | YDB primary + Supabase cache требуют invalidation, TTL, reset/delete propagation и conflict handling. |
| Малая экономия | Основной cache уже живёт в браузере; page navigation не должна обращаться к backend. |
| Опасный reset/delete | Требуется доказуемая очистка во всех foreign cache/spool слоях. |
| Stale recommendations | Foreign cache может отдать старую projection после свежего действия. |

**Решение:** browser cache — единственный default profile cache. Supabase bridge — transient transport, а не storage.

Исключение возможно только как отдельный `availability_risk_mode` для невосполнимых операций: например, encrypted emergency spool для feedback text или raffle registration с коротким TTL и отдельным risk acceptance. Для обычных likes/hides/saves это запрещено.

## 3. Два plane без лишней сложности

### 3.1 Auth/session plane

```text
Browser
  ├─ direct Supabase Auth
  └─ Yandex API Gateway relay → Supabase Auth
```

- Supabase держит session, JWT и refresh lifecycle.
- YDB не участвует в обычном восстановлении и поддержании сессии.
- Yandex ingress может локально проверять Supabase JWT по JWKS для низкорисковых действий.
- Чувствительные операции требуют fresh session или step-up proof.

### 3.2 Personalization/social-actions plane

```text
Browser
  ├─ Yandex ingress → Function/Container → YDB
  └─ Supabase Edge blind bridge → Yandex ingress → Function/Container → YDB
```

Оба маршрута доставляют один и тот же `batch_id` / `action_id` в один YDB primary.

Bridge rules:

- не расшифровывает payload;
- не пишет payload в Supabase DB/Storage;
- не логирует body;
- возвращает success только после YDB/YMQ durable acknowledgement;
- не становится system of record.

### 3.3 Event/search plane

Канонические события остаются во Fly SQLite и публикуются как static artifacts. Supabase содержит только bounded event/search/vector projection, где она нужна авторизованному поиску и related/search canary. Второй canonical event store не создаётся.

## 4. Листание страниц: cost и reliability gate

Плохая модель:

```text
5 000 users × 4 sessions/month × 20 pages/session = 400 000 profile reads/month
5 000 users × 4 sessions/month × 50 pages/session = 1 000 000 profile reads/month
```

Целевая модель:

```text
5 000 users × 4 sessions/month × 1 projection refresh/session = 20 000 profile reads/month
```

Обычное листание календаря, дат, подборок и event pages:

```text
YDB requests = 0
Supabase requests = 0
```

Refresh projection допускается только:

- при отсутствии совместимой projection;
- после `fresh_until`;
- после server `profile_hint`;
- при reset/link/delete;
- вручную в debug/test flow.

Refresh на каждый page view, scroll, дату или подборку запрещён.

## 5. Browser storage budget

| Часть | Target | Hard |
|---|---:|---:|
| `profile_projection` | `<= 4 KiB` | `<= 8 KiB` |
| explicit current-state overlay | `<= 8 KiB` | `<= 12 KiB` |
| outbox lanes total | `<= 12 KiB` | `<= 16 actions` |
| transport/profile hints | `<= 1 KiB` | `<= 2 KiB` |
| diagnostics | generated on demand | no durable raw body |
| KenigEvents aggregate | `<= 32 KiB` | `<= 64 KiB` |

Projection lifetime:

```text
fresh:             24h
stale-compatible:  up to 7d
incompatible/too old: static-only fallback
```

Eviction order:

1. disposable diagnostics samples;
2. expired route hints;
3. stale search/feed hints;
4. acknowledged outbox entries;
5. old non-authoritative projection.

Never silently evict:

- reset/delete markers;
- unacknowledged strong actions;
- latest exact user state;
- activation/document version marker while profile is active.

Browser state may be reused by several surfaces, but no page creates a second durable copy of the same collection.

## 6. Profile page / user center

Добавляется отдельная utility page:

```text
/profil/
```

Она `noindex` и отвечает только за:

| Блок | Источник | Поведение |
|---|---|---|
| Account/session | Supabase session snapshot | YDB не нужен для обычной session check. |
| Interests | local projection + optional rare YDB refresh | Cached-first, честный stale/degraded state. |
| Diagnostics | generated on demand | Redacted bundle без PII/JWT/body. |
| Management | local + server commands | Login/logout, reset, future export/delete. |

Профиль не отображает event cards, collection counters или restore controls.

### 6.1 Mobile menu

- direct `Выйти` в account-блоке заменяется ссылкой `Профиль`;
- `Избранное` остаётся отдельным пунктом;
- hidden recovery остаётся внутри `Подборки → Помечены «не интересует»`;
- logout находится внутри профиля.

### 6.2 Desktop

Desktop может иметь compact account popover: status, `Открыть профиль`, optional logout. Full interests, diagnostics, reset/delete живут на `/profil/`.

`Избранное` и hidden recovery сохраняют собственные product surfaces.

### 6.3 Cached/online states

```text
local-fresh
synced
local-stale
static-only
pending-actions
reset-pending
```

UI показывает источник данных: `Синхронизировано`, `На этом устройстве`, `Показываем сохранённую копию`, `Персонализация сброшена`.

### 6.4 Adjacent event collections — без изменения IA

Этот architecture slice сохраняет, а не заменяет существующие контракты:

- current `/izbrannoe/` из main;
- target two-zone Favorites из открытого PR #235;
- hidden recovery collection из personalization requirements/implementation contract.

Профиль не является shortcut-обоснованием для перемещения или объединения этих поверхностей.

## 7. Диагностическая информация

### 7.1 Ручное копирование в профиле

```text
build_id / repo_sha / release_id
page_family / route / surface_id
auth route health class
p13n route health class
profile projection revision / age class
outbox counts by lane
oldest pending age bucket
local storage budget used
service worker version
web/app mode
connectivity timing class
last error class
```

Запрещены email, ФИО, JWT/refresh token, OTP, raw feedback, screenshot body, полный user agent, IP и bearer preview URL.

### 7.2 Автоматическое приложение к feedback

При feedback/NPS/page score/screenshot прикладывается redacted diagnostic bundle с отдельным receipt:

```text
feedback_text: committed|pending|failed
feedback_screenshot: committed|pending|failed|skipped
feedback_diagnostics: attached|failed|skipped
```

Diagnostics помогает разбору, но не подтверждает доставку feedback.

## 8. YDB data ecology

YDB хранит current state, а не бесконечный журнал.

```text
identity_vault
  person_id
  email_hmac
  encrypted_email/name
  auth_alias
  raffle_state
  purpose_consents

personalization_current
  person_bucket + person_id
  activation_epoch
  reset_epoch
  profile_revision
  compact top facets
  projection_etag

social_current
  person_bucket + person_id + event_id
  like_state
  favorite_saved
  calendar_saved
  hide_state
  latest_sequence

action_receipt_recent
  action_id
  payload_hash
  result_revision
  expires_at

materialization_due
  bucket
  due_at
  person_id
  reason_mask
```

Важно: общая физическая таблица current-state не означает общую UI-коллекцию. Представление определяется отдельным продуктовым контрактом:

- Favorites читает calendar/favorite state;
- hidden recovery читает hide state;
- profile не рендерит event collection.

Rules:

- no page-view firehose;
- no per-scroll telemetry;
- no full profile rewrite per page;
- no full event copies;
- no raw profile in Supabase;
- public counters fold в static CDN manifest.

## 9. Transport and action semantics

| Class | Examples | Rule |
|---|---|---|
| `desired_state` | like/unlike, calendar save/remove, hide/restore | Coalesce по конкретному action kind, не смешивать разные семантики. |
| `append_command` | activation, feedback text, raffle registration | Deduplicate by `action_id`, не coalesce разные payload. |
| `selected_once` | OTP issue, provider send | No blind retry после ambiguous dispatch. |
| `disposable` | weak diagnostics/analytics sample | Drop first under pressure. |

Каждое durable action содержит:

```text
action_id
device_id
device_sequence
person_epoch
base_entity_revision
operation_kind
target_id
payload_hash
schema_version
expires_at
```

Reset/delete меняет epoch; late replay старого epoch отклоняется.

## 10. Autotest expansion

### 10.1 Zero-backend navigation

```text
p13n.calendar_50_pages_zero_backend
p13n.collections_20_pages_zero_backend
p13n.page_navigation_uses_cached_projection
p13n.projection_refresh_once_per_session
p13n.stale_projection_ydb_down_static_usable
favorites.navigation_zero_backend_when_cached
hidden.collection_navigation_zero_backend_when_cached
```

### 10.2 Dual-plane transport

```text
auth.supabase_direct_down_yandex_relay_up
auth.yandex_relay_down_supabase_direct_up
p13n.yandex_down_supabase_bridge_up
p13n.supabase_bridge_down_yandex_up
p13n.both_slow_within_deadline
p13n.both_slow_beyond_deadline
p13n.headers_fast_body_stalled
p13n.commit_success_response_lost
p13n.vpn_toggle_route_change
```

### 10.3 Profile and adjacent surfaces

```text
profile.mobile_menu_links_to_profile_not_logout
profile.desktop_popover_opens_profile
profile.local_fresh_state
profile.local_stale_state
profile.static_only_state
profile.copy_diagnostics_redacted
profile.reset_blocks_late_replay
profile.storage_budget_enforced
profile.does_not_render_event_collections

favorites.calendar_and_like_signals_remain_distinct
favorites.current_contract_not_redefined
favorites.two_zone_target_owned_by_pr235
hidden.collection_remains_under_mobile_collections
hidden.restore_flow_is_separate
signals.like_favorite_calendar_hide_are_distinct
```

### 10.4 Focus feedback diagnostics

```text
focus.feedback_attaches_redacted_diagnostics
focus.feedback_partial_component_receipts
focus.feedback_screenshot_failed_text_committed
focus.feedback_diagnostics_does_not_include_pii
```

Evidence: DOM, AX tree, selected Android/iOS native tree, HAR/trace, redacted storage summary, YDB rows/bytes/RU, screenshots for selected failures/states.

## 11. Operational guardrails

- YDB target: `<= 500k RU/month` during early rollout;
- warning: `600k`;
- noncritical shedding: `700k`;
- hard boundary: `800k` until owner approval;
- ordinary page view: `0 backend calls`;
- canary measures projection p95 RU;
- external monitoring covers API Gateway, Function/Container and YDB;
- quarterly restore drill before broad rollout.

## 12. Open decisions

1. Route: `/profil/`, `/moy-profil/` или `/account/`.
2. Real email vs opaque Supabase alias — отдельное legal/implementation решение.
3. Encrypted emergency spool in Supabase — default off.
4. Projection freshness — initial hypothesis `24h fresh / 7d stale-compatible`.
5. Step-up auth matrix.
6. Может ли desktop popover содержать logout или только link to profile.

Не является open decision этого документа:

- состав `Избранного`;
- placement hidden recovery;
- объединение hide с like/favorite.

## 13. Non-goals

- Production remote personalization writes.
- Supabase profile cache.
- Feedback admin dashboard.
- Изменение IA `Избранного`.
- Перенос hidden recovery из `Подборки`.
- Second canonical event store.
- Page-level server ranking.
- Infinite clickstream.

## 14. Next implementation slice

1. Обновить ownership ADR только после отдельного согласования YDB-primary.
2. Синхронизировать implementation status и reliability docs с dual-plane моделью.
3. Реализовать local profile skeleton + diagnostic copy, без remote writes.
4. Сохранить current Favorites и hidden contracts без изменений в profile slice.
5. Расширить scenario registry zero-backend, slow transport, profile boundary и feedback diagnostics.
