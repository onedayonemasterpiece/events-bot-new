# Dual-plane персонализация, транспортная надёжность и экологичный профиль

> **Статус:** промежуточное архитектурное решение для согласования.  
> **Дата среза:** 2026-08-04.  
> **База:** `main@0d1848bc324ef8c44df146ec2a7126a116a94bf4`.  
> **Scope:** статический сайт, персонализация, social actions, профиль пользователя, диагностика, фокус-группа, Supabase/YDB/Yandex transport, автотесты.  
> **Не является:** финальным юридическим заключением по 152-ФЗ, production migration, runtime implementation или разрешением на включение remote personalization writes.

## 1. Короткое решение

Целевая модель строится не вокруг Supabase-cache профиля, а вокруг **zero-backend navigation**:

```text
Static page / CDN event manifest
        ↓
Browser local profile projection
        ↓
Local rerank / exact hide / save state
        ↓
Обычное листание календаря и подборок: 0 запросов к YDB и 0 запросов к Supabase
```

YDB используется только когда есть явная необходимость:

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

Гипотеза про Supabase-cache понятна: если часть пользователей быстрее достаёт Supabase, можно сократить прямые обращения к YDB. Но для текущего продукта это ухудшает архитектуру.

| Риск Supabase profile cache | Почему это плохо |
|---|---|
| Юридический риск | Даже encrypted profile projection остаётся псевдонимным пользовательским профилем, если оператор может связать его с человеком. |
| Две версии правды | YDB primary + Supabase cache требуют invalidation, TTL, reset/delete propagation и conflict handling. |
| Малая экономия | Основной cache должен жить в браузере. Тогда Supabase-cache почти не снижает обращения при листании страниц. |
| Reset/delete усложняются | Нужно доказывать удаление/инвалидацию во всех foreign cache/spool слоях. |
| Устаревшие рекомендации | Supabase может отдать stale projection после свежего действия, если invalidation запаздывает. |

**Решение:** browser cache — единственный default profile cache. Supabase bridge — transient transport, а не storage.

Исключение возможно только как отдельный `availability_risk_mode` для невосполнимых операций: например, encrypted emergency spool для текста feedback или raffle registration с коротким TTL и отдельным risk acceptance. Для обычных likes/hides/saves это запрещено.

## 3. Два plane, но не космолёт

### 3.1 Auth/session plane

```text
Browser
  ├─ direct Supabase Auth
  └─ Yandex API Gateway relay → Supabase Auth
```

- Supabase держит сессию, JWT и refresh lifecycle.
- YDB не участвует в обычном восстановлении и поддержании сессии.
- Yandex ingress может проверять Supabase JWT локально по JWKS для низкорисковых действий.
- Чувствительные операции требуют более свежего session/step-up proof.

### 3.2 Personalization/social actions plane

```text
Browser
  ├─ Yandex ingress → Function/Container → YDB
  └─ Supabase Edge blind bridge → Yandex ingress → Function/Container → YDB
```

Оба маршрута доставляют один и тот же `batch_id` / `action_id` в один YDB primary. Они отличаются только сетевым путём.

Bridge rules:

- не расшифровывает payload;
- не пишет payload в Supabase DB/Storage;
- не логирует body;
- возвращает успех только после YDB/YMQ durable acknowledgement;
- не становится системой истины.

### 3.3 Event/search plane

Канонические события остаются во Fly SQLite и публикуются как static artifacts. Supabase содержит только bounded event/search/vector projection, где это уже нужно для авторизованного поиска и related/search canary. Перенос канонических событий в Supabase не входит в это решение.

## 4. Листание страниц: главный cost и reliability gate

Плохая модель:

```text
5 000 users × 4 sessions/month × 20 pages/session = 400 000 profile reads/month
5 000 users × 4 sessions/month × 50 pages/session = 1 000 000 profile reads/month
```

Такая модель способна съесть YDB free tier только чтениями профиля и плохо переживает нестабильную сеть.

Целевая модель:

```text
5 000 users × 4 sessions/month × 1 projection refresh/session = 20 000 profile reads/month
```

Обычное листание календаря, дат вперёд, подборок и event pages работает так:

```text
page view / navigation / scroll:
  YDB requests = 0
  Supabase requests = 0
```

Refresh profile projection допускается только:

- при отсутствии совместимой projection;
- при истечении `fresh_until`;
- после server `profile_hint` в ACK;
- при явном reset/link/delete;
- вручную в debug/test flow.

Не допускается refresh на каждый page view, scroll, календарную дату или подборку.

## 5. Browser storage budget

KenigEvents-owned browser storage остаётся маленьким и управляемым.

| Часть | Target | Hard |
|---|---:|---:|
| `profile_projection` | `<= 4 KiB` | `<= 8 KiB` |
| explicit exact state | `<= 8 KiB` | `<= 12 KiB` |
| outbox lanes total | `<= 12 KiB` | `<= 16 actions` |
| transport/profile hints | `<= 1 KiB` | `<= 2 KiB` |
| diagnostics cached | generated on demand | no durable raw body |
| KenigEvents aggregate | `<= 32 KiB` | `<= 64 KiB` |

Projection lifetime:

```text
fresh:             24h
stale-but-usable:  up to 7d if schema/model compatible
invalid/stale too old: static-only fallback
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
- latest exact hide/save state;
- activation/document version marker while profile is active.

## 6. Profile page / user center

Добавляется отдельная пользовательская поверхность: **профиль пользователя**.

Рекомендуемый route name для обсуждения:

```text
/profil/
```

Это utility page, не SEO surface. Она должна быть `noindex`.

### 6.1 Что показывает профиль

| Блок | Источник | Поведение |
|---|---|---|
| Состояние входа | Supabase session snapshot | Показывает статус и способ входа; YDB не нужен для обычной сессии. |
| Профиль интересов | local projection + optional YDB refresh | Сначала показывает cached/local, затем обновляет не чаще policy. |
| Сохранённые / скрытые | local exact state + server current state when available | Показывает текущий scope: «на этом устройстве» или «синхронизировано». |
| Диагностика | generated on demand | Копирует redacted diagnostic bundle без email/JWT/body. |
| Управление | local + server actions | Выйти, сбросить персонализацию, удалить/запросить данные — отдельные действия. |

### 6.2 Mobile menu

В мобильном меню под биркой не показывать отдельную кнопку `Выйти`. Вместо этого:

```text
Профиль
```

Внутри профиля:

- статус входа;
- войти / выйти;
- сброс персонализации;
- диагностическая информация;
- ссылки на сохранённое и скрытое.

### 6.3 Desktop

Desktop contract:

- account button / avatar может открывать small popover только для быстрых действий: статус, «Открыть профиль», «Выйти»;
- full settings, interests, diagnostics, reset/delete живут на полноценной странице `/profil/`;
- popover не становится вторым местом управления персонализацией.

### 6.4 Online или cached

Профиль сначала всегда отображает локальное состояние:

```text
local-fresh
local-stale
static-only
```

Если сеть доступна и policy разрешает, profile page запускает background refresh. UI обязан подписывать источник:

- «Синхронизировано»;
- «На этом устройстве»;
- «Показываем сохранённую копию; обновим позже»;
- «Персонализация сброшена».

## 7. Диагностическая информация

### 7.1 Профиль: ручное копирование

Профиль должен уметь скопировать diagnostic bundle:

```text
build_id / repo_sha / release_id
page_family / route / surface_id
auth route health class
p13n route health class
profile_projection_revision / age class
outbox counts by lane
oldest pending age bucket
local storage budget used
service worker version
browser display mode: web/app
connectivity timing class
last error code class
```

Запрещено включать:

- email;
- ФИО;
- JWT/refresh token;
- OTP;
- raw feedback text;
- screenshot/body;
- полный user agent;
- точные IP/network identifiers.

### 7.2 Фокус-группа: автоматическое приложение диагностики

При отправке feedback/NPS/page score/screenshot автоматически прикладывается redacted diagnostic bundle. Это отдельный component receipt:

```text
feedback_text: committed
feedback_screenshot: pending|committed|failed
feedback_diagnostics: attached|skipped|failed
```

Диагностика не подтверждает сам feedback, а помогает разбору в ChatGPT/операторском анализе.

## 8. YDB data ecology

YDB primary хранит current state, а не бесконечный журнал.

Recommended contours:

```text
identity_vault
  person_id
  email_hmac
  encrypted_email
  encrypted_name
  auth_alias
  raffle_state
  purpose_consents

personalization_current
  person_id bucket
  activation_epoch
  reset_epoch
  profile_revision
  compact top facets
  projection etag

social_current
  person_id bucket
  event_id
  like_state
  hide_state
  save_state
  calendar_state
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

Rules:

- no page view firehose;
- no per-scroll telemetry;
- no full profile rewrite per page;
- no full event copies;
- no raw profile in Supabase;
- public social counters are folded into static CDN manifest.

## 9. Transport and action semantics

Operation classes:

| Class | Examples | Rule |
|---|---|---|
| `desired_state` | like/unlike, save/unsave, hide/restore | Coalesce to latest desired state. |
| `append_command` | activation, feedback text, raffle registration | Deduplicate by `action_id`, never coalesce different payloads. |
| `selected_once` | OTP issue, provider send | Never blind retry after ambiguous dispatch. |
| `disposable` | weak diagnostics, analytics sample | Drop first under pressure. |

Every durable action carries:

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

Reset/delete increments epoch. Late replay from an old epoch is rejected.

## 10. Autotest expansion

New blocking test families:

### 10.1 Zero backend navigation

```text
p13n.calendar_50_pages_zero_backend
p13n.collections_20_pages_zero_backend
p13n.page_navigation_uses_cached_projection
p13n.projection_refresh_once_per_session
p13n.stale_projection_ydb_down_static_usable
```

Assertions:

```text
YDB requests == 0 for ordinary navigation
Supabase requests == 0 for ordinary navigation unless Auth/session explicitly needs refresh
profile refresh <= 1 per session
local rerank works with fresh/stale compatible projection
static fallback works with no projection
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

### 10.3 Profile page

```text
profile.mobile_menu_links_to_profile_not_logout
profile.desktop_popover_opens_profile
profile.local_fresh_state
profile.local_stale_state
profile.static_only_state
profile.copy_diagnostics_redacted
profile.reset_blocks_late_replay
profile.storage_budget_enforced
```

### 10.4 Focus feedback diagnostics

```text
focus.feedback_attaches_redacted_diagnostics
focus.feedback_partial_component_receipts
focus.feedback_screenshot_failed_text_committed
focus.feedback_diagnostics_does_not_include_pii
```

Evidence bundle:

- DOM snapshot;
- accessibility tree;
- Android/iOS native tree for mobile-critical cases;
- HAR/trace with DNS/connect/TTFB/body/total;
- IndexedDB/localStorage before/after redacted summaries;
- YDB rows/bytes/RU for write tests;
- screenshot only for selected states and failures.

## 11. Operational guardrails

- YDB RU target for personalization/social/identity: `<= 500k RU/month` during early rollout.
- Warning: `600k RU/month`.
- Noncritical shedding: `700k RU/month`.
- Hard boundary: `800k RU/month` until owner approval.
- Ordinary page view must stay `0 backend calls`.
- Profile projection p95 read/write cost must be measured in canary before rollout.
- External status monitoring must include Yandex API Gateway/Functions/YDB, not only Fly `/healthz`.
- Quarterly restore drill for YDB identity/profile data before broad rollout.

## 12. Open decisions

1. Final route name: `/profil/`, `/moy-profil/`, or `/account/`.
2. Whether Supabase Auth receives real email or opaque alias. Current direction: opaque alias via YDB email vault, but this needs implementation/legal review.
3. Whether encrypted emergency spool in Supabase is allowed for feedback/raffle. Default: off.
4. Exact projection freshness: initial proposal `24h fresh / 7d stale-compatible`.
5. Which operations require step-up auth versus local JWT verification only.
6. Whether desktop account popover may contain `Выйти` directly or only link to profile.

## 13. Non-goals for the next slice

- No production remote personalization writes.
- No Supabase profile cache.
- No full user dashboard/admin UI for feedback review.
- No second canonical event store.
- No page-level server ranking.
- No infinite clickstream.

## 14. Next implementation slice

1. Update canonical ownership ADR from Supabase-primary to dual-plane/YDB-primary for PII+p13n+social state.
2. Update implementation status with this decision and block PR #295 merge unless rewritten.
3. Add `/profil/` product/UX contract.
4. Extend scenario registry with zero-backend navigation, slow transport, profile, diagnostics, and feedback-component tests.
5. Implement only local profile page skeleton and diagnostic copy first; no remote writes.
