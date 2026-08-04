# Тест-план: персонализация, транспорт, профиль и экологичность

> **Статус:** test-design companion к [`../features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md`](../features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md).  
> **Дата среза:** 2026-08-04.  
> **Цель:** доказать, что персонализация не создаёт лишних запросов к YDB/Supabase при листании статических страниц, а strong actions переживают отказы, замедление каналов, VPN toggle, reload и multi-tab.

## 1. Главные инварианты

```text
ordinary page navigation = 0 YDB requests + 0 Supabase requests
profile refresh <= 1 per session unless explicit invalidation
one logical action = one durable effect
no false success before durable acknowledgement
no silent outbox eviction
no PII/tokens/raw feedback in diagnostics
profile does not duplicate saved/hidden collections
favorites is the sole saved/hidden user surface
```

## 2. Слои тестирования

| Layer | Назначение |
|---|---|
| L0 unit/contracts | Schemas, operation classes, route decision, storage budgets. |
| L1 browser deterministic | Full fault matrix in Chromium/Playwright with network throttling and route interception. |
| L2 mobile representative | Android Chrome and iOS Safari/PWA critical cells only. |
| L3 server/integration | YDB RU, Function ingress, bridge behavior, YMQ/spool if enabled. |
| L4 restore/ops | Backups, wrong account, stopped gateways, RU limits, runbooks. |

## 3. Zero-backend navigation suite

### `p13n.calendar_50_pages_zero_backend`

Steps:

1. Load calendar/listing with valid local profile projection.
2. Navigate 50 date/listing pages using normal UI.
3. Intercept network.

Assertions:

```text
YDB requests == 0
Supabase data/profile requests == 0
Auth refresh requests == 0 unless token refresh is actually due
local rerank applied only to allowed surfaces
calendar primary chronology preserved
storage growth <= 1 KiB
```

### `p13n.collections_20_pages_zero_backend`

Same for подборки, popular, thematic surfaces and event detail related blocks.

### `favorites.saved_hidden_navigation_zero_backend_when_cached`

Steps:

1. Open `Избранное` with compatible cached saved/hidden current state.
2. Switch between `Сохранённые` and `Скрытые / Не интересно`.
3. Open and return from several event cards without mutating state.

Assertions:

```text
profile page is not opened or used as a collection renderer
YDB requests == 0
Supabase data/profile requests == 0
saved and hidden collections are rendered only inside Favorites
storage growth is bounded and no duplicate collection cache is created
```

### `p13n.projection_refresh_once_per_session`

Assertions:

```text
first profile page/session may refresh projection once
subsequent page navigation uses cached projection
ETag/304 path does not rewrite local state unnecessarily
```

### `p13n.stale_projection_ydb_down_static_usable`

Assertions:

```text
stale compatible projection used with clear stale label
YDB down does not break navigation
no repeated refresh storm
strong actions queue locally
```

## 4. Transport slow/fault matrix

### Personalization route states

| Yandex direct | Supabase bridge | Expected |
|---|---|---|
| fast | fast | choose last-known-good / fastest by policy |
| fast | slow | choose Yandex direct |
| slow | fast | choose bridge |
| slow | slow | one dispatch, no duplicate, user sees pending if deadline exceeded |
| down | fast | bridge works |
| fast | down | direct works |
| down | down | local outbox only |
| stalled body | fast | body stall not counted as success; bridge may be used before dispatch if unambiguous |
| commit then response lost | any | reconcile by same `action_id` before replay |

### Required fault phases

```text
DNS failure
TCP/connect timeout
TLS failure
request upload throttled
TTFB slow
headers ok + body stalled
partial JSON/body then close
429 Retry-After
5xx
connection reset
jitter/packet loss
VPN toggle mid-session
```

## 5. Auth/session suite

```text
auth.session_restore_no_ydb
auth.jwt_valid_supabase_unreachable
auth.jwt_expired_supabase_unreachable
auth.refresh_direct_slow_relay_fast
auth.refresh_relay_slow_direct_fast
auth.revoked_session_with_unexpired_jwt
auth.jwks_rotation
auth.device_clock_skew
```

Assertions:

- Supabase session is maintained without YDB calls.
- Low-risk personalization actions can use valid cached JWT/JWKS.
- Sensitive profile/PII actions require fresh/step-up proof.
- Logout does not erase personalization unless reset/delete is explicit.

## 6. Profile and favorites suite

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
favorites.profile_navigation_does_not_duplicate_state
```

Assertions:

- Mobile menu exposes `Профиль`, not hidden direct logout under the brand tag.
- Profile page renders account, interests, synchronization and diagnostics from local cache first.
- Profile page contains no saved/hidden cards, lists, counters or collection controls.
- `Избранное` is the sole surface for `Сохранённые` and `Скрытые / Не интересно`.
- Hidden-event restoration is performed inside `Избранное` and updates the shared exact state once.
- Diagnostic copy excludes email/JWT/OTP/raw body/user agent.
- Reset creates epoch and blocks late offline replay.
- Storage remains within 64 KiB KenigEvents-owned aggregate ceiling.
- Opening profile after Favorites does not create a second local copy of saved/hidden collections.

## 7. Focus feedback suite

```text
focus.feedback_attaches_diagnostics_bundle
focus.feedback_component_receipts_are_truthful
focus.feedback_screenshot_failed_text_committed
focus.feedback_diagnostics_does_not_include_pii
focus.feedback_reload_reconnect_exactly_once
```

Component receipts:

```text
feedback_score
feedback_text
feedback_screenshot
feedback_diagnostics
```

Assertions:

- Partial success is shown truthfully.
- Diagnostics are attached automatically when available.
- Missing diagnostics must not block the feedback text itself.
- Screenshot failure does not roll back text/score.

## 8. Storage and RU suite

```text
capacity.profile_projection_1k_10k_100k
capacity.action_batch_ru_p95
capacity.public_counter_hot_event
capacity.materialization_coalescing
capacity.no_pageview_firehose
capacity.no_supabase_profile_cache
capacity.no_profile_duplicate_of_favorites
```

Required metrics:

```text
rows read/written
bytes read/written
estimated YQL IO RU floor
server-side consumed RU when available
Function duration
Gateway calls
Supabase Edge calls
storage bytes per active subject
```

Pass conditions before rollout:

- p95 profile projection payload `<= 4 KiB`, hard `<= 8 KiB`;
- p95 current state payload `<= 1.5–2 KiB`;
- ordinary page view creates no YDB write/read;
- profile page creates no second saved/hidden projection or manifest;
- noncritical features shed before current state/reset/delete.

## 9. Evidence bundle

Every reliability run saves a redacted artifact with:

```text
repo_sha
build_id
scenario_id
operation_class
selected_route
route_timeline
failure_phase
DOM snapshot
AX tree
native tree for Android/iOS
localStorage/IndexedDB summary
network summary/HAR
YDB rows/bytes/RU summary
outbox state before/after
user_message_class
redaction_status
```

Forbidden evidence fields:

```text
email
ФИО
OTP
JWT/refresh token
raw feedback text
screenshot body unless private screenshot artifact test explicitly needs it
full user agent
IP address
bearer preview URL
```

## 10. Release gates

NO-GO if any is true:

- page navigation reads YDB profile repeatedly;
- Supabase stores profile projection/cache by default;
- bridge returns success without YDB/YMQ durable acknowledgement;
- action can be duplicated after route switch or response loss;
- reset/delete does not dominate old pending actions;
- outbox item expires silently;
- feedback component partial failure is reported as full success;
- diagnostics contain PII/secrets;
- mobile profile menu still exposes logout as the primary account control;
- profile renders or independently caches saved/hidden event collections;
- `Избранное` is not the single recovery surface for hidden events;
- no representative slow-channel Android/iOS evidence for changed mobile-critical flow.
