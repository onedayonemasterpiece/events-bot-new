# Тест-план: персонализация, транспорт, профиль и экологичность

> **Статус:** test-design companion к [`../features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md`](../features/static-site-pages/personalizaion/transport-ecology-profile-architecture.md).  
> **Дата среза:** 2026-08-04.  
> **Цель:** доказать zero-backend navigation, надёжность dual-plane транспорта, компактность storage и отсутствие регрессий существующих product surfaces.

## 1. Source contracts

Тест-план не изобретает новую IA. Он проверяет существующие источники:

- [`../features/static-site-pages/schedule-user-requirements.md`](../features/static-site-pages/schedule-user-requirements.md);
- [`../features/event-favorites-calendar/README.md`](../features/event-favorites-calendar/README.md);
- [PR #235](https://github.com/onedayonemasterpiece/events-bot-new/pull/235) как открытый target двухзонного Favorites;
- [`../features/static-site-pages/personalizaion/requirements.md`](../features/static-site-pages/personalizaion/requirements.md);
- [`../features/static-site-pages/personalizaion/personalization-implementation-contract.md`](../features/static-site-pages/personalizaion/personalization-implementation-contract.md).

## 2. Главные инварианты

```text
ordinary page navigation = 0 YDB requests + 0 Supabase requests
profile refresh <= 1 per session unless explicit invalidation
one logical action = one durable effect
no false success before durable acknowledgement
no silent outbox eviction
no PII/tokens/raw feedback in diagnostics

profile renders no event collection
Favorites semantics are not changed by profile work
hidden recovery remains Подборки → Помечены «не интересует»
like, calendar save/favorite and hide remain distinct states
```

## 3. Слои тестирования

| Layer | Назначение |
|---|---|
| L0 unit/contracts | Schemas, action semantics, route decisions, product-boundary assertions, storage budgets. |
| L1 browser deterministic | Full fault/slow matrix in Chromium/Playwright. |
| L2 mobile representative | Android Chrome and iOS Safari/PWA critical cells. |
| L3 server/integration | YDB RU, ingress, bridge, YMQ/spool if enabled. |
| L4 restore/ops | Backups, wrong account, stopped resources, RU limits and runbooks. |

## 4. Zero-backend navigation suite

### `p13n.calendar_50_pages_zero_backend`

1. Load listing with compatible local projection.
2. Navigate 50 date/listing pages.
3. Intercept network.

Assertions:

```text
YDB requests == 0
Supabase data/profile requests == 0
Auth refresh requests == 0 unless actually due
calendar primary chronology preserved
storage growth <= 1 KiB
```

### `p13n.collections_20_pages_zero_backend`

Same for thematic collections, Popular and related blocks.

### `favorites.navigation_zero_backend_when_cached`

Current-main acceptance:

```text
/izbrannoe/ uses cached local saved/liked state without profile read
ordinary card browsing creates no YDB request
no profile page or profile projection is used as collection renderer
```

Target PR #235 acceptance, when that target is implemented:

```text
Мой календарь reads calendar_saved only
Понравилось reads favorite_saved only
same event may appear once per zone when both states are true
no duplicate inside either zone
```

### `hidden.collection_navigation_zero_backend_when_cached`

1. Open `Подборки → Помечены «не интересует»` with cached exact-hide state.
2. Browse without mutation.

Assertions:

```text
YDB requests == 0
Supabase profile requests == 0
collection is not rendered inside Profile or Favorites
restore control belongs to hidden recovery surface
```

### `p13n.projection_refresh_once_per_session`

```text
initial eligible refresh <= 1
subsequent navigation uses cached projection
ETag/304 does not rewrite state unnecessarily
```

### `p13n.stale_projection_ydb_down_static_usable`

```text
compatible stale projection remains usable
no repeated refresh storm
navigation remains available
strong actions queue locally
```

## 5. Transport slow/fault matrix

| Yandex direct | Supabase bridge | Expected |
|---|---|---|
| fast | fast | use fresh last-known-good/route policy |
| fast | slow | direct wins |
| slow | fast | bridge wins before command dispatch |
| slow | slow | one dispatch; pending after deadline, no duplicate |
| down | fast | bridge works |
| fast | down | direct works |
| down | down | local outbox only |
| body stalled | fast | headers are not success; safe route recovery only by operation policy |
| commit then response lost | any | reconcile same `action_id` before replay |

Fault phases:

```text
DNS
TCP/connect
TLS
request upload throttled
TTFB slow
headers ok + body stalled
partial body then close
429 Retry-After
5xx
connection reset
jitter/packet loss
VPN toggle mid-session
```

## 6. Auth/session suite

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

- Supabase session maintenance does not read YDB.
- Low-risk p13n actions may use valid cached JWT/JWKS.
- Sensitive PII/account actions require fresh/step-up proof.
- Logout does not imply personalization reset/delete.

## 7. Profile suite

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
profile.does_not_copy_collection_manifests
```

Assertions:

- Mobile account block exposes `Профиль`, while existing `Избранное` remains separate.
- Profile contains account/session, interests, sync state, diagnostics and privacy controls only.
- No calendar agenda, liked cards, hidden list, collection counters or restore controls appear in profile.
- Opening profile does not create a second local copy of event collections.

## 8. Favorites and hidden-boundary suite

### Current Favorites

```text
favorites.current_calendar_first_merge_contract
favorites.calendar_and_like_sources_are_distinct
favorites.not_interested_is_not_a_saved_source
favorites.profile_change_does_not_change_favorites_ia
```

### Target Favorites from open PR #235

```text
favorites.target_two_sequential_zones
favorites.target_calendar_zone_calendar_saved_only
favorites.target_liked_zone_favorite_saved_only
favorites.cross_zone_reuse_is_intentional
favorites.no_duplicate_inside_zone
```

These scenarios become blocking only when the PR #235 target is integrated; until then they are design-target evidence, not current-runtime PASS.

### Hidden recovery

```text
hidden.collection_remains_under_mobile_collections
hidden.exact_hide_applies_cross_surface
hidden.restore_flow_uses_one_exact_state_change
hidden.not_moved_to_profile
hidden.not_moved_to_favorites
```

### Signal separation

```text
signals.like_favorite_calendar_hide_are_distinct
signals.unlike_does_not_remove_calendar_save
signals.calendar_remove_does_not_remove_like
signals.restore_hide_does_not_create_like_or_save
```

## 9. Focus feedback suite

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

Diagnostics failure does not block committed text/score. Screenshot failure does not roll them back.

## 10. Storage and RU suite

```text
capacity.profile_projection_1k_10k_100k
capacity.action_batch_ru_p95
capacity.public_counter_hot_event
capacity.materialization_coalescing
capacity.no_pageview_firehose
capacity.no_supabase_profile_cache
capacity.no_profile_collection_copy
capacity.separate_state_kinds_without_duplicate_log
```

Required metrics:

```text
rows/bytes read
rows/bytes written
estimated YQL IO RU floor
server consumed RU
Function duration
Gateway calls
Supabase Edge calls
storage bytes per active subject
```

Pass conditions:

- profile projection p95 `<= 4 KiB`, hard `<= 8 KiB`;
- current-state p95 `<= 1.5–2 KiB`;
- ordinary page view creates no YDB read/write;
- profile creates no event collection cache;
- different action kinds may share compact physical storage but remain separately addressable and semantically independent;
- noncritical features shed before reset/delete/current strong state.

## 11. Evidence bundle

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
native tree for selected Android/iOS cases
localStorage/IndexedDB redacted summary
network/HAR summary
YDB rows/bytes/RU
outbox before/after
user_message_class
redaction_status
```

Forbidden:

```text
email
ФИО
OTP
JWT/refresh token
raw feedback text
screenshot body outside explicit private fixture
full user agent
IP
bearer preview URL
```

## 12. Release gates

NO-GO if any is true:

- page navigation repeatedly reads profile from YDB;
- Supabase stores default profile cache;
- bridge ACKs without YDB/YMQ durability;
- route switch duplicates an action;
- reset/delete permits late replay;
- outbox expires silently;
- feedback partial failure is shown as full success;
- diagnostics contain PII/secrets;
- mobile menu keeps logout as primary account control;
- profile renders or caches any event collection;
- Favorites is redefined to include hidden events by this profile/transport slice;
- hidden recovery is moved away from `Подборки → Помечены «не интересует»` without a separate owner decision and canonical requirements update;
- like, calendar save/favorite and hide are conflated;
- target PR #235 behavior is claimed as current implementation before integration;
- changed mobile-critical flow lacks representative slow-channel Android/iOS evidence.
