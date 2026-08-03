# Аудит фактического runtime персонализации — 2026-08-02

> **Scope:** `main@079a9dc7a6830925456116770f54b82a5fe59fb0`.
> **Основной runtime:** `site/src/layouts/EventLayout.astro`.
> **Вердикт:** полезный локальный prototype, но **NO-GO как основа durable production personalization без Wave 0–3**.

## 1. Что уже сделано хорошо и должно быть сохранено

- статический HTML остаётся полезным без персонального backend;
- event cards имеют устойчивые ids и data hooks;
- runtime-карточки клонируются из канонического `EventCard`, а не строят второй
  handwritten DOM;
- local profile уже bounded по ids/maps;
- personal-feed cache больше не сохраняет full manifest для каждого preview;
- related/personal continuation ограничены и имеют diversity caps;
- current-event context не полностью уничтожается profile score;
- `resilientDataClient` и `resilientSupabaseTransport` уже содержат полезные
  идеи capability classification, direct/relay selection, circuit breaker,
  ambiguous-write distinction и bounded response validation;
- static catalog `/data/personal-feed.json` является хорошим zero-private-data
  candidate source;
- tests защищают card renderer, finite continuation, family dedupe и некоторые
  layout invariants.

Эти части нужно извлекать и переиспользовать, а не переписывать вслепую.

## 2. P0-расхождения

### P0-1. Legacy consent dialog противоречит activation contract

Фактический код:

- `PROFILE_KEY = 'ke_personalization_profile'`;
- `createEmptyProfile()` сразу записывает `consent_ok: true`;
- `profileHasConsent()` и `isCompatibleProfile()` используют этот boolean как
  главный eligibility gate;
- `consentBanner()` показывает modal `Пока нет / ОК`;
- acceptance создаёт profile до повторного выполнения исходного action.

Почему это блокирует реализацию:

- `ОК` превращается в псевдо-согласие вместо функциональной activation;
- UI обещает хранение только в браузере, а remote feed path уже может передать
  profile в RPC;
- открытый юридический PR #266 требует иной state machine;
- дальнейшее расширение такого profile усложнит миграцию и доказательство
  activation evidence.

Требование:

- удалить dialog как production activation path;
- like/hide/interest/personal-mode action сам запускает `activation epoch`;
- notice — информирование, не отдельное разрешение;
- legacy profile импортировать device-local и не считать remote activation.

### P0-2. Browser передаёт compact profile в public Supabase RPC

Фактический path:

- `personalFeedConfig()` читает public Supabase URL/relay/publishable key;
- `compactProfileForRpc()` включает `anon_id`, `session_id`, positive/negative
  maps и capped action/id arrays;
- `fetchPersonalFeedManifest()` POST-ит это в
  `get_listing_personal_feed_v1` через `ResilientDataClient`.

Проблемы:

- transitional public-read design расходится с целевым same-origin private
  profile API;
- browser-supplied `anon_id` не является ownership proof;
- сервер получает profile snapshot вместо typed idempotent actions;
- каждая refresh может стать повторной remote ranking operation;
- current docs одновременно допускают и запрещают этот путь.

Решение:

- public/static catalog остаётся source кандидатов;
- durable actions/profile идут только в same-origin API;
- public RPC, если временно сохранён, принимает только public/non-personal read
  parameters и не является SOR;
- operation catalog не должен классифицировать private personalization через
  обычный read-only RPC.

### P0-3. Нет durable action loop

Есть local `LOG_KEY = 'ke_event_feedback_log_v2'`, но нет:

- bounded action outbox;
- stable idempotency ids/sequences;
- batch ACK/reject;
- ambiguous reconcile;
- server current state;
- materialization queue;
- profile projection revision.

Следствие: цепочка `action → server state → profile revision → следующая page`
не доказуема. Local log — debug artifact, не ledger.

### P0-4. Exact hide смешан с semantic dislike

`updateProfileForAction('not_interested')` одновременно:

- добавляет event в `not_interested_event_ids`;
- добавляет его в `hidden_event_ids`;
- снимает like;
- повышает `negative_interest_tags` кандидата на `0.7`.

Это четыре разных смысла в одном действии. Один скрытый концерт может без
дополнительного evidence понизить весь жанр/категорию.

Требование:

- exact hide — один global tombstone;
- semantic negative — отдельный typed/repeated evidence;
- sensitive/campaign contexts не создают facet;
- `undo` восстанавливает exact state, а не пытается приблизительно вычесть
  предыдущий facet (`-0.35` не является inverse к `+0.7`).

### P0-5. Hide не имеет требуемого pending/undo commit window

Current card сразу записывается и rerank/filter применяется немедленно. Plate
`Отменить` существует, но нет:

- progress/countdown;
- pending state;
- delayed durable commit;
- гарантии сохранения focus/scroll anchor;
- единого hidden collection.

Требование: explicit state machine из implementation contract.

### P0-6. Reset только локальный

`data-reset-personalization` удаляет local keys, записывает timestamp и reload.
Remote profile, actions, identity link, projection и server epoch не
отзываются.

Требование: local reset epoch + idempotent remote reset receipt + purge state.

### P0-7. Scoring размножен внутри layout

В одном inline block одновременно существуют:

- `scoreRelatedCandidate()`;
- `rankEventDetailRelated()`;
- `rankPersonalFeedCandidates()`;
- `rankPopularFallbackCandidates()`;
- собственные diversity/fatigue/profile-map helpers.

Это уже несколько policy в одном файле без versioned surface registry. Новые
подборки неизбежно добавят ещё ветки.

Требование: один pure scorer + declarative surface policy + independent tests.

### P0-8. Нет доказанной сквозности

Runtime подключён через `EventLayout`, но нет generated route inventory,
который доказывает:

- все public HTML families используют layout/runtime;
- runtime ровно один;
- каждая collection зарегистрирована;
- календарная surface не rerank;
- search/popular/related сохраняют собственные eligibility semantics.

Сквозность нельзя выводить из имени layout.

## 3. P1-расхождения

### P1-1. Session id долговечен

`session_id` создаётся вместе с profile и сохраняется в localStorage. Он может
переживать browser session и перестаёт быть session identifier.

Исправление: session id только `sessionStorage`, rotation при новой session,
profile не зависит от него для ownership.

### P1-2. Local state дублирует ids

`not_interested_event_ids` и `hidden_event_ids` часто содержат одно и то же.
`share_counts` хранит per-event map, отдельный log ещё раз хранит event ids.
Это лишние bytes и source-of-truth ambiguity.

Исправление: typed compact exact state + bounded overlay; debug log отключён в
production.

### P1-3. Нет aggregate localStorage budget enforcement

Отдельные arrays/maps capped, но runtime не:

- измеряет общий KenigEvents budget;
- применяет deterministic eviction priority;
- различает quota/corruption/private-mode;
- гарантирует atomic migration;
- выдаёт честный UI при невозможности сохранить strong action.

### P1-4. Served list живёт только в memory

`createServedListSummary()` создаёт полезную структуру, но она не связана с
strong action и не проходит через durable transport. Это нельзя использовать
как доказательство rank outcome.

Решение: отправлять minimal action-bound served hash/rank, не каждый impression.

### P1-5. Нет frozen-prefix engine

`reorderExistingCards()` умеет не двигать clicked card и prefix перед ним в
частном flow, но нет общей IntersectionObserver/focus/visible policy. Другие
rerank вызовы могут менять уже замеченную часть списка.

### P1-6. Profile refresh cadence не соответствует целевой модели

Personal feed использует 30-minute hint/memory cache и при необходимости remote
RPC. Целевая derived projection должна обновляться редко по ETag, а candidates
приходят из public static manifest.

### P1-7. Versioning привязано к event-detail prototype

`FEATURE_SCHEMA_VERSION = 'event-detail-related-v1'` используется и для
listing personal feed. Это затрудняет независимое развитие feature schema,
surface policy и model version.

### P1-8. Transport cache расходует общий localStorage без единого владельца

`resilientSupabaseTransport` хранит route cache, runtime чистит legacy route
keys, а общий storage controller отсутствует. Transport hint должен иметь
собственный bounded namespace/TTL, но участвовать в aggregate budget.

### P1-9. Tests слишком часто проверяют текст исходника regex-ами

Current tests полезны как regression alarms, но многие assertions ищут функции
и строки в `EventLayout.astro`. После extraction их нужно заменить на:

- importable module tests;
- contract fixtures;
- generated route inventory;
- real browser behavior/network/storage evidence.

Regex может остаться только как запрет возвращения giant inline code/legacy
keys.

### P1-10. Документация расходится

`listing-personal-feed.md` называет public Supabase RPC допустимым fallback, а
`personalization-to-be.md` и ownership architecture требуют private/same-origin
boundary. До реализации нужен один normative contract; текущий пакет назначает
таким контрактом `personalization-implementation-contract.md`.

## 4. Что не является дефектом само по себе

- static public candidate manifest до 500 событий — нормальный способ избежать
  per-user server fetch;
- local scoring — целевой паттерн;
- direct/relay transport — полезен, если operation policy и data boundary
  корректны;
- finite continuation вместо infinite feed — осознанная product policy;
- local hint с event ids — допустим при expiry и отсутствии full card copies;
- hard exclusions до scoring — правильно;
- exploration/diversity — нужны, но только после eligibility и exact hide.

## 5. Миграционная граница

### До Wave 0

- не добавлять новые personalization branches/functions в inline script;
- не создавать production profile tables;
- не включать remote profile writes;
- не настраивать model weights по prototype telemetry.

### Wave 0 допускает

- characterization tests;
- extraction pure modules;
- adapter, повторяющий current output;
- surface inventory;
- feature flags `off/characterize/local-shadow`;
- legacy read-only migration parser;
- storage byte-report fixtures.

### Wave 0 не допускает

- видимый redesign;
- новую consent/activation UX до утверждённого state machine;
- DB migration;
- background weak telemetry;
- direct private RPC;
- удаление static fallback;
- переименование prototype в production.

## 6. Обязательная последовательность устранения P0

1. Characterize current behavior and extract modules.
2. Introduce fail-closed surface registry.
3. Replace `consent_ok` with activation epoch and compact atomic state.
4. Separate exact tombstone from semantic evidence; add pending undo.
5. Prove cross-route exact hide/calendar invariants.
6. Add bounded idempotent outbox and wire schemas.
7. Add same-origin API + compact current-state schema.
8. Add materializer/projection/reconcile.
9. Add remote reset/link/delete.
10. Only then tune models and expand surfaces.

## 7. Контрольный список фактической реализации

| Проверка | Current | Release target |
|---|---|---|
| static fallback | есть | сохранить |
| one shared runtime | giant inline partial | extracted shared modules |
| route inventory | нет | 100% public HTML classified |
| activation epoch | нет | server/local versioned |
| local atomic envelope | нет | schema + byte gates |
| session rotation | нет | sessionStorage |
| exact hide typed state | частично/duplicated | one tombstone model |
| pending undo | нет | accessible timed commit |
| hidden collection | нет | restore flow |
| one scorer | нет | pure/versioned |
| calendar no-rerank | не доказано | route/browser invariant |
| zero-network thematic rerank | частично | explicit request-count gate |
| bounded outbox | нет | IDB + fallback |
| idempotent ACK/reconcile | нет | schema-tested |
| direct/relay fault matrix | transport-level partial | personalization E2E |
| same-origin private API | нет | required |
| compact primary state | нет | measured 1k/10k/100k |
| materializer/projection | нет | coalesced + ETag |
| remote reset/link | нет | required |
| sensitive/campaign gate | нет | required |
| Android/iOS/browser critical run | нет для p13n | required before rollout |

## 8. Вердикт

Текущий код **не нужно выбрасывать**: в нём есть полезные card/rendering,
static-catalog и resilience primitives. Но его нельзя расширять как единый
production runtime. Правильный следующий шаг — Wave 0 extraction с
characterization tests и surface registry; добавление backend schema до этого
создаст дорогой dual-contract и закрепит ошибки activation/storage/scoring.
