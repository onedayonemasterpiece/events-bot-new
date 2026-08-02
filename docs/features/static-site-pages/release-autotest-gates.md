# Автотесты как release gate статического сайта

> **Статус:** нормативный companion к [`release-plan.md`](release-plan.md).
> Этот документ не создаёт второй release plan. Он определяет, какие
> автоматизированные доказательства нужны для закрытия соответствующих gates.
> Полная стратегия: [`../../operations/static-site-autotest-strategy.md`](../../operations/static-site-autotest-strategy.md).

## 1. Release truth

Release truth остаётся `origin/main` + exact immutable candidate identity.
Локальный checkout, side branch, mobile viewport screenshot и незавершённый
background run не закрывают release gate.

Каждый release evidence record содержит:

- full repository SHA;
- build/snapshot/tree identity;
- exact target;
- suite/scenario/platform;
- selector reason;
- PASS/FAIL/BLOCKED;
- artifact/run link;
- redaction result;
- disposition advisory/background signals.

## 2. Обязательные gates по типу изменения

| Release surface | Обязательное доказательство |
|---|---|
| Artifact/data/exporter | L0 full affected contract + browser sample |
| Event/listing route layout | L0 + L1 affected route families + frozen geometry fixtures |
| Full catalog publication | full L0 catalog + sharded L1 route health |
| Input/focus/keyboard | L1 + Android Emulator + iOS Simulator critical scenario |
| PWA manifest/install/start URL/scope/SW | L0 + L1 + Android/iOS system integration |
| Focus onboarding/Auth/OTP | browser OTP + Android browser-tab OTP + iOS browser-tab OTP |
| Supabase/Yandex route change | direct/relay contracts + affected browser/mobile journey |
| Personalization/personal pages | no-leak/data contract + authenticated browser journey |
| Favorites two-zone surface | L0 independent calendar/like state + L1 compact agenda above large liked cards; once per zone |
| Event Push subscription/preferences/scheduler | L0 preferences/outbox/idempotency + L1 + Android/iOS L2; L3 background canary before enablement |
| Promo campaign Web Push activity | L0 campaign/consent/caps + L1 + Android/iOS L2; focus canary and L3 before public enablement |
| Focus event-delivery orientation | two-zone Favorites + permission-neutral reminder/promo journeys + ordinary feedback path |
| Postbox calendar invitation | L0 MIME/UID/SEQUENCE + protected Postbox/mailbox roundtrip + client matrix before enablement |
| Android Calendar Connector | L0 manifest/App Links/permissions + L1 fallback + Android native editor L2 + bounded L3 OEM canary |
| Data-only copy/facts update | no mandatory emulator unless it changes a mobile-critical component |

## 3. Blocking, background и manual

### Blocking

Агент ждёт terminal result до handoff и release не продолжается:

- affected contracts;
- changed feature browser smoke;
- Android/iOS при прямом изменении mobile-system contract;
- protected real OTP при promotion Auth/onboarding/mail-routing change;
- protected calendar-email roundtrip при изменении активного invitation route;
- evidence redaction gate.

### Background advisory

Можно запустить и не ждать в текущем PR:

- full catalog crawl после affected pass;
- expanded visual sample;
- Android/iOS nightly при data-only изменении;
- cross-browser extended matrix.

Handoff обозначает такой run как `STARTED_BACKGROUND`, указывает run ID/URL,
SHA и scenario set. До release promotion все обязательные background signals
должны иметь terminal result и disposition.

### Protected manual

- real mailbox OTP;
- calendar invitation REQUEST/update/CANCEL sequence;
- fresh-user identity;
- production write probe;
- paid device-cloud L3.

Эти jobs используют protected Environment, bounded concurrency и отдельный
side-effect contract. Secrets не передаются catalog/visual jobs.

## 4. Первый mobile milestone

Первый законченный mobile milestone — расширение существующего focus OTP harness:

1. сохранить Chromium + mailbox baseline;
2. выделить shared semantic journey;
3. добавить Android Emulator + Chrome + keyboard acceptance;
4. добавить iOS Simulator + Mobile Safari + keyboard acceptance;
5. выполнять real-mail variants последовательно;
6. сохранить one issue / one verify / one participant registration;
7. выпустить единый sanitized evidence contract;
8. не смешивать PWA install/relaunch в тот же первый PR.

До terminal PASS Android/iOS OTP transport не считается доказанным для других
authorized pages.

## 5. Отдельный PWA gate

`focus.otp.installed_pwa`:

- Android Chrome install UI → Launcher → standalone → relaunch;
- iOS Safari Share Sheet → Add to Home Screen → SpringBoard → relaunch;
- stable manifest `id`, `scope`, `start_url`;
- persisted participant state;
- честное network-only service-worker поведение.

Offline content availability не является текущим обязательством.

## 6. Page/data rollout

Сценарии добавляются поступательно:

- route non-empty/content minimum;
- transport blocks;
- venue/source medallions;
- people/headliner cards;
- authenticated personal pages;
- Supabase direct/relay;
- personalization ordering/feedback;
- typed empty states.

`planned` не превращается в blocking до появления product contract. При
переходе в `implemented` одновременно обновляются registry, реализующий test,
release gate и evidence sample.

## 7. Stage 14: «Не пропустить»

Этот раздел является evidence companion к Stage 14 общего
[`release-plan.md`](release-plan.md), а не самостоятельным release plan.

Исходные требования:
[`schedule-user-requirements.md`](schedule-user-requirements.md).

Product strategy:
[`event-reminders-calendar-strategy.md`](event-reminders-calendar-strategy.md).

Test design:
[`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md).

### 7.1. Dynamic current-event prerequisite

Каждый production-like run начинает `event.current_event.selection`:

- exact deployed HTTPS target/full repo SHA;
- current listings того же build prefix;
- adjacent event ICS;
- timed future event с UID/title/location;
- immutable `selected-event.json`;
- revalidation до первого side effect.

После первого Push/email side effect event нельзя заменить новым.

### 7.2. Двухзонное «Избранное»

После реализации обязательны:

- `event.saved_calendar_view`;
- compact `Мой календарь` расположен первым;
- строка `время–время | мероприятие | локация` находится в правильной date group;
- ниже начинаются крупные canonical event cards `Понравилось`;
- same event may appear once per zone when both independent states are true;
- repeat save/like не создаёт duplicate внутри зоны;
- снятие одного состояния не снимает другое;
- reschedule/cancel/unknown-field/independent-empty-state fixtures;
- Push off не удаляет ни calendar row, ни liked card.

### 7.3. Utility reminder Push

После реализации обязательны:

- `event.reminder.push_subscription`;
- `event.reminder.preferences`;
- `event.reminder.push_delivery`;
- `event.reminder.lifecycle`;
- exact-once T−24h + ровно один near kind:
  - T−1h для текущего города;
  - T−3h для другого города;
- server-owned current-city source;
- unknown city typed fail-closed до owner decision;
- server-side test clock для CI;
- native permission/UI/click-through L2;
- L3 Android OEM + real-iPhone background canary.

### 7.4. Promo campaign Web Push

После реализации `promo.web_push.activity` требует:

- active `promo_campaign` + grounded future target;
- explicit `promo_push` consent, independent from reminder consent;
- separate campaign/activity outbox and idempotency;
- send window/caps/disclosure;
- campaign pause/archive invalidates only promo jobs;
- provider accepted/displayed/opened are separate states;
- first bounded focus canary before any public enablement.

### 7.5. Focus-group acceptance

`focus.event_delivery.orientation` runs only after focus onboarding/PWA
continuity is healthy. It verifies the two Favorites zones, permission-neutral
utility reminder journey, optional separate promo opt-in and the ordinary focus
feedback path. Deny/skip is a valid outcome and does not affect programme or
prize scoring. Focus PASS is product evidence, not automatic public enablement.

### 7.6. Calendar email

Postbox Raw transport проверяется отдельно от client interpretation:

1. `event.calendar_email.postbox_mime` — deterministic MIME/UID/SEQUENCE;
2. `event.calendar_email.postbox_roundtrip` — one protected REQUEST/update/CANCEL sequence;
3. `event.calendar_email.client_action` — Gmail/Apple Mail/Outlook matrix.

NotiSend остаётся comparison-only и не становится hidden fallback без отдельного
architecture decision.

### 7.7. Android connector

`event.calendar_connector.android` требует:

- signed stable package;
- verified App Links;
- no calendar/storage permissions;
- current event payload from allowlisted first-party endpoint;
- native `ACTION_INSERT` editor field assertions;
- web fallback without connector;
- real-device/OEM canary before public distribution.

## 8. NO-GO

Release blocked, если:

- mobile-sensitive code изменён без required Android/iOS result;
- OTP result FAIL/BLOCKED либо target SHA не совпал;
- mandatory background run не terminal;
- evidence содержит PII/OTP/token или не прошёл redaction;
- full catalog имеет unexplained empty/broken route;
- simulator подменён desktop viewport/WebKit;
- planned test представлен как PASS;
- один fixed mailbox используется параллельно;
- hardcoded event URL заменил current-event resolver;
- selected event переключился после side effect;
- browser supplied authoritative event time/revision/current city;
- T−1h и T−3h near kinds созданы одновременно;
- выключенный reminder type продолжает создавать jobs;
- Favorites не содержит compact calendar сверху и large liked cards ниже;
- cross-zone reuse ошибочно дедуплицирован либо внутри зоны есть duplicate;
- reminder consent использован как promo consent;
- paused/archived campaign продолжает слать promo Push;
- focus mission требует permission grant/положительную оценку или влияет на prize;
- environment smoke выдан за Push/calendar PASS;
- Postbox acceptance выдан за client recognition;
- ICS download выдан за external calendar save.

## 9. Экономический guardrail

- не запускать iOS/macOS для data-only PR;
- не открывать полный каталог на эмуляторах;
- сначала L0/L1, затем L2;
- screenshots/video only-on-failure или для specimens;
- real OTP/calendar-email только явно и последовательно;
- один bounded retry для infrastructure flake;
- deterministic gates решают release, AI-review помогает triage.
