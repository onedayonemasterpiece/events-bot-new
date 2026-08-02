# Автотесты как release gate статического сайта

> **Статус:** нормативный companion к [`release-plan.md`](release-plan.md).
> Этот документ не создаёт второй release plan. Он определяет, какие
> автоматизированные доказательства нужны для закрытия соответствующих gates.
> Полная стратегия: [`../../operations/static-site-autotest-strategy.md`](../../operations/static-site-autotest-strategy.md).

## 1. Release truth

Release truth остаётся `origin/main` + exact immutable candidate identity.
Локальный checkout, side branch, mobile viewport screenshot и незавершённый
background run не закрывают release gate.

Каждый release evidence record должен содержать:

- full repository SHA;
- build/snapshot/tree identity;
- exact target;
- suite/scenario/platform;
- selector reason;
- PASS/FAIL/BLOCKED;
- artifact/run link;
- redaction result;
- disposition для advisory/background signals.

## 2. Обязательные gates по типу изменения

| Release surface | Обязательное доказательство |
|---|---|
| Artifact/data/exporter | L0 full affected contract + browser sample |
| Event/listing route layout | L0 + L1 affected route families + frozen geometry fixtures |
| Full catalog publication | full L0 catalog + sharded L1 route health |
| Input/focus/keyboard | L1 + Android Emulator + iOS Simulator critical scenario |
| PWA manifest/install/start URL/scope/SW | L0 + L1 + Android/iOS system integration |
| Focus onboarding/Auth/OTP | existing browser OTP + Android browser-tab OTP + iOS browser-tab OTP |
| Supabase/Yandex route change | direct/relay contracts + affected browser/mobile journey |
| Personalization/personal pages | no-leak/data contract + authenticated browser journey; mobile sample when UI/input changes |
| Event Push subscription/reminder scheduler | L0 outbox/idempotency + L1 + Android/iOS L2; L3 background canary before enablement |
| Postbox calendar invitation | L0 MIME/UID/SEQUENCE + protected Postbox/mailbox roundtrip + client matrix before product enablement |
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

- full catalog crawl после локального affected pass;
- expanded visual sample;
- Android/iOS nightly при data-only изменении;
- cross-browser extended matrix.

Handoff обязан назвать run как `STARTED_BACKGROUND`, указать run ID/URL, SHA и
scenario set. Такой run не является PASS. Перед release promotion все связанные
signals должны иметь terminal result и disposition.

### Protected manual

- real mailbox OTP;
- calendar invitation REQUEST/update/CANCEL sequence;
- fresh-user identity;
- production write probe;
- paid device-cloud L3.

Эти jobs используют защищённый Environment, bounded concurrency и отдельный
side-effect contract. Secrets не передаются browser catalog или visual jobs.

## 4. Первый release milestone

Первый законченный mobile milestone — не общий framework всех страниц, а
модификация существующего isolated focus-group OTP harness:

1. сохранить текущий Chromium + IMAPS baseline;
2. выделить shared semantic journey;
3. добавить Android Emulator + Chrome + реальную keyboard acceptance;
4. добавить iOS Simulator + Mobile Safari + реальную keyboard acceptance;
5. выполнять real-mail variants последовательно;
6. сохранить one issue / one verify / one participant registration;
7. выпустить одинаковый sanitized evidence contract;
8. не включать PWA install/relaunch в этот же первый PR.

До terminal PASS Android и iOS новый OTP transport нельзя объявлять доказанным
для переноса на остальные authorized static pages.

## 5. Отдельный PWA gate

После browser-tab OTP добавляется `focus.otp.installed_pwa`:

- Android Chrome install UI → Launcher → standalone → relaunch;
- iOS Safari Share Sheet → Add to Home Screen → SpringBoard → relaunch;
- stable manifest `id`, `scope`, `start_url`;
- persisted participant state;
- честное network-only поведение service worker.

Offline content availability не является текущим обязательством и не должна
появляться как ложный release gate.

## 6. Page/data rollout

Сценарии добавляются поступательно вместе с реализацией или аудитом surface:

- route non-empty/content minimum;
- transport blocks;
- venue/source medallions;
- people/headliner/celebrity cards;
- authenticated pre-generated `Для меня` pages;
- Supabase direct/relay и Yandex connectivity;
- personalization ordering/feedback;
- expected block content and typed empty states.

`planned` не превращается в blocking до появления product contract. При
переходе в `implemented` одновременно обновляются machine-readable registry,
реализующий test, release gate и evidence sample.

## 7. NO-GO

Release blocked, если:

- mobile-sensitive code изменён, а required Android/iOS result отсутствует;
- OTP result FAIL/BLOCKED либо target SHA не совпал;
- mandatory background run ещё не terminal;
- evidence содержит PII/OTP/token или не прошёл redaction;
- full catalog имеет unexplained empty/broken route;
- simulator run подменён desktop mobile viewport/WebKit;
- planned test представлен как passed implementation;
- один fixed mailbox используется параллельно несколькими real OTP jobs;
- hardcoded event URL устарел и scenario не использовал current-event resolver;
- selected event переключился после первого Push/email side effect;
- emulator environment smoke выдан за Push/calendar PASS;
- Postbox provider acceptance выдан за calendar-client recognition;
- ICS download выдан за внешнее calendar save.

## 8. Экономический guardrail

- не запускать iOS/macOS для data-only PR;
- не открывать весь каталог на эмуляторах;
- сначала L0/L1, затем L2;
- screenshots/video only-on-failure или для selected specimens;
- real OTP и calendar-email sequence только явно и последовательно;
- один bounded retry только для инфраструктурного flake;
- deterministic gates решают release, AI visual review помогает triage.

## 9. Event reminders and calendar delivery

Канонический test design:
[`../../testing/event-reminders-calendar-e2e.md`](../../testing/event-reminders-calendar-e2e.md).

### Dynamic current-event prerequisite

Каждый production-like reminder/calendar run сначала выполняет
`event.current_event.selection`:

- exact deployed HTTPS target и full repo SHA;
- current listing routes того же build prefix;
- adjacent `event.ics`;
- timed future event с UID/title/location;
- immutable `selected-event.json` для downstream jobs;
- revalidation до первого side effect.

После первого Push/email side effect event нельзя тихо заменить новым.

### Push

После реализации channel release требует:

- `event.reminder.push_subscription`;
- `event.reminder.push_delivery`;
- `event.reminder.lifecycle`;
- exact-once kinds T−24h/T−1h;
- server-side test clock для CI, не browser-supplied time;
- L2 notification permission/UI/click-through;
- L3 Android OEM и real-iPhone background canary до общего enablement.

### Calendar email

Postbox Raw transport проверяется отдельно от mail-client interpretation:

1. `event.calendar_email.postbox_mime` — deterministic MIME/UID/SEQUENCE;
2. `event.calendar_email.postbox_roundtrip` — one protected REQUEST/update/CANCEL sequence;
3. `event.calendar_email.client_action` — Gmail/Apple Mail/Outlook matrix.

NotiSend не становится hidden fallback от Postbox и не входит в release gate без
отдельного architecture decision.

### Android connector

`event.calendar_connector.android` требует:

- signed stable package;
- verified App Links;
- no calendar/storage permissions;
- current event payload from allowlisted first-party endpoint;
- native `ACTION_INSERT` editor field assertions;
- web fallback when connector absent;
- real-device/OEM canary before public distribution.
