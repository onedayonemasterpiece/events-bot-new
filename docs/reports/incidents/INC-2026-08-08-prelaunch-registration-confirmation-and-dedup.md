# INC-2026-08-08-prelaunch-registration-confirmation-and-dedup Prelaunch confirmation and dedup gap

Status: monitoring
Severity: sev2
Service: `kenigevents.ru` production root / prelaunch registration
Opened: 2026-08-08
Closed: —
Owners: static-site / personalization database / release owner
Related incidents: —
Related docs: `docs/features/static-site-pages/prelaunch.md`, `docs/operations/release-governance.md`

## Summary

Пользователь сообщил, что после отправки email видел «Сохраняем…», затем снова
доступную форму без постоянного подтверждения. Production также не имел
зафиксированного доказательства server-truth repeat status и race-safe финальной
вставки. Это user-visible production degradation на индексируемом корне.

## User / Business Impact

- пользователь не мог уверенно понять, сохранён ли email;
- повторная отправка не отличалась от первой в backend receipt;
- server-side дедупликация не была закрыта явным final-insert upsert contract;
- отсутствие функции в `main` делало production surface невоспроизводимой через
  стандартный steady-state release.

## Detection

- пользовательский отчёт;
- production Network/console и защищённое чтение таблицы являются обязательным
  evidence, а DOM/localStorage не доказывают запись;
- проверки не публикуют полный email.

## Timeline

- 2026-08-08 — получен отчёт и включён incident workflow.
- 2026-08-08 — production source SHA зафиксирован как
  `9d8fc9203a69f385407a57e23310bb47f2db4e2d`.
- 2026-08-08 — подготовлена main-based reconciliation формы и v3 RPC contract.
- post-deploy timestamps и GitHub Actions run добавляются при closure.

## Root Cause

1. Production prelaunch root был опубликован из side-branch SHA и отсутствовал
   в `origin/main`, поэтому обычный main-only release path не мог исправить и
   регрессионно проверить форму.
2. RPC v2 возвращал `status='registered'` и для первой, и для повторной заявки;
   браузер не мог показать server-truth «Вы уже записаны».
3. Финальная вставка RPC была обычным `INSERT`: уникальность таблицы защищала
   строку, но concurrent conflict не превращался явно в успешный повторный
   receipt `already_registered`.
4. Closure gate не требовал одним browser scenario доказать постоянный success,
   reload, reset, retention on error и повторный submit lock.

Production reproduction evidence определяет точную причину исходного исчезновения
confirmation; этот record нельзя закрывать только по статическому анализу.

## Contributing Factors

- localStorage был UX-hint, но production DB proof не входил в release evidence;
- старый релизный control был привязан к feature branch и историческому SHA;
- одинаковый response status маскировал first/repeat семантику.

## Automation Contract

### Treat as regression guard when

Изменяются prelaunch form/runtime, `register_prelaunch_notification_v1`, таблица
подписок, resilient operation catalog или production root publisher.

### Affected surfaces

- `site/src/scripts/prelaunchForm.ts`;
- `site/src/components/PrelaunchPage.astro`;
- `site/src/lib/prelaunchEmail.ts` и backend operation catalog;
- `supabase/migrations/*prelaunch*`;
- exact-main root release workflow.

### Mandatory checks before closure or deploy

- unit/contract tests формы и email normalization;
- browser first success и exact «Готово, вы записаны»;
- repeat success и exact «Вы уже записаны»;
- registered-after-reload и explicit reset;
- retained input on error и in-flight duplicate-submit lock;
- production double-submit + protected DB count = 1 + synthetic cleanup;
- visual/background/assets and SEO/GEO byte baseline unchanged;
- deploy SHA reachable from `origin/main`.

### Required evidence

- exact deployed SHA and GitHub Actions run;
- masked operator query result for user-provided row(s);
- masked synthetic row evidence after two real form calls;
- production browser success and reload state;
- cleanup confirmation for synthetic row.

## Immediate Mitigation

Reconcile only the already deployed root/form closure into `main`; do not change
visual assets, background, SEO/GEO or unrelated site behavior.

## Corrective Actions

- v3 returns `registered` vs `already_registered`;
- unique-index + `ON CONFLICT(email) DO NOTHING` closes concurrent insert;
- client maps server receipt to permanent distinct state;
- runtime in-flight lock complements disabled CTA;
- artifact-bound browser checker covers the complete form state machine;
- exact-main manual root release control restores release governance.

## Follow-up Actions

- [ ] release owner: attach production GitHub Actions run and deployed SHA.
- [ ] operator: attach masked user-row and synthetic two-call/one-row evidence.
- [ ] incident owner: close only after live success/reload and cleanup checks pass.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending exact-main manual GitHub Actions root release
- regression checks: local implementation gates pending commit evidence
- post-deploy verification: pending

## Prevention

Server-truth response states, explicit conflict handling, an in-flight browser
lock and one exact-main browser release gate are now regression contracts.
