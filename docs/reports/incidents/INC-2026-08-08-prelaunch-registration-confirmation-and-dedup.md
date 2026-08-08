# INC-2026-08-08-prelaunch-registration-confirmation-and-dedup Prelaunch confirmation and dedup gap

Status: closed
Severity: sev2
Service: `kenigevents.ru` production root / prelaunch registration
Opened: 2026-08-08
Closed: 2026-08-08T15:11:18Z
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
- 2026-08-08 — production reproduction: реальный POST RPC вернул HTTP 400 / PostgreSQL `22023` / `invalid_prelaunch_consent`; защищённый `pg_get_functiondef` показал только `launch-2026-09-01-v1`, а migration history не содержала v2.
- 2026-08-08 — существующая v2 migration применена транзакционно; тот же реальный form call вернул HTTP 200, показал first-success, а reload — registered.
- 2026-08-08 — подготовлена main-based reconciliation формы и v3 RPC contract.
- 2026-08-08T15:09:02Z — exact-main release
  `prelaunch-main-31263560430-1` опубликовал SHA
  `5a9d804438377f65fe4b26bd7019e73626529864`.
- 2026-08-08T15:09:56Z — первый реальный production submit вернул
  `registered`; reload показал registered-state; второй submit того же адреса
  вернул `already_registered`.
- 2026-08-08T15:10Z — защищённое чтение доказало одну строку и
  `request_count=2`; синтетическая строка удалена, контрольный count равен 0.

## Root Cause

1. Опубликованный browser bundle отправлял consent
   `prelaunch-updates-2026-v1`, но production RPC оставался на v1-контракте
   `launch-2026-09-01-v1`. Каждый реальный submit доходил до backend и получал
   HTTP 400 / `22023 invalid_prelaunch_consent`; client корректно возвращал
   форму в error state вместо ложного success.
2. Production migration history не содержала существующую repo migration v2,
   поэтому source/runtime и database function contract разошлись.
3. Production prelaunch root и обе prelaunch migrations находились только в
   side-branch lineage, а не в `origin/main`; steady-state main release не мог
   воспроизвести, проверить или исправить drift.
4. RPC v2 возвращал `status='registered'` и для первой, и для повторной заявки,
   поэтому даже после восстановления consent contract браузер не получал
   server-truth repeat receipt.
5. Финальная вставка RPC была обычным `INSERT`: verified `UNIQUE(email)` не
   допускал две строки, но concurrent conflict не превращался явно в успешный
   `already_registered` receipt.

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
- verified `UNIQUE(email)` + `ON CONFLICT(email) DO NOTHING` closes concurrent insert;
- client maps server receipt to permanent distinct state;
- runtime in-flight lock complements disabled CTA;
- artifact-bound browser checker covers the complete form state machine;
- exact-main manual root release control restores release governance.

## Follow-up Actions

- [x] release owner: production GitHub Actions run and deployed SHA attached.
- [x] operator: masked user-row audit and synthetic two-call/one-row evidence attached.
- [x] incident owner: live success/reload and cleanup checks passed.

## Release And Closure Evidence

- deployed SHA: `5a9d804438377f65fe4b26bd7019e73626529864`, exact
  `origin/main` at release time and verified as its ancestor after release;
- deploy path: exact-main manual GitHub Actions
  [run 31263560430](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31263560430),
  successful; release receipt `prelaunch-main-31263560430-1`;
- production schema: migrations `20260803113000`, `20260806163000` and
  `20260808143744` present; the live RPC contains `already_registered` and
  `ON CONFLICT (email) DO NOTHING`; exactly one `UNIQUE(email)` constraint;
  table RLS remains enabled and `anon`/`authenticated` have no table SELECT;
- operator audit requested by the reporter: an actual protected table read for
  `source='prelaunch_home'` after `2026-08-08T05:29:24Z` returned 0 rows for
  both `first_requested_at` and `last_requested_at`; therefore there is no
  masked customer row or associated timestamp/count/status to report;
- real synthetic proof: `s***@e***.test` submitted twice through the stable
  production form; one row, `request_count=2`,
  `first_requested_at=2026-08-08T15:09:56.023762Z`,
  `last_requested_at=2026-08-08T15:09:57.035180Z`,
  `consent_version=prelaunch-updates-2026-v1`,
  `delivery_status=pending`; cleanup deleted one row and the verification count
  was 0;
- production browser proof: two POSTs total, both HTTP 200; first receipt
  `registered` displayed exact «Готово, вы записаны» and hid the form; submit
  was disabled in flight; reload displayed exact «Вы уже записаны»; only
  «Другой e-mail» restored the form; second receipt `already_registered`
  displayed «Вы уже записаны»; console errors: 0;
- regression checks: prelaunch unit/contract tests 7/7; exact-artifact and live
  form gates passed first/repeat/reload/reset/error-retention/double-submit
  scenarios; PR #373 and rollback follow-up PR #375 were green before merge;
- visual/SEO closure: live CSS and all four approved image hashes match the
  pre-fix baseline; title, canonical, description, robots and absence of
  `noindex` all match. Only root HTML/JS changed for the form behavior.

## Prevention

Server-truth response states, explicit conflict handling, an in-flight browser
lock and one exact-main browser release gate are now regression contracts.
