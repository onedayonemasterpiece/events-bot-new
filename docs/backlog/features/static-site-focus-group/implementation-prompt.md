# Prompt для отдельного окна: реализация focus-group release

> **Scope update, 27.07.2026:** текущая integration-ветка реализует только
> page/product prototype из
> [product-prototype.md](product-prototype.md). Ниже сохранён исходный prompt
> будущей production-реализации; его Supabase/Auth/email/deploy gates в этой
> ветке намеренно не выполняются и не считаются закрытыми.

Скопируй текст ниже в отдельное окно Codex.

---

Ты работаешь в `/home/dev/projects/events-bot-new`.

## Цель

Подготовить **закрытый focus-group release статического KenigEvents сайта на
30.07.2026**, а не публичный релиз. Целевой cohort — максимум 200 verified
участников, рекомендуемый период — 30 дней. Реализовать P0-контур end-to-end,
не смешивать его с дальнейшим public-root релизом.

Канонические входы:

- `docs/backlog/features/static-site-focus-group/README.md`
- `docs/backlog/features/static-site-focus-group/current-state-audit.md`
- `docs/features/static-site-pages/release-plan.md`
- `docs/features/static-site-pages/presentation-release-checklist.md`
- `docs/architecture/personalization-data-ownership.md`
- `.codex/skills/events-bot-dual-db/SKILL.md`
- Supabase skill и актуальная официальная документация Supabase.

## Неподвижные решения

1. `origin/main` — единственный source of truth. Основной checkout грязный:
   работай из новой integration branch/worktree от свежего `origin/main`.
2. Не выпускать сайт на всю аудиторию и не объявлять canonical-root GO.
   Focus candidate — новый immutable `noindex/no-referrer` build. Opaque URL не
   называть авторизацией.
3. Fly SQLite владеет событиями/Smart Update/publication/jobs. Supabase владеет
   Auth, tester membership/invites, consent, feedback, compact personal state и
   email control. Large private reports — Object Storage. Не создавать YDB/Kaggle
   contour для 200 пользователей и 30 дней.
4. Verified email обязателен для active tester. Реализовать Supabase email OTP и
   magic link как один identity flow. Yandex Auth остаётся альтернативой.
5. Tester mode передаётся только bounded invite, а не сессия: 256-bit token,
   fragment URL consumed and stripped by the client, HMAC at rest,
   expiry/revoke/max uses, own verified identity, atomic global cap 200,
   idempotent redeem. Не использовать user-editable `user_metadata` как
   authorization source и не писать raw token в HTTP/application logs.
6. После `active_until` tester становится `alumni`; Auth account и явно
   сохранённые события остаются обычному пользователю. Tester mail/invites/
   privileges прекращаются. Marketing consent не возникает.
7. Page-family score — usefulness `0..10`, не выдавать его за standard NPS.
   Standard overall NPS — weekly/exit. Sampling: максимум один ответ на family
   за 7 дней, максимум два auto-prompts/day; improvement action доступен всегда.
8. Разделить feedback kinds: `surface_score`, `improvement`,
   `event_fact_issue`. Factual issue не меняет event автоматически и не доверяет
   client snapshot; canonical repair проходит human triage/Smart Update/incident
   contract.
9. Один idempotent daily analysis job: watermark/input hash, PII redaction before
   LLM, bounded clustering, human triage, не более одной task на theme. Critical
   event issues видимы сразу. Повторный run — no-op.
10. Weekly mail — отдельные `focus_group_research` consent и
    `focus_group_weekly` stream/kind через существующий email control/NotiSend.
    Не переиспользовать exact-three recommendation contract. Нужны unsubscribe,
    suppression, provider id/status evidence, seed canary и kill switch.
    Research review — явное условие active tester membership: отказ не мешает
    пользоваться обычным сайтом, а последующий отзыв переводит member в
    `alumni`; marketing/recommendation consent остаётся независимым.
11. Персонализация к launch — существующий bounded local prototype, явно
    помеченный `Прототип`, с static fallback/holdout и `Почему это`. Не строить
    online ML backend.
12. PWA install/icon/compact telemetry уже в main. Не переписывать. Проверить на
    exact candidate и вывести cohort aggregate; не связывать старый anonymous
    installation UUID с email без consent.
13. Пасхалки к launch — non-prize orientation canary либо off. Не делать
    feedback/positive score/invites/share условием или multiplier выигрыша.
    Prize release только после отдельных правил/legal/privacy/anti-abuse.
14. Vertical event video — optional canary, не GO blocker. Только если есть 1–3
    approved clips/posters: allowlist sidecar, optional projection, poster grid,
    click-only accessible story dialog, `preload=none`, no autoplay/sound, no
    initial bytes, broken-media fallback. Иначе оставить post-release.
15. Не merge wholesale: stale CTA branch, stale ArtKodex branch, old
    `docs/static-site-video-guides-20260718`, old artifact registry branch.

## Сначала обязательный discovery/fanout

Используй feature-fanout. Создай requirement matrix и lane map. Параллельно:

- lane A: Supabase/Auth/membership/invites/email verification;
- lane B: feedback/NPS/daily analysis/ArtKodex-safe bridge;
- lane C: frontend tester shell/onboarding/PWA/personalization;
- lane D: weekly email control/NotiSend;
- lane E: release integration/E2E/security/rollback;
- optional lane F: 1–3 video canary only after owner assets/rights are present.

Writable lanes — отдельные branches/worktrees, disjoint ownership. Интегратор
cherry-pick/merges into one main-based integration branch. Reviewer проверяет
все original requirements.

## Gate 0: устранить обнаруженный live schema drift

Live Supabase имеет migration history
`20260727151208 durable_saved_events_v1_20260727`; R15 имеет candidate file
`supabase/migrations/20260727141820_durable_saved_events_v1.sql`.

До любой новой migration:

1. read-only inspect live definitions/policies/grants/functions and hash them;
2. compare with R15 candidate SQL;
3. create a restorable backup and migration reconciliation plan;
4. materialize canonical repository migration without double-applying objects;
5. run SQL contract tests, RLS negative tests and advisors;
6. wire durable saved state only after exact reconciliation.

Не менять live DB наугад и не объявлять drift закрытым по наличию таблицы.

## Минимальная schema

Private `focus_group` schema, no raw browser table access:

- `program(id, starts_at, ends_at, status, capacity=200, active_count,
  terms_version)`;
- `membership(user_id PK, program_id, status, joined_at, active_until,
  invited_by, verified_email_at, referral_quota, terms_version)`;
- `invite(token_hmac PK, key_version, program_id, issuer_user_id nullable,
  max_uses, used_count, expires_at, revoked_at)`;
- `member_page_daily(user_id, metric_date, page_family, release_id, views,
  active_seconds, useful_actions, saves, feedback_count)` with compact UPSERT;
- `feedback_item(id, user_id, client_request_id UNIQUE, kind, page_family,
  event_id nullable, score nullable, tags bounded, body<=2000,
  snapshot_hash, release_id, status, created_at)`;
- `feedback_daily_analysis(analysis_date PK, input_watermark, input_hash,
  status, bounded counts/themes, private_report_path)`;
- extend current `email_control` with purpose `focus_group_research` and outbox
  kind/stream `focus_group_weekly`; no parallel mail DB.

Implement narrow authenticated RPCs. Revoke `PUBLIC`; explicit grants; fixed
`search_path`; `auth.uid()` + active membership; RLS owner policies;
idempotency/rate/cap before inserts. Member privilege checks `active_until`
synchronously, not only by cron.

## P0 deliverables before first seed user

1. Email OTP/magic-link UI, callback/session recovery and real E2E.
2. Focus programme, member/invite/redeem/withdraw/expire, seed QR, one referral,
   atomic capacity and concurrency tests.
3. Focus terms/privacy/retention and separate consents.
4. Shared feedback trigger and accessible sheet on all required page families.
5. Separate typed event factual-error flow with authoritative server snapshot,
   operator receipt and no automatic DB mutation.
6. Daily compact aggregation/analysis + PII redaction + human triage + rerun
   idempotency + kill switch.
7. Honest local personalization prototype on `/dlya-menya/`, static fallback,
   experiment assignment and bounded metrics.
8. PWA install/standalone metrics verified on exact candidate.
9. Create and live-test `tester@kenigevents.ru`; never publish an unprovisioned
   address or silently substitute `info@`.
10. One new frozen main-reachable immutable candidate, current DB snapshot,
    freshness and rollback drill, real OAuth/Search, mobile/desktop/a11y/no-JS,
    RLS/abuse and full tester journey E2E.

## Required before first weekly send

- weekly issue/template + per-member compact stats;
- narrow NotiSend worker, purpose consent, unsubscribe/leave, suppression;
- rotate/reissue the previously exposed NotiSend key or record explicit security
  acceptance; seed send, delivery evidence and rollback;
- never send raw history/inferred interests or claim a fix without linked evidence.

## Must-pass acceptance

- anonymous/nonmember cannot call tester RPCs;
- concurrent invite redemptions never exceed 200;
- token replay is idempotent; после client consumption token не остаётся в
  address bar/history/referrer и никогда не попадает в HTTP/application logs;
- expired/withdrawn member loses tester controls without deleting account/saves;
- email verification code and link recover the same identity;
- score sampling and upsert enforce one family/period answer;
- feedback retry produces one row; rate/body limits fail closed;
- critical event issue appears immediately, daily run remains exactly once;
- LLM input/logs contain no direct email/phone/token;
- email send requires exact consent and suppression recheck;
- personalization/PWA/feedback outages leave static pages usable;
- focus candidate remains noindex and root/current objects remain unchanged;
- rollback returns last-good candidate;
- every test produces SHA/build/run/evidence.

## Scope discipline

If P0 cannot be proved by freeze, reduce the first wave—not the security/data
contract. Optional videos, prize mechanics, participant likes and polished
weekly automation are the first items to defer. Do not disguise missing backend
as UI success.

Update canonical docs and `CHANGELOG.md`, commit/push each durable lane, produce
integration report and final matrix `Done|Partial|Missing|Blocked` for every
requirement.

---
