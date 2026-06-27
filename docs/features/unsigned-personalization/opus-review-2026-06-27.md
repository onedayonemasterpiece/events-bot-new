# Architecture Gate Review: KenigEvents Anonymous Personalization + Static Event Pages

> **Reviewer:** External senior product/system architect (Opus-level)
> **Date:** 2026-06-27
> **Scope:** `docs/features/unsigned-personalization/*`, `docs/features/static-site-pages/*`, `static_site/personalization/personalization.js`, `tests/playwright/static_personalization_contract.spec.ts`
> **Verdict:** ⚠️ **Engineering spike ready. NOT canary-ready. NOT production-ready.**

> **Model/source:** Opus via Antigravity/agy command `a-opus`. Gemini Pro review remains blocked by provider `429 RESOURCE_EXHAUSTED`; Gemini Flash-Lite output is supplementary probe material only.
> **Raw artifact:** `artifacts/codex/unsigned-personalization-review-2026-06-27/a_opus_architecture_gate_review_2026-06-27.md` (not committed).

---

## 1. Executive Summary

Проектирование грамотно спроектировано на уровне *intent* — разделение Fly SQLite / Supabase, static-first, LLM offline-only, manifest-first — это правильные решения. Но между документацией и исполняемым кодом зияет gap: всё, что критично для production safety (grants, RLS, quotas, kill-switch, retention), существует только в виде прозы.

**Главный риск:** система описана как готовая, но на деле у неё нет ни одного SQL-теста, ни одной миграции, ни одного deployed endpoint. Supabase-таблицы не созданы. `same_origin_endpoint_v1` — только имя без кода. Документация находится в состоянии «декларативного оптимизма».

---

## 2. Verdict Table

| Область | Статус | Обоснование |
| --- | --- | --- |
| **Engineering spike** | ✅ Ready | Scope narrow (`event_detail_related`), static fallback works, LLM out of hot path, client JS functional |
| **Canary** | 🔴 P0 blocked | Нет SQL миграций, нет grants/RLS enforcement, нет quota guards, нет kill-switch |
| **Production** | 🔴 P0 blocked | Write path, retention, abuse handling, observability, product quality gates — всё design-only |
| **Recommendation quality** | ⚠️ Unproven | Probe golden-smoke, не editorial; negative-interest taxonomy warnings не resolved |
| **Bot resistance** | ⚠️ Designed only | Same-origin endpoint не существует; RPC bot detection — design fiction |
| **SEO/static pages** | ⚠️ Spec only | Astro SSG, sitemap, JSON-LD — описаны, ни одна страница не сгенерирована |

---

## 3. Что спроектировано правильно (и я с этим НЕ спорю)

1. **Static-first architecture.** HTML генерируется из Fly SQLite, персонализация — enhancement поверх готовой страницы. Правильно. Это единственный путь для SEO + resilience на shared-cpu.

2. **Dual DB boundary.** Fly SQLite = source of truth для events. Supabase = only personalization telemetry. Запрет прямых инсертов из браузера в таблицы. Правильно.

3. **Smart Update firewall.** Персонализационная телеметрия не влияет на dedup/match/extraction. Feedback loops явно запрещены. Это critical design decision, и оно корректно.

4. **LLM offline-only.** Никаких моделей в hot path. Enrichment — batch. Правильно для shared-cpu.

5. **Manifest-first client pattern.** Чтение same-origin JSON, scoring в браузере, запись через gated endpoint. Правильный паттерн для static-first + anonymous personalization.

6. **Client JS implementation.** [personalization.js](../../../static_site/personalization/personalization.js) — чистый, defensive, с UUID validation, profile version checks, served-list deduplication, resize debounce. Качество кода клиентской части выше среднего.

---

## 4. P0 Blockers (блокируют canary)

### P0.1 — SQL Grants и RLS не существуют как исполняемый код

**Проблема:** [database.md](../../../docs/features/unsigned-personalization/database.md) описывает `REVOKE ALL`, RLS policies, `SECURITY DEFINER` — но нет ни одной SQL миграции в `supabase/migrations/`. Роль `anon` сейчас имеет неизвестный (=дефолтный) доступ к таблицам, которые ещё не созданы.

**Почему P0:** Без applied grants первый же деплой Supabase-таблиц сделает их доступными для прямого `INSERT`/`SELECT` через `anon` key. Supabase по умолчанию включает RLS, но таблицы создаются с неопределёнными policies.

**Action:**
- Создать `supabase/migrations/YYYYMMDD_personalization_schema.sql`
- `REVOKE ALL ON ALL TABLES IN SCHEMA personalization FROM anon, authenticated`
- `GRANT EXECUTE ON FUNCTION ingest_personalization_summary_v1 TO anon` (и только эту)
- Добавить pgTAP / psql negative tests: `SET ROLE anon; SELECT * FROM personalization.served_list_summary;` → ожидание `permission denied`

### P0.2 — Cardinality mismatch: 100 vs 24

**Проблема:** [database.md](../../../docs/features/unsigned-personalization/database.md) говорит `shown_event_ids <= 100`. [event-detail-related.md](../../../docs/features/unsigned-personalization/event-detail-related.md) говорит `<= 24`. Клиентский JS ограничивает `limit` до 4-6 по viewport, но **RPC/endpoint не знает об этом ограничении**.

**Почему P0:** "Умный" клиент или бот может отправить `shown_event_ids` длиной 100 (по документации `database.md` — валидно). Это x4 больше данных на каждую строку. На free-tier Supabase (500MB) это ускорит переполнение.

**Action:**
- RPC должен принимать `surface` как аргумент и применять surface-specific cap:
  - `event_detail_related` → `<= 24`
  - future surfaces → задать явно
- Документацию привести в консистентное состояние: `database.md` должен ссылаться на surface-specific caps, а не на generic `100`

### P0.3 — Kill-switch и storage guard отсутствуют

**Проблема:** Нигде нет механизма аварийного отключения персонализации. Нет автоматического `emergency_disable` при приближении к лимиту хранилища Supabase.

**Почему P0:** Без kill-switch единственный способ остановить abuse или переполнение — ручное вмешательство в Supabase dashboard. На free-tier это может занять от минут до часов. За это время БД может стать readonly.

**Action:**
- Таблица `personalization.config` с `personalization_enabled BOOLEAN DEFAULT TRUE`
- RPC проверяет этот flag при каждом вызове (кэш 60 сек допустим)
- Scheduled check (`pg_cron` или external worker): `pg_total_relation_size()` > 75% → auto-disable + alert
- В `personalization.js` endpoint unavailable → graceful fallback (это уже реализовано ✅)

### P0.4 — Write path endpoint не существует

**Проблема:** `same_origin_endpoint_v1` — только имя в документации. Нет кода Fly endpoint. Нет handler'а. `supabase_rpc_ingest_v1` — описан контракт, но RPC function не написана.

**Почему P0:** Без хотя бы одного рабочего write path canary невозможен. Нельзя тестировать rate-limiting, dedupe, quota enforcement на несуществующем endpoint.

**Action для spike:**
1. `PERSONALIZATION_WRITE_PATH=none|same_origin_endpoint_v1|supabase_rpc_ingest_v1` в конфиге
2. Для canary: реализовать `same_origin_endpoint_v1` как thin Fly handler (validate → rate-limit → compact → write)
3. `supabase_rpc_ingest_v1` — как alternative path, включаемый после сбора abuse-статистики на Fly

---

## 5. P1 Risks (блокируют production, но не canary)

### P1.1 — Observability полностью отсутствует

Нет ни одной метрики: accept/drop/quarantine rate, storage growth, p95 ingest latency, fallback rate, bot rate. Без этого canary запустить можно, но принять решение о production rollout — нельзя.

**Action:** Добавить в ops-runbook.md конкретные metric names + alert thresholds. Реализовать counting на первой итерации endpoint.

### P1.2 — Equivalence двух write paths не гарантирована

[production-integration.md](../../../docs/features/unsigned-personalization/production-integration.md) описывает два write path, но нет fixture теста, что оба пишут идентичные canonical rows. Аналитика может "развалиться" если один путь пишет compact, а другой — сырой payload.

**Action:** Fixture test: один и тот же browser payload → `same_origin_endpoint_v1` → row A; тот же payload → `supabase_rpc_ingest_v1` → row B; `A == B`.

### P1.3 — Retention не реализован

[database.md](../../../docs/features/unsigned-personalization/database.md) упоминает retention, но нет `pg_cron` job, нет external worker, нет даже SQL для `DELETE WHERE received_at < now() - interval '14 days'`.

### P1.4 — Прошедшие события: retention policy описана, не исполнена

[static-site-pages/README.md](../../../docs/features/static-site-pages/README.md) описывает 30-дневный retention для прошедших страниц, `410 Gone` для удалённых. Но:
- Yandex Object Storage не поддерживает `410` нативно
- Нет CDN/edge rules для реализации
- Sitemap update при removal — design only

### P1.5 — Promo vs negative interests

[production-integration.md](../../../docs/features/unsigned-personalization/production-integration.md) описывает promo-события, которые могут быть вставлены в рекомендации. Но:
- Не описано, как promo взаимодействует с `audience_exclusion_tags`
- `negative_interest_tags` vs explicit `hidden_event_ids` — разная семантика, но в scoring они смешиваются
- Explicit hide должен быть hard veto. Inferred negative — soft decay. Promo не должен override ни одного из них для `audience_exclusion_tags`

---

## 6. Критический анализ Playwright-тестов

### Что тесты покрывают хорошо

[static_personalization_contract.spec.ts](../../../tests/playwright/static_personalization_contract.spec.ts) — 7 тестов, покрывают:

| Сценарий | Verdict |
| --- | --- |
| No consent → static fallback, no telemetry | ✅ Корректно |
| Mobile consent → local rerank + served-list telemetry | ✅ Хорошо |
| Legacy profile (с `negative_tags`) → rejected | ✅ Критично, правильно |
| Missing `taxonomy_version` → rejected | ✅ Хорошо |
| Non-UUID ids → rejected | ✅ Критично |
| Broken localStorage → static fallback, no crash | ✅ Хорошо |
| Desktop → grid/module, not feed | ✅ Правильно |
| Backend unavailable → local fallback, CTA usable | ✅ Критично |

### Что тесты НЕ покрывают (gaps)

| Missing coverage | Severity | Why it matters |
| --- | --- | --- |
| No visible jump/reorder after late rerank | **P0** | UX invariant из [interface-references.md](../../../docs/features/static-site-pages/interface-references.md#L58); пользователь увидит "глюк" |
| Resize debounce telemetry flood | **P1** | 10+ resize events/sec → 10+ served-list summaries if not deduped correctly |
| `shown_event_ids` cardinality > 24 rejection by RPC | **P0** | RPC не существует, тест невозможен; но нужен SQL-level тест |
| Duplicate served-list emission across page navigations | **P1** | `servedListByHash` живёт в памяти, при SPA navigation может leak |
| `audience_exclusion_tags` hard veto | **P1** | Отдельный от `negative_interest_tags`, не тестируется |
| Promo event injection without overriding user hides | **P1** | Promo logic не реализована |
| Session summary completeness | **P2** | `createSessionSummary` возвращает пустые deltas, не тестируется |

### Structural concern

Тесты маршрутизируют `https://kenigevents.test/**` через route interception. Это правильно для unit-level контрактов, но не тестирует:
- Реальный Astro SSG output (HTML structure)
- Реальный same-origin manifest fetch
- Реальный telemetry endpoint response codes
- CSP/CORS/cookie behavior на production domain

Нужен отдельный integration-level тест на реальном Astro build output.

---

## 7. Несостыковки в документации

| Документ A | Документ B | Противоречие |
| --- | --- | --- |
| [database.md](../../../docs/features/unsigned-personalization/database.md) | [event-detail-related.md](../../../docs/features/unsigned-personalization/event-detail-related.md) | `shown_event_ids <= 100` vs `<= 24` |
| [README.md](../../../docs/features/unsigned-personalization/README.md) | [personalization.js](../../../static_site/personalization/personalization.js) | `feature_schema_version: "event-features-v1"` (docs) vs `"event-detail-related-v1"` (code) |
| [bots-and-automation.md](../../../docs/features/unsigned-personalization/bots-and-automation.md) | [gemini-review-2026-06-27.md](../../../docs/features/unsigned-personalization/gemini-review-2026-06-27.md) | RPC может использовать headers для bot detection vs headers не доверяемы в RPC-only mode |
| [production-integration.md](../../../docs/features/unsigned-personalization/production-integration.md) | реальность | `same_origin_endpoint_v1` описан как "default canary path", но не существует как код |

---

## 8. Security/Privacy Analysis

### Что правильно

- Anonymous-only, no PII by design
- `anon_id` / `session_id` — client-generated UUIDs, not server-tracked
- Profile in localStorage, not in cookies
- Consent gate before any telemetry
- Profile version + taxonomy version matching → incompatible profiles rejected

### Что критически недостаточно

| Risk | Severity | Detail |
| --- | --- | --- |
| **SECURITY DEFINER без minimal role** | 🔴 Critical | [database.md](../../../docs/features/unsigned-personalization/database.md) описывает SECURITY DEFINER function, но не определяет dedicated `ingest_role`. Если function owned by `postgres`/`service_role`, SQL injection в параметрах → full DB access |
| **`search_path` not locked** | 🔴 Critical | Нигде не видно `SET search_path = ''`. SECURITY DEFINER + default search_path = classic PostgreSQL privilege escalation vector |
| **JSONB sink** | ⚠️ High | Если RPC принимает `jsonb` аргумент, нужен strict whitelist + `jsonb_to_record` с explicit AS. Произвольные ключи → storage bloat + potential injection |
| **Client-controlled trust fields** | ⚠️ High | `actor_class`, `trust_state`, `training_eligible` не должны приниматься от клиента. Только server-side code может их назначить |
| **UUID validation on server** | ⚠️ Medium | Client JS валидирует UUID. Но RPC/endpoint тоже должен `p_anon_id::uuid` — если не парсится, `DROP` |

---

## 9. Probe Report Analysis

[event-detail-related-probe.md](../../../docs/features/unsigned-personalization/event-detail-related-probe.md) — 40 anchors, 10 personas, 765 строк. Мой анализ:

### Хорошо

- Все 40 anchors проходят hard invariants (non-empty, self-excluded, hidden excluded, diversity caps)
- Static top-5 — coherent по категории/теме
- Local rerank корректно двигает результаты по профилю

### Проблемы

1. **36/40 WARN на `negative_interest_top5_count_le_1`** — это не "фоновый шум", это **системная проблема таксономии**. Если 10% anchors нарушают negative-interest invariant, значит taxonomy overlap слишком велик. При реальных пользователях с accumulating negative signals это будет видно как "персонализация сломана — она показывает то, что я скрыл".

2. **Anchor 6038 (workshop) + persona `music_no_kids`:** local rerank top-1 score **0.2557**, top-4 score **-0.273**. Отрицательный score в top-5 — это не рекомендация, это "у нас нет ничего лучше этого мусора". Нужен floor: если `personal_score < 0`, не показывать.

3. **Anchor 5981 (nightlife) + persona `theatre_evening`:** local rerank полностью заменяет категорию. Top-3 — всё theatre, ни одного nightlife. Это корректное поведение для persona, но anti-pattern для **page context**: если пользователь на странице nightlife-события, блок "Похожие" должен сохранять минимальную связь с контекстом страницы.

4. **Дубликаты в static results:** anchor 4130 (Кальмания) → static top-5 содержит `6252` и `6276` — оба "Женщины Мира. Война" с идентичным score `0.434`. Это или дубликат события, или split по датам. Dedup-фильтр в ranker должен схлопывать same-title-same-score кандидатов.

5. **Anchor 5510 (kids/sport):** persona `music_no_kids` получает в local rerank `6268` (cinema) на втором месте. Это переход из kids+sport в cinema — слишком большой скачок. Reason: `profile:positive_affinity` — значит persona просто любит кино, и система тащит его в любой контекст.

### Verdict по quality

Golden-smoke probe — полезный инструмент, но **не заменяет** human editorial review. Для canary нужен:
- Manual top-10 review на 5-10 реальных event pages
- Mobile/desktop UX check на живом Astro build
- Negative score floor check
- Page context preservation check

---

## 10. SEO/Static Pages Gap Analysis

[static-site-pages/README.md](../../../docs/features/static-site-pages/README.md) — подробный design. Но:

| Requirement | Status | Gap |
| --- | --- | --- |
| Astro SSG build pipeline | 📝 Designed | Нет `site/` directory, нет `package.json`, нет Astro config |
| Event page HTML template | 📝 Designed | Нет `.astro` component |
| JSON-LD `schema.org/Event` | 📝 Spec'd | Нет шаблона, нет валидации |
| Sitemap auto-generation | 📝 Spec'd | Нет build step |
| `robots.txt` with consent-aware rules | 📝 Mentioned | Нет файла |
| Preview deploy (secret prefix) | 📝 From kdg80 | Нет adapted script |
| Production deploy to Yandex Object Storage | 📝 Planned | Нет credentials, нет script |
| Canonical URL / redirect policy | 📝 Designed | Нет redirect map, нет 301/410 implementation |
| Telegraph coexistence | 📝 1-month plan | Нет dual-run monitoring |

**Bottom line:** Static pages — pure design document. Ни одна HTML-страница не генерируется. SEO benefit = 0 до первого Astro build.

---

## 11. Рекомендации: Engineering Spike → Canary → Production

### Engineering Spike (можно начинать)

```
PR title: spike: gated personalization write-path contract and supabase rpc ingest
```

Minimum scope:
1. `PERSONALIZATION_WRITE_PATH=none|same_origin_endpoint_v1|supabase_rpc_ingest_v1`
2. Supabase migration: closed schema, tables, grants, RPC function
3. SQL negative tests (grants/RLS/cardinality/dedupe/quota)
4. Fixture tests: same browser payload → both write paths → identical canonical row
5. `ops-runbook.md`: disable path, verify grants, check DB size, retention, kill-switch

### Canary Gate (P0 must be green)

- [ ] `anon`/`authenticated` cannot SELECT/INSERT/UPDATE/DELETE personalization tables
- [ ] RPC has dedicated minimal owner role, `search_path = ''`, no dynamic SQL
- [ ] Payload rejects `shown_event_ids > 24` for `event_detail_related`
- [ ] Client cannot set `actor_class`, `trust_state`, `training_eligible`
- [ ] Dedupe by `anon_id + served_list_hash` exists
- [ ] Per-anon/session/time quotas exist
- [ ] Kill-switch + storage alert thresholds exist
- [ ] Supabase unavailable → page renders without degradation (already ✅ in JS)
- [ ] No visible jump/reorder after late rerank (Playwright test needed)
- [ ] `negative_score < 0` candidates excluded from display

### Production Gate (P1 must be green)

- [ ] Both write paths produce equivalent canonical summaries (fixture test)
- [ ] Observability: accept/drop/quarantine rates, storage growth, p95 latency
- [ ] Retention cleanup scheduled and verified
- [ ] At least 5 real event pages generated by Astro SSG
- [ ] JSON-LD validated via Rich Results Test on 3+ pages
- [ ] Sitemap submitted to Yandex Webmaster + Google Search Console
- [ ] Editorial top-10 review for representative pages × personas
- [ ] Mobile/desktop UX review on real device/viewport
- [ ] Telegraph dual-run monitoring active
- [ ] Promo/exclusion interaction specified and tested

---

## 12. Что НЕ строить (anti-roadmap)

| Don't build | Why |
| --- | --- |
| Online ML ranker | Static + local rerank достаточно для MVP-0; ML adds latency + complexity |
| LLM in hot path | Документация запрещает, и правильно |
| Direct browser INSERT into tables | Даже для spike — создаёт привычку, которую потом невозможно искоренить |
| Semantic/vector search MVP-0 | Probe report показывает, что static + local rerank уже дают когерентные результаты |
| Auto profile reset on any error | Только при incompatible schema version; иначе пользователь теряет все hides/preferences |
| Complex multi-horizon profile without basic single-layer working | `session/short/mid/long` horizons — design overkill для MVP-0 без live traffic data |

---

## 13. Summary для Gemini Pro review (когда станет доступен)

> [!IMPORTANT]
> Gemini Pro (`gemini-3-pro-preview`, `gemini-3.1-pro-preview`) возвращал `429 RESOURCE_EXHAUSTED` на все ключи. Flash-Lite ответы в [gemini-review-2026-06-27.md](../../../docs/features/unsigned-personalization/gemini-review-2026-06-27.md) корректно маркированы как supplementary probe material.

Когда Pro станет доступен, запросить review по:
1. SQL migration draft (grants + RLS + RPC function)
2. SECURITY DEFINER ownership model
3. Storage guard implementation (trigger vs scheduled check vs approximate)
4. Negative-interest taxonomy overlap (36/40 WARN — systemic or acceptable?)
5. Promo vs exclusion interaction matrix

---

## 14. Final Verdict

**Система спроектирована грамотно, но не реализована.** Документация описывает идеальную архитектуру; код содержит только клиентский JS (хороший) и Playwright-тесты (приличные, но с gaps). Между "design intent" и "executable evidence" — пропасть.

**Следующий шаг:** engineering spike PR с SQL migration + write path + negative tests. Без этого PR любые дальнейшие обсуждения архитектуры — это prose, не engineering.
