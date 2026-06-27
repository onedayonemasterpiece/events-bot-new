# Critical Self + External Consultant Review — write-path hardening

> **Date:** 2026-06-27
> **Commit under review:** `634a896c` (`docs: allow gated supabase rpc ingest path`)
> **Scope:** anonymous personalization MVP-0, static event pages, Supabase/Postgres telemetry write path.
> **Status:** review artifact, not an acceptance decision. Current state remains **engineering-spike ready only**; not canary-ready and not production-ready.
> **Model-validity correction:** the raw Gemini Flash-Lite outputs in this file are **supplementary probe material only**. They are not a valid external consultant review. Valid Gemini consultation requires `gemini-3-pro-preview` or `gemini-3.1-pro-preview`; both were retried and blocked by provider quota on 2026-06-27.

## Reviewed evidence

- External consultant feedback from `5055ff43-df23-4ab6-a3bc-67cd8ad6bb44/pasted-text.txt`.
- Current canonical docs:
  - `docs/features/unsigned-personalization/README.md`
  - `docs/features/unsigned-personalization/production-integration.md`
  - `docs/features/unsigned-personalization/database.md`
  - `docs/features/unsigned-personalization/bots-and-automation.md`
  - `docs/features/unsigned-personalization/event-detail-related.md`
  - `docs/features/unsigned-personalization/neural-flow.md`
  - `docs/features/unsigned-personalization/gemini-review-2026-06-26.md`
- Official Supabase contract assumptions used for the critique: API exposure/RLS, database functions, API key separation, and security-definer precautions.

Prompt/response artifacts are intentionally kept outside git in `artifacts/codex/unsigned-personalization-review-2026-06-27/`.

## Codex self-analysis

### Verdict

The consultant feedback is correct: `634a896c` is a useful **documentation hardening** commit, not a production write path. It fixes an over-broad rule (“browser must never call Supabase”) by replacing it with a more precise boundary:

- forbidden: browser direct table writes/updates/deletes, browser public profile/table reads, raw browser JSON persisted as a DB row;
- allowed only as a gated mode: browser calls a dedicated compact append-only Supabase RPC that validates, compacts, dedupes, quotas and stores only an accepted summary or tiny quarantine/drop evidence.

That architectural correction is valuable, but it does not yet prove security, capacity, bot resistance or recommendation quality. The next milestone must be executable evidence, not more prose.

### Status table

| Area | Status after review | Reason |
| --- | --- | --- |
| Engineering spike | **Yes** | Scope is narrow (`event_detail_related`), static fallback is preserved, LLM/ML is out of hot path. |
| Canary | **No / P0 blocked** | No SQL/grants/RLS tests, no applied migration, no quota/storage/kill-switch evidence. |
| Production | **No / P0 blocked** | Write path, retention, abuse handling, observability and quality gates are still design-level. |
| Recommendation quality | **Not proven** | Probe is golden-smoke, not editorial/human proof; negative-interest and taxonomy risks remain. |
| Bot resistance | **Designed, not implemented** | Same-origin can classify better; pure RPC cannot reliably verify crawlers without an external layer. |
| Supabase RPC | **Allowed spike, not first canary default** | It moves backend work into Postgres/PostgREST and needs stronger DB-level guards. |

### Main findings

1. **First canary should stay on `same_origin_endpoint_v1`.**
   A thin Fly endpoint can drop obvious abuse before PostgREST/Postgres. It must not do ML, embeddings, catalog scans or profile rebuilds; it only validates, rate-limits, compacts and writes/calls a private DB path.

2. **`supabase_rpc_ingest_v1` is not “backendless”.**
   It is a backend moved into SQL/RPC. That is acceptable only if the function itself enforces typed contract, surface caps, dedupe, quotas, storage guard, retention compatibility, closed grants and minimal response.

3. **The current docs still contain contradictions that would break implementation.**
   - `database.md` says generic `shown_event_ids <= 100`, while MVP-0 `event_detail_related` requires `<= 24`. The table may be future-capable, but the RPC must enforce surface-specific `<=24` now.
   - `README.md` examples still use `feature_schema_version: "event-features-v1"`, while the MVP surface uses `event-detail-related-v1`.
   - `event-detail-related.md` has stale “Last local run: `8 passed`”, but the test set changed.

4. **Bot handling must be split by write path.**
   - Same-origin can use IP/UA/server logs/CDN/Fly heuristics and optional reverse-DNS allowlist checks.
   - Pure RPC can only use weak request metadata and shape/rate heuristics; it must not trust client-provided `actor_class`, `trust_state`, `training_eligible` or “bot score”. In RPC-only mode suspicious traffic should fail closed: drop/quarantine, never profile/training eligible.

5. **Telemetry must remain compact.**
   The dangerous failure mode is a weak impression firehose that fills the Supabase free-tier DB. MVP should store accepted served-list summaries and strong actions tied to `served_list_id/hash`; raw/debug telemetry stays disabled or sampled with short retention.

6. **Recommendation quality is explicitly unproven.**
   No claim should say the personalization is “smart enough” before editorial top-N review, persona fixtures, negative-interest triage, mobile/desktop UX checks and canary metrics.

7. **No visible jump/reorder is a UX invariant, not a nice-to-have.**
   If local rerank finishes after the related block is already read/visible, the UI must avoid sudden reorder without user action. This needs an explicit Playwright/visual contract.

### Acceptance gates before any canary

P0 gates:

- `anon`/`authenticated` cannot `SELECT/INSERT/UPDATE/DELETE` telemetry/profile tables.
- `PUBLIC` execute is revoked from functions; `anon` can execute only the exact ingest RPC when that mode is selected.
- RPC has minimal owner privileges, fixed safe `search_path`, schema-qualified relations, no dynamic SQL, no raw client SQL, no profile/recommendation/debug response.
- Payload rejects/drops invalid UUIDs, incompatible versions, unknown surface/layout/algorithm, array mismatch, scores out of range, oversized payload, missing `served_list_id/hash`, and `shown_event_ids > 24` for `event_detail_related`.
- Client cannot set server trust fields: `actor_class`, `trust_state`, `training_eligible`, quota state, server timestamp, request IP interpretation, debug sampling.
- Dedupe/idempotency exists by `anon_id + served_list_id` and/or `client_summary_id`/`served_list_hash` bucket.
- Per-anon/session/time quotas exist; malformed/oversized calls count toward quota.
- Accepted row stays compact; raw payload is not persisted; quarantine stores tiny evidence only.
- Retention cleanup is scheduled and verified.
- Emergency disable/kill switch and storage alert thresholds exist before DB budget is threatened.
- Supabase/RPC unavailable or quota exceeded never breaks page render, CTA, calendar/share/ticket flows.

P1 gates:

- Same browser fixture maps to the same canonical compact row in `same_origin_endpoint_v1` and `supabase_rpc_ingest_v1`.
- Observability exposes accept/drop/quarantine rates, storage growth, fallback rate, local-rerank rate, p95 ingest latency, strong-action-without-served-list rate, bot/quarantine rate.
- Product review covers top-N results for representative event pages/personas and separates mobile feed metrics from desktop grid/module metrics.

### Correction notes for Gemini output

Gemini is useful as a critical reviewer, but several suggestions must not be copied blindly:

- The security response suggests a function that “does `SET ROLE ingest_role`”. In PostgreSQL, a safer implementation is to own the `SECURITY DEFINER` function by a minimal role and grant only the required rights; do not depend on client-controlled or ad-hoc role switching inside the function.
- A `search_path` containing `public` is weaker than an empty or tightly controlled path with fully schema-qualified objects. Prefer `set search_path = ''` and explicit schema qualifications for security-definer functions.
- A per-insert `pg_total_relation_size(...)` trigger can be too expensive under load. Use a kill switch plus quota buckets and scheduled/periodic size checks; a hard guard inside RPC can be approximate and cheap.
- “Hard filter all negative-interest tags” is product-sensitive: explicit hide/not-interested can be a hard veto, but inferred negative interests may need decay/thresholding to avoid a new filter bubble. Promo must never override `audience_exclusion_tags` or explicit user hides.
- A browser-sent “bot score” is not trusted. Only same-origin/server code can attach a trusted bot/trust decision; pure browser→Supabase RPC must derive trust server-side or fail closed.

## External consultant request transparency

### Policy applied after correction

For external architecture/product/security consultation in this project, valid consultant sources are:

- Gemini Pro class only: `gemini-3-pro-preview` or `gemini-3.1-pro-preview`;
- Opus through Antigravity/agy: `a-opus`;
- Opus through Claude Code project alias `Opus` when Claude access/tariff is active.

Gemini Flash/Flash-Lite/Lite outputs may be useful probes, but they do not satisfy the external-consultant requirement and must not be used as acceptance evidence.

### Gemini Pro retry evidence

A Pro-only retry was attempted after the operator clarified the policy. The same full review prompt was used; it was countable but not generatable under the current project quota/capacity.

| Attempt | Result |
| --- | --- |
| `gemini-3.1-pro-preview` `countTokens` via `GOOGLE_API_KEY` | HTTP 200, `totalTokens=53751`. |
| `gemini-3.1-pro-preview` `generateContent` via `GOOGLE_API_KEY`, `GOOGLE_API_KEY2`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY4` | HTTP 429 `RESOURCE_EXHAUSTED` on every key. |
| `gemini-3-pro-preview` `generateContent` via `GOOGLE_API_KEY`, `GOOGLE_API_KEY2`, `GOOGLE_API_KEY3`, `GOOGLE_API_KEY4` | HTTP 429 `RESOURCE_EXHAUSTED` on every key. |

Artifact evidence, with secret values omitted, is stored outside git:

- `artifacts/codex/unsigned-personalization-review-2026-06-27/gemini_pro_attempts_2026-06-27.md`
- `artifacts/codex/unsigned-personalization-review-2026-06-27/gemini_pro_attempts_2026-06-27.json`

Conclusion: **Gemini Pro external review is blocked**, not completed. Do not cite the Flash-Lite response below as a Gemini consultant review.

### Opus/a-opus consultation evidence

A short Opus consultation through Antigravity/agy (`a-opus`) was run to validate the policy correction and patch points. It recommended exactly the current correction: add an external-consultant policy to `AGENTS.md`, cross-reference it from Claude/Codex docs, and reclassify this document's Flash-Lite answer as supplementary probe material.

Artifact evidence is stored outside git:

- `artifacts/codex/unsigned-personalization-review-2026-06-27/a_opus_policy_consultation_2026-06-27.md`

### Earlier non-authoritative Gemini probe

Before the policy correction, the following lower-tier probes were run. They remain below for transparency only:

| Attempt | Result |
| --- | --- |
| `gemini-3.5-flash` | HTTP 503 twice; not valid consultant evidence. |
| `gemini-2.5-pro` | HTTP 429/quota unavailable; also not allowed by the corrected project policy. |
| `gemini-3.1-flash-lite` | Main probe response; response id `GJA_arGlHNvZxN8P9OWjkAY`; supplementary only. |
| `gemini-3.1-flash-lite` | Focused security/RLS probe; response id `2JA_ao-SFvyH28oPos6muAQ`; supplementary only. |
| `gemini-3.1-flash-lite` | Focused product/UX probe; response id `35A_aqjcDI2qnsEP-or24AU`; supplementary only. |
| `gemini-2.5-flash` | Backup probe; shorter/less actionable; supplementary only. |

Earlier Flash-Lite usage metadata:

```json
{
  "model": "gemini-3.1-flash-lite",
  "usageMetadata": {
    "promptTokenCount": 32448,
    "candidatesTokenCount": 1510,
    "totalTokenCount": 33958,
    "promptTokensDetails": [
      {
        "modality": "TEXT",
        "tokenCount": 32448
      }
    ],
    "serviceTier": "standard"
  },
  "note": "supplementary probe only; not a valid external consultant review"
}
```

## Gemini Flash-Lite supplementary probe — raw response

```text
Это ревью архитектуры, основанное на предоставленных документах. Мой вердикт: **архитектура находится в состоянии «декларативного оптимизма»**. Документы описывают идеальную систему, но текущие артефакты (SQL, тесты, контракты) — это лишь каркас, который не прошел проверку на реальную нагрузку и безопасность.

### 1. Verdict Table

| Область | Статус | Комментарий |
| :--- | :--- | :--- |
| **Engineering Spike** | Ready | Концепция thin-runtime и разделения write-path верна. |
| **Canary Readiness** | **P0 Blocked** | Нет исполняемых SQL-тестов, нет quota/storage guards. |
| **Production Readiness** | **P0 Blocked** | Отсутствует механизм аварийного отключения (kill-switch). |
| **Security** | P1 Risk | RLS и grants описаны, но не применены в миграциях. |
| **Data Integrity** | P1 Risk | Cardinality mismatch (100 vs 24) — риск переполнения. |

---

### 2. Что правильно
*   **Thin-runtime constraint:** Жесткое разделение Fly (static/API) и Batch (ML/LLM/Rebuild) — это единственный способ выжить на `shared-cpu` инстансах.
*   **Write-path policy:** Отказ от прямого browser-to-table insert в пользу `same_origin_endpoint_v1` (как default) и `supabase_rpc_ingest_v1` (как gated alternative) — это верная стратегия защиты БД от мусора.
*   **MVP-0 Scope:** Фокус на `event_detail_related` — это идеальный «песочный» полигон, где ошибка алгоритма не ломает весь сайт.

---

### 3. P0 Blockers до canary (Критический приоритет)
1.  **SQL Grants & RLS Enforcement:** В `database.md` описаны `revoke all`, но в коде нет миграции, которая это гарантирует. **Блокировка:** Нельзя выкатывать, пока `anon` роль имеет хоть какой-то доступ к таблицам, кроме вызова RPC.
2.  **Storage Guard:** Отсутствует автоматический `emergency_disable` при достижении 75% квоты Supabase (500MB).
3.  **Payload Contract:** В `database.md` указано `cardinality <= 100`, а в `event-detail-related.md` — `24`. **Блокировка:** RPC должен жестко отсекать всё, что >24 для этого surface. Иначе «умный» клиент забьет БД мусором.
4.  **Bot Contract:** Нет реализации `fail-closed` для RPC-only режима. Если RPC не может верифицировать бота, он должен дропать запрос, а не принимать его как "unknown".

---

### 4. P1/P2 Risks
*   **P1 (Observability):** Нет дашборда с метриками `drop_rate` vs `accept_rate`. Мы не узнаем, что нас атакуют, пока не кончится место в БД.
*   **P1 (Equivalence):** Нет гарантии, что `same_origin_endpoint` и `supabase_rpc` пишут идентичные данные. Это приведет к «развалу» аналитики.
*   **P2 (UX):** Отсутствие дебаунса при ререндере на resize. Если пользователь крутит телефон, мы можем сгенерировать 10+ телеметрических событий за секунду.

---

### 5. Contradictions & Outdated Wording
*   **`database.md` vs `event-detail-related.md`:** Конфликт лимитов (100 vs 24). Решение: RPC должен принимать `surface` как аргумент и применять лимит динамически.
*   **`README.md`:** Ссылка на `event-features-v1` конфликтует с `event-detail-related-v1`. Нужно унифицировать нейминг версий схем.
*   **`bots-and-automation.md`:** Утверждение, что RPC может использовать headers для классификации, противоречит реальности (headers легко подделываются). Нужно убрать надежду на headers как на доказательство "human".

---

### 6. same_origin_endpoint_v1 vs supabase_rpc_ingest_v1
*   **Выбор:** `same_origin_endpoint_v1` — **единственный** путь для canary.
*   **Почему:** Fly-endpoint позволяет сделать `rate-limit` по IP/Session до того, как запрос попадет в Postgres. Supabase RPC ingest — это «открытая дверь» в БД. Его можно включать только после того, как мы соберем статистику по abuse на Fly.

---

### 7. Exact Next PR Scope (Executable Tests)
PR должен содержать только файлы в `supabase/migrations/` и `tests/sql/`:
1.  **Migration:** Создание схемы `personalization`, таблиц с RLS, revoke всех прав у `anon`.
2.  **RPC:** Создание `ingest_personalization_summary_v1` с `SECURITY DEFINER` и `search_path = ''`.
3.  **Test:** `tests/sql/ingest_security.sql` — скрипт, который пытается выполнить `SELECT * FROM public.personalization_served_list_summary` от имени `anon` и ожидает `403`.
4.  **Test:** `tests/sql/ingest_validation.sql` — попытка вызвать RPC с `shown_event_ids` длиной 25 и ожидание `400` (или `drop`).

---

### 8. What NOT to build
*   **НЕ строить:** Online ML-ranker. Сейчас это преждевременно.
*   **НЕ строить:** Интеграцию с LLM в hot-path. Только offline enrichment.
*   **НЕ строить:** Прямой INSERT в таблицы из браузера. Даже для spike. Это создает привычку, которую потом не вытравить из фронтенд-разработчиков.
*   **НЕ строить:** Автоматический сброс профиля при любой ошибке. Только при несовпадении версий схемы.

**Итого:** Вы слишком много внимания уделяете описанию "как это будет работать" и слишком мало — "как это сломается". Сфокусируйтесь на **SQL-тестах безопасности**, иначе ваш Supabase станет свалкой JSON-мусора через 48 часов после запуска.
```

## Gemini Flash-Lite focused security/RLS supplementary probe — raw response

```text
Этот security review сфокусирован на **write-path** и **Postgres/Supabase** безопасности.

### 1. Критический анализ архитектуры (Write-Path)

*   **Риск `SECURITY DEFINER`:** Это самый опасный элемент. Если функция `ingest_personalization_summary_v1` будет иметь `SECURITY DEFINER` и `search_path` по умолчанию (или не заданный), любой `SQL injection` в параметрах функции (даже через `text` поля) может привести к выполнению кода от имени владельца схемы (обычно `postgres` или `service_role`).
    *   **Action:** Установить `set search_path = pg_catalog, pg_temp` (или конкретная схема). Использовать `SECURITY INVOKER` везде, где это возможно. Если нужен `SECURITY DEFINER` для записи в закрытые таблицы, используйте **отдельную роль с минимальными правами** (только `INSERT` в конкретные таблицы), а не `postgres` или `service_role`.
*   **Риск `JSONB` sink:** Даже если вы нормализуете JSON внутри RPC, сам факт принятия `jsonb` в качестве аргумента — это риск.
    *   **Action:** Используйте строго типизированные аргументы (как в вашем примере контракта). Если `jsonb` неизбежен, используйте `jsonb_to_record` с жестким `AS` (типизацией) и немедленно отбрасывайте любые ключи, не входящие в белый список.
*   **Риск `RLS` vs `Grants`:** Вы правильно отметили, что RLS не заменяет `GRANT`.
    *   **Action:** Убедитесь, что `anon` роль не имеет `USAGE` на схему `personalization` и `EXECUTE` на любые функции, кроме `ingest_personalization_summary_v1`.

### 2. Acceptance Tests (SQL/Grants)

Эти тесты должны быть частью CI/CD (например, через `pgTAP` или простые `psql` скрипты в GitHub Actions).

**Тест 1: Изоляция прав (Negative Test)**
```sql
-- Должно упасть с ошибкой доступа
SET ROLE anon;
INSERT INTO public.personalization_served_list_summary (anon_id, ...) VALUES (...);
-- Ожидание: ERROR: permission denied for table
```

**Тест 2: Изоляция RPC (Negative Test)**
```sql
-- Попытка вызвать любую другую функцию
SET ROLE anon;
SELECT * FROM some_other_function();
-- Ожидание: ERROR: permission denied
```

**Тест 3: Валидация контракта (RPC Logic Test)**
```sql
-- Попытка вставить массив > 24 элементов
SELECT public.ingest_personalization_summary_v1(
  p_shown_event_ids => ARRAY[1,2,3,...,25], ...
);
-- Ожидание: RAISE EXCEPTION 'cardinality limit exceeded'
```

**Тест 4: Dedupe/Quota (Logic Test)**
```sql
-- Вызов функции дважды с одним served_list_hash в течение 1 минуты
SELECT public.ingest_personalization_summary_v1(p_served_list_hash => 'abc', ...);
SELECT public.ingest_personalization_summary_v1(p_served_list_hash => 'abc', ...);
-- Ожидание: Второй вызов должен вернуть 204/void или залогировать в quarantine, но не создавать вторую запись
```

### 3. Конкретные Actionable замечания по коду/SQL

1.  **Retention:** В `database.md` указано, что retention должен быть scheduled.
    *   **Action:** Создайте `pg_cron` задачу (если Supabase поддерживает) или внешний `worker`, который делает `DELETE FROM ... WHERE received_at < now() - interval '14 days'`. Не полагайтесь на то, что это "кто-то сделает".
2.  **Storage Guard:**
    *   **Action:** Добавьте `TRIGGER` на `INSERT` в `personalization_served_list_summary`, который проверяет `pg_total_relation_size('public.personalization_served_list_summary')`. Если > 450MB — `RAISE EXCEPTION` или переключение в режим `soft-drop` (запись только в `quarantine`).
3.  **UUID:**
    *   **Action:** В RPC функции принудительно делайте `p_anon_id::uuid`. Если клиент прислал строку, которая не парсится в UUID — это `400 Bad Request` (или `drop` в RPC). Никаких `anon-` префиксов.
4.  **Search Path:**
    *   **Action:** В каждом `CREATE FUNCTION` явно прописывайте `set search_path = personalization, public, pg_temp`.

### 4. Резюме по статусу
Вы находитесь в точке "Architectural Proof". Чтобы перейти к **Canary Readiness**, вам нужно:
1.  **SQL-миграция**, которая создает роль `ingest_role` с правами только на `INSERT` в целевые таблицы.
2.  **Функция RPC**, которая делает `SET ROLE ingest_role` (или использует `SECURITY DEFINER` от имени этой роли).
3.  **Тестовый набор (pgTAP)**, который прогоняет 10-15 сценариев (от "несанкционированный SELECT" до "переполнение массива").

**Вердикт:** Не начинайте canary, пока не будет автоматизированного теста, который пытается сделать `SELECT * FROM public.personalization_served_list_summary` под ролью `anon` и получает `permission denied`. Это ваш главный "gate".
```

## Gemini Flash-Lite focused product/UX supplementary probe — raw response

```text
Этот обзор сфокусирован на критических точках, где архитектурный дизайн расходится с production-реальностью.

### 1. Product/Recommendation Quality
*   **Проблема:** Использование `static_related_score` как основы (80%) при наличии `negative_interest_match` (-55%) создает риск "мертвых зон". Если пользователь пометил категорию как неинтересную, но она доминирует в static-контексте, вы получите конфликт, где алгоритм будет пытаться "выдавить" событие, которое по логике должно быть там.
*   **Actionable:** Введите **Hard Filter Layer** до этапа скоринга. Если `event.category` в `negative_interest_tags`, событие должно исключаться из пула кандидатов до вычисления `personalized_related_score`. Не пытайтесь "задавить" негатив весами — это создает непредсказуемый UX.
*   **Negative Interest Warning:** 36 из 40 anchors с WARN — это не "не баг", это **отсутствие таксономической чистоты**. Если вы не исправите это на уровне `event-taxonomy-v1`, персонализация будет "шуметь" с первого дня.

### 2. Bot Resistance & Anti-Bubble
*   **Проблема:** RPC-only путь (supabase_rpc_ingest_v1) слеп к IP/UA-репутации.
*   **Actionable:** Внедрите **"Bot-Score" в payload**. Даже если Fly не делает тяжелых вычислений, он должен добавлять в RPC-запрос `X-Bot-Confidence` (на основе анализа заголовков/IP). RPC-функция должна делать `DROP`, если `bot_confidence > threshold`, не дожидаясь записи в БД.
*   **Anti-Bubble:** Вы заложили 10-20% exploration, но в MVP-0 (6 карточек) это всего 1 слот. Этого недостаточно для выхода из "пузыря". Увеличьте до 2 слотов (33%) для мобильного feed.

### 3. Mobile/Desktop UX
*   **Проблема:** "Резкий jump/reorder" при rerank.
*   **Actionable:** Внедрите **CSS-контракт**: контейнер `related-block` должен иметь `min-height` (рассчитанный по static-версии) до завершения rerank. Использование `opacity: 0` -> `opacity: 1` с `transition` обязательно. Без этого rerank будет выглядеть как "глюк" верстки.

### 4. Static Page Integration
*   **Проблема:** `event-detail-related` manifest генерируется статически, но rerank происходит в браузере.
*   **Actionable:** В manifest обязательно включите `reason_codes` для каждого кандидата. Если клиентский JS не может сделать rerank (например, из-за ошибки в localStorage), он должен уметь отобразить `reason_codes` из статики, чтобы блок не выглядел "пустым" или "случайным".

### 5. Promo & CTA
*   **Проблема:** Promo-события могут конфликтовать с `audience_exclusion_tags`.
*   **Actionable:** **Promo-override policy**. Если событие помечено как `promo`, оно должно игнорировать `negative_interest_tags` (но не `audience_exclusion_tags`!), иначе вы будете платить за показы, которые алгоритм будет скрывать. Это должно быть явно прописано в коде скоринга.

### 6. Canary Metrics & Supabase RPC
*   **Проблема:** RPC-функция — это "черный ящик" для DB.
*   **Actionable:**
    *   **Storage Guard:** Внутри `ingest_personalization_summary_v1` добавьте проверку: `IF (SELECT count(*) FROM telemetry WHERE created_at > NOW() - INTERVAL '1 hour') > MAX_LIMIT THEN RETURN;`. Это спасет от "атаки" или бага в JS-клиенте, который начнет спамить запросами.
    *   **Canary Readiness:** Вы не готовы к canary, пока нет **"Kill Switch"**. В Supabase должна быть таблица `config`, которую RPC-функция проверяет при каждом вызове (или кэширует на 60 сек). Если `personalization_enabled = false`, функция должна мгновенно возвращать `204` без записи в БД.

### 7. Итоговый вердикт по реализации (Action Plan)
1.  **Не делайте RPC-инжест основным.** Используйте `same_origin_endpoint_v1` (Fly). Это дает вам возможность логировать `request_headers` (IP, UA, Referer) до того, как они попадут в Postgres. В RPC вы потеряете контекст запроса.
2.  **Schema Versioning:** В каждом запросе от клиента должен быть `feature_schema_version`. Если версия не совпадает с текущей в БД — `DROP` запрос. Это предотвратит "отравление" данных старыми версиями JS-клиента.
3.  **Тесты:** Вместо Playwright-тестов "счастливого пути", напишите **SQL-тесты на RLS**. Создайте пользователя `anon`, попробуйте выполнить `SELECT` из таблицы `telemetry` — если запрос прошел, вы не готовы к production. Это критический P0.
```

## Next PR should be a non-production spike

Recommended PR title:

```text
spike: gated personalization write-path contract and supabase rpc ingest
```

Minimum scope:

1. Add config: `PERSONALIZATION_WRITE_PATH=none|same_origin_endpoint_v1|supabase_rpc_ingest_v1`.
2. Add one client writer interface: `sendServedListSummary(summary)` returning `accepted|dropped|unavailable`.
3. Add `telemetry-contract-v1` schema with client payload, canonical compact row, reason-mask dictionary, surface-specific caps and strong-action relation.
4. Add Supabase migration draft for closed tables, tiny quarantine table, quota bucket, retention cleanup function and `ingest_personalization_summary_v1`.
5. Add SQL tests for grants/RLS/execute rights/validation/dedupe/quota.
6. Add fixture tests proving `same_origin_endpoint_v1` and `supabase_rpc_ingest_v1` produce equivalent canonical summaries.
7. Add Playwright writer modes: `none`, mocked same-origin, mocked Supabase RPC; include no-consent, incompatible profile, backend unavailable, duplicate resize, and no visible jump/reorder checks.
8. Add `ops-runbook.md`: disable write path, verify grants, check DB size, run retention cleanup, watch canary metrics and kill switch.
