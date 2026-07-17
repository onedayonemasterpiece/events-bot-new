# Release plan: «Клубы по интересам»

## Release authority и неизменяемые условия

Owner GO 2026-07-17 сначала разрешил implementation RC из research baseline PR #54 / `e1a14afc`, а затем явно уточнил, что полное внедрение должно включать production delivery без отдельного напоминания. Это является production GO для additive migration/bootstrap, bounded replay, Smart Update enablement и checked public static publication. Rollout всё равно fail-closes на correctness/health gates; GO не разрешает обходить backup, tests, canary, rollback или evidence requirements.

Каждый gate фиксирует `git SHA`, migration revision, `catalog_hash`, `gold_fixture_hash`, matcher `policy_version`, exact model id, static `build_id/manifest_hash` и reviewer. «Последний код» или незахешированный snapshot не является release evidence.

## Flags и rollback handles

RC фиксирует три отдельные fail-closed switches:

1. `ENABLE_INTEREST_CLUB_PIPELINE=0` — relation evaluation/write и неблокирующий Smart Update handoff;
2. `ENABLE_INTEREST_CLUB_STATIC_PROJECTION=0` — чтение accepted rows в `interest-clubs-static-v1`;
3. `PUBLIC_INTEREST_CLUBS_ENABLED=0` — detail paths, sitemap и navigation/public promotion.

Все production defaults — OFF. Дополнительные bounded controls: `INTEREST_CLUB_PROVIDER_TIMEOUT_SECONDS=30` и `INTEREST_CLUB_STATIC_DEBOUNCE_SECONDS=90`. Public flag нельзя связывать с migration или shadow flag. `site/scripts/build-preview.mjs` включает projection/public gates только для noindex preview; это не production enablement. Static publisher обязан хранить last-good manifest/tree и уметь атомарно вернуть его без DB downgrade.

## Stage 0 — RC freeze и owner gold

**Действия:** зафиксировать RC SHA; сверить additive schema; превратить reviewed fixture в owner-approved gold; записать confirmed identities, hard negatives, linked/festival/co-hosting cases и expected review outcomes.

**GO gate:**

- 100% fixture cases имеют source ids/URLs и bounded evidence;
- 100% public identities имеют owner verdict `confirmed`;
- matcher: `0` unsafe false positives, positive recall `≥90%`, exact-quote validity `100%` на frozen gold;
- all `no/unclear/provider_error/invalid_quote` не создают relation;
- linked siblings и source duplicates не увеличивают distinct meeting count;
- migration upgrade/downgrade-or-forward-recovery проверены на копии production snapshot.

**NO-GO:** незакрытый false merge, меняющийся fixture/hash, direct provider bypass, positive Lite fallback, non-additive migration.

## Stage 1 — migration и bootstrap

Только после production GO: снять проверяемый backup Fly SQLite; записать pre-migration schema/count/hash; применить additive migration с evaluation/projection/public flags OFF; создать пустые versioned identity/relation/review/projection tables; импортировать только owner-approved bootstrap identities/aliases без public promotion.

**GO gate:** migration один раз применена и повторный bootstrap идемпотентен; event/source counts и hashes не изменились; новые relations `0`; foreign-key/orphan check `0`; backup restore drill проходит; health/Smart Update baseline не деградировал.

**Rollback:** выключить все flags, вернуть code SHA; при schema fault восстановить backup/forward-fix по migration runbook. Не удалять relation tables вручную во время инцидента.

## Stage 2 — shadow import и historical replay

Запустить frozen production snapshot и затем changed-only replay без public writes. Сначала bootstrap known identities, затем replay в хронологическом порядке с cutoff, отдельно holdout/future. Все LLM calls идут через `GoogleAIClient` limiter ledger. Results пишутся в shadow/review state и artifact, но не в public projection.

**GO gate:**

- input event coverage `100%`, duplicate processing `0`, dangling relations `0`;
- повтор того же input даёт те же relation/projection hashes и `0` дополнительных semantic calls для unchanged packets;
- unsafe false merge `0`; quote validity `100%`; provider success не маскируется fallback;
- relation decisions reconcile to `accepted + rejected + deferred + review = candidates`;
- owner проверил все новые identities, все merge/split/co-hosting proposals и каждую relation из festival/linked warning set;
- shadow freshness p95 от completed Smart Update до ready projection `≤45 min`.

При failure flags остаются OFF; исправляется policy/fixture и весь affected hash-slice replayed заново, без ручной «доклейки» public rows.

## Stage 3 — checked static preview

Собрать immutable noindex preview из exact accepted projection. Проверить `/kluby-po-interesam/`, approved detail pages, event links, redirects, sitemap/canonical/JSON-LD и empty/deferred states.

**GO gate:** manifest relation parity `100%`; approved clubs/events coverage `100%`; non-public identity leakage `0`; broken internal links/redirect loops/dangling event ids `0`; preview canonical leakage into production `0`; mobile + desktop visual smoke, keyboard/a11y smoke и no-JS content pass; second identical build has identical content manifest; failed build оставляет last-good preview доступным.

## Stage 4 — canary

Выбрать 3–5 owner-approved clubs: минимум один rename/alias, один linked-occurrence boundary и один changing/future meeting case; ambiguous co-hosting/festival candidates сначала не включать. Продвинуть exact canary manifest и navigation только для canary allowlist.

**Наблюдение:** минимум один полный scheduled cycle и не менее 24 часов после новой/изменённой встречи.

**GO gate:** public false merges `0`; public relations без valid quote `0`; dangling/stale wrong cards `0`; новая accepted meeting появляется `≤45 min` после successful Smart Update; cancellation/move/merge удаляет или обновляет relation в том же SLO; HTTP 5xx на club routes `0`; static build success `100%`; owner подписал exact RC SHA/catalog/manifest hashes.

Любое нарушение correctness threshold — автоматический NO-GO и rollback, даже если traffic metrics хорошие.

## Stage 5 — production rollout

Расширять allowlist 25% → 50% → 100% confirmed active identities, выдерживая минимум один successful build между ступенями. `probable/needs_evidence` не повышаются автоматически. После 100% сохранить canary metrics семь дней и провести owner audit случайной выборки не меньше 20 relations плюс всех boundary warnings.

**Release complete:** семь дней `0` confirmed false merges, `0` invalid public quotes, freshness p95 `≤45 min`, review queue не старше 48 часов для high-risk cases, rollback drill повторён на текущем build, docs/evidence ссылаются на production SHA и manifest.

## Metrics и alerts

Минимальная telemetry (названия адаптировать один раз к коду, не плодить синонимы):

| Metric | Gate/alert |
|---|---|
| `interest_club_candidates_total{result}` | reconcile exactly with terminal states |
| `interest_club_relations_total{decision}` | positive only for grounded `yes` |
| `interest_club_relation_quote_invalid_total` | must be `0` public; any value pages owner |
| `interest_club_relation_provider_failures_total{model}` | alert on any canary cycle; relation stays deferred |
| `interest_club_review_queue{risk}` | high-risk age `<48h` after full rollout |
| `interest_club_projection_freshness_seconds` | p95 `≤2700` |
| `interest_club_projection_dangling_relations` | must be `0` |
| `interest_club_static_builds_total{result}` | canary success rate `100%` |
| `interest_club_public_false_merges_confirmed_total` | must remain `0`; rollback trigger |
| `interest_club_active/dormant/archived` | explain lifecycle movements; no sudden unreviewed spike |

Logs include run id, event/club ids, packet/policy/model hashes and reason code, never full private payload or credentials. Runtime investigation follows `docs/operations/runtime-logs.md` before claiming logs are missing.

## Rollback and recovery

1. Turn public navigation/promotion OFF; atomically restore last-good static manifest/tree.
2. Turn projection and relation writes OFF; keep evidence/review data read-only for diagnosis.
3. Roll application back to recorded main-reachable SHA. Schema remains additive unless migration itself caused corruption; then execute tested backup/forward recovery.
4. Quarantine affected identities/relations by run/catalog/policy hash; do not delete audit evidence.
5. Rebuild a noindex preview from last-good projection; verify old canonical routes and redirects.
6. Reopen only after replay plus every failed gate passes on a new exact RC.

### Missed scheduled slot / production catch-up

Deploy или rollback не закрывает релизный инцидент, если сегодняшний scheduled Smart Update/static build был пропущен или завершился ошибкой. После фикса оператор обязан:

1. определить потерянное UTC/local time window и exact source/event changes;
2. выполнить compensating changed-only replay с тем же production policy, не full blind rescan;
3. дождаться relation evaluation/review для high-risk packets;
4. запустить coalesced static rebuild/promotion;
5. доказать current-day event coverage `100%`, terminal-state reconciliation, freshness `≤45 min`, dangling `0`, expected club pages/manifest hash;
6. записать rerun id, build id, production SHA и HTTP/UI smoke в release/incident evidence.

Без catch-up и проверки сегодняшних данных release остаётся Partial.

## Postrelease product design: discovery and entry surfaces

Этот track **не входит в первый club production release**. Его нужно спроектировать, проверить на пользователях и принять отдельным product GO; текущий RC не должен молча добавлять эти поверхности.

### Сценарии входа

1. **Navigation.** Отдельный понятный пункт «Клубы по интересам» в discovery navigation; проверить mobile drawer/desktop/footer, archive affordance и отличие от категорий событий.
2. **Search.** Поиск по имени, alias, интересу, городу и устойчивой площадке; результат явно маркирует «клуб», не смешивает identity с одноимённым event/venue и ведёт на stable club URL.
3. **Event detail.** Grounded блок «Это встреча клуба …» только для accepted relation; linked dates остаются «другими датами» события, festival badge не подменяет club relation.
4. **Recommendations.** Неперсональный «Похожие клубы/следующие встречи» сначала строится по approved taxonomy/evidence. Персонализация — отдельный consented scope с guardrails против inferred sensitive interests.
5. **Editorial.** Редакторские подборки («киноклубы», «где играть в настолки») используют canonical ids, показывают дату проверки и не создают relation своим упоминанием.
6. **Deep links.** Stable slug/aliases, campaign/QR/social links и redirects после rename/merge; UTM не влияет на canonical. Removed/private club не раскрывается через старый deep link.
7. **Hero Talk — product concept, не существующий код.** Спроектировать entry surface, в котором hero-блок или короткий guided conversational prompt предлагает сформулировать интерес («Хочу обсуждать кино», «Ищу разговорный клуб») и ведёт к нескольким evidence-backed клубам/встречам. Нужно отдельно решить placement, wording, deterministic vs conversational UX, accessibility/no-JS fallback, analytics, moderation и privacy. До дизайн-ревью нельзя писать в документации или UI, что Hero Talk реализован, использует LLM либо собирает профиль.

### Design/acceptance questions

- какую задачу решает каждая поверхность и не дублирует ли обычный event search;
- кто и как попадает в editorial/Hero Talk выдачу, как оспорить/исправить identity;
- какие empty/stale/archive states понятны пользователю;
- как измерять `surface impression → club detail → future meeting`, не объявляя посещение членством;
- какие данные остаются local/anonymous, для каких recommendations требуется consent;
- как Hero Talk работает без JS и без backend/LLM, если dynamic слой недоступен;
- как не усилить самые частые clubs в ущерб новым региональным identities.

Postrelease delivery требует отдельной спеки, прототипов, accessibility/privacy review, feature flags, canary и метрик по каждой поверхности.
