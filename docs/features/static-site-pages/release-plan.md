# План production-релиза статических страниц событий

> **Срез:** 2026-08-01
> **Решение:** `NO-GO` для переключения event pages на canonical root прямо сейчас.
> **Scope:** production-контур статических страниц событий и переход event-detail
> с Telegraph. Полный релиз всех F1–F17 персональных анонсов остаётся отдельным
> umbrella-gate в [Static personal announcements](../static-personal-announcements/README.md).

## Текущий ledger, 2026-07-27

| Gate | Статус | Текущий факт |
|---|---|---|
| Main-based source | Done for secret candidate | R14 runtime merged; immutable candidate built from main-reachable `161c911f37a9ad52d8b97dd89390c41abeb41908` |
| Production/secret build profiles | Done in code | Production и secret checks существуют; обязателен повтор на замороженном main SHA и свежем DB snapshot |
| Immutable `_review/<token>/` publisher | Done for visual review | Create-only candidate published: 1242 objects, public `46/46` route × viewport gate; это не atomic root promotion |
| Stable URL/lifecycle registry | Missing | Persisted canonical identity, aliases, redirects/410 и cleanup apply ещё не закрыты |
| Freshness/outbox | Partial | Coalesced build/outbox реализованы, но presentation-day freshness и failure drill отсутствуют |
| Telegraph dual-run/public resolver | Missing | D0/D10 outward switch и запрет create/recreate после cutover не доказаны |
| UI/product acceptance | Partial | Frozen public desktop/mobile passed; real OAuth/Edge owner-session smoke и product owner sign-off ещё нужны |

Текущий operational blocker: нормальный Fly → Kaggle запуск останавливается до
старта kernel с `400 INVALID_ARGUMENT: Invalid token` при создании временного
private input dataset. Для визуальной приёмки разрешён только документированный
host fallback: тот же immutable snapshot, чистый main SHA, production exporter,
fingerprint/result validation и публикация исключительно secret/noindex
архива. Такой кандидат не закрывает Kaggle status-ledger, promotion/rollback
или canonical-root gates.
| Schedule freshness | Blocked | Актуальный rail+bus snapshot/manifest и failed-refresh drill не приложены |
| Production root promotion/rollback | Implemented default-off; live blocked | Два полных page-only bucket + ALB state machine, inactive-only reconcile, exact readback, stable smoke и rollback реализованы; buckets/ALB/SWS/DNS ещё не созданы, apply не запускался |

Следовательно, до provisioned ALB разрешены только новый immutable secret
noindex candidate на свежем production snapshot и read-only `plan` atomic-root
publisher против подготовленного inventory. `apply`, DNS и перенос текущих
страниц на canonical root не разрешены.
Исторические controlled runs ниже — regression evidence, а не текущий GO.

### R14 immutable review evidence, 2026-07-27

- build:
  `production-secret-host-fallback-r2-20260727T142927-930012ec`;
- main-reachable source SHA:
  `161c911f37a9ad52d8b97dd89390c41abeb41908`;
- snapshot:
  `snapshot-20260727T110420-r14manual`,
  SHA-256 `6331fe3178250594f3cc73cbf1bd525944baa54a4831252a0d23baaba7e41931`,
  `292651008` bytes, `quick_check=ok`;
- input fingerprint:
  `e4ba87375514dc23b86e1d8cbdfa638ac63f3e229348882b31d8062dbb509bd6`;
- result: `280` eligible/event pages, production and secret generated-output
  gates green, both browser-release gates green;
- secret manifest SHA-256:
  `2929afe1fa15b300d11e8c3ef373d7850eb7a805f0cafb6b6a7f655c750fe349`,
  `1242` create-only objects;
- public acceptance: `46/46` desktop/mobile route checks, zero loaded broken
  images, overflow or script errors; Search input/Enter hint, one global auth
  runtime, Free collection, one artifact placement/five-slot collection,
  clubs sticky header and real medallion specimens were verified;
- root SHA-256 before/after publication:
  `2684c7dd72a265d75b059f43837baecc19ce750f39d962317fc5afec99a75449`;
- bearer URL is excluded from Git and was sent only to topic `548`, messages
  `725–726`.

This closes the secret visual-review lane only. Normal Fly → Kaggle remains
blocked at private-dataset creation (`400 INVALID_ARGUMENT: Invalid token`);
real owner-session Yandex OAuth/Edge Search, schedule freshness, root atomic
promotion and rollback remain open.

## Static selections data-prep track, 2026-08-01

Этот track добавлен в общий release plan, чтобы реализация подборок не осталась
только в исследовательском документе или side-ветке. Исходные требования
неизменно хранятся в [`podborki.md`](podborki.md), полный анализ и product/data
решения — в [`podborki-to-be.md`](podborki-to-be.md). Изначальная
implementation-ветка —
`integration/static-collections-data-prep-20260801`. Код достиг
`origin/main` через PR #182 (`6c870d178b6b22474b56743a36a4b65252c1daa5`) и
развёрнут вместе с актуальным main
`c5e3f6bc79e912992379280644515137917a414d` на Fly runtime v1853. Поэтому ниже
разделены уже выполненные production data-prep шаги и ещё открытые UI/public
gates.

### Что уже сделано в candidate branch

| Gate | Статус | Evidence/граница |
|---|---|---|
| Production data audit | Done, read-only | Fly SQLite 2026-08-01; `integrity_check=ok`; 6 approved clubs/13 grounded relations, 8 theatre organizations, 6 venue pilots; runtime не хардкодит counts |
| Quality PR A | Done, fail-closed | ontology v2; provisional seed отделён от owner gold; EventSource quotes, receipt hashes и occurrence families проверяются `--mode review`; 4 receipt groups остаются `needs_source_review`, 5 semantic labels ниже minimum independent-family supply |
| Club registry refresh | Live; catch-up draining | durable `interest_club_relation` outbox, one successor, evaluation history, provider-deferred retry, shadow discovery, inclusive six-calendar-month v2 projection; 80 exact six-month candidates поставлены в outbox, provider-deferred хвост остаётся durable и не стирает accepted relation |
| Place/organization registry | Done in code | checked-in exact registry, separate theatre/venue roles, 8 official theatres, 6 venue candidates, structured membership reasons |
| Admission/audience/people facts | Done in code | nullable source-bound `Event.collection_decisions`; candidate-only strict LLM schema; `unknown` preserves truth; `Event.is_free` remains compatible bool; no prose `ticket_status` free inference |
| Shared collection BGE | Live cold canary running | evidence-only `collection_semantics_v1`, one float32 BGE-M3 cache, prototype-independent event reuse, one `collection-batch-v1.json`; compute обязателен для production-candidate независимо от Unusual publication flag; run указан ниже |
| Static scheduling | Done in code | strict trailing `latest Smart Update + 15m`, one running + one pending successor; operator/calendar remain immediate |
| Data handoff | Done in code | `collection-batch-v1.json`, `venue-pages-v1.json`, `interest-clubs-static-v2.json`; exact IDs/status/hashes only, Astro does not redefine membership |
| Cinema/festivals boundary | Preserved | no cinema source additions/changes; no festival extraction/page changes |
| Supabase egress | Preserved | core source is the already transferred Fly SQLite snapshot; `supabase_core_reads=0`, no second Kaggle notebook/snapshot |
| Astro routes/navigation/sitemap | Not started by design | belongs to the next UI integration window after data quality gates |
| Production migration/backfill/deploy | Done for data-prep | Fly v1853, exact main SHA; `collection_decisions` live; 4608 legacy evaluation rows сохранены при переходе на history uniqueness; bounded admission/audience/people apply и club enqueue выполнены; финальный successor build поставлен после backfill |

### Production data-prep evidence, 2026-08-01

- pre-deploy online backup:
  `/data/backups/pre-static-collections-20260801T165142Z.sqlite.gz`; source
  SHA-256 `4b54324ae672db9c1f90967ce34b1cc295440dd6637582b2a9c1e8cb4e77ed61`;
- deploy: Fly machine version `1853`, image repository marker and fingerprint
  repository SHA
  `c5e3f6bc79e912992379280644515137917a414d`; `/healthz` после deploy —
  `ok=true`, `ready=true`, DB/schedulers/outbox worker healthy;
- SQLite: `PRAGMA quick_check=ok`; `event.collection_decisions` существует;
  `interest_club_evaluation` содержит `4608` сохранённых до catch-up rows сразу
  после migration и unique history key
  `(club_id,event_id,policy_version,input_hash)` вместо legacy pair key;
- fact plan/apply artifacts сохранены на volume в
  `/data/static_collection_backfill/`. Admission: `6` events/`9` sources,
  `5` применённых решений, `4` source passes без изменения, `0` provider
  deferrals. Audience: `73` sources, `58` применённых изменений, `11` без
  изменения, `4` provider deferrals. People: `38` sources, `29` применённых,
  `1` без изменения, `8` provider deferrals. Provider failure/abstention не
  фабрикует decision и не снимает last-good;
- после apply среди `433` current/future DB rows есть `55` source-bound
  admission decisions (`16 confirmed_free`, `39 confirmed_paid`), `27`
  audience decisions (`18 family`, `9 kids`) и `39` подтверждённых appearances
  в `24` событиях. Это coverage evidence candidate set, а не обещание, что
  каждый current event обязан иметь искусственно заполненный label;
- club catch-up: `80` exact known-identity candidates поставлены в durable
  outbox. После первой волны relations выросли `19 -> 34`; все шесть approved
  identities имеют подтверждённую активность в окне 2026-02-01..2026-08-01
  (`15, 3, 7, 2, 2, 1` active rows соответственно). Provider-limited jobs
  остаются retryable outbox rows; две `shadow` identities не становятся
  публичными автоматически;
- cold production-candidate run:
  `static-site:production-secret-20260801T191228-efb845fd:c1acf5c7d03b`,
  snapshot `snapshot-20260801T171228-1202c99aa1`, input fingerprint
  `caf3b9aa567ec7457e7241b8760b978b0a789dca323e73620d1740ad01186bc0`.
  На момент записи run жив, status ledger получает heartbeat; terminal
  artifact/hash/coverage ещё нельзя считать принятым;
- post-backfill successor `JobOutbox.id=46465` поставлен immediate operator
  request с correlation
  `static-site:manual:static-collections-20260801`. Он обязан взять уже полный
  fact/club state после освобождения single-flight owner.

Astro routes/navigation/sitemap, cinema sources и festival extraction/pages
этим production change не менялись. Public/root promotion также не выполнялся.

Integrated local evidence on the candidate: post-merge collection/semantic/
release `123 passed`; post-merge club/outbox/Smart Update/facts/DB `131 passed`;
May-incident replay is included in the earlier `85 passed` focused facts run;
Kaggle status/handoff/unusual/outbox `116 passed`. Exact commands, lane
SHAs, merge reconciliation and final checks live in
`.codex/integration/static-collections-data-prep-20260801-INTEGRATION_REPORT.md`.
Local tests do not substitute for the real Kaggle gate.

### Обязательная последовательность до UI и public rollout

1. Merge candidate into a fresh main-based integration only after the final diff
   audit; retain the generated-manifest boundary and both additive migrations.
2. On a verified production copy, run the `Database.init()` upgrade/preservation
   check, read-only plans from both backfill scripts, then bounded `--apply` for
   admission conflicts, audience/people candidates and six-month club relations;
   do not mass-extract the historical archive.
3. Run one current-catalog pinned Kaggle CPU cold build and an identical warm
   build. Require complete catalog coverage, `provider_calls=0`, unchanged event
   re-encode `0`, exact cache/receipt/batch hashes, no second notebook and no new
   Supabase core reads.
4. Keep Unusual/science-pop/research-in-action/strong-impressions/medieval and
   audience-v2 heads blocked. PR A review gate не заменяет PR-B owner gold,
   all-event scores, temporal holdout и evidence-only recalibration; old
   related-v1 canary не принимает новый document contract.
5. Produce an immutable noindex secret candidate containing the exact manifests.
   Verify 8 theatres, all currently eligible approved clubs, the 6 venue pilots,
   admission corrections/review abstentions and per-label failure states.
6. Only in the next UI branch add collection/venue routes, medallion schedule
   pages and navigation. Sitemap/indexability is a separate gate: pilot venue
   pages are indexable only after content/relation/robots acceptance; raw-location
   mass pages are forbidden.
7. Promote labels separately. Exact existing shelves may ship first; children/
   visiting guests follow grounded-fact backfill; semantic heads follow owner
   gold. `Для меня` remains a separate personalization delivery track.

### NO-GO / rollback conditions for this track

- missing/partial collection batch, cache self-hash mismatch or event ID outside
  the frozen catalog;
- any new semantic label reported `ready` without owner gold and quality pass;
- provider failure erases accepted admission/club truth;
- automatic build starts before the 15-minute quiet window or starts a second
  concurrent StaticSiteBuilder;
- unexpected Supabase core read, second Fly snapshot transfer/notebook, cinema
  source change or festival extraction/page coupling;
- Astro computes membership from prose/topics instead of consuming the accepted
  ID-only manifests.

Rollback before public UI is simply no merge/no migration/no promotion. After a
future label is public, failed data-prep must retain the previously deployed tree
or block candidate promotion; it must never silently publish a disabled/empty
replacement. The open Unusual incident
[`INC-2026-08-01-unusual-feed-disabled-by-config`](../../reports/incidents/INC-2026-08-01-unusual-feed-disabled-by-config.md)
remains the regression contract until a current accepted route exists.

## Где находится release truth

Единственная release-база — `origin/main`. Side branches, опубликованные preview и
локальные dirty checkout считаются только evidence/WIP, пока изменения не достигли
`origin/main` и не получили production evidence.

Целевой контур:

```text
immutable Fly SQLite snapshot
  -> coalesced static_site_build after effective Smart Update
  -> Kaggle CPU checked artifact
  -> immutable review candidate + static_release_manifest_v1 root proof
  -> inactive complete root bucket
  -> Yandex ALB old/new complete-tree weight convergence
  -> retained previous bucket and verified rollback
```

Код production-root publisher находится в `static_site_atomic_root.py`, а
единственный setup/plan/apply/rollback runbook — в
[`docs/operations/static-site-atomic-root.md`](../../operations/static-site-atomic-root.md).
Он намеренно не копирует файлы в active root и не создаёт browser pointer.
`_finish_static_site_candidate` вызывает его только после result/root/candidate
gates и успешной immutable review publication, только при явном
`ENABLE_STATIC_SITE_ROOT_PROMOTION=1`. Текущий default `0`; `plan` не пишет ни
objects, ни ALB config. До live inventory, SWS/ARL/WAF, failure drill и DNS
приёмки статус остаётся `NO-GO`.

## Реализованный secret-candidate этап

Текущий этап намеренно останавливается до root promotion:

```text
effectful Smart Update / durable operator request
  -> last-effect + 15 min debounce
  -> immutable SQLite online backup + quick_check + SHA-256
  -> unique Kaggle CPU input + static_site:builder lease/status ledger
  -> checked full-catalog root-form artifact
  -> separately checked noindex/no-referrer candidate
  -> create-only /_review/<256-bit-token>/ upload
  -> verified bearer URL; root/current/stable ICS unchanged
```

`static_site_build:prod` сохраняет bounded reasons, event ids/revisions,
correlation ids и target watermark. Running owner не поглощает следующий effect:
он получает ровно один pending follow-up с объединённым payload. Automatic и
manual path используют один outbox; ручной entrypoint —
`scripts/request_static_site_build.py`.

Kaggle production result связывает exact pushed repo SHA, run/build/snapshot ids,
snapshot hash/size, catalog ledger, checks, counts и оба archive hashes. Publisher
повторно проверяет result/manifest/tree и допускает только новые keys внутри
одного `_review/<token>/`; root, `current.json`, release-control objects и
стабильные `/ics/*` невыразимы через его API.

После успешного readback/public probe единственный внутренний current-review
receipt сохраняется в `static_site_build_state`. Он hash-связывает bearer URL с
build/run/repo/snapshot/result/manifest и используется всеми link-producing
paths через `resolve_current_secret_candidate`. Failed/no-op/artifact-only run
не сдвигает pointer; неполный receipt fail-closed. Это не публичный redirect и
не production `current` object. Оператор читает его командой
`scripts/request_static_site_build.py --db /data/db.sqlite --show-current-review`.
Все именованные `preview-…` ссылки в документации — только историческое
evidence.

Secret URL — **не авторизация**. До его первой публикации anonymous ListObjects
для bucket должен отвечать `403`, сохраняя public object read. `noindex`,
`no-referrer` и `Cache-Control: private, no-store` уменьшают утечки, но не мешают
получателю переслать ссылку.

### Диагностика, dual-channel и архив

`scripts/static_site_build_diagnostics.py` формирует read-only redacted отчёт:
24h outcomes, current channel pointers, event/page/file/object/byte counts и
history ↔ Kaggle-ledger orphans. Bearer URL/tokens не выводятся.

После production GO Smart Update не запускает два тяжёлых build. Один immutable
snapshot/content cohort создаёт secret noindex и root-form production artifact;
у них независимые publication state/pointers. Content, artifact и publication
fingerprints разделены; failure одного publish lane не сдвигает другой.

Past-event policy по Google Search Central, Schema.org и RFC 9110:

- полезное завершившееся событие сохраняет тот же URL/HTTP 200, truthful dates,
  видимую метку `Событие завершилось · Архив` и не показывает stale purchase CTA;
- выдуманного `EventCompleted` нет; cancellation/postponement/reschedule используют
  документированные `EventStatusType` и previous dates;
- реальный 1:1 move получает 301/308 (redirect обычно хранится минимум год), а
  permanent removal без близкой замены — настоящий 404/410, не soft-404/home redirect;
- sitemap содержит только canonical indexable URLs. Фиксированного SEO-срока
  хранения past-event pages нет: полезные сохраняются, thin/duplicate/empty
  периодически удаляются;
- secret/test prefixes не входят в sitemap/internal links и всегда noindex +
  no-referrer. Non-current test prefixes удаляются только manifest-bound;
  current/last-good/pinned evidence защищены;
- до stable release обязателен URL lifecycle registry для current/previous/
  retained/expired/deleted, aliases, redirects/410 и двухфазного cleanup plan/apply.
  Последовательный root-wide copy/delete запрещён.

Retain-in-place и archive hubs — продуктовая inference из этих правил, а не
отдельное предписание Google.

Primary references: [Google event structured data](https://developers.google.com/search/docs/appearance/structured-data/event),
[structured-data policies](https://developers.google.com/search/docs/appearance/structured-data/sd-policies),
[redirects](https://developers.google.com/search/docs/crawling-indexing/301-redirects),
[site moves](https://developers.google.com/search/docs/crawling-indexing/site-move-with-url-changes),
[sitemaps](https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap),
[`noindex`](https://developers.google.com/search/docs/crawling-indexing/block-indexing),
[Schema.org EventStatusType](https://schema.org/EventStatusType) и
[RFC 9110 §15.5](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.5).

Флаги по умолчанию выключены:

```text
ENABLE_STATIC_SITE_KAGGLE_BUILDER=0
ENABLE_STATIC_SITE_SECRET_PUBLISH=0
```

При включении обязателен точный `STATIC_SITE_REPO_SHA`. Production root command и
root promotion flag отсутствуют. Это закрывает candidate isolation, но не
`ADD-BUILD-10 reader-atomic root promotion`.

### Controlled evidence 2026-07-17

Финальный контролируемый CPU-run выполнен на pushed implementation SHA
`bb34a89540e707a1580fd31b75f6b8e680a86505`:

- build `production-20260717t-controlled-kaggle-final`, run
  `static-site:controlled-kaggle:20260717-final`;
- snapshot `snapshot-20260717t-controlled-secret`, SHA-256
  `78905f8e4b8c36c925a6a5611fd418380b85f48b55785b8ce1f1b7d5459d1ca8`,
  `quick_check=ok`, `263331840` bytes;
- `307` eligible events, `307` event pages, `993` files; production and secret
  checks green, sparse related state explicitly `optional_degraded`;
- bounded result SHA-256
  `f976ee86eaf2a98dcbbcb81c325863bb40a01ab16488752310167ed25db1ecc9`,
  secret artifact SHA-256
  `84af8bca9b1dc271b5f3bb4ae342d5fa7e0c248b472e864adccaf485cfebe822`,
  published manifest SHA-256
  `a20ebb3f748b3961204cc5cd8cb2ff29795e8f698e529e37f8c118498a2ffdc7`;
- anonymous ListObjects changed from `200` to `403`, while anonymous object read
  and the existing root stayed `200`;
- authenticated upload/readback verified all `994` candidate objects; Playwright
  verified all three forced timetable arms at `320/390/768/1366`, one visible arm,
  zero horizontal overflow/console errors and boarding at `Северный вокзал`;
- protected root body SHA-256 remained
  `e2ddecb6c2856a94d4579a3091604b7c0804f3545220f43e94eac73e0aab450d`,
  stable `ics/4671.ics` remained
  `7b6ccebab5f4ff3c53fcdedcd06279a94236709902544b19b828a17e812041f5`.

Bearer URL/token are deliberately absent from Git. Ссылка передана только в
Telegram Saved Messages; первая candidate, на которой browser gate обнаружил
unresolved external Astro asset prefix, отозвана полностью (`994` objects,
remaining `0`) до handoff. Production root/current promotion не выполнялась.

Replacement template-parity run on 2026-07-17 uses pushed SHA
`0fcc92223790f293b2f12137f3a99754ba2f30e1`, snapshot
`snapshot-20260717t-template-parity-v11` (`quick_check=ok`, SHA-256
`3105191211ed79a3e34284c8d4c01a4e5631b6adbb62512035b0f00ef120d443`,
`263548928` bytes) and build
`production-20260717t-template-parity-v11-kaggle-v2`. It generated `307`
event pages / `1119` files, pinned `static-event-detail-v11`, passed production
and secret checks, and published `1120` create-only candidate objects. Public
Playwright passed `30/30` representative page/viewport combinations covering
all production media families plus phone CTA and three transport treatments.
The obsolete wrong-template candidate was revoked (`994` objects, remaining
`0`, public HTTP `404`). The replacement bearer token remains outside Git;
production root stayed byte-identical and root promotion remained disabled.


На базе среза до этой реализации общий event-page контур был preview-only. Ниже
сохранён исходный audit gap как regression context:

- production build/check profile отсутствовал;
- event HTML, `robots.txt` и sitemap всё ещё имеют preview/noindex semantics;
- Kaggle job получал checked tarball, но не выполнял production manifest,
  staging promotion и rollback;
- canonical event slug всё ещё вычисляется из изменяемых title/city/id вместо
  persisted publication registry;
- Telegraph остаётся обязательной зависимостью части event publication flows.

Production canary клубов по интересам от 2026-07-17 не меняет этот вывод: это
ограниченный root overlay из семи файлов, а не promotion полного event catalog.

## Что изменилось за 15–17 июля 2026

| Изменение | Состояние | Учитываем в релизе |
|---|---|---|
| Desktop Editorial event experience и fail-closed media roles (`58abfb19`) | merged в `origin/main` | Да; нужен явный regression scenario, но это не закрывает production publish |
| Declared/assessed age-rating data path (`aa95900a`) | merged в `origin/main` | Да; проверять parity карточки/detail/JSON-LD/export, не считать автоматическую оценку публичной маркировкой |
| Social popularity batches и owned-channel aggregation (`fe211a88`, `d25b15d6`, `b34a97d3`) | merged в `origin/main` | Да; owned reposts не должны суммироваться как независимые аудитории |
| Telegram RichMessage medallions (`14e25b43`) | merged в `origin/main` | Смежный social surface; не является static release gate |
| Gated interest-club projection/pages и production canary (`98180d1e`, `6b234a52`, evidence `6cdae545`) | merged и canary live | Да как дополнительный consumer общего checked build; семь дней наблюдения ещё идут |
| Atomic event-site publisher (`62ba7110`) и последующее hardening | только side/local branches | **Нет:** не считать реализованным, пока не перенесён на свежую main-based ветку, не проверен и не слит |
| Transport, mobile-v8, saved-event identity, последние personalization/media fixes | side branches | **Нет:** branch evidence, не release truth |
| Подробный каталог тестов `test-scenarios.md` | добавлен этим изменением | Да как routed inventory; большинство сценариев ещё не автоматизировано |

Итог аудита: feature-документы хорошо описывают отдельные slices, но до этого
изменения не было одного актуального event-page release plan; routing не ссылался
на подробный test inventory, а E2E index не различал draft/demo/release E2E.

## Release gates до D0

`D0` нельзя назначать календарно, пока не закрыты все P0-gates:

1. **Production profile:** отдельные `build:production` и `check:production`, root
   canonical URLs, indexable robots, sitemap без preview/lab routes, полный eligible
   catalog; preview profile остаётся noindex и неизменным.
2. **Stable page identity:** persisted slug/revision/publication registry, aliases,
   redirect/tombstone/retention contract; title/location edits не меняют canonical URL.
3. **Safe publisher:** immutable release prefix, signed/hashed manifest, catalog
   parity, atomic promotion, failed-candidate isolation, retained last-good release и
   проверенный rollback.
4. **Freshness/outbox:** update B во время долгого build A гарантирует ровно один
   follow-up build более нового snapshot; есть max-staleness alert и catch-up runbook.
5. **Downstream decoupling:** Telegram/VK/import/admin flows используют единый
   public-page resolver и не требуют успешного `telegraph_build`, когда static page
   уже ready.
6. **Acceptance evidence:** автоматизируемый RC subset из
   [test-scenarios.md](test-scenarios.md) прошёл на clean main-reachable SHA; native
   share/calendar/maps/unfurl проверки приложены вручную там, где mocks недостаточны.

Release owner фиксирует точные `T0` и `T0+10 days` в UTC и
`Europe/Kaliningrad`, production SHA, snapshot id, build id, manifest hash и rollback
target. Формулировка «через 10 дней» без этих полей не является scheduled cutover.

## Десятидневный Telegraph coexistence

### Конфигурационный контракт

```text
EVENT_PUBLIC_PAGE_MODE=telegraph|dual|static
TELEGRAPH_EVENT_WRITE_MODE=create_edit|existing_only|off
STATIC_SITE_CANARY_PERCENT=0..100
```

- `EVENT_PUBLIC_PAGE_MODE` выбирает outward URL, а не сам факт записи.
- Static URL разрешён только если current promoted manifest содержит нужные
  `event_id` и source revision/hash.
- `existing_only` разрешает при необходимости обновлять уже существующую страницу,
  но запрещает create и fallback-recreate после ошибки edit.
- `off` запрещает любые event-detail Telegraph API writes, но не очищает сохранённые
  `telegraph_url`/`telegraph_path`.
- Aggregate month/weekend/festival Telegraph pages имеют отдельный режим и **не
  выключаются D10** до появления эквивалентных static surfaces. Текущий D10 scope —
  только event-detail pages.

### График после фактического T0

| День | Режим | Gate |
|---|---|---|
| D0 | full static release promoted; `dual`; static links 10%; Telegraph `create_edit` | 100% catalog parity, public canonical/robots/sitemap/JSON-LD/OG/ICS smoke |
| D2 | static links 25% | нет broken outward URLs; freshness и build health в target |
| D4 | static links 50% | sampled Telegram/VK/MAX unfurl не хуже принятого baseline |
| D6 | static links 100%; Telegraph всё ещё shadow-created/edited | каждый outward static URL подтверждён current manifest |
| D7–D9 | 72-hour soak на 100% static links | `0` release-critical errors, reconciliation всех eligible events/surfaces |
| D10 | `EVENT_PUBLIC_PAGE_MODE=static`; `TELEGRAPH_EVENT_WRITE_MODE=existing_only` | create/recreate attempts after cutoff = `0`; legacy URLs сохранены |

Старые Telegram/VK посты массово не редактируются, старые Telegraph URLs не
удаляются. Позднее отдельным решением можно перевести `existing_only -> off`.

### Go/no-go и rollback

Минимальные D10-инварианты:

- eligible event catalog parity `100%`, ineligible leak `0`;
- static-ready before outward link emission `100%`;
- HTML/assets/ICS success `>=99.9%` в soak-окне;
- preview/noindex/canonical leakage `0`;
- freshness p95 `<=30 min`, max `<=60 min` после due time;
- broken outward links `0`;
- `telegraph_create_attempts_after_cutover=0` и
  `telegraph_recreate_attempts_after_cutover=0`.

До D10 rollback возвращает outward mode в `telegraph`/`dual` и current static pointer
на last-good. После D10 emergency rollback может временно вернуть `create_edit`, но
только явным операторским решением и bounded backfill пропущенных eligible events.
Ни один rollback не очищает legacy Telegraph fields.

## Top-5 задач, которые можно запускать сейчас

Задачи не включают проектирование UI листингов или event detail.

| Priority | Задача | Зависимости | Acceptance |
|---|---|---|---|
| P0-1 | **Production build profile** | нет | root output; production canonicals; indexable robots; sitemap без preview/lab; full eligible catalog; `check:production` green; preview profile unchanged |
| P0-2 | **Release manifest, staged promotion и rollback** | P0-1 | immutable prefix; SHA/snapshot/counts/checks в `static_release_manifest_v1`; failed candidate не меняет current; one-command verified rollback; release/lease evidence |
| P0-3 | **Stable event URL и lifecycle registry** | может идти параллельно P0-1; нужен P0-4 | persisted slug; aliases; redirect/410/retention rules; merge/delete/update idempotence; sitemap содержит только canonical eligible URLs |
| P0-4 | **Telegraph dual-run и public-link resolver** | P0-1..3 | три режима tested; static URL только после readiness; downstream не зависит от Telegraph; D10 создаёт/recreate `0`; legacy URLs сохранены |
| P0-5 | **Observability и automated acceptance pack** | contracts можно начать сразу; full E2E после P0-1/2 | catalog/freshness/resolver metrics; `ADD-CUTOVER-*` + release subset automated; 72-hour gate report; rollback drill evidence |

## Test/evidence contract

Канонический каталог сценариев: [test-scenarios.md](test-scenarios.md).

Важно: наличие ID не означает, что сценарий реализован или пройден. На дату среза:

- `npm --prefix site run build:preview && npm --prefix site run check:preview` —
  сильный fixture/build-time gate, но не production E2E;
- `tests/test_static_site_public_gate.py`,
  `tests/test_static_site_build_handoff.py` и related exporter tests — узкие
  unit/contracts;
- `tests/playwright/static_personalization_contract.spec.ts` — standalone demo
  contract из 9 tests, не Astro/public-site release E2E;
- `tests/e2e/features/static_site_personalization.feature` имеет `@draft` и пока не
  имеет Behave step definitions;
- atomic promotion/rollback, production-root browser/HTTP, 10-day cutover и native
  device flows ещё требуют реализации/evidence.

Полный F1–F17 readiness и release evidence pack остаются в
[аудите 2026-07-11](../../reports/static-personal-announcements-release-readiness-2026-07-11.md);
этот документ не ослабляет его gates.

## Separate Stage 13 — «Пасхалки о Калининграде»

Research-механика может проверяться до D10 только в явно включённом immutable
noindex/secret candidate и не входит в current static-page GO. Обычный
production/root обязан fail-closed. Production-включение возможно только после
стабильного D10 и отдельного RC. Механика не меняет текущий Top-5. План: product research →
owner decisions → clickable accessible prototype → first-class egg/progress and
`site_easter_egg` promo-activity architecture → scheduler shadow/admin report →
одна non-prize collection canary с holdout и automatic stop rules → ship/narrow/stop
review. Материальный приз и social-share multiplier не наследуют этот релиз и
требуют отдельного legal/anti-abuse решения. Полный contract:
[static-site easter eggs](../static-site-easter-eggs/README.md).
