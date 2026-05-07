# INC-2026-05-07-vk-auto-import-merge-regression-gemma4 VK auto-import duplicate after Gemma 4 draft parse

Status: open
Severity: sev2
Service: VK auto-import → Smart Update merge guard
Opened: 2026-05-07
Closed: —
Owners: events-bot maintainers
Related incidents:
- `INC-2026-04-20-club-znakomstv-duplicate-event-cards.md` — prior duplicate-card class on the same Smart Update surface.
- `INC-2026-05-01-future-event-quality-audit.md` — future event quality regression contract for duplicate guards.
- `INC-2026-04-28-vk-smart-update-false-skips.md` — last touched the smart_update VK code path.
Related docs:
- `docs/llm/smart-update-lollipop-gemma-4-migration.md` — canonical Gemma 4 migration contract.
- `docs/features/vk-auto-queue/README.md` — `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL` rollout.
- `docs/features/smart-event-update/README.md` — Smart Update merge guard contract.

## Summary

Сегодня в production VK auto-import создал duplicate `event` row 4679 (`Портрет девушки-бойца`, 2026-05-08 19:00, `Научная библиотека`, ticket_link `https://vmuzey.com/event/portret-devushek-boycov-melodii-smelyh-serdec`) при наличии существующего `event` 4445 (`Концерт «Портрет девушки-бойца»`) с **идентичными** date/time/location/ticket_link, импортированного 2026-04-30 из Telegram (`t.me/kraftmarket39/204`). Smart Update merge guard должен был склеить новый VK-источник в 4445 как `merged=True`, но создал новый event и затем подцепил оба через `linked_event_ids` пост-фактум. Это нарушение базовой инвариантности «один реальный ивент — одна public card», которая закреплена в `INC-2026-04-20-...` и `INC-2026-05-01-future-event-quality-audit.md`.

Регрессия совпадает по времени с rollout `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL=models/gemma-4-31b-it` (commit `69ab6e8c`), при этом prompt-family для Gemma-backed VK draft parse **не была переписана** под Gemma 4 контракт (`system + user`, tighter schema), что прямо запрещено каноническим migration doc.

Дополнительные регрессии того же rollout, подтверждённые в новом раунде live-тестирования 2026-05-07 19:03–19:26 KGD на `/data/runtime_logs/events-bot.log` (file mirror только что включён):

- **Hallucinated venue (event 4681)**: Gemma 4 vk_intake вернул `location_name='Киноленд', location_address='Киевская 71', city='Калининград'` для поста про «бывшую пивоварню Понарт» (`wall-211696971_5570`). В source_text слова «Киноленд» нет, есть только «На Понарте!!!» и «комплекс пивоварни Понарт». Канонический ряд `Понарт, Судостроительная 6, Калининград` существует в `docs/reference/locations.md:43`, но vk_intake его проигнорировал. Это второй класс контентной регрессии — теперь по location, не только по title (4677/4679).
- **Phone-only contact lost (event 4681)**: единственная точка записи в источнике — телефон `8-967-356-9479 Николай`. Текущие writer-prompt'ы запрещают телефоны в description/facts как logistics, и поскольку URL ticket_link нет, конечная карточка не имеет ни кликабельного `tel:`-ссылки, ни упоминания контакта. Пользователь не может записаться.
- **Multi-event digest contamination + provider 500 stall (post `wall-194968_17449`)**: пост-роспись из 4 спектаклей по строчке (`<дата>. <город>. <"НАЗВАНИЕ">. Билеты: <ссылка|имя>`). Gemma 4 vk_intake свернул его в один кандидат `🎭 ПИРОСМАНИ` с противоречивыми anchors (`city=Пятигорск` + `location_name='Театр Третий этаж, Коммунальная 6, Калининград'`). На этапе merge Gemma 4 нагаллюцинировала программу несуществующего концерта (`Бах — Токката`, `Моцарт — Симфония №40`, `Бетховен — Симфония №5`) и предложила ретитл существующего event 3979 «Пиросмани» → «Концерт `«Музыка Времён»`». Существующие fact-/title-grounding guards отработали (`smart_update.fact_rejected ungrounded_sensitive_fact`, `smart_update.title_rejected`), но затем Gemma merge call упал в `500 INTERNAL` и держал retry ~15 минут (`duration_ms=920763`), потом fall back на `gpt-4o`. После fallback pipeline не дописал `vk_inbox.status` обратно — row застрял в `pending`, attempts=0, и каждый следующий `/vk_auto_import 1` снова поднимал тот же пост. Production-blocker устранён вручную (см. Immediate Mitigation), но базовая проблема — отсутствие hard timeout governance в `_ask_gemma_json` + отсутствие ранней digest-detection в vk_intake — остаётся.

## User / Business Impact

- Пользователь видит два telegraph-карточки и две `event` row для одного концерта: 4445 (`Koncert-Portret-devushki-bojca-04-30`) + 4679 (`Portret-devushki-bojca-05-07`). 4679 даже сохранил VK-источник, но без merge.
- В `4679.description` writer вынес «Концерт проходит в рамках просветительского проекта `&quot;Оперный класс&quot;`» — HTML entity `&quot;` утёк в public Telegraph через `simple_md_to_html(html.escape(...))` потому, что VK source_text не нормализован `html.unescape` до prompt input, а Gemma 4 более verbatim-grounded и не «съел» entity, как съедал Gemma 3.
- Smart Update report операторам показывает `✅1 🔄0` при том, что должна была быть `✅0 🔄1`.
- Ломается инвариант «один ивент — одна public card», который зафиксирован как regression contract в `INC-2026-05-01-future-event-quality-audit.md`.

## Detection

- Замечено оператором в ходе live-проверки `/vk_auto_import 1` в Telegram bot UI (run в 17:19 KGD = 15:19 UTC, см. `ops_run.id=1080`, batch `auto:1778167157`).
- Авто-сигнала нет: smart_update result `status="created"` not flagged как duplicate. Текущая observability не включает «pre-create dup probe» по `(date,time,location,ticket_link)`.
- Связанный симптом: `&quot;` HTML entity в `event.description.4679` — автоматических guard на HTML entities в публичном тексте нет.

## Timeline (UTC)

- 2026-04-26 08:15 — commit `69ab6e8c vk auto queue: route draft parse to gemma 4`. `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL` переведён на `gemma-4-31b-it` без family-prompt rewrite.
- 2026-04-30 23:33 — Telegram Monitoring создаёт `event` 4445 (`Концерт «Портрет девушки-бойца»`) из `t.me/kraftmarket39/204`.
- 2026-05-07 15:18:42 — `vk_inbox` row 6730 для `wall-30777579_15151` создан (post date 2026-05-07 12:26 UTC).
- 2026-05-07 15:19:17 — `ops_run.id=1080 vk_auto_import` старт (manual, operator 185169715).
- 2026-05-07 15:22:24 — Smart Update создаёт `event` 4679 как `created` (НЕ `merged`).
- 2026-05-07 15:22:30 — `ops_run.id=1080` finished `success`. metrics `events_created=1, events_updated=0`.
- 2026-05-07 ≈15:25 — `linked_events` post-process связывает 4445 ↔ 4679 (linked_event_ids), но это уже cosmetic, не merge.

## Root Cause

Текущая гипотеза с rank-ordered confidence:

1. **Primary (high confidence): VK draft parse на Gemma 4 без family-prompt rewrite даёт candidate field shape, который ломает existing merge guards.** Канонический migration doc `docs/llm/smart-update-lollipop-gemma-4-migration.md` явно запрещает «переносить legacy Gemma-3 prompt style как есть» при переходе на Gemma 4 (`system`/`user` separation, tighter schema, examples rewrite). Commit `69ab6e8c` поменял только model id, не trim/role-split prompt. Без переписанной prompt-family Gemma 4 может выдать `candidate.title` (e.g. без слова «Портрет», или сразу festival-shaped `«Оперный класс»: концерт ...`), при котором `_titles_look_related(candidate.title, event.4445.title)` возвращает False → `_single_candidate_auto_match_ok` возвращает False → `_deterministic_exact_title_match` тоже не fire → ticket-anchor не fire (`_deterministic_ticket_source_anchor_match` требует ticket-link match + дополнительные signals) → `_llm_match_or_create_bundle` (всё ещё на Gemma 3, single big-blob user prompt) выдаёт `action=create` с низкой confidence.
2. **Secondary (medium confidence): VK source_text не проходит `html.unescape` перед prompt input.** [vk_intake.py / vk_auto_queue.py не вызывают `html.unescape`](vk_intake.py); `unescape_public_text_escapes` в [markup.py:41-63](markup.py#L41-L63) снимает только `\\"`/`\\n`/`\\t`, но не HTML entities (`&quot;`, `&amp;`, `&lt;`, `&gt;`, `&nbsp;`). Gemma 4 более grounded ⇒ entity проходит насквозь до writer и до `simple_md_to_html`. Gemma 3 эту нормализацию делал «по-себе» как побочный эффект rephrasing.
3. **Tertiary (lower confidence): Gemma 4 vk_intake может выдавать иной `time_is_default` сигнал** (e.g. `True` для `19:00` если post text выглядит как cycle promo), что обнуляет `_candidate_anchor_time` и снимает ticket-link strong-match с дополнительного `time_anchor` confirm. По данным db, `4679.time_is_default=0`, поэтому это **скорее всего исключено**, но окончательно подтвердить можно только на свежем prod-логе.

## Contributing Factors

- `ENABLE_RUNTIME_FILE_LOGGING="0"` в `fly.toml` — runtime file mirror отключён, поэтому через ~30 мин detail-логи `smart_update.shortlist`, `smart_update.match`, `match_create_bundle`, candidate JSON уже потеряны для 17:19 run. Не получается ретроспективно достать `candidate.title` который Gemma 4 выдала.
- Rollout commit `69ab6e8c` поставил Gemma 4 в production без LLM-first family-prompt audit, без отдельного A/B на VK auto-import duplicates, без observability на `match_reason` distribution.
- Smart Update merge guard не имеет «pre-create duplicate probe» по `(date,time,location_name,ticket_link)`: даже если LLM ошибся, отдельный sanity-check мог бы поймать identical ticket-link на той же дате/площадке.
- В тексте post `«Портрет девушки-бойца»` source_text содержит cycle marker `«Оперный класс»`, что заставляет VK draft parse классифицировать пост как festival-context. На прошлой неделе уже был связанный fix `INC-2026-04-28-vk-smart-update-false-skips.md` с похожей рамкой festival-context routing.

## Automation Contract

### Treat as regression guard when

- любые изменения в `vk_intake.py`, `vk_auto_queue.py`, `smart_event_update.py` (особенно `_match_existing_event_by_*`, `_single_candidate_auto_match_ok`, `_llm_match_or_create_bundle`, prompt families);
- любые изменения `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL`, `SMART_UPDATE_MODEL`, `EVENT_PARSE_GEMMA_MODEL` env;
- любые изменения prompt families в `docs/llm/prompts.md`, `docs/llm/smart-update-*`;
- любые миграции upstream stage на новый Gemma checkpoint.

### Affected surfaces

- `vk_intake.py::vk_intake_parse_llm` / `build_event_drafts_from_vk` (Gemma 4 draft parse).
- `vk_auto_queue.py::_vk_auto_parse_gemma_model` / `_process_vk_inbox_row`.
- `smart_event_update.py::smart_update_event` matching block 9932-10402.
- `smart_event_update.py::_llm_match_or_create_bundle` 6834-6995.
- `markup.py::unescape_public_text_escapes` / `simple_md_to_html` (HTML entity boundary).
- env: `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL`, `SMART_UPDATE_MODEL`, `ENABLE_RUNTIME_FILE_LOGGING`.
- production db.event rows for 4445, 4679; future VK auto-import rows on libraries / cycle-context venues.

### Mandatory checks before closure or deploy

- репродукция парса `wall-30777579_15151` source_text локально на `gemma-4-31b-it`, dump candidate JSON, подтверждённый match against existing event 4445 (`single_candidate` или `strong_match`/`deterministic_ticket_source_anchor`).
- репродукция парса `wall-212760444_4953` (event 4677) — title должен содержать format-anchor `Мастер-класс`.
- HTML entity smoke: writer на любом VK source_text с `«…»` НЕ должен оставлять `&quot;` / `&amp;` / `&lt;` / `&gt;` в `event.description`.
- production run `/vk_auto_import 1` на ≥3 разных постах подряд без duplicate-creation.
- 4679 либо merged в 4445 (предпочтительно), либо явно архивирован/обнулён как cancelled; ICS / month-page / day-page / digest / `/daily` rebuild для затронутых rows.

### Required evidence

- deployed SHA fix-commit'а;
- prod sqlite дамп `event WHERE id IN (4445,4679)` + `event_source` показывает либо merge либо явный split-clean;
- prod log выдержки с `smart_update.shortlist`, `smart_update.match`, `match_create_bundle` для контрольного run (включить `ENABLE_RUNTIME_FILE_LOGGING=1` хотя бы временно для evidence);
- regression test: новый pytest, который кладёт в prod-shaped fixture event-row 4445-like + candidate-shaped JSON и проверяет, что `smart_update_event` возвращает `merged=True`, а не `created=True`.

## Immediate Mitigation

- 2026-05-07 — `fly secrets set ENABLE_RUNTIME_FILE_LOGGING=1` применён на app `events-bot-new-wngqia`; rolling restart healthy (machine `48e42d5b714228`); `/data/runtime_logs/events-bot.log` подтверждён через `env` и `ls`.
- 2026-05-07 — duplicate event 4679 архивирован: `lifecycle_status='cancelled', silent=1`, VK source `wall-30777579_15151` перенесён в 4445 (`event_source.event_id=4445`); telegraph/month/week/weekend rebuild jobs enqueued; backup в `incident_vk_merge_regression_backup_20260507`. Артефакт: `artifacts/codex/INC-2026-05-07-vk-merge-regression-gemma4/inc_vk_merge_regression_remediation_20260507.json`.
- 2026-05-07 — production blocker `vk_inbox.id=6728 (wall-194968_17449)` разблокирован: `status='rejected'`, `locked_by=NULL, locked_at=NULL, review_batch=NULL`. Backup сохранён в `incident_vk_merge_regression_backup_20260507(kind='before_vk_inbox_6728_unblock')`.
- (proposed, not yet applied) временно переключить `VK_AUTO_IMPORT_PARSE_GEMMA_MODEL` обратно на `gemma-3-27b-it` через `fly secrets`, если новые duplicates / hallucinated locations продолжат появляться в течение текущих суток до prompt-family rewrite.

## Corrective Actions (proposed)

LLM-first, в порядке приоритета:

1. **Prompt family audit & rewrite for VK draft parse под Gemma 4**: `system`/`user` separation, tighter title schema (требовать формат-anchor `мастер-класс/лекция/спектакль/концерт`, если он явно присутствует в source), стабильный `location_name` shape (не дописывать адрес/город в `location_name`), стабильный `time_is_default` сигнал. Покрыть positive/negative examples из текущих событий 4677/4679. См. `docs/llm/smart-update-lollipop-gemma-4-migration.md` § «Prompt-contract deltas для Gemma 4».
2. **Pre-create duplicate probe in `smart_update_event`**: deterministic guard, который ПОСЛЕ create-decision и ДО `INSERT event`, проверяет shortlist на `(date,time,location_name,ticket_link)` exact-match и если совпадение есть — переключает результат на merge с явным `match_reason="post_decision_ticket_anchor"`. Это второй уровень защиты на случай, когда LLM matcher промахивается при changed candidate shape.
3. **HTML entity normalization at boundary**: добавить `html.unescape(...)` в нормализацию VK `source_text` (там же, где `unescape_public_text_escapes`). Без этого Gemma 4 продолжит передавать `&quot;` сквозь writer. Это input-boundary cleanup, не контентная регулярка.
4. **Writer prompt rule (LLM-first)**: в writer-final / writer-pack / match-create-bundle prompts явно запретить ASCII `"..."` в публичной прозе и потребовать русские «…». Это закрывает кейс, где исходник чистый, а writer всё равно ставит ASCII quotes.
5. **Observability**: добавить лог `smart_update.duplicate_pre_create_probe` с полями `(candidate_title, shortlist_size, match_reason, ticket_link, location_norm)` и `smart_update.candidate_parse_signature` (Gemma model + prompt revision id) — без thoughts, только structured fields. Включить `ENABLE_RUNTIME_FILE_LOGGING=1` на проде хотя бы на 7 дней для evidence-collection.

## Follow-up Actions

- [x] events-bot / 2026-05-07 / включить `ENABLE_RUNTIME_FILE_LOGGING=1` хотя бы на 7 дней evidence window (done).
- [x] events-bot / 2026-05-07 / merge `event` 4679 → 4445 либо архивировать 4679 (archived; 4445 хранит оба источника; rebuild jobs enqueued).
- [x] events-bot / 2026-05-07 / разблокировать `vk_inbox.id=6728 wall-194968_17449` (status=rejected, locked_*=NULL).
- [ ] events-bot / 2026-05-08 / собрать candidate JSON парса `wall-30777579_15151`, `wall-211696971_5570`, `wall-194968_17449` через `gemma-4-31b-it` локально или из нового runtime log и зафиксировать diff vs. expected anchor / vs. Gemma 3 baseline.
- [ ] events-bot / 2026-05-08 / **prompt-family rewrite** для VK draft parse под Gemma 4 contract (`system + user` separation, format-anchor preserve в title `мастер-класс/лекция/спектакль/концерт`, anti-fabrication policy для `location_name` / `location_address` с явной opt-in под `docs/reference/locations.md` reference layer, anti-fabrication policy для расписания/программы при tersely-described постах). Покрыть positive/negative examples из 4677/4679/4681/17449.
- [ ] events-bot / 2026-05-08 / **multi-event digest skip policy** в vk_intake schema: новое поле `is_multi_event_digest: bool` с явным system-prompt rule. Когда `true` — vk_inbox row пишется как `status='skipped', reason='multi_event_digest'` без отправки в smart_update. Покрыть post `wall-194968_17449` как canary fixture.
- [ ] events-bot / 2026-05-08 / **phone-as-ticket-link** writer rule: если в source единственная точка записи — телефон без URL, бандл должен класть `ticket_link='tel:<digits-only>'`, а в `description` упоминать имя контакта и факт «запись по телефону» без самого номера; render в Telegraph через существующий `simple_md_to_html`. Canary fixture — пост `wall-211696971_5570`.
- [ ] events-bot / 2026-05-08 / **provider 500 timeout governance** в `_ask_gemma_json` и `_ask_gemma_text`: hard wall-clock per-call cap (например 90 сек на attempt, 3 минуты в сумме) с явным `provider_timeout` exception, который у smart_update вызывает clean failure path (vk_inbox `status='deferred'` с retry-after, а не infinite Gemma retry без записи статуса).
- [ ] events-bot / 2026-05-08 / **pre-create duplicate probe** в `smart_update_event` ПОСЛЕ create-decision и ДО `INSERT event` — если найден row в shortlist по `(date,time,location_name,ticket_link)`, переключить на merge с `match_reason='post_decision_ticket_anchor'`.
- [ ] events-bot / 2026-05-08 / **`html.unescape` boundary** на VK source_text перед prompt input.
- [ ] events-bot / 2026-05-08 / regression test pack: smart_update merge guard на ticket-link parity (4445/4679); vk_intake digest detector (17449); vk_intake location anti-fabrication (4681); writer phone-as-ticket-link (4681).

## Release And Closure Evidence

- deployed SHA: —
- deploy path: —
- regression checks: —
- post-deploy verification: —

## Prevention

- canonical migration contract в `docs/llm/smart-update-lollipop-gemma-4-migration.md` уже требует family-prompt rewrite перед rollout — нарушение этого контракта в commit `69ab6e8c` было реальной причиной. Добавить explicit pre-flight check в `docs/operations/release-governance.md`: «миграция Gemma-backed stage на новый checkpoint без family-prompt rewrite не считается рабочей и должна откатиться».
- regression test fixture для duplicate-merge на ticket-link parity (см. follow-up).
- runtime file logging + structured `match_reason` evidence по умолчанию для всех VK auto-import production runs.
