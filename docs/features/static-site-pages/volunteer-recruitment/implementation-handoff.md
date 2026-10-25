# Implementation handoff: волонтёрские заявки

> **Статус:** исполняемый план реализации принятого контракта.
> **Base:** свежий `main`; не переносить generated preview JSON или production DB в git.
> **Рекомендуемая стратегия:** одна integration branch, изолированные worktrees для schema/monitor/matching/UI/tests, серийная интеграция общих файлов.

## 1. Неизменяемые решения

- `Добро.рф` остаётся владельцем заявки; сайт публикует только external link.
- Public membership ограничена существующими Event и FestivalEdition.
- Отсутствующий festival-like target поступает в существующий `festival_queue` как raw URL seed.
- Volunteer Monitor не угадывает официальный сайт фестиваля.
- Интернет-discovery официального источника принадлежит Festival Web Research.
- Пока Festival Web Research provider blocked, seed остаётся recoverable review item.
- Availability проверяется детерминированно ежедневно; BGE/LLM используются только для matching.
- Card UI — простой текстовый label; detail UI — fact medallion + content block/button.
- Astro получает ID-bound projection и не делает semantic inference.

## 2. Целевые runtime-файлы

Предлагаемые новые модули:

```text
volunteer_monitor/
  __init__.py
  source_config.py
  dobro_adapter.py
  extraction.py
  availability.py
  matching.py
  festival_handoff.py
  projection.py
  service.py

kaggle/VolunteerMonitor/
  kernel-metadata.json
  volunteer_monitor.py

scripts/run_volunteer_monitor_kaggle.py
scripts/request_volunteer_monitor.py
site/scripts/volunteer_match_bge.py
site/src/data/volunteer-links-v1.json        # bounded fixture only; production overwrites
site/src/lib/volunteerLinks.ts
site/src/components/VolunteerLabel.astro
site/src/components/VolunteerOpportunityBlock.astro
site/src/pages/volontery/index.astro
```

Предлагаемые изменяемые владельцы:

```text
models.py
db.py
main.py
festival_queue.py
static_site_release.py
site/scripts/export-production-preview-data.py
site/src/lib/types.ts
site/src/components/EventCard.astro          # или canonical shared card owner
site/src/components/DesktopEventPage.astro   # canonical detail composition owner
site/src/pages/festivali/index.astro
site/scripts/check-preview.mjs
.env.example
CHANGELOG.md
```

Точные component paths нужно подтвердить по current main перед изменением; нельзя создавать параллельную card/detail реализацию.

## 3. Work packages

### W0. Schema и operational state

**Владелец:** core SQLite.

Добавить:

```text
VolunteerOpportunity
VolunteerTargetLink
VolunteerMonitorRun
JobTask.volunteer_monitor
```

Обязательные свойства:

- additive migrations и runtime-safe `Database.init()` path;
- unique canonical application identity;
- separate availability/semantic hashes;
- bounded evidence JSON;
- accepted relation history не стирается provider failure;
- foreign target IDs проверяются перед projection;
- one-running/one-follow-up durable job behavior.

**Acceptance:** migration upgrade/downgrade-copy probe на старом snapshot; duplicate URL, stale link, recovery и FK tests.

### W1. `Добро.рф` discovery и daily availability

**Владелец:** `volunteer_monitor/dobro_adapter.py`, Kaggle kernel.

Реализовать adapter order:

1. plain HTTP для metadata/embedded state;
2. Playwright для region filter, pagination и dynamic application state;
3. bounded screenshot/DOM artifact только при failure или canary evidence.

Production config:

```text
ENABLE_VOLUNTEER_MONITOR=0
VOLUNTEER_MONITOR_TIME_LOCAL=15:20
VOLUNTEER_MONITOR_TZ=Europe/Kaliningrad
VOLUNTEER_MONITOR_SOURCE=dobro_ru_kaliningrad
VOLUNTEER_MONITOR_FRESHNESS_HOURS=36
VOLUNTEER_MONITOR_MAX_PAGES=<bounded>
VOLUNTEER_MONITOR_MAX_ITEMS=<bounded>
VOLUNTEER_MONITOR_PLAYWRIGHT_TIMEOUT_MS=<bounded>
```

Source config содержит permission reference, allowed host/path, fetch frequency и parser version.

**Acceptance:** discovery count не может стать ложным нулём при DOM error; OPEN/CLOSED/EXPIRED/UNKNOWN fixtures; idempotent daily recheck.

### W2. BGE shortlist и LLM adjudication

**Владелец:** shared BGE boundary + volunteer matcher.

- переиспользовать pinned BGE-M3 constants/encoder contract из `site/scripts/static_event_bge.py`;
- не импортировать второй model ID/revision;
- добавить document kind `volunteer_match_v1` или thin wrapper с тем же encoder boundary;
- build/reuse Event and FestivalEdition vectors по content hash;
- top `8` Event + top `5` FestivalEdition;
- один bounded adjudication request на changed opportunity;
- schema-only output;
- deterministic date/city/evidence gates после LLM.

Result:

```text
MATCH_EVENT
MATCH_FESTIVAL_EDITION
FESTIVAL_DISCOVERY_SEED
NO_RELEVANT_TARGET
NEEDS_REVIEW
```

**Acceptance:** owner gold evaluation, shortlist recall artifact, wrong-year adversarial negatives, LLM abstention and exact-quote validation.

### W3. Festival queue handoff

**Владелец:** existing `festival_queue.py`; отдельную volunteer festival queue не создавать.

При `FESTIVAL_DISCOVERY_SEED`:

- insert existing `FestivalQueueItem`;
- `source_kind=url`;
- `source_url` — exact canonical `Добро.рф` application URL;
- bounded `source_text` и `signals_json`;
- explicit outbound URLs в `dedup_links_json`;
- idempotency key по opportunity/source/date hint;
- queue status `pending`, не `done`;
- no `Festival`/`festival_calendar_item` write.

Festival Web Research получает volunteer seed как ещё один source role:

```text
volunteer_recruitment
```

Он может использовать заявку как evidence существования/дат/организатора, но official destination обязан получить из explicit link, existing registry, web discovery или operator review.

**Current blocker:** Antigravity Festival Web Research provider eligibility. Реализация handoff не должна скрывать этот blocker или переключаться на legacy direct writer.

**Acceptance:** one row only, no fabricated URL, blocked researcher preserves review item, approved research can link back to opportunity.

### W4. Projection и UI

**Владелец:** existing production exporter + canonical Astro card/detail components.

Exporter пишет `volunteer-links-v1.json` только из:

```text
availability=OPEN
last_successful_check within freshness TTL
match_status=accepted
target exists in current public catalog
```

UI:

- Event/Festival cards: metadata label `Требуются волонтёры`;
- Event detail: Secondary InlineSlot fact pill medallion;
- Event content: block after search digest and before full description;
- button: `Открыть заявку`;
- `/volontery/`: Event + Festival cards only;
- permanent corps: separate curated page-end config, excluded from dynamic count.

Medallion token proposal:

```text
kind=volunteer_recruitment
shape=pill
copy=Требуются волонтёры
placement=Secondary InlineSlot
interactive=false
listing_allowed=false
```

**Acceptance:** no media overlay, no primary CTA replacement, exact URL, a11y, close/removal behavior.

### W5. Smart tests

**Владелец:** pytest + Node/Playwright + GitHub Actions.

Создать:

```text
tests/fixtures/volunteer_monitor/
tests/test_volunteer_source_parser.py
tests/test_volunteer_availability.py
tests/test_volunteer_matching.py
tests/test_volunteer_festival_handoff.py
tests/test_volunteer_projection.py
site/scripts/volunteer-links.e2e.mjs
.github/workflows/volunteer-monitor-canary.yml
```

Live canary dynamically selects current Event match, Festival match or festival seed. Hardcoded current event ID запрещён. Полная матрица — в [test-plan.md](test-plan.md).

**Acceptance:** E2E live specimen artifact; closure removal; no-supply warning separated from functional failure; no silently dropped source rows.

### W6. Scheduler, observability и release

**Владелец:** Fly worker, runner, ops receipts.

Расписание:

```text
15:20 Volunteer Monitor
16:30 existing festival queue
StaticSiteBuilder +15m only on projection diff
```

Run receipt:

```json
{
  "run_uid": "...",
  "source_pages_seen": 0,
  "opportunities_seen": 0,
  "open_count": 0,
  "closed_count": 0,
  "semantic_changed_count": 0,
  "matched_event_count": 0,
  "matched_festival_count": 0,
  "festival_seed_count": 0,
  "projection_changed": false,
  "result_sha256": "..."
}
```

Alerts:

- discovery/extraction functional failure;
- published link stale beyond TTL;
- projection/hash mismatch;
- repeated no-live-supply as informational/operator condition;
- Festival Web Research blocker surfaced separately;
- closed link still present in generated site — critical.

**Acceptance:** crash adoption/idempotency, no-op unchanged run, one follow-up during active run, failed run preserves last-good within TTL.

## 4. Последовательность реализации

```text
W0 schema
  -> W1 discovery/availability
  -> W2 matching
  -> W3 festival handoff
  -> W4 projection/UI
  -> W5 live + browser tests
  -> W6 scheduler/release canary
```

W3 queue seam можно реализовать параллельно W2 после утверждения result schema. W4 нельзя публиковать до W0–W3 и fixture gates.

## 5. Разделение canary и production

### Production

- Fly-owned durable job;
- production SQLite apply;
- exact Kaggle input/result identities;
- changes public projection;
- invokes StaticSiteBuilder only on diff.

### Live canary

- read-only catalog artifact;
- same source/parser/matcher code;
- no production DB write;
- dynamic specimen selection;
- ephemeral noindex site build;
- GitHub Actions artifact and summary.

Canary не является вторым monitor и не создаёт отдельную semantic truth.

## 6. Definition of done P0

- daily `Добро.рф` discovery/recheck работает из одного notebook;
- OPEN/CLOSED/EXPIRED/UNKNOWN state доказан fixtures и live artifact;
- BGE shortlist + LLM matcher calibrated на owner gold;
- хотя бы один real live specimen автоматически выбран canary;
- unmatched festival-like application создаёт raw URL queue seed;
- официальный festival URL нигде не фабрикуется;
- event/festival card label, detail medallion и content CTA проходят browser E2E;
- закрытая заявка снимается максимум в следующем успешном daily apply;
- stale failure скрывает link после 36 часов;
- `/volontery/` содержит только linked Event/Festival cards;
- run receipt и release manifest связывают exact hashes/counts;
- feature flags остаются off до checked preproduction candidate.

## 7. Не включать в этот PR/проект без отдельного решения

- исправление provider eligibility Antigravity через неограниченные повторные вызовы;
- произвольный browser scraping поисковиков;
- новый платный Search API;
- автоматическое создание public festival edition из одной volunteer application;
- user volunteer profile, applications, hours or messaging;
- универсальный каталог добрых дел;
- redesign общей medallion system;
- ручное заполнение generated projection JSON.
