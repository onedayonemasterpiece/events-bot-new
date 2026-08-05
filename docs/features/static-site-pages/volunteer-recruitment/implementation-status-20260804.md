# Volunteer Monitor: implementation and live-acceptance status

> **Status, 2026-08-05:** read-only `Добро.рф` source boundary, exact vacancy CTA extraction, fixture suite and real GitHub-hosted Chromium acceptance are green. Kaggle canary is blocked only by credential provisioning. Production SQLite apply, BGE/LLM target matching, `festival_queue` apply and public Astro UI are not implemented.
> **Branch / PR:** `agent/volunteer-monitor-skeleton-20260804`, PR #335, stacked on the accepted docs contract PR #331.

Related documents:

- `README.md` — product/runtime contract;
- `test-plan.md` — fixture, dynamic live canary and browser E2E contract;
- `implementation-handoff.md` — full W0–W6 implementation plan;
- `search-and-kaggle-infrastructure.md` — free search and execution choices;
- `code-agent-live-acceptance.md` — credential-only external task.

## 1. Implemented read-only source boundary

Current runtime paths:

```text
volunteer_monitor/
  source_config.py
  types.py
  dobro_common.py
  dobro_page.py
  dobro_adapter.py
  playwright_discovery.py
  playwright_live.py
  service.py
  festival_source_search.py
  cli.py
scripts/run_volunteer_monitor.py
scripts/validate_volunteer_monitor_canary.py
scripts/run_volunteer_lifecycle_probe.py
kaggle/VolunteerMonitor/
  kernel-metadata.example.json
  volunteer_monitor.py
.github/workflows/
  volunteer-monitor-smoke.yml
  volunteer-monitor-live-acceptance.yml
```

Implemented behavior:

- current Dobro.ru location UI with both observed cold-start states;
- selection of `Калининградская обл`;
- current vacancy-only `Вакансии` tab;
- asynchronous region suggestion and vacancy-card readiness;
- bounded `Показать еще` traversal;
- extraction of `event_id`, `vacancy_id` and exact source CTA URL;
- one parent-event fetch per selected parent, including multiple vacancies;
- JSON-LD / visible-text enrichment from the parent event page;
- `OPEN / CLOSED / EXPIRED / UNKNOWN` lifecycle parsing;
- active vacancy inventory as source-grounded OPEN evidence;
- separate `semantic_hash` and `availability_hash`;
- target-region evidence from vacancy card or parent source;
- contact PII redaction before bounded excerpts are persisted;
- explicit zero-supply versus broken-discovery distinction;
- exact terminal accounting for every sampled parent source;
- SHA-bound monitor and lifecycle receipts.

The future public CTA must use `application_urls[]`. It must never derive a
vacancy URL from a festival/event name or substitute the parent event page for
the source application destination.

## 2. Fixture evidence

Latest fixture workflow:

```text
workflow: Volunteer monitor smoke
run_id:   30989937721
status:   SUCCESS
pytest:   20 passed
fixture:  PASS, 2 pages, 1 OPEN, 1 CLOSED
```

Coverage includes:

- direct event links;
- current `/login/?__target_path=/event/<event>/vacancy/<vacancy>` CTA shape;
- duplicate vacancy anchors;
- OPEN, CLOSED, EXPIRED and UNKNOWN;
- stale enabled CTA after application deadline;
- target-region rejection;
- PII redaction;
- separate semantic and availability hashes;
- grounded free-search provider URL rules.

The monitor tests run with `--noconftest` so this autonomous source boundary does
not load the entire Telegram bot and unrelated dependencies.

## 3. Accepted real live acceptance

### 3.1. Bounded PR acceptance

```text
workflow:                 Volunteer monitor live acceptance
run_id:                   30989937771
job_id:                   92253781830
artifact_id:              8923815849
artifact:                 volunteer-monitor-live-acceptance-30989937771-1
result_sha256:             dc46b23238276a9d71a8ee13e1f3fa68d7c5aba2dc7a03cfc051c04bb5da734c
lifecycle_result_sha256:   fd3e3701d586c33525a231e0f0ee6249ad7456b96081e066e2fe9e94e132a12f
status:                    SUCCESS
```

Observed source result:

```text
parent pages selected:       24
opportunities parsed:        24
OPEN:                        24
outside-region:               0
transport/parser errors:      0
accounted sources:           24 / 24
vacancy records discovered: 162
parents with application URL: 24 / 24
vacancy IDs retained:         48
load-more clicks:              5
```

Historical lifecycle probe:

```text
sources parsed: 2
CLOSED:         0
EXPIRED:        2
UNKNOWN:        0
errors:         0
non-open proof: 2
```

`CLOSED` and `EXPIRED` remain distinct reasons, but both are non-public states
that remove a volunteer link. The active `Вакансии` tab naturally proves OPEN;
a separate rotating historical probe proves at least one source-backed
`CLOSED | EXPIRED` state.

### 3.2. Full diagnostic run

A broader run remains useful as load/error evidence:

```text
workflow run:   30986388903
job:            92242069968
artifact:       8922510733
result_sha256:  e0afa6324bd67d5c582e3420b8d943cf9806ae3ea57f3db9e8df21725532296c
selected:       101 parent pages
parsed:          88
OPEN:            86
UNKNOWN:          2
outside-region:  12
transport error:  1
```

Accounting was exact:

```text
88 parsed + 12 outside-region + 1 error = 101 selected
```

This result is `PARTIAL`, not failure. CI accepts `PARTIAL` only after the
independent accounting validator proves that every selected source has exactly
one terminal disposition.

## 4. Current live Dobro.ru contract

These selectors and semantics were derived from saved HTML/UI diagnostics:

```text
cold location state A:   geolocation confirmation + `Изменить`
cold location state B:   selected location button, e.g. `Москва`
location search input:   placeholder="Введите название"
region option:           Калининградская обл
vacancy-only surface:    role=tab, name="Вакансии"
vacancy CTA text:        Отправить заявку
CTA target shape:        /login/?__target_path=/event/<event_id>/vacancy/<vacancy_id>
load-more action:        Показать еще
```

Important corrections to the original hypothesis:

- the previous `С доступными вакансиями` checkbox is absent;
- its current product equivalent is the `Вакансии` tab;
- location and vacancy results are asynchronous;
- the DOM contains hidden/visible duplicate option trees;
- region selection may be reflected in URL state or only in the visible header
  location button;
- the search surface is vacancy-grained, while event details are parent-grained.

Fixed sleeps are not readiness evidence. The monitor waits for an exact visible
region option and for vacancy CTAs or an explicit zero-result source state.

## 5. Opportunity identity

The source inventory retains:

```text
event_id
vacancy_id
exact source application URL
card-local role/date/location text
parent event source fields
```

Several vacancy records may map to one parent event. The future Event/Festival
projection may group them visually, but it must preserve exact vacancy IDs and
source CTA destinations.

## 6. Canary semantics

Allowed terminal run states:

```text
PASS
PARTIAL
WARN_NO_LIVE_SUPPLY
functional failure
```

`PARTIAL` is allowed only when every selected parent source has exactly one of:

```text
parsed opportunity
outside-target-region
recorded fetch/parser error
```

The validator rejects count mismatch, duplicate dispositions, unclassified
warnings, non-Dobro warning URLs, no OPEN opportunity under live supply, PASS
with warnings, PARTIAL without warnings and empty success without explicit
source zero-state evidence.

## 7. Free official-festival source discovery

Search is triggered only after no existing Event/Festival match and no explicit
or known official URL.

Order:

```text
1. explicit outbound source URL
2. existing exact festival/organizer registry URL
3. free grounded web-search candidates
4. source-role / edition verification
5. operator approval when officiality remains ambiguous
```

Primary free lane:

```text
Gemini 2.5 Flash-Lite + Google Search grounding
one request per unresolved seed
max 8 grounded candidate URLs
free-form model URLs/prose discarded
```

Fallbacks:

```text
Tavily Researcher free tier, basic, max 8, no generated answer/raw content
operator-owned SearXNG JSON endpoint
```

A search result never writes a festival destination directly.

## 8. Execution ownership

GitHub Actions owns fixtures, read-only direct acceptance, optional Kaggle
canary and private evidence artifacts. It never writes production SQLite.

The private Kaggle canary uses the same source package and result schema. It is
automatically eligible after these settings exist:

```text
GitHub Environment: volunteer-monitor-canary
Environment secret: KAGGLE_API_TOKEN
Repository variable: VOLUNTEER_KAGGLE_KERNEL_SLUG=eventsbot/kenigevents-volunteer-monitor
Repository variable: VOLUNTEER_KAGGLE_CANARY_ENABLED=true
```

Production ownership remains:

```text
Fly durable job / immutable input
  -> Kaggle CPU batch
  -> hash-validated result
  -> Fly transactional SQLite apply
  -> StaticSiteBuilder only on public projection diff
```

## 9. Only remaining code-agent task

The code agent is not needed for code, selector repair, GitHub Actions runs,
artifact analysis or documentation. Its task is restricted to creating the
Environment and provisioning the one unavailable secret plus the two variables.
The exact minimal instruction is stored in `code-agent-live-acceptance.md`.

## 10. Not yet implemented

This branch still does not provide:

- Fly SQLite tables/migrations and durable daily outbox scheduling;
- shared BGE-M3 Event/Festival shortlist;
- bounded LLM target adjudication;
- `festival_queue` write/apply;
- verified official-source candidate apply;
- production `volunteer-links-v1.json` export;
- Event/Festival card label;
- event-detail fact medallion and content CTA;
- `/volontery/` page;
- generated-site removal after source closure;
- production deployment.

Those are the next implementation phase after Kaggle source parity is accepted.
