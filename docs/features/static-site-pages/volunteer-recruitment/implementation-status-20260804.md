# Volunteer Monitor: implementation and live-acceptance status

> **Status, 2026-08-05:** fixture contract and real GitHub-hosted Chromium source boundary are proven; current vacancy-grain CTA preservation and accounted-`PARTIAL` CI are implemented and under branch revalidation. Kaggle canary is blocked only by credential provisioning. Production SQLite apply, BGE/LLM target matching, festival-queue apply and public Astro UI are not implemented.
> **Branch / PR:** `agent/volunteer-monitor-skeleton-20260804`, PR #335, stacked on the accepted docs contract PR #331.

This document supplements:

- `README.md` — product/runtime contract;
- `test-plan.md` — fixture, dynamic live canary and browser E2E contract;
- `implementation-handoff.md` — full W0–W6 implementation plan;
- `search-and-kaggle-infrastructure.md` — free search and execution choices.

## 1. Implemented read-only source boundary

Current paths:

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
kaggle/VolunteerMonitor/
  kernel-metadata.example.json
  volunteer_monitor.py
.github/workflows/
  volunteer-monitor-smoke.yml
  volunteer-monitor-live-acceptance.yml
tests/test_dobro_volunteer_monitor.py
tests/test_festival_source_search.py
tests/fixtures/volunteer_monitor/*
```

The source boundary now provides:

- Playwright selection of the current region UI;
- activation of the current vacancy-only search surface;
- bounded `Показать еще` traversal;
- extraction of active vacancy identities from source CTA target paths;
- exact source application URLs retained separately from parent event URLs;
- one parent-event fetch per selected parent, even when it has several vacancies;
- JSON-LD and visible-text enrichment from the parent event page;
- `OPEN / CLOSED / EXPIRED / UNKNOWN` lifecycle parsing for detail pages;
- active vacancy inventory as source-grounded positive OPEN evidence;
- separate `semantic_hash` and `availability_hash`;
- target-region evidence from the vacancy card or parent source;
- contact PII redaction before bounded excerpts are persisted;
- explicit zero-supply versus broken-discovery distinction;
- deterministic fixtures and SHA-bound result receipts;
- exact source accounting for every sampled parent page.

The future public button must use `application_urls[]`. It must not construct a
vacancy URL from an event name or use the parent event page as a substitute for
the source application CTA.

## 2. Fixture evidence

The autonomous source tests do not load the bot-wide `tests/conftest.py`; they
exercise only the bounded monitor dependencies.

Current fixture coverage includes:

- direct event links;
- current `/login/?__target_path=/event/<event>/vacancy/<vacancy>` CTA shape;
- duplicate CTA anchors;
- OPEN, CLOSED, EXPIRED and UNKNOWN;
- stale enabled CTA after a deadline;
- target-region rejection;
- PII redaction;
- separate semantic and availability hashes;
- free-search provider URL-grounding rules.

Latest established fixture baseline before the vacancy CTA addition:

```text
19 passed
fixture CLI: PASS
source pages: 2
OPEN: 1
CLOSED: 1
```

The current branch adds the exact vacancy-CTA regression and therefore expects
at least 20 monitor/search tests in the next green fixture run.

## 3. Real `Добро.рф` live evidence

### 3.1. Accepted source-discovery run

```text
GitHub Actions workflow: Volunteer monitor smoke
run_id:                    30986388903
job_id:                    92242069968
artifact_id:               8922510733
artifact:                  volunteer-monitor-live-30986388903-1
result_sha256:              e0afa6324bd67d5c582e3420b8d943cf9806ae3ea57f3db9e8df21725532296c
```

Observed result:

```text
run_status:          PARTIAL
parent pages sampled: 101
opportunities parsed: 88
OPEN:                 86
CLOSED:                0
EXPIRED:               0
UNKNOWN:               2
outside-region rows:  12
transport failures:    1
```

The accounting is complete:

```text
88 parsed + 12 outside-region + 1 transport failure = 101 selected pages
```

Nothing was silently dropped. `PARTIAL` reflected recorded per-source warnings,
not a failed discovery. CI now treats this as a warning only after the separate
accounting validator proves the equality above.

The run lasted roughly ten minutes with `max_items=120`, including browser setup,
five pagination actions and 101 sequential parent-page requests. The dedicated
PR acceptance workflow uses 24 parent pages and a shorter bounded detail timeout;
the scheduled/manual diagnostic workflow retains the larger limit.

### 3.2. Current live DOM contract

The following selectors/semantics came from saved HTML and UI diagnostics, not
from a guessed implementation:

```text
location opener:       Изменить
location input:        placeholder="Введите название"
region option:         Калининградская обл
vacancy-only surface:  role=tab, name="Вакансии"
vacancy CTA text:      Отправить заявку
CTA target shape:      /login/?__target_path=/event/<event_id>/vacancy/<vacancy_id>
load-more action:      Показать еще
```

Important changes from the initial design hypothesis:

- the old `С доступными вакансиями` checkbox is not present in the current UI;
- its current product equivalent is the `Вакансии` tab;
- region suggestions and vacancy cards are asynchronous;
- hidden and visible duplicate option trees exist;
- the region-level source label is abbreviated as `Калининградская обл`;
- the search surface is vacancy-grained, while the enrichment page is event-grained.

The implementation therefore waits for visible exact region options and for the
vacancy result panel to expose application targets or a proved empty state.
Fixed sleeps alone are not accepted as readiness evidence.

### 3.3. Current opportunity identity

The live search card exposes both:

```text
event_id
vacancy_id
exact source application href
card-local role/date/location text
```

The monitor retains all exact vacancy IDs and application URLs for each parent
event. The parent event page remains an enrichment source for title,
organizer, dates, description and outbound official links. A future public
projection may group several vacancy CTAs under one matched Event/Festival card,
but it must not lose the source vacancy identities.

### 3.4. Remaining lifecycle proof

The active `Вакансии` tab naturally contains open vacancy CTAs and therefore does
not supply a closed example. CLOSED/EXPIRED fixture behavior is green, but a
separate rotating read-only lifecycle probe of historical source pages is still
needed before production activation. It must not hardcode one forever-valid
closed page as the only proof.

## 4. Canary result semantics

A live canary can terminate as:

```text
PASS
PARTIAL
WARN_NO_LIVE_SUPPLY
functional failure
```

`PARTIAL` is acceptable only when every selected parent source has exactly one
terminal disposition:

```text
parsed opportunity
outside-target-region
recorded fetch/parser error
```

The validator rejects:

- source-count mismatch;
- duplicated terminal dispositions;
- unclassified warnings;
- non-Dobro source URLs in warnings;
- no OPEN opportunity when live supply exists;
- PASS with warnings;
- PARTIAL without warnings;
- empty success without an explicit zero-result source state.

## 5. Free official-festival URL discovery

An official festival URL can be searched after the monitor cannot match an
existing Event/Festival and the source application contains no usable outbound
link. Search output is always a candidate set; it never directly becomes a
festival destination.

Decision order:

```text
1. explicit outbound URL in the Dobro.ru source
2. exact URL/alias already stored for the festival series or organizer
3. free grounded web-search provider
4. independent source-role / edition verification
5. operator approval when officiality remains ambiguous
```

Forbidden:

```text
festival name -> guessed domain
first result -> official URL
LLM prose -> URL absent from grounded tool metadata
search confidence -> automatic festival apply
```

### Primary free lane

Use Gemini 2.5 Flash-Lite with Google Search grounding through the installed
`google-genai` client.

```text
consumer = volunteer_festival_source_search
max grounded requests per unresolved opportunity = 1
max candidate URLs = 8
generated prose is discarded
only grounding_chunks.web.uri is retained
```

Only public, redacted festival facts may be sent.

### Independent free fallback

Use Tavily Researcher free tier with:

```text
search_depth = basic
max_results = 8
include_answer = false
include_raw_content = false
```

### Optional infrastructure fallback

An operator-owned SearXNG instance may provide JSON candidates. Public arbitrary
instances and browser scraping of search-result HTML are not production sources.

## 6. Execution ownership

### GitHub Actions

Owns:

- fixtures;
- direct read-only live acceptance;
- optional Kaggle canary;
- bounded private artifacts and diagnostics.

It does not write production SQLite.

### Kaggle

The self-contained private kernel uses the same source package and result schema.
It becomes automatically eligible on PR pushes only when repository variable
`VOLUNTEER_KAGGLE_CANARY_ENABLED=true` is present. It still requires GitHub
Environment `volunteer-monitor-canary` with secret `KAGGLE_API_TOKEN` and
repository variable:

```text
VOLUNTEER_KAGGLE_KERNEL_SLUG=eventsbot/kenigevents-volunteer-monitor
```

### Production

The intended owner remains:

```text
Fly durable job / immutable input
  -> Kaggle CPU batch
  -> hash-validated result
  -> Fly transactional SQLite apply
  -> StaticSiteBuilder only on public projection diff
```

GitHub Actions is not the production state owner.

## 7. Only remaining code-agent task

The code agent is no longer needed for source code, selector debugging, workflow
runs, artifact inspection or documentation. Its task is limited to provisioning
one unavailable credential and two variables. The exact minimal instruction is
stored in `code-agent-live-acceptance.md`.

## 8. Not yet implemented

The branch does **not** yet provide:

- Fly SQLite tables and migrations;
- durable daily outbox scheduling;
- reuse of the shared BGE-M3 event/festival vector artifact;
- bounded LLM target adjudication;
- `festival_queue` write/apply;
- official-source verifier apply;
- `volunteer-links-v1.json` production export;
- card label, detail medallion, content CTA or `/volontery/` page;
- automatic removal from the generated site after CLOSED/EXPIRED;
- production deployment.

Those remain the next implementation phase after source/Kaggle acceptance, not
work to hide inside the credential-provisioning task.
