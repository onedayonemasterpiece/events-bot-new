# Volunteer monitoring: implementation status and free web-search fallback

> Status: **source adapter skeleton fixture-debugged; live GitHub/Kaggle canary and production apply not yet executed**.
> Scope: read-only `Добро.рф` discovery/extraction, free candidate-source search policy, and orchestration boundary.

This document supplements:

- `README.md` — product/runtime contract;
- `test-plan.md` — fixture, dynamic live canary and browser E2E contract;
- `implementation-handoff.md` — full W0–W6 implementation plan.

## 1. What is implemented in the skeleton

Paths:

```text
volunteer_monitor/
  source_config.py
  types.py
  dobro_adapter.py
  playwright_discovery.py
  service.py
  festival_source_search.py
  cli.py
scripts/run_volunteer_monitor.py
.github/workflows/volunteer-monitor-smoke.yml
tests/test_dobro_volunteer_monitor.py
tests/test_festival_source_search.py
tests/fixtures/volunteer_monitor/*
```

The read-only source boundary now has:

- canonical `/event/<id>` URL discovery;
- accessible Playwright interaction with region and available-vacancy filters;
- bounded `Показать еще` traversal;
- explicit `WARN_NO_LIVE_SUPPLY` versus broken discovery;
- source-local extraction from JSON-LD and visible current-page text;
- `OPEN / CLOSED / EXPIRED / UNKNOWN` state;
- Russian date-range fallback;
- exact external-link scope limited to the event description / explicit source markers;
- contact PII redaction before bounded evidence is persisted;
- target-region proof and refusal of a false Moscow/default-region success;
- separate `semantic_hash` and `availability_hash`;
- deterministic fixtures and a CLI result receipt.

Local verification on 2026-08-04:

```text
19 passed
fixture CLI: PASS, 2 source pages, 1 OPEN, 1 CLOSED
```

Not yet proven:

- selectors against a real GitHub-hosted Chromium run;
- current Kaliningrad live result count;
- daily recheck against a source transition from `OPEN` to `CLOSED`;
- Fly SQLite persistence/outbox;
- BGE/LLM target matching;
- Kaggle push/poll/result adoption;
- public Astro projection.

The first live run must therefore be described as a **read-only canary**, not production acceptance.

## 2. Official festival URL discovery when Dobro.ru has no outbound link

An official URL can be searched, but search output is always a candidate set. It never directly becomes `festival_calendar_item.source_url`.

Decision order:

```text
1. explicit outbound URL in the Dobro.ru application
2. exact URL/alias already stored for the festival series or organizer
3. free grounded web-search provider
4. source-role and edition verification
5. operator review when officiality remains ambiguous
```

Forbidden:

```text
festival name -> guessed domain
first result -> official URL
LLM prose -> URL not present in grounded tool metadata
search provider confidence -> automatic festival apply
```

### Primary free replacement for Antigravity

Use **Gemini 2.5 Flash-Lite with Google Search grounding** through the already installed `google-genai` client.

Reasons:

- current free-tier allowance includes a bounded number of Google Search grounding prompts per day;
- the repository already has Google API key pooling, accounting and `google-genai`;
- grounding metadata exposes actual source URLs;
- the same strict quota/cooldown boundary can be reused.

Contract:

```text
model = gemini-2.5-flash-lite
consumer = volunteer_festival_source_search
max grounded requests per opportunity = 1
max returned URLs = 8
generated prose is discarded
only grounding_chunks.web.uri is retained
```

Only public festival facts are sent. Free-tier data handling must be treated as unsuitable for private/user data; contact details are redacted before the request.

### Independent free fallback

Use **Tavily Researcher free tier** only after the primary lane is unavailable or exhausted.

Contract:

```text
search_depth = basic
max_results = 8
include_answer = false
include_raw_content = false
monthly hard stop below the free allowance
```

Tavily returns candidate URLs only. The verifier still has to prove current edition, self-identification/organizer relationship, date compatibility and source role.

### Optional self-hosted fallback

An operator-owned **SearXNG** instance may be used as a third lane. The software is free, but compute, proxy reliability and upstream-engine terms are operational dependencies. It is not a default P0 dependency.

### Rejected as P0 primary options

- Google Programmable Search JSON API: closed to new customers and approaching transition deadline;
- Yandex Search API: paid outside temporary grants;
- scraping arbitrary public search-result HTML: brittle and potentially contrary to provider rules;
- Common Crawl: useful for historical domain hints but too stale for current-edition authority;
- unauthenticated reader/search proxies: acceptable only for diagnostics, not a production source of truth.

## 3. Search-result verification gate

Every candidate URL is fetched independently and classified:

```text
official_home
official_program
official_organizer
official_event
ticket_single_event
regional_tourism
media
aggregator
unknown
```

A URL can become the official festival destination only when at least one condition is grounded in the fetched source:

- the page self-identifies as the current festival edition;
- the known organizer links to it as the festival destination;
- the current edition page links back to the known organizer/series identity;
- an operator approves the exact URL with evidence.

Date/year conflict blocks automatic approval even when title similarity is high.

## 4. Recommended execution ownership

### Production

```text
Fly scheduler/outbox
  -> immutable input + run identity
  -> Kaggle CPU kernel (Playwright + changed BGE batch)
  -> hash-checked result
  -> Fly transactional apply
  -> StaticSiteBuilder only on projection diff
```

Fly remains the state owner. GitHub Actions must not write production SQLite or decide last-good state.

### GitHub Actions

The included workflow is for:

- PR fixture tests;
- scheduled read-only live canary;
- manual diagnostics;
- bounded evidence artifacts.

The direct Playwright canary needs no Kaggle or LLM secret. This is the fastest way to debug the real Dobro.ru selectors before introducing remote-kernel orchestration.

### Kaggle

Kaggle should be introduced when one of these becomes true:

- BGE target vectors are computed in the same changed-input batch;
- browser workload no longer fits the bounded Fly job;
- production needs the existing remote-run adoption/receipt protocol.

Do not create a separate notebook per source or match type.

## 5. Secret and identity plan

Preferred Kaggle authentication for new automation:

```text
KAGGLE_API_TOKEN
```

Store it in a GitHub Environment named `volunteer-monitor-canary` only for manual/diagnostic Kaggle jobs. Production Fly should use its own least-privileged Kaggle secret and should not depend on a GitHub secret.

Optional search secrets:

```text
GOOGLE_API_KEY_VOLUNTEER_SEARCH   # or an approved existing shared Google key pool
TAVILY_API_KEY                    # fallback only
```

Repository variable, not secret:

```text
VOLUNTEER_MONITOR_PERMISSION_REFERENCE
```

Secret values cannot be recovered from GitHub after creation. A code agent can add workflow code and run `gh secret set` only when the token is securely mounted into its environment; it cannot invent or read an existing secret value.

## 6. Why GitHub Actions should not be the production orchestrator

A workflow can push and poll Kaggle, but waiting for every daily kernel consumes Actions minutes and splits the durable state between GitHub and Fly. The project already has a stronger pattern: Fly owns the outbox and can adopt a matching Kaggle result after restart.

Recommended split:

```text
GitHub Actions = test / live canary / manual diagnosis
Fly + Kaggle    = scheduled production monitor and matching
```

## 7. Current tool-access limitation

The current ChatGPT GitHub connector can inspect branches, PRs, runs, jobs and artifacts, but it does not expose workflow dispatch or repository-secret writes in this session. Adding a Kaggle secret does not by itself make the workflow invokable from this chat.

After a human or code agent starts a run, this session can inspect the run, logs and artifacts and continue debugging from evidence.

## 8. Next implementation sequence

1. Apply this skeleton on top of PR #331.
2. Run the fixture workflow.
3. Run one manual direct live canary and repair selectors against artifacts.
4. Freeze a current real Kaliningrad fixture with contacts redacted.
5. Add Fly SQLite tables and durable `JobTask.volunteer_monitor`.
6. Integrate changed-document BGE matching.
7. Add Gemini-grounded festival-source search with hard quota and Tavily fallback.
8. Add Kaggle runner/adoption only after the direct live source boundary is stable.
9. Complete festival queue handoff and public projection E2E.
