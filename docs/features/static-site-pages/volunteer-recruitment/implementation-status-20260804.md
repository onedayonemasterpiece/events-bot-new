# Volunteer Monitor: implementation and acceptance status

> **Status, 2026-08-06:** read-only `Добро.рф` source boundary, deterministic fixtures, GitHub-hosted browser canary and private Kaggle execution are proven. Production persistence, daily state apply, BGE/LLM matching, festival-queue handoff and public Astro UI are not implemented.
> **Branch / PR:** `agent/volunteer-monitor-skeleton-20260804`, draft PR #335, stacked on docs contract PR #331.

## 1. Implemented source boundary

Runtime paths:

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
  volunteer-monitor-kaggle.yml
```

Implemented behavior:

- both observed cold-start states of the current location UI;
- exact selection of `Калининградская обл`;
- activation of the current `Вакансии` tab;
- asynchronous region/vacancy readiness;
- bounded `Показать еще` traversal;
- extraction of `event_id`, `vacancy_id` and exact application URL;
- one parent-event fetch per selected event;
- JSON-LD/visible-text enrichment;
- `OPEN / CLOSED / EXPIRED / UNKNOWN` parsing;
- active inventory as authoritative positive OPEN evidence;
- separate `semantic_hash` and `availability_hash`;
- PII redaction and bounded evidence;
- explicit zero-supply versus broken discovery;
- exact terminal accounting for sampled sources;
- SHA-bound result and receipt contracts.

## 2. Fixture evidence

```text
workflow run: 31080728482
job:          92548695664
status:       SUCCESS
pytest:       20 passed
fixture CLI:  PASS
pages:        2
OPEN:         1
CLOSED:       1
artifact:     8959343631
```

The fixture suite covers exact current vacancy CTA shape, duplicate links, OPEN/CLOSED/EXPIRED/UNKNOWN, stale CTA after deadline, target-region rejection, PII redaction, separated hashes and grounded search-provider URL rules.

## 3. Direct live evidence

```text
workflow run:          31080728463
job:                   92548820167
status:                SUCCESS
source pages:          24
opportunities:         24
OPEN:                  24
warnings:               0
outside-region:         0
source errors:          0
accounted:             24 / 24
artifact ID:           8959425369
artifact ZIP SHA-256:  48f0c11d958fbb325a63a20adfb01be6693f6678c3610150e8e58aa3982eaa9f
```

Monitor integrity:

```text
result file SHA-256: f3f8a941b72b7dc66169f629fac333732aeafd6cbc485674584f85ef5b196e0b
internal result hash: 818937933f8fe28443b9bff30c52072e3ba69b96e9b09350c7a09a129800b1e9
```

Lifecycle probe:

```text
requested:             3
parsed:                3
CLOSED:                0
EXPIRED:               3
UNKNOWN:               0
errors:                0
non-public proof:      3
probe file SHA-256:    722beb70e40dc61c8cc4d81a060cf98aafae403c29701c1839dc00094e193a5c
internal probe hash:   5e1fabd09817e42aef82ba6f30e5a0214fac3cabed3b6606fcd77f825a760001
```

## 4. Private Kaggle evidence

The first design used the wrong owner/client contract (`eventsbot`, Kaggle CLI 2.x). The repository's working Kaggle infrastructure uses account `zigomaro`, `KAGGLE_USERNAME + KAGGLE_KEY` and Kaggle 1.8.x. Volunteer Monitor now reuses that contract.

```text
GitHub Actions run: 31079828744
job:                92545879373
status:             SUCCESS
kernel:             zigomaro/kenigevents-volunteer-monitor
kernel version:     1
run_uid:            volunteer-monitor-20260806T070946Z
started_at:         2026-08-06T07:09:46.700291Z
completed_at:       2026-08-06T07:14:27.870758Z
```

Result:

```text
run_status:         PASS
source pages:       24
opportunities:      24
OPEN:               24
warnings:           0
outside-region:     0
source errors:      0
accounted:          24 / 24
```

Discovery receipt:

```text
region_proven:           true
available_filter_proven: true
parent URLs:             101
vacancies:               159
load-more clicks:          5
```

Integrity:

```text
result file SHA-256: 58808e44af5cac4e7577b7dc817b9344fd37771fec6c11083ca5dc28f0ebae44
internal result hash: 843b5fd966bfab88c467101ca4db88e541449ed894711838791ecbee5fcd592a
artifact ID:          8959127103
artifact ZIP SHA-256: 2a41a8a112af6bda3dc14751e2eb4464c70c0ff70ce1d031d671e2ee21d15c17
```

## 5. Lifecycle and removal contract

A live source row is public only while its exact vacancy identity is present in a successfully completed regional vacancy inventory.

```text
present in complete inventory -> OPEN
previously OPEN but absent     -> no longer OPEN
explicit close evidence        -> CLOSED
deadline/event end passed      -> EXPIRED
transport/DOM uncertainty      -> UNKNOWN
```

`CLOSED` and `EXPIRED` are distinct reasons but both remove the label/link in the next successful apply. `UNKNOWN` never becomes a false close. After 36 hours without a successful check, projection hides fail-closed.

Static HTTP for reviewed historical pages currently proves `EXPIRED`; dynamic UI wording `Набор закрыт` is not consistently present in the source HTML. Therefore the live gate correctly requires source-backed `CLOSED | EXPIRED`, while exact `CLOSED` remains a deterministic fixture contract.

## 6. Canonical workflows

```text
volunteer-monitor-smoke.yml
  PR fixtures
  scheduled/manual direct monitor

volunteer-monitor-live-acceptance.yml
  bounded PR direct specimen
  source-backed non-public lifecycle probe

volunteer-monitor-kaggle.yml
  manual private Kaggle canary
  proven zigomaro / kaggle 1.8.4 path
```

The temporary acceptance workflow was removed after its successful run.

## 7. Official festival source search

Search runs only after there is no existing Event/Festival match and no explicit/known official URL.

```text
explicit source URL
  -> existing registry
  -> bounded grounded search candidates
  -> independent fetch and source-role/edition verification
  -> operator approval when ambiguous
```

Configured free lanes remain Gemini Google Search grounding, Tavily free tier and optional operator-owned SearXNG. Search output never directly writes an official festival destination.

## 8. Remaining implementation

Not implemented yet:

- SQLite migrations/tables for opportunities, links and monitor runs;
- Fly-owned durable daily job and last-good adoption;
- previous-open inventory diff and transactional state apply;
- shared BGE-M3 Event/Festival shortlist;
- bounded LLM adjudication and deterministic post-gates;
- idempotent `festival_queue` insertion;
- verified official-source candidate apply;
- production `volunteer-links-v1.json` export;
- card label, detail medallion/content CTA and `/volontery/`;
- generated-site removal E2E;
- production deployment.

No code-agent credential task remains. The next phase is normal product implementation, not infrastructure recovery.
