# Integration Report — INC-2026-07-10 future-date quality

Base / release source: `origin/main`
Integration branch: `integration/incident-20260710-future-date`
Prevention/deploy SHA: `732e34702f68f94b47b3c034d34999d1444a8efd`
Mode: read-only parallel discovery, then one serial production integrator

## Requirement closure

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Find and repair/delete Zoo 31-December event everywhere | Done | event `6783` cancelled/silent; TG event+calendar, live VK and ICS deleted; Telegraph tombstoned; source preserved |
| R02 | Find and resolve Knight Tournament 1-May series defect | Done | event `3980` rechecked as correct `2026-07-10 20:00`; earlier season-flattening repair remains under `INC-2026-07-09` |
| R03 | Open/classify incidents | Done | new Zoo incident plus full-future semantic-audit incident; existing Knight incident updated/reused |
| R04 | Inspect all managed/public surfaces | Done | DB, source, Telegram E2E, VK API, Telegraph, ICS and outbox evidence; stale/live VK ids resolved before destructive writes |
| R05 | Audit every current future event vector-first + LLM-first | Done | frozen `305/305`; `193` reused sidecar vectors + `112` local fills; final LLM `174 pass / 82 repair-candidate / 6 remove-candidate / 42 needs_review / 1 indeterminate`; zero provider-error verdicts |
| R06 | Identify root-cause classes | Done | date-role loss, operational-time loss, recurrence flattening, identity/merge contamination, venue-prior overreach, projection lag |
| R07 | Prepare prevention, LLM-first/vector-first | Done | LLM schedule/date-role screen, unsafe rescue removal, Smart Update fail-closed LLM eventness routing, vector-first audit/public-gate contract and exact replay |
| R08 | Release, catch up and verify | Done | clean `origin/main` deploy, Fly image/machine healthy, scoped S22 catch-up `ops_run=3421` produced zero events |

## Lane outcomes

| Lane | Requirement IDs | Status | Integrated result |
|---|---|---|---|
| prod-event-discovery | R01, R02, R04 | completed | exact target/public/source inventory and survivor verification |
| incident-code-mapping | R03, R06 | completed | prior incident family and concrete producer/Smart Update root path |
| vector-audit-design | R05, R07 | completed | compatible vector recall → LLM source adjudication → fail-closed publication contract |
| serial-integrator | R01–R08 | completed | code/tests/docs, production repair, deploy, catch-up and closure evidence |

## Production repairs

- Source-confirmed non-events removed: `6783`, `6057`, `6787`.
- Source/vector-confirmed bad duplicates superseded and removed: `2759 → 2758`, `6622 → 6510`, `6771 → 6720`.
- All wrong `@kldevents` messages are absent. Old Calendar documents `4881`/`7238`, which exceeded Telegram's delete window, were replaced in place by `removed.txt` tombstones; Calendar `7374` was deleted.
- VK managed posts `3746`, `5974`, `6707`, `6892`, `6900` return `is_deleted=true`; Telegraph pages are tombstones; repaired DB quick check is `ok`.
- Direct source review left a narrow repair queue instead of unsafe bulk mutation: `4517`, `3864`, `5735`, `6312`, `6517`, `6725`, `6782`, `6798`.

## Validation

- `python3 -m py_compile kaggle/TelegramMonitor/telegram_monitor.py smart_event_update.py`
- Focused producer/Smart Update/incident/recurrence suite: `59 passed`.
- Separate recurrence suite: `19 passed`; exact producer/Zoo suite: `40 passed`.
- Broader two-file run: `70 passed, 1 failed`; the same `test_zero_ticket_price_without_explicit_free_evidence_stays_not_free` failure reproduces on pre-change `origin/main`.
- Deploy: Fly image `deployment-01KX62P844FWPSCVYCPZDBE8NR`, machine version `1615`, one passing machine check.
- Post-deploy `/healthz`: `ok=true`, `ready=true`, `db=ok`, `issues=[]`.
- Compensating production replay: `ops_run=3421`, `success`, one forced `@kldzoo/7641`, zero extracted/imported events, zero errors.

## Material follow-ups / blockers

- Durable hash-bound semantic quality decisions before every public projection and automatic vector-sidecar lag alert remain P1 follow-ups; Fly SQLite remains canonical.
- Full source adjudication of the fail-closed LLM candidate queue is intentionally not converted into automatic destructive repair.
- External Opus acceptance review was not completed because the available lane reached quota; no lower-class response was presented as consultant review. This did not block incident tests, source verification or release gates.
