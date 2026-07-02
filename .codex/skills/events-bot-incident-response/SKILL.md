---
name: events-bot-incident-response
description: Use first for any events-bot production incident, user-visible regression, duplicate/wrong event data, missed/failed publication, scheduler/Kaggle failure, or any message starting with "Инцидент". Routes to logs/DB/Telethon/VK evidence, enforces LLM-first fixes for event/text semantics, creates/updates INC records, and requires closure evidence before deploy/closure.
---

# Events Bot Incident Response

Use this as the top-level incident router. Pair it with narrower skills when
needed: `events-bot-runtime-logs`, `fly-prod-db-access`,
`telegram-link-inspection`, `events-bot-vk-posts`, `future-event-quality-audit`,
Kaggle status/publish skills, or feature-specific docs.

## Start

1. Open:
   - `docs/operations/incident-management.md`
   - `docs/reports/incidents/README.md`
   - `docs/operations/runtime-logs.md`
   - `docs/operations/release-governance.md` if code/deploy may be needed.
2. If the incident has no `INC-*` record yet, create one from
   `docs/reports/incidents/TEMPLATE.md` before closing the task.
3. State a compact control block in the working notes/final report:
   `Incident ID`, `status`, `affected surfaces`, `target behavior`,
   `mandatory checks`, `release evidence`, `follow-ups`.
4. Gather evidence before changing code or data. Do not replace evidence with
   assumptions.

## Evidence routing

- Production logs: use `events-bot-runtime-logs`; check `/data/runtime_logs`
  before saying logs are absent.
- Production DB: use `fly-prod-db-access`; run schema-first, targeted SQL.
- Telegram links: use `telegram-link-inspection` and read through Telethon
  first. Public `t.me/s` HTML is fallback only.
- VK posts/queues: use authenticated VK inspection skills, not public HTML
  alone, when post existence/text/duplicates matter.
- Kaggle/remote runs: use Kaggle status skills; treat active/UNKNOWN remote
  Telegram sessions as session-conflict evidence, not as disposable state.

## LLM-first hard gate

Before implementing prevention for any event/content-quality incident, classify
the suspected root cause:

- **Semantic/event meaning**: `title`, `description`, `search_digest`,
  `location_name`, `location_address`, eventness/non-event, duplicate/match,
  free/paid/ticket status, venue identity, date interpretation from prose,
  source summarization, public writer copy. Fix these LLM-first: prompts,
  staged review/rescue, Smart Update LLM shortlist/merge, schema tightening,
  or provider/runtime contract. Deterministic code may only validate, ground,
  fail closed, normalize syntax, or route to the LLM.
- **Mechanical/idempotency/transport**: Bot API timeout, retry policy, DB
  transaction, queue state, API pagination, public cleanup bookkeeping,
  exact source URL/ticket URL identity. Deterministic fixes are allowed, but
  must not smuggle semantic decisions through regex/keyword lists.

If a proposed deterministic change decides event meaning from broad keywords,
stop and redesign it as LLM-first or a narrow fail-closed guard with negative
controls.

## Public repair rules

- Stabilize the visible surface first when safe: delete/edit duplicate managed
  Telegram/VK posts only after DB/API evidence identifies the managed duplicate.
- Prefer keeping the public post id already stored in DB unless evidence shows
  it is worse or missing.
- For source-import/Smart Update incidents, production data repair is not
  prevention. Add replay/fixture coverage and pass the same import boundary
  unless the incident record explicitly documents a blocker/follow-up.

## Closure gate

Do not close until the incident record contains:

- root cause and contributing factors;
- immediate mitigation and corrective actions;
- `Treat as regression guard when`;
- affected surfaces;
- mandatory checks and required evidence;
- release/deploy evidence if code changed;
- post-repair verification of public surfaces and DB/log state;
- follow-up actions for any manual reconciliation or missing automation.

Final response must include the incident ID, what was mitigated, what was
changed, LLM-first/regression checks performed, deploy SHA/image if deployed,
and any remaining follow-ups.
