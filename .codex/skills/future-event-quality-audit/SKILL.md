---
name: future-event-quality-audit
description: Use when auditing or repairing production future/ongoing event quality in this repo: duplicates, malformed dates/times, prose or wrong venues, risky default locations such as Kaliningrad City Jazz, Telegraph/Telegram/VK public cleanup, and LLM-first prevention work.
---

# Future Event Quality Audit

Use this skill for production audits of future or ongoing events, especially
when the request mentions duplicates, wrong locations, City Jazz drift, bad
descriptions, malformed dates, bad times, or public Telegraph/Telegram/VK
event-surface cleanup.

## Start

- Read `docs/README.md`, `docs/routes.yml`, `docs/operations/incident-management.md`,
  `docs/operations/runtime-logs.md`, and `docs/operations/release-governance.md`.
- Use the `fly-prod-db-access` skill for production SQLite and Fly checks.
- If production data or public surfaces are already wrong, open or update an
  `INC-*` record before repair. Treat it as the regression contract.
- Save raw probes, SQL outputs, and API cleanup results under
  `artifacts/codex/<incident-id>/`. Do not commit artifacts.

## Audit Scope

Audit active future/ongoing rows as of the current absolute date, using both
`date` and `end_date`.

Check at minimum:

- non-ISO or malformed active `date` / `end_date`;
- time values that look like date markers (`DD.MM` -> `HH:MM`);
- prose, contact handles, people names, programme bullets, or ticket-office
  fragments in `location_name` / `location_address`;
- risky default locations that need source grounding, especially Kaliningrad
  City Jazz / Mira 33-35;
- duplicate clusters with same specific ticket URL, same source text, same
  date/time, or related titles even when one row has a wrong/default venue;
- archived duplicate rows that still have `tg_event_post_url`,
  `source_vk_post_url`, or pending public jobs;
- negative controls: valid same-time different productions and source-grounded
  City Jazz rows must remain separate/active.

## LLM-First Rule

Semantic fixes must stay LLM-first. Tighten producer prompts, review stages,
Smart Update match/merge prompts, or LLM handoff schema before adding broad
keyword logic.

Deterministic code is allowed only for narrow guardrails:

- schema validation and fail-closed ISO date checks;
- source-grounding gates for risky defaults;
- exact identity plumbing for specific tickets/source URLs;
- public cleanup bookkeeping;
- rejecting obviously invalid technical shapes such as `DD.MM` date markers as
  times when the exact `HH:MM` time is absent from source text.

Do not use broad regex/keyword rules to decide event meaning, venue identity,
duplicate semantics, ticket status, free/paid status, or title/description
quality.

## Production Probe Pattern

Prefer small Fly-side SQLite probes over full DB downloads. Always record
`PRAGMA quick_check`.

Use read-only probes first:

```bash
set -a; . /home/dev/.config/fly/release.env; set +a
export PATH="$HOME/.fly/bin:$PATH"
python3 - <<'PY' > /tmp/future_quality_probe.b64
import base64
code = r'''
import sqlite3, json
TODAY = "YYYY-MM-DD"
con = sqlite3.connect("/data/db.sqlite")
con.row_factory = sqlite3.Row
out = {}
out["quick_check"] = con.execute("pragma quick_check").fetchone()[0]
out["malformed_dates"] = [dict(r) for r in con.execute("""
select id,title,date,end_date,time,lifecycle_status
from event
where coalesce(lifecycle_status,'active')='active'
  and ((date is null or length(date)<10 or substr(date,5,1)!='-' or substr(date,8,1)!='-')
       or (end_date is not null and end_date!='' and (length(end_date)<10 or substr(end_date,5,1)!='-' or substr(end_date,8,1)!='-')))
order by id
""")]
out["future_city_jazz"] = [dict(r) for r in con.execute("""
select id,title,date,end_date,time,location_name,location_address,source_post_url,source_vk_post_url
from event
where coalesce(lifecycle_status,'active')='active'
  and ((length(date)>=10 and substr(date,1,10)>=?)
       or (end_date is not null and length(end_date)>=10 and substr(end_date,1,10)>=?))
  and (coalesce(location_name,'') like '%Сити Джаз%'
       or coalesce(location_address,'') like '%Мира 33-35%'
       or coalesce(location_address,'') like '%мира 33-35%')
order by date,id
""", (TODAY, TODAY))]
print(json.dumps(out, ensure_ascii=False, indent=2))
'''
print(base64.b64encode(code.encode()).decode())
PY
B64=$(cat /tmp/future_quality_probe.b64)
fly ssh console -a events-bot-new-wngqia -C "python3 -c \"import base64; exec(base64.b64decode('$B64'))\""
```

Add targeted probes for duplicate clusters, source evidence, `joboutbox`, and
public URLs as the audit narrows.

## Repair Workflow

Before mutating production:

1. Create a compressed logical backup or full DB snapshot and an in-DB row-level
   backup table for the affected rows.
2. Dry-run the repair in one transaction and run `PRAGMA quick_check` inside
   the transaction.
3. Apply in one narrow transaction. Preserve source/poster/fact links by moving
   them from archived duplicates to survivors where appropriate.
4. Enqueue or run `telegraph_build` and month/page rebuild jobs for affected
   active rows.
5. Verify post-repair rows, active/archived lifecycle state, public URLs,
   `joboutbox`, and `PRAGMA quick_check`.

For public cleanup:

- Telegram: delete archived duplicate posts when the Bot API allows it. If
  deletion fails with `message can't be deleted`, edit text/caption to a short
  "removed as duplicate" note pointing at the survivor.
- VK: delete only managed `klgdevents` posts whose owner is `-VK_EVENTS_GROUP_ID`
  and whose DB row is archived/duplicate. Never delete external source wall
  posts.
- After successful public deletion, clear stale DB URL/hash fields or mark
  public jobs as manually cleaned so old links do not reappear.

Avoid broad manual re-publish loops. If refreshing survivors, prefer exact
event IDs and stop if a handler starts creating unrelated downstream fanout.

## Prevention Work

For each confirmed class, add one prevention change and one regression test:

- prompt/schema tightening for LLM producer or review stage;
- source-grounding handoff guard;
- Smart Update duplicate recall/match improvement;
- VK/TG date-marker time cleanup;
- replay fixture in `tests/replays/<incident-id>/`.

Include at least one negative control for valid data that must not be collapsed
or removed.

## Closure Checklist

Do not close the incident until all are true:

- root cause and contributing factors are in the incident record;
- targeted tests and `py_compile` pass for touched modules;
- replay/source fixtures are saved;
- production backup, repair output, post-repair DB probe, and public cleanup
  evidence are saved under artifacts;
- Telegraph/month surfaces are rebuilt or explicit blockers are documented;
- Telegram/VK posts are deleted/edited where possible, with blockers recorded;
- `/healthz` is ready after deploy if code changed;
- deployed code SHA is reachable from `origin/main`;
- final production probe confirms malformed known rows are gone, duplicates are
  archived/merged, risky future default venues are source-grounded, and negative
  controls remain valid.
