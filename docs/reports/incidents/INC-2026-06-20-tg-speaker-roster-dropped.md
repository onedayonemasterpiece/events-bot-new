# INC-2026-06-20-tg-speaker-roster-dropped Speaker roster dropped from TG-imported lecture

Status: monitoring
Severity: sev3
Service: Telegram import / Smart Update G4 split-create / public Telegraph + VK event posts
Opened: 2026-06-20
Closed: —
Owners: Smart Update / Telegram import owner
Related incidents: `INC-2026-05-11-lecturer-name-and-title-dropped-from-description`, `INC-2026-06-20-tg-forward-service-chat-leak`
Related docs: `docs/features/smart-event-update/README.md`, `docs/operations/incident-management.md`, `CHANGELOG.md`

## Summary

Production event `6244` (`https://t.me/kenigevents/4104`, lecture/public talk «Калининград: город-сад или микрорайон для жизни у моря!», 2026-06-23 18:30) was imported with a generic description and search digest that omitted the visible speaker roster. The source Telegram post explicitly lists Артур Сарниц, Андрей Анисимов, Валерия Надымова, Михаил Марковец and Игорь Селин with roles, but the public Telegraph and managed VK post showed only a generic phrase about architects/краеведы/учёные.

## User / Business Impact

- Public event cards lost the strongest attendance signal for a lecture/public talk: who is speaking.
- The already published Telegraph page and VK `klgdevents` post were weaker than the source and misleadingly hid notable participants.
- The pending Telegram event publication could have shipped with the same omission if not repaired before its slot.

## Detection

- 2026-06-20 operator report: “Ещё при импорте того события проигнорированы все спикеры”.
- No automated alert fired: Smart Update created the event successfully and the public publisher jobs completed normally.

## Timeline

- 2026-06-20 07:21 UTC: source post `https://t.me/kenigevents/4104` published.
- 2026-06-20 08:51 UTC: repost/forward was processed by the legacy private-forward import path.
- 2026-06-20 08:57 UTC: Smart Update created event `6244`; runtime log shows `g4_split_description_writer rejected description with logistics` and `g4_split_create_v2_rich_facts_unavailable`.
- 2026-06-20 08:58 UTC: Telegraph and VK jobs completed with the generic description.
- 2026-06-20 09:52 UTC: incident investigation confirmed DB facts/public surfaces/source mismatch; Telegram event post was still pending for 11:00 UTC.
- 2026-06-20 09:56 UTC: production DB row repaired, missing `Андрей Анисимов` fact inserted, Telegraph/VK jobs requeued.
- 2026-06-20 09:56 UTC: Telegraph page edited and VK replacement postponed post `wall-231920894_3986` created with all five speakers.
- 2026-06-20 10:00 UTC: stale VK postponed duplicate `wall-231920894_3978` (generic text without speakers) deleted after owner/text verification.
- 2026-06-20 10:02 UTC: prevention code deployed to Fly app `events-bot-new-wngqia` as machine version `1466`, image `deployment-01KVJ7N1MH1F2F10RZD3VA705B`.
- 2026-06-20 10:03 UTC: `tg_event_publish` job `25025` requeued earlier, but remained pending due normal `tg_spacing`; corrected DB fields will be used when the Telegram cadence reaches event `6244`.

## Root Cause

1. This is a semantic/content-quality defect, not a transport/idempotency defect. The source text was preserved in production DB and contained the full speaker roster.
2. `rich_facts_extract` preserved several speaker facts but still missed one roster item (`Андрей Анисимов, главный архитектор Калининграда`) and the downstream writer did not reliably treat multi-speaker rosters as mandatory public-description content.
3. The first split-create description draft was rejected for repeated logistics; after rejection the create path fell back to a weak baseline/generic description instead of using an LLM cleanup pass to remove logistics while preserving the extracted speaker facts.

## Contributing Factors

- The previous speaker regression contract (`INC-2026-05-11-lecturer-name-and-title-dropped-from-description`) covered a single dedicated `О спикере` section, but not a roster formatted as `NAME` line + role line for a public talk.
- The writer prompt allowed generic category compression (`краеведы`, `учёные`, `эксперты`) instead of explicitly requiring named roster coverage for lectures/discussions.
- The logistics reject was fail-closed for duplicate logistics but too lossy for a successful fact-ledger extraction.

## Automation Contract

### Treat as regression guard when

- changing `rich_facts_extract`, `split_description_writer`, `split_derived_fields` prompts or schemas;
- changing logistics cleanup/reject behavior in the Smart Update split-create path;
- changing Telegram Monitoring / manual forward import flows for lecture/public-talk events;
- changing public Telegraph/VK/TG rendering of `description`, `short_description`, or `search_digest`.

### Affected surfaces

- code: `smart_event_update.py` (`rich_facts_extract`, `split_description_writer`, logistics cleanup rescue);
- tests: `tests/test_smart_update_native_schema.py`;
- data: production event `6244` fields and public Telegraph/VK surfaces;
- queues: `joboutbox` rows `telegraph_build`, `vk_sync`, pending `tg_event_publish` for event `6244`.

### Mandatory checks before closure or deploy

- `python -m pytest tests/test_smart_update_native_schema.py -q` (or equivalent project venv) passes.
- Static prompt check: `rich_facts_extract` requires one named fact for each `NAME` + role roster block and forbids collapsing roster into categories.
- Writer check: split writer can repair a logistics-tainted draft through `split_description_writer_remove_logistics` and keep named speakers.
- Production repair check: event `6244` DB fields, Telegraph page, VK post, and (when published) Telegram event post mention the source-grounded speaker roster.
- Release-governance check: deployed SHA is reachable from `origin/main` if code changed.

### Required evidence

- Source evidence: Telethon read of `https://t.me/kenigevents/4104` with all five speakers.
- Pre-repair DB/public evidence under `artifacts/codex/INC-2026-06-20-tg-speaker-roster-dropped/`.
- Post-repair DB/public evidence showing all five speaker names on managed surfaces.
- Deploy evidence: Fly app/version/image and `/healthz` after release.

## Immediate Mitigation

- Repaired event `6244` canonical DB fields (`description`, `short_description`, `search_digest`) with all five source-grounded speakers.
- Inserted missing `event_source_fact`: `Спикер: Андрей Анисимов, главный архитектор Калининграда`.
- Cleared publication hashes and requeued `telegraph_build` / `vk_sync`.
- Verified Telegraph page now contains all five names.
- Verified VK managed postponed post `wall-231920894_3986` contains all five names and deleted stale managed postponed duplicate `wall-231920894_3978`.
- Requeued `tg_event_publish` earlier; it remains pending under normal Telegram spacing, with corrected DB fields ready for publication.

## Corrective Actions

- Tightened `rich_facts_extract` prompt: multi-speaker `NAME` + role rosters must produce one named fact per block and must not collapse into categories.
- Tightened `split_description_writer` prompt: lecture/public-talk descriptions must include named speaker rosters from `facts_text_clean`.
- Added LLM-first rescue `split_description_writer_remove_logistics`: when a draft is rejected for duplicate logistics, Smart Update asks the LLM to remove only logistics and then re-validates instead of immediately dropping to a generic fallback.

## Follow-up Actions

- [ ] Owner: Smart Update / next quality audit / scan recent lecture/public-talk events for `Спикер:` facts present in `event_source_fact` but absent from `description`/public surfaces.

## Release And Closure Evidence

- deployed SHA: `41d65d42da616650f92ea09d3e90e551fbeee06c` (reachable from `origin/main`).
- deploy path: manual `flyctl deploy -a events-bot-new-wngqia` from clean task worktree; Fly machine `683961db016e28`, version `1466`, image `events-bot-new-wngqia:deployment-01KVJ7N1MH1F2F10RZD3VA705B`.
- regression checks: `/home/dev/projects/events-bot-new-video-lanes-20260618/.venv/bin/python -m pytest tests/test_smart_update_native_schema.py -q` → `27 passed`; `/home/dev/projects/events-bot-new-video-lanes-20260618/.venv/bin/python -m pytest tests/test_tg_monitoring_on_demand.py -q` → `6 passed`.
- production DB/public repair evidence: `artifacts/codex/INC-2026-06-20-tg-speaker-roster-dropped/prod_6244_verify_after_repair.json` shows all five speaker names in DB and only VK postponed `3986` for this title; Telegraph page fetch with cache-bust confirmed all five names and no old generic description.
- post-deploy smoke: `https://events-bot-new-wngqia.fly.dev/healthz` returned `ok=true`, `ready=true`, `issues=[]` after deploy.
- remaining monitoring item: `tg_event_publish` for event `6244` is pending under normal `tg_spacing`; DB `description/search_digest` and Telegraph/VK are already corrected before it publishes.

## Prevention

- Regression tests pin the multi-speaker roster prompt and the logistics-cleanup rescue path.
- Incident index lists this record as an active regression contract for Smart Update prompt/schema/logistics changes.
