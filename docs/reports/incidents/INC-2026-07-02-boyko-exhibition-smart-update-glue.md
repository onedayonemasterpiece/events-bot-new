# INC-2026-07-02 Boyko lecture glued to KGD80 exhibition in Telegram event post

Status: mitigated
Severity: sev3
Service: Smart Update / Telegram event publishing / Telegraph event page
Opened: 2026-07-02
Closed: —
Owners: events-bot operators
Related incidents: `INC-2026-06-29-kgd80-ticket-location-drift`, `INC-2026-05-30-active-duplicate-events-recall-gate`, `INC-2026-07-02-exhibition-duplicates-static-site`
Related docs: `docs/operations/incident-management.md`, `docs/reports/incidents/README.md`, `docs/llm/request-guide.md`, `docs/features/tg-publishing/README.md`

## Summary

On 2026-07-02, public Telegram event post `https://t.me/kldevents/1734?single` exposed a hybrid event:

- lecture: `Калининград и область как кинодекорация — история съёмок художественных фильмов в регионе`, speaker Андрей Бойко, `2026-07-03 18:30`, Историко-художественный музей;
- exhibition: `Калининградская область. История любви`, opening/ongoing exhibition from `2026-07-03`.

The public copy incorrectly titled the card as `... и выставка «Калининградская область. История любви»` and said that visitors would see the exhibition in parallel with the lecture, creating the impression that Андрей Бойко was related to the exhibition. The operator reported that Андрей Бойко said he has no relation to the exhibition.

This incident is limited to source investigation, production data repair, public artifact deletion, and incident documentation. No code changes were made because the offending Smart Update merge happened before the vector-search recall work that is expected to reduce this class of false matches.

## User / Business Impact

- Public `@kldevents` post `1734` misrepresented the event identity and a speaker's relation to an exhibition.
- The event row `5077` mixed lecture and exhibition sources, posters, title, description, date range, topics, and Telegraph page content.
- Users could see a month-long event range (`3 июля–3 августа 18:30`) on a lecture and a misleading hybrid title.

## Detection

- Detected manually by the operator from `https://t.me/kldevents/1734?single` after feedback from Андрей Бойко.
- Public Telegram embed and production DB rows confirmed the hybrid copy.
- Source-only investigation compared the attached event sources and poster OCR; no replay or code experiment was needed.

## Timeline

- 2026-05-18 04:59 UTC: `@kraftmarket39/237` created event `5077` as the lecture by Андрей Бойко. Poster OCR says `Лекция... 3 июля | 18:30`, `Историко-художественный музей`, Андрей Бойко.
- 2026-06-29: prior KGD80 repair restored the exact registration URL for event `5077` and produced correct public Telegram compensation post `https://t.me/kldevents/1611` / `1612`.
- 2026-07-02 00:11-04:17 UTC: Smart Update attached exhibition sources `@koihm/5811`, `@koihm/5812`, and `vk.com/wall-29891284_13972` to event `5077`; facts recorded title/date conflicts but the row still absorbed exhibition facts, posters, end date, and topics.
- 2026-07-02 08:22 UTC: normal `tg_event_publish` published hybrid album `https://t.me/c/3954607218/1734`/`1735` (`https://t.me/kldevents/1734?single`).
- 2026-07-02 18:08 UTC: production DB backup and repair applied for event `5077`; bad Telegram messages `1734` and `1735` deleted.
- 2026-07-02 18:09-18:12 UTC: Telegraph/ICS/festival jobs completed after invalidation; Telegraph public page was rebuilt lecture-only.

## Root Cause

1. Event `5077` was a legitimate KGD80 lecture event with title/date/location anchors overlapping the exhibition opening day and venue.
2. Smart Update accepted exhibition source posts as duplicate/update sources for the lecture because both sources shared `2026-07-03`, the Историко-художественный музей, KGD80 context, and the general `80 историй о главном` campaign context.
3. The merge wrote conflict facts (`semantic_title_mismatch`, lecture date conflict) but still persisted exhibition facts/posters/end date into the canonical lecture row.
4. Telegram event publishing then rendered the already-corrupted row, combining lecture and exhibition into one public album.

## Contributing Factors

- The original source `@kraftmarket39/237` itself is not perfectly clean: text contains `пятница 13 июня 18:30`, while poster/OCR and current registration page indicate `3 июля 18:30`. This date ambiguity made the row historically fragile.
- Pre-vector Smart Update duplicate recall relied too much on date/location/campaign overlap for semantically different KGD80 events.
- Exhibition source imports were not stopped by the recorded title/date conflicts.
- Old managed VK/source rows were present as `event_source` rows and inflated the Telegraph source count until cleanup.

## Automation Contract

### Treat as regression guard when

- changing Smart Update duplicate/update matching, especially for KGD80 / `80 историй о главном` events;
- changing source aggregation, event_source facts, poster rehydration, or Telegraph/TG rendering for multi-source rows;
- importing or repairing lecture/exhibition pairs sharing date, venue, and festival/campaign context;
- touching vector-search recall or semantic identity checks for Smart Update.

### Affected surfaces

- production `event` row `5077`;
- `event_source`, `event_source_fact`, and `eventposter` aggregation for event `5077`;
- public Telegram `@kldevents` album `1734`/`1735`;
- existing correct Telegram post `@kldevents/1611`/`1612`;
- Telegraph page `https://telegra.ph/Kaliningrad-i-oblast-kak-kinodekoraciya--istoriya-syomok-hudozhestvennyh-filmov-v-regione-05-18-2`.

### Mandatory checks before closure or deploy

- Source audit must show lecture-only source facts for `event_id=5077` and no `Калининградская область. История любви` / exhibition facts in the row description, title, topics, date range, posters, Telegraph page, or current Telegram canonical post.
- Public smoke must confirm `@kldevents/1734` and `/1735` are no longer visible with hybrid text.
- Public smoke must confirm `@kldevents/1611` remains lecture-only and points to the repaired Telegraph page.
- Telegraph page must contain `Андрей Бойко`, `3 июля в 18:30`, `Источников: 2`, and no exhibition wording.
- `/healthz` must remain `ok=true`, `ready=true` after production data repair.

### Required evidence

- Production backup tables:
  - `codex_backup_20260702_boyko_glue_5077_event` (1 row)
  - `codex_backup_20260702_boyko_glue_5077_event_source` (8 rows)
  - `codex_backup_20260702_boyko_glue_5077_event_source_fact` (57 rows)
  - `codex_backup_20260702_boyko_glue_5077_eventposter` (4 rows)
  - `codex_backup_20260702_boyko_glue_5077_joboutbox` (6 rows)
  - `codex_backup_20260702_boyko_glue_5077_promo_exposure` (23 rows)
  - `codex_backup_20260702_boyko_glue_5077_stale_vk_sources_event_source` (3 rows)
- Production DB `PRAGMA quick_check=ok` before and after repair.
- Public Telegram/Telegraph smoke excerpts.
- `/healthz` JSON after repair.

## Immediate Mitigation

Applied on production DB on 2026-07-02:

- repaired event `5077` back to lecture-only:
  - title `Калининград и область как кинодекорация — история съёмок художественных фильмов в регионе`;
  - `date=2026-07-03`, `time=18:30`, `end_date=NULL`, `event_type=лекция`;
  - description/search digest/short description removed all exhibition wording;
  - topics reduced to lecture/cinema/Kaliningrad-local-history themes;
  - source_texts reduced to the lecture source;
  - current canonical Telegram post restored to already-correct `https://t.me/c/3954607218/1611` (`@kldevents/1611`).
- deleted exhibition source rows `7644282`, `7644306`, `7644327` and their facts from event `5077`;
- deleted exhibition poster rows `13260`, `13261`, `13262`;
- deleted stale managed VK source rows `2897481`, `2975151`, `7265803` so Telegraph source count is no longer inflated;
- deleted problematic Telegram album messages `@kldevents/1734` and `@kldevents/1735`;
- rebuilt Telegraph page; current public page is lecture-only and has `Источников: 2`;
- invalidated/rebuilt dependent Telegraph/ICS/festival jobs through the normal outbox.

## Corrective Actions

No code changes in this incident by design. The suspected class is a pre-vector Smart Update semantic-glue failure. Current vector-search/semantic recall work should be used as the preventive layer, but this incident remains a regression contract for future Smart Update matching changes.

## Follow-up Actions

- [ ] On the next Smart Update/vector-search validation pass, include a lecture-vs-exhibition same-date/same-venue KGD80 fixture based on `@kraftmarket39/237` + `@koihm/5811`/`5812`/`vk.com/wall-29891284_13972`.
- [ ] Decide whether the exhibition `Калининградская область. История любви` needs a separate clean canonical event row; do not recreate it by attaching it to event `5077`.
- [ ] If similar title/date conflict facts are produced in future, verify that they block merge side effects instead of merely recording a conflict after data has been absorbed.

## Release And Closure Evidence

- deployed SHA: not applicable; code was not changed.
- deploy path: no deploy.
- regression checks: source-only production audit; row-level DB backups; public Telegram deletion; Telegraph smoke; healthz smoke.
- post-repair verification:
  - `event.id=5077`: lecture-only title, no end date, `photo_count=2`, current `tg_event_post_url=https://t.me/c/3954607218/1611`.
  - public `@kldevents/1734` and `/1735`: no public hybrid text after deletion.
  - public `@kldevents/1611` and `/1612`: lecture-only text, no exhibition wording.
  - Telegraph page: contains `Андрей Бойко`, `3 июля в 18:30`, `Источников: 2`; does not contain `выстав` / `Калининградская область. История любви`.
  - production `/healthz`: `ok=true`, `ready=true`, `db=ok`.

## Prevention

- Do not use shared date/location/campaign context alone as proof that a lecture and an exhibition are one event.
- For Smart Update source aggregation, recorded `semantic_title_mismatch` or date conflict must be treated as a merge safety signal, not just an informational fact.
- Keep this record as the regression contract for KGD80 lecture/exhibition glue failures.
