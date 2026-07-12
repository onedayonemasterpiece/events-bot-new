# INC-2026-07-12 Autoretro one-day exhibition location and period drift

Status: open
Severity: sev2
Service: event ingestion / Smart Update / public event projections
Opened: 2026-07-12
Closed: —
Owners: events-bot production
Related incidents: `INC-2026-07-10-future-event-semantic-audit.md`, `INC-2026-07-09-recurring-occurrence-date-drift.md`, `INC-2026-07-02-exhibition-duplicates-static-site.md`, `INC-2026-06-28-opening-exhibition-range-duplicate.md`, `INC-2026-06-24-future-event-date-default-venue-regressions.md`, `INC-2026-05-01-daily-location-drift.md`
Related docs: `docs/features/smart-event-update/README.md`, `docs/features/telegram-monitoring/README.md`, `docs/operations/incident-management.md`

## Summary

Public Telegram post `https://t.me/kldevents/2297` exposed a wrong location,
wrong city and invented month-long period for a one-day outdoor classic-car
exhibition. The same Autoretro source and same City Jazz/default-30-day defect
had already been a regression fixture in `INC-2026-07-02`; the deployed guard
matched phrases rather than occurrence roles and missed the next wording.

A vector-first audit then covered the complete bounded current/future inventory
(`326/326` rows, `326/326` persisted `related_v1` vectors) and confirmed further
wrong periods, locations, duplicates, non-events and multi-event description
bleed. Vector similarity was recall only; every action was checked against the
linked source.

## User / Business Impact

- Readers can arrive at the wrong place or on a day when the exhibition does not exist.
- A one-day street exhibition is misrepresented as a venue-bound long-running event.
- The defect can contaminate duplicate matching, vector recall and every managed public projection.

## Detection

- Reported directly by the operator from the public `@kldevents` post.
- Existing broad future-event audits did not surface the defect; that miss is part of the incident.
- Frozen audit export SHA-256: `256ec6c8e18f6c4f7b51eeec7a786dc2b3c7752069ca8657973f6cc841257ae6`.

## Timeline

- 2026-07-12 UTC — operator reported `@kldevents/2297`; incident workflow opened.
- 2026-07-12 16:52 UTC — scheduled `vk_auto_import` run `ops_run=3643`
  created event `6853` from `wall-127107743_14707`.
- 2026-07-12 21:06 UTC — vector sidecar indexed the already-wrong canonical
  event into both `search_v3` and `related_v1`.
- 2026-07-12 22:10 UTC — full audit froze and checked `326/326` active
  current/future rows.

## Root Cause

1. **Direct regression of INC-2026-07-02.** The old deterministic
   `retrospective_future_teaser` shape recognised “прошедшая выставка / ждём на
   следующей выставке”, but not “11 июля наш клуб провёл / увидимся уже в
   следующую субботу”. It therefore did not route the semantic past-recap versus
   future-invite question.
2. **Cross-occurrence duration inference.**
   `_maybe_apply_default_end_date_for_long_event` saw “традиционная выставка” in
   the *past* Светлый block and added one month to the *future* Янтарный date.
3. **Ungrounded reference binding.** City Jazz/Mira 33-35 was absent from the
   source and from `vk_source` defaults, but the location grounding review only
   ran when the source exposed a conventional location label. The hallucinated
   venue passed through.
4. **Temporal provenance was lost between stages.** Rich facts correctly
   described the Mercedes, ГАЗ-24-10 and “Свадебный выезд” as past, while
   canonical logistics facts contained the invented range/venue; the public
   writer then promoted past-occurrence details into the future description.
5. **Roundup block locality was not a stage boundary.** The facts/writer stages
   received the full five-event source for events `6845–6849`, allowing sibling
   dates, performers and programs into target descriptions.
6. **Queue-hint parser contamination.** `ГАЗ-24-10` became a false 24 October
   `event_ts_hint`. It did not determine the final event date, but it is a
   separate retrieval/date-signal defect.

## Contributing Factors

- The active-event vector sidecar prunes cancelled/repaired historical examples,
  so `6853` retrieved generic long exhibitions rather than the prior bad
  Autoretro rows. Vectors cannot replace source-grounded LLM adjudication.
- Production runtime file logging was disabled (`ENABLE_RUNTIME_FILE_LOGGING=0`)
  and the file mirror stopped at 8 July; `ops_run`, DB rows and public surfaces
  were required as fallback evidence.
- Managed VK postponed IDs had promoted/deleted into new live IDs while DB URLs
  remained stale, complicating direct public repair.

## Automation Contract

### Treat as regression guard when

- changing Telegram/VK extraction of exhibition dates/locations, Smart Update merge/date-range semantics, vector identity recall, or public event fanout.

### Affected surfaces

- original Telegram/VK/web sources and poster OCR;
- Telegram Monitoring or VK auto-import extraction;
- `smart_event_update.py` match/merge/writer paths;
- Fly SQLite canonical event/source/poster/job rows;
- `@kldevents`, managed `klgdevents`, Telegraph and static/vector sidecars.

### Mandatory checks before closure or deploy

- source-grounded verdict for exact date/time/outdoor venue/address/city;
- production-boundary replay through the responsible importer and Smart Update;
- at least one negative control for a real multi-day exhibition and one for a one-day indoor event;
- vector-first full future-catalog recall followed by LLM/source adjudication;
- direct post-repair Telegram/VK/Telegraph verification;
- deployed SHA reachable from `origin/main`.

### Required evidence

- pre-repair backups and DB/public surface map;
- replay fixture and pre/post diff;
- targeted regression results;
- deployed SHA and post-deploy/catch-up evidence.

## Immediate Mitigation

Source truth is frozen: one day `2026-07-18`, time unknown, city `Янтарный`,
outdoor City Day auto exhibition; exact square/address is not source-grounded.
The primary repair must use no end date and no invented indoor venue/address.

## Corrective Actions

- [x] Add early LLM-first mixed-occurrence eventness review before defaults,
  vectors and writes; uncertainty/provider failure fails closed.
- [x] Suppress the one-month exhibition fallback when exhibition language belongs
  to a mixed past/future source.
- [x] Add an LLM-selected, verbatim, target-occurrence scope for multi-event
  roundups; rich facts and public writers use the scoped view while preserving
  full source provenance.
- [x] Reject non-verbatim scope output and scope without the target date.
- [x] Ignore compact alphanumeric product/model date shapes such as
  `ГАЗ-24-10` in VK queue hints.
- [x] Freeze direct Telegram/VK/Telegraph/vector/media surface evidence.
- [x] Audit `326/326` future/current rows vector-first and source-adjudicate all
  findings.

## Follow-up Actions

- [x] Record the exact future repair/remove queue in the audit artifact and this
  incident's release evidence.
- [ ] Align `audit_future_event_vectors.py` document hashing with the production
  `related_v1` sync document; today all stored vectors are available but the
  audit runner unnecessarily attempts a full re-embed.
- [ ] Add a persistent incident-prototype/source-pattern vector corpus. Active
  catalog vectors alone forget cancelled regression examples; prototype vectors
  remain recall-only and must feed LLM adjudication.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Prevention is LLM-first: deterministic structure only routes semantic review and
validates verbatim/date grounding. It is vector-first at audit/recall boundaries:
the exact catalog and nearest pairs are retrieved before source-grounded review,
but vector scores never approve a field, merge or publication.
