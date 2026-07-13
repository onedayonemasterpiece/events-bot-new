# INC-2026-07-12 Autoretro one-day exhibition location and period drift

Status: monitoring
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

On 2026-07-13 the operator reported a remaining semantic location defect in the
forwarded announcement `https://t.me/kenigevents/4405`: the first repair had
replaced the hallucinated venue with the source phrase `День города в
Янтарном`, but that phrase names the occasion, not an attendee-facing place.
The original source confirms only the settlement, so the honest canonical
location is city-level `Янтарный` with no invented venue/address.

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
- The recurrence was reported directly from `@kenigevents/4405`; authenticated
  Telegram/VK inspection then confirmed the same prose location in
  `@kldevents/2301` and managed VK post `wall-231920894_7233`.
- Frozen audit export SHA-256: `256ec6c8e18f6c4f7b51eeec7a786dc2b3c7752069ca8657973f6cc841257ae6`.

## Timeline

- 2026-07-12 UTC — operator reported `@kldevents/2297`; incident workflow opened.
- 2026-07-12 16:52 UTC — scheduled `vk_auto_import` run `ops_run=3643`
  created event `6853` from `wall-127107743_14707`.
- 2026-07-12 21:06 UTC — vector sidecar indexed the already-wrong canonical
  event into both `search_v3` and `related_v1`.
- 2026-07-12 22:10 UTC — full audit froze and checked `326/326` active
  current/future rows.
- 2026-07-13 UTC — operator reported prose in location at
  `@kenigevents/4405`; the canonical event was mapped back to `6853`, the
  replacement source album `@kldevents/2301–2304`, managed VK `7233` and the
  existing Telegraph page.

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
7. **The first mitigation preserved an occasion as a venue.** The repair
   correctly removed City Jazz and the invented month, but wrote the verbatim
   source phrase `День города в Янтарном` into `location_name`. Source grounding
   alone was insufficient because the phrase was grounded as event context,
   not as an attendee-facing place.
8. **The semantic grounding router was too narrow.**
   `_candidate_needs_llm_location_grounding_review` required an explicit venue
   role or missing source evidence. The future teaser had neither a `📍`/address
   cue nor an exact venue; because the occasion phrase occurred verbatim, it
   bypassed the LLM verdict and propagated through every public projection.

## Contributing Factors

- The active-event vector sidecar prunes cancelled/repaired historical examples,
  so `6853` retrieved generic long exhibitions rather than the prior bad
  Autoretro rows. Vectors cannot replace source-grounded LLM adjudication.
- Production runtime file logging was disabled during the first investigation,
  so its oldest evidence came from `ops_run`, DB rows and public surfaces. By
  the 2026-07-13 recurrence it was enabled with bounded rotation at
  `/data/runtime_logs`; the active files confirmed event-vector and Telegraph
  work for `6853`. The exact manual/forwarded `@kenigevents/4405` action is not
  represented in `promo_exposure`, which remains an observability gap.
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

Production event `6853` is now repaired to one day (`2026-07-18`), location
`День города в Янтарном`, city `Янтарный`, no address/end date and an
event-local description. The wrong Telegram album `2297–2300` was deleted and
replaced by `https://t.me/kldevents/2301`; managed VK `7233` and the existing
Telegraph page were edited in place. The replacement still uses prior
Autoretro show photos as illustrations, but its caption no longer presents the
past cars/performance as the future program.

The audit repair queue was also applied:

- duplicates `6743 -> 6742` and `6852 -> 6191` merged/tombstoned;
- non-events `6032`, `6033`, `6062`, `6088`, `6098`, `6423`, `6635`
  cancelled; old Telegram captions now explicitly say the card was removed;
- ranges/dates repaired for `6818`, `6117`, `3569`, `3592`, `5581`, `6093`,
  `6346`, `6414`, `6466`, `6497`, `6629`;
- location/time repaired for `6841`, `6844`, `6567`, `6568`;
- event-local text repaired for `6845`, `6846`, `6847`, `6849`, `6407`;
  `6848` remained the verified negative control.

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
- [x] Route `location_name` overlap with structured festival/occasion context
  to the existing bounded LLM grounding review; the router does not rewrite
  semantics, and city-only/occasion-only evidence fails closed.
- [x] Add exact Autoretro recurrence and genuine unrelated-venue negative
  controls; no new per-event LLM stage or deterministic venue inference was
  introduced.

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

- deployed SHA: `59312251` (prevention `e95a596f` + atomic exhibition date
  renderer); both commits are ancestors of `origin/main`. Local E2E diagnostic
  hardening `130d228f` is also on `origin/main` and requires no runtime deploy.
- deploy path: two clean-worktree `flyctl deploy --remote-only` releases;
  machine `2860d45f312248` reached `started`, `/healthz` returned
  `ok=true`, `ready=true`, DB/scheduler/tasks all `ok`.
- regression checks:
  - Smart Update/VK date tests compiled and passed; focused occurrence-role,
    roundup-scope, date-model and one-day Telegraph tests passed;
  - deployed shadow-DB replay of `wall-127107743_14707` called the real LLM
    boundary and returned
    `skipped_non_event:mixed_occurrence_role_review_non_event` before create;
  - grounded one-day indoor and explicit multi-day controls remained accepted;
  - frozen full audit covered `326/326` rows and `326/326` persisted
    `related_v1` vectors before source adjudication.
- production backup tables:
  `codex_backup_event_20260712_autoretro`,
  `codex_backup_event_source_20260712_autoretro`,
  `codex_backup_eventposter_20260712_autoretro`,
  `codex_backup_event_source_fact_20260712_autoretro`,
  `codex_backup_joboutbox_20260712_autoretro`;
  post-repair `PRAGMA quick_check=ok`.
- vector catch-up: `ops_run=3655`, `success`, `complete=true`, `262` actionable
  events, `20` embedding writes, `504` unchanged documents, `64` stale event
  ids pruned and `0` left due to provider cap. Event `6853` has fresh
  `search_v3` + `related_v1` hashes and corrected Янтарный/one-day documents.
- public verification:
  - Telegram `2297` is absent; `https://t.me/kldevents/2301` says only
    `18 июля` and `День города в Янтарном`;
  - VK `https://vk.com/wall-231920894_7233` contains only the corrected
    event-local description, without City Jazz/month/past-performance claims;
  - Telegraph `https://telegra.ph/Den-goroda-v-YAntarnom-07-12` renders
    atomic `18 июля`, no `с 18 июля`, and no wrong venue/range.

### Monitoring blockers before `closed`

- Live Telegram-UI VK auto-import did not start: the role-scoped E2E human
  account `8336351413` receives `Not authorized`. The harness now captures this
  immediately (`terminal_kind=authorization_denied`) instead of timing out.
  Authorization must be restored by the operator/security owner before a real
  1-row live import can close this check; no privilege was granted implicitly.
- Production file-mirror logging remains intentionally disabled after the prior
  Fly disk-pressure incident (`ENABLE_RUNTIME_FILE_LOGGING=0`). Current evidence
  therefore comes from `ops_run`, DB, Telegram UI, VK API and provider logs.
  A bounded-volume/retention design is required before re-enabling it.
- Static-site Kaggle production handoff is disabled and no `static_site_build`
  outbox exists. The old advertised static path was 404 (not stale wrong data);
  the corrected vector card now points at the new canonical path, but publishing
  that page remains a separate static-site operational gate.

## Prevention

Prevention is LLM-first: deterministic structure only routes semantic review and
validates verbatim/date grounding. It is vector-first at audit/recall boundaries:
the exact catalog and nearest pairs are retrieved before source-grounded review,
but vector scores never approve a field, merge or publication.

For the 2026-07-13 recurrence, structured festival overlap is only a high-recall
router into the already-existing `location_grounding_review`. The LLM decides
whether the value is a venue; uncertainty blocks create/merge/publication. This
adds at most one bounded grounding call only to the rare overlap candidates and
does not consume a new call for every processed event.
