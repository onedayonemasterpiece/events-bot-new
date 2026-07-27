# INC-2026-07-27 ICAE Casting Wrong Venue

Status: open
Severity: sev1
Service: canonical event data + static site + managed VK/Telegram event publications
Opened: 2026-07-27
Closed: —
Owners: events-bot event import / Smart Update / publication owner
Related incidents: `INC-2026-06-24-future-event-date-default-venue-regressions`, `INC-2026-06-18-tg-location-prose-still-extracted`, `INC-2026-05-09-event-location-alias-free-dup-regressions`, `INC-2026-04-15-gate-location-and-linked-facts-drift`
Related docs: `docs/features/telegram-monitoring/README.md`, `docs/features/smart-event-update/README.md`, `docs/features/static-site-pages/README.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

The public event card for the fashion-show casting on 2026-07-26 named ICAE
Kaliningrad as the venue while the casting organizer confirmed that no casting
was planned at ICAE and that the actual address was Sovetsky Prospekt 12. This
is a customer-visible wrong-venue incident: an unrelated organization was
presented as the host and visitors could be sent to the wrong place.

## User / Business Impact

- ICAE staff had to contact the casting agency after seeing the false venue.
- Readers could travel to an unrelated organization instead of the organizer's
  confirmed address at Sovetsky Prospekt 12.
- The false association damages trust in both the event catalog and ICAE.

## Detection

- Reported by Julia from ICAE Kaliningrad at 09:35–09:37 EET
  (07:35–07:37 UTC) on 2026-07-27 with a screenshot of the managed VK event
  post.
- The screenshot shows `ИЦАЭ (в КГТУ), Советский проспект 12`, combining an
  unrelated canonical venue name with the offsite address.
- No automated venue/address contradiction gate blocked publication; the exact
  import and merge path remains under investigation.

## Timeline

- 2026-07-19 19:00 local: first casting occurrence shown on the poster.
- 2026-07-26 19:00 local: second casting occurrence shown on the reported card.
- 2026-07-27 07:35 UTC: ICAE reports the wrong venue and confirms the agency's
  address as Sovetsky Prospekt 12.
- 2026-07-27 UTC: incident record opened; production evidence collection starts.
- 2026-07-27 UTC: production event `7124`, both Telegram sources and the
  managed Telegram/VK/Telegraph surfaces are confirmed wrong. Runtime evidence
  shows Smart Update first repaired the venue to `студия 809`, then reference
  normalization rebound it to ICAE before insert.
- 2026-07-27 UTC: cohort audit finds one older public false association,
  event `4454`; all ICAE rows with other dates use the real `Советский 1`
  address or need no change for this incident class.

## Root Cause

1. `location_reference.match_known_venue_by_address()` used raw substring
   containment for fuzzy addresses. Normalized `Советский 1` was therefore
   considered contained in `Советский 12`, and the unique reference hit was
   ICAE.
2. The primary candidate entered Smart Update as source-grounded
   `Советский пр-т 12, 8 этаж, студия 809`. The LLM
   `location_grounding_review` correctly repaired it to `студия 809`, but the
   unconditional reference-normalization pass after that LLM decision applied
   the false address-prefix match again and overwrote the repair with ICAE.
3. The linked `sofit_models/145` enrichment cloned the already-mutated
   candidate. Its source supported the address but never mentioned ICAE.
   `_candidate_needs_llm_location_grounding_review()` treated either a
   supported name **or** supported address as sufficient, so the unsupported
   canonical venue name bypassed the second semantic review.
4. Smart Update duplicate/address recall used the same unsafe substring
   relation, leaving another path by which house `1` and house `12` could be
   treated as the same venue anchor.

## Contributing Factors

- The public value combines a canonical organization label with a different
  street address, so name-only normalization was allowed to survive an
  address-level contradiction.
- Generic room tokens such as `этаж` and `студия` were eligible for fuzzy venue
  name overlap. The Telegram server guard rejected one attempted
  `Театр Третий этаж` normalization, but that safety did not cover the later
  Smart Update address rebind.
- There was no regression test for a house-number prefix collision with a
  positive control that retains legitimate room/floor suffix tolerance.

## Automation Contract

### Treat as regression guard when

- changing Telegram/VK event extraction or source-default location handling;
- changing location reference/alias normalization or Smart Update merge logic;
- changing event identity matching where an existing row may donate its venue;
- rebuilding static-site or managed VK/Telegram/Telegraph event publications.

### Affected surfaces

- production `event`, `event_source`, source decision/log and publication rows;
- `source_parsing/telegram/handlers.py`, Telegram/VK intake and Smart Update;
- `location_reference.py` and the canonical location references;
- static site, managed VK `klgdevents`, Telegram `@kldevents`, and Telegraph.

### Mandatory checks before closure or deploy

- Recover the exact event id, all source rows and the original source post(s).
- Prove where `ИЦАЭ` entered the pipeline and whether an existing event/default
  location contributed it.
- Add an incident replay with the reported venue/address contradiction and at
  least one valid same-venue control.
- Verify the corrected canonical row and every already-published surface.
- Audit the adjacent 2026-07-19 occurrence and other active rows with the same
  `ИЦАЭ` + `Советский проспект 12` contradiction.
- Run targeted venue grounding/merge tests, touched-module compile checks,
  `git diff --check`, and release-governance checks.
- After deploy, verify `/healthz`, production logs/DB state, and that the
  deployed SHA is reachable from `origin/main`.

### Required evidence

- Pre/post production DB JSON and minimal source/runtime excerpts under
  `artifacts/codex/INC-2026-07-27-icae-casting-wrong-venue/`.
- Original and repaired public VK/Telegram/Telegraph/static URLs where present.
- Targeted test output and deployed SHA/release id.
- Confirmation that the prevention commit is reachable from `origin/main`.

## Immediate Mitigation

- Incident-scoped production/public repair is queued after the prevention SHA
  is deployed. No row was mutated before the source chain and public post ids
  were preserved.

## Corrective Actions

- Fuzzy reference-address and duplicate-address matching now require complete
  normalized token boundaries, so `1` cannot match `12` while a suffix such as
  `2 этаж` remains allowed.
- Generic room/layout tokens (`студия`, `кабинет`, `этаж`, `корпус`,
  `помещение`) no longer identify a known venue by themselves.
- A supported address no longer grounds an unmentioned `location_name`;
  suspicious social candidates route to the LLM review.
- Post-review reference normalization cannot undo a source-grounded
  `llm_repair` with a new ungrounded venue name.
- Added the exact replay fixture and positive/negative controls for event
  `7124`, `meowafisha/8049`, and `sofit_models/145`.

## Follow-up Actions

- [ ] Repair the canonical event and all public surfaces after preserving a
  narrow row-level backup.
- [ ] Add a fail-closed venue-name/address contradiction guard without replacing
  the LLM-first semantic venue decision.
- [ ] Add replay and cohort audit coverage for venue inheritance/default drift.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: pending
- post-deploy verification: pending

## Prevention

Closure requires a source-grounded fix that rejects or escalates a canonical
venue name when its known address contradicts an explicit event address. A
one-off SQL correction does not satisfy this contract.
