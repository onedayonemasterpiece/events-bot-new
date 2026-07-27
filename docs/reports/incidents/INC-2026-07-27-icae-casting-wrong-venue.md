# INC-2026-07-27 ICAE Casting Wrong Venue

Status: closed
Severity: sev1
Service: canonical event data + static site + managed VK/Telegram event publications
Opened: 2026-07-27
Closed: 2026-07-27 08:08 UTC
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
- 2026-07-27 07:54 UTC: prevention commit `96dd54e3` merged to `origin/main`
  in PR `#119`; merge commit `c9a710c8`.
- 2026-07-27 07:56 UTC: Fly release `v1751`, image
  `deployment-01KYH93S03RWEWQ53FPXW3GFS2`, deployed from the clean prevention
  worktree. Machine health checks passed.
- 2026-07-27 07:59–08:01 UTC: production rows `7124` and `4454` and their
  location fact ledger were repaired to `студия 809, Советский проспект, 12`
  after narrow backup tables were created. Telegraph pages for both events,
  the managed Telegram/VK post for `7124`, its Supabase ICS and Telegram
  calendar document were updated in place.
- 2026-07-27 08:06 UTC: a no-write replay on the deployed image used the exact
  `meowafisha/8049` + `sofit_models/145` evidence. `Советский 12` no longer
  matched the ICAE reference, the unsupported ICAE name routed to
  `canonical_location_name_not_in_source`, and the semantic review returned a
  source-grounded studio repair. The valid `Советский 1, 2 этаж` ICAE control
  still matched.
- 2026-07-27 08:07 UTC: final `/healthz` returned `ok=true`, `ready=true`,
  `db=ok`, all scheduler/task checks `ok`, and no issues.

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

- Before mutation, production backups
  `codex_backup_inc_20260727_icae_event` (2 rows) and
  `codex_backup_inc_20260727_icae_event_source_fact` (38 rows) were created.
- Repaired event `7124` and the older cohort hit `4454` to `студия 809`,
  `Советский проспект, 12`, `Калининград`; corrected the three affected
  location fact rows. Post-repair cohort queries return zero event or fact
  rows containing the `ИЦАЭ` + `Советский 12` contradiction.
- Repaired public projections:
  - VK: `https://vk.com/wall-231920894_8045` (the stored `8033` URL was stale);
  - Telegram Afisha: `https://t.me/c/3954607218/2768`;
  - Telegram calendar: `https://t.me/kenigeventscalendar/7705`;
  - Telegraph: `https://telegra.ph/KASTING-na-pokaz-mod-07-26`;
  - older cohort Telegraph:
    `https://telegra.ph/Kasting-v-modelnoe-agentstvo-SOFIT-05-01`;
  - Supabase ICS:
    `https://lnvfarbbofsnkedbfhlt.supabase.co/storage/v1/object/public/events-ics/event-7124-2026-07-26.ics`.
- Authenticated Telegram/VK reads and public Telegraph/ICS reads confirmed
  `студия 809, Советский проспект 12` and no `ИЦАЭ`/`КГТУ` marker. The event
  was already past and therefore absent from the current static event catalog;
  there was no live static page left to edit.

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

- [x] Repair the canonical events and all existing public surfaces after
  preserving narrow row-level backups.
- [x] Add a fail-closed venue-name grounding gate without replacing the
  LLM-first semantic venue decision: a supported address cannot validate an
  unmentioned venue name, and unavailable/uncertain review returns `invalid`.
- [x] Add the exact incident replay, house-number negative/positive controls,
  and a production cohort audit for venue inheritance/default drift.

## Release And Closure Evidence

- prevention SHA: `96dd54e3f0f2c775181079e109df42149e78dbce`;
  reachable from `origin/main` merge `c9a710c877a598469a2c7e77dd496cc16c8a2bb9`
  via `https://github.com/onedayonemasterpiece/events-bot-new/pull/119`.
- deploy path: clean hotfix worktree, `flyctl deploy --remote-only`, Fly release
  `v1751`, image `deployment-01KYH93S03RWEWQ53FPXW3GFS2`, machine
  `2860d45f312248`.
- regression checks:
  - 72 targeted location-reference, location-grounding, Telegram candidate,
    alias-overwrite and bastion tests passed;
  - touched modules compiled and `git diff --check` passed;
  - deployed exact-source no-write replay and the real ICAE positive control
    passed;
  - production cohort query found zero remaining `ИЦАЭ` + `Советский 12`
    event/fact rows.
- CI: `python-ci` passed. The unrelated `static-browser-release-gate` failed
  on pre-existing recommendation-card geometry for event `6822`; this change
  touched no static-site code and the incident-specific static/public checks
  above passed.
- post-deploy verification: `/healthz` ready with no issues; production DB,
  VK, Telegram Afisha, Telegram calendar, both Telegraph pages and the ICS
  projection all show the corrected location.

## Prevention

The source-grounded prevention is now live. House-number matching is
token-boundary safe, generic room words cannot name a known venue, an
unmentioned venue name is escalated even when its address is supported, the
semantic review fails closed, and a successful LLM repair cannot be silently
overwritten by reference normalization. The SQL/public correction was applied
only after these guards were merged and deployed.
