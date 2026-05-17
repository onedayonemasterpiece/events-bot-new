# INC-2026-05-17 Eco CherryFlash Underfilled Event Recall

Status: open
Severity: sev2
Service: CherryFlash partner story tracks / eco event selection
Opened: 2026-05-17
Closed: —
Owners: bot/runtime
Related incidents: `INC-2026-05-15-cherryflash-partner-fanout-promo-filter`
Related docs: `docs/features/cherryflash/partner-story-tracks.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-05-17 the scheduled `partner_eco_nature_001` CherryFlash story selected only two eco/local-history events, and the visible result looked underfilled. A clearly relevant same-day event, `Фестиваль «Зелёный Кёнигсберг»` (`event_id=4174`), was active in production with a poster and source metrics but did not enter the selector.

## User / Business Impact

- The eco/nature partner story underrepresented the available same-day event inventory.
- A high-fit botanical festival was missed even though it was active on the day of publication.
- The miss weakened trust in the partner filter because the output looked like there were no other profile events.

## Detection

The operator reported after the 2026-05-17 eco/nature CherryFlash publication that only one event appeared visible and asked why `Зелёный Кёнигсберг` / the eco festival was absent. Production file logs and production DB rows were checked first.

## Timeline

- 2026-04-23 09:35 UTC: source post `https://t.me/kulturnaya_chaika/7603` for `event_id=4174` was published.
- 2026-04-23 23:43 UTC: Telegram metrics for that source were collected (`views=832`, `likes=5`, `age_day=0`).
- 2026-05-16 10:59 UTC: eco partner session `#310` published six events but did not include `event_id=4174`.
- 2026-05-17 10:30 UTC: `video_partner_track_eco` launched session `#316`.
- 2026-05-17 10:32 UTC: logs show LLM filtering over recent popularity candidates and final selection `[5045, 4947]`; `event_id=4174` was not evaluated by the partner filter.
- 2026-05-17 11:06 UTC: session `#316` reached `PUBLISHED_TEST`.
- 2026-05-17 11:25 UTC: operator reported the underfilled output and missing botanical festival.

## Root Cause

1. The partner eco selector inherited the base CherryFlash candidate universe from `/popular_posts`, which only considers source posts published in the last 1/3/7 days.
2. `Фестиваль «Зелёный Кёнигсберг»` was a current same-day event, but both known source posts were from 2026-04-23, outside the 7-day publication window on both 2026-05-16 and 2026-05-17.
3. The LLM-first eco classifier only ran after the recent-popularity candidate stage, so it never received this highly relevant event.
4. Selection trace stored only selected events, not enough rejected/never-considered candidate evidence to make the recall gap obvious without a production probe.

## Contributing Factors

- Base CherryFlash optimizes for recently popular posts, while partner tracks also need profile completeness for current/future events.
- The eco filter had a broad semantic contract, but the upstream candidate recall was narrower than that contract.
- The production run also spent several Gemma attempts on unrelated recent popularity candidates, increasing quota pressure before the selector reached enough profile matches.

## Automation Contract

### Treat as regression guard when

- Changing `video_announce/popular_review.py` partner-track candidate collection, event-date logic, cooldown, or trace generation.
- Changing `eco_prirodnaya` recall keywords or LLM filter wiring.
- Changing `/popular_posts` source publication windows used by CherryFlash.

### Affected surfaces

- `video_announce/popular_review.py`
- `video_announce/partner_filters.py`
- `handlers/popular_posts_cmd.py`
- `video_announce/scenario.py`
- scheduled job `video_partner_track_eco`
- production runtime logs and `videoannounce_session.selection_params["popular_review_trace"]`

### Mandatory checks before closure or deploy

- `tests/test_video_announce_popular_review.py` must prove the eco partner selector recalls a current/future event whose source post is older than the recent popularity windows, while still requiring the partner `event_filter`.
- Existing partner promo regression tests must still pass: off-filter promo only after three profile matches and max one off-filter item.
- Production `/healthz` after deploy.
- Post-deploy read-only production check that the current-day `Фестиваль «Зелёный Кёнигсберг»` enters the eco partner selection candidate path or is selected by a compensating rerun while still passing the partner filter.
- If the same-day scheduled slot already published underfilled content, perform a compensating rerun/catch-up and verify the replacement/current-day data path.

### Required evidence

- deployed SHA:
- tests:
- production logs / DB probe:
- compensating rerun session id:
- production health:
- fix reachable from `origin/main`:

## Immediate Mitigation

Pending deploy: add an event-date recall path for `partner_eco_nature_001` so current/future eco-track candidates with older source posts can be submitted to the existing LLM-first partner filter.

## Corrective Actions

- Add `partner_event_date_recall` candidates for `partner_eco_nature_001` from active current/future events within a bounded lookahead window.
- Use deterministic keyword hints only as a broad recall prefilter; the semantic include/exclude decision remains the existing LLM partner `event_filter`.
- Keep recall candidates below true popularity scores and after recent-popularity matches, preserving CherryFlash popularity priority while filling underfilled partner selections.
- Record `source_window=partner_event_date_recall` in selection trace for observability.

## Follow-up Actions

- [ ] Add richer candidate-count diagnostics to `popular_review_trace`: total recent hits, filtered hits, recall hits, and top rejected reasons.
- [ ] Consider an operator dry-run report for partner tracks that lists high-fit current/future candidates excluded before LLM and why.

## Release And Closure Evidence

- deployed SHA:
- deploy path:
- regression checks:
- post-deploy verification:
- compensating rerun:

## Prevention

The incident record is the regression contract for partner eco event-date recall: a current/future profile event must not be invisible solely because its source post was published more than seven days earlier.
