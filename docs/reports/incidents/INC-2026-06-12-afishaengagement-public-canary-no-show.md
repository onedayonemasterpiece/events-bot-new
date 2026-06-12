# INC-2026-06-12-afishaengagement-public-canary-no-show Afisha Engagement Public Canary No-Show

Status: closed
Severity: sev3
Service: Afisha Engagement / VK event publishing
Opened: 2026-06-12
Closed: 2026-06-12
Owners: events-bot
Related incidents: `INC-2026-06-12-vk-partial-media-family-cta`
Related docs: `docs/features/afishaengagement/README.md`, `docs/features/promo-campaigns/README.md`, `docs/operations/runtime-logs.md`

## Summary

After enabling Afisha Engagement public canary activities, operators still saw
only shadow/debug CTA posts and no normal public VK event posts with CTA media.
The all-events public canary was functioning but had only dice misses. The
dedicated `80 историй о главном` public canary did not participate for a
festival event because its activity used the human-readable
`target_group="klgdevents"` while Smart Update passed numeric
`target_group_id="231920894"`.

## User / Business Impact

- Operators could not observe a real public Afisha Engagement CTA post even
  after production rollout.
- The intended 0.5 public canary rate for `80 историй о главном` silently fell
  back to the all-events 0.1 rate, slowing validation and making the rollout
  look broken.
- Shadow fallback continued to work, so normal event publishing was not blocked.

## Detection

- Operator reported on 2026-06-12 that there was still no public non-shadow
  Afisha Engagement CTA post.
- Production `promo_exposure` showed only `VK_SCHEDULED_DEBUG` rows for
  `surface='afishaengagement'` after rollout.
- Runtime file logs showed public all-events dice misses and no 80-stories
  public candidate for event `4759`, despite `event.festival='80 историй о
  главном'`.

## Timeline

- 2026-06-12 17:10 UTC: Afisha Engagement public canary campaigns were active
  in production.
- 2026-06-12 17:15-17:38 UTC: events `5953`-`5957` matched the all-events
  public activity but missed dice at `apply_rate=0.1`, then fell through to
  shadow fallback.
- 2026-06-12 18:41 UTC: event `4759` (`80 историй о главном`) also matched only
  the all-events public activity, missed dice at `apply_rate=0.1`, and fell
  through to shadow fallback.
- 2026-06-12: investigation found `_group_matches` compared `klgdevents`
  literally with `231920894`, so the 80-stories public campaign was skipped.

## Root Cause

1. Afisha Engagement group matching compared configured `target_group` and
   runtime `target_group_id` as raw strings after removing `-`.
2. Initial 80-stories configs used the older readable group name `klgdevents`,
   while Smart Update called Afisha Engagement with numeric VK group id
   `231920894`.
3. There was no regression test pinning the alias behavior for public canary
   candidate ordering.

## Contributing Factors

- Public dice misses are not stored as `promo_exposure` rows, so DB-only review
  made the issue look like total public-path absence until runtime logs were
  checked.
- The all-events fallback was working and masked the missing 80-stories
  candidate by still creating shadow/debug output.

## Automation Contract

### Treat as regression guard when

- Changing `afishaengagement.py::resolve_candidates`,
  `afishaengagement.py::_group_matches`, Afisha Engagement public/shadow
  fallback ordering, or production promo activity target-group config.

### Affected surfaces

- `afishaengagement.py::_group_matches`
- `afishaengagement.py::resolve_candidates`
- `promo_activity.config_json.target_group`
- Afisha Engagement public canary and shadow fallback scheduling
- Runtime logs under `/data/runtime_logs`

### Mandatory checks before closure or deploy

- `tests/test_afishaengagement.py::test_resolve_candidates_matches_klgdevents_alias_to_numeric_group`
- `tests/test_afishaengagement.py::test_public_engagement_copy_schedules_without_debug_marker`
- `tests/test_afishaengagement.py::test_shadow_debug_copy_falls_through_after_candidate_dice_miss`
- Full `tests/test_afishaengagement.py`
- Production candidate/config verification that campaign `8` / activity `25`
  can match `event_id=4759` when called with target group `231920894`, or an
  explicit blocker if the event is no longer available.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted and full Afisha Engagement pytest output.
- Production DB/log evidence for the pre-fix miss.
- Post-deploy production candidate check for `event_id=4759`.

## Immediate Mitigation

- Keep the shadow fallback active at `apply_rate=1.0` so all missed or skipped
  public candidates continue to create debug audit copies.

## Corrective Actions

- Add Afisha Engagement group alias matching so default
  `target_group="klgdevents"` matches numeric `231920894`.
- Add regression coverage for 80-stories public candidate ordering ahead of
  all-events fallback.
- Document the alias behavior in the Afisha Engagement feature guide.

## Follow-up Actions

- [x] After deploy, verify the next 80-stories event evaluates the 0.5 public
  campaign before all-events fallback.
- [x] After a real public dice winner appears, capture `VK_SCHEDULED`
  `promo_exposure` evidence and operator-visible VK URL.

## Release And Closure Evidence

- deployed SHA: `0ae4c1c2`, reachable from `origin/main`
- deploy path: `flyctl deploy -a events-bot-new-wngqia --remote-only`;
  release `v1359`, image
  `events-bot-new-wngqia:deployment-01KTYR2VGAXJXNS777R6KMDDAC`
- regression checks:
  - `tests/test_afishaengagement.py::test_resolve_candidates_matches_klgdevents_alias_to_numeric_group`
  - `tests/test_afishaengagement.py::test_public_engagement_copy_schedules_without_debug_marker`
  - `tests/test_afishaengagement.py::test_shadow_debug_copy_falls_through_after_candidate_dice_miss`
  - `tests/test_afishaengagement.py::test_family_market_uses_soft_mom_friend_repost_copy`
  - full `tests/test_afishaengagement.py` -> `87 passed`
  - `py_compile afishaengagement.py tests/test_afishaengagement.py`
  - `git diff --cached --check`
- post-deploy verification:
  - `/healthz` returned `ok=true`, `ready=true`, DB and scheduler checks ok.
  - Production `resolve_candidates(event_id=4759, target_group_id=231920894)`
    returned campaign `8` / activity `25` (`target_group=klgdevents`,
    `publish_mode=public`, `apply_rate=0.5`) before all-events fallback; this
    was rechecked after a later parallel deploy left production on machine
    version `1360`.
  - A real public canary winner was compensated for event `5951`: exposure
    `277`, `publish_status=VK_SCHEDULED`, `placement_kind=vk_engagement`,
    URL `https://vk.com/wall-231920894_3137`, `publish_mode=public`,
    `debug_shadow=false`, dice `0.08243301562464822 < 0.1`.
  - VK `wall.getById` confirmed `wall-231920894_3137` exists as a postponed
    wall post, has one generated attachment, and contains no
    `AFISHAENGAGEMENT DEBUG COPY` / `#afishaengagement` marker.
  - Accidental shadow fallback from the 4759 compensation attempt
    (`wall-231920894_3136`, exposure `276`) was deleted via VK
    `wall.delete -> {"response": 1}` and marked `VK_DELETED_DEBUG`.

## Prevention

- Candidate resolution now has a test for short-name-to-numeric group aliasing,
  so future target-group matching changes cannot silently drop the higher-rate
  public canary.
