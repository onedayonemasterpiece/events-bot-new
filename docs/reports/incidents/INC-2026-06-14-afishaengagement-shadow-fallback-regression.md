# INC-2026-06-14-afishaengagement-shadow-fallback-regression Afisha Engagement Shadow Fallback Regression

Status: monitoring
Severity: sev3
Service: Afisha Engagement / VK event publishing
Opened: 2026-06-14
Closed: —
Owners: events-bot
Related incidents: `INC-2026-06-12-afishaengagement-public-canary-no-show`, `INC-2026-06-12-vk-partial-media-family-cta`
Related docs: `docs/features/afishaengagement/README.md`, `docs/reports/incidents/README.md`, `docs/operations/runtime-logs.md`

## Summary

After expanding public Afisha Engagement CTA rates, the old visual-audit
debug-shadow fallback still ran after normal plain VK event posts. A public CTA
dice miss therefore produced one normal plain postponed post and then a separate
marked debug CTA copy, making the VK postponed queue look dominated by shadow
posts and violating the production contract of one chosen publication variant.

## User / Business Impact

- Operators saw many `#afishaengagement` debug-shadow postponed posts shortly
  after Telegram Monitoring.
- Ordinary production posts were still created, but public CTA winners were
  hidden among many debug copies and dice misses created noisy duplicates.
- The rollout could not be validated visually because the queue no longer
  reflected the intended production behavior: CTA or plain, not both.

## Detection

- Operator reported on 2026-06-14 that most newly postponed VK posts were shadow
  copies and there were no obvious ordinary public CTA posts in the queue.
- Authenticated VK postponed inspection for `klgdevents` showed 32 postponed
  items, 16 of them marked `AFISHAENGAGEMENT DEBUG COPY` /
  `#afishaengagement`.
- Production `promo_exposure` rows after the Telegram Monitoring import were
  dominated by campaign `6` / activity `15` with
  `publish_status='VK_SCHEDULED_DEBUG'`.
- Runtime file logs showed `public_create_preflight` dice misses followed by
  `shadow_after_plain_create` applying campaign `6` / activity `15`.

## Timeline

- 2026-06-13 20:15 UTC: production public CTA rates were expanded and new
  public campaign targets were enabled.
- 2026-06-13 23:58-2026-06-14 00:20 UTC: Telegram Monitoring imported new
  events. Public all-events candidates sometimes won, but public dice misses
  fell into `shadow_after_plain_create`.
- 2026-06-14: operator reported that VK postponed was again mostly shadow
  copies instead of production CTA posts.
- 2026-06-14: investigation confirmed normal `sync_vk_source_post` still called
  `shadow_after_plain_create` and `shadow_after_plain_update`.
- 2026-06-14 01:36 UTC: production debug-shadow activities `13`, `14`, and
  `15` were disabled; public activities `25`, `26`, `27`, `28`, and `29`
  remained enabled.
- 2026-06-14 01:40 UTC: 17 postponed VK posts with Afisha Engagement debug
  markers were deleted; a follow-up VK postponed scan returned zero debug
  marker candidates and 15 remaining ordinary postponed posts.
- 2026-06-14 01:44 UTC: fix deployed to Fly machine version `1400`.

## Root Cause

1. The visual-audit phase implemented shadow fallback as a post-write side
   effect after plain `wall.post` / `wall.edit`.
2. Public CTA rollout moved the fresh-post decision into
   `public_create_preflight`, but the old `shadow_after_plain_create` and
   `shadow_after_plain_update` hooks were left in the normal Smart Update path.
3. Legacy broad debug activities remained enabled in production, especially
   the all-events debug-shadow activity with `apply_rate=1.0`.

## Contributing Factors

- Older regression docs still described public dice misses falling through to
  shadow fallback.
- Unit tests pinned the transitional visual-audit behavior instead of the final
  production contract.
- Public dice misses are log-only, while debug shadows create durable
  `promo_exposure` rows, making shadow output look even more dominant in DB
  review.

## Automation Contract

### Treat as regression guard when

- Changing `main_part2.py::sync_vk_source_post`,
  `afishaengagement.py::maybe_publish_shadow_debug_copy`, Afisha Engagement
  public/shadow candidate config, or production promo activity enablement.

### Affected surfaces

- `main_part2.py::sync_vk_source_post`
- `afishaengagement.py`
- `promo_activity.config_json.debug_shadow`
- VK postponed queue for `klgdevents`
- `promo_exposure` rows with `surface='afishaengagement'`

### Mandatory checks before closure or deploy

- `tests/test_vk_source.py::test_sync_vk_source_post_uses_afishaengagement_preflight_for_new_public_post`
- `tests/test_vk_source.py::test_sync_vk_source_post_keeps_plain_post_after_public_cta_miss`
- `tests/test_vk_source.py::test_sync_vk_source_post_does_not_run_afishaengagement_shadow_on_update`
- `tests/test_afishaengagement.py::test_resolve_candidates_matches_klgdevents_alias_to_numeric_group`
- `tests/test_afishaengagement.py::test_public_engagement_copy_schedules_without_debug_marker`
- Production DB check that legacy debug-shadow activities are disabled or have
  no normal-sync path to execute.
- VK postponed check that marked debug copies were removed or are explicitly
  approved for a manual debug batch.

### Required evidence

- Deployed SHA reachable from `origin/main`.
- Targeted pytest output.
- Production DB evidence for disabled debug activities.
- VK postponed cleanup evidence for debug-marker posts.
- Runtime log or DB evidence that new public misses create only plain posts.

## Immediate Mitigation

- Disabled legacy debug-shadow Afisha Engagement activities `13`, `14`, and
  `15` in production.
- Deleted 17 postponed VK posts containing Afisha Engagement debug markers.
- Verified VK postponed debug-marker candidates: `0`.

## Corrective Actions

- Remove `shadow_after_plain_create` and `shadow_after_plain_update` from the
  normal Smart Update VK sync path.
- Update regression tests to require one chosen production publication variant.
- Update Afisha Engagement docs and supersede the old visual-audit fallback
  requirement from `INC-2026-06-12-afishaengagement-public-canary-no-show`.

## Follow-up Actions

- [ ] Capture post-deploy evidence from the next Telegram Monitoring batch that
  public dice misses leave only plain VK event posts.
- [ ] Keep the global all-events public `apply_rate=0.15`; it was restored on
  2026-06-15 after the CTA format audit found production had drifted to 0.1.

## Release And Closure Evidence

- deployed SHA: `db7a85ba`, reachable from `origin/main`
- deploy path: `flyctl deploy -a events-bot-new-wngqia --remote-only`; image
  `events-bot-new-wngqia:deployment-01KV1WS0HM0R77MXS7M9C668ZV`, machine
  version `1400`
- regression checks:
  - `python3 -m py_compile main_part2.py tests/test_vk_source.py afishaengagement.py`
  - `git diff --check`
  - targeted pytest was not runnable in this worktree because `pytest` was not
    installed (`python3: No module named pytest`)
- post-deploy verification:
  - `/healthz`: `ok=true`, `ready=true`, `db=ok`, scheduler critical jobs `ok`
  - `fly status`: machine `48e42d5b714228` started, `1 total, 1 passing`
  - Fly logs: `BOOT_OK pid=644` and `Running on http://0.0.0.0:8080`
  - production DB: debug-shadow activities `(13, 14, 15)` disabled and public
    activities `(25, 26, 27, 28, 29)` enabled
  - VK postponed queue: `postponed_count=15`, debug-marker candidates `0`

## Prevention

- The normal VK event publication boundary owns the CTA/plain decision. Debug
  shadow copies may be created only by explicit debug/manual flows, not as a
  side effect after production `wall.post` or `wall.edit`.
