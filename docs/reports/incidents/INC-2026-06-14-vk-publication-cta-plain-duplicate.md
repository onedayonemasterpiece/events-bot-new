# INC-2026-06-14 VK Publication CTA/Plain Duplicate

Status: mitigated
Severity: sev2
Service: promo VK publication / Afisha Engagement public CTA rollout
Opened: 2026-06-14
Closed: —
Owners: Codex / events-bot maintainers
Related incidents: `INC-2026-06-13-vk-poster-text-datetime-conflict-and-duplicate-cta`, `INC-2026-06-14-afishaengagement-shadow-fallback-regression`, `INC-2026-06-14-morning-import-quality-and-outbox-stale`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/features/afishaengagement/README.md`, `docs/operations/runtime-logs.md`, `docs/operations/release-governance.md`

## Summary

On 2026-06-14 the `klgdevents` VK wall received the same event post twice for
`event_id=5783` / `https://t.me/kraftmarket39/274`: a plain `vk_publication`
post and a public Afisha Engagement CTA post. The desired contract is exactly
one production variant per publication pass, preferably CTA when public CTA
generation wins.

## User / Business Impact

- Readers could see two consecutive VK posts with identical text for the same
  event.
- The plain duplicate diluted the intended CTA rollout and made the wall look
  spammy.
- Promo exposure accounting counted the plain `vk_publication` as a successful
  public action even though it was superseded by the CTA variant.

## Detection

- Operator reported `https://vk.com/wall-231920894_3369` as the postponed CTA variant and
  `https://vk.com/wall-231920894_3370` as the same post without CTA. After
  publication, VK remapped the CTA postponed id `3369` to live wall post
  `https://vk.com/wall-231920894_3372`.
- Authenticated VK API, not public HTML, confirmed both posts belonged to owner
  `-231920894`, had the same normalized text hash `31b93cb07f26df69`, and used
  different photo attachments.
- Production DB confirmed `promo_exposure.id=358` as
  `surface='afishaengagement'`, `placement_kind='vk_engagement'`,
  `publish_status='VK_SCHEDULED'`, target postponed id `wall-231920894_3369` (later reconciled to live `wall-231920894_3372`), while
  `promo_exposure.id=359` was the plain `surface='vk_publication'` exposure for
  the same `event_id=5783`.

## Timeline

- 2026-06-14 16:15 UTC: promo runner scheduled the event; production DB created
  Afisha Engagement public exposure `358` and plain `vk_publication` exposure
  `359` for `event_id=5783`.
- 2026-06-14 16:26 UTC: VK published the plain duplicate as
  `wall-231920894_3370` (stored exposure still referenced postponed id `3368`).
- 2026-06-14 16:33 UTC: operator reported the CTA/plain duplicate pair.
- 2026-06-14 16:34 UTC: authenticated VK and DB probes confirmed the duplicate;
  `3369` was still in the postponed queue and mapped to the CTA exposure.
- 2026-06-14 16:35 UTC: immediate mitigation deleted plain duplicate `3370`,
  kept the CTA postponed post `3369`, and marked exposure `359` as
  `VK_DELETED_DUPLICATE`.
- 2026-06-14 16:40 UTC: VK wall scan found the kept CTA post published as
  live `https://vk.com/wall-231920894_3372`; exposure `358` was reconciled to
  this live URL and exposure `359` now points `superseded_by_url` at `3372`.

## Root Cause

1. Previous CTA/plain duplicate fixes covered Smart Update managed VK sync
   (`sync_vk_source_post`) but not promo `vk_publication`.
2. `promo.py::_build_promo_vk_source_post` created the plain `post_to_vk` wall
   post first, then called `maybe_publish_shadow_debug_copy` without
   `public_only` or `shadow_only` restrictions.
3. After public Afisha Engagement rollout, that post-write call could select a
   public CTA activity and create a second public wall post for the same promo
   publication pass.

## Contributing Factors

- Existing tests only asserted that promo `vk_publication` invoked the old
  shadow hook after the plain post; they did not encode the final production
  one-write CTA/plain contract.
- VK remapped the stored postponed id for the plain exposure (`3368`) to the
  live wall id (`3370`), so DB review alone did not show the exact user-reported
  duplicate URL.

## Automation Contract

### Treat as regression guard when

- changing `promo.py::_build_promo_vk_source_post`;
- changing `afishaengagement.py::maybe_publish_shadow_debug_copy` public/shadow
  mode flags;
- changing promo `vk_publication` exposure accounting or VK postponed cleanup;
- changing Afisha Engagement public CTA rollout rates or target resolution.

### Affected surfaces

- `promo.py::_build_promo_vk_source_post`
- `afishaengagement.py::maybe_publish_shadow_debug_copy`
- VK postponed/live wall for group `-231920894`
- `promo_exposure` rows for `surface in ('vk_publication','afishaengagement')`
- Promo campaign reports and rolling-window counts

### Mandatory checks before closure or deploy

- `tests/test_promo.py::test_promo_vk_publication_uses_public_afishaengagement_as_primary_post`
- `tests/test_promo.py::test_promo_vk_publication_runs_afishaengagement_shadow`
- Regression checks from related CTA/plain records:
  - `tests/test_vk_source.py::test_sync_vk_source_post_uses_afishaengagement_preflight_for_new_public_post`
  - `tests/test_vk_source.py::test_sync_vk_source_post_keeps_plain_post_after_public_cta_miss`
  - `tests/test_vk_source.py::test_sync_vk_source_post_does_not_run_afishaengagement_shadow_on_update`
  - `tests/test_afishaengagement.py::test_public_engagement_copy_schedules_without_debug_marker`
- Authenticated VK API check that only the kept CTA post remains active for the
  reported duplicate pair.
- Production DB check that the plain duplicate exposure is not counted as a
  public success after manual deletion.

### Required evidence

- Targeted pytest output.
- VK API evidence for deleted plain duplicate and kept CTA post.
- Production DB evidence for `promo_exposure.id=359` status update.
- Deployed SHA reachable from `origin/main` before closure.

## Immediate Mitigation

- Deleted plain duplicate `https://vk.com/wall-231920894_3370` via VK API after
  verifying owner id `-231920894` and text hash match.
- Kept CTA post: postponed URL `https://vk.com/wall-231920894_3369`, resolved by
  VK to live `https://vk.com/wall-231920894_3372`.
- Updated production `promo_exposure.id=358` to the live CTA URL `3372`.
- Updated production `promo_exposure.id=359` to `VK_DELETED_DUPLICATE` and added
  incident metadata pointing to the kept live CTA URL.

## Corrective Actions

- `promo.py::_build_promo_vk_source_post` now runs Afisha Engagement public
  preflight before plain `post_to_vk`; when CTA succeeds, it returns that URL
  and does not create a plain wall post.
- After a plain fallback, the promo runner calls Afisha Engagement with
  `shadow_only=True`, so public CTA activities cannot create a second wall post
  as a post-write side effect.
- Added targeted regression coverage for public CTA as the primary promo VK
  publication and updated the legacy shadow test to require `shadow_only=True`.
- Updated canonical promo/Afisha Engagement docs.

## Follow-up Actions

- [ ] Deploy fix to production from clean worktree and record SHA/image.
- [ ] Re-check the next `promo_vk` run for CTA/plain one-write behavior.
- [ ] Consider adding a reconciliation job that resolves stored postponed ids to
  live ids before duplicate cleanup/reporting.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `python3 -m py_compile promo.py tests/test_promo.py`
  - `../events-bot-new-inc-20260614-garazhka/.venv/bin/python -m pytest tests/test_promo.py -q` → `45 passed`
  - `../events-bot-new-inc-20260614-garazhka/.venv/bin/python -m pytest tests/test_vk_source.py::test_sync_vk_source_post_uses_afishaengagement_preflight_for_new_public_post tests/test_vk_source.py::test_sync_vk_source_post_keeps_plain_post_after_public_cta_miss tests/test_vk_source.py::test_sync_vk_source_post_does_not_run_afishaengagement_shadow_on_update tests/test_afishaengagement.py::test_public_engagement_copy_schedules_without_debug_marker -q` → `4 passed`
- production mitigation evidence:
  - artifact `artifacts/codex/INC-2026-06-14-vk-cta-duplicate/cleanup_delete_3370.json` shows `wall.delete response=1` and `db_exposure_359_updated=true`;
  - artifact `artifacts/codex/INC-2026-06-14-vk-cta-duplicate/find_cta_live_after_publish.json` shows live CTA `wall-231920894_3372` with postponed id `3369`;
  - artifact `artifacts/codex/INC-2026-06-14-vk-cta-duplicate/update_plain_exposure_incident.json` shows `id=358` reconciled to `3372` and `id=359` as `VK_DELETED_DUPLICATE`.
- post-deploy verification: pending

## Prevention

Promo `vk_publication` and Smart Update VK sync now share the same publication
boundary invariant: public CTA or plain fallback is selected before the write,
and public CTA must never be layered as a second wall post after the plain VK
post already exists.
