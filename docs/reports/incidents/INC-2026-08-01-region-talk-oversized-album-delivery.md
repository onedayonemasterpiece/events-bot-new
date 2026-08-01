# INC-2026-08-01 Region Talk oversized album delivery

Status: closed
Severity: sev2
Service: Region Talk operator-review media delivery
Opened: 2026-08-01
Closed: 2026-08-01
Owners: events-bot / Region Talk
Related incidents: `INC-2026-08-01-region-talk-draft-backfill-nameerror`, `INC-2026-07-31-region-talk-candidate-chat-incomplete-drafts`
Related docs: `docs/features/region-talk-channel/telegram-vk-publishing.md`, `docs/features/region-talk-channel/editorial-visual-product.md`

## Summary

A current v8 social draft for `https://t.me/zorkjy/3147` reached real operator
delivery with a source-album locator. The source group contains ten photos,
while Region Talk's reviewed carousel contract is three to six. The notifier
downloaded all ten and aborted before `send_file`; it also wrote `sending`
before materialization, leaving an ambiguous-looking delivery ledger row even
though no Telegram send had started.

## User / Business Impact

- one otherwise ready social candidate did not reach the operator chat;
- the already reviewed three-frame ImageDiagnostic selection was not used;
- an automatic retry was fail-closed by the ambiguous `sending` guard;
- no malformed ten-photo Region Talk post was published.

## Detection

The compensating notifier invocation failed with
`RuntimeError: social_album must materialize 3..6 ordered items, got 10` before
the Telethon `send_file` call. YDB comparison then showed that ImageDiagnostic
had selected `telegram:3147`, `telegram:3156`, `telegram:3149`, but the
publication row had no `selected_media_ids` and retained only
`media_id=source:album`.

## Timeline

- 2026-08-01 15:10 UTC — notifier began delivery revision
  `f4de8fdd…` and persisted `status=sending`.
- 2026-08-01 15:10 UTC — materialization resolved ten source photos and failed
  the 3–6 contract before any Telegram send.
- 2026-08-01 15:14 UTC — production YDB audit found the three reviewed media
  IDs in the matching `image_queue_item` and confirmed no message ID on the
  failed delivery row.
- 2026-08-01 15:18 UTC — code correction and focused regression tests were in
  progress.
- 2026-08-01 15:27 UTC — exact `origin/main` SHA `59d5edd5` reached Fly as
  version `1850`; health and immutable image SHA checks passed.
- 2026-08-01 15:29 UTC — the publication manifest was repaired from the exact
  ImageDiagnostic selection and the proven pre-send ledger row was reconciled.
- 2026-08-01 15:30 UTC — Telethon delivered the corrected native three-photo
  album as messages `33795`–`33797`.
- 2026-08-01 15:31 UTC — direct Telegram inspection confirmed one grouped
  album, caption/link contract and exact selected-frame order.

## Root Cause

1. Draft backfill joined current `image_queue_item` presentation evidence only
   for article rows, so social candidates lost their reviewed ordered
   `selected_media_ids` at the publication boundary.
2. A `source:album` locator materialized the complete Telegram group but did
   not apply the product maximum before validating its length.
3. The notifier persisted `sending` before local media materialization even
   though no external send had started.

## Contributing Factors

- the earlier source-album regression covered valid albums of at most six
  frames but had no ten-frame Telegram group control;
- ImageDiagnostic and publication drafts deliberately use separate durable
  rows, making the missing social evidence join non-obvious.

## Automation Contract

### Treat as regression guard when

- changing Region Talk image-ledger joins, social-album planning, Telegram
  grouped-media materialization or Telethon delivery state transitions.

### Affected surfaces

- `scripts/region_talk_publication_draft_backfill.py`;
- `scripts/region_talk_goal_notify.py`;
- YDB `image_queue_item`, `publication_candidate_item` and
  `publication_delivery_item`;
- role-scoped DISCOVERY2 Telethon delivery to the operator chat.

### Mandatory checks before closure or deploy

- prove a reviewed non-sequential social selection is preserved in manifest
  order;
- prove a ten-frame source-album locator is deterministically capped at six in
  original order when no reviewed selection exists;
- prove a pre-send materialization failure writes no `sending` state;
- run the focused notifier/backfill tests and full Region Talk suite;
- deploy an exact clean `origin/main` SHA and verify Fly health/image SHA;
- reconcile the known pre-send ledger row and deliver the corrected Zorkjy
  revision as a native 3–6-photo album with an actual Telegram message ID.

### Required evidence

- production YDB before/after rows for the candidate, image selection and
  delivery state;
- focused/full test output;
- deployed SHA reachable from `origin/main`, Fly version and health;
- exact operator message ID and observed native album item count/order.

## Immediate Mitigation

Delivery failed closed before Telegram transmission. The ambiguous row will be
reconciled only from the proven pre-send exception, not treated as a generic
safe retry.

## Corrective Actions

- join missing social media-selection evidence from the latest exact-URL image
  row, without overwriting a newer explicit selection;
- cap a source-album-only materialization at the first six frames in source
  order;
- materialize and validate media before writing the external-send `sending`
  state.

## Follow-up Actions

- [x] deploy and complete the compensating operator delivery;
- [x] verify the autonomous backfill joins reviewed social selections in the
  focused real-boundary regression and repairs the affected durable row.
- [ ] observe that a newly generated autonomous social draft persists the
  reviewed selection without manual repair; this is a monitoring check, not a
  closure blocker because the exact production boundary has been replayed.

## Release And Closure Evidence

- deployed SHA: `59d5edd520bdd75f097832f5017ad6ea695b172d`, reachable
  from `origin/main`, Fly version `1850`
- deploy path: clean exact-`origin/main` `scripts/deploy_fly_main.sh`
- regression checks: focused notifier/backfill tests `48 passed`; full Region
  Talk suite `694 passed`; `/healthz ok=true, ready=true`; machine image SHA
  exactly `59d5edd5…`
- post-deploy verification: `https://t.me/zorkjy/3147` was delivered as native
  Telegram album messages `33795`–`33797`, all sharing grouped ID
  `14284785448176994`. Direct download comparison matched source IDs
  `3147 → 3156 → 3149` in that order (each dHash distance `0` to its expected
  source and much larger to the alternatives). The visible caption is 620
  characters with exactly two paragraph breaks and linked source/original
  entities. The old `sending` row is durably marked
  `materialization_failed_pre_send` with no message ID.

## Prevention

The regression set now covers both preferred reviewed selections and the
bounded source-order fallback, plus the delivery state boundary before any
external Telegram side effect.
