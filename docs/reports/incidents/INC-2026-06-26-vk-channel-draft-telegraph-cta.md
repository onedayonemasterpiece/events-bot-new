# INC-2026-06-26 VK Channel manual draft used Telegraph instead of registration CTA

Status: mitigated
Severity: sev3
Service: promo campaigns / VK Channel manual drafts
Opened: 2026-06-26
Closed: —
Owners: events-bot operations
Related incidents: `INC-2026-06-25-vk-channel-wrong-surface`
Related docs: `docs/features/promo-campaigns/README.md`, `docs/features/vk-publishing/README.md`, `docs/operations/incident-management.md`, `docs/operations/release-governance.md`

## Summary

The operator-assisted `vk_channel_publish` draft for the `80 историй о главном`
campaign selected event `4417` and sent a ready-to-copy VK Messenger/Favorites
message with its Telegraph details page as the only CTA. The event source and
campaign require a direct registration link, so a Telegraph page was the wrong
link for manual VK Channel publication.

## User / Business Impact

- The operator saw a draft in VK Favorites that could be copied into the VK
  Channel with the wrong CTA.
- The promoted festival registration path was one click farther away than
  intended.
- Campaign evidence row `promo_exposure.id=541` did not record the intended CTA
  URL until manual repair.

## Detection

The user reported on 2026-06-26 that the Favorites draft for the festival promo
contained a Telegraph link where the registration link should have been.
Production DB inspection identified `promo_exposure.id=541`, event `4417`, and
VK message `https://vk.com/im?sel=868977531&msgid=287` as the affected record.

## Timeline

- 2026-06-26 13:27:56 UTC: `vk_channel_publish` sent manual draft message
  `msgid=287` for event `4417`.
- 2026-06-26 13:28:35 UTC: production recorded `promo_exposure.id=541` as
  non-public `VK_CHANNEL_DRAFT_SENT` with target URL `vk.com/im?...`.
- 2026-06-26 21:04 UTC: user report was investigated; runtime file mirror was
  present and production DB showed event `4417.ticket_link` was empty while
  `telegraph_url` was populated.
- 2026-06-26 21:08 UTC: VK message `msgid=287` was edited in place to use the
  registration URL, event `4417.ticket_link` was repaired, and exposure `541`
  details were updated with `cta_url` and repair metadata.

## Root Cause

1. `_single_event_link()` correctly preferred `ticket_link` when present, but
   still fell back to `telegraph_url` when the event's direct registration link
   was missing.
2. The `80 историй о главном` manual draft contract did not fail closed for
   registration-required events without a direct CTA.
3. The earlier regression test covered "ticket_link beats Telegraph" but not
   "Telegraph must not be used when registration is required and ticket_link is
   missing".

## Contributing Factors

- The canonical event row had no `ticket_link` even though the source text said
  "Бесплатно, по регистрации".
- Source text from the imported Telegram post did not include the button URL, so
  the draft builder could not recover the registration link from local event
  text.
- The runner attempted only the selected candidate and had no special handling
  for missing direct CTA versus transport failures.

## Automation Contract

### Treat as regression guard when

- changing `main_part2.py` compact VK Channel draft copy/link selection;
- changing `promo.py` `vk_channel_publish` candidate loop or exposure statuses;
- changing `80 историй о главном` campaign seeding or event registration-link
  enrichment.

### Affected surfaces

- `main_part2.py`: `_single_event_link()`, `build_vk_channel_promo_event_publication_message()`,
  and manual draft send helper;
- `promo.py`: `vk_channel_publish` runner branch;
- production DB: `event.ticket_link`, `promo_exposure.details_json`;
- VK Messenger/Favorites manual-copy draft.

### Mandatory checks before closure or deploy

- Test that a direct registration/ticket URL beats Telegraph in VK Channel draft
  copy.
- Test that a `80 историй о главном`/registration-required draft with only a
  Telegraph URL raises/skips instead of sending Telegraph as CTA.
- Test that a registration URL embedded in source text can be used as the direct
  CTA.
- Test that the manual draft still attaches an event poster when media exists.
- Production verification for the concrete exposure/message: corrected text has
  the `kgd80.ru/.../?register=1` URL, event `4417.ticket_link` is populated, and
  exposure `541.details_json.cta_url` matches.
- Release evidence: deployed SHA and confirmation that the SHA is reachable from
  `origin/main`.

### Required evidence

- Targeted pytest output for the VK Channel manual draft tests.
- Production SQL / VK API verification for event `4417`, exposure `541`, and
  message `msgid=287`.
- Deployed SHA / PR or merge commit.

## Immediate Mitigation

- Edited VK Favorites message `msgid=287` in place so the final line is
  `https://kgd80.ru/sobytiya/kaliningradskiy-morskoy-torgovyy-port-yarkie-stranitsy-sovetskoy-istorii-i-sovremennost/?register=1`.
- Updated production `event.id=4417.ticket_link` to the same registration URL
  and set `ticket_trust_level='operator_verified'`.
- Updated `promo_exposure.id=541.details_json` with `cta_url`,
  `previous_cta_url`, repair timestamp, and VK edit metadata.

## Corrective Actions

- VK Channel manual draft copy now treats `80 историй о главном` and any event
  text mentioning registration/tickets as requiring a direct CTA.
- The draft builder now refuses Telegraph fallback for registration-required
  events without a direct registration/ticket URL.
- Source-text URL extraction can use a registration/ticket URL embedded near
  registration wording before considering details-page fallback.
- The promo runner continues to the next candidate after a missing-direct-CTA
  skip instead of blocking the daily draft slot on a bad candidate.

## Follow-up Actions

- [ ] Audit remaining future `80 историй о главном` events with empty
  `ticket_link` and source text mentioning registration, then enrich high-confidence
  registration URLs before the next drafts.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks:
  - `python -m pytest -q tests/test_promo.py::test_promo_runner_sends_vk_channel_manual_draft_nonpublic tests/test_promo.py::test_vk_channel_manual_draft_prefers_registration_link_over_telegraph tests/test_promo.py::test_vk_channel_manual_draft_refuses_80_stories_telegraph_fallback tests/test_promo.py::test_vk_channel_manual_draft_extracts_registration_link_from_source_text tests/test_promo.py::test_vk_channel_manual_draft_sends_poster_attachment` — passed locally (`5 passed`).
- production mitigation evidence:
  - `event.id=4417.ticket_link` = `https://kgd80.ru/sobytiya/kaliningradskiy-morskoy-torgovyy-port-yarkie-stranitsy-sovetskoy-istorii-i-sovremennost/?register=1`.
  - `promo_exposure.id=541.details_json.cta_url` equals the same URL.
  - VK API `messages.getById(message_ids=287)` returned text and link attachment with the same registration URL.

## Prevention

Registration-required manual drafts now fail closed on missing direct CTA and
are covered by explicit regression tests. Telegraph can remain a details-page
fallback only for events that do not indicate a registration/ticket requirement.
