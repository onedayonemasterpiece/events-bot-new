# INC-2026-10-18 Guide digest link complaint

Status: open
Severity: sev2 (reported impact; newspaper destination not reproduced)
Service: guide excursions visual digest / Telegram
Opened: 2026-10-18
Closed: —
Owners: events-bot operations
Registry: `inc_0c91cbdac1e0be9f567527a0`
Work: `work_5d0bdbd6ff20b4ffa9249ad8`
Related incidents: `INC-2026-07-05-guide-visual-digest-stale-vk-booking-link`, `INC-2026-07-05-guide-visual-digest-phone-link`
Related docs: `docs/features/guide-excursions-monitoring/README.md`, `docs/operations/telegram-link-inspection.md`, `docs/operations/incident-management.md`

## Summary

The owner forwarded a guide's complaint that the first three links in
https://t.me/youwillsee39/485 lead to the newspaper «ЖИР». Exact Telegram
entities and production DB were inspected. The reported newspaper destination
has **not** been reproduced: all three URLs currently resolve to the guide's
personal Telegram profile. Do not describe this as a fixed or disproved report.

## User / Business Impact

- Reported: readers are sent to a newspaper instead of excursion details or booking.
- Observed: visual issue `299` uses a personal booking profile for items `671`,
  `672`, `673`, a guide channel for `674`, and a source post for `677`.
- Both live copies have identical first three URLs:
  https://t.me/youwillsee39/485 and https://t.me/wheretogo39/466.
- Previous issue `298` also contains the personal booking URL; DB receipts map
  its text parts to `@youwillsee39/483`, `/484` and `@wheretogo39/464`, `/465`.
  Those older public messages were not independently read in this investigation.
- Future occurrences `671`, `672`, `673`, `676` currently retain this booking URL.
  This is potential scope if the contact is rejected, not evidence of four bad URLs.

## Detection and Timeline

- Owner supplied a complaint quoted at `18.10.2026 10:36`; its timezone and
  exact relationship to the linked message remain unverified.
- `2026-10-18 08:30:17 UTC` (`10:30:17 Europe/Kaliningrad`): Telegram date of
  message `485`; `edit_date=null` at inspection.
- `2026-10-18 09:53–09:57 UTC`: read-only Telegram, production SQLite,
  runtime mirror and public-web probes; retained evidence captured.

## Root Cause

Newspaper routing: **unconfirmed**. Evidence establishes the current link chain:

1. Source https://t.me/katimartihobby/2213 explicitly says
   `Запись в личные сообщения @KaterinaMartiHobby`; its live entity is a mention.
2. Production `guide_source.id=5.about_text` also advertises this booking contact.
3. Occurrence booking URLs and issue `299` store `https://t.me/KaterinaMartiHobby`.
4. `guide_excursions/visual_digest.py::_primary_link` prioritizes `booking_url`.
5. Telegram resolves that username to user `1151505376`, named
   `Катя Марти | Прогулки без повода Гунашвили | экскурсовод | Калининград`.
   Its bio points to `@katimartihobby`; no newspaper link was observed there.
6. A supplementary HTTP probe returns status 200 without redirect and the same
   guide's profile title. This supports current state, not historical client state.

No malformed-URL, transport corruption or unsupported model-generated username
has been established. Do not add a username blacklist or change extraction
semantics on this evidence. If later evidence establishes a semantic extraction
error, apply the repository's LLM-first gate and production-boundary replay.

## Automation Contract

### Treat as regression guard when

- Changing visual digest primary-link priority, booking contact grounding,
  Telegram entity handling or caption repair.

### Affected surfaces

- `guide_excursions/visual_digest.py::_primary_link` and Telegram caption builder.
- `guide_occurrence`, `guide_source`, `guide_monitor_post`, `guide_digest_issue`.
- Current issue `299` Telegram photo captions; potentially issue `298` booking CTAs.

### Mandatory checks before closure or deploy

- Obtain the actual newspaper destination URL or enough original client/post
  evidence to explain the mismatch with exact current Telegram entities.
- Agree on intended destination: booking profile versus source announcement.
- For any approved public edit, preserve text, photo, message ID, footer and
  unrelated URLs; re-read exact entities from both live sibling messages.
- If changing ingestion semantics, replay the source through the applicable
  production guide import path with a legitimate-contact negative control.
- If changing renderer behavior, verify explicit valid booking URLs and phone
  fallback as well as this case; follow normal release governance.

### Required evidence

- Exact before/after Telegram entities; production provenance and repair receipt.
- Explanation of the newspaper observation, or an explicit unresolved limitation.
- Deployed SHA and regression results only if code is changed and deployed.

## Immediate Mitigation / Corrective Actions

No production data, public messages or product behavior changed. No new inference
was needed to read exact URLs. The service container was healthy at inspection.

Two **unpublished** caption proposals replace only the first three title links
with the source announcement `https://t.me/katimartihobby/2213`. Validation compares
against live text/entities: three links changed per message, visible text and all
other links unchanged. This is a reviewable mitigation option, not an approved
new global link-selection policy or completed repair.

## Evidence and Release State

Retained evidence directory:
`/home/dev/artifacts/events-bot-new/20261018T095314Z-digest-wrong-links-20261018/`.

- `telegram-entities-485.json`, `telegram-entities-466.json`: exact live captions.
- `telegram-source-2213.json`, `profile-evidence.json`: original announcement,
  mention and current profile identity.
- `production-evidence.json`, `source-evidence.json`: bounded read-only DB extracts.
- `runtime-log-excerpt.json`, `live-release-health.json`: runtime evidence.
- `public-web-link-check.json`: supplementary public HTTP evidence.
- `proposed-caption-*.html`, `proposal-validation.json`: unpublished repair candidates.

Production observed: RunCoveer `deployment-events-1`, image tag
`runcoveer/events:scheduler-recovery-20261014`; no deployment performed here.
No unit suite was run for this documentation-only investigation. Exact live
entity/provenance checks and offline caption proposal checks passed.

## Follow-up Actions

- [ ] Clarify the actual newspaper URL/client observation or intended post.
- [ ] Resolve intended link destination with the owner; apply and verify any
  separately authorized public repair, including affected siblings.
- [ ] Decide whether a global product change is needed after root cause is known.
- [ ] Keep the incident open until live impact and the mismatch are resolved.
