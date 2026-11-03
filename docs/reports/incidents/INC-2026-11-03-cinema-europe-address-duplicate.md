# INC-2026-11-03 Cinema Europe duplicate screening

Status: investigating
Severity: sev2
Opened: 2026-11-03
Service: Smart Update venue grounding, identity, daily announcement
Registry: inc_179ed0abbed414458fc3f97e
Related: INC-2026-11-02-intake-quota-vk-media; INC-2026-08-22-sos-dedup-veto-location-tyunin-farm; INC-2026-08-22-tyunin-farm-location-drift

## Owner request and scope

The owner supplied a screenshot of two «Плетёный муж» entries on November 3 at
19:30 in Синема Парк Европа, one at Гвардейский проспект 3 and one at
Театральная 30. They requested incident handling in addition to continuing
scheduled ingestion verification. That existing work and its wake remain active.

## Evidence and root cause

Production has two active canonical rows: 10242 and 10295. Read-only Telethon
inspection confirms source https://t.me/kenigevents/5399 gives Гвардейский 3,
while https://t.me/meowafisha/9050 gives Театральная 30. Both name the same
cinema, film, date and time. The latter message links to the same official
film page through a hidden entity in the persisted candidate payload.

Official https://kinoteatr.ru/film/pletenyy-muzh-2/kaliningrad/?date=2026-11-03&openWidget=16775068
confirms the November 3, 19:30 screening at Синема Парк Европа, Гвардейский
проспект 3. The cinema page confirms that address too:
https://kinoteatr.ru/raspisanie-kinoteatrov/kaliningrad/evropa/ .

Candidate state 25553 recalled owner 10242, but identity decisions 18640/18641
classified it related_but_distinct / FINAL_DISTINCT with reason
location_address_conflict. This allowed creation of 10295. This is a semantic
venue-grounding error; it is not a recall miss or provider quota error.
The source contains the incorrect address; the LLM did not invent that string.
The maintained venue/alias reference has no Cinema Europe row yet.

## Remediation plan

Keep canonical 10242, which already has the official address, ticket URL and
public projections. Use the existing guarded manifest repair tool to mark
10295 merged and move source, poster and accepted-candidate ownership without
losing source text. Its before-state backup, graph/row checks and durable receipt
must pass. Verify final canonical count and references, then correct the public
daily entry and obsolete projections using existing product paths.

For prevention, add the authoritative venue and its exact name alias to the
existing reference data. Existing shared, parser and Smart Update normalization
already trust curated aliases. No new regex, merge exception, worker or retry
path. A distinguishing test uses the actual conflicting address and a different
cinema as negative control. LLM identity and conflict guards stay unchanged.

## Required verification and closure

- [x] Read both actual Telegram sources and official programme/address.
- [x] Identify candidate and decisions responsible for duplicate creation.
- [x] Guarded repair dry-run, apply, verify and second-apply noop receipt.
- [x] Verify source ownership, poster graph, candidate ledger and one canonical event.
- [ ] Correct and inspect the owner-visible daily announcement/projections.
- [x] Reference regression fails before and passes after, with negative control; 18 related tests pass.
- [ ] Replay through normal import and Smart Update boundary; unit normalization alone is insufficient for closure.
- [ ] Merge and deliver qualified main reference change, preserving healthy imports.
- [ ] Continue original intake checks at 12:44, around 14:30 and normal TG 21:40.

Evidence is retained in the existing intake task directory under
/home/dev/artifacts/events-bot-new/20261102T233947Z-intake-recovery-20261103/:
cinema-sources.json, cinema-evidence.json and cinema-manifest.json.
No production repair or prevention deployment is claimed yet.

## Mitigation evidence, 12:10 UTC

Guarded manifest 6aae1e879814fbb3f87678297748e31e221cf6bf44fbae2e0f80e2e1870f5dd6
passed dry-run, apply and verify. Source 13774018 and three posters moved to
10242; candidate 25553 now accepts 10242, one obsolete job cancelled, and
10295 is marked merged/cancelled without deleting source evidence.
The existing August repair implementation is reused with this incident label;
its durable backup/receipt tables preserve rollback state. The manifest records
the running SHA as provenance, not as proof of the new prevention being deployed.
Full before-state remains in the retained task artifacts and production receipt.

Public daily correction targets existing message https://t.me/kenigevents/5453,
removing only the duplicate second entry. Exact fresh post matched the reviewed
before text; UTF-16 entities outside the removed block are retained/rebased.
Public readback and obsolete projection cleanup remain to verify.

Daily message 5453 is corrected and read back through Telethon: one screening
entry remains, and Театральная 30 is absent. The obsolete calendar document
10236 was read before deletion and removed via the existing ICS invalidation
function; its ledger fields were cleared. Obsolete Telegraph page
https://telegra.ph/Pletyonyj-muzh-11-01 now points to the canonical page with
the official address. Canonical public rows are retained. The cleanup emitted
an existing source-keyboard validation warning; it did not block calendar
removal or Telegraph correction. Direct public readback of those two cleanup
results and the obsolete VK projection still needs verification.
