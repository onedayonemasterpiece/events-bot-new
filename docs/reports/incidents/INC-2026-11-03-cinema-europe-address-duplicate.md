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
- [x] Correct and inspect daily announcement, calendar, Telegraph and duplicate VK projection.
- [x] Reference regression fails before and passes after, with negative control; 18 related tests pass.
- [x] Real normal Telegram importer and Smart Update shadow replay passes with source receipt; different-slot and different-cinema controls reject merge.
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

## Full boundary replay found missing reference context

PR #772 merged as 167dd9d572033ea7ebe938e9847f2726e0c3ac7e; CI, 18 related
tests, full 4,429-file image verification and isolated startup passed. It is
NOT deployed. A fresh shadow DB seeded with before-repair canonical 10242 and
its original sources ran the actual Telegram results importer and Smart Update,
with real LLM calls and unchanged semantic gates. Only external projections were
deferred. Candidate normalization correctly used Гвардейский 3, but the merge
LLM still read Театральная 30 from raw source text and returned unsafe_to_merge.
Result: no duplicate, but FINAL_RETRY and no source receipt. This is not passing
replay or complete prevention. Evidence: retained cinema-shadow-replay.log.

The identity payload omitted the authority behind normalized location fields.
Pass the existing exact curated alias reference as separate LLM evidence and
explain source typo resolution in that same prompt. No automatic merge, conflict
exception, new model call or state. Different slots and branches stay distinct.
Repeat the same full boundary replay and negative controls before delivery.

## Final candidate qualification and public readback

Follow-up PR #773 merged as d61a1f6af2c6442dd56c6e9bf0958ce8fd15c560 after
all required CI passed. A real fresh shadow Telegram import through Smart Update
now returns one MERGED into 10242, zero CREATED, and a source receipt binding
meowafisha/9050 to 10242. Official address is preserved; wrong address is absent
from public description/digest. No semantic decisions were mocked; external
projections were deferred and the shadow had no outbox worker. Two real LLM
negative controls reject a different explicit screening time and another cinema.
The original raw source remains unchanged.

Final image intake-cinema-identity-20261103 is qualified as immutable
sha256:225f27bbc0562ba595582c58d2cd7bb42f1970f82d1d29e11d50d9648bf4d657.
Full 4,429-file verification, isolated HTTP/SQLite preflight and effective venue
reference checks pass. Its identity function matches the exact function used
in the real replay and controls. Thirty-six related tests and provider/prompt
audits pass. Corrected baseline fails on missing curated reference; the first
baseline invocation lacked an asyncio marker and is not valid failure evidence.

Public readback confirms daily post 5453 has one screening, calendar 10236 is
absent, and the obsolete Telegraph page links to canonical. Managed duplicate
VK post 13090 was checked against both live posts and DB ownership, deleted
through normal app actor routing, and read back as is_deleted=true. Canonical
VK post 12989 remains. Group-only delete first rejected code 27 before effect;
normal existing actor routing succeeded. Other publication-ledger URLs were
not used blindly as deletion targets.

Candidate is NOT DEPLOYED yet: production remains fd4da1b8, with healthy source
parser 11581 active at 12:32:30. Preserve it and deliver the qualified image in a
safe interval. Existing native wake at 12:44:30 covers delivery and continued
intake verification. Original ordinary VK 13:30, phase-three near 14:30 and
ordinary TG 21:40 remain required. Both incidents stay open.


## Combined release delivered, 2026-11-03 13:58 UTC

Actual RunCoveer Events runtime is now `7d03b15853d3548299aa334418a9a907ae415312`,
image `intake-guide-promo-spacing-20261103`, immutable
`sha256:96896a2b00717740524948c4b49caedc4b6f2a96e6dba4bf23c5482789ade566`.
PRs #777 and #778 passed all required CI before merge. Source parser completed
261/261 before cutover; the final guard found no active ops. Events alone was
recreated. Kotopogoda container ID remained unchanged. Explicit runtime/model
settings checks, SQLite quick_check, effective cinema reference and identity
function hash passed. Live health reports ready=true, database/scheduler ok,
no issues. The existing promo interval is five minutes.

Evidence is retained in the intake recovery directory under
`promo-spacing-cutover.log`, `promo-spacing-live-verification.log`,
`promo-spacing-live-health.json` and `promo-spacing-startup.log`.
This supersedes earlier not-deployed candidate status; it does not close the
incident or substitute for actual scheduled source receipts.


## Preserved through receipt-codec release, 2026-11-03 16:24 UTC

Actual runtime215c6a219edec24551e2d38215cda8ccc396091c, immutable
sha256:fa97c6e09aa6634ceda9575569abd9a6568802f7ab73e7bdaf3d1b40a0f03a72.
PR783 passed all required CI; exact source and isolated initialized preflight
passed before safe Events-only cutover. Live model settings, cinema reference
and qualified identity/adjudicator hashes are unchanged; promo interval5,
readiness/database/scheduler ok and Kotopogoda unchanged. Existing public
receipts remain authoritative. Future normal promo timing on November4/6 and
normal scheduled imports still need acceptance; incident remains open.
