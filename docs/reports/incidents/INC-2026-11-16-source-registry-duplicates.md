# INC-2026-11-16 Duplicate source registry identities

Status: resolved
Severity: sev3
Service: Events Bot source registries
Opened: 2026-11-16
Closed: 2026-11-16
Related registry: inc_57638126a2445b9d11e88b69

## Impact and detection

The owner requires no repeated source within either event or excursion monitoring.
Read-only production inspection found event registry pairs 5028/5120 (Поэты 39)
and 117/5126 (Старый Люнет). Each pair is the same community with opposite signs
in `group_id`. Excursion registry pairs 17/111, 18/181, 20/97 and 30/83 are
vanity/numeric handles of the same exact VK wall. Different platforms and a
person's separate community remain separate sources.

## Root cause

The original saturation registrar accepted signed community IDs; its exact-ID
normalization was subsequently corrected, but the existing duplicate rows remained.
The event unique index compared the signed column. Guide uniqueness compared
`platform, username`, allowing two handles with the same cached owner ID.
Six original guide seed entries had no durable owner ID. Their actual provider
identities were read on November 16; the seed now includes those existing metadata
fields. No semantic identity or text decision is changed.

## Corrective change and regression contract

- Add unique event owner identity `(owner_type, abs(group_id))`.
- Add unique resolved guide VK identity `(personal/community, abs(flags.vk_id))`.
- Keep existing source metadata, seed merge behavior, queues and scans.
- Original initialized database controls accept both defects (two failing tests).
  Changed initialized controls reject both duplicates while allowing other walls
  and the distinct personal/community owner with the same numeric ID.
- Source, seed and import transaction tests: 35 passed locally. The first broader
  collection failed because local Telethon was absent; installed it only in the
  retained task dependency directory, then ran the same suite successfully.

Treat this as a regression guard when changing source insertion, metadata seeding,
owner signs, aliases or database initialization. No new state, retry, worker,
model call or crawling mechanism was added. Net production change: +12 lines,
within the 20-line budget.

## Data reconciliation and live acceptance

Before source-record removal, retain affected rows and rebind all post/occurrence/
fact references. Current overlapping guide posts have equal source URLs, text and
raw extraction facts. Keep canonical IDs, accepted events and public references.
The active guide run has both aliases in its frozen input; the current importer
looks up username exactly. Do not remove its source binding until that original
run is materialized. This is a specific saved-input dependency, not a blanket
idle-system release gate. Profile 156→7 is a separate, previously rejected action
and is not part of this repair.

November 16 production reconciliation completed: event pairs 5120→5028 and
5126→117 at 09:58 UTC; guide pairs 111→17, 181→18, 97→20 and 83→30 at
10:55 UTC, after original FULL run 8a5351821148 materialized through operation
12375 at 10:44:49. No source scan or kernel was replayed. Nineteen duplicate post
records were consolidated with occurrence and fact references rebound; all 318
profiles, 542 templates, 782 occurrences and 27,919 facts were preserved at commit.
Two posts acquired different raw extraction outputs in the completed run. Both
complete variants were retained in the existing raw-facts list; no semantic choice
or public tour field was changed. Independent affected-row comparison verified
entity fields, source text/URLs, raw outputs and occurrence links.

The DB-only prevention was delivered at 11:02:27 UTC from merged main
`a6e7679cf43219b1561ee642f16ae1b427f5a6ac` after all four latest-head required
CI checks passed. Both exact index literals were applied in one transaction.
Real same-owner attempts were rejected by the respective unique indexes inside
rolled-back SAVEPOINTs; no synthetic rows remained. Health was ready, DB healthy,
settings and Kotopogoda unchanged. No application build or restart was needed.

Ordinary post-delivery acceptance at 12:29 UTC:
- Three real event sources registered through the existing saturation workflow:
  5285 (personal wall146294125), 5286 (community214375155) and5290
  (personal wall168798170). Both registries still have zero duplicate resolved
  VK-owner groups. No new guide registry row was added during this interval.
- Scheduled LIGHT run495d0491c019 started at11:20, survived an independent
  container update through existing recovery, and materialized as operation12385
  SUCCESS at12:15:27:192 sources,173 posts,51 successful parses,zero model errors.
- All four retained canonical handles were scanned successfully. All25 occurrence
  changes have exact source-post/occurrence links and matching stored dates.
  This is an ordinary source-path result, not a replay of the repair or test data.

The guard covers event owner/sign identities and guide VK owners with resolved
IDs. Different platforms and personal/community owners remain distinct. It does
not infer an unresolved vanity handle's owner or merge people or tour semantics.
Independent source registration and future import monitoring remain normal
operations; the source-duplicate incident's repair and prevention are complete.

Receipts: `guide-pairs-receipt.json`, `guide-preservation-verification.json`,
`unique-indexes-receipt.json`, `normal-process-verification.json` in the retained
task directory below. Isolated fixture caveats remain: a partial FK-disabled
excerpt was used for local repair contrasts, not full semantic import acceptance.
Actual repair enabled foreign keys and actual ordinary source links were checked.

Retained evidence:
`/home/dev/artifacts/events-bot-new/20261116T093430Z-source-registry-dedup-20261116/`
contains the read-only inventory, provider identity reads, baseline/after tests and
restricted affected-row snapshot. No credentials are stored there.
