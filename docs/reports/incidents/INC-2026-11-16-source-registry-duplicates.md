# INC-2026-11-16 Duplicate source registry identities

Status: open
Severity: sev3
Service: Events Bot source registries
Opened: 2026-11-16
Closed: —
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
model call or crawling mechanism was added. Net production change: +18 lines,
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

At this commit no production record has been merged and this source constraint
change has not been delivered. Required completion: exact latest-head CI, original
run materialization, lossless reconciliation, zero duplicate identities, fresh
same-owner rejection, exact-main delivery, health and normal import acceptance.

Retained evidence:
`/home/dev/artifacts/events-bot-new/20261116T093430Z-source-registry-dedup-20261116/`
contains the read-only inventory, provider identity reads, baseline/after tests and
restricted affected-row snapshot. No credentials are stored there.
