# Current UI Decoder v1 — final integration report

## Verdict

`GO_FOR_FAMILY_SCOPED_DEFRAGMENTATION`

This is an evidence-readiness verdict only. It does not approve component
equivalence, merge/split, style normalization, token creation, Penpot
materialization, or Astro/CSS/runtime changes.

## Delivered evidence

| Area | Result |
|---|---|
| Exact source union | 107 logical component paths: 106 candidate + public-root-only `PrelaunchPage` |
| Classification | 107/107 closed dispositions; 0 needs-verification |
| Candidate contracts | 12 `candidate-as-is-not-accepted` |
| Capsules | 6 source → specimen → page reconciliations, manually reviewed |
| Browser evidence | 46 page + 109 component rasters |
| Human visual inspection | 155/155 reviewed and SHA-bound |
| Heavy evidence | Permanent GitHub Release asset, SHA-256 bound |
| Compact handoff | 203 files / 201 indexed outputs in `lovekgd-design-system` |
| Blocking unresolved | 0 for decoder handoff |
| Normalization/Penpot/UI mutation | 0 |

## Visually confirmed AS-IS formats

- Event Detail desktop editorial/landscape composition with side/stacked CTA;
- Event Detail desktop split/portrait-poster composition with inline CTA;
- independent mobile event composition;
- large poster/primary companion versus smaller remaining-photo previews;
- rail explicit-return and cutoff/last-train states, controlled forecast state;
- bus schedule and KAUP compact/desktop states;
- medallion top/inline, zero/one/multiple, main/secondary and badge/pill states;
- two separate collectible systems: Amber research collectible and Focus Egg
  prototype, explicitly not merged;
- Exhibitions, For Me, Search, Favorites, Clubs, Festivals, Popular, Day and
  Weekend page families.

The requested exact event IDs `7052`, `7301`, `7048` and `7186` were not
present in the exact candidate manifest and are recorded as
`explicit-unreachable`. Equivalent **formats**, not those exact IDs, were
captured on other hash-bound runtime representatives.

## Colors, typography and fragmentation boundary

Raw source/computed color and typography evidence remains in the graph. It is
not a normalized palette or type system. The earlier full graph recorded 34,985
style observations and 809 divergence candidates; these are review inputs, not
809 confirmed conflicts. The component graph likewise preserves fragmentation
candidates as `NOT_MERGED / unresolved`.

## Immutable chain

- capture: [events-bot-new Actions 31291052330](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31291052330);
- heavy evidence: [GitHub Release](https://github.com/onedayonemasterpiece/events-bot-new/releases/tag/current-ui-decoder-v1-snapshot-20260808T124842-4786ac53bc),
  SHA-256 `abbbb5bbe6cd472c3814a9586fa51375a708fc5cfe0319e3b9cfcd0cc4b2d279`;
- compact import: [lovekgd-design-system PR #26](https://github.com/onedayonemasterpiece/lovekgd-design-system/pull/26);
- design-system merge: [`1ca65d165d01c10c9db323fd2c8ebf20f8f8b7ec`](https://github.com/onedayonemasterpiece/lovekgd-design-system/commit/1ca65d165d01c10c9db323fd2c8ebf20f8f8b7ec);
- merged-main validation:
  [snapshot](https://github.com/onedayonemasterpiece/lovekgd-design-system/actions/runs/31292505954) and
  [contracts](https://github.com/onedayonemasterpiece/lovekgd-design-system/actions/runs/31292506001), both PASS.

Machine-readable details: [`HANDOFF_RECEIPT.json`](HANDOFF_RECEIPT.json).
Receipt SHA-256:
`32a040ffb98b29158a0f6b7a4101b799444a67bc7c50821cd37d5a44963623d1`.

## Integrated implementation lanes

| Lane | Worker commits | Outcome |
|---|---|---|
| Core AST/classification/snapshot | `b5fe8c37d` | integrated |
| Transport | `52664315f`, `842028314` | integrated |
| Medallions | `67a38732b` | integrated |
| Artifacts | `3a6135f7c` | integrated |
| Candidate contracts/capsules | `8457a3c08` | integrated |
| Specimen harness/real routes | `c7f2bac21`, `96831bec8` | integrated |
| Capture/review/handoff integration | `ad2fb5a9c` | merged to `origin/main` |

No worker patch remains required for this decoder handoff.
