# Participant portrait canaries

Source portraits copied on 2026-07-27 from the internal KGD80 project:

- `site/src/assets/participants/source/udovenko-tatyana.original.webp` ←
  `/home/dev/projects/kdg80/site/public/generated/speakers/udovenko-tatyana.webp`,
  SHA-256 `fd55f47f6df05067f82b55b37eaafea6c014e0950cf2cb7148a4fd855256534d`,
  840×916 RGBA.
- `site/src/assets/participants/source/levchenkov-andrey.original.webp` ←
  `/home/dev/projects/kdg80/site/public/generated/speakers/levchenkov-andrey.webp`,
  SHA-256 `8f99f9f184fbbe848354da30770ef0f02b8b875052d39f11c652bf2af322115a`,
  840×916 RGBA.

The first UI canaries used deterministic 512×512 lossless WebP crops made with
Pillow LANCZOS:

- `udovenko-tatyana.webp`: source crop `(340, 230, 680, 570)`, SHA-256
  `5839122c79445d8adfcbb65519be1eb9f682384b87f32c7b96bd9816ed943be6`;
- `levchenkov-andrey.webp`: source crop `(280, 300, 560, 580)`, SHA-256
  `8bfbc3a59201ea827ef2f3bd55433e8061bd05a3a9e1c034fef1e8819b346667`.

Those one-off crops were superseded when the complete catalog was connected.
The sync now creates all 512×512 runtime avatars with one deterministic
CPU-only alpha-bound crop: it focuses the upper part of the transparent KGD80
cut-out so faces remain legible in the 64px medallion. No face recognition,
identity inference or generative editing is used.

The runtime catalog is no longer limited to these two canaries. All current
KGD80 public portraits are synchronized into
`site/public/assets/participants/` by
`scripts/sync_kgd80_people_catalog.py`; person mapping and public provenance
live in `event_people/data/kgd80_people.json`.

The two checked-in source copies remain provenance/visual-QA canaries. Public
admission still requires the participant registry row to carry verified
identity and explicit rights/source evidence. The static exporter never treats
this directory alone as proof that a person participates in an event.
