# PWA application icons

The canonical source is the operator-approved
`docs/reference/PKA-PWA2.png`. It contains the complete
`Полюбить Калининград / Анонсы` leather lockup, while the launcher label stays
the short, readable `Анонсы`.

Run `python site/scripts/generate-pwa-icons.py` from the repository root to
create:

- `announcements-brand-v2-192.png` and `announcements-brand-v2-512.png` for
  `purpose: any`;
- `announcements-brand-v2-maskable-192.png` and
  `announcements-brand-v2-maskable-512.png` for `purpose: maskable`.

The ordinary icons preserve the complete square source. The maskable pair
scales the artwork to 82% and centers it on the source's warm-white corner
colour. This keeps the leather edge and stitched outline inside Android circle
and squircle masks instead of letting the launcher crop them away. Resampling
is deterministic Pillow Lanczos and does not use an image generation model.
