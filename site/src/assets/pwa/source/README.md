# PWA application icons

The canonical source is the operator-approved
`docs/reference/PWA-icon.png`. It contains the complete
`Полюбить Калининград / Анонсы` leather lockup, while the launcher label stays
the short, readable `Анонсы`.

Run `python site/scripts/generate-pwa-icons.py` from the repository root to
create:

- `announcements-brand-192.png` and `announcements-brand-512.png` for
  `purpose: any`;
- `announcements-brand-maskable-192.png` and
  `announcements-brand-maskable-512.png` for `purpose: maskable`.

The ordinary icons preserve the complete square source. The maskable pair uses
an equal 60 px inset crop from the 1254 px source: the leather surface reaches
the launcher mask, while the complete wordmark remains inside the central safe
area. Resampling is deterministic Pillow Lanczos and does not use an image
generation model.
