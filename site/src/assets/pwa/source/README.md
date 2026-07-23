# PWA application icons

The runtime icons `site/public/assets/pwa/announcements-192.png` and
`announcements-512.png` are deterministic local rasterizations of the canonical
`site/public/favicon.svg`. The mark is centered at 72% of the canvas over the
existing warm KenigEvents surface colors so platform launcher masks do not clip
the tag. They are declared as `purpose: any`; no claim of a maskable safe-zone
asset is made.

Generated locally with the repository-installed `sharp` dependency. No image
generation model or external artwork was used.
