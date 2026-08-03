# Reference transfer contract — tile mosaic v2

## Reference mechanic

- **Dominant visual:** a full-height physical 12×6 field of nearly square dark leather/metal tiles; an orange leather PWA squircle is continuously projected through individual tile surfaces.
- **Grid/composition:** fullscreen dark canvas; semantic copy is left; grid begins around 36–37% viewport width, touches the top, and overflows the right edge instead of compressing.
- **Main trick:** one continuous image below 72 physical tiles, with tile states controlling reveal; opaque nearly-black grout always separates tiles and never reveals the image.
- **Typography:** square PWA mark at upper-left, three-line H1 “Полюбить / Калининград / Анонсы”, orange tracked date, concise four-line service description.
- **Image treatment:** default brand mode hides the PWA image’s pale outer square and reads as a bounded orange leather squircle; generic mode uses ordinary cover and focal point without the brand mask.
- **Service zone:** large glass email field with envelope and a materially deep terracotta CTA, accessible hidden label and durable form feedback.

## Preserve

- Fullscreen, restrained, premium dark composition.
- One projection image plus 72 real tiles.
- Slow sparse seeded state movement, moving light and pointer bias.
- Existing secure selected-once subscription RPC behavior.
- Mobile semantic order and reduced-motion static state.

## Adapt

- Desktop tile sizing follows viewport height, so the 12-column matrix is intentionally clipped on the right.
- The PWA source remains byte-identical; masking/cropping is presentation-only.
- On mobile the same 72 nodes reflow to 6×12 and the page may scroll.
- SEO detail stays in metadata/structured data instead of overloading first-screen copy.

## Do not copy blindly

- Do not turn gaps into image-colored luminous lines.
- Do not expose the source PNG’s pale outer field.
- Do not use repeating diagonal carbon hatching.
- Do not fit all 12 columns into the right column at the expense of square geometry.
- Do not bake per-tile images or create 72 cropped assets.
- Do not hide content or form semantics inside decoration.

## Last-good lock

- **Base:** commit `8b22af29008456ec125b1404055a4283ddb2b57a`.
- **Do not change:** route/noindex boundary, Supabase RPC/security contract, one-image/72-tile architecture, safe URL validation, mobile reading order.
- **Allowed changes:** hero copy, brand presentation, grid geometry, material layers, tile distribution, form presentation and QA hooks.
