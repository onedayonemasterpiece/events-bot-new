# Transport artwork sources

## `kppk-lastochka.png`

- Supplied by the product owner in the repository working tree as `docs/reference/Frame 214 (3).png` on 2026-07-11.
- 500×101 RGBA PNG, exported from Figma; SHA-256 of the supplied original: `6f69c8d7da73a7203bb65ccf14da7f825cb3fc6e2538f9c5fbb258140a374951`.
- Content: transparent side-view artwork of a red/grey Lastochka train for the event travel block.
- Runtime derivative: lossless WebP at `site/public/assets/transport/kppk-lastochka.webp`.
- Do not treat this artwork as a carrier logo. The separately sourced official КППК/RZD logo remains under `site/public/assets/partners/`.

## `romanovo-holmogorye-route-square.png` and `romanovo-holmogorye-route-portrait.png`

- A pedestrian route was calculated on 2026-07-11 from the central `Романово` stop (`54.8958609, 20.2759337`) to the venue (`54.8817051, 20.2792613`): about `2.0 km / 26 min`.
- The Valhalla/OSM geometry was decoded, simplified to 29 points and drawn as a blue polyline over Yandex Static Maps at zoom 14 with start/end pins and Yandex attribution.
- Square desktop/tablet asset: 450×450 PNG, SHA-256 `113cb5c061803157d757c186dc76dbfecad89c96f198789544f8e686e521d2eb`.
- Portrait mobile asset: 360×450 PNG, SHA-256 `fae27c0d4180ce146adc817c79631ab4e19bbf21269a4d844b53a63bae8d98a8`.
- Runtime uses `<picture>` to select the portrait crop at `max-width: 720px` and the square crop otherwise. The page exposes one preferred interactive Yandex pedestrian route plus the venue-coordinate link; the longer `Романовский поворот` walk remains disclosed as text.

## `bus-svgrepo-337651.svg`

- SVG Repo asset `Tour Bus` (`337651`): <https://www.svgrepo.com/svg/337651/tour-bus>.
- License: CC0; copied from the shared local SVG Repo library on 2026-07-11.
- SHA-256 `62ab427900ef1829cc88066e9e3a8cd92e74fbed7c07b8af9cec89be1f629bb2`.
- Runtime path: `site/public/assets/transport/bus-svgrepo-337651.svg`; used as the decorative bus-mode icon instead of a text letter.
