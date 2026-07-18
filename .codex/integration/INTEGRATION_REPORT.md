# Integration report — static-event-v11-regression-repair

## Integrated scope

| Requirement | Status | Integration evidence |
|---|---|---|
| R01 | Done | One shared quality-admitted desktop/mobile media set; event `4783` keeps seven strong images and drops five weak renditions only because alternatives exist. |
| R02 | Done | Exact occurrence projection presents `5756` as the play «Женитьба», with event-photo hero and non-cropped OCR companion. |
| R03 | Done | Event `5658` lead ends with the complete source-backed sentence «превращается в остросюжетный конфликт.» |
| R04 | Done | KAUP ordinary/off/no-JS/elapsed fallback is `departure_board_v1`; all three accepted arms remain query-forceable, North station and bus/minibus copy are preserved. |
| R05 | Done | Weak-only `180×320` event `6815` remains available at source-size `contain` in desktop/mobile hero and fullscreen instead of crop/upscale. |
| R06 | Done | Main-reachable production-Kaggle build, immutable secret publication, public HTTP/Playwright, visual screenshot audit and Telegram handoff completed. |
| R07 | Partial | Exact negative/positive Smart Update identity replays are merged, but production remains shadow and canonical `5754`–`5757`/public repair awaits the documented manual audit. |

## Integration decisions

- The accepted v11 family matrix, not the named three pages alone, is the
  automatic template baseline.
- Weak images are excluded only when an event-local strong alternative exists;
  weak-only media stays visible without destructive enlargement.
- The accepted departure board is the safe transport default. Query-forced A/B/C
  arms are review controls and do not create trusted experiment telemetry.
- The static `5756` source-consistency guard is a safe projection, not a claim
  that canonical production data or existing public posts are repaired.
- The production root was not promoted. The bearer token is kept only in ignored
  release artifacts and the Telegram review message, never in tracked docs.

## Verification

- `pytest -q tests/test_static_site_*.py tests/test_smart_update_merge_identity_gate.py`: `63 passed`.
- `node --test site/tests/event-media-quality.test.mjs`: `5 passed`.
- `npm --prefix site run test:static-release`: `5 passed`.
- GitHub CI for PR `#76`: success; source SHA
  `a6ad22fba8b63e3dee7a71b8ca0837494c554033` is in `origin/main`.
- Kaggle build
  `production-20260718t-static-event-v11-regression-repair-kaggle-v2b`:
  snapshot SHA-256
  `8c784e2d14b34738a89f4cf0101645a46e470a2147c7752f73db7dcf83629972`,
  `323` event pages / `1172` files, all mandatory production/secret checks green.
- Secret prefix: `1173` exact authenticated objects and public hash/MIME checks.
  A tool-session interruption left `745` already-created objects; each ETag was
  checked against the local single-part body, the remaining `428` were created
  with `If-None-Match`, and final inventory/body/MIME parity was exact.
- Public acceptance: seven HTTP specimens with `noindex`/`no-referrer`; `21`
  focused Playwright checks, `36 × 2` mobile page checks and five real
  transitions; production root and sitemap hashes unchanged.
- Telegram delivery/readback: chat `4337049383`, topic `2`, message `300`.

## Remaining gates

- User visual acceptance before any production-root promotion.
- Source-grounded canonical repair and authenticated TG/VK/Telegraph repair for
  events `5754`–`5757`.
- Manual precision audit before changing the Smart Update identity gate from
  `shadow` to `enforce`.
