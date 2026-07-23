# L03 — clubs projection + ICAE partner

## Outcome

- Rebuilt `interest-clubs.json` from one consistent read-only Fly production SQLite snapshot captured on 2026-07-23.
- The policy-current projection contains three approved/fresh identities: `game-vibes`, `neural-researchers`, and `technology-researchers`; it does not restore the two-club donor fixture or the stale four-club live result.
- Kept the one current approved future meeting (`event_id=6990`) in the club activity projection, but failed its action closed because the shared base `preview-events.json` does not materialize that event. No external fallback or broken prefixed link is emitted.
- Added ИЦАЭ Калининграда as the deterministic sixth partner tile, linking to `https://klgd.myatom.ru/`.
- Copied the official wide `logo-footer-h.svg` byte-for-byte into both source provenance and the runtime asset.
- Updated the owned preview-hub labels and added focused projection/link/partner/provenance tests.

## Production snapshot / reproducibility

- Snapshot: Fly `events-bot-new-wngqia:/data/db.sqlite`, downloaded with `scripts/sync_prod_db.sh`.
- Local ignored artifact: `artifacts/db/static-unified-corrections-clubs-partners-20260723.sqlite`.
- Size: `279195648` bytes.
- SHA-256: `f49c5e829d6c230a92b76f3dc4a937d18991b6659c60fa73b85d648cc7953175`.
- `PRAGMA quick_check`: `ok`.
- Snapshot counts at capture: `event=6636`, `interest_club=8`, `interest_club_event=19`.
- Export date: `2026-07-23`; source contract: `sqlite-interest-clubs-v1`.
- Reproduction command:

```bash
ENABLE_INTEREST_CLUB_STATIC_PROJECTION=1 \
python3 site/scripts/export-production-preview-data.py \
  --db artifacts/db/static-unified-corrections-clubs-partners-20260723.sqlite \
  --output-dir /tmp/l03-fresh-export \
  --current-date 2026-07-23 \
  --focus-date-from 2026-07-23 \
  --focus-date-to 2026-07-26 \
  --include-ids 6990 \
  --skip-related \
  --skip-image-probes
```

The exporter materializes `event_id=6990` with `--include-ids 6990`, but L03 intentionally did not edit the shared `preview-events.json`. After copying the clubs projection, L03 cleared both `event_path` and `source_url` for 6990 because the base preview catalog does not contain it. Focused tests enforce this fail-closed rule. Integration should retain `--include-ids 6990` (or otherwise materialize it from the same snapshot) and regenerate clubs from that same event set if it wants the internal meeting link restored.

## ICAE asset provenance and risk

- Official city page: `https://klgd.myatom.ru/`.
- Official asset: `https://klgd.myatom.ru/wp-content/themes/icao2/image/logo-footer-h.svg`.
- Captured: 2026-07-23.
- Source/runtime byte size: `13523`.
- Source/runtime SHA-256: `e59541c9ffa5c4865d87c1273068b2440ebf89bc794de6d5d18387cc9a0f3797`.
- No image generation or asset transformation was used.
- **Authorization risk:** this is prototype-only. Partnership status and permission to use the ICAE mark must be confirmed before production publication.

## Verification

- `node --test site/tests/clubs-partners-current-projection.test.mjs` — 4 passed.
- `/home/dev/.venvs/events-bot-image-geometry/bin/python -m pytest -q tests/test_interest_clubs_static_export.py` — 3 passed.
- `PUBLIC_INTEREST_CLUBS_ENABLED=1 npm run build:preview` — 314 pages built; local build `preview-20260723t064645-5c2db868`.
- Built-output spot check: all three club detail routes exist; the unmaterialized 6990 title has no broken/external link; ICAE official URL and prefixed local SVG are present; runtime SVG was copied.
- `git diff --check` — passed.
