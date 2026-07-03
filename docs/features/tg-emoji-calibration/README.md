# Telegram emoji calibration

Status: smoke-test tooling.

## Goal

Calibrate how Telegram clients render `100x100` custom/premium emoji when they are placed as a small multi-line image mosaic inside a post, for example a venue medallion rendered as `3×3` custom emoji.

Telegram's official creator docs define the upload asset size for static custom emoji as exactly `100×100` PNG/WEBP, but do not document message line-height, baseline, clipping, or overlap metrics. Therefore the usable source-pixel rectangle must be measured empirically on the target Telegram clients.

Official references:

- <https://core.telegram.org/stickers> — static custom emoji images: `100×100`, PNG/WEBP.
- <https://core.telegram.org/stickers/webm-vp9-encoding> — video emoji: `100×100` WEBM/VP9.

## Calibration workflow

Use the existing non-adaptive pack for experiments:

- pack link: <https://t.me/addemoji/kenigeventspack>
- short name: `kenigeventspack`

Generated assets and receipts belong under `artifacts/codex/tg-emoji-calibration/` and must not be committed.

### 1. Generate deterministic probe emoji

```bash
python3 scripts/tg_emoji_calibration.py generate \
  --out-dir artifacts/codex/tg-emoji-calibration/generated \
  --prefix tgcal \
  --format png
```

This creates strict `100×100` assets:

- `y_stripes`: one unique color per source Y pixel.
- `x_stripes`: one unique color per source X pixel.
- `edge_probe`: visible top/bottom/left/right sentinels.
- `tile_r1c1` … `tile_r3c3`: a `3×3` mosaic probe with per-tile labels and coordinate-colored rows.

### 2. Upload probes into `kenigeventspack`

Use only the local human-client session (`TELEGRAM_AUTH_BUNDLE_E2E` or `TELEGRAM_SESSION`), never the Kaggle/remote `TELEGRAM_AUTH_BUNDLE_S22` bundle.

```bash
python3 scripts/tg_emoji_calibration.py upload \
  --env-file /home/dev/projects/events-bot-new/.env \
  --manifest artifacts/codex/tg-emoji-calibration/generated/tgcal_manifest.json \
  --short-name kenigeventspack \
  --out artifacts/codex/tg-emoji-calibration/tgcal_upload_receipt.json
```

Telegram may need up to about an hour before newly added emoji render in every client/search surface. The MTProto upload receipt and `GetStickerSet` count verify that the server accepted the items; visual UI verification can lag.

### 3. Send a smoke message to Saved Messages

```bash
python3 scripts/tg_emoji_calibration.py send-smoke \
  --env-file /home/dev/projects/events-bot-new/.env \
  --receipt artifacts/codex/tg-emoji-calibration/tgcal_upload_receipt.json \
  --chat me \
  --out artifacts/codex/tg-emoji-calibration/tgcal_send_receipt.json
```

The message contains four blocks:

1. `Y_STRIPES_3_ROWS`
2. `X_STRIPES_1_ROW`
3. `EDGE_3_ROWS`
4. `TILE_3X3`

If the message still shows fallback `🧩`, wait for pack propagation, restart/reopen the Telegram client, and retry the screenshot.

### 4. Screenshot and analyze

Take a screenshot of the target Telegram client at the intended zoom/font settings. Crop tightly around one calibration block, then run:

```bash
python3 scripts/tg_emoji_calibration.py analyze-screenshot block-y.png \
  --rows 3 \
  --cols 3 \
  --axis y \
  --out artifacts/codex/tg-emoji-calibration/analysis-y.json
```

For the horizontal probe:

```bash
python3 scripts/tg_emoji_calibration.py analyze-screenshot block-x.png \
  --rows 1 \
  --cols 3 \
  --axis x \
  --out artifacts/codex/tg-emoji-calibration/analysis-x.json
```

The report field `safe_source_pixel_range_intersection` is the conservative source-pixel range visible in every analyzed cell. If, for example, Y analysis returns `[10, 89]`, real `3×3` medallion slices should keep meaningful artwork inside source rows `10…89` of each `100×100` emoji tile and leave the rest transparent or background-colored for that Telegram client/zoom.

## Production use after calibration

For real medallion emoji mosaics:

1. Slice the source picture into `3×3` tiles.
2. For each `100×100` tile, place meaningful pixels only inside the calibrated safe X/Y ranges.
3. Fill unsafe overlap bands with transparency or with the target medallion background color.
4. Keep semantic fallback emoji meaningful enough for search/accessibility when possible.
5. Re-run calibration after major Telegram client updates, target-surface changes, or zoom/font changes.

## Known limitations

- Telegram rendering metrics are client-specific and undocumented; test Desktop and the mobile client that will be used for QA.
- Screenshots include scaling and antialiasing. The analyzer is intentionally conservative and should be treated as a practical rendering calibration, not a formal Telegram contract.
- Newly added custom emoji can be accepted server-side before they visually appear in clients.
