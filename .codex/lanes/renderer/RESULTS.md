# Renderer lane results

## Scope completed

- Ported the accepted cube-scene renderer, three deterministic daily layout
  families, dated face preparation, bundled brand assets, and GPU-debug / exact
  bundle CPU-final Kaggle gate from the experiment branch.
- Added a fail-closed accepted-catalog snapshot adapter for Fly SQLite. It emits
  eligible current/future distinct event count, normalized city count/list,
  seven-day additions, measured timestamp, and a stable catalog hash.
- Added read-only promo resolution. Exact `service_share_card` activities win;
  while production has none, preview fallback considers only active explicit
  `event`/`festival` targets. Broad `all` and author targets are ignored. Promo
  underfill is explicit and fallback faces retain their real popular/random
  labels.
- Reserved promo candidates before popularity, used current source reaction/view
  aggregates for popularity, and used a stable day-dependent SHA-256 rotation
  for random faces. Added a visibility-aware 8-face slot order.
- Removed hard-coded `274` / `75`; product copy and city list now consume the
  accepted snapshot. Metric grammar is invariant `СОБЫТИЙ` and its label follows
  the measured number width.
- Added immutable true PNG + WebP export and the UI-owned manifest contract at
  local `service-share/current/manifest.json`: `schema_version`,
  `asset_version`, `visual_payload_hash`, canonical URL, share text, and exact
  MIME/bytes/SHA records. Asset URLs are manifest-relative and versioned, so
  preview build prefixes are retained.
- Added an explicit daily orchestrator with local-date idempotency and a scheduler
  hook at 08:45 `Europe/Kaliningrad`, gated by
  `ENABLE_SERVICE_SHARE_CARD_DAILY=0` by default.
- Added standard signed Kaggle status-dataset wiring when callback + DB are
  configured, `service_share:renderer` lease acquire/release, business
  checkpoints/heartbeats, redacted local JSONL fallback, and cleanup for both
  bundle and status datasets. No blank callback/token config is created.

## Validation

- Focused renderer/data/status/scheduler tests: `16 passed`.
- Existing scheduling tests: `36 passed, 1 failed`; the remaining
  `tests/test_scheduler_limits.py::test_scheduler_offsets_and_limits` expects
  every pre-existing job to use a 30-second grace period, but the existing
  scheduler already registers jobs with longer explicit grace periods. The new
  job is disabled in that run and is not present in its log, so this is not a
  renderer regression.
- `python3 -m py_compile` passed for all added/modified Python modules.
- `git diff --check` passed.
- Read-only production snapshot preparation (no Kaggle/public mutation) against
  `prod-20260715.sqlite` produced: 284 eligible events, 15 normalized places,
  84 additions in the exact trailing 168-hour window, 60 events with approved +
  semantically classified public posters, and 8 downloadable dated faces.
  Active explicit promo festival inventory had no eligible approved/classified
  future poster, so the result correctly recorded promo underfill and selected
  four popular + four stable-random faces without mislabelling.

## Deliberately not done in this lane

- No Kaggle kernel was pushed and no GPU/CPU public run was started.
- No Object Storage/CDN upload and no production `current` pointer mutation.
- Scheduler remains OFF by default. Local filesystem date acceptance prevents a
  second accepted result in one process/volume, but a future production publisher
  still needs the dedicated Object Storage compare-and-swap handoff for
  cross-host exactly-once delivery.
- The renderer preserves the user-approved square 1024×1024 cube composition;
  it does not silently convert it to the earlier 1080×1350 F18 concept.
- Documentation and `CHANGELOG.md` were intentionally left to the integration
  owner per lane ownership instructions.
