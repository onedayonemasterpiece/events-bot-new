# Event transport guidance

> Status: validated JSON-adapter refresh foundation implemented; **official-provider ingestion and the production schedule are not activated**. A controlled KPPK and bus canary, concrete official-source adapter review and static presentation integration are still release gates.

## Product contract

For an eligible event outside Kaliningrad, a static event page may consume source-backed public-transport guidance. The browser is not a journey planner and never calls a provider. It receives only an immutable manifest accepted on the server.

Fail-closed rules:

- the event has a named city and venue with coordinates and is outside Kaliningrad;
- the service has an exact `service_date` in `Europe/Kaliningrad`;
- city, reviewed venue name/alias and optional event id binding match;
- outbound arrival is no later than the event start;
- a return is offered only for an explicit event end and departs after that end;
- missing, stale, invalid or unbound data renders no suggestion;
- event ICS and transport-leg ICS are separate artifacts; a transport leg has its own stable UID, exact departure/arrival and 30-minute alarm.

The deterministic reference implementation is in `transport_refresh/selection.py` and `transport_refresh/ics.py`. It deliberately does not copy the old PR #37 presentation components.

## Refresh architecture

There are two independently runnable Kaggle CPU lanes:

- `scripts/run_kppk_transport_refresh_kaggle.py` / `kaggle/TransportKppkRefresh`;
- `scripts/run_bus_transport_refresh_kaggle.py` / `kaggle/TransportBusRefresh`.

Both use the shared `transport_refresh` package and versioned `kenigevents.transport_provider.v1` contract. A provider kernel fetches or reads a controlled-canary **JSON adapter output**, validates it, and emits only:

- `transport-<provider>-manifest.json`;
- `transport_provider_result.json`;
- Kaggle status/log evidence.

Each status-aware run uses the existing `kaggle_run_ledger`, heartbeat client and a provider-specific resource lease (`transport_schedule:kppk:refresh` or `transport_schedule:bus:refresh`). Separate leases make the two providers independently runnable; server fan-in remains serialized.

The public KPPK and regional-bus pages currently expose HTML/PDF, not this JSON contract. The generic job intentionally refuses to infer schedule semantics from either format. Therefore TR-1/TR-2 job isolation is implemented, but real provider ingestion remains **Partial/Blocked** until separately reviewed KPPK and bus adapters produce explicit dated JSON with source provenance. Supplying an official page URL directly is not a valid canary and must fail JSON decoding rather than silently scrape it.

Kaggle runs only the declared script file; sibling Python packages are not a reliable kernel-code boundary. The runner therefore places the shared `transport_refresh` package in the same unique private input dataset as the config, together with `kenigevents.transport_runtime_package.v1`. The script accepts exactly the allowlisted module paths and verifies every SHA-256 before adding that dataset root to `sys.path`. Staged metadata is rewritten to the authenticated `${KAGGLE_USERNAME}` owner before push.

## Schema contract

Every provider snapshot contains:

- provider, schema version, immutable snapshot id;
- source HTTPS URL, source byte SHA-256 and `fetched_at`;
- `valid_from`, `valid_until`, `status=valid`;
- exact service date and `Europe/Kaliningrad` timezone;
- provider route/trip identity;
- at least two named stops with stable ids and coordinates;
- timezone-aware departure and arrival with stop ids;
- direction (`outbound` / `return`);
- event ids (optional restriction), city, named venue/aliases and venue coordinates;
- per-service HTTPS source.

KPPK services must be `rail`; bus services must be `bus`. Departure must fall on the exact local service date, arrival must be later, and the service date must be inside source validity.

Two hashes have different purposes:

- `snapshot_hash` covers retrieval metadata and identifies an immutable provider/combined artifact;
- `content_hash` covers the semantic schedule. A successful re-fetch with identical services advances freshness without rebuilding the site.

## Server fan-in and last-good

`TransportManifestStore` writes under `TRANSPORT_MANIFEST_ROOT`:

```text
providers/{kppk,bus}/manifests/<snapshot_hash>.json
providers/{kppk,bus}/current.json
providers/{kppk,bus}/attempts/<attempt_id>.json
providers/{kppk,bus}/status.json
combined/manifests/<snapshot_hash>.json
combined/current.json
combined/status.json
combined/rebuild.json
```

Writes use a local lock, file+directory fsync and atomic rename. Provider validation happens before its `current.json` is moved. Timeout, empty, invalid and stale candidates do not replace provider last-good. Every attempt is nevertheless immutable in `attempts/` and projected into `status.json`; it distinguishes `attempt_status` (`fresh|failed|invalid|partial|stale`), `serving_status` (`fresh|last_good|missing|stale`) and fan-in `complete|partial`. Fan-in uses both fresh provider last-good manifests; if either is missing/stale, combined current is not advanced. Status reports expose machine-readable reasons such as `provider:timeout`, `services:empty`, `freshness:source_stale`, `freshness:source_document_stale` and `freshness:validity_expired`.

The combined manifest is deterministic for the same provider inputs. The current pointer is accepted only when it stays under `combined/manifests/` and its immutable snapshot hash matches. A new immutable combined snapshot/current pointer may record fresher source retrieval, but the existing outbox receives exactly one `static_site_build:prod` coalesced task only when the accepted **combined semantic content hash** changes. An unchanged run enqueues zero unless it is retrying a previously failed handoff.

`combined/rebuild.json` is a durable outbox-intent handshake. The desired hash is fsynced before `combined/current.json` moves. Only a successful `enqueue_job(..., coalesce_key="static_site_build:prod")` acknowledges it; an enqueue failure leaves it `pending`, and even an unchanged later provider check retries it. If a static build already owns the key in `running`, `enqueue_job` creates exactly one deferred coalesced follow-up and later updates merge into that row. This closes both the pointer-before-enqueue loss window and the update-during-long-build loss window.

Server-side import is available directly after a waited runner (`--state-root`, optionally `--publish-db`) or separately:

```bash
python scripts/publish_transport_schedule.py \
  --provider kppk \
  --manifest artifacts/codex/transport-refresh/kppk-<run>/transport-kppk-manifest.json \
  --state-root /data/transport \
  --db /data/db.sqlite
```

Record provider failures without deleting last-good:

```bash
python scripts/publish_transport_schedule.py \
  --provider bus --failure-reason timeout --state-root /data/transport
```

## Production gate and rollback

Production activation is intentionally absent from `scheduling.py`/`fly.toml`. See the nightly canary plan in [the static transport schedule contract](../static-site-pages/event-transport-schedule.md). Before activation:

1. inspect real KPPK and bus adapter output against official source pages;
2. run both private CPU kernels with status DB/callback;
3. verify heartbeat, lease release, exact-date services and source hashes;
4. fan in without a publish DB, review the immutable combined artifact;
5. repeat with publish DB and confirm one changed build / zero unchanged builds;
6. validate generated event and transport ICS plus representative outside-city pages.

Waited terminal runs delete their unique private input/status datasets in `finally` by default; `--keep-input-datasets` is evidence/debug-only. `--no-wait` cannot delete mounted inputs and therefore leaves explicit operator-owned retention items. Timeouts do not delete inputs while a potentially live kernel may still need them.

### Controlled mechanics canary — 2026-07-17

After local contracts passed, private CPU runs `controlled-20260717-kppk-v3` and `controlled-20260717-bus-v1` completed under `zigomaro/kenigevents-transport-{kppk,bus}-refresh`, each producing one validated service and a provider manifest. Controlled fan-in produced semantic hash `28b103cec10c768ed5c49a962956982cfb159e7248720b3a382bdfda0f4d191a`; no DB was supplied, so the rebuild handshake correctly remained pending and no static build was created. Both unique inputs were deleted.

This is **synthetic JSON-payload mechanics evidence only**. Status DB/callback were absent, and neither official-source adapter exists, so it is not provider-data, production ledger, heartbeat/lease callback, activation or production readiness evidence.

Rollback is pointer-based: restore the previous reviewed `combined/current.json` target and enqueue one coalesced static rebuild. Never delete immutable manifests or repoint a provider to a rejected snapshot.

## Related documentation

- [Static transport schedule contract](../static-site-pages/event-transport-schedule.md)
- [Kaggle static-site builder](../../operations/kaggle-static-site-builder.md)
- [Static event pages](../static-site-pages/README.md)
