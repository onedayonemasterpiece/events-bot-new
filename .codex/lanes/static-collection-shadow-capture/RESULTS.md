# Lane results: static-collection-shadow-capture

## Scope

- Lane ID: `static-collection-shadow-capture`
- Requirement IDs: `R3`, `R5`
- Base SHA: `61b8d7dcf58a4299c2e9a7538fa55c3eeda9be79`
- Implementation head SHA: `a0b3e4e104ea4b59cd72f68b87ca353533862767`
- Branch: `agent/static-collection-shadow-e2e/capture`
- Effort/risk: high; closed evidence contract and replay boundary, but no production mutation.

## Result

Implemented a manual, pure capture helper/CLI for
`static-collection-upstream-capture-v1` and narrowly taught the existing
ordinary-ingestion replay harness to consume the captures.

The capture layer:

- never imports `Database`, bot code, an ingestion handler, or publication code;
- writes exactly one packet per artifact and refuses an existing output or any
  output below `/data`;
- records the exact handler name, repository SHA, actual CLI capture time,
  immutable source binding, canonical serialization contract and sanitized
  payload SHA-256;
- fails closed on credential-shaped fields and credential URL query parameters;
- selects one exact Telegram message plus its exact matching `sources_meta`,
  preserves unknown nested fields, and rejects cross-message/reply/linked-source,
  poster-owner and unresolved grouped-media dependencies;
- serializes one VK `EventDraft` with replay-relevant `PosterMedia` metadata,
  omitting poster bytes while recording their hash/size/availability;
- serializes one official-parser `TheatreEvent`;
- validates a closed Draft 2020-12 JSON Schema plus cross-field/hash/source
  invariants on both write and replay load.

The replay extension accepts legacy fixtures unchanged. For v1 captures it
validates the artifact and manifest source binding, unwraps a Telegram
handler-compatible envelope, reconstructs only a `PosterMedia(data=b"")` with
the captured digest/metadata for VK, and restores the parser dataclass. No
location, parser identity, matching, prompt, retry, or publication semantics
were modified.

## Changed files

- `scripts/capture_static_collection_upstream_packet.py`
- `docs/review-data/static_collection_upstream_capture.schema.json`
- `scripts/run_static_collection_ingestion_replay.py`
- `tests/test_static_collection_upstream_capture.py`
- `.codex/lanes/static-collection-shadow-capture/RESULTS.md`

Shared feature/release docs and `CHANGELOG.md` were intentionally not edited;
the lane assignment reserves them for the integrator.

## Evidence and commands

```text
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_static_collection_upstream_capture.py \
  tests/test_static_collection_ingestion_replay.py

20 passed in 1.80s
```

```text
/home/dev/.codex/venvs/events-bot-new/bin/python -m py_compile \
  scripts/capture_static_collection_upstream_packet.py \
  scripts/run_static_collection_ingestion_replay.py

git diff --check
```

Both completed successfully.

CLI smoke used a temporary parser input and output, returned status
`captured`, produced a schema-valid artifact, and exited `0`.

## Risks / follow-up

- This lane implements capture and replay compatibility only; it does not
  acquire the three fresh production packets or claim Gate E PASS.
- Telegram dependency detection is intentionally conservative. An ambiguous
  new dependency representation will be rejected rather than reconstructed.
- Closed `EventDraft`, `PosterMedia`, and `TheatreEvent` schema fields must be
  updated deliberately if their production dataclasses evolve.
- VK poster binary cannot be replayed by design. The replay-relevant digest,
  URLs, OCR and usage metadata are retained, while `data` is restored as empty
  bytes to prevent network/media side effects.
