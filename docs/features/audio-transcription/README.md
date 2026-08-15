# Telegram-native audio transcription

Status: production-enabled for the private ChatGPT/OpenCode MCP since
2026-08-15. Access accepts either the narrow `audio:transcribe` scope or the
existing stable `telegram:publish` capability family; Codex remains read-only
and automatic/batch ingestion is not enabled.

## Purpose

This capability accepts a Russian-language audio recording from ChatGPT `fileParams`, preserves the original bytes and recording-time evidence, delegates media preprocessing to a private Kaggle CPU kernel, sends bounded OGG/Opus chunks to Telegram as temporary voice notes, requests Telegram native transcription through Telethon, and returns both ordinary text and a timeline suitable for correlation with photos and video.

The recognizer is Telegram. Kaggle is the media and orchestration worker; it does not run a second ASR model.

## Public MCP surface

The ChatGPT/OpenCode resource receives three tools only when
`PRIVATE_EVENTS_MCP_AUDIO_TRANSCRIPTION_ENABLED=1`. A new least-privilege token
may request `audio:transcribe`; existing ChatGPT/OpenCode connections holding
the stable `telegram:publish` capability family receive the same typed tools
without re-consent. `telegram:read` alone is insufficient.

| Tool | Contract |
|---|---|
| `audio_transcription_start` | Authenticated `fileParams` ingress, immutable source asset, idempotent durable job, immediate `job_ref` return |
| `audio_transcription_status` | Durable state, bounded progress, terminal error code, result availability |
| `audio_transcription_get` | Paginated `segments`, `plain`, `timeline`, `json`, `srt`, or `vtt` result |

Codex remains the exact seven read-only evidence tools and never receives the transcription scope or tools.

The start request accepts one source file per durable job:

```json
{
  "file": {
    "download_url": "<connector supplied>",
    "file_id": "<connector supplied>",
    "mime_type": "audio/mp4",
    "file_name": "20260812_100257.m4a"
  },
  "idempotency_key": "lecture-2026-08-12-main-recorder",
  "precision": "phrase",
  "timezone": "Europe/Kaliningrad",
  "recording_started_at": null
}
```

`recording_started_at`, when supplied, must be timezone-aware RFC 3339. A missing anchor is not guessed. A set of recorder files is submitted as separate idempotent jobs; their absolute segment timestamps can then be sorted into one event timeline without physically joining the media.

## Data flow

```text
ChatGPT fileParams
  -> owner-bound immutable audio asset
  -> durable queued job
  -> remote Telegram session guard
  -> private Kaggle input dataset + separate key dataset
  -> ffprobe metadata and duration
  -> ffmpeg silence detection
  -> pause-aware OGG/Opus voice chunks
  -> Telethon send_file(..., voice_note=True)
  -> messages.TranscribeAudioRequest
  -> UpdateTranscribedAudio or immediate result
  -> delete temporary Telegram voice note
  -> canonical transcript.json
  -> TXT / timeline TXT / SRT / VTT
  -> digest-verified Kaggle output collection
  -> source asset and ephemeral datasets cleanup
```

## Time model

Telegram returns text but no word or phrase timestamps. The system therefore binds every returned text block to the exact source range that produced the temporary voice note.

Uploading, converting, and sending a chunk to Telegram do not change this
timeline. The worker plans ranges on the decoded timeline of the original
source and retains each original `source_start_ms` / `source_end_ms` while
ffmpeg creates a temporary voice-note representation. Telegram message time,
upload time, Kaggle start time, temporary-file `mtime`, and transcoded duration
are never used as the recording anchor.

For each segment:

```text
absolute_start = recording_started_at + source_start_ms
absolute_end   = recording_started_at + source_end_ms
```

Anchor confidence order:

1. Explicit `recording_started_at` with UTC offset.
2. `com.apple.quicktime.creationdate`.
3. Format or stream `creation_time`.
4. Date and time parsed from the source filename in the requested IANA timezone.
5. Missing: return source-relative timestamps only.

The canonical result records the anchor source and uncertainty. Telegram message time and filesystem `mtime` are deliberately not treated as recording start evidence.

Example timeline output:

```text
[12.08.26 10:03:13] Сегодня мы поговорим об истории этого здания.

[12.08.26 10:03:58] Оно было построено в начале двадцатого века.
```

Without a trustworthy anchor:

```text
[00:00:16.420] Сегодня мы поговорим об истории этого здания.
```

For photo/video correlation, prefer the JSON result: every segment contains
both the source-relative millisecond range and, when an anchor is trustworthy,
`absolute_start` / `absolute_end`. Plain TXT intentionally has no timing;
timeline TXT has the readable absolute/relative timeline; SRT and VTT retain
the source-relative timeline. A photo timestamp can therefore be matched to
the segment interval that contains it without using any upload timestamp.

Current precision is chunk-level, not word-level. With `precision=phrase` the
normal target is about 45 seconds (pause-aware, bounded to 90 seconds), so the
system truthfully identifies the source interval in which a phrase block was
spoken but does not claim an exact word instant inside that interval.

## Production rollout

- PR [#505](https://github.com/onedayonemasterpiece/events-bot-new/pull/505)
  merged as `f0d5f3b4de8d968fd1b43ec5b07ffda33409ecca` after all required CI jobs passed.
- The exact merged SHA was deployed through `scripts/deploy_fly_main.sh`; the
  Fly health check passed and the in-container immutable SHA matched.
- The production OAuth/MCP smoke requested `audio:transcribe` and returned all
  three audio tools from `tools/list`.
- The dedicated Premium session, private transcription group, Kaggle and
  Telegram credentials are present; automatic ingestion remains absent.
- Existing ChatGPT/OpenCode connections with the stable `telegram:publish`
  scope discover the audio tools without re-consent. A connection that was
  intentionally authorized read-only remains read-only and may instead request
  the narrower `audio:transcribe` scope.
- ChatGPT conversation discovery is order-sensitive for this large shared MCP
  catalog. The three audio workflow tools are therefore registered first while
  every existing tool remains present. Regression contract:
  `INC-2026-08-15-audio-mcp-runtime-catalog-truncation`.

## Chunking profiles

`precision=segment` targets approximately 150 seconds and never exceeds 240 seconds before a server-directed recursive split.

`precision=phrase` targets approximately 45 seconds and never exceeds 90 seconds before a server-directed recursive split.

Both modes prefer an observed silence boundary, cover the original recording exactly once, preserve source offsets, and make no overlapping chunks. If Telegram responds with `MSG_VOICE_TOO_LONG`, the rejected source range is split near its midpoint, preferably at a pause, and retried. The maximum recursion depth is bounded.

A future word-alignment mode is intentionally not advertised. The present contract does not claim word timestamps that Telegram does not supply.

## Source ingress and retention

The audio store is independent from the social image/document store because its media policy and lifecycle are different.

Ingress invariants:

- exactly one connector-supplied HTTPS `fileParams` object;
- no caller-provided local path, Telegram native ID, arbitrary URL tool, or raw bytes field;
- exact or explicit wildcard hostname allowlist;
- public DNS answers only, pinned for the request;
- no redirects, cookies, proxy inheritance, or content decompression;
- bounded content length and streaming byte count;
- byte-signature classification for OGG, FLAC, WAV, MP3, AAC, M4A/MP4, and WebM;
- `ffprobe` on Kaggle remains the authoritative audio-stream validator;
- immutable regular file, SHA-256 verification, owner binding, opaque `aud_*` reference, expiry and capacity limits;
- signed source URL and connector file ID are never stored as usable values.

The source asset is deleted after a terminal job. Kaggle input and key datasets are deleted after collection unless the explicit diagnostic retention flag is enabled. Output retention defaults to seven days.

## Telegram session isolation

The default auth source is exactly:

```text
TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION
```

It must be a dedicated Telethon `StringSession`. The code rejects local E2E session names and rejects borrowing `TELEGRAM_AUTH_BUNDLE_S22` unless an explicit migration override is present.

Before dispatch, the worker applies the shared remote Telegram session guard and an additional rolling-deploy compatibility check for already registered `audio_transcription` jobs. Each Kaggle run registers:

```json
{
  "type": "audio_transcription",
  "remote_telegram_auth_scope": "TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION",
  "run_id": "atr_...",
  "input_dataset_ref": "...",
  "key_dataset_ref": "..."
}
```

Unknown active status fails closed. Terminal registry rows are cleaned. The same auth key must never be active concurrently from Fly and Kaggle.

## Kaggle handoff

Two private, job-specific datasets are created:

1. Input dataset:
   - source audio;
   - `request.json`;
   - encrypted Telegram secret payload;
   - importable `audio-transcription-runtime.bundle`.
2. Key dataset:
   - Fernet key only.

Neither dataset contains Kaggle credentials. The repo-local `kaggle/AudioTranscription` script bootstraps the runtime bundle and executes the worker. The kernel is CPU-only and internet-enabled for Telethon and dependency bootstrap.

Output collection accepts only a matching schema, `job_ref`, source digest, safe manifest filenames, existing files, exact SHA-256 hashes, and internally consistent aggregate evidence for native transcript count and temporary-message cleanup. Telegram message IDs and transcription IDs are not exposed.

## Durable job states

```text
queued
  -> dispatching
  -> running
  -> complete

queued/dispatching/running
  -> failed
```

A background monitor recovers queued jobs after restart, polls running Kaggle kernels, collects verified outputs, and cleans terminal assets. Dispatch is serialized because one dedicated Telegram auth scope backs the lane. The idempotency key is bound to the source digest and semantic request; reusing it for another recording fails closed.

## Configuration

Required only when enabled:

| Variable | Purpose |
|---|---|
| `PRIVATE_EVENTS_MCP_AUDIO_TRANSCRIPTION_ENABLED=1` | Runtime kill switch |
| `AUDIO_TRANSCRIPTION_ALLOWED_HOSTS` | Exact/wildcard `fileParams` download hosts; falls back to existing MCP media host allowlist |
| `KAGGLE_USERNAME`, `KAGGLE_KEY` | Existing server-side Kaggle credentials |
| `TG_API_ID`, `TG_API_HASH` | Telegram API application credentials |
| `TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION` | Dedicated user session with native transcription entitlement |
| `AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_REF` | Isolated target kernel, default `<KAGGLE_USERNAME>/events-bot-audio-transcription` |

Optional:

| Variable | Default |
|---|---|
| `AUDIO_TRANSCRIPTION_ROOT` | `/data/audio-transcription` |
| `AUDIO_TRANSCRIPTION_MAX_ASSET_BYTES` | 512 MiB |
| `AUDIO_TRANSCRIPTION_MAX_STORE_BYTES` | 2 GiB |
| `AUDIO_TRANSCRIPTION_ASSET_TTL_SECONDS` | 24 hours |
| `AUDIO_TRANSCRIPTION_DOWNLOAD_TIMEOUT_SECONDS` | 120 |
| `AUDIO_TRANSCRIPTION_RESULT_RETENTION_DAYS` | 7 |
| `AUDIO_TRANSCRIPTION_POLL_INTERVAL_SECONDS` | 20 |
| `AUDIO_TRANSCRIPTION_MAX_RUN_HOURS` | 8 |
| `AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_SOURCE` | `local:AudioTranscription` |
| `AUDIO_TRANSCRIPTION_AUTH_BUNDLE_ENV` | `TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION` |
| `AUDIO_TRANSCRIPTION_TELEGRAM_PEER` | `me` |
| `AUDIO_TRANSCRIPTION_CLEANUP_MESSAGES` | `1` |
| `AUDIO_TRANSCRIPTION_KEEP_KAGGLE_DATASETS` | `0` |

## Failure contract

Public MCP errors are bounded codes. URLs, local paths, native Telegram IDs, auth values, source bytes, and provider response bodies are not exposed.

Representative codes:

```text
AUDIO_REF_UNRESOLVED
AUDIO_HOST_NOT_ALLOWED
AUDIO_FILE_TOO_LARGE
AUDIO_MIME_NOT_ALLOWED
AUDIO_MIME_MISMATCH
AUDIO_FILE_INVALID
AUDIO_FILE_EXPIRED
AUDIO_FILE_INTEGRITY_FAILED
TRANSCRIPTION_INVALID_ARGUMENTS
TRANSCRIPTION_NOT_FOUND
TRANSCRIPTION_PRINCIPAL_MISMATCH
REMOTE_TELEGRAM_SESSION_BUSY
KAGGLE_DISPATCH_FAILED
KAGGLE_DISPATCH_OUTCOME_UNKNOWN
KAGGLE_RUN_FAILED
KAGGLE_OUTPUT_MISSING
KAGGLE_OUTPUT_INVALID
```

A Telegram timeout or unknown provider outcome is not blindly resent under a different idempotency key.
Telegram internal-DC exhaustion (including Telethon's bounded
`Request was unsuccessful N time(s)` wrapper) is retried against the same
temporary voice message with bounded backoff before failing closed; it does not
upload a duplicate voice note merely to recover from a provider transient.

For the live lane, prefer a dedicated private supergroup over `me`. Telegram
native transcription can fail with an internal DC error for Saved Messages on
an otherwise Premium-capable account while the same native and generated voice
documents transcribe successfully in a private supergroup. A private numeric
`-100...` peer is resolved from the dedicated account's dialogs so the group
does not need a public username. The account must already be a member; the
worker never joins or creates chats automatically.

## Code map

```text
audio_transcription/
  asset_store.py       hardened ChatGPT audio ingress
  chunking.py          pure pause-aware source-range planner
  config.py            strict default-off environment contract
  contracts.py         canonical schema and dataclasses
  exports.py           plain/timeline/JSON/SRT/VTT
  ffmpeg.py            ffprobe, silence detection, OGG/Opus conversion
  job_store.py         durable owner-bound idempotent SQLite jobs
  kaggle_backend.py    datasets, kernel launch, registry, output verification
  kaggle_worker.py     actual Kaggle/Telethon processing
  mcp.py               three typed ToolSpec definitions and runtime attachment
  service.py           restart recovery and background reconciliation
  session_guard.py     shared guard plus rolling-deploy compatibility check
  telegram_native.py   TranscribeAudioRequest and UpdateTranscribedAudio
  time_anchor.py       explicit/metadata/filename anchor resolution

kaggle/AudioTranscription/
  audio_transcription.py
  kernel-metadata.json
```

## Acceptance

Static/local acceptance in this change:

- pure timestamp, filename and anchor precedence tests;
- exact chunk coverage and hard-cap tests;
- timeline/TXT/SRT/VTT export tests;
- audio signature policy tests;
- owner-bound durable job/idempotency tests;
- real local ffprobe and OGG/Opus transcode test;
- Telegram native error classification test;
- compile check for the complete package and kernel bootstrap.

Production acceptance still requires one explicit live canary after secrets are configured:

1. Upload a short Russian `m4a` whose start time is independently known.
2. Confirm one `atr_*` job and one guarded Kaggle run.
3. Confirm the manifest reports every temporary-message cleanup attempt and zero failures.
4. Confirm native Telegram text is returned.
5. Confirm the first absolute segment equals recording start plus source offset.
6. Confirm all five export digests.
7. Confirm registry and both private datasets are removed.
8. Confirm no `AuthKeyDuplicatedError`, URL/path/token leakage, or second submission under the same idempotency key.

Do not enable production scheduling or automatic batch ingestion as part of the first canary.
