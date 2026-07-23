# L03-R2 — key-driven duration enrichment

Status: **Done**

## Delivery

- Added `site/scripts/enrich-event-duration-estimates.py`, a bounded build-time
  enrichment command that:
  - calls the repository `google_ai.client.GoogleAIClient`, never `agy` and
    never a browser-side model;
  - uses the configured `GOOGLE_API_KEY*` pool through the Supabase limiter;
  - defaults to the established low-cost `gemini-3.1-flash-lite`;
  - requests strict structured JSON and validates exact fields, ordering and
    30–480 minute bounds;
  - derives the conservative routing duration deterministically;
  - caches by model + prompt version + canonical event input SHA-256;
  - caps fresh provider calls (`--max-events`, hard maximum 50);
  - omits invalid/failed results so transport falls back to unknown-duration
    behavior.
- Replaced the previous manually generated `agy` specimen for event 6529 with a
  real API-key result. Persisted provenance says
  `generation_method=provider_api`,
  `gateway=google_ai.client.GoogleAIClient`,
  `model=gemini-3.1-flash-lite`, plus prompt version and input hash.
- Build-time estimates are available to both preview and production static
  generation and never become a canonical event end.
- Public transport UI no longer exposes model identity, confidence,
  “Экспериментальный”, “прогноз ИИ”, ranges or internal routing arithmetic.
  It keeps only the useful evening trains and restrained uncertainty copy.
- Event 6529 retains returns at **18:56** and **19:43** and has no next-morning
  06:42 option.

## Live API evidence

Executed locally with the repository `.env` loaded without printing any secret:

```text
python site/scripts/enrich-event-duration-estimates.py \
  --event-id 6529 --max-events 1 --require-complete
```

Result:

```json
{"status":"ok","model":"gemini-3.1-flash-lite","estimates":1,"failures":[]}
```

The provider returned most-likely 120 minutes, plausible 90–180 minutes,
confidence medium. The deterministic routing rule produced 150 minutes. A
second run with all Google key env variables removed reused the valid cache and
left the output SHA-256 unchanged:
`9ceaf047293419ba0afdb6a5d0685a00fd6beb752cd101fb23984b6498c3d393`.

## Validation

- `site/tests/event-transport-estimates.test.mjs`: **5/5 passed**
- `tests/test_static_site_preview_duration.py` +
  `tests/test_static_site_duration_enrichment.py`: **7/7 passed**
- Preview Astro build: **389 pages**
- `npm --prefix site run check:preview`: **passed**, 288 events
- Generated event 6529:
  - contains 18:56 and 19:43;
  - does not contain 06:42;
  - contains no Gemini/Gemma/model/confidence/experimental-AI public copy.
- `git diff --check`: passed

## Integration command

Production/export automation can invoke the same server-side cached command
before the Astro build. The command is repeatable without Codex or `agy`; only
the configured provider keys and Supabase limiter credentials are required for
cache misses.
