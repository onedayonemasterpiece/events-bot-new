# L04 transport results

- **Status:** Done
- **Requirement IDs:** R08
- **Branch:** `agent/static-unified-corrections/transport`
- **Worktree:** `/home/dev/.codex/worktrees/events-bot-new/static-unified-corrections-transport`
- **Base SHA:** `5c2db86811c34355a1894748b87af73fdb5b19e3`

## Outcome

- Added an optional `preview_only` duration-estimate dataset. Event `6529` retains a null canonical `time_range_end`; its separate `llm_estimated` record includes the Gemini 3.1 Pro (Low) model id/label, timestamp, retained artifact provenance, most-likely `120` minutes, plausible `90–180`, medium confidence, and conservative routing duration `150` minutes.
- The estimate path is enabled only when `PUBLIC_PREVIEW_BUILD_ID` is present. Production/root-form builds fail closed to the unknown-end contract instead of consuming the prototype estimate.
- Event `6529` now uses the conservative predicted end `17:30` plus the existing 25-minute access buffer, producing a ready time of `17:55` and the same-day return shortlist `18:56` and `19:43`. Both are same-date services and neither the data model nor generated ICS entries include a next-day train.
- The UI labels this as an experimental AI prediction, gives the likely/plausible/conservative durations, calls the end time approximate, says it is not organizer-confirmed, and explicitly advises confirmation.
- Unknown-end events without an estimate now expose only the last same-day boundary. Next-morning/overnight suggestions and their ICS files are no longer emitted.
- Event `3103` keeps its source-explicit `1 h 40 min` desktop repair: exact end `19:40`, 30-minute access buffer, ready time `20:10`, and returns `20:36` / `21:43`. Its un-repaired mobile projection fails closed to the last same-day train rather than suggesting a morning wait.

## Changed files

- `site/src/lib/eventTransport.ts`
- `site/src/components/EventTransportSchedule.astro`
- `site/src/data/event-duration-estimates.json`
- `site/tests/event-transport-estimates.test.mjs`
- `tests/test_static_site_preview_duration.py`
- `.codex/lanes/L04/RESULTS.md`

No preview-events data, shared generated-output gate, canonical documentation, or `CHANGELOG.md` file was changed.

## Validation

```text
node --test site/tests/event-transport-estimates.test.mjs
  5 passed

uv run --with pytest==8.3.5 pytest -q --noconftest tests/test_static_site_preview_duration.py
  4 passed

npm --prefix site run check:rail-directory
  PASS: 13 official pages, 9 routes, 17 locality policies, 10 service patterns

npm --prefix site run build:preview
  PASS: 311 pages
  dist/preview-20260723t064225-5c2db868/

Final Astro preview-mode on-demand smoke:
  event 6529 HTTP 200
  two llm_estimated surfaces
  predicted end 17:30
  return rows 18:56 / 19:43
  zero legacy next-morning copy

Final Astro preview-mode on-demand explicit regression:
  event 3103 HTTP 200
  desktop explicit end 19:40
  return rows 20:36 / 21:43
  mobile same-day boundary 22:40
  zero legacy next-morning copy

git diff --check
  PASS
```

## Integration notes

- The integrator should retain the cited ignored consultant evidence at `artifacts/codex/static-unified-corrections-20260723/gemini-duration-6529/`.
- Canonical docs and `CHANGELOG.md` remain integration-owned.
- The optional estimate is deliberately not written into `preview-events.json` or any canonical event end field.
