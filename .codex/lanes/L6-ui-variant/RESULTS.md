# L6-ui-variant results

## Scope

- Lane: `L6-ui-variant`
- Requirements: `R06`, `R09`
- Base SHA: `ec09c011674eecddf9e9b8e154e3d102f9384b12`
- Implementation SHA: `6fe3bb33ae782eb54cde759141611457bb75385f`

## Outcome

- Added a secret-candidate-only `search_variant` URL control with the exact closed vocabulary:
  `cached_vector`, `cold_vector`, `cold_vector_llm`, `degraded_vector_fallback`.
- Regular production/preview Search omits `execution_mode`; the normal LLM-verifier defaults remain unchanged.
- Each accepted gesture creates one request body with a UUID `client_request_id`; canary requests add the exact `execution_mode` expected by the server.
- Vector-only modes retain the explicit legacy `use_llm_verifier: false` guard. Pagination reuses the first-page mode and creates a fresh request UUID.
- Added stable lifecycle/terminal markers plus opaque response/card/family IDs. No query or card text was added to evidence markers.
- Validation terminates before request invocation and yields no POST body.

## Evidence and commands

- `node --test tests/search-*.test.mjs` — PASS, 30/30.
- `node --test tests/search-execution-variant.test.mjs tests/search-progress-button.test.mjs tests/search-recovery.test.mjs tests/search-initial-state.test.mjs` — PASS, 15/15 after final test changes.
- Secret-candidate-configured `astro build` compiled the client bundles and generated `/poisk/index.html`; the full unrelated route build later stopped with `ENOSPC` while writing an event page.
- `git diff --check` — PASS.

## Risks / constraints

- Full-site Astro completion is blocked by environment capacity, not by a Search compile error: `/dev/vda2` reports zero user-available bytes and `/dev/shm` has limited capacity. Generated build output was removed.
- The server must enforce the canary persona for explicit `execution_mode`; the UI deliberately provides no authorization claim of its own.

## Changed files

- `site/src/components/AuthorizedEventSearch.astro`
- `site/tests/search-execution-variant.test.mjs`
- `site/tests/search-progress-button.test.mjs`
- `.codex/lanes/L6-ui-variant/RESULTS.md`
