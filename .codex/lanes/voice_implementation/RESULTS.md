# Voice implementation lane — 2026-09-06

## Source and ownership

- Base: `d1e845cde434ff247df1662ae8b2d5044fabf11c`, actual PR #587 HEAD (already
  includes `8f349f0fa` connected voice implementation, not only 49-test kernel).
- Source fix: `b54c4a622daaf58956f8d9be5268a58d03a2bef2`.
- Branch: `agent/voice-search/implementation`; dedicated worktree
  `/home/dev/.codex/worktrees/events-bot-new/voice-search-implementation`.
- No push, migration, runtime deployment, provider call, phone mutation or root
  promotion performed by this lane. Parent owns integration/publication.

## Actual corrections

1. Serialize local failed-audio retries and deduplicate frame accounting;
   preserve `captureComplete` separately from durable completeness; update
   IndexedDB recording receipt after retry. Interrupted/background/device or
   storage-auto-stop audio remains partial. A verified user-stop tail can become
   saved after the local durable write succeeds. Concurrent retry tests cover
   one same Promise and no duplicate persisted frames.
2. First signed-out snapshot now displays existing Search sign-in guidance;
   no second Auth and no automatic submit.
3. ASR uses bounded, server-owned public place vocabulary, version
   `kenigevents-regional-places-v1`, stamped in durable outcome. Hints are data,
   only acoustically/contextually compatible and never forced replacement.
4. Bounded candidate membership is explicitly incomplete. Retain Search
   `has_more`; explain UI scope and distinction between subset and expansion.
   This is truthful scope disclosure, **not full-universe pagination**.
5. Full Chromium native harness uses correct `microphone` permission descriptor;
   default headless shell was not a working media implementation here.
6. Canonical voice doc, scenario registry, E2E index and CHANGELOG synchronized;
   obsolete 49-helper-only module README now routes to current authority.

## Reuse inspected

- wonderful-lections `3b345d078061c82f458fa502726c7adabcfdd203`,
  `src/presentation/review-feedback.mjs`: glossary-as-untrusted-data, no added
  requirements, preserve negation/conditions/uncertainty; `reviews.mjs` serialization.
- record-idea-hub `294c3485f377570505800516e2e86e58a6141781`,
  `EfficientVad.kt`: native VAD fail-open logic. No Kotlin or energy threshold
  transplanted to browser; continuous foreground capture retained.
- my-data-hub pinned deployed source `12c330a96e5db7d781a9283d38f6bc0069d8f89d`,
  `/home/dev/.local/opt/my-data-hub-control-plane/pr39-source`,
  `voice_intake_v2/inference.py`: aggregate full-utterance transcription,
  acoustic compatibility of terminology, checkpoint/accounting boundary.
- No private donor terms/utterances, owner-only auth, private `_generate`,
  IdeaHub publisher or raw-key provider client copied.

## Verified local checks

- `node --experimental-strip-types --test site/tests/assistant-*.test.mjs`:
  **80 PASS, 0 fail/skip** (78 baseline + vocabulary/limited-membership regressions).
- TypeScript **5.8.3** strict browser/server assistant module check: PASS.
  Existing pinned cached compiler used after `npm exec` had fetched it:
  `node /home/dev/.npm/_npx/587588907e7c3318/node_modules/typescript/bin/tsc
  --noEmit --strict --skipLibCheck --allowJs --target es2023 --module esnext
  --moduleResolution bundler --lib es2023,dom,dom.iterable
  --allowImportingTsExtensions site/src/lib/assistant/*.ts
  supabase/functions/event-search/assistant-*.ts`.
- `PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=/home/dev/.cache/ms-playwright/chromium-1200/chrome-linux64/chrome
  node --test site/tests/voice-browser.integration.mjs`: **5 PASS, 0 fail/skip**.
  Native AudioWorklet/IndexedDB with synthetic media device: stop/contiguous WAV,
  reload and owner isolation, deny, pagination, duplicate retry/partial truth,
  user-stop tail durable recovery. **Not physical microphone, ASR or phone.**
- Provider path offline audit: PASS `unapproved=0 allowlisted_debt=0`.
- Search generated revision check: PASS
  `sha256:47ec3f8c3b635972f8b8790ad94d78b9c31cbbb00936666c0763f99d9930be2f`.
- Existing design-system production surface source contract: PASS.
- Scenario registry YAML parse and `git diff --check`: PASS.
- Local preview build: 436 pages, existing build:preview path. Local artifact
  is **not uploaded/public**. Preview generated from historical fixture corpus;
  no current catalogue claim.
- Render smoke at 390/1440 px: one inline assistant, signed-out record disabled,
  no document horizontal overflow and no page errors after serving actual MIME
  types. Screenshots inspected; no new sticky shell was added. Authenticated
  card/refinement UI still requires permitted live account/runtime.

Logs/screenshots (ignored, not committed): `artifacts/codex/voice-source/`:
`unit.log`, `browser.log`, `typecheck.log`, `provider-audit.log`,
`design-system.log`, `build.log`, `search-390.png`, `search-1440.png`.
Native browser issue diagnosis used official CDP Browser.setPermission contract
and MDN getUserMedia: headless_shell1228 returned `NotSupportedError`; full
Chrome1200 succeeded. No fake success or microphone API replacement was used.

## Remaining parent integration gates (not PASS)

- Confirm real permitted assistant policy/model, ordinary-user allowlist,
  staging schema/RLS and route deployment through existing services; never
  invent a POLICY_REF or enable production roots.
- Whole `event-search/index.ts` Deno check, deployed DB grants/CAS, live direct
  and relay real-size upload/timeout/429/ACK-loss checks.
- Full Search logical membership beyond current 60-candidate window; dynamic
  shared capacity/fairness; long-audio continuation beyond explicit byte bound.
- Real ASR quality from independent frozen spoken expectations, including short
  negation, place names, numbers, correction and midnight Kaliningrad anchor.
- Actual profile/global hide/saved receipt and analytics/test sink aggregate.
  Local preview measurement remains NOT an external analytics sink.
- Current islands adapter integration and actual authenticated mobile keyboard/
  stop geometry. Never duplicate shell or change parallel island owners.
- Publish exact-commit HTTPS noindex preview through existing builder/bucket,
  then actual authorized OpenCode/ADB PWA tests preserving session/audio/queues.
  No connected phone/policy environment was supplied to this lane.


## Follow-up integration/read-only discovery

Original source+handoff commits were fast-forward pushed to the existing PR587
branch and GitHub/ls-remote read back `b92b55d6b` exactly. Follow-up canonical
runtime/policy/phone evidence and minimal next steps are in
`docs/features/static-site-pages/smart-vector-search/20260906-voice-prototype-codex.md`
under **Read-only integration discovery**. No new provider/live pass is claimed.
Current shared event-search v79 exists, assistant schema/secrets do not;
existing CLI management credential works, stale project env PAT does not.
No cloud/DB writes or device actions occurred. Static prefix publication and
shared production Edge/DB mutation are explicitly distinguished.
