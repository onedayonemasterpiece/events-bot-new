# Home inline capture → shared Search handoff

Implementation contract for the home final assembly. Home does not own ASR, providers,
search results, history, personalization or a floating composer. `HomeSearchEntry.astro`
is a no-props inline Field/Button presentation of the existing #587 capture and store.
`homeSearchCapability(import.meta.env)` is the shared SSR gate for the entry and page-end CTA.

## Identity and durability

- Same existing IndexedDB `kenigevents-voice-v1`, existing `commands`, `recordings`,
  audio parts and `conversations` stores; no schema upgrade, new storage engine or deletion.
- A scoped `VoiceStore` encodes the authenticated owner plus a SHA-256 origin/base-prefix
  namespace into its existing owner keys. Unscoped legacy recordings are retained, not
  silently adopted into a different preview. Auth logout never clears these stores.
- Explicit submit allocates an opaque handoff ID, new task ID, interpretation ID, search ID
  and ASR ID once. The versioned, owner-bound payload and submitted time have a 24-hour
  admission TTL. Only the opaque ID travels in `withBase('/poisk/')?voice_handoff=…`.
  No transcript, raw audio, Blob URL, profile or credential appears in the URL.
- Capture's acknowledged worklet flush, PCM/container saves, track release, context close,
  recording finalization and handoff strict IDB transaction + readback precede navigation.
  A completed silence endpoint saves only; the user explicitly sends it. Empty input,
  energy-silent recording and cancel do not enter search. Energy evidence is deliberately
  conservative, not an assertion of semantic speech; an empty ASR transcript also stops search.
- `adoptHandoff` atomically persists the new task epoch, accepted command and adoption
  receipt. It never calls ordinary `submit()` on mount, never appends into a pending older
  task and never deletes the transfer before provider processing. Reload/Back reuse IDs.
- Receiver ASR uses the existing client and source-first audio manifest; saved transcripts
  are reused. Existing server receipts remain the provider-dispatch authority; browser
  Web Locks reduce duplicate transport but are not the idempotency guarantee.
- The handoff receipt retains `prepared`, `adopted`, `completed`, `empty` or `cancelled`.
  Lost ACK/reload reconciles the existing command rather than allocating another request.
  A later explicit task wins over an old Back link.

## Lifecycle and recovery

`data-home-search-state`: `disabled`, `signed-out`, `idle`, `requesting`, `recording`,
`saving`, `submitted`, `error`. `data-search-enabled` additionally requires current shared
Auth owner and usable storage. No login action submits a draft or starts the microphone.
Owner-scoped text/recording drafts restore for explicit continuation. A prior submitted
handoff offers an explicit link to its existing Search receipt; it is not auto-resubmitted
on home. Page-end CTA suppresses unavailable, capture, save and submitted states.

Repeated mounts share one adapter; pagehide releases capture/unsubscribes and BFCache
pageshow restores the adapter without starting capture. Failed local PCM saves remain in
capture memory and can retry via the existing `retryUnsaved` before leaving; partial
recordings remain persisted. Do not claim unsaved bytes survive a forced browser kill.
Denied mic allows text input. Backend/offline failure leaves the handoff and recording
on the device; reopening the submitted URL resumes the same receipt. No reset/clear path.

Portable domain modules copied verbatim from #587 live once under `site/src/lib/assistant/domain`, so the canonical site-only builder archive has a complete import closure. The previous `supabase/functions/event-search/assistant-*` paths are thin compatibility re-exports, not a second handwritten domain implementation.

## Integration boundaries

- Homepage owner imports `<HomeSearchEntry />` immediately after Hero-talk.
- `/poisk` nests `<ConversationalSearch />` in the additive `AuthorizedEventSearch` slot;
  ordinary Search code, Auth singleton and public Search URLs are unchanged.
- EventLayout owner supplies the existing SearchCardHost feedback/actions bridge so
  conversational results use ordinary card rendering/actions, not a second card family.
- Runner owner forwards existing public assistant enable/host/capture-only flags. Only
  approved non-production preview config enables this path; no backend deployment here.

## Verification

`site/tests/home-search-handoff.browser.test.mjs` uses real Chromium documents, real IDB
and the real store/controller/receiver with explicitly mocked Auth, capture and transport.
It checks text and audio transfer, release-before-navigation, duplicate mount, reload,
Back-equivalent revisits, explicit draft restoration, login-not-submit, cancellation,
silence, owner/prefix isolation, new-task separation and a lost ACK after mock execution.
This is integration evidence, **not live microphone, ASR or phone acceptance**.

`home-search-contract.test.mjs` checks scope/version/TTL/opaque URL, public capability
and compilation of the actual Astro entry/receiver/Auth-slot composition. The copied
shared assistant tests retain capture/compression/streaming/worklet/dialogue coverage.
Physical-phone microphone, real Auth/ASR/provider checks, rendered final home composition
and published preview acceptance belong to the integration owner; no full build or
production promotion is authorized by this lane.

## Home review: navigation-only launcher (v2)

The home review uses the existing `floating-link` variant without starting capture on
home. It preserves `inline-capture` and its durable handoff contract for consumers
that explicitly choose it. The floating link now displays the **unchanged** microphone
SVG from `ConversationalSearch.astro` (26px outline microphone), verified against the
published `/preview-voice-plan-20260906/poisk/` HTML on 2026-09-06; it is not the lower
navigation magnifying-glass icon and introduces no new icon geometry.

The launcher resolves `/poisk/` with `mobileDiscoveryHref`, `BASE_PATH` and the same
`PUBLIC_MOBILE_SEARCH_BASE_URL` used by the existing mobile discovery navigation.
The integration review config must set that base to
`https://kenigevents.ru/preview-voice-plan-20260906`, so it opens the approved Search
prototype rather than the old bundled Search page. No hostname/prefix is hardcoded in
the component; normal local-prefix fallback remains available outside that review.
The launcher is a link: activation navigates but does not request microphone permission.

`HomeQuickNav` v2 variant `rectangular-grid` consumes shared `Button secondary` controls,
uses the existing control-radius role, fills each cell, and lays out two equal columns
on mobile and three from 768px. Available collection filtering and existing destination
paths remain unchanged. Parent integration owns registry/CHANGELOG synchronization and
final page publication. Narrow regression test: `site/tests/home-voice-review.test.mjs`.
