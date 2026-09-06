# Conversation and audio core — first executable source slice

2026-09-06. **Implemented pure helpers, not a connected voice-search product.**

Implementation continuation: [ChatGPT prototype task](../../../../docs/features/static-site-pages/smart-vector-search/20260906-voice-prototype-chatgpt.md).
Product authority: [conversational Search](../../../../docs/features/static-site-pages/smart-vector-search/agent-assisted-event-discovery.md), [voice solution](../../../../docs/features/static-site-pages/smart-vector-search/voice-search-solution-v1.md).
Shared layout authority: [current Floating Island routing](../../../../docs/features/static-site-pages/design-system/floating-islands.md). The new top-row locator contract takes precedence over an old second sticky heading in an illustrative search design. This module owns no layout.

## Actual implementation

`conversationState.ts` implements immutable host-applied transitions for a bounded text/confirmed-transcript working set. It retains accepted input, enforces contiguous interpretation, rejects conflicting duplicate IDs and stale results, separates committed sections from the pending draft, supports explicit old-section refinement/expansion and projects current hides without mutating historical membership. IDs from a model cannot authorize results: the host supplies an independently validated eligible set. Neither that set nor this helper verifies factual prose or user identity.

`audioSegments.ts` encodes mono PCM16 WAV parts with the actual capture sample rate, continuous frame offsets and a caller-provided wire budget including base64/envelope overhead. It validates actual headers, size and continuity. It does not discard silent samples, resample, record the microphone, detect speech, upload, authenticate or invoke ASR. PCM16 encoding is quantized; “lossless-boundary” means no omitted or duplicated samples at segmentation boundaries, not mathematically lossless float-to-PCM conversion.

No provider SDK, credentials, new transport, database, profile, user-facing page, instrumentation collector or publishing route was added. No existing UI component, common island family or STATUS was edited.

## Verification

From the repository root:

```sh
node --experimental-strip-types --test site/tests/assistant-*.test.mjs
```

49 tests passed locally on Node 22.16.0; 0 failed, 0 skipped. They include explicit negative cases and a 1–50-addition loop with stale completion rejection. Pure audio input is synthetic PCM, not Qwen output or a human microphone recording. The two modules also passed a separate strict TypeScript 5.8.3 check:

```sh
tsc --noEmit --strict --target es2023 --module esnext --moduleResolution bundler --lib es2023,dom site/src/lib/assistant/*.ts
```

GitHub blob identities for all four source/test files matched the locally tested bytes. The existing `static-browser-release-gate` in `.github/workflows/ci.yaml` now runs the same test command. The workflow change is exactly two added lines; there is no new runner/workflow. A complete remote CI verdict is not claimed here.

## Do not mistake these helpers for finished infrastructure

- `State` is not a client persistence format. Its full JSON must not be written to the shared 64 KiB localStorage envelope. A durable store/receipts, owner checks, atomic CAS, compaction, paging and retention still need implementation.
- The current reduced Intent fields do not implement the full dates/timezone/audience/location schema. Expand them using the feature contract; do not drop unsupported conditions or pretend `goal` prose replaces typed filters.
- The helper rejects sequence gaps without acceptance. A durable intake may accept/store out-of-order uploads separately and feeds the core only once a contiguous prefix is available. Never drop an already acknowledged U2 because of a helper conflict.
- A revision conflict does not authorize another provider call. Re-read the durable base and safely apply the already stored interpretation only if its original predecessor/base is still valid.
- Current kernel memory ceilings are safety bounds for this slice, not the user’s product allowance. Add bounded persistence/paging before extending conversation length; never solve pressure by silently losing accepted input.
- A retrieval ticket protects retrieval/presenter completion. Capture, upload and ASR have their own stage IDs/receipts and failure transitions in the integration implementation.
- The host must validate current lifecycle, pricing, ownership, taxonomy and source evidence. Result membership passed to this helper is not an ASR or retrieval implementation.
- Server validation must independently validate the same audio contract and hashes. Client checks do not protect a public endpoint. WAV files cannot simply be byte-concatenated into one playable recording; decode/assemble data frames in order.

## Remaining delivery steps

Continue the linked ChatGPT implementation task through actual capture, API, durable state, existing limiter/retrieval/profile and honest UI, then HTTP/DB/browser/live tests. Reuse the inspected my-data-hub inference/checkpoint/media primitives without inheriting its owner credential, IdeaHub publisher or long-form prompt.

The main CHANGELOG and canonical scenario registry still require narrow updates during source integration; neither was overwritten with a partial file read. This source start is intentionally not declared release-ready until those updates and the relevant integration checks exist. The task document names this debt explicitly rather than inventing a second changelog or test registry.
