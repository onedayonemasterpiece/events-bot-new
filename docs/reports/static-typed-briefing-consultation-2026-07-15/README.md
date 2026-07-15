# Typed briefing consultation evidence

> **Evidence status:** committed research provenance, not a production contract.
> **Consultant:** Antigravity/agy display model `Gemini 3.1 Pro (High)` through the local `a-gemini` wrapper.
> **Runs:** 2026-07-15 08:18:32–08:20:34 UTC and 08:21:26–08:22:49 UTC; both exited with status `0`.
> **Thread/session:** agy brain `e01102a9-57e2-4b1d-ba48-35f271156ffd`; Part II continued Part I and is not an independent review.
> **Tooling:** `/home/dev/.local/bin/a-gemini` over agy `1.1.2`; display alias `Gemini 3.1 Pro (High)`. The exact provider model ID and sampling parameters were not exposed by the wrapper.
> **Invocation:** `--print-timeout 20m`; Part II additionally used `--continue`.
> **Input repository SHA:** `926dad8a91fc7f1070126d32a05281aa92ff1666` (`origin/main` at consultation start).

## Committed evidence

| File | Role | SHA-256 |
|---|---|---|
| [`prompt-v1.md`](prompt-v1.md) | Original product/design/motion/scenario consultation prompt with supplied prior analyses appended. | `0b9be70dca677b97f39a0b462f860f33d42f0462ce01ef11a350687f85d7920c` |
| [`gemini-part1.md`](gemini-part1.md) | Full primary consultant document saved from the agy brain output. | `6ad4b2f632dca8a8a500d873f4eb7b7c71e6a10657f4f49d26f34cb47f45de9e` |
| [`prompt-v2.md`](prompt-v2.md) | Corrective follow-up requesting missing depth and correcting SSG/provider assumptions. | `5c16cb06a87f331fbc8a4d29f4abd0cc8e2758ce0173220ea7dd3a37654318af` |
| [`gemini-part2.md`](gemini-part2.md) | Full follow-up consultant document. | `8a8124b65d2af816cadc3c70ed30084d0839433b321d39a3039b03cd8e045be8` |

The hashes above are for the committed byte-for-byte copies and can be checked with:

```bash
sha256sum docs/reports/static-typed-briefing-consultation-2026-07-15/{prompt-v1.md,gemini-part1.md,prompt-v2.md,gemini-part2.md}
```

The local run directory remains ignored as operational evidence:

```text
artifacts/codex/static-typed-intro-consultation-20260715/
```

The committed copies are the durable review surface; the ignored directory is not required to audit the product decision.

## Decision trace

### Accepted from Gemini

- Treat the surface as a navigational/editorial briefing, not an AI assistant.
- Keep the first useful text static-first and useful without JS.
- Prefer a flat editorial treatment over a terminal/video metaphor.
- User interaction interrupts motion immediately.
- Keep LLM out of the page-view path.

### Corrected or deferred

- The project is Astro **SSG**, not SSR, and publishes through Yandex Object Storage/CDN rather than Cloudflare/Vercel.
- The two runs are one correlated iterative consultation, not independent product validation.
- The consultant did not prove user desirability or metric impact.
- Its 33-scenario, personalization and Gemini Lite system is retained only as post-validation research, not MVP scope.
- The initial lab uses at most eight deterministic messages plus a neutral fallback, no Gemini Lite and no personalization.
- Mobile layout is governed by first-event visibility. `12–18svh` and `160px` are challenger hypotheses, not hard acceptance limits.

## External audit application

A later external audit correctly distinguished design completeness from product evidence. Its resulting decision is:

```text
desk_research_synthesis: complete
decision: GO_TO_PROTOTYPE_ONLY
product_desirability: unvalidated
user_validation: false
metric_validation: false
production_approval: no
```

The isolated `/lab/briefing/` prototype is the only approved implementation scope. Production routes, deploy, Gemini Lite, personalization, runtime APIs and wordmark animation remain excluded.

## Later external audit provenance

The corrective audit supplied by the user was read from an IDE attachment and is not copied into this report because it was not a consultant invocation made by this branch. Its source SHA-256 was `d0755feab8065e2ce5872c8c5d62e264fa307f157ba8587581489e0c405873d5`; requirements R01–R10 are resolved in the canonical feature document.
