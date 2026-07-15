Your first report is directionally useful but too compressed for the requested research gate and omits major deliverables. Produce a **substantive Part II in Russian**, without repeating generic conclusions. Correct these issues explicitly:

1. This project is **Astro SSG/static HTML**, not Astro SSR. The global manifest is a static/versioned artifact on the current Yandex Object Storage/CDN publication path; do not assume Cloudflare or Vercel. Personalization is progressive client-side enhancement.
2. Do not recommend “tickets are running out” unless a source-grounded ticket-status field exists. Do not invent an event example such as “Эрмитаж-Урал”; use placeholders/tokens or clearly illustrative Kaliningrad-category wording.
3. Do not use `[⏸]` as an unlabeled icon recommendation. Specify visible/accessible control names and keyboard/touch behavior.
4. Do not prescribe hidden duplicate text plus an automatically changing `aria-live` node without addressing duplicate announcements. Give a safer accessible DOM/announcement model.
5. Use `pointerdown`, not a touch-only event contract, unless explaining an explicit fallback.

The missing detail must include all of the following:

## A. Scenario contract and copy library
Create a detailed table covering **at least 18 scenario families**. Each row must include:
- stable scenario ID;
- visitor state;
- exact eligibility facts and provenance;
- priority and mutual exclusions;
- safe wording rule;
- forbidden claim/example;
- 2–4 complete friendly Russian 1–3-line messages (minimum 45 message variants total), each with at most two link tokens in a machine-safe notation such as `{{link:event:123|Название}}` or `{{link:route:/vyhodnye/|события выходных}}`;
- target/deep link;
- placement (scene 1 / later / manual only);
- cooldown/frequency cap.

Must cover: first anonymous, returning anonymous, authenticated, high-frequency revisit with zero changes, many changes, explicit favorite category, inferred affinity, saved organizer, saved event approaching, today/tonight/tomorrow/weekend, family, exhibitions, concerts, theatre, lectures, free, Pushkin card, verified popular, human editorial selection, newly added, diversification/serendipity, sparse catalog, missing profile, stale manifest, offline/error, no safe recommendation, already viewed, explicitly dismissed.

Give an assembly algorithm with deterministic exclusions and tie-breakers. Address multi-tab `last_visit`, back-forward cache, repeated sessions, local time, viewed-vs-dismissed semantics, and filter-bubble control.

## B. Precise layout/wireframes
For 1440×900, 1366×768, 390×844, 360×800, and 320×568 give a pixel budget table that clearly distinguishes:
- global header;
- briefing component height;
- categories/date shortcuts outside it;
- visible beginning of feed.
Resolve the ambiguity of “hero ≤50%”: state whether the header counts or not and define the acceptance formula.
Provide text wireframes for desktop, normal mobile, and 320×568 fallback. Specify line clamps, font-size ranges, spacing, control placement, and what gets cut first.

## C. Visual design critique
Develop the 3 directions into implementable mini-specs: type scale, max line length, color roles, grid/alignment, surface treatment, relationship to the existing warm paper/graphite/terracotta brand, role of the wide «О», and mobile adaptation. Score each on brand fit, readability, novelty durability, implementation risk, and banner-blindness risk. Select one and give exact reasons.

## D. Motion state machine and accessible DOM model
Give a full transition table for `static_ssg`, `hydrating`, `entering`, `reading`, `paused_auto`, `paused_user`, `manual`, `exhausted`, `stale`, `error`, `reduced_motion`, and `document_hidden`:
- entry trigger;
- allowed exits;
- timer behavior;
- DOM/visual behavior;
- analytics event;
- accessible announcement behavior.
Specify desktop/mobile/reduced-motion timings and a strict rule for hover, `focusin/focusout`, pointerdown, scroll, visibilitychange, link click, browser back, and resize/orientation change. Resolve whether pointer interruption completes in <50 ms or 100–150 ms. Explain reserved geometry/CLS and fixed link hitboxes.

## E. Gemini Lite “lollipop” prompt family and schema
Give a versioned input/output schema with:
- scenario ID;
- immutable fact IDs/values/provenance timestamps;
- claim allowlist;
- tokenized links (no URLs/HTML);
- explicit vs inferred personalization evidence;
- locale and grammatical constraints;
- output fragments and validation metadata.
Split into small prompt stages (eligibility is deterministic; phrasing; compression; safety audit) and provide concrete prompt templates. List hard validators, canonicalization rules, fail-closed logic, deterministic fallbacks, cache keys, invalidation triggers, and editorial overrides.

## F. Experiment plan beyond hero CTR
Give:
- exact hypotheses for A=no briefing/categories-first, B=static briefing, C=semantic-fragment motion, D=literal typewriter diagnostic;
- primary metric that represents downstream discovery quality, not just briefing CTR;
- secondary and guardrail metrics;
- event taxonomy/payload examples;
- segmentation and novelty-decay analysis;
- qualitative study script;
- sample-size/MDE caveats without inventing traffic levels;
- stop/ship/iterate thresholds;
- a11y, reduced-motion, low-end Android, slow-network, JS-off, and corrupted-cache acceptance checks.

## G. Red-team risk register
Provide at least 20 risks with severity, likelihood, detectability, mitigation, owner, and whether each blocks prototype, experiment, or rollout. Distinguish evidence vs hypothesis.

End Part II with a prioritized list of open decisions and the exact prototype artifacts that must exist before implementation can be approved.
