# ChatGPT window: finish product icon-action composites (implementation, not census)

Work independently in GitHub. Do not delegate to Codex, DevCoveer, another agent,
or the owner. This is a long-running implementation turn: census is already done
and is not a deliverable.

## Fresh state

Repository: `onedayonemasterpiece/events-bot-new`.

Sole executable trunk: `agent/static-site-single-kaggle-contract`.
Checkpoint when this brief was written:
`a01fb4dd330e756e8ae4adf7249a7dade39e7995`; resolve the current remote head
again before editing.

Continue the existing branch only:
`work/ui-normalization-product-icon-composites-20260904`.
Its current `09f0638f5...` tip is rejected as workflow-only: it contains no
product source. Rebase/recreate that same branch from current trunk; do not open
a second branch.

Product scope excludes `/lab/**` and the `/__preview/` directory page.

## Already-established factual census

The workflow-only run mixed markup, CSS selectors and lab-only source. The real
product-rendered event-like heart/count owners are:

1. `site/src/components/EventCard.astro`;
2. `site/src/components/DesktopEventActionPanel.astro`;
3. `site/src/components/EventHero.astro`;
4. `site/src/components/listings/MobileListingRailRow.astro`.

Do not conflate these semantically distinct cases:

- `ExhibitionPrototypeRow`: exhibition like/count/optional-label action;
- `EventParticipants`: participant entity like action;
- `ListingEventCard`: non-interactive social-proof display;
- festival route: festival save/instruction behavior;
- `EventCtaPanel`: lab-only;
- `KeyboardEventNavigationPrototype`: selector/runtime fixture, not a product
  markup owner;
- `EventLayout`: runtime wiring/factory, not a second static visual owner.

## Product result

Deliver actual source implementation on the same branch:

- one canonical event feedback icon+count composite used by every compatible
  product-rendered event-like owner above;
- one canonical heart glyph identity through `SemanticIcon`/the existing icon
  SoT;
- only the existing four icon-size roles (`16/20/24/32` mapping); no new local
  size;
- target at least `44x44` wherever interactive;
- optional visible label and count are variants/props of the same anatomy, not
  copied markup;
- preserve existing event id/title/base count, `data-feedback-action`,
  `aria-pressed`, accessible names, focus state, consent replay, persistence,
  client-created/hydrated EventCards and event action ordering;
- preserve intentionally different exhibition/participant/festival/social-proof
  semantics unless a separate common primitive is proven behaviorally exact.

Prefer a narrow Astro component plus existing runtime hooks over a framework or
new registry. Do not redesign palette, geometry, wording or action policy.

## Required execution sequence

1. Fresh-read current trunk, issue #621 after comment `5543954739`, current
   source and existing tests.
2. Replace the workflow-only branch base with the current trunk and keep one
   coherent branch.
3. Add a failing source/DOM contract first.
4. Implement the smallest compatible composite and migrate all four real
   event-like owners in the same batch. If one owner is not behaviorally
   compatible, retain it and document the exact semantic mismatch rather than
   forcing it.
5. Run focused Node tests for EventCard, desktop event actions, runtime-created
   EventCards, consent/feedback state and iconography.
6. Run production-surface, Astro-family, iconography and token-impact checks;
   regenerate committed graphs only when source truth requires it.
7. Run local browser evidence at `390x844` and `1440x900` on product routes
   covering listing card, event hero/action panel and a client-created card.
   Assert accessible identity, glyph identity, count/state update, >=44px
   target, no horizontal overflow and no page errors.
8. Update the one canonical design-system document and `CHANGELOG.md`.
9. Self-review the complete diff, commit, push, and publish one merge-ready
   result to #621 with exact head/base, changed product owners, test exits and
   browser evidence.
10. Fresh-read once more and continue fixing any failure caused by this batch;
    do not finish on a workflow or census result.

## Hard rejection conditions

- workflow-only or report-only output;
- lab work counted as product progress;
- copied raw SVG or component-local icon size;
- semantics collapsed across event/exhibition/participant/festival entities;
- removed runtime hooks, consent replay, persistence or accessibility state;
- new palette or visual redesign;
- no docs/changelog/tests/browser evidence.
