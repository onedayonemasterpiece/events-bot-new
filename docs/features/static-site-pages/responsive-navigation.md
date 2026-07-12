# Responsive navigation decision for the public static site

> Status: **release direction selected; visual parameters and owner sign-off remain open**. Canonical owner: F5 release UI. This document replaces informal desktop/mobile navigation assumptions; historical hero labs are evidence only.

## Product decision

Desktop and mobile must preserve the **same navigation mental model**, but they do not need pixel-identical geometry or the same disclosure mechanic.

The release direction is an adaptive hybrid:

- **Mobile/compact:** retain the stronger terracotta brand tag/handle already explored in the event-hero lab. It may open the compact discovery/navigation sheet because all destinations cannot remain visible without crowding.
- **Desktop/large:** keep the primary destinations persistently visible in one horizontal top navigation bar. Integrate a restrained, shallow version of the brand tag into that header so the family resemblance remains, but do not copy the deep mobile protrusion or hide the ordinary desktop navigation behind it.
- **Tablet/medium:** choose the least crowded of the two modes from measured available width, not from user-agent/device labels. It may use the compact disclosure pattern or a reduced horizontal row, but it keeps the same destination order and labels.
- Never show two competing primary navigation systems at once. A desktop tag is brand treatment, not a second drawer trigger when the full primary row is already visible.

The invariant across breakpoints and every generated HTML page is:

1. the same primary destinations, labels and relative order;
2. the same active/current-location semantics;
3. the same search and global identity/account actions;
4. consistent accessible names, focus behavior and brand vocabulary;
5. static/no-JS access to the primary destinations.

The geometry may adapt to available space: horizontal desktop row versus compact mobile disclosure. This is intentional responsive behavior, not inconsistency.

## Research basis

The decision does **not** assume that visitors use only mobile or only desktop. There is no project evidence that justifies that assumption, and cross-device use is a real acceptance case.

- A controlled cross-device menu study found that consistent item order improved post-transition performance, while changing the menu layout between horizontal desktop and vertical mobile did not significantly reduce it. That supports preserving information architecture/order rather than forcing the same shape: [International Journal of Human-Computer Studies, 2019](https://doi.org/10.1016/j.ijhcs.2019.06.001).
- Current Android adaptive-navigation guidance explicitly recommends choosing a navigation component for the window-size class and warns against keeping the same compact navigation component on large screens: [Android Developers — Adaptive navigation](https://developer.android.com/design/ui/mobile/guides/layout-and-content/layout-and-nav-patterns#adaptive-navigation).
- WCAG 2.2 requires repeated navigation to keep the same relative order and repeated functions to be identified consistently. It also treats responsive variants as part of the page that must conform; it does not require identical visual geometry at different breakpoints: [WCAG 2.2 — 3.2.3 and 3.2.4](https://www.w3.org/TR/WCAG22/#consistent-navigation), [WCAG 2.2 conformance for responsive variants](https://www.w3.org/TR/WCAG22/#cc5).

Therefore the release criterion is **conceptual and functional consistency plus breakpoint-appropriate presentation**. Device switching remains supported, but desktop does not inherit a mobile compromise merely for visual sameness.

## Release prototype and decision gate

The UI-freeze task must produce one immutable preview build containing these desktop comparison routes with identical content:

- **A — plain bar:** ordinary persistent horizontal top navigation without the tag motif;
- **B — shallow hybrid (recommended):** persistent horizontal navigation plus a restrained brand tab/tag integrated into the header;
- **C — pronounced tag:** a mobile-like protruding tag on desktop, retained only as a negative/control candidate if it does not obscure content.

The project owner signs off one exact variant. B is the default recommendation unless evidence shows that its brand treatment hurts navigation or event-content scanability.

Acceptance at `375`, `768`, `1366` and `1440` CSS px:

- every primary destination is findable without an unnecessary extra action on desktop;
- navigation labels/order and current-page indication stay consistent across page families and breakpoints;
- the tag/header never covers the hero, title, focus ring or browser zoomed content and causes no horizontal overflow or layout shift;
- keyboard order is logical; `Esc`, outside-click and focus return work for any disclosed menu; `prefers-reduced-motion` and no-JS remain usable;
- identity/search controls retain the same meaning and account state on all pages;
- at least one test session deliberately moves through the same find-event/search/account tasks on mobile and desktop; record first-click success, wrong destination, completion time and qualitative confusion rather than asking only which mockup “looks better”.

The release plan must not decide from aggregate device share alone. Analytics can choose the canary emphasis, but both responsive variants remain release surfaces.

## Relationship to earlier UI work

The mobile tag/top-sheet work in [the 2026-06-27 event hero lab](event-hero-lab-2026-06-27.md) is the starting visual evidence, not permission to copy its geometry to desktop. The final reusable component belongs in the common release layout described by [the release UI contract](release-ui-contract.md), so listings, event pages, search, tags and personal-secret pages cannot drift.
