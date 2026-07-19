---
name: keyboard-interface-navigation
description: Design, implement, review, or test keyboard-first web UI navigation and hotkeys. Use for physical-key shortcuts across keyboard layouts, arrow navigation in card grids, focus recovery after rerenders, modal or overlay focus return, visible shortcut hints, accessible command routing, key-repeat behavior, or privacy-conscious shortcut learning and usage telemetry.
---

# Keyboard Interface Navigation

Build keyboard behavior as an explicit interaction system rather than scattered `keydown` handlers. Preserve browser, form, and assistive-technology behavior unless a command is valid in the current scope.

## Establish ownership and contracts

1. Record requirement IDs, writable files, forbidden files, and done-when checks before editing.
2. Map existing focus management, overlays, routing, card rendering, analytics, and keyboard handlers. Reuse the product's established primitives.
3. Define a command table before implementation. For each command, state:
   - physical chord (`KeyboardEvent.code` plus modifiers);
   - user-facing label;
   - eligible scope and blocking scopes;
   - repeat policy;
   - focus/result behavior;
   - telemetry action ID, if any.
4. Keep command resolution, command execution, focus policy, display metadata, and usage recording separable and testable.

## Route key events safely

Install one scoped router at the narrowest stable owner. Let the active topmost modal or overlay handle an event before the underlying page.

- Match layout-independent physical shortcuts with `event.code`, not character-producing `event.key`. For example, a physical `KeyK` command remains on the same key under Latin and Cyrillic layouts.
- Use `event.key` only when semantic value is intended, such as `Escape`, `Enter`, or arrow-key behavior. Do not infer a physical key from localized characters.
- Treat modifiers explicitly. Reject unexpected `Ctrl`, `Meta`, `Alt`, `Shift`, and `AltGraph`; never shadow browser, OS, or assistive-technology chords accidentally.
- Ignore events during IME composition (`event.isComposing` or composition sentinel values).
- Ignore character shortcuts from editable targets: `input`, `textarea`, `select`, contenteditable elements, and product-specific editors. Permit only deliberately documented editor commands.
- Preserve native `Tab` order. Prefer real `button`, `a`, form controls, `dialog`, and platform activation semantics over recreating them on generic elements.
- Call `preventDefault()` and `stopPropagation()` only after a command is recognized, allowed, and executed. Do not cancel unknown or disabled chords.
- Make every command available without a shortcut. Keyboard acceleration must not be the sole path to an action.

Keep a command result such as `{ handled, effect }` so tests can prove when native behavior remains untouched.

## Manage focus as durable state

Represent logical focus with a stable item ID plus a containing surface ID; do not persist a DOM node or array index across rerenders.

### Recover focus loss

- Before an update, retain the logical focus identity when the active element belongs to the managed surface.
- After rendering, resolve that identity to the current element. If it vanished, choose the documented deterministic fallback: nearest surviving neighbor, surface heading/control, then container.
- Restore focus automatically only after a managed transition. Separately, define an explicit lost-focus re-entry command when product scope is unambiguous: execute only on the user's recognized keypress, prefer a hovered managed item, then a still-visible logical owner, then the page surface. Never re-enter from editors, dialogs, browser chrome transitions, or assistive-technology movement.
- Clear or suspend held-key state on `window.blur`, hidden `visibilitychange`, surface unmount, route change, and overlay transfer.

### Enter and leave overlays

1. Save the logical opener when opening a modal, drawer, popover, or command palette.
2. Move focus to the first meaningful control, a validated initial target, or the overlay itself.
3. Trap focus only for modal layers. Ensure only the top layer receives commands.
4. Close on `Escape` when the product contract permits it.
5. Restore focus to the still-connected opener. Otherwise resolve its logical ID; if absent, use the nearest stable control, then the owning surface.

Test close by `Escape`, activation, outside click, navigation, and programmatic dismissal.

## Navigate cards as a graph

Use roving `tabindex` for ordinary focusable collections or `aria-activedescendant` when composite-widget semantics genuinely require it. Keep exactly one active item.

- Build the graph from currently visible, enabled cards; exclude hidden, inert, collapsed, and detached nodes.
- For uniform grids, use semantic row/column data. For responsive or masonry layouts, compare current DOM rectangles: filter candidates to the requested directional half-plane, rank by primary-axis distance and cross-axis deviation, and break ties by stable DOM order or ID.
- Define boundary behavior explicitly: stop, wrap within a row/column, or move to another region. Never let incidental DOM order decide.
- Rebuild or invalidate geometry after resize, filtering, pagination, lazy loading, and reorder. Re-resolve the active ID after every dynamic rerender.
- Move logical and DOM focus together, and scroll the target into view without unnecessary motion. Respect reduced-motion preferences.
- Keep arrow keys native outside the collection and inside editable descendants.

## Make repeat deterministic

Declare each command as `once`, `native-repeat`, or `controlled-repeat`.

- For `once`, ignore `event.repeat` and latch by physical `code` until `keyup`.
- For arrow-key repeat latches, either accept browser repeat events or run one controlled repeat timer; never combine both.
- Store latches per code so unrelated simultaneous keys do not release one another.
- Release on matching `keyup` and clear every latch/timer on blur, visibility loss, unmount, route transition, and overlay ownership change.
- Test rapid press/release, long hold, multiple arrows, focus loss while held, and return to the page. A lost `keyup` must not leave navigation stuck.

## Expose shortcuts accessibly

Derive behavior, help text, tooltips, and keycaps from the same command metadata.

- Show visible keycaps near important actions or in a discoverable shortcut help surface. Render platform-appropriate modifier names and a label meaningful for the user's layout; do not leak `KeyK` as UI copy.
- Add concise `title` text only as a supplementary pointer hint, not the only documentation.
- Add valid `aria-keyshortcuts` tokens to the actionable element. Keep them synchronized with enabled commands and do not claim unsupported alternatives.
- Preserve visible focus indication, contrast, target size, zoom/reflow, screen-reader names, high-contrast modes, and `prefers-reduced-motion`.
- Announce mode or result changes only when necessary; avoid noisy live regions for every arrow movement.

## Learn without surprising or surveilling

Make situational learning improve discovery, never secretly change control meaning.

- Keep the documented key map stable. Use recent successful actions to rank shortcut hints or suggest a command in relevant surfaces; require explicit confirmation for any remap.
- Apply minimum evidence and cooldowns so one accidental use does not produce persistent UI churn. Provide dismiss, opt-out, and reset controls.
- Prefer on-device aggregation. If daily usage facts must be stored or sent, default to one boolean fact per subject/day/action ID with set semantics; omit counts, route, surface, and object identity unless a separately reviewed decision requires them. Derive subject and day server-side, deduplicate on write, and use short documented retention.
- Do not collect raw key streams, typed content, search terms, card titles, full URLs, DOM text, precise event timestamps, clipboard data, or focus trails. Do not use shortcut facts for advertising or unrelated profiling.
- Record only successful command execution, never every keydown. Treat accessibility settings and failed input as especially sensitive.
- Verify consent, deletion, retention, and data-contract requirements before adding remote telemetry.

## Validate incrementally

Add unit tests around pure command matching, modifier/editor exclusions, graph selection, fallback selection, repeat state, and telemetry aggregation. Add integration tests for focus and rerender behavior. Use real-browser tests because synthetic DOM environments do not reproduce focus, layout rectangles, native repeat, or visibility transitions fully.

Test at least one non-Latin layout by constructing events whose `code` is stable while `key` differs. Exercise mouse/touch and screen-reader paths to ensure keyboard work did not regress them.

## Acceptance matrix

Complete the applicable rows and preserve evidence in the task report.

| Area | Required acceptance |
| --- | --- |
| Ownership | Diff changes only declared files; requirements map to tests and documentation. |
| Layouts | Physical hotkeys use `code`; Latin/non-Latin cases behave identically; displayed labels remain understandable. |
| Native behavior | Tab, editing, IME, browser/OS chords, links, buttons, and unknown keys retain native behavior. |
| Focus recovery | Focus survives rerender/reorder; removed targets use the documented fallback without stealing intentional focus. |
| Overlays | Top layer owns input; initial focus, trap where modal, `Escape`, and opener/fallback restoration work. |
| Card graph | All directions, edges, disabled/hidden cards, responsive layouts, resize, and dynamic updates are deterministic. |
| Repeat | Tap, hold, diagonal/multiple keys, lost `keyup`, blur, visibility loss, and unmount leave no stuck latch or duplicate loop. |
| Discoverability | Visible keycaps, help text, `title`, and `aria-keyshortcuts` match the executable command registry. |
| Accessibility | Non-shortcut path, visible focus, zoom/reflow, high contrast, reduced motion, and assistive technology remain usable. |
| Learning/privacy | Suggestions are contextual and dismissible; mappings stay stable; only successful, coarse daily facts are retained and resettable. |
| Regression | Unit, integration, and real-browser checks pass on supported browsers and input layouts. |

Report untested rows, platform assumptions, and residual risks explicitly; do not infer acceptance from a single happy-path keypress.
