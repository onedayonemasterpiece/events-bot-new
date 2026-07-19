#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${STATIC_SITE_REVIEW_BASE_URL:-http://127.0.0.1:4321}"
ROUTE="${BASE_URL%/}/lab/keyboard-event-navigation/"
SESSION="keyboard-event-navigation-$$"
OUTPUT_DIR="${KEYBOARD_NAVIGATION_ARTIFACT_DIR:-../artifacts/codex/keyboard-event-navigation-prototype}"

command -v playwright-cli >/dev/null 2>&1 || {
  echo "playwright-cli is required" >&2
  exit 1
}

mkdir -p "$OUTPUT_DIR"
trap 'playwright-cli -s="$SESSION" close >/dev/null 2>&1 || true' EXIT

playwright-cli -s="$SESSION" open --browser=chromium "$ROUTE" >/dev/null
playwright-cli -s="$SESSION" resize 1536 864 >/dev/null
RUN_OUTPUT="$(playwright-cli -s="$SESSION" run-code 'async page => {
  const assert = (condition, message) => { if (!condition) throw new Error(message); };
  const surfaceSelector = "[data-keyboard-event-surface]";
  const cardSelector = "[data-related-start] [data-event-card]";
  await page.route("**/event.ics", (route) => route.fulfill({
    status: 200,
    contentType: "text/calendar",
    body: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n",
  }));
  await page.waitForSelector(surfaceSelector);
  await page.waitForTimeout(250);

  const activeState = () => page.evaluate(() => {
    const active = document.activeElement;
    const card = active?.closest?.("[data-event-card]");
    return {
      surface: Boolean(active?.hasAttribute?.("data-keyboard-event-surface")),
      cardId: card?.getAttribute("data-event-id") || null,
      cardRoot: active === card,
      tag: active?.tagName || null,
      scrollY: window.scrollY,
    };
  });

  const report = { route: page.url() };
  report.initial = await activeState();
  assert(report.initial.surface, "The prototype must initially focus the current-event surface");
  assert(await page.locator("[data-keyboard-event-surface]").getAttribute("data-desktop-action-panel") !== null, "Initial keyboard context must be the CTA panel");
  assert(await page.locator(".keyboard-prototype-dock").count() === 0, "Floating service dock must not exist");
  assert(await page.locator(".keyboard-prototype-surface-hint").count() === 0, "Service hint inside the event title must not exist");
  assert(await page.locator("[data-keyboard-quickstart]").count() === 1, "Expected the in-flow quick navigation explainer before the footer");
  assert(await page.locator("[data-desktop-action-panel] [data-keyboard-shortcut-badge]").count() >= 4, "CTA controls must expose subtle shortcut badges");
  assert(await page.locator(cardSelector).count() >= 6, "Expected a useful related-event grid");
  assert(await page.locator("meta[name=robots]").getAttribute("content") === "noindex,nofollow,noarchive", "Lab route must remain noindex");

  const heroBefore = await page.locator("[data-clean-hero-image]").getAttribute("src");
  await page.keyboard.press("ArrowRight");
  await page.waitForTimeout(100);
  report.heroRight = await page.locator("[data-clean-hero-image]").getAttribute("src");
  assert(report.heroRight && report.heroRight !== heroBefore, "ArrowRight in the CTA context must advance the closed hero carousel");
  await page.keyboard.press("ArrowLeft");
  await page.waitForTimeout(100);
  report.heroLeft = await page.locator("[data-clean-hero-image]").getAttribute("src");
  assert(report.heroLeft === heroBefore, "ArrowLeft must restore the previous hero image");

  await page.keyboard.press("ArrowDown");
  report.firstCard = await activeState();
  assert(report.firstCard.cardRoot, "ArrowDown from the event surface must focus a card root");

  await page.keyboard.press("ArrowRight");
  report.rightCard = await activeState();
  assert(report.rightCard.cardRoot && report.rightCard.cardId !== report.firstCard.cardId, "ArrowRight must move to the adjacent card");

  await page.keyboard.press("ArrowDown");
  report.lowerCard = await activeState();
  assert(report.lowerCard.cardRoot && report.lowerCard.cardId !== report.rightCard.cardId, "ArrowDown must move to the nearest card in the next row");

  await page.keyboard.press("ArrowUp");
  report.upperCard = await activeState();
  assert(report.upperCard.cardId === report.rightCard.cardId, "ArrowUp must restore the spatially matching card");
  await page.keyboard.press("ArrowUp");
  report.surfaceReturn = await activeState();
  assert(report.surfaceReturn.surface, "ArrowUp from the first row must return to the current event");

  await page.locator(`${cardSelector} [data-native-share]`).first().focus();
  const innerCard = await activeState();
  assert(innerCard.cardId && !innerCard.cardRoot, "Test setup must focus an inner card action");
  await page.keyboard.press("Escape");
  report.escape = await activeState();
  assert(report.escape.cardRoot && report.escape.cardId === innerCard.cardId, "Escape must collapse inner-control focus to the card root");

  await page.locator(`${cardSelector} [data-native-share]`).first().focus();
  await page.keyboard.press("ArrowRight");
  report.innerArrow = await activeState();
  assert(report.innerArrow.cardRoot && report.innerArrow.cardId !== innerCard.cardId, "Arrows from an inner card action must continue card surfing");

  const interceptAction = async (scopeSelector, actionSelector, focusSelector, key) => {
    await page.evaluate(({ scopeSelector, actionSelector, focusSelector }) => {
      window.__keyboardPrototypeAction = null;
      const scope = document.querySelector(scopeSelector);
      const action = scope?.querySelector(actionSelector);
      const focusTarget = document.querySelector(focusSelector);
      if (!(action instanceof HTMLElement) || !(focusTarget instanceof HTMLElement)) return;
      action.addEventListener("click", (event) => {
        window.__keyboardPrototypeAction = {
          eventId: action.getAttribute("data-event-id") || action.getAttribute("data-calendar-event-id") || action.getAttribute("data-share-event-id"),
          selector: actionSelector,
        };
        event.preventDefault();
        event.stopImmediatePropagation();
      }, { capture: true, once: true });
      focusTarget.focus({ preventScroll: true });
    }, { scopeSelector, actionSelector, focusSelector });
    await page.keyboard.press(key);
    return page.evaluate(() => window.__keyboardPrototypeAction);
  };

  report.currentLike = await interceptAction(
    "[data-desktop-clean-event] [data-desktop-action-panel]",
    "[data-feedback-action=like]",
    surfaceSelector,
    "l",
  );
  assert(report.currentLike?.eventId === "6408", "L must target the current event while its surface is focused");

  const firstCardSelector = `${cardSelector}:first-of-type`;
  const firstCardId = await page.locator(cardSelector).first().getAttribute("data-event-id");
  report.cardCalendar = await interceptAction(firstCardSelector, "[data-calendar-action]", firstCardSelector, "k");
  assert(report.cardCalendar?.eventId === firstCardId, "K must target the focused related card");
  report.cardShare = await interceptAction(firstCardSelector, "[data-native-share]", firstCardSelector, "s");
  assert(report.cardShare?.eventId === firstCardId, "S must target the focused related card");

  report.primaryCta = await interceptAction(
    "[data-desktop-clean-event]",
    ".desktop-prototype__primary-action:not(.is-disabled)",
    surfaceSelector,
    "Enter",
  );
  assert(report.primaryCta, "Enter on the current-event surface must dispatch the visible primary CTA");

  await page.evaluate(() => {
    window.scrollTo(0, 0);
    document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true });
  });
  const beforeSpace = await page.evaluate(() => window.scrollY);
  await page.keyboard.press("Space");
  await page.waitForTimeout(350);
  report.spaceScroll = { before: beforeSpace, after: await page.evaluate(() => window.scrollY) };
  assert(report.spaceScroll.after > report.spaceScroll.before, "Space must retain native page scrolling");

  const headerLink = page.locator("header a").first();
  await headerLink.focus();
  const headerHref = await headerLink.getAttribute("href");
  await page.keyboard.press("ArrowDown");
  report.outsideScope = await page.evaluate((href) => ({
    sameFocus: document.activeElement?.getAttribute?.("href") === href,
    scrollY: window.scrollY,
  }), headerHref);
  assert(report.outsideScope.sameFocus, "Arrow handling must not hijack focus outside the event navigator");

  report.horizontalOverflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  assert(report.horizontalOverflow <= 1, "Prototype must not add horizontal overflow");
  return report;
}')"
echo "$RUN_OUTPUT"
if grep -q '^### Error' <<<"$RUN_OUTPUT"; then
  echo "Keyboard event navigation Playwright check: FAIL" >&2
  exit 1
fi

playwright-cli -s="$SESSION" screenshot --filename="$OUTPUT_DIR/keyboard-navigation-1536x864.png" >/dev/null
echo "Keyboard event navigation Playwright check: PASS"
echo "Screenshot: $OUTPUT_DIR/keyboard-navigation-1536x864.png"
