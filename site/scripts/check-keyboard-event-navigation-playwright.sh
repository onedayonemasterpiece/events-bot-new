#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${STATIC_SITE_REVIEW_BASE_URL:-http://127.0.0.1:4321}"
ROUTE_SUFFIX="${KEYBOARD_NAVIGATION_ROUTE:-lab/keyboard-event-navigation/}"
ROUTE="${BASE_URL%/}/${ROUTE_SUFFIX#/}"
SESSION="keyboard-event-navigation-$$"
OUTPUT_DIR="${KEYBOARD_NAVIGATION_ARTIFACT_DIR:-../artifacts/codex/keyboard-event-navigation-prototype}"
SCREENSHOT_SLUG="${KEYBOARD_NAVIGATION_SCREENSHOT_SLUG:-keyboard-navigation}"

command -v playwright-cli >/dev/null 2>&1 || {
  echo "playwright-cli is required" >&2
  exit 1
}

mkdir -p "$OUTPUT_DIR"
trap 'playwright-cli -s="$SESSION" close >/dev/null 2>&1 || true' EXIT

playwright-cli -s="$SESSION" open --browser=chromium "$ROUTE" >/dev/null
playwright-cli -s="$SESSION" resize 1536 864 >/dev/null
RUN_OUTPUT="$(playwright-cli -s="$SESSION" run-code "$(cat <<'JS'
async page => {
  const assert = (condition, message) => { if (!condition) throw new Error(message); };
  const waitFor = async (label, fn, arg) => {
    try { return await page.waitForFunction(fn, arg, { timeout: 9000 }); }
    catch { throw new Error(`Timed out waiting for ${label}`); }
  };
  const surfaceSelector = "[data-keyboard-event-surface]";
  const cardSelector = "[data-related-start] [data-event-card]";
  await page.route("**/event.ics", (route) => route.fulfill({
    status: 200,
    contentType: "text/calendar",
    body: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n",
  }));
  await page.addInitScript(() => {
    if (sessionStorage.getItem("ke_keyboard_test_initialized") !== "1") {
      localStorage.removeItem("ke_keyboard_shortcut_daily_v2");
      localStorage.removeItem("ke_personalization_profile");
      localStorage.removeItem("ke_calendar_saved_v1");
      sessionStorage.setItem("ke_keyboard_test_initialized", "1");
    }
    window.__keyboardClipboardText = [];
    window.__keyboardClipboardImages = [];
    window.__keyboardNativeShareCalls = 0;
    window.__keyboardDailyFacts = [];
    window.addEventListener("kenigevents:shortcut-daily-fact", (event) => window.__keyboardDailyFacts.push(event.detail));
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: async (value) => { window.__keyboardClipboardText.push(String(value)); },
        write: async (items) => { window.__keyboardClipboardImages.push(items.map((item) => [...item.types])); },
      },
    });
    Object.defineProperty(navigator, "share", {
      configurable: true,
      value: async () => { window.__keyboardNativeShareCalls += 1; },
    });
  });
  await page.reload();
  await page.waitForSelector(surfaceSelector);
  await page.waitForTimeout(350);

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
  const report = await page.locator(surfaceSelector).evaluate((surface) => ({
    route: location.href,
    eventId: surface.querySelector("[data-feedback-action=like]")?.getAttribute("data-event-id"),
    family: surface.closest("[data-desktop-clean-event]")?.getAttribute("data-desktop-family"),
  }));
  assert(["6408", "6593"].includes(report.eventId), "Only fixtures 6408 and 6593 are accepted");
  assert(["split", "editorial"].includes(report.family), "Expected split/editorial desktop family");
  assert((await activeState()).surface, "The prototype must initially focus the current-event CTA surface");
  assert(await page.locator("meta[name=robots]").getAttribute("content") === "noindex,nofollow,noarchive", "Prototype must remain noindex");
  assert(await page.locator("[data-keyboard-quickstart]").count() === 1, "Expected one situational quick-navigation block");
  assert(await page.locator("[data-keyboard-quickstart] .keyboard-quickstart__contexts > p").count() === 3, "Quickstart must explain three concise contexts");
  assert(await page.locator(".keyboard-prototype-dock").count() === 0, "No floating service dock is allowed");
  assert(await page.locator(cardSelector).count() === 10, "Both fixtures must expose ten related cards");
  assert(await page.locator("[data-related-start] [data-calendar-action] [data-related-calendar-shortcut]").count() === 10, "Every eligible related calendar must receive one K hint");
  assert(await page.locator("[data-related-start] [data-calendar-action] [data-related-calendar-shortcut]:visible").count() >= 1, "Roomy cards must visibly show the K hint");
  assert(await page.locator("[data-event-content-copy-actions]").count() === 1, "Expected one description action group in the desktop description");
  assert(await page.locator(".desktop-clean-description__text + [data-event-content-copy-actions]").count() === 1, "Copy controls must follow the full description text");

  // A single Down remains native; a released second Down within 430 ms jumps.
  await page.evaluate(() => { window.scrollTo(0, 0); document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true }); });
  const beforeSingleDown = await page.evaluate(() => scrollY);
  await page.keyboard.press("ArrowDown");
  await page.waitForTimeout(80);
  report.singleDown = { before: beforeSingleDown, after: await page.evaluate(() => scrollY), active: await activeState() };
  assert(report.singleDown.after > report.singleDown.before && report.singleDown.active.surface, "One Down must keep focus and perform native scrolling");
  await page.evaluate(() => { window.scrollTo(0, 0); document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true }); });
  await page.keyboard.press("ArrowDown");
  await page.waitForTimeout(40);
  await page.keyboard.press("ArrowDown");
  report.doubleDown = await activeState();
  assert(report.doubleDown.cardRoot, "Two separate quick Down presses must focus the first related card");

  // First-row Up returns to the real page top and a held repeat cannot open the gallery.
  const firstCard = page.locator(cardSelector).first();
  await firstCard.focus();
  await firstCard.evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowUp", key: "ArrowUp", bubbles: true })));
  await page.locator(surfaceSelector).evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowUp", key: "ArrowUp", repeat: true, bubbles: true })));
  report.returnTop = await page.evaluate(() => ({
    surface: document.activeElement?.hasAttribute?.("data-keyboard-event-surface"),
    rootTop: document.querySelector("[data-desktop-clean-event]")?.getBoundingClientRect().top,
    galleryOpen: Boolean(document.querySelector("[data-hero-gallery]:not([hidden]).is-open")),
  }));
  assert(report.returnTop.surface && Math.abs(report.returnTop.rootTop) < 8 && !report.returnTop.galleryOpen, `Up from first row failed: ${JSON.stringify(report.returnTop)}`);
  await page.locator(surfaceSelector).evaluate((node) => node.dispatchEvent(new KeyboardEvent("keyup", { code: "ArrowUp", key: "ArrowUp", bubbles: true })));

  // Closed hero arrows support both multi-image and single-image templates.
  const heroBefore = await page.locator("[data-clean-hero-image]").getAttribute("src");
  const heroImageSlides = await page.locator("[data-desktop-clean-event] [data-hero-gallery-slide][data-gallery-slide-kind=image]").count();
  await page.keyboard.press("ArrowRight");
  await page.waitForTimeout(80);
  report.heroRight = await page.locator("[data-clean-hero-image]").getAttribute("src");
  if (heroImageSlides > 1) assert(report.heroRight !== heroBefore, "Multi-image template must advance its closed hero");
  else assert(report.heroRight === heroBefore, "Single-image template must remain stable on closed-hero arrows");
  if (heroImageSlides > 1) {
    await page.keyboard.press("ArrowLeft");
    assert(await page.locator("[data-clean-hero-image]").getAttribute("src") === heroBefore, "ArrowLeft must restore the prior hero");
  }

  // Fresh Up opens the gallery; Enter/Space activate the final related-event CTA.
  await page.keyboard.press("ArrowUp");
  const gallery = page.locator("[data-hero-gallery]:not([hidden]).is-open");
  await gallery.waitFor();
  report.galleryOpen = await gallery.evaluate((node) => ({ ownsFocus: node.contains(document.activeElement), parentIsBody: node.parentElement === document.body }));
  assert(report.galleryOpen.ownsFocus && report.galleryOpen.parentIsBody, "Fresh Up at event top must open and focus the fullscreen gallery");
  const gallerySlideCount = await gallery.locator("[data-hero-gallery-slide]").count();
  for (let index = 1; index < gallerySlideCount; index += 1) await page.keyboard.press("ArrowRight");
  const galleryCta = gallery.locator("[data-hero-gallery-slide][data-gallery-slide-kind=cta][aria-hidden=false]");
  await galleryCta.waitFor();
  const armGalleryActivation = async (key) => {
    await gallery.focus();
    await galleryCta.locator("a[href]").evaluate((link, activationKey) => {
      window.__keyboardGalleryActivation = null;
      link.addEventListener("click", (event) => {
        window.__keyboardGalleryActivation = { key: activationKey, href: link.href };
        event.preventDefault();
        event.stopImmediatePropagation();
      }, { capture: true, once: true });
    }, key);
    await page.keyboard.press(key);
    return page.evaluate(() => window.__keyboardGalleryActivation);
  };
  report.galleryEnter = await armGalleryActivation("Enter");
  report.gallerySpace = await armGalleryActivation("Space");
  assert(report.galleryEnter?.href && report.gallerySpace?.href === report.galleryEnter.href, "Enter and Space must activate the same final recommendation");
  const galleryClose = gallery.locator("[data-hero-gallery-close]");
  await galleryClose.focus();
  await page.keyboard.press("Enter");
  await waitFor("gallery close", () => !document.querySelector("[data-hero-gallery]:not([hidden])"));
  await page.locator(surfaceSelector).focus();

  // C copies title, rendered lead/body and the canonical event URL; poster is button-only.
  const expectedDescription = await page.evaluate(() => {
    const surface = document.querySelector("[data-keyboard-event-surface]");
    const description = document.querySelector("[data-desktop-clean-event] .desktop-clean-description");
    const title = String(surface?.querySelector("[data-native-share]")?.getAttribute("data-share-event-title") || "").trim();
    const lead = String(description?.querySelector(".desktop-clean-description__lead")?.textContent || "").trim();
    const body = String(description?.querySelector(".desktop-clean-description__text")?.innerText || "").trim();
    const url = String(surface?.querySelector("[data-native-share]")?.getAttribute("data-share-url") || "").trim();
    return [title, ...(lead && lead !== title ? [lead] : []), ...(body && body !== lead ? [body] : []), url].join("\n\n");
  });
  await page.keyboard.press("c");
  await waitFor("description clipboard", (expected) => window.__keyboardClipboardText?.at(-1) === expected, expectedDescription);
  assert((await page.locator("[data-keyboard-action-toast]").innerText()).includes("Описание"), "C must show resolved description-copy feedback");
  const descriptionCopyCount = await page.evaluate(() => window.__keyboardClipboardText.length);
  await page.keyboard.press("c");
  assert(await page.evaluate((count) => window.__keyboardClipboardText.length === count + 1, descriptionCopyCount), "C remains a real repeatable copy action");
  const factsAfterDescription = await page.evaluate(() => JSON.parse(localStorage.getItem("ke_keyboard_shortcut_daily_v2") || "null"));
  const currentFactActions = Object.values(factsAfterDescription.days).flat();
  assert(currentFactActions.filter((value) => value === "copy_description").length === 1, "Repeated C use must leave one daily description fact");
  assert(!JSON.stringify(factsAfterDescription).match(/6408|6593|https?:|title|url|timestamp|route/iu), "Daily fact storage must not contain event identity, URL, route or raw timestamps");
  const posterButton = page.locator("[data-copy-event-poster]");
  await posterButton.click();
  await waitFor("poster clipboard", () => window.__keyboardClipboardImages.length > 0);
  report.posterCopy = await page.evaluate(() => window.__keyboardClipboardImages.at(-1));
  assert(report.posterCopy.length === 1 && report.posterCopy[0].length === 1 && report.posterCopy[0][0] === "image/png", "Poster button must write exactly one PNG ClipboardItem");
  assert(await page.evaluate(() => window.__keyboardNativeShareCalls) === 0, "Event copy actions must never call navigator.share on desktop");

  // Consent keyboard flow and visible CTA states are fixture-independent.
  await page.locator(surfaceSelector).focus();
  await page.keyboard.press("l");
  const consent = page.locator("[data-personalization-consent].is-visible");
  await consent.waitFor();
  await page.keyboard.press("Escape");
  assert(await page.locator("[data-personalization-consent].is-visible").count() === 0, "Escape must decline consent");
  await page.keyboard.press("l");
  await consent.waitFor();
  await page.keyboard.press("Enter");
  await waitFor("like consent replay", () => document.querySelector("[data-keyboard-event-surface] [data-feedback-action=like]")?.getAttribute("aria-pressed") === "true");
  report.currentLike = await page.locator(`${surfaceSelector} [data-feedback-action=like]`).evaluate((button) => ({
    pressed: button.getAttribute("aria-pressed"), color: getComputedStyle(button).backgroundColor,
    eventId: button.getAttribute("data-event-id"), base: Number(button.getAttribute("data-base-count") || 0),
    count: Number(button.querySelector("[data-feedback-count]")?.textContent || 0),
  }));
  assert(report.currentLike.eventId === report.eventId && report.currentLike.pressed === "true" && report.currentLike.color === "rgb(201, 52, 52)" && report.currentLike.count === report.currentLike.base + 1, "L + consent must leave the current event visibly liked");
  await waitFor("consented like daily fact", () => window.__keyboardDailyFacts.some((fact) => fact.action_code === "like_toggle"));
  const remoteFact = await page.evaluate(() => window.__keyboardDailyFacts.find((fact) => fact.action_code === "like_toggle"));
  assert(Object.keys(remoteFact).sort().join(",") === "action_code,schema_version", "Consent-gated collector event must contain only schema and allowlisted action");

  // An action used locally before consent is reported on its next real use,
  // but remains one compact collector event per action/day.
  await page.locator(surfaceSelector).focus();
  const descriptionFactsBeforeConsentReplay = await page.evaluate(() => window.__keyboardDailyFacts.filter((fact) => fact.action_code === "copy_description").length);
  await page.keyboard.press("c");
  await waitFor("post-consent description fact", () => window.__keyboardDailyFacts.filter((fact) => fact.action_code === "copy_description").length === 1);
  await page.keyboard.press("c");
  await page.waitForTimeout(80);
  assert(descriptionFactsBeforeConsentReplay === 0, "Pre-consent local use must not emit a collector event");
  assert(await page.evaluate(() => window.__keyboardDailyFacts.filter((fact) => fact.action_code === "copy_description").length) === 1, "Collector facts must remain deduplicated after consent");

  await page.locator(surfaceSelector).focus();
  await page.keyboard.press("k");
  const currentCalendar = page.locator(`${surfaceSelector} [data-calendar-action]`);
  await waitFor("current calendar success", () => document.querySelector("[data-keyboard-event-surface] [data-calendar-action]")?.getAttribute("data-calendar-state") === "added");
  report.currentCalendar = await currentCalendar.evaluate((anchor) => ({
    eventId: anchor.getAttribute("data-calendar-event-id"), state: anchor.getAttribute("data-calendar-state"),
    label: anchor.querySelector("[data-calendar-label]")?.textContent?.trim(), color: getComputedStyle(anchor).backgroundColor,
  }));
  assert(report.currentCalendar.eventId === report.eventId && report.currentCalendar.state === "added" && report.currentCalendar.label === "Добавлено" && report.currentCalendar.color === "rgb(38, 120, 72)", "K must persist a green Added state");

  await page.locator(surfaceSelector).focus();
  const currentShare = page.locator(`${surfaceSelector} [data-native-share]`);
  const expectedEventCopy = await currentShare.evaluate((button) => `${button.getAttribute("data-share-event-title")}\n${button.getAttribute("data-share-url")}`);
  await page.keyboard.press("s");
  await waitFor("event link clipboard", (expected) => window.__keyboardClipboardText?.at(-1) === expected, expectedEventCopy);
  assert(await page.evaluate(() => window.__keyboardNativeShareCalls) === 0, "S must copy the event title+URL without system share");

  // Related spatial navigation, K action and lost-focus re-entry.
  await firstCard.focus();
  const firstCardId = await firstCard.getAttribute("data-event-id");
  await page.keyboard.press("ArrowRight");
  report.rightCard = await activeState();
  assert(report.rightCard.cardRoot && report.rightCard.cardId !== firstCardId, "Right must move to the adjacent card");
  await page.keyboard.press("ArrowDown");
  report.lowerCard = await activeState();
  assert(report.lowerCard.cardRoot && report.lowerCard.cardId !== report.rightCard.cardId, "Down inside related must move to the next row");
  await page.evaluate(() => document.activeElement?.blur());
  await page.keyboard.press("ArrowDown");
  assert((await activeState()).cardRoot, "Down must re-enter navigation after focus is lost");
  const calendarCardId = await page.evaluate(() => {
    const today = Math.floor((Date.now() + 2 * 60 * 60 * 1000) / 86400000);
    return Array.from(document.querySelectorAll("[data-related-start] [data-event-card]")).find((card) => Number(card.querySelector("[data-calendar-action]")?.getAttribute("data-calendar-expiry-day") || 0) > today)?.getAttribute("data-event-id");
  });
  assert(calendarCardId, "Expected at least one future related calendar fixture");
  const calendarCard = page.locator(`${cardSelector}[data-event-id="${calendarCardId}"]`).first();
  await calendarCard.focus();
  await page.keyboard.press("k");
  await waitFor("related calendar success", (id) => document.querySelector(`[data-related-start] [data-event-card][data-event-id="${id}"] [data-calendar-action]`)?.getAttribute("data-calendar-state") === "added", calendarCardId);
  assert(await calendarCard.locator("[data-related-calendar-shortcut]").count() === 1, "Calendar success must preserve the related K hint");

  // Boundary Down: when the related section has entered the viewport, one press focuses card 1.
  await page.evaluate(() => {
    const related = document.querySelector("[data-related-start]");
    const top = related.getBoundingClientRect().top + scrollY;
    window.scrollTo({ top: Math.max(0, top - innerHeight + 80), behavior: "instant" });
    document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true });
  });
  await page.waitForTimeout(60);
  const boundaryBefore = await page.evaluate(() => ({ relatedTop: document.querySelector("[data-related-start]")?.getBoundingClientRect().top, innerHeight, activeSurface: document.activeElement?.hasAttribute?.("data-keyboard-event-surface"), scrollY }));
  await page.keyboard.press("ArrowDown");
  const boundaryAfter = await activeState();
  assert(boundaryAfter.cardRoot, `Boundary Down failed: ${JSON.stringify({ boundaryBefore, boundaryAfter })}`);

  // Footer physical codes work in Russian layout and only record success.
  const footerShare = page.locator("[data-service-share-root][data-service-share-surface=footer]");
  await page.locator(surfaceSelector).focus();
  await footerShare.scrollIntoViewIfNeeded();
  await waitFor("footer hydration", () => document.querySelector("[data-service-share-surface=footer]")?.getAttribute("data-service-share-hydrated") === "true");
  await waitFor("footer readiness", () => ["file", "text"].includes(document.querySelector("[data-service-share-surface=footer]")?.getAttribute("data-service-share-ready")));
  const imageWritesBeforeFooter = await page.evaluate(() => window.__keyboardClipboardImages.length);
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", bubbles: true })));
  await waitFor("footer image clipboard", (count) => window.__keyboardClipboardImages.length === count + 1, imageWritesBeforeFooter);
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyS", key: "ы", bubbles: true })));
  await waitFor("footer text clipboard", () => window.__keyboardClipboardText?.at(-1)?.endsWith("\nhttps://kenigevents.ru/"));
  assert(await page.evaluate(() => window.__keyboardNativeShareCalls) === 0, "Footer P/S must also avoid native share");

  // Per-action mastery is three distinct recent local days; stale use restores hints.
  await page.evaluate(() => {
    const parts = (offset) => {
      const date = new Date(Date.now() - offset * 86400000);
      const entries = Object.fromEntries(new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Kaliningrad", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date).filter((p) => p.type !== "literal").map((p) => [p.type, p.value]));
      return `${entries.year}-${entries.month}-${entries.day}`;
    };
    const actions = ["primary_cta", "calendar_add", "copy_event", "like_toggle"];
    localStorage.setItem("ke_keyboard_shortcut_daily_v2", JSON.stringify({ v: 2, days: { [parts(0)]: actions, [parts(1)]: actions, [parts(2)]: actions } }));
  });
  await page.reload();
  await page.waitForSelector(surfaceSelector);
  assert(await page.locator(surfaceSelector).getAttribute("data-keyboard-shortcut-hints") === "hidden", "Three recent usage days must hide mastered CTA badges");
  assert(await page.locator("[data-desktop-action-panel] [data-keyboard-shortcut-badge]:visible").count() === 0, "Mastered CTA badges must be visually clean");
  assert(await page.locator("[data-desktop-action-panel] [title*=клавиша]").count() >= 4, "Hover/focus help remains when badges are hidden");
  await page.evaluate(() => {
    const old = new Date(Date.now() - 20 * 86400000).toISOString().slice(0, 10);
    localStorage.setItem("ke_keyboard_shortcut_daily_v2", JSON.stringify({ v: 2, days: { [old]: ["primary_cta", "calendar_add", "copy_event", "like_toggle"] } }));
  });
  await page.reload();
  await page.waitForSelector(surfaceSelector);
  assert(await page.locator(surfaceSelector).getAttribute("data-keyboard-shortcut-hints") === "visible", "Hints must return after mastery lapses");

  report.horizontalOverflow = await page.evaluate(() => document.documentElement.scrollWidth - window.innerWidth);
  assert(report.horizontalOverflow <= 1, "Prototype must not add horizontal overflow");
  return report;
}
JS
)" 2>&1)"
echo "$RUN_OUTPUT"
if grep -q '^### Error' <<<"$RUN_OUTPUT"; then
  echo "Keyboard event navigation Playwright check: FAIL" >&2
  exit 1
fi

playwright-cli -s="$SESSION" screenshot --filename="$OUTPUT_DIR/${SCREENSHOT_SLUG}-1536x864.png" >/dev/null
echo "Keyboard event navigation Playwright check: PASS"
echo "Screenshot: $OUTPUT_DIR/${SCREENSHOT_SLUG}-1536x864.png"
