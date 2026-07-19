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
  await page.evaluate(() => {
    window.__keyboardClipboardText = [];
    window.__keyboardClipboardImages = [];
    window.__keyboardNativeShareCalls = 0;
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
  assert(await page.locator("[data-desktop-action-panel] [title*=клавиша]").count() >= 4, "CTA controls must expose native hover text with the shortcut key");
  assert(await page.locator("[data-service-share-surface=footer] [data-service-shortcut-badge]").count() === 2, "Footer copy actions must show inline P/S keycaps");
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

  await page.keyboard.press("ArrowUp");
  const requestedGalleryIndex = await page.locator("[data-clean-hero-image]").evaluate((image) => image.closest("[data-hero-gallery-open]")?.getAttribute("data-hero-gallery-index"));
  const gallery = page.locator("[data-hero-gallery]:not([hidden]).is-open");
  await gallery.waitFor();
  report.galleryOpen = await gallery.evaluate((node) => ({
    activeIndex: node.getAttribute("data-active-index"),
    ownsFocus: node.contains(document.activeElement),
    parentIsBody: node.parentElement === document.body,
  }));
  assert(report.galleryOpen.ownsFocus && report.galleryOpen.parentIsBody && report.galleryOpen.activeIndex === requestedGalleryIndex, "ArrowUp on the current-event CTA must open the fullscreen gallery at the selected image and move focus into it");
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
  assert(report.galleryEnter?.href, "Enter on the active final gallery slide must activate its real related-event link");
  report.gallerySpace = await armGalleryActivation("Space");
  assert(report.gallerySpace?.href === report.galleryEnter.href, "Space on the active final gallery slide must activate the same related-event link");
  await page.evaluate(() => { window.__keyboardUnexpectedGalleryActivation = 0; });
  await galleryCta.locator("a[href]").evaluate((link) => {
    link.addEventListener("click", (event) => {
      window.__keyboardUnexpectedGalleryActivation += 1;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, { capture: true });
  });
  await gallery.focus();
  await gallery.evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "Space", key: " ", bubbles: true })));
  const galleryClose = gallery.locator("[data-hero-gallery-close]");
  await galleryClose.focus();
  await galleryClose.evaluate((node) => node.dispatchEvent(new KeyboardEvent("keyup", { code: "Space", key: " ", bubbles: true })));
  assert(await page.evaluate(() => window.__keyboardUnexpectedGalleryActivation) === 0, "A gallery Space arm must cancel when focus moves to another control");
  await page.keyboard.press("Enter");
  await page.waitForFunction(() => !document.querySelector("[data-hero-gallery]:not([hidden])"));
  assert(await page.evaluate(() => window.__keyboardUnexpectedGalleryActivation) === 0, "Enter on the gallery close control must close rather than activate the recommendation");
  assert(await page.locator("[data-clean-hero-image]").evaluate((image) => document.activeElement === image.closest("[data-hero-gallery-open]")), "Closing the gallery must restore focus to its exact opener");
  await page.locator(surfaceSelector).focus();

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

  await page.evaluate(() => document.activeElement?.blur());
  report.lostFocus = await activeState();
  assert(!report.lostFocus.surface && !report.lostFocus.cardId, "Test setup must clear navigator focus");
  await page.keyboard.press("ArrowDown");
  report.reentry = await activeState();
  assert(report.reentry.cardRoot, "ArrowDown must re-enter related-event navigation after focus is lost");

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

  await page.evaluate(() => {
    localStorage.removeItem("ke_personalization_profile");
    document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true });
  });
  await page.keyboard.press("l");
  const consent = page.locator("[data-personalization-consent].is-visible");
  await consent.waitFor();
  await page.keyboard.press("Escape");
  await page.waitForTimeout(50);
  assert(await page.locator("[data-personalization-consent].is-visible").count() === 0, "Escape must activate the consent decline action");
  await page.keyboard.press("l");
  await consent.waitFor();
  await page.keyboard.press("Enter");
  await page.waitForFunction(() => document.querySelector("[data-keyboard-event-surface] [data-feedback-action=like]")?.getAttribute("aria-pressed") === "true");
  assert(await page.locator("[data-personalization-consent].is-visible").count() === 0, "Enter must activate the consent accept action");
  assert(await page.evaluate(() => JSON.parse(localStorage.getItem("ke_personalization_profile") || "null")?.consent_ok === true), "Enter acceptance must create the consented local profile");
  report.currentLike = await page.locator(`${surfaceSelector} [data-feedback-action=like]`).evaluate((button) => ({
    pressed: button.getAttribute("aria-pressed"),
    color: getComputedStyle(button).backgroundColor,
    eventId: button.getAttribute("data-event-id"),
    base: Number(button.getAttribute("data-base-count") || 0),
    count: Number(button.querySelector("[data-feedback-count]")?.textContent || 0),
    panelLiked: button.closest("[data-feedback-scope]")?.classList.contains("is-liked"),
  }));
  assert(report.currentLike.eventId === "6408" && report.currentLike.pressed === "true" && report.currentLike.color === "rgb(201, 52, 52)" && report.currentLike.panelLiked && report.currentLike.count === report.currentLike.base + 1, "Consent replay must leave the current-event like visibly red, pressed and incremented");
  assert(await page.evaluate(() => JSON.parse(localStorage.getItem("ke_personalization_profile") || "null")?.liked_event_ids?.includes("6408")), "Consent replay must persist event 6408 in liked_event_ids");

  await page.locator(surfaceSelector).focus();
  await page.keyboard.press("k");
  const currentCalendar = page.locator(`${surfaceSelector} [data-calendar-action]`);
  await page.waitForFunction(() => document.querySelector("[data-keyboard-event-surface] [data-calendar-action]")?.getAttribute("data-calendar-state") === "added");
  report.currentCalendar = await currentCalendar.evaluate((anchor) => ({
    eventId: anchor.getAttribute("data-calendar-event-id"),
    state: anchor.getAttribute("data-calendar-state"),
    label: anchor.querySelector("[data-calendar-label]")?.textContent?.trim(),
    color: getComputedStyle(anchor).backgroundColor,
    expiry: Number(anchor.getAttribute("data-calendar-expiry-day") || 0),
    aria: anchor.getAttribute("aria-label"),
  }));
  assert(report.currentCalendar.eventId === "6408" && report.currentCalendar.state === "added" && report.currentCalendar.label === "Добавлено" && report.currentCalendar.color === "rgb(38, 120, 72)" && report.currentCalendar.aria.startsWith("Скачать файл календаря ещё раз"), "K must persist the current event and render its calendar action green with repeat-download semantics");
  assert(await page.evaluate((expiry) => JSON.parse(localStorage.getItem("ke_calendar_saved_v1") || "null")?.e?.["6408"] === expiry, report.currentCalendar.expiry), "Calendar state storage must contain event 6408 with its expiry day");

  await page.locator(surfaceSelector).focus();
  const currentShare = page.locator(`${surfaceSelector} [data-native-share]`);
  const expectedCurrentCopy = await currentShare.evaluate((button) => `${button.getAttribute("data-share-event-title")}\n${button.getAttribute("data-share-url")}`);
  const currentShareDom = await currentShare.evaluate((button) => ({ icons: button.querySelectorAll("svg").length, count: button.querySelectorAll("[data-share-count]").length, badges: button.querySelectorAll("[data-keyboard-shortcut-badge]").length }));
  await page.keyboard.press("s");
  await page.waitForFunction((expected) => window.__keyboardClipboardText?.at(-1) === expected, expectedCurrentCopy);
  report.currentShare = await page.evaluate(() => ({ text: window.__keyboardClipboardText.at(-1), nativeShareCalls: window.__keyboardNativeShareCalls }));
  assert(report.currentShare.text === expectedCurrentCopy && report.currentShare.nativeShareCalls === 0, "Desktop S must copy exactly the current event title and URL without navigator.share");
  assert(await page.locator(surfaceSelector).evaluate((node) => document.activeElement === node), "Successful event copy must preserve focus on the current-event surface");
  assert(await currentShare.evaluate((button, before) => button.querySelectorAll("svg").length === before.icons && button.querySelectorAll("[data-share-count]").length === before.count && button.querySelectorAll("[data-keyboard-shortcut-badge]").length === before.badges, currentShareDom), "Event copy feedback must not replace the share icon, count or shortcut badge");
  const toast = page.locator("[data-keyboard-action-toast]");
  assert(await toast.isVisible() && /Название и ссылка.+скопированы/u.test(await toast.innerText()), "Successful event copy must show an explicit visual toast");
  await page.waitForTimeout(2850);
  assert(!(await toast.isVisible()), "The event-copy toast must dismiss itself");
  const copyCountBeforeMissingUrl = await page.evaluate(() => window.__keyboardClipboardText.length);
  await currentShare.evaluate((button) => {
    window.__keyboardRemovedShareUrl = button.getAttribute("data-share-url");
    button.removeAttribute("data-share-url");
  });
  await page.locator(surfaceSelector).focus();
  await page.keyboard.press("s");
  await page.waitForFunction(() => document.querySelector("[data-keyboard-event-surface] [data-native-share]")?.getAttribute("data-keyboard-copy-state") === "error");
  assert(await page.evaluate((count) => window.__keyboardClipboardText.length === count, copyCountBeforeMissingUrl), "Event copy must fail closed instead of substituting the current page when its canonical URL is absent");
  assert(await page.locator(surfaceSelector).evaluate((node) => document.activeElement === node), "Failed event copy must also preserve focus");
  await currentShare.evaluate((button) => button.setAttribute("data-share-url", window.__keyboardRemovedShareUrl));

  const firstCardSelector = `${cardSelector}:first-of-type`;
  const firstCardId = await page.locator(cardSelector).first().getAttribute("data-event-id");
  await page.locator(firstCardSelector).focus();
  await page.keyboard.press("k");
  await page.waitForFunction((selector) => document.querySelector(`${selector} [data-calendar-action]`)?.getAttribute("data-calendar-state") === "added", firstCardSelector);
  report.cardCalendar = await page.locator(`${firstCardSelector} [data-calendar-action]`).getAttribute("data-calendar-event-id");
  assert(report.cardCalendar === firstCardId, "K must save the focused related card");
  const cardShare = page.locator(`${firstCardSelector} [data-native-share]`);
  const expectedCardCopy = await cardShare.evaluate((button) => `${button.getAttribute("data-share-event-title")}\n${button.getAttribute("data-share-url")}`);
  await page.locator(firstCardSelector).focus();
  await page.keyboard.press("s");
  await page.waitForFunction((expected) => window.__keyboardClipboardText?.at(-1) === expected, expectedCardCopy);
  report.cardShare = await cardShare.getAttribute("data-share-event-id");
  assert(report.cardShare === firstCardId, "S must copy the focused related card title and URL");

  report.primaryCta = await interceptAction(
    "[data-desktop-clean-event]",
    ".desktop-prototype__primary-action:not(.is-disabled)",
    surfaceSelector,
    "Enter",
  );
  assert(report.primaryCta, "Enter on the current-event surface must dispatch the visible primary CTA");

  const footerShare = page.locator("[data-service-share-root][data-service-share-surface=footer]");
  await page.locator(surfaceSelector).focus();
  await footerShare.scrollIntoViewIfNeeded();
  await page.waitForFunction(() => document.querySelector("[data-service-share-root][data-service-share-surface=footer]")?.getAttribute("data-service-share-hydrated") === "true");
  await page.waitForFunction(() => ["file", "text"].includes(document.querySelector("[data-service-share-root][data-service-share-surface=footer]")?.getAttribute("data-service-share-ready")));
  await page.evaluate(() => {
    document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", bubbles: true }));
  });
  const footerImage = footerShare.locator("[data-service-share-intent=image]");
  await page.waitForFunction(() => document.querySelector("[data-service-share-surface=footer] [data-service-share-intent=image]")?.getAttribute("data-service-share-state") === "success");
  report.serviceImage = await page.evaluate(() => ({ writes: window.__keyboardClipboardImages, nativeShareCalls: window.__keyboardNativeShareCalls }));
  assert(report.serviceImage.writes.length === 1 && report.serviceImage.writes[0][0].length === 1 && report.serviceImage.writes[0][0][0] === "image/png", "Russian-layout physical KeyP must copy exactly the footer PNG card");
  assert(await footerImage.getAttribute("data-service-share-state") === "success", "Footer image action must visibly confirm success");
  assert((await footerShare.locator("[data-service-share-status]").innerText()).trim() === "Карточка скопирована в буфер", "Footer image shortcut must announce confirmed clipboard success");
  await page.evaluate(() => {
    document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", repeat: true, bubbles: true }));
    document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", ctrlKey: true, bubbles: true }));
  });
  assert(await page.evaluate(() => window.__keyboardClipboardImages.length) === 1, "Repeat and modified footer P shortcuts must be ignored");

  await page.evaluate(() => {
    document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyS", key: "ы", bubbles: true }));
  });
  const footerText = footerShare.locator("[data-service-share-intent=text]");
  await page.waitForFunction(() => document.querySelector("[data-service-share-surface=footer] [data-service-share-intent=text]")?.getAttribute("data-service-share-state") === "success");
  report.serviceText = await page.evaluate(() => ({ text: window.__keyboardClipboardText.at(-1), nativeShareCalls: window.__keyboardNativeShareCalls }));
  assert(report.serviceText.text.endsWith("\nhttps://kenigevents.ru/") && report.serviceText.nativeShareCalls === 0, "Russian-layout physical KeyS must copy service text plus the canonical service link without navigator.share");
  assert(await footerText.getAttribute("data-service-share-state") === "success" && /скопирован/iu.test(await toast.innerText()), "Footer text action must visibly confirm success");
  assert((await footerShare.locator("[data-service-share-status]").innerText()).trim() === "Текст и ссылка скопированы", "Footer text shortcut must announce confirmed clipboard success");

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

  await page.evaluate(() => localStorage.setItem("ke_keyboard_shortcut_usage_v1", JSON.stringify({ count: 6, lastUsedAt: Date.now() })));
  await page.reload();
  await page.waitForSelector(surfaceSelector);
  assert(await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed") === "true", "The accepted current-event like must survive reload");
  assert(await page.locator(`${surfaceSelector} [data-calendar-action]`).getAttribute("data-calendar-state") === "added", "The saved current-event calendar state must survive reload");
  assert(await page.locator(surfaceSelector).getAttribute("data-keyboard-shortcut-hints") === "hidden", "Frequent recent shortcut use must hide CTA key badges");
  assert(await page.locator("[data-desktop-action-panel] [data-keyboard-shortcut-badge]:visible").count() === 0, "Adaptive cleanup must leave no visible CTA badges for regular users");
  assert(await page.locator("[data-desktop-action-panel] [title*=клавиша]").count() >= 4, "Native hover help must remain available when visual badges are hidden");

  await page.evaluate(() => localStorage.setItem("ke_keyboard_shortcut_usage_v1", JSON.stringify({ count: 20, lastUsedAt: Date.now() - 15 * 24 * 60 * 60 * 1000 })));
  await page.reload();
  await page.waitForSelector(surfaceSelector);
  assert(await page.locator(surfaceSelector).getAttribute("data-keyboard-shortcut-hints") === "visible", "CTA key badges must return after shortcut use lapses");
  await page.keyboard.press("k");
  assert(await page.locator(surfaceSelector).getAttribute("data-keyboard-shortcut-hints") === "visible", "One shortcut after a lapse must restart learning instead of immediately hiding badges");
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
