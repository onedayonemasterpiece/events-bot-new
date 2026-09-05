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

BROWSER="${STATIC_SITE_PLAYWRIGHT_BROWSER:-chromium}"
playwright-cli -s="$SESSION" open --browser="$BROWSER" "$ROUTE" >/dev/null
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
  assert(!(await activeState()).surface, "Keyboard navigation must not autofocus the current-event CTA surface");
  await page.locator(surfaceSelector).focus();
  const robots = await page.locator("meta[name=robots]").getAttribute("content");
  assert(["noindex,nofollow,noarchive", "noindex,nofollow,noarchive,nosnippet"].includes(robots), "Keyboard navigation acceptance must remain noindex");
  const quickstartCount = await page.locator("[data-keyboard-quickstart]").count();
  assert(quickstartCount <= 1, "At most one situational quick-navigation block is allowed");
  if (quickstartCount === 1) {
    assert(await page.locator("[data-keyboard-quickstart] .keyboard-quickstart__contexts > p").count() === 3, "Quickstart must explain three concise contexts");
    const learningCopy = await page.locator("[data-keyboard-quickstart] .keyboard-quickstart__contexts").innerText();
    for (const phrase of ["Событие", "в галерее", "закрыть", "L нравится", "K в календарь", "S ссылка", "C описание", "P афиша", "Выбранная карточка", "Поделиться сервисом", "скопировать карточку «Анонсов»", "скопировать текст и ссылку"]) {
      assert(learningCopy.includes(phrase), `Learning block is missing: ${phrase}`);
    }
  }
  assert(await page.locator(".keyboard-prototype-dock").count() === 0, "No floating service dock is allowed");
  assert(await page.locator(cardSelector).count() === 10, "Both fixtures must expose ten related cards");
  assert(await page.locator("[data-related-start] [data-calendar-action] [data-related-calendar-shortcut]").count() === 10, "Every eligible related calendar must receive one K hint");
  assert(await page.locator("[data-related-start] [data-calendar-action] [data-related-calendar-shortcut]:visible").count() === 0, "Unfocused cards must not show K hints");
  await page.locator(cardSelector).nth(4).hover();
  assert(await page.locator("[data-related-start] [data-related-calendar-shortcut]:visible").count() === 0, "Hover alone must not show a K hint");
  await page.locator(cardSelector).first().focus();
  assert(await page.locator("[data-related-start] [data-related-calendar-shortcut]:visible").count() === 1, "Exactly one focused card must show a K hint");
  assert(await page.locator(`${cardSelector}:focus-within [data-related-calendar-shortcut]:visible`).count() === 1, "The visible K hint must belong to the focused card");
  assert(await page.locator("[data-event-content-copy-actions]").count() === 1, "Expected one description action group in the desktop description");
  assert(await page.locator(".desktop-clean-description__text + [data-event-content-copy-actions]").count() === 1, "Copy controls must follow the full description text");
  assert(await page.locator("[data-copy-event-poster][aria-keyshortcuts=P] kbd", { hasText: "P" }).count() === 1, "Poster control must visibly and accessibly expose P");

  // AR-11: Down follows title → every rendered prose paragraph → practical
  // summary → related cards. It is not a scroll amount or timing gesture.
  await page.evaluate(() => { window.scrollTo(0, 0); document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true }); });
  await page.keyboard.press("ArrowDown");
  assert(await page.locator("[data-keyboard-reading-stop=title]:focus").count() === 1, "First Down must focus the event title");
  const expectedReadingStops = await page.evaluate(() => [
    ...document.querySelectorAll(".desktop-clean-description__lead, .desktop-clean-description__text p, .desktop-clean-practical"),
  ].filter((node) => {
    const style = getComputedStyle(node);
    const rect = node.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
  }).length);
  for (let index = 0; index < expectedReadingStops; index += 1) await page.keyboard.press("ArrowDown");
  report.semanticReading = await activeState();
  assert(report.semanticReading.cardRoot, "Down after practical summary must enter the first related card");

  // Reverse the same semantic route: the first related row returns to the
  // practical summary, then paragraphs, title and finally the action surface.
  await page.keyboard.press("ArrowUp");
  assert(await page.locator("[data-keyboard-reading-stop=practical]:focus").count() === 1, "Up from first card must focus practical summary");
  for (let index = 0; index < expectedReadingStops - 1; index += 1) await page.keyboard.press("ArrowUp");
  assert(await page.locator("[data-keyboard-reading-stop=title]:focus").count() === 1, "Reverse reading must reach title before hero controls");
  await page.keyboard.press("ArrowUp");
  assert((await activeState()).surface, "Up from title must return to the event action surface");

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

  // Fresh Up opens the gallery. Down closes it as one isolated gesture and
  // returns the logical owner without leaking into page scrolling.
  await page.keyboard.press("ArrowUp");
  const gallery = page.locator("[data-hero-gallery]:not([hidden]).is-open");
  await gallery.waitFor();
  report.galleryOpen = await gallery.evaluate((node) => ({ ownsFocus: node.contains(document.activeElement), parentIsBody: node.parentElement === document.body }));
  assert(report.galleryOpen.ownsFocus && report.galleryOpen.parentIsBody, "Fresh Up at event top must open and focus the fullscreen gallery");
  const galleryDownScroll = await page.evaluate(() => window.scrollY);
  await page.keyboard.down("ArrowDown");
  await waitFor("gallery ArrowDown close", () => !document.querySelector("[data-hero-gallery].is-open"));
  await waitFor("gallery ArrowDown focus return", () => document.activeElement?.hasAttribute?.("data-keyboard-event-surface"));
  assert(await page.evaluate((before) => window.scrollY === before, galleryDownScroll), "Gallery ArrowDown must close without scrolling the page beneath it");
  await page.locator(surfaceSelector).evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowDown", key: "ArrowDown", bubbles: true, cancelable: true })));
  await page.waitForTimeout(60);
  assert(await page.evaluate((before) => window.scrollY === before, galleryDownScroll), "A held gallery Down must not leak into a page step after focus restoration");
  await page.keyboard.up("ArrowDown");
  assert(await page.evaluate(() => Object.values(JSON.parse(localStorage.getItem("ke_keyboard_shortcut_daily_v2") || '{"days":{}}').days).flat().includes("gallery_close_down")), "Completed gallery ArrowDown close must record one compact daily fact");
  await page.keyboard.press("ArrowDown");
  assert(await page.locator("[data-keyboard-reading-stop=title]:focus").count() === 1, "A new Down after gallery close must restart semantic reading at the title");
  await page.evaluate(() => { window.scrollTo({ top: 0, behavior: "instant" }); document.querySelector("[data-keyboard-event-surface]")?.focus({ preventScroll: true }); });

  // Escape remains an equivalent gallery exit and Enter/Space activate the
  // final related-event CTA.
  await page.keyboard.press("ArrowUp");
  await gallery.waitFor();
  await page.keyboard.press("Escape");
  await waitFor("gallery Escape close", () => !document.querySelector("[data-hero-gallery].is-open"));
  await waitFor("gallery Escape focus return", () => document.activeElement?.hasAttribute?.("data-keyboard-event-surface"));
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "instant" }));
  await page.keyboard.press("ArrowDown");
  assert(await page.locator("[data-keyboard-reading-stop=title]:focus").count() === 1, "Semantic Down must resume immediately after gallery Escape");
  await page.keyboard.press("Escape");
  await page.evaluate(() => window.scrollTo({ top: 0, behavior: "instant" }));
  await page.keyboard.press("ArrowUp");
  await gallery.waitFor();
  assert(await gallery.evaluate((node) => node.contains(document.activeElement)), "ArrowUp must work immediately after gallery Escape restoration");
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
  await page.keyboard.press("Escape");
  await waitFor("gallery close", () => !document.querySelector("[data-hero-gallery].is-open"));
  await waitFor("gallery logical focus return", () => document.activeElement?.hasAttribute?.("data-keyboard-event-surface"));

  // A pointer-opened gallery that switches to keyboard Down also receives a
  // logical keyboard owner and returns to the surface without background scroll.
  const pointerGalleryScroll = await page.evaluate(() => window.scrollY);
  await page.locator("[data-hero-gallery-open]").first().click();
  await gallery.waitFor();
  await page.keyboard.press("ArrowDown");
  await waitFor("pointer gallery ArrowDown close", () => !document.querySelector("[data-hero-gallery].is-open"));
  await waitFor("pointer gallery ArrowDown owner return", () => document.activeElement?.hasAttribute?.("data-keyboard-event-surface"));
  assert(await page.evaluate((before) => window.scrollY === before, pointerGalleryScroll), "Pointer-opened gallery Down must not scroll the covered page");

  // C copies title, rendered lead/body and URL; scoped P copies the canonical poster.
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
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyC", key: "с", bubbles: true, cancelable: true })));
  assert(await page.evaluate((count) => window.__keyboardClipboardText.length === count + 1, descriptionCopyCount), "Physical KeyC remains repeatable in Cyrillic layout");
  const factsAfterDescription = await page.evaluate(() => JSON.parse(localStorage.getItem("ke_keyboard_shortcut_daily_v2") || "null"));
  const currentFactActions = Object.values(factsAfterDescription.days).flat();
  assert(currentFactActions.filter((value) => value === "copy_description").length === 1, "Repeated C use must leave one daily description fact");
  assert(!JSON.stringify(factsAfterDescription).match(/6408|6593|https?:|title|url|timestamp|route/iu), "Daily fact storage must not contain event identity, URL, route or raw timestamps");
  await page.locator(surfaceSelector).focus();
  await page.keyboard.press("p");
  await waitFor("poster clipboard", () => window.__keyboardClipboardImages.length > 0);
  report.posterCopy = await page.evaluate(() => window.__keyboardClipboardImages.at(-1));
  assert(report.posterCopy.length === 1 && report.posterCopy[0].length === 1 && report.posterCopy[0][0] === "image/png", "Scoped P must write exactly one PNG ClipboardItem");
  const posterWrites = await page.evaluate(() => window.__keyboardClipboardImages.length);
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", bubbles: true, cancelable: true })));
  await waitFor("Russian-layout event poster", (count) => window.__keyboardClipboardImages.length === count + 1, posterWrites);
  const posterFacts = await page.evaluate(() => JSON.parse(localStorage.getItem("ke_keyboard_shortcut_daily_v2") || "null"));
  assert(Object.values(posterFacts.days).flat().filter((value) => value === "copy_event_poster").length === 1, "Repeated event P must leave one completed daily poster fact");
  assert(await page.evaluate(() => window.__keyboardNativeShareCalls) === 0, "Event copy actions must never call navigator.share on desktop");

  // Consent keyboard flow and visible CTA states are fixture-independent.
  await page.locator(surfaceSelector).focus();
  await page.keyboard.press("l");
  const consent = page.locator("[data-personalization-consent].is-visible");
  await consent.waitFor();
  await waitFor("consent focus entry", () => document.activeElement?.hasAttribute?.("data-personalization-consent-accept"));
  assert(await consent.evaluate((node) => node.contains(document.activeElement)), "L must move focus into the lazy consent dialog");
  await page.keyboard.press("Escape");
  assert(await page.locator("[data-personalization-consent].is-visible").count() === 0, "Escape must decline consent");
  await waitFor("consent decline focus return", () => document.activeElement?.hasAttribute?.("data-keyboard-event-surface"));
  await page.keyboard.press("l");
  await consent.waitFor();
  await waitFor("reopened consent focus entry", () => document.activeElement?.hasAttribute?.("data-personalization-consent-accept"));
  await page.keyboard.press("Enter");
  await waitFor("like consent replay", () => document.querySelector("[data-keyboard-event-surface] [data-feedback-action=like]")?.getAttribute("aria-pressed") === "true");
  await waitFor("consent accept focus return", () => document.activeElement?.hasAttribute?.("data-keyboard-event-surface"));
  report.currentLike = await page.locator(`${surfaceSelector} [data-feedback-action=like]`).evaluate((button) => ({
    pressed: button.getAttribute("aria-pressed"), color: getComputedStyle(button).backgroundColor,
    eventId: button.getAttribute("data-event-id"), base: Number(button.getAttribute("data-base-count") || 0),
    count: Number(button.querySelector("[data-feedback-count]")?.textContent || 0),
  }));
  assert(report.currentLike.eventId === report.eventId && report.currentLike.pressed === "true" && report.currentLike.color === "rgb(201, 52, 52)" && report.currentLike.count === report.currentLike.base + 1, "L + consent must leave the current event visibly liked");
  await waitFor("consented like daily fact", () => window.__keyboardDailyFacts.some((fact) => fact.action_code === "like_toggle"));
  const remoteFact = await page.evaluate(() => window.__keyboardDailyFacts.find((fact) => fact.action_code === "like_toggle"));
  assert(Object.keys(remoteFact).sort().join(",") === "action_code,schema_version", "Consent-gated collector event must contain only schema and allowlisted action");

  // If a managed DOM transition leaves BODY active, L recovers the logical
  // event owner instead of becoming a silent no-op.
  await page.evaluate(() => document.activeElement?.blur());
  assert((await activeState()).tag === "BODY", "Lost-focus fixture must put focus on BODY");
  await page.keyboard.press("l");
  await waitFor("lost-focus L unlike", () => document.querySelector("[data-keyboard-event-surface] [data-feedback-action=like]")?.getAttribute("aria-pressed") === "false");
  assert((await activeState()).surface, "Lost-focus L must re-enter the current-event keyboard surface");
  await page.evaluate(() => document.activeElement?.blur());
  await page.evaluate(() => document.body.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyL", key: "д", bubbles: true, cancelable: true })));
  await waitFor("lost-focus L relike", () => document.querySelector("[data-keyboard-event-surface] [data-feedback-action=like]")?.getAttribute("aria-pressed") === "true");
  assert((await activeState()).surface, "Repeated lost-focus L recovery must remain stable");

  // Explicit unrelated pointer ownership disarms BODY recovery, and editable
  // targets keep character input native.
  const surfaceLikeBeforeNegative = await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed");
  for (const unrelated of [".site-header", ".site-footer"]) {
    await page.locator(surfaceSelector).focus();
    await page.locator(unrelated).dispatchEvent("pointerdown");
    await page.evaluate(() => document.activeElement?.blur());
    await page.keyboard.press("l");
    await page.waitForTimeout(60);
    assert(await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed") === surfaceLikeBeforeNegative, `BODY L must remain unowned after ${unrelated} pointer ownership`);
  }
  await page.evaluate(() => {
    const input = document.createElement("input");
    input.dataset.keyboardTestEditor = "";
    document.querySelector("[data-keyboard-event-surface]")?.append(input);
    input.focus({ preventScroll: true });
  });
  await page.keyboard.type("l");
  assert(await page.locator("[data-keyboard-test-editor]").inputValue() === "l", "Editable L must remain native text input");
  assert(await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed") === surfaceLikeBeforeNegative, "Editable L must not trigger like");
  await page.locator("[data-keyboard-test-editor]").evaluate((node) => node.remove());
  await page.evaluate(() => {
    const editor = document.createElement("div");
    editor.contentEditable = "true";
    editor.dataset.keyboardTestContenteditable = "";
    document.querySelector("[data-keyboard-event-surface]")?.append(editor);
    editor.focus({ preventScroll: true });
  });
  const likeBeforeIme = await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed");
  await page.locator("[data-keyboard-test-contenteditable]").evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyL", key: "д", isComposing: true, bubbles: true, cancelable: true })));
  assert(await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed") === likeBeforeIme, "IME composition in contenteditable must not trigger L");
  await page.locator("[data-keyboard-test-contenteditable]").evaluate((node) => node.remove());

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
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyK", key: "л", bubbles: true, cancelable: true })));
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
  const firstCardLikeBeforeRecovery = await firstCard.locator('[data-feedback-action="like"]').getAttribute("aria-pressed");
  await page.evaluate(() => document.activeElement?.blur());
  await page.keyboard.press("l");
  await waitFor("card lost-focus L recovery", ([id, before]) => {
    const card = document.querySelector(`[data-related-start] [data-event-card][data-event-id="${id}"]`);
    return document.activeElement === card && card?.querySelector('[data-feedback-action="like"]')?.getAttribute('aria-pressed') !== before;
  }, [firstCardId, firstCardLikeBeforeRecovery]);
  assert((await activeState()).cardId === firstCardId && (await activeState()).cardRoot, "Lost-focus card L must restore and act on the same logical card");
  await firstCard.evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowRight", key: "ArrowRight", bubbles: true, cancelable: true })));
  const oneRightId = (await activeState()).cardId;
  for (let index = 0; index < 4; index += 1) {
    await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowRight", key: "ArrowRight", repeat: true, bubbles: true, cancelable: true })));
  }
  assert((await activeState()).cardId === oneRightId, "Held/repeated Right must produce at most one semantic card step");
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keyup", { code: "ArrowRight", key: "ArrowRight", bubbles: true })));
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowLeft", key: "ArrowLeft", bubbles: true, cancelable: true })));
  for (let index = 0; index < 4; index += 1) {
    await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowLeft", key: "ArrowLeft", repeat: true, bubbles: true, cancelable: true })));
  }
  assert((await activeState()).cardId === firstCardId, "Held/repeated Left must also produce at most one semantic card step");
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keyup", { code: "ArrowLeft", key: "ArrowLeft", bubbles: true })));
  await firstCard.focus();
  await firstCard.evaluate((node) => node.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowDown", key: "ArrowDown", bubbles: true, cancelable: true })));
  const oneDownId = (await activeState()).cardId;
  for (let index = 0; index < 4; index += 1) {
    await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "ArrowDown", key: "ArrowDown", repeat: true, bubbles: true, cancelable: true })));
  }
  assert((await activeState()).cardId === oneDownId, "Held/repeated Down must produce at most one semantic card step");
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keyup", { code: "ArrowDown", key: "ArrowDown", bubbles: true })));
  await firstCard.focus();
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

  // The graph continues through the dynamically hydrated “Ещё события” zone.
  const lastRelated = page.locator(cardSelector).last();
  await lastRelated.focus();
  await page.keyboard.press("ArrowDown");
  await waitFor("continuation bridge", () => Boolean(document.activeElement?.closest?.('[data-personal-feed-section][data-listing-context="event-detail"] [data-event-card]')));
  report.continuation = await page.evaluate(() => ({
    count: document.querySelectorAll('[data-personal-feed-slot] [data-event-card]').length,
    activeId: document.activeElement?.closest?.('[data-event-card]')?.getAttribute('data-event-id'),
    href: document.activeElement?.closest?.('[data-event-card]')?.getAttribute('data-card-href'),
    inZone: Boolean(document.activeElement?.closest?.('[data-personal-feed-section]')),
  }));
  assert(report.continuation.count === 6 && report.continuation.inZone && report.continuation.href.includes('/sobytiya/') && !report.continuation.href.includes('/preview-'), "Down must bridge into six canonical continuation cards");
  const visibleContinuationCalendars = await page.locator('[data-personal-feed-slot] [data-calendar-action]:visible').count();
  assert(visibleContinuationCalendars > 0 && await page.locator('[data-personal-feed-slot] [data-calendar-action]:visible [data-related-calendar-shortcut]').count() === visibleContinuationCalendars, "Every visible continuation calendar must receive K hint markup");
  assert(await page.locator('[data-personal-feed-slot] [data-related-calendar-shortcut]:visible').count() === 1, "Only the focused continuation card may show its K hint");
  await page.keyboard.press("ArrowUp");
  assert((await activeState()).cardId === await lastRelated.getAttribute("data-event-id"), "Up from the first continuation row must bridge to the last related row");

  const firstContinuation = page.locator('[data-personal-feed-slot] [data-event-card]').first();
  await firstContinuation.focus();
  const continuationShare = firstContinuation.locator('[data-native-share]');
  const expectedContinuationCopy = await continuationShare.evaluate((button) => `${button.getAttribute('data-share-event-title')}\n${button.getAttribute('data-share-url')}`);
  await page.keyboard.press("s");
  await waitFor("continuation share copy", (expected) => window.__keyboardClipboardText?.at(-1) === expected, expectedContinuationCopy);
  await continuationShare.focus();
  await page.keyboard.press("Escape");
  assert((await activeState()).cardRoot, "Escape inside a continuation action must return to its card root");
  const continuationCalendarId = await page.evaluate(() => {
    const today = Math.floor((Date.now() + 2 * 60 * 60 * 1000) / 86400000);
    return Array.from(document.querySelectorAll('[data-personal-feed-slot] [data-event-card]')).find((card) => Number(card.querySelector('[data-calendar-action]')?.getAttribute('data-calendar-expiry-day') || 0) > today)?.getAttribute('data-event-id');
  });
  assert(continuationCalendarId, "Expected one eligible continuation calendar");
  const continuationCalendarCard = page.locator(`[data-personal-feed-slot] [data-event-card][data-event-id="${continuationCalendarId}"]`).first();
  await continuationCalendarCard.focus();
  await page.keyboard.press("k");
  await waitFor("continuation calendar success", (id) => document.querySelector(`[data-personal-feed-slot] [data-event-card][data-event-id="${id}"] [data-calendar-action]`)?.getAttribute('data-calendar-state') === 'added', continuationCalendarId);
  await page.evaluate(() => {
    const card = document.querySelector('[data-personal-feed-slot] [data-event-card]');
    const link = card?.querySelector('[data-card-title][href], [data-card-media-link][href]');
    window.__keyboardContinuationEnter = null;
    link?.addEventListener('click', (event) => {
      window.__keyboardContinuationEnter = link.href;
      event.preventDefault();
      event.stopImmediatePropagation();
    }, { capture: true, once: true });
    card?.focus({ preventScroll: true });
  });
  await page.keyboard.press("Enter");
  assert(await page.evaluate(() => Boolean(window.__keyboardContinuationEnter?.includes('/sobytiya/'))), "Enter must open the selected continuation card through its canonical link");
  assert(await page.evaluate(() => Object.values(JSON.parse(localStorage.getItem('ke_keyboard_shortcut_daily_v2') || '{"days":{}}').days).flat().includes('card_open')), "Continuation Enter must record one completed card-open fact");
  await firstContinuation.focus();
  const focusedContinuationId = (await activeState()).cardId;
  await page.evaluate(() => {
    const slot = document.querySelector('[data-personal-feed-slot]');
    if (!slot) return;
    slot.replaceChildren(...Array.from(slot.children).map((card) => card.cloneNode(true)));
  });
  await waitFor("continuation rerender focus restoration", (id) => document.activeElement?.closest?.('[data-personal-feed-slot] [data-event-card]')?.getAttribute('data-event-id') === id, focusedContinuationId);
  await page.waitForTimeout(80);
  await waitFor("continuation rerender settled focus", (id) => document.activeElement?.closest?.('[data-personal-feed-slot] [data-event-card]')?.getAttribute('data-event-id') === id, focusedContinuationId);

  // A real feedback action may rerank the feed. Restore the same event when it
  // survives, otherwise the nearest card at the saved zone/index.
  await page.keyboard.press("l");
  await waitFor("continuation like focus preservation", () => Boolean(document.activeElement?.closest?.('[data-personal-feed-slot] [data-event-card]')));
  await page.waitForTimeout(80);
  const feedbackRestore = await page.evaluate((previousId) => {
    const cards = Array.from(document.querySelectorAll('[data-personal-feed-slot] [data-event-card]'));
    const active = document.activeElement?.closest?.('[data-personal-feed-slot] [data-event-card]');
    return {
      activeId: active?.getAttribute('data-event-id') || null,
      expectedId: (cards.find((card) => card.getAttribute('data-event-id') === previousId) || cards[0])?.getAttribute('data-event-id') || null,
    };
  }, focusedContinuationId);
  assert(feedbackRestore.activeId === feedbackRestore.expectedId, `Feedback rerender must preserve the logical card owner: ${JSON.stringify(feedbackRestore)}`);
  await page.keyboard.press("Home");
  const homeState = await activeState();
  assert(homeState.cardId === firstCardId, `Home must reach the first card across both zones: ${JSON.stringify({ homeState, firstCardId })}`);
  await page.keyboard.press("End");
  assert(Boolean(await page.evaluate(() => document.activeElement?.closest?.('[data-personal-feed-slot] [data-event-card]'))), "End must reach the final continuation card across both zones");

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
  await page.evaluate(() => {
    document.activeElement?.blur();
    document.body.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", bubbles: true, cancelable: true }));
  });
  await page.waitForTimeout(80);
  assert(await page.evaluate((count) => window.__keyboardClipboardImages.length === count, imageWritesBeforeFooter), "Body P must not ambiguously copy either event or service image");
  await firstCard.focus();
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", bubbles: true, cancelable: true })));
  await page.waitForTimeout(80);
  assert(await page.evaluate((count) => window.__keyboardClipboardImages.length === count, imageWritesBeforeFooter), "Card P must remain unassigned");
  const footerImageButton = footerShare.locator('[data-service-share-intent="image"]');
  await footerImageButton.focus();
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyP", key: "з", bubbles: true, cancelable: true })));
  await waitFor("footer image clipboard", (count) => window.__keyboardClipboardImages.length === count + 1, imageWritesBeforeFooter);
  const footerTextButton = footerShare.locator('[data-service-share-intent="text"]');
  await footerTextButton.focus();
  await page.evaluate(() => document.activeElement?.dispatchEvent(new KeyboardEvent("keydown", { code: "KeyS", key: "ы", bubbles: true, cancelable: true })));
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
  const likeBeforeDestroy = await page.locator(`${surfaceSelector} [data-feedback-action=like]`).getAttribute("aria-pressed");
  const originalPrimaryTitle = await page.locator(`${surfaceSelector} .desktop-prototype__primary-action`).evaluate((node) => node.hasAttribute("data-desktop-phone-copy") ? "Показать телефон" : null);
  await page.evaluate(() => window.KenigEventsKeyboardNavigation?.destroy?.());
  assert(await page.locator(surfaceSelector).count() === 0, "destroy must remove the keyboard surface contract");
  assert(await page.locator("[data-event-content-copy-actions]").count() === 0, "destroy must remove injected copy controls");
  const teardown = await page.evaluate(() => {
    const panel = document.querySelector("[data-desktop-action-panel]");
    const primary = panel?.querySelector(".desktop-prototype__primary-action");
    const compactCalendar = panel?.querySelector("[data-desktop-action-row] [data-calendar-action]");
    const relatedCalendars = Array.from(document.querySelectorAll("[data-related-start] [data-calendar-action]"));
    return {
      rootScope: document.querySelector("[data-desktop-clean-event]")?.hasAttribute("data-closed-hero-keyboard-scope"),
      panelTabindex: panel?.getAttribute("tabindex"),
      panelShortcuts: panel?.getAttribute("aria-keyshortcuts"),
      panelEventId: panel?.getAttribute("data-event-id"),
      primaryTitle: primary?.getAttribute("title"),
      primaryShortcuts: primary?.getAttribute("aria-keyshortcuts"),
      compactCalendarTitle: compactCalendar?.getAttribute("title") ?? null,
      compactCalendarShortcuts: compactCalendar?.getAttribute("aria-keyshortcuts") ?? null,
      relatedDecorations: relatedCalendars.filter((node) => node.hasAttribute("aria-keyshortcuts") || node.hasAttribute("title") || node.hasAttribute("data-keyboard-shortcut-target")).length,
    };
  });
  assert(teardown.rootScope === false && teardown.panelTabindex === null && teardown.panelShortcuts === null && teardown.panelEventId === null, `destroy must restore the original surface attributes: ${JSON.stringify(teardown)}`);
  assert(teardown.primaryTitle === originalPrimaryTitle && teardown.primaryShortcuts === null, `destroy must restore the original primary CTA attributes: ${JSON.stringify(teardown)}`);
  assert(teardown.compactCalendarTitle === "В календарь" && teardown.compactCalendarShortcuts === null, `destroy must preserve the compact calendar's authored title: ${JSON.stringify(teardown)}`);
  assert(teardown.relatedDecorations === 0, `destroy must remove router-only related-card attributes: ${JSON.stringify(teardown)}`);
  await page.keyboard.press("l");
  assert(await page.locator("[data-desktop-action-panel] [data-feedback-action=like]").getAttribute("aria-pressed") === likeBeforeDestroy, "destroy must remove keyboard listeners");
  report.lifecycleDestroy = "pass";
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
