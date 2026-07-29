import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  INTERACTION_VISUAL_CONTRACTS,
  TOMORROW_MOBILE_CONTRACT,
  selectDeterministicMobileEvent,
} from "../scenario-contract.mjs";

const source = await readFile(new URL("../agent.mjs", import.meta.url), "utf8");

test("declares separate mobile and desktop interaction semantics", () => {
  assert.deepEqual(INTERACTION_VISUAL_CONTRACTS.mobile, {
    pointerVisual: "tap-circle",
    gestureVisual: "swipe-trail-with-direction",
    forbiddenVisuals: ["mouse-cursor"],
  });
  assert.deepEqual(INTERACTION_VISUAL_CONTRACTS.desktop, {
    inputVisual: "currently-pressed-keyboard-keys",
    responseVisual: "ui-response",
  });
  assert.deepEqual(TOMORROW_MOBILE_CONTRACT, {
    id: "tomorrow-mobile",
    surface: "mobile",
    completion: "concrete-event-detail-description-visible-after-horizontal-rail-gesture",
  });
});

test("deterministic event selection prefers the shortest rail then numeric event id", () => {
  assert.deepEqual(
    selectDeterministicMobileEvent([
      { eventId: "902", title: "Second", galleryCount: "1" },
      { eventId: "17", title: "Selected", galleryCount: "1" },
      { eventId: "4", title: "Long gallery", galleryCount: "3" },
    ]),
    { eventId: "17", title: "Selected", galleryCount: 1 },
  );
  assert.equal(
    selectDeterministicMobileEvent([
      { eventId: "", title: "Missing id", galleryCount: 0 },
      { eventId: "7", title: "", galleryCount: 0 },
      { eventId: "not-numeric", title: "Invalid id", galleryCount: 0 },
    ]),
    null,
  );
});

test("tomorrow-mobile reaches a concrete detail description before completion", () => {
  assert.match(source, /data-presenter-id="nav-tomorrow"/);
  assert.match(source, /data-presenter-id="tomorrow-page-ready"/);
  assert.match(source, /data-mobile-v23-page="tomorrow"/);
  assert.match(source, /data-mobile-listing-row\]\[data-event-id\]/);
  assert.match(source, /event-digest\[aria-label="О событии"\]/);
  assert.match(source, /\[data-mobile-event-production\] \.mobile-event-production__prose/);

  const selectIndex = source.indexOf("await this.selectTomorrowEvent(frame)");
  const swipeIndex = source.indexOf("await this.swipeRailTowardDescription");
  const railDwellIndex = source.indexOf(
    'await this.dwellOnDescription(digest, signal, "rail")',
  );
  const detailIndex = source.indexOf(
    "await frame.locator(MOBILE_DETAIL_SELECTOR).waitFor",
  );
  const detailDwellIndex = source.indexOf(
    'await this.dwellOnDescription(detailDescription, signal, "event-detail")',
  );
  assert.ok(
    0 < selectIndex &&
      selectIndex < swipeIndex &&
      swipeIndex < railDwellIndex &&
      railDwellIndex < detailIndex &&
      detailIndex < detailDwellIndex,
  );
  assert.match(
    source,
    /this\.runTomorrowMobile\(this\.runController\.signal\)\s*\.then\(async \(evidence\) => \{/s,
  );
  assert.match(source, /event \$\{evidence\.eventId\} "\$\{evidence\.title\}"/);
  assert.match(
    source,
    /digest revealed after horizontal swipe; detail description visible/,
  );
});

test("mobile uses tap circles and directional touch trails without a mouse cursor", () => {
  assert.match(source, /tap\.dataset\.autopresenterTap = "true"/);
  assert.match(source, /trail\.dataset\.autopresenterSwipeTrail = "true"/);
  assert.match(source, /trail\.dataset\.autopresenterSwipeFingerDirection = "left"/);
  assert.match(source, /trail\.dataset\.autopresenterSwipeContentDirection = "right"/);
  assert.match(source, /←━━━━━━━━.*Листаем событие вправо →/);
  assert.match(source, /await locator\.click\(/);
  assert.match(source, /await this\.page\.mouse\.down\(\)/);
  assert.match(source, /await this\.page\.mouse\.move\(/);
  assert.match(source, /await this\.page\.mouse\.up\(\)/);
  assert.match(source, /cursor:none!important/);
  assert.doesNotMatch(source, /data-autopresenter-cursor|autopresenterCursor/);
  assert.doesNotMatch(source, /\.hover\(/);
});

test("desktop mode visualizes pressed keys and the matching UI response", () => {
  assert.match(source, /presenterInteractionMode !== "desktop"/);
  assert.match(source, /data-autopresenter-keyboard/);
  assert.match(source, /key\.dataset\.autopresenterKeyPressed = "true"/);
  assert.match(source, /pressedKeys\.set\(event\.code/);
  assert.match(source, /pressedKeys\.delete\(event\.code\)/);
  assert.match(source, /presenter:desktop-ui-response/);
  assert.match(source, /window\.addEventListener\("presenter:status"/);
  assert.match(source, /data-autopresenter-ui-response/);
  assert.match(source, /autopresenterUiResponded = "true"/);
});

test("uses real Playwright locator actions and never DOM activation", () => {
  assert.match(source, /await locator\.scrollIntoViewIfNeeded\(\)/);
  assert.match(source, /await locator\.boundingBox\(\)/);
  assert.match(source, /await rail\.boundingBox\(\)/);
  assert.doesNotMatch(source, /element\.click|node\.click|\.evaluate\([^)]*=>[^;]*\.click/s);
});

test("poll, idempotent ack, TTL and bounded hard stop contracts are explicit", () => {
  assert.match(source, /\/api\/commands\/next/);
  assert.match(source, /after_seq/);
  assert.match(source, /ackCache/);
  assert.match(source, /isExpired\(command\)/);
  assert.match(source, /hardStopMs/);
  assert.match(source, /hardRecoverContext/);
  assert.match(source, /agent confirmed stopped/);
});

test("remote relay requests carry the dedicated agent bearer token", () => {
  assert.match(source, /AUTOPRESENTER_AGENT_TOKEN/);
  assert.match(source, /authorization: `Bearer \$\{config\.agentToken\}`/);
  assert.match(source, /headers: this\.authHeaders\(\)/);
});

test("local fallback keys cover run, stop and reset", () => {
  assert.match(source, /event\.code === "Space" \|\| event\.code === "ArrowRight"/);
  assert.match(source, /event\.code === "Escape"/);
  assert.match(source, /event\.code === "KeyR"/);
});

test("stage status, reset, and fixed-resolution evidence contracts remain intact", () => {
  assert.match(source, /new CustomEvent\("presenter:status"/);
  assert.match(source, /this\.context\.pages\(\)/);
  assert.match(source, /extraPage !== this\.page/);
  assert.match(source, /width: 1920, height: 1080/);
  assert.match(source, /deviceScaleFactor: 1/);
  assert.match(source, /tomorrow-mobile-1920x1080\.png/);
  assert.match(source, /recordVideo/);
  assert.match(source, /this\.shutdownPromise/);
  assert.match(source, /await this\.context\?\.close/);
});

test("headed demonstrator forces the native browser window to fullscreen", () => {
  assert.match(source, /"--kiosk"/);
  assert.match(source, /"--start-fullscreen"/);
  assert.match(source, /newCDPSession\(page\)/);
  assert.match(source, /Browser\.getWindowForTarget/);
  assert.match(source, /Browser\.setWindowBounds/);
  assert.match(source, /windowState: "fullscreen"/);
});
