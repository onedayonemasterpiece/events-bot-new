import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  DEFAULT_SCENARIO_ID,
  INTERACTION_VISUAL_CONTRACTS,
  SCENARIO_IDS,
  TOMORROW_MOBILE_CONTRACT,
  TOMORROW_RAIL_LIKE_CONTRACT,
  WEEKEND_AMBER_ARTIFACT_CONTRACT,
  INTRO_LOOP_CONTRACT,
  LECTURE_SCENE_CONTRACTS,
  SERVICE_SCENE_CONTRACTS,
  WEEKEND_DESKTOP_CONTRACT,
  OUTRO_QR_CONTRACT,
  LONG_SCENE_TIMEOUT_CEILING_MS,
  SCENARIO_TIMEOUT_POLICY,
  resolveScenarioId,
  resolveScenarioTimeoutMs,
  selectDeterministicMobileEvent,
} from "../scenario-contract.mjs";
import { PACING } from "../pacing.mjs";

const source = await readFile(new URL("../agent.mjs", import.meta.url), "utf8");
const controlSource = await readFile(
  new URL("../../relay/control/index.html", import.meta.url),
  "utf8",
);
const stageSource = await readFile(
  new URL("../../../../site/src/pages/internal/presenter-stage/index.astro", import.meta.url),
  "utf8",
);
const baseScenarioSource = await readFile(
  new URL(
    "../../../../docs/features/static-site-pages/auto-present/scenario-30072026-base.md",
    import.meta.url,
  ),
  "utf8",
);

test("declares the accepted journeys plus explicit intro, lecture, desktop and outro scenes", () => {
  for (const id of [
    "intro-loop", "lecture-01", "lecture-07", "lecture-ui-ux-path",
    "lecture-convenience-emergence", "lecture-usability-measurement",
    "lecture-good-ui", "lecture-poor-ui",
    "market-01-primary", "market-04-position", "tomorrow-mobile",
    "tomorrow-rail-like", "weekend-amber-artifact", "service-search-live",
    "service-focus-group", "service-laws", "service-keyboard-event",
    "service-navigation-map", "service-social-proof", "service-transport-bus",
    "service-report-problem", "service-share-friends", "service-calendar-memory",
    "service-friends-club", "service-navigation-exhibitions",
    "service-navigation-festivals", "joke-db-01", "joke-db-09",
    "manual-page-home-mobile", "manual-page-home-desktop",
    "manual-page-favorites-mobile", "manual-page-favorites-desktop",
    "weekend-desktop", "outro-qr",
  ]) assert.ok(SCENARIO_IDS.includes(id), `${id} is allowlisted`);
  assert.equal(DEFAULT_SCENARIO_ID, "tomorrow-mobile");
  assert.equal(resolveScenarioId(undefined), "tomorrow-mobile");
  assert.equal(resolveScenarioId("tomorrow-rail-like"), "tomorrow-rail-like");
  assert.throws(() => resolveScenarioId("invented"), /unsupported scenario/u);
  assert.deepEqual(TOMORROW_MOBILE_CONTRACT, {
    id: "tomorrow-mobile",
    surface: "mobile",
    completion: "concrete-event-detail-description-visible-after-horizontal-rail-gesture",
  });
  assert.equal(TOMORROW_RAIL_LIKE_CONTRACT.eventId, 5297);
  assert.equal(TOMORROW_RAIL_LIKE_CONTRACT.eventTitle, "Фестиваль Pianissimo: Игорь Сидоров");
  assert.equal(WEEKEND_AMBER_ARTIFACT_CONTRACT.snapshotEventId, 7164);
  assert.equal(INTRO_LOOP_CONTRACT.completion, "fifty-minute-logical-randomized-two-line-hero-talk-loop");
  assert.equal(LECTURE_SCENE_CONTRACTS.length, 12);
  assert.ok(LECTURE_SCENE_CONTRACTS.every(({ completion }) => completion === "held-until-another-explicit-command"));
  assert.ok(SERVICE_SCENE_CONTRACTS.some(({ id }) => id === "service-search-live"));
  assert.ok(SERVICE_SCENE_CONTRACTS.some(({ id }) => id === "service-personalization"));
  assert.ok(SERVICE_SCENE_CONTRACTS.some(({ id }) => id === "service-transport-rail"));
  assert.ok(SERVICE_SCENE_CONTRACTS.some(({ id }) => id === "service-navigation-exhibitions"));
  assert.ok(SERVICE_SCENE_CONTRACTS.some(({ id }) => id === "service-navigation-festivals"));
  assert.equal(WEEKEND_DESKTOP_CONTRACT.surface, "desktop");
  assert.deepEqual(OUTRO_QR_CONTRACT, {
    id: "outro-qr",
    surface: "stage",
    completion: "fullscreen-survey-qr-loaded-and-visible",
  });
  assert.match(source, /resolveScenarioId\(command\.scenario\)/u);
  assert.match(source, /if \(scenarioId === TOMORROW_MOBILE_CONTRACT\.id\)/u);
  assert.match(source, /if \(scenarioId === TOMORROW_RAIL_LIKE_CONTRACT\.id\)/u);
  assert.match(source, /if \(scenarioId === WEEKEND_AMBER_ARTIFACT_CONTRACT\.id\)/u);
  assert.match(source, /if \(scenarioId === INTRO_LOOP_CONTRACT\.id\)/u);
  assert.match(source, /isStaticPresentationScenario\(scenarioId\)/u);
  assert.match(source, /scenarioId === "service-search-live"/u);
  assert.match(source, /scenarioId === FOCUS_INVITATION_SCENE_ID/u);
  assert.match(source, /if \(scenarioId === WEEKEND_DESKTOP_CONTRACT\.id\)/u);
  assert.match(source, /if \(scenarioId === OUTRO_QR_CONTRACT\.id\)/u);
  assert.match(source, /async runManualPage\(scenarioId, signal\)/u);
  assert.match(source, /left untouched for manual pult scrolling/u);
});

test("lecture controls follow the eleven-point author order", () => {
  assert.ok(baseScenarioSource.indexOf("10. Хороший UI") > baseScenarioSource.indexOf("9. Удобство это результат"));
  assert.ok(baseScenarioSource.indexOf("11. Так себе UI") > baseScenarioSource.indexOf("10. Хороший UI"));
  const lectureAt = controlSource.indexOf("<summary>Лекция · 11 пунктов");
  const marketAt = controlSource.indexOf("<summary>Рынок конкурентов");
  assert.ok(lectureAt > 0 && marketAt > lectureAt);
  const lectureBlock = controlSource.slice(lectureAt, marketAt);
  const expected = [
    "lecture-01",
    "lecture-02",
    "lecture-ui-ux-path",
    "lecture-03",
    "lecture-04",
    "lecture-05",
    "lecture-06",
    "lecture-convenience-emergence",
    "lecture-usability-measurement",
    "lecture-07",
    "lecture-good-ui",
    "lecture-poor-ui",
  ];
  let previous = -1;
  for (const scenario of expected) {
    const current = lectureBlock.indexOf(`data-scenario="${scenario}"`);
    assert.ok(current > previous, `${scenario} follows the author order`);
    previous = current;
  }
  assert.match(stageSource, /data-lecture-image-first="true"/u);
  assert.match(stageSource, /pause\(10_000, token\)/u);
  assert.match(stageSource, /Т-Банк/u);
  assert.match(stageSource, /Яндекс Go/u);
  assert.match(stageSource, /Wildberries/u);
  assert.match(stageSource, /ВКонтакте/u);
  assert.match(stageSource, /Авито/u);
});

test("scenario timeout policy is explicit and can admit a future one-hour scene", () => {
  assert.equal(Object.keys(SCENARIO_TIMEOUT_POLICY).length, SCENARIO_IDS.length);
  assert.equal(resolveScenarioTimeoutMs("intro-loop"), 3_600_000);
  assert.equal(resolveScenarioTimeoutMs("lecture-01"), 30_000);
  assert.equal(resolveScenarioTimeoutMs("tomorrow-mobile"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("tomorrow-rail-like"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("weekend-amber-artifact"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("service-focus-group"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("weekend-desktop"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("service-navigation-exhibitions"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("service-navigation-festivals"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("service-report-problem"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("service-transport-bus"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("manual-page-home-mobile"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("manual-page-home-desktop"), 120_000);
  assert.equal(resolveScenarioTimeoutMs("outro-qr"), 30_000);
  assert.equal(LONG_SCENE_TIMEOUT_CEILING_MS, 3_600_000);
  assert.equal(
    resolveScenarioTimeoutMs("future-hour-scene", {
      "future-hour-scene": LONG_SCENE_TIMEOUT_CEILING_MS,
    }),
    3_600_000,
  );
  assert.throws(
    () => resolveScenarioTimeoutMs("future-too-long", { "future-too-long": 3_600_001 }),
    /needs an explicit timeout/u,
  );
  assert.match(source, /const timeoutMs = resolveScenarioTimeoutMs\(scenarioId\)/u);
  assert.doesNotMatch(source, /PACING\.scenarioMaxMs/u);
});

test("manual page reserve follows the author order and never auto-scrolls", () => {
  const serviceAt = controlSource.indexOf("<summary>Презентация сервиса");
  const manualAt = controlSource.indexOf("<summary>Демонстрация страниц · ручная");
  const outroAt = controlSource.indexOf("<summary>Аутро");
  const jokesAt = controlSource.indexOf("<summary>База шуток");
  assert.ok(serviceAt > 0 && manualAt > serviceAt && outroAt > manualAt && jokesAt > outroAt);
  const manualBlock = controlSource.slice(manualAt, outroAt);
  assert.equal((manualBlock.match(/data-scenario="manual-page-/gu) || []).length, 30);
  for (const slug of [
    "home", "mobile-menu", "event-wide-hero", "event-ocr-split", "festivals",
    "exhibitions", "tomorrow", "weekend", "unusual", "clubs", "free", "popular",
    "partners", "search", "favorites",
  ]) {
    const mobileAt = manualBlock.indexOf(`data-scenario="manual-page-${slug}-mobile"`);
    const desktopAt = manualBlock.indexOf(`data-scenario="manual-page-${slug}-desktop"`);
    assert.ok(mobileAt >= 0 && desktopAt > mobileAt, `${slug} keeps mobile then desktop`);
  }
  const start = source.indexOf("async runManualPage");
  const end = source.indexOf("\n  async handleStop", start);
  const runner = source.slice(start, end);
  assert.match(runner, /showPresenterScene\(stageSceneId, signal/u);
  assert.doesNotMatch(runner, /naturalVerticalScroll|mouse\.wheel|scrollIntoViewIfNeeded/u);
  assert.match(stageSource, /data-presenter-scene-id="manual-page-mobile"/u);
  assert.match(stageSource, /data-presenter-scene-id="manual-page-desktop"/u);
});

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
});

test("desktop D-pad is a state-preserving iframe keyboard overlay", () => {
  const start = source.indexOf("async handleDesktopNavigation");
  const end = source.indexOf("\n  isExpired", start);
  const handler = source.slice(start, end);
  assert.ok(start > 0 && end > start);
  assert.match(handler, /up: "ArrowUp"[\s\S]*left: "ArrowLeft"[\s\S]*down: "ArrowDown"[\s\S]*right: "ArrowRight"/u);
  assert.match(handler, /contentFrame\(\)/u);
  assert.match(handler, /frame\.locator\(":focus"\)\.press\(key\)/u);
  assert.match(handler, /presenter:desktop-key-visual/u);
  assert.match(handler, /presenter:desktop-ui-response/u);
  assert.doesNotMatch(handler, /handleRun|confirmStopped|openStage|page\.goto/u);
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

test("pacing contract is bounded, sampled, and typical duration is explicit", () => {
  assert.deepEqual(PACING, {
    scenarioTypicalMinMs: 12_000,
    verticalVelocityPxPerSecond: 850,
    verticalMinDurationMs: 650,
    verticalMaxDurationMs: 2_600,
    verticalSampleMs: 55,
    verticalFinalCorrectionPx: 120,
    settleSampleMs: 50,
    settleStableSamples: 3,
    settleMaxMs: 1_200,
    tapLeadMs: 360,
    routeDwellMs: 1_100,
    railSteps: 24,
    railStepMs: 45,
  });
  assert.match(source, /buildVerticalWheelTrajectory\(geometry\.deltaY\)/u);
  assert.match(source, /await this\.page\.mouse\.wheel\(0, step\.deltaY\)/u);
  assert.match(source, /stable >= PACING\.settleStableSamples/u);
  assert.match(source, /Date\.now\(\) - startedAt <= PACING\.settleMaxMs/u);
});

test("readiness waits for document, fonts, optional mobile ready marker, and visible media", () => {
  assert.match(source, /document\.readyState !== "complete"/u);
  assert.match(source, /await document\.fonts\?\.ready/u);
  assert.match(source, /data-mobile-v23-ready/u);
  assert.match(source, /waitForVisibleMediaSettled/u);
  assert.match(source, /pendingImages/u);
  assert.match(source, /pendingVideos/u);
  assert.match(source, /pendingMediaStates/u);
  assert.match(source, /rect\.right > 0/u);
  assert.match(source, /rect\.left < innerWidth/u);
  assert.match(source, /deadlineMs: 12_000/u);
  assert.match(source, /continuing with scenario-specific gates/u);
  assert.match(source, /initial vertical scroll did not settle; continuing with the scenario target gate/u);
  assert.doesNotMatch(source, /visible embedded media did not settle/u);
});

test("tomorrow-mobile naturally reveals a concrete detail description", () => {
  assert.match(source, /\[data-mobile-discovery-menu\] > summary/u);
  assert.match(source, /nav\[aria-label="Быстрый выбор даты"\] a\[href\$="\/zavtra\/"\]/u);
  assert.match(source, /await abortableDelay\(2_200, signal\)/u);
  assert.match(source, /data-mobile-v23-ready="true"/u);
  assert.match(source, /data-mobile-v23-page="tomorrow"/u);
  assert.match(source, /event-digest\[aria-label="О событии"\]/u);
  assert.match(source, /\[data-mobile-event-production\] \.mobile-event-production__prose/u);
  assert.match(source, /await this\.naturalVerticalScroll\(frame, row, signal\)/u);
  assert.match(source, /await this\.swipeRailLeft\(frame, rail, signal, "Листаем к описанию"\)/u);
  assert.match(source, /await this\.naturalVerticalScroll\(frame, detailDescription, signal\)/u);
});

test("tap helper has a visible 360ms lead and no hidden scrolling", () => {
  const start = source.indexOf("async tapMobileLocator");
  const end = source.indexOf("\n  async showSwipeCue", start);
  assert.ok(start > 0 && end > start);
  const tapHelper = source.slice(start, end);
  assert.match(tapHelper, /tap\.dataset\.autopresenterTap = "true"/u);
  assert.match(tapHelper, /await abortableDelay\(PACING\.tapLeadMs, signal\)/u);
  assert.match(tapHelper, /locator\.click\(\{ timeout: 5_000, force \}\)/u);
  assert.match(tapHelper, /locator\.dispatchEvent\("click"\)/u);
  assert.doesNotMatch(tapHelper, /scrollIntoView|scrollTop|scrollLeft|mouse\.wheel/u);
});

test("mobile hides the pointer and shows directional rail cues", () => {
  assert.match(source, /cursor:none!important/u);
  assert.match(source, /trail\.dataset\.autopresenterSwipeTrail = "true"/u);
  assert.match(source, /trail\.dataset\.autopresenterSwipeFingerDirection = cue\.direction/u);
  assert.match(source, /"←━━━━━━━━"/u);
  assert.doesNotMatch(source, /data-autopresenter-cursor|autopresenterCursor/u);
  assert.doesNotMatch(source, /\.hover\(/u);
});

test("tomorrow like is armed before mouseup and never agent-clicked", () => {
  const start = source.indexOf("async runTomorrowRailLike");
  const end = source.indexOf("\n  async runWeekendAmberArtifact", start);
  const likeScenario = source.slice(start, end);
  assert.match(likeScenario, /dragRailToEndInOneRelease/u);
  assert.match(source, /maxScroll - geometry\.scrollLeft <= 1/u);
  assert.match(source, /const pull = Math\.max\(132,/u);
  const callbackIndex = source.indexOf("if (beforeMouseUp) await beforeMouseUp()");
  const upIndex = source.indexOf("await this.page.mouse.up()", callbackIndex);
  assert.ok(callbackIndex > 0 && upIndex > callbackIndex);
  assert.match(source, /beforeMouseUp: async \(\) => \{[\s\S]*classList\.contains\("is-like-armed"\)/u);
  assert.match(likeScenario, /data-personalization-consent-accept/u);
  assert.match(likeScenario, /consentAccept, signal, \{ dispatch: true \}/u);
  assert.match(likeScenario, /waitForConsentProfile\(frame, signal\)/u);
  assert.match(source, /liked_event_ids/u);
  assert.match(source, /event_id/u);
  assert.match(likeScenario, /like count did not increment exactly once/u);
  assert.match(likeScenario, /RAIL СДВИНУТ ВПРАВО/u);
  assert.match(likeScenario, /ЛАЙК СОХРАНЁН/u);
  assert.doesNotMatch(likeScenario, /like\.click\(|locator\('\[data-feedback-action="like"\]'\)\.click/u);
});

test("weekend artifact uses the visible menu, real rails, storage, reload and dialog", () => {
  assert.match(source, /\[data-mobile-discovery-menu\] > summary/u);
  assert.match(source, /nav\[aria-label="Быстрый выбор даты"\] a\[href\$="\/vyhodnye\/"\]/u);
  assert.match(source, /data-date-listing="weekend"\]\[data-amber-artifact-research="tail"/u);
  assert.match(source, /data-amber-artifact-event-id/u);
  assert.match(source, /weekend artifact snapshot drift/u);
  assert.match(source, /ke_artifact_collection_v1/u);
  assert.match(source, /kenigevents:artifact-collected/u);
  assert.match(source, /first artifact tap changed URL/u);
  assert.match(source, /\/artefakty\/#amber_cosmonaut/u);
  assert.match(source, /data-artifact-dialog/u);
  assert.match(source, /found count/u);
  assert.match(source, /АРТЕФАКТ НАЙДЕН/u);
  assert.match(source, /КОЛЛЕКЦИЯ · НАЙДЕНО 1/u);
  assert.doesNotMatch(source, /scrollLeft\s*=/u);
});

test("search has an explicit one-time persistent Yandex demo-session setup", () => {
  assert.match(source, /async runSearchAuthSetup\(signal\)/u);
  assert.match(source, /SEARCH_AUTH_SETUP_SCENE_ID/u);
  assert.match(source, /data-search-login/u);
  assert.match(source, /data-search-logout/u);
  assert.match(source, /9 \* 60 \* 1_000/u);
  assert.match(source, /this\.context\.storageState\(\{ path: config\.storageStatePath \}\)/u);
  assert.match(source, /const authPage = await this\.context\.newPage\(\)/u);
  assert.match(source, /this\.auxiliaryPage = authPage/u);
  assert.match(source, /await this\.ensureStageReady\(`run \$\{scenarioId\}`\)/u);
  assert.doesNotMatch(
    source.match(/async runSearchAuthSetup\(signal\) \{[\\s\\S]*?\\n  \}/u)?.[0] || "",
    /this\.page\.goto/u,
  );
  assert.doesNotMatch(source, /password|service_role/iu);
});

test("relay, lifecycle, fullscreen, and evidence contracts remain intact", () => {
  assert.match(source, /\/api\/commands\/next/u);
  assert.match(source, /after_seq/u);
  assert.match(source, /ackCache/u);
  assert.match(source, /isExpired\(command\)/u);
  assert.match(source, /hardStopMs/u);
  assert.match(source, /recoverPersistentStage/u);
  assert.match(source, /AUTOPRESENTER_AGENT_TOKEN/u);
  assert.match(source, /authorization: `Bearer \$\{config\.agentToken\}`/u);
  assert.match(source, /AUTOPRESENTER_DEPENDENCY_ROOT/u);
  assert.match(source, /recordVideo/u);
  assert.match(source, /`\$\{scenarioId\}-1920x1080\.png`/u);
  assert.match(source, /"--kiosk"/u);
  assert.match(source, /"--start-fullscreen"/u);
  assert.match(source, /"--autoplay-policy=no-user-gesture-required"/u);
  assert.match(source, /Browser\.setWindowBounds/u);
  assert.match(source, /windowState: "fullscreen"/u);
  assert.match(source, /this\.shutdownPromise/u);
});

test("intro, held lecture and desktop scenes use pinned assets and real FHD scrolling", () => {
  assert.match(source, /async runIntroLoop\(signal, options = \{\}\)/u);
  assert.match(source, /config\.introRuntimeMs/u);
  assert.match(source, /INTRO_MUSIC_ASSET\.url/u);
  assert.match(source, /INTRO_MUSIC_ASSETS\.length/u);
  assert.match(source, /ZNANIE_LOGO_ASSET\.url/u);
  assert.match(source, /async runHeldPresentationScene\(scenarioId, signal\)/u);
  assert.match(source, /LECTURE_SCENES\.find/u);
  assert.match(source, /async runFocusInvitation\(signal\)/u);
  assert.match(source, /focus QR points to unexpected URL/u);
  assert.match(source, /focus invitation QR SVG is missing/u);
  assert.match(source, /await this\.naturalVerticalScroll\(frame, inlineMedallions, signal, frameSelector\)/u);
  assert.match(source, /desktop example has no enabled top medallion slot/u);
  assert.match(source, /tokenCount >= \(mode === "desktop" \? 2 : 1\)/u);
  assert.match(source, /supplied focus-preview event 6865/u);
  assert.match(source, /ratio >= 1\.45/u);
  assert.match(source, /async runWeekendDesktop\(signal\)/u);
  assert.match(source, /setInteractionMode\("desktop-passive"\)/u);
  assert.match(source, /WEEKEND_DESKTOP_ROOT_SELECTOR/u);
  assert.match(source, /SITE_FOOTER_SELECTOR/u);
  assert.match(
    source,
    /naturalVerticalScroll\([\s\S]*?DESKTOP_FRAME_SELECTOR/u,
  );
  assert.match(source, /iframeSelector = FRAME_SELECTOR/u);
});

test("normal scenarios reuse the sole page and clear embedded state without closing context", () => {
  const prepareStart = source.indexOf("async prepareScenarioStage");
  const prepareEnd = source.indexOf("\n  async openTomorrowFromHome", prepareStart);
  const prepare = source.slice(prepareStart, prepareEnd);
  assert.ok(prepareStart > 0 && prepareEnd > prepareStart);
  assert.match(prepare, /this\.openStage\(this\.page\)/u);
  assert.match(prepare, /this\.resetEmbeddedState\(frame, signal\)/u);
  assert.doesNotMatch(prepare, /freshContext|createContextAndStage|context.*close/u);
  assert.doesNotMatch(source, /freshContext/u);
  assert.doesNotMatch(source, /oldContext/u);
  assert.match(source, /kenigevents:focus-participation:v1/u);
  assert.doesNotMatch(source, /localStorage\.clear\(\)/u);
  assert.match(source, /sessionStorage\.clear\(\)/u);
  assert.match(source, /await this\.reloadEmbeddedFrame\(signal\)/u);
  assert.equal(source.match(/this\.context\?\.close\(\)/gu)?.length, 1);
});

test("fullscreen outro switches the existing stage without navigation or context recreation", () => {
  const outroStart = source.indexOf("async runOutroQr");
  const nextMethod = source.indexOf("async openTomorrowFromHome", outroStart);
  assert.ok(outroStart >= 0 && nextMethod > outroStart);
  const outro = source.slice(outroStart, nextMethod);
  assert.match(outro, /showPresenterScene\(OUTRO_SCENE_ID, signal\)/u);
  assert.doesNotMatch(outro, /openStage|createContextAndStage|context.*close/u);
});

test("a new run is a bounded cooperative scene switch, never already-running rejection", () => {
  assert.match(source, /switching \$\{previousScenario\} → \$\{scenarioId\}/u);
  assert.match(source, /await this\.confirmStopped\(`scene switch to \$\{scenarioId\}`\)/u);
  assert.match(source, /abortableDelay\(config\.hardStopMs\)/u);
  assert.doesNotMatch(source, /is already running/u);
});

test("remote shutdown acknowledges a durable closed state before browser exit", () => {
  assert.match(source, /\["run", "scroll", "navigate", "stop", "reset", "shutdown"\]/u);
  assert.match(source, /async handleShutdown\(command, remote\)/u);
  assert.match(source, /if \(command\.action === "shutdown"\) \{[\s\S]*await dispatchPromise/u);
  assert.match(source, /this\.shuttingDown = true/u);
  assert.match(source, /await this\.confirmStopped\(\)/u);
  assert.match(
    source,
    /await this\.setAgentState\("closed", "presentation closed; browser and agent stopped"\)/u,
  );
  assert.match(source, /await this\.ack\([\s\S]*?"closed"[\s\S]*?browser and agent stopped/u);
  assert.match(source, /await this\.shutdown\("remote-command"\)/u);
});
