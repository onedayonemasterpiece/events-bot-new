#!/usr/bin/env node

import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { access, mkdir } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  DEFAULT_SCENARIO_ID,
  SCENARIO_IDS,
  TOMORROW_MOBILE_CONTRACT,
  TOMORROW_RAIL_LIKE_CONTRACT,
  WEEKEND_AMBER_ARTIFACT_CONTRACT,
  INTRO_LOOP_CONTRACT,
  WEEKEND_DESKTOP_CONTRACT,
  OUTRO_QR_CONTRACT,
  SEARCH_AUTH_SETUP_SCENE_ID,
  isStaticPresentationScenario,
  resolveScenarioId,
  resolveScenarioTimeoutMs,
  selectDeterministicMobileEvent,
} from "./scenario-contract.mjs";
import {
  INTRO_LOOP_RUNTIME_MS,
  INTRO_MUSIC_ASSET,
  INTRO_MUSIC_ASSETS,
  INTRO_SCENE_ID,
  FOCUS_PREVIEW_BASE_URL,
  FOCUS_INVITATION_SCENE_ID,
  FOCUS_INVITATION_URL,
  FOCUS_PAGE_RATING_URL,
  LECTURE_SCENES,
  LECTURE_UI_REFERENCE_ASSETS,
  MANUAL_PAGE_SCENES,
  WEEKEND_DESKTOP_SCENE_ID,
  ZNANIE_LOGO_ASSET,
  CAT_KEYBOARD_ASSET,
} from "./presentation-contract.mjs";
import {
  DEFAULT_PRESENTER_SCENE_ID,
  OUTRO_QR_ASSET,
  OUTRO_SCENE_ID,
} from "./outro-contract.mjs";
import { buildVerticalWheelTrajectory, PACING } from "./pacing.mjs";
import { abortableDelay, assertNotAborted } from "./abort-utils.mjs";

const VIEWPORT = Object.freeze({ width: 1920, height: 1080 });
const FRAME_SELECTOR = '#presenter-mobile-frame[data-presenter-id="mobile-site-frame"]';
const STAGE_READY_SELECTOR =
  '[data-presenter-id="presenter-stage"][data-presenter-stage-ready="true"]';
const OUTRO_READY_SELECTOR =
  '[data-presenter-id="presenter-stage"][data-presenter-scene="outro-qr"]';
const OUTRO_QR_SELECTOR = '[data-presenter-id="outro-qr-image"]';
const INTRO_SCENE_SELECTOR =
  '[data-presenter-id="intro-scene"][data-presenter-scene-id="intro-loop"]';
const INTRO_LOGO_SELECTOR = `${INTRO_SCENE_SELECTOR} .brand-plate--intro img`;
const INTRO_AUDIO_SELECTOR = '[data-presenter-id="intro-music"]';
const DESKTOP_FRAME_SELECTOR =
  '#presenter-desktop-frame[data-presenter-id="desktop-site-frame"]';
const EXHIBITIONS_FRAME_SELECTOR =
  '[data-presenter-id="exhibitions-desktop-frame"]';
const FESTIVALS_DESKTOP_FRAME_SELECTOR =
  '[data-presenter-id="festivals-desktop-frame"]';
const FESTIVALS_MOBILE_FRAME_SELECTOR =
  '[data-presenter-id="festivals-mobile-frame"]';
const MANUAL_MOBILE_FRAME_SELECTOR =
  '[data-presenter-id="manual-page-mobile-frame"]';
const MANUAL_DESKTOP_FRAME_SELECTOR =
  '[data-presenter-id="manual-page-desktop-frame"]';
const WEEKEND_DESKTOP_ROOT_SELECTOR = '[data-date-listing="weekend"]';
const SITE_FOOTER_SELECTOR = '[data-site-footer]';
const TOMORROW_MENU_SUMMARY_SELECTOR = "[data-mobile-discovery-menu] > summary";
const TOMORROW_MENU_LINK_SELECTOR =
  'nav[aria-label="Быстрый выбор даты"] a[href$="/zavtra/"]';
const TOMORROW_READY_SELECTOR =
  '[data-mobile-v23-page="tomorrow"][data-mobile-v23-ready="true"]';
const TOMORROW_ROWS_SELECTOR =
  '[data-mobile-v23-page="tomorrow"] [data-mobile-listing-row][data-event-id]';
const MOBILE_EVENT_RAIL_SELECTOR = ".rail-window";
const MOBILE_EVENT_DESCRIPTION_SELECTOR = '.event-digest[aria-label="О событии"]';
const MOBILE_DETAIL_SELECTOR = "[data-mobile-event-production]";
const MOBILE_DETAIL_DESCRIPTION_SELECTOR =
  "[data-mobile-event-production] .mobile-event-production__prose";
const WEEKEND_MENU_SUMMARY_SELECTOR = "[data-mobile-discovery-menu] > summary";
const WEEKEND_MENU_LINK_SELECTOR =
  'nav[aria-label="Быстрый выбор даты"] a[href$="/vyhodnye/"]';
const WEEKEND_ROOT_SELECTOR =
  '[data-date-listing="weekend"][data-amber-artifact-research="tail"]';
const ARTIFACT_PREVIEW_BASE_URL =
  "https://kenigevents.ru/_review/pp1wRctXBd6boYU1EcnBrod3z8MmKpD7SGEufK1t-xw";
const ARTIFACT_SELECTOR = "[data-amber-artifact]";
const ARTIFACT_STORAGE_KEY = "ke_artifact_collection_v1";
const PROFILE_STORAGE_KEY = "ke_personalization_profile";
const FEEDBACK_LOG_STORAGE_KEY = "ke_event_feedback_log_v1";
const DESCRIPTION_DWELL_MS = 1_700;
const DETAIL_DWELL_MS = 2_100;

const config = Object.freeze({
  relayUrl: (process.env.AUTOPRESENTER_RELAY_URL || "http://127.0.0.1:8787").replace(/\/$/, ""),
  agentToken: process.env.AUTOPRESENTER_AGENT_TOKEN || "",
  stageUrl:
    process.env.AUTOPRESENTER_STAGE_URL ||
    "http://127.0.0.1:4321/internal/presenter-stage/",
  agentId: process.env.AUTOPRESENTER_AGENT_ID || `prototype-${process.pid}`,
  headless: process.env.AUTOPRESENTER_HEADLESS === "1",
  fullscreen: process.env.AUTOPRESENTER_FULLSCREEN !== "0",
  artifactDir: process.env.AUTOPRESENTER_ARTIFACT_DIR || "",
  storageStatePath: process.env.AUTOPRESENTER_STORAGE_STATE_PATH || "",
  pollWaitMs: numberFromEnv("AUTOPRESENTER_POLL_WAIT_MS", 20_000, 100, 25_000),
  commandTtlGraceMs: numberFromEnv("AUTOPRESENTER_TTL_GRACE_MS", 250, 0, 5_000),
  hardStopMs: numberFromEnv("AUTOPRESENTER_HARD_STOP_MS", 2_000, 250, 10_000),
  introRuntimeMs: numberFromEnv(
    "AUTOPRESENTER_INTRO_RUNTIME_MS",
    INTRO_LOOP_RUNTIME_MS,
    5_000,
    INTRO_LOOP_RUNTIME_MS,
  ),
});

function numberFromEnv(name, fallback, minimum, maximum) {
  const value = Number(process.env[name]);
  return Number.isFinite(value) ? Math.max(minimum, Math.min(maximum, value)) : fallback;
}

function log(message, details = undefined) {
  const suffix = details === undefined ? "" : ` ${JSON.stringify(details)}`;
  process.stdout.write(`[autopresenter-agent] ${new Date().toISOString()} ${message}${suffix}\n`);
}

function errorText(error) {
  return error instanceof Error ? error.message : String(error);
}

function assertCondition(condition, message) {
  if (!condition) throw new Error(message);
}

function loadPlaywright() {
  const localRequire = createRequire(import.meta.url);
  const sharedDependencyRoot = process.env.AUTOPRESENTER_DEPENDENCY_ROOT || "";
  if (sharedDependencyRoot) {
    try {
      const sharedRequire = createRequire(
        path.join(sharedDependencyRoot, "__autopresenter_loader__.cjs"),
      );
      return sharedRequire("playwright");
    } catch (sharedError) {
      log("shared Playwright cache is unavailable; trying local fallback", {
        error: errorText(sharedError),
      });
    }
  }
  try {
    return localRequire("playwright");
  } catch (localError) {
    try {
      const globalRoot = execFileSync("npm", ["root", "-g"], { encoding: "utf8" }).trim();
      const globalRequire = createRequire(path.join(globalRoot, "__autopresenter_loader__.cjs"));
      return globalRequire("playwright");
    } catch (globalError) {
      throw new Error(
        `Playwright is unavailable. Install it locally or expose the central install via NODE_PATH. ` +
          `local=${errorText(localError)}; global=${errorText(globalError)}`,
      );
    }
  }
}

async function raceWithAbort(promise, signal) {
  assertNotAborted(signal);
  let abortHandler;
  const aborted = new Promise((_, reject) => {
    abortHandler = () => reject(signal.reason || new DOMException("Aborted", "AbortError"));
    signal.addEventListener("abort", abortHandler, { once: true });
  });
  try {
    return await Promise.race([promise, aborted]);
  } finally {
    signal.removeEventListener("abort", abortHandler);
  }
}

class PrototypeAgent {
  constructor(chromium) {
    this.chromium = chromium;
    this.browser = null;
    this.context = null;
    this.page = null;
    this.auxiliaryPage = null;
    this.afterSequence = 0;
    this.agentStatus = "disconnected";
    this.statusDetail = "starting";
    this.activeRun = null;
    this.runController = null;
    this.activeScenario = null;
    this.ackCache = new Map();
    this.shuttingDown = false;
    this.shutdownPromise = null;
    this.heartbeatTimer = null;
    this.pollAbort = null;
    this.contextGeneration = 0;
    this.dispatchTail = Promise.resolve();
  }

  async start() {
    if (config.artifactDir) await mkdir(config.artifactDir, { recursive: true });

    this.browser = await this.chromium.launch({
      headless: config.headless,
      args: [
        "--force-device-scale-factor=1",
        "--kiosk",
        "--start-fullscreen",
        "--window-position=0,0",
        "--window-size=1920,1080",
        "--no-first-run",
        "--disable-session-crashed-bubble",
        "--disable-features=Translate,TranslateUI",
        "--autoplay-policy=no-user-gesture-required",
        "--lang=ru-RU",
      ],
    });
    await this.createContextAndStage();
    await this.setAgentState("idle", "stage ready");

    const metrics = await this.page.evaluate(() => ({
      innerWidth,
      innerHeight,
      devicePixelRatio,
    }));
    log("ready", {
      agentId: config.agentId,
      scenarios: SCENARIO_IDS,
      defaultScenario: DEFAULT_SCENARIO_ID,
      viewport: VIEWPORT,
      zoom: "100% assumed",
      ...metrics,
      headed: !config.headless,
    });

    this.heartbeatTimer = setInterval(() => {
      void this.publishState().catch((error) => log("heartbeat failed", errorText(error)));
    }, 3_000);
    await this.pollLoop();
  }

  async contextOptions() {
    const options = { viewport: VIEWPORT, deviceScaleFactor: 1 };
    if (config.storageStatePath) {
      try {
        await access(config.storageStatePath);
        options.storageState = config.storageStatePath;
      } catch {
        // First launch intentionally starts without credentials; a later authenticated
        // session is persisted to this file without shipping secrets in the package.
      }
    }
    if (config.artifactDir) {
      options.recordVideo = { dir: config.artifactDir, size: VIEWPORT };
    }
    return options;
  }

  async createContextAndStage() {
    this.contextGeneration += 1;
    const generation = this.contextGeneration;
    this.context = await this.browser.newContext(await this.contextOptions());
    this.page = await this.context.newPage();

    await this.page.exposeBinding("__autopresenterShortcut", (_source, action) => {
      if (!["run", "stop", "reset"].includes(action)) return;
      void this.dispatch(
        {
          id: `local-${randomUUID()}`,
          sequence: -Date.now(),
          action,
          expires_at: null,
        },
        { remote: false },
      );
    });

    await this.page.addInitScript(() => {
      if (window !== window.top) return;
      const keyLabels = new Map([
        ["Space", "Space"],
        ["ArrowLeft", "←"],
        ["ArrowRight", "→"],
        ["ArrowUp", "↑"],
        ["ArrowDown", "↓"],
        ["Escape", "Esc"],
      ]);
      const pressedKeys = new Map();

      const desktopOverlay = () => {
        if (document.documentElement.dataset.presenterInteractionMode !== "desktop") return null;
        let overlay = document.querySelector("[data-autopresenter-keyboard]");
        if (overlay) return overlay;
        overlay = document.createElement("aside");
        overlay.dataset.autopresenterKeyboard = "true";
        overlay.setAttribute("aria-live", "polite");
        Object.assign(overlay.style, {
          position: "fixed",
          zIndex: "2147483647",
          right: "32px",
          bottom: "32px",
          minWidth: "220px",
          padding: "16px",
          border: "1px solid rgba(255,255,255,.28)",
          borderRadius: "18px",
          background: "rgba(17,24,39,.9)",
          boxShadow: "0 18px 48px rgba(0,0,0,.34)",
          color: "#fff",
          font: "700 16px/1.25 Inter,system-ui,sans-serif",
          pointerEvents: "none",
        });
        overlay.innerHTML =
          '<div data-autopresenter-pressed-keys></div><div data-autopresenter-ui-response style="margin-top:8px;color:#7de6c2;font-size:13px">Интерфейс готов</div>';
        document.body.append(overlay);
        return overlay;
      };

      const renderPressedKeys = () => {
        const overlay = desktopOverlay();
        if (!overlay) return;
        const host = overlay.querySelector("[data-autopresenter-pressed-keys]");
        host.replaceChildren(
          ...[...pressedKeys.values()].map((label) => {
            const key = document.createElement("kbd");
            key.dataset.autopresenterKeyPressed = "true";
            key.textContent = label;
            Object.assign(key.style, {
              display: "inline-grid",
              minWidth: "44px",
              minHeight: "40px",
              marginRight: "8px",
              padding: "0 10px",
              placeItems: "center",
              border: "1px solid rgba(255,255,255,.46)",
              borderRadius: "10px",
              background: "rgba(255,255,255,.15)",
              boxShadow: "inset 0 -3px rgba(0,0,0,.25)",
            });
            return key;
          }),
        );
      };

      const renderDesktopResponse = (label) => {
        const overlay = desktopOverlay();
        const response = overlay?.querySelector("[data-autopresenter-ui-response]");
        if (!response) return;
        response.textContent = label || "Интерфейс ответил";
        response.dataset.autopresenterUiResponded = "true";
      };
      window.addEventListener("presenter:desktop-ui-response", (event) => {
        renderDesktopResponse(event.detail?.label);
      });
      window.addEventListener("presenter:desktop-key-visual", (event) => {
        const code = event.detail?.code;
        if (!code) return;
        if (event.detail?.pressed) {
          pressedKeys.set(code, event.detail?.label || keyLabels.get(code) || code);
        } else {
          pressedKeys.delete(code);
        }
        renderPressedKeys();
      });
      window.addEventListener("presenter:status", (event) => {
        renderDesktopResponse(event.detail?.detail || event.detail?.status);
      });

      window.addEventListener(
        "keydown",
        (event) => {
          const target = event.target;
          if (
            target instanceof HTMLElement &&
            (target.isContentEditable || ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName))
          ) {
            return;
          }

          let action = null;
          if (event.code === "Space" || event.code === "ArrowRight") action = "run";
          if (event.code === "Escape") action = "stop";
          if (event.code === "KeyR") action = "reset";
          if (!action) return;
          event.preventDefault();
          void window.__autopresenterShortcut(action);
        },
        { capture: true },
      );
      window.addEventListener(
        "keydown",
        (event) => {
          if (document.documentElement.dataset.presenterInteractionMode !== "desktop") return;
          pressedKeys.set(event.code, keyLabels.get(event.code) || event.key);
          renderPressedKeys();
        },
        { capture: true },
      );
      window.addEventListener(
        "keyup",
        (event) => {
          if (!pressedKeys.delete(event.code)) return;
          renderPressedKeys();
        },
        { capture: true },
      );
    });

    await this.openStage(this.page);
    log("browser context ready", { generation });
  }

  async openStage(page = this.page) {
    await page.goto(config.stageUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.locator(STAGE_READY_SELECTOR).waitFor({ state: "visible", timeout: 30_000 });
    await page.locator(FRAME_SELECTOR).waitFor({ state: "visible", timeout: 30_000 });
    await this.setInteractionMode("mobile");
    if (!config.headless && config.fullscreen) {
      let browserWindowFullscreen = false;
      try {
        const session = await this.context.newCDPSession(page);
        const { windowId } = await session.send("Browser.getWindowForTarget");
        await session.send("Browser.setWindowBounds", {
          windowId,
          bounds: { windowState: "fullscreen" },
        });
        await session.detach();
        browserWindowFullscreen = true;
      } catch (error) {
        log("native browser fullscreen failed; trying document fullscreen", errorText(error));
      }
      if (!browserWindowFullscreen) {
        await page.keyboard.press("f");
        await page
          .waitForFunction(() => Boolean(document.fullscreenElement), undefined, { timeout: 2_000 })
          .catch(() => log("automatic fullscreen unavailable; use local F"));
      }
    }
  }

  async ensureStageReady(reason = "command") {
    if (this.auxiliaryPage && !this.auxiliaryPage.isClosed()) {
      await this.auxiliaryPage.close().catch((error) =>
        log("auxiliary page close failed", errorText(error)));
    }
    this.auxiliaryPage = null;
    assertCondition(this.page && !this.page.isClosed(), "persistent stage page is unavailable");

    const stageReady = await this.page
      .locator(STAGE_READY_SELECTOR)
      .isVisible()
      .catch(() => false);
    if (!stageReady) {
      log("restoring persistent stage before command", { reason, url: this.page.url() });
      await this.openStage(this.page);
    } else {
      await this.page.bringToFront();
    }
  }

  async setInteractionMode(mode) {
    await this.page.evaluate((nextMode) => {
      document.documentElement.dataset.presenterInteractionMode = nextMode;
      document.querySelector("[data-autopresenter-keyboard]")?.remove();
      let shield = document.querySelector("[data-autopresenter-mobile-pointer-shield]");
      if (nextMode !== "mobile") {
        shield?.remove();
        return;
      }
      if (!shield) {
        shield = document.createElement("style");
        shield.dataset.autopresenterMobilePointerShield = "true";
        shield.textContent = "*,*::before,*::after{cursor:none!important}";
        document.head.append(shield);
      }
    }, mode);
  }

  async enforceMobilePointerShield(frame) {
    await frame.locator("head").evaluate((head) => {
      let shield = document.querySelector("[data-autopresenter-mobile-pointer-shield]");
      if (shield) return;
      shield = document.createElement("style");
      shield.dataset.autopresenterMobilePointerShield = "true";
      shield.textContent = "*,*::before,*::after{cursor:none!important}";
      head.append(shield);
    });
  }

  async pollLoop() {
    while (!this.shuttingDown) {
      try {
        this.pollAbort = new AbortController();
        const params = new URLSearchParams({
          agent_id: config.agentId,
          after_seq: String(this.afterSequence),
          wait_ms: String(config.pollWaitMs),
        });
        const response = await fetch(`${config.relayUrl}/api/commands/next?${params}`, {
          headers: this.authHeaders(),
          signal: this.pollAbort.signal,
        });
        if (!response.ok) throw new Error(`poll HTTP ${response.status}`);
        const payload = await response.json();
        const command = payload.command;
        if (!command) continue;

        this.afterSequence = Math.max(this.afterSequence, Number(command.sequence) || 0);
        const dispatchPromise = this.dispatch(command, { remote: true });
        if (command.action === "shutdown") {
          // The terminal command must publish and acknowledge `closed` before
          // start() can leave the polling loop and enter its finalizer.
          await dispatchPromise;
        } else {
          void dispatchPromise.catch((error) =>
            log("command dispatch failed", { id: command.id, error: errorText(error) }),
          );
        }
      } catch (error) {
        if (this.shuttingDown) break;
        log("poll failed; retrying", errorText(error));
        await abortableDelay(750).catch(() => {});
      } finally {
        this.pollAbort = null;
      }
    }
  }

  async dispatch(command, { remote }) {
    const operation = this.dispatchTail.then(() =>
      this.dispatchCommand(command, { remote }),
    );
    this.dispatchTail = operation.catch(() => {});
    return operation;
  }

  async dispatchCommand(command, { remote }) {
    if (!command || !["run", "scroll", "navigate", "stop", "reset", "shutdown"].includes(command.action)) return;

    if (remote && this.ackCache.has(command.id)) {
      const previous = this.ackCache.get(command.id);
      await this.ack(command, previous.status, previous.detail);
      return;
    }

    if (this.isExpired(command)) {
      if (remote) await this.ack(command, "error", "command TTL expired");
      return;
    }

    if (command.action === "run") {
      let scenarioId;
      try {
        scenarioId = resolveScenarioId(command.scenario);
      } catch (error) {
        const detail = errorText(error);
        await this.setAgentState("error", detail);
        if (remote) await this.ack(command, "error", detail);
        return;
      }
      await this.handleRun(command, remote, scenarioId);
      return;
    }
    if (command.action === "stop") {
      await this.handleStop(command, remote);
      return;
    }
    if (command.action === "scroll") {
      await this.handleManualScroll(command, remote);
      return;
    }
    if (command.action === "navigate") {
      await this.handleDesktopNavigation(command, remote);
      return;
    }
    if (command.action === "shutdown") {
      await this.handleShutdown(command, remote);
      return;
    }
    await this.handleReset(command, remote);
  }

  async handleManualScroll(command, remote) {
    const direction = command.options?.direction === "up" ? -1 : 1;
    const amount = Math.max(120, Math.min(1_200, Number(command.options?.amount) || 420));
    const visibleFrame = this.page.locator("iframe:visible").last();
    const frameCount = await visibleFrame.count();
    const target = frameCount ? visibleFrame : this.page.locator("body");
    const box = await target.boundingBox();
    if (!box) {
      if (remote) await this.ack(command, "error", "visible presentation surface is unavailable");
      return;
    }

    await this.page.mouse.move(
      Math.round(box.x + box.width / 2),
      Math.round(box.y + box.height / 2),
    );
    const delta = direction * amount;
    for (const factor of [.22, .33, .45]) {
      await this.page.mouse.wheel(0, Math.round(delta * factor));
      await abortableDelay(70);
    }
    const detail = `manual scroll ${command.options?.direction || "down"} ${amount}px`;
    log(detail, { activeScenario: this.activeScenario });
    if (remote) await this.ack(command, "completed", detail);
  }

  async handleDesktopNavigation(command, remote) {
    const keyByDirection = {
      up: "ArrowUp",
      left: "ArrowLeft",
      down: "ArrowDown",
      right: "ArrowRight",
    };
    const direction = String(command.options?.direction || "").toLowerCase();
    const key = keyByDirection[direction];
    if (!key) {
      if (remote) await this.ack(command, "error", "unsupported desktop navigation direction");
      return;
    }

    const sceneId = await this.page.locator(STAGE_READY_SELECTOR).getAttribute("data-presenter-scene");
    let frameSelector = {
      "service-navigation-exhibitions": EXHIBITIONS_FRAME_SELECTOR,
      "service-medallions-desktop": '[data-presenter-id="medallion-desktop-frame"]',
      "weekend-desktop": DESKTOP_FRAME_SELECTOR,
    }[sceneId || ""];
    if (sceneId === "service-navigation-festivals") {
      const phase = await this.page
        .locator('[data-presenter-scene-id="service-navigation-festivals"]')
        .getAttribute("data-festival-phase");
      frameSelector = phase === "mobile"
        ? FESTIVALS_MOBILE_FRAME_SELECTOR
        : FESTIVALS_DESKTOP_FRAME_SELECTOR;
    }
    frameSelector ||= "iframe:visible";
    const iframe = this.page.locator(frameSelector).last();
    if (!(await iframe.isVisible().catch(() => false))) {
      if (remote) await this.ack(command, "error", "visible desktop navigation surface is unavailable");
      return;
    }
    const handle = await iframe.elementHandle();
    const frame = await handle?.contentFrame();
    if (!frame) {
      if (remote) await this.ack(command, "error", "desktop navigation frame did not attach");
      return;
    }

    await this.setInteractionMode("desktop");
    const focused = frame.locator(":focus");
    const activeTag = await focused.evaluate((node) => node.tagName).catch(() => "");
    if (!activeTag || ["HTML", "BODY"].includes(activeTag)) {
      const target = frame
        .locator(
          '[data-keyboard-event-surface]:visible, [data-row-focus]:visible, [data-deck]:visible, a[href]:visible, button:not([disabled]):visible, [tabindex]:not([tabindex="-1"]):visible',
        )
        .first();
      if (!(await target.count())) {
        if (remote) await this.ack(command, "error", "page has no keyboard navigation target");
        return;
      }
      await target.focus();
    }

    await this.page.evaluate(({ code, label }) => {
      window.dispatchEvent(
        new CustomEvent("presenter:desktop-key-visual", {
          detail: { code, label, pressed: true },
        }),
      );
    }, { code: key, label: { ArrowUp: "↑", ArrowLeft: "←", ArrowDown: "↓", ArrowRight: "→" }[key] });
    await frame.locator(":focus").press(key);
    const response = await frame.locator(":focus").evaluate((node) => {
      const card = node.closest?.("[data-event-card], [data-exhibition-row], [data-deck]");
      const title = card?.querySelector?.("h2,h3,[data-event-title]")?.textContent?.trim();
      return title || node.getAttribute("aria-label") || node.textContent?.trim().slice(0, 90) || "Интерфейс ответил";
    }).catch(() => "Интерфейс ответил");
    await this.page.evaluate(({ code, label }) => {
      window.dispatchEvent(
        new CustomEvent("presenter:desktop-ui-response", {
          detail: { label },
        }),
      );
      window.setTimeout(() => {
        window.dispatchEvent(
          new CustomEvent("presenter:desktop-key-visual", {
            detail: { code, pressed: false },
          }),
        );
      }, 460);
    }, { code: key, label: `${key.replace("Arrow", "")} · ${response}` });
    const detail = `desktop ${direction}: ${response}`;
    log(detail, { sceneId });
    if (remote) await this.ack(command, "completed", detail);
  }

  isExpired(command) {
    if (!command.expires_at) return false;
    return Date.now() > Date.parse(command.expires_at) + config.commandTtlGraceMs;
  }

  async handleRun(command, remote, scenarioId) {
    if (this.activeRun) {
      const previousScenario = this.activeScenario || "active scenario";
      const detail = `switching ${previousScenario} → ${scenarioId}`;
      await this.setAgentState("stopping", detail);
      if (remote) await this.ack(command, "stopping", detail);
      await this.confirmStopped(`scene switch to ${scenarioId}`);
    }

    try {
      await this.ensureStageReady(`run ${scenarioId}`);
    } catch (error) {
      const detail = `stage recovery failed before ${scenarioId}: ${errorText(error)}`;
      await this.setAgentState("error", detail);
      if (remote) await this.ack(command, "error", detail);
      return;
    }

    const controller = new AbortController();
    this.runController = controller;
    this.activeScenario = scenarioId;
    await this.setAgentState("running", scenarioId);
    if (remote) await this.ack(command, "running", scenarioId);

    const timeoutMs = resolveScenarioTimeoutMs(scenarioId);
    const timeout = setTimeout(() => {
      const error = new Error(`${scenarioId} exceeded ${timeoutMs}ms`);
      error.name = "TimeoutError";
      controller.abort(error);
    }, timeoutMs);

    const runPromise = this.runScenario(scenarioId, controller.signal, command)
      .then(async (evidence) => {
        const detail = `${scenarioId}: ${evidence.summary}`;
        await this.setAgentState("completed", detail);
        if (remote) await this.ack(command, "completed", detail);
      })
      .catch(async (error) => {
        const timedOut =
          error?.name === "TimeoutError" || controller.signal.reason?.name === "TimeoutError";
        const failure = timedOut ? controller.signal.reason : error;
        if (!timedOut && (error?.name === "AbortError" || controller.signal.aborted)) {
          log("scenario cooperatively stopped", { commandId: command.id, scenario: scenarioId });
          return;
        }
        log("scenario failed", {
          commandId: command.id,
          scenario: scenarioId,
          error: errorText(failure),
          stack: failure?.stack,
        });
        const detail = `${scenarioId}: ${errorText(failure)}`;
        await this.setAgentState("error", detail);
        if (remote) await this.ack(command, "error", detail);
      })
      .finally(() => {
        clearTimeout(timeout);
        if (this.activeRun === runPromise) {
          this.activeRun = null;
          this.runController = null;
          this.activeScenario = null;
        }
      });

    this.activeRun = runPromise;
  }

  async runScenario(scenarioId, signal, command = {}) {
    if (MANUAL_PAGE_SCENES.some((scene) => scene.id === scenarioId)) {
      return this.runManualPage(scenarioId, signal);
    }
    if (scenarioId === TOMORROW_MOBILE_CONTRACT.id) {
      return this.runTomorrowMobile(signal);
    }
    if (scenarioId === TOMORROW_RAIL_LIKE_CONTRACT.id) {
      return this.runTomorrowRailLike(signal);
    }
    if (scenarioId === WEEKEND_AMBER_ARTIFACT_CONTRACT.id) {
      return this.runWeekendAmberArtifact(signal);
    }
    if (scenarioId === INTRO_LOOP_CONTRACT.id) {
      return this.runIntroLoop(signal, command.options);
    }
    if (scenarioId === FOCUS_INVITATION_SCENE_ID) {
      return this.runFocusInvitation(signal);
    }
    if (scenarioId === "service-nps") {
      return this.runFocusNps(signal);
    }
    if (scenarioId === "service-future-celebrity") {
      return this.runPeopleScene(signal);
    }
    if (scenarioId === "service-transport-rail") {
      return this.runTransportRail(signal);
    }
    if (scenarioId === "service-transport-bus") {
      return this.runTransportBus(signal);
    }
    if (scenarioId === "service-navigation-map") {
      return this.runNavigationMap(signal);
    }
    if (scenarioId === "service-social-proof" || scenarioId === "service-fast-find") {
      return this.runFeedbackCollection(scenarioId, signal);
    }
    if (scenarioId === "service-artifact-desktop") {
      return this.runArtifactDesktop(signal);
    }
    if (scenarioId === "service-keyboard-day" || scenarioId === "service-keyboard-event") {
      return this.runKeyboardInterface(scenarioId, signal);
    }
    if (scenarioId === "service-report-problem") {
      return this.runReportProblem(signal);
    }
    if (scenarioId === "service-share-friends") {
      return this.runShareFriends(signal);
    }
    if (scenarioId === "service-calendar-memory") {
      return this.runCalendarMemory(signal);
    }
    if (scenarioId === "service-community-curator") {
      return this.runCommunityCurator(signal);
    }
    if (scenarioId === "service-navigation-exhibitions") {
      return this.runExhibitionsNavigation(signal);
    }
    if (scenarioId === "service-navigation-festivals") {
      return this.runFestivalsNavigation(signal);
    }
    if (isStaticPresentationScenario(scenarioId)) {
      return this.runHeldPresentationScene(scenarioId, signal);
    }
    if (scenarioId === "service-medallions-desktop") {
      return this.runFocusMedallionScene(scenarioId, signal, "desktop");
    }
    if (scenarioId === "service-medallions-mobile") {
      return this.runFocusMedallionScene(scenarioId, signal, "mobile");
    }
    if (scenarioId === "service-search-live") {
      return this.runFocusSearch(signal, command.options?.query);
    }
    if (scenarioId === SEARCH_AUTH_SETUP_SCENE_ID) {
      return this.runSearchAuthSetup(signal);
    }
    if (scenarioId === WEEKEND_DESKTOP_CONTRACT.id) {
      return this.runWeekendDesktop(signal);
    }
    if (scenarioId === OUTRO_QR_CONTRACT.id) {
      return this.runOutroQr(signal);
    }
    throw new Error(`unreachable scenario dispatch: ${scenarioId}`);
  }

  async runManualPage(scenarioId, signal) {
    const startedAt = Date.now();
    const scene = MANUAL_PAGE_SCENES.find((candidate) => candidate.id === scenarioId);
    assertCondition(scene, `manual page contract is missing for ${scenarioId}`);
    const stageSceneId = scene.mode === "mobile" ? "manual-page-mobile" : "manual-page-desktop";
    const frameSelector = scene.mode === "mobile"
      ? MANUAL_MOBILE_FRAME_SELECTOR
      : MANUAL_DESKTOP_FRAME_SELECTOR;
    await this.setInteractionMode(scene.mode);
    await this.setAgentState("running", `${scenarioId} · ручной показ`);
    await this.showPresenterScene(stageSceneId, signal, {
      url: scene.url,
      label: scene.label,
    });
    const frame = await this.focusFrame(frameSelector, signal);
    await this.waitForEmbeddedReady(frame, signal);
    if (scene.openMobileMenu) {
      const menu = frame.locator("[data-mobile-discovery-menu] > summary").first();
      if (await menu.count()) {
        await menu.click();
        await abortableDelay(450, signal);
      }
    }
    await abortableDelay(600, signal);
    await this.captureScenario(scenarioId);
    return {
      summary:
        `${scene.label} opened in ${scene.mode} mode and left untouched for manual pult scrolling`,
      durationMs: Date.now() - startedAt,
    };
  }

  async handleStop(command, remote) {
    await this.setAgentState("stopping", "stop requested");
    if (remote) await this.ack(command, "stopping", "stop requested");
    await this.confirmStopped();
    await this.stopPresenterScene();
    await this.setAgentState("idle", "agent confirmed stopped");
    if (remote) await this.ack(command, "idle", "agent confirmed stopped");
  }

  async handleShutdown(command, remote) {
    // Mark shutdown before the first await so the long-poll loop cannot open
    // another request while the final state/ack is being delivered.
    this.shuttingDown = true;
    this.pollAbort?.abort();
    await this.setAgentState("stopping", "closing presentation");
    if (remote) await this.ack(command, "stopping", "closing presentation");
    await this.confirmStopped();
    await this.setAgentState("closed", "presentation closed; browser and agent stopped");
    if (remote) {
      await this.ack(
        command,
        "closed",
        "presentation closed; browser and agent stopped",
      );
    }
    await this.shutdown("remote-command");
  }

  async handleReset(command, remote) {
    if (this.activeRun) {
      await this.setAgentState("stopping", "reset requested");
      if (remote) await this.ack(command, "stopping", "reset requested");
      await this.confirmStopped();
    }

    try {
      await this.openStage(this.page);
      const frame = this.page.frameLocator(FRAME_SELECTOR);
      await this.waitForEmbeddedReady(frame, new AbortController().signal);
      await this.resetEmbeddedState(frame, new AbortController().signal);
      await this.setAgentState("idle", "stage reset");
      if (remote) await this.ack(command, "idle", "stage reset");
    } catch (error) {
      await this.recoverPersistentStage(`reset recovery: ${errorText(error)}`);
      await this.openStage(this.page);
      await this.setAgentState("idle", "stage reset after same-page recovery");
      if (remote) await this.ack(command, "idle", "stage reset after same-page recovery");
    }
  }

  async confirmStopped(reason = "stop requested") {
    if (!this.activeRun) return;
    const active = this.activeRun;
    const controller = this.runController;
    controller?.abort(new DOMException(reason, "AbortError"));
    const settled = Symbol("settled");
    const outcome = await Promise.race([
      active.then(() => settled, () => settled),
      abortableDelay(config.hardStopMs).then(() => "timeout"),
    ]);
    if (outcome === "timeout") {
      await this.recoverPersistentStage("cooperative stop deadline exceeded");
    }
    if (this.activeRun === active) {
      this.activeRun = null;
      if (this.runController === controller) this.runController = null;
      this.activeScenario = null;
    }
    await this.stopPresenterScene();
  }

  async recoverPersistentStage(reason) {
    log("recovering persistent stage", { reason, generation: this.contextGeneration });
    if (!this.shuttingDown && this.page && !this.page.isClosed()) {
      await this.page
        .goto(config.stageUrl, { waitUntil: "commit", timeout: config.hardStopMs })
        .catch((error) => log("persistent stage recovery navigation failed", errorText(error)));
    }
  }

  async resetEmbeddedState(frame, signal) {
    await raceWithAbort(
      frame.locator("body").evaluate(() => {
        const focusParticipation = localStorage.getItem("kenigevents:focus-participation:v1");
        const presenterOwnedKeys = [
          "ke_artifact_collection_v1",
          "ke_personalization_profile",
          "ke_event_feedback_log_v1",
        ];
        presenterOwnedKeys.forEach((key) => localStorage.removeItem(key));
        sessionStorage.clear();
        if (focusParticipation !== null) {
          localStorage.setItem("kenigevents:focus-participation:v1", focusParticipation);
        }
      }),
      signal,
    );
    await this.reloadEmbeddedFrame(signal);
    await this.waitForEmbeddedReady(frame, signal);
  }

  async prepareScenarioStage(scenarioId, signal) {
    assertNotAborted(signal);
    await raceWithAbort(this.openStage(this.page), signal);
    await this.showPresenterScene(DEFAULT_PRESENTER_SCENE_ID, signal);
    await this.setAgentState("running", scenarioId);
    const frame = this.page.frameLocator(FRAME_SELECTOR);
    await this.waitForEmbeddedReady(frame, signal);
    await this.resetEmbeddedState(frame, signal);
    await this.enforceMobilePointerShield(frame);
    return frame;
  }

  async showPresenterScene(sceneId, signal, options = {}) {
    assertNotAborted(signal);
    await raceWithAbort(
      this.page.evaluate(({ nextSceneId, sceneOptions }) => {
        window.dispatchEvent(
          new CustomEvent("presenter:scene", {
            detail: { id: nextSceneId, ...sceneOptions },
          }),
        );
      }, { nextSceneId: sceneId, sceneOptions: options }),
      signal,
    );
    await raceWithAbort(
      this.page
        .locator(`[data-presenter-id="presenter-stage"][data-presenter-scene="${sceneId}"]`)
        .waitFor({ state: "visible", timeout: 10_000 }),
      signal,
    );
  }

  async stopPresenterScene() {
    if (
      !this.page ||
      typeof this.page.evaluate !== "function" ||
      (typeof this.page.isClosed === "function" && this.page.isClosed())
    ) {
      return;
    }
    await this.page
      .evaluate(() => window.dispatchEvent(new CustomEvent("presenter:stop")))
      .catch(() => {});
  }

  async assertStageAssetLoaded(selector, expectedUrl, signal) {
    const asset = this.page.locator(selector);
    await raceWithAbort(asset.waitFor({ state: "attached", timeout: 10_000 }), signal);
    await raceWithAbort(
      asset.evaluate(async (node, url) => {
        if (node instanceof HTMLImageElement) {
          if (!node.complete) {
            await new Promise((resolve, reject) => {
              node.addEventListener("load", resolve, { once: true });
              node.addEventListener("error", reject, { once: true });
            });
          }
          if (node.naturalWidth <= 0 || node.currentSrc !== url) {
            throw new Error(`stage image failed to load from pinned CDN URL: ${node.currentSrc}`);
          }
          return;
        }
        if (node instanceof HTMLMediaElement) {
          if (node.readyState < HTMLMediaElement.HAVE_METADATA) {
            await new Promise((resolve, reject) => {
              node.addEventListener("loadedmetadata", resolve, { once: true });
              node.addEventListener("error", reject, { once: true });
            });
          }
          if (node.currentSrc !== url) {
            throw new Error(`stage media failed to load from pinned CDN URL: ${node.currentSrc}`);
          }
          return;
        }
        throw new Error("stage asset node is neither image nor media");
      }, expectedUrl),
      signal,
    );
  }

  async runIntroLoop(signal, options = {}) {
    const startedAt = Date.now();
    await this.setInteractionMode("stage");
    await this.setAgentState("running", INTRO_LOOP_CONTRACT.id);
    await this.showPresenterScene(INTRO_SCENE_ID, signal, {
      startAt: typeof options?.start_at === "string" ? options.start_at : "",
    });
    const scene = this.page.locator(INTRO_SCENE_SELECTOR);
    await raceWithAbort(scene.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.assertStageAssetLoaded(INTRO_LOGO_SELECTOR, ZNANIE_LOGO_ASSET.url, signal);
    await this.assertStageAssetLoaded(INTRO_AUDIO_SELECTOR, INTRO_MUSIC_ASSET.url, signal);
    const playlistSize = await scene.getAttribute("data-intro-playlist-size");
    if (playlistSize !== String(INTRO_MUSIC_ASSETS.length)) {
      throw new Error(
        `intro playlist size mismatch: expected ${INTRO_MUSIC_ASSETS.length}, got ${playlistSize}`,
      );
    }
    await raceWithAbort(
      this.page.waitForFunction(
        (selector) =>
          document.querySelector(selector)?.getAttribute("data-intro-state") === "running",
        INTRO_SCENE_SELECTOR,
        { timeout: 10_000 },
      ),
      signal,
    );
    await abortableDelay(config.introRuntimeMs, signal);
    await this.captureScenario(INTRO_LOOP_CONTRACT.id);
    return {
      summary:
        `two-line human-like intro loop ran ${config.introRuntimeMs}ms with Znanie logo ` +
        "and CDN music in the persistent stage",
      durationMs: Date.now() - startedAt,
    };
  }

  async runHeldPresentationScene(scenarioId, signal) {
    const startedAt = Date.now();
    await this.setInteractionMode("stage");
    await this.setAgentState("running", scenarioId);
    await this.showPresenterScene(scenarioId, signal);
    const selector = `[data-presenter-scene-id="${scenarioId}"]`;
    const scene = this.page.locator(selector);
    await raceWithAbort(scene.waitFor({ state: "visible", timeout: 10_000 }), signal);
    const lecture = LECTURE_SCENES.find(({ id }) => id === scenarioId);
    if (lecture) {
      await this.assertStageAssetLoaded(
        `${selector} .lecture-visual img`,
        lecture.url,
        signal,
      );
    }
    if (scenarioId === "lecture-good-ui") {
      for (const asset of LECTURE_UI_REFERENCE_ASSETS.slice(0, 2)) {
        await this.assertStageAssetLoaded(
          `${selector} img[src="${asset.url}"]`,
          asset.url,
          signal,
        );
      }
    }
    if (scenarioId === "lecture-poor-ui") {
      const asset = LECTURE_UI_REFERENCE_ASSETS[2];
      await this.assertStageAssetLoaded(
        `${selector} img[src="${asset.url}"]`,
        asset.url,
        signal,
      );
    }
    if (scenarioId.startsWith("lecture-")) {
      await this.assertStageAssetLoaded(
        `${selector} .brand-plate--lecture img`,
        ZNANIE_LOGO_ASSET.url,
        signal,
      );
      await raceWithAbort(
        this.page.waitForFunction(
          (lectureSelector) =>
            document.querySelector(lectureSelector)?.getAttribute("data-lecture-state") ===
            "complete",
          selector,
          { timeout: 15_000 },
        ),
        signal,
      );
    }
    if (scenarioId === "service-joke") {
      await raceWithAbort(
        this.page.waitForFunction(
          (selector) =>
            document.querySelector(selector)?.getAttribute("data-joke-state") === "complete",
          selector,
          { timeout: 15_000 },
        ),
        signal,
      );
    }
    if (scenarioId === "service-medallions") {
      await raceWithAbort(
        this.page.waitForFunction(
          (selector) =>
            document.querySelector(selector)?.getAttribute("data-medallion-state") === "complete",
          selector,
          { timeout: 25_000 },
        ),
        signal,
      );
    }
    if (scenarioId === "service-disruption") {
      await this.assertStageAssetLoaded(
        '[data-presenter-id="error-cat-image"]',
        CAT_KEYBOARD_ASSET.url,
        signal,
      );
      await raceWithAbort(
        this.page.waitForFunction(
          (sceneSelector) => {
            const scene = document.querySelector(sceneSelector);
            const audio = scene?.querySelector('[data-presenter-id="error-audio"]');
            return scene?.getAttribute("data-error-phase") === "complete"
              && scene?.getAttribute("data-error-audio") === "complete"
              && audio instanceof HTMLAudioElement
              && audio.currentTime > 0;
          },
          selector,
          { timeout: 10_000 },
        ),
        signal,
      );
    }
    if (scenarioId === "service-taste" || scenarioId === "service-feedback") {
      await raceWithAbort(
        this.page.waitForFunction(
          (sceneSelector) =>
            document.querySelector(sceneSelector)?.getAttribute("data-service-copy-state")
              === "complete",
          selector,
          { timeout: 15_000 },
        ),
        signal,
      );
    }
    if (scenarioId.startsWith("market-")) {
      await raceWithAbort(
        this.page.waitForFunction(
          (sceneSelector) =>
            document.querySelector(sceneSelector)?.getAttribute("data-market-state") ===
            "complete",
          selector,
          { timeout: 15_000 },
        ),
        signal,
      );
    }
    if (await this.page.locator(selector).getAttribute("data-visual-state") !== null) {
      await raceWithAbort(
        this.page.waitForFunction(
          (sceneSelector) =>
            document.querySelector(sceneSelector)?.getAttribute("data-visual-state") ===
            "complete",
          selector,
          { timeout: 15_000 },
        ),
        signal,
      );
    }
    if (scenarioId === "service-friends-club") {
      await raceWithAbort(
        this.page.waitForFunction(
          (sceneSelector) => {
            const video = document.querySelector(sceneSelector)
              ?.querySelector('[data-presenter-id="friends-club-video"]');
            return video instanceof HTMLVideoElement
              && video.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA;
          },
          selector,
          { timeout: 15_000 },
        ),
        signal,
      );
    }
    await abortableDelay(scenarioId === "service-wordmark" ? 3_600 : 900, signal);
    await this.captureScenario(scenarioId);
    return {
      summary: lecture
        ? `source-backed lecture frame ${scenarioId} is visible and held until the next command`
        : `presentation frame ${scenarioId} is visible and held until the next command`,
      durationMs: Date.now() - startedAt,
    };
  }

  async runExhibitionsNavigation(signal) {
    const scenarioId = "service-navigation-exhibitions";
    const startedAt = Date.now();
    await raceWithAbort(this.openStage(this.page), signal);
    await this.setInteractionMode("desktop");
    await this.setAgentState("running", scenarioId);
    await this.showPresenterScene(scenarioId, signal);
    const iframe = this.page.locator(EXHIBITIONS_FRAME_SELECTOR);
    await raceWithAbort(iframe.waitFor({ state: "visible", timeout: 15_000 }), signal);
    const frame = this.page.frameLocator(EXHIBITIONS_FRAME_SELECTOR);
    const root = frame.locator("[data-exhibitions-prototype]");
    await raceWithAbort(root.waitFor({ state: "visible", timeout: 30_000 }), signal);
    const row = frame.locator('[data-exhibition-row][data-event-id="5370"]');
    await raceWithAbort(row.waitFor({ state: "attached", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, row, signal, EXHIBITIONS_FRAME_SELECTOR);
    const deck = row.locator("[data-deck]");
    const mediaTotal = Number(await deck.getAttribute("data-media-total"));
    assertCondition(mediaTotal >= 4, `exhibition 5370 exposes only ${mediaTotal} image(s)`);
    await deck.focus();
    await this.page.evaluate(() => {
      window.dispatchEvent(
        new CustomEvent("presenter:desktop-ui-response", {
          detail: { label: "Выставка найдена · нажимайте → на пульте" },
        }),
      );
    });
    await abortableDelay(1_200, signal);
    await this.captureScenario(scenarioId);
    return {
      summary:
        `live /vystavki/ scrolled to exhibition 5370 with ${mediaTotal} images; ` +
        "desktop D-pad is focused on its gallery",
      durationMs: Date.now() - startedAt,
    };
  }

  async runFestivalsNavigation(signal) {
    const scenarioId = "service-navigation-festivals";
    const startedAt = Date.now();
    await raceWithAbort(this.openStage(this.page), signal);
    await this.setInteractionMode("desktop-passive");
    await this.setAgentState("running", `${scenarioId} · desktop`);
    await this.showPresenterScene(scenarioId, signal);
    const scene = this.page.locator(`[data-presenter-scene-id="${scenarioId}"]`);
    await raceWithAbort(scene.waitFor({ state: "visible", timeout: 15_000 }), signal);
    const desktop = this.page.frameLocator(FESTIVALS_DESKTOP_FRAME_SELECTOR);
    const desktopRoot = desktop.locator("[data-festival-timeline]");
    await raceWithAbort(desktopRoot.waitFor({ state: "visible", timeout: 30_000 }), signal);
    const desktopMonths = desktop.locator("[data-festival-month]");
    assertCondition((await desktopMonths.count()) >= 2, "festival desktop timeline has fewer than two months");
    await this.naturalVerticalScroll(
      desktop,
      desktopMonths.nth(1),
      signal,
      FESTIVALS_DESKTOP_FRAME_SELECTOR,
    );
    await abortableDelay(1_300, signal);
    await this.naturalVerticalScroll(
      desktop,
      desktopMonths.last(),
      signal,
      FESTIVALS_DESKTOP_FRAME_SELECTOR,
    );
    await abortableDelay(1_500, signal);

    await scene.evaluate((node) => node.setAttribute("data-festival-phase", "mobile"));
    await this.setInteractionMode("mobile");
    await this.setAgentState("running", `${scenarioId} · mobile`);
    const mobileIframe = this.page.locator(FESTIVALS_MOBILE_FRAME_SELECTOR);
    await raceWithAbort(mobileIframe.waitFor({ state: "visible", timeout: 10_000 }), signal);
    const mobile = this.page.frameLocator(FESTIVALS_MOBILE_FRAME_SELECTOR);
    const mobileRoot = mobile.locator("[data-festival-timeline]");
    await raceWithAbort(mobileRoot.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.enforceMobilePointerShield(mobile);
    const mobileMonths = mobile.locator("[data-festival-month]");
    await this.naturalVerticalScroll(
      mobile,
      mobileMonths.nth(Math.min(1, Math.max(0, (await mobileMonths.count()) - 1))),
      signal,
      FESTIVALS_MOBILE_FRAME_SELECTOR,
    );
    await abortableDelay(1_300, signal);
    await this.naturalVerticalScroll(
      mobile,
      mobileMonths.last(),
      signal,
      FESTIVALS_MOBILE_FRAME_SELECTOR,
    );
    await abortableDelay(1_500, signal);
    await this.captureScenario(scenarioId);
    return {
      summary:
        "live /festivali/ slowly scrolled desktop, switched to mobile, and slowly scrolled again",
      durationMs: Date.now() - startedAt,
    };
  }

  async stageFrame(frameSelector, signal) {
    const iframe = this.page.locator(frameSelector);
    await raceWithAbort(iframe.waitFor({ state: "visible", timeout: 15_000 }), signal);
    const handle = await iframe.elementHandle();
    const frame = await handle?.contentFrame();
    assertCondition(frame, `stage frame ${frameSelector} did not attach`);
    await raceWithAbort(
      frame.waitForURL((url) => url.href.startsWith("https://kenigevents.ru/"), {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      }),
      signal,
    );
    return frame;
  }

  async showDesktopKey(code, label, response, signal, action) {
    await this.page.evaluate(({ code: nextCode, label: nextLabel }) => {
      window.dispatchEvent(new CustomEvent("presenter:desktop-key-visual", {
        detail: { code: nextCode, label: nextLabel, pressed: true },
      }));
    }, { code, label });
    await raceWithAbort(action(), signal);
    await this.page.evaluate(({ code: nextCode, response: nextResponse }) => {
      window.dispatchEvent(new CustomEvent("presenter:desktop-ui-response", {
        detail: { label: nextResponse },
      }));
      window.setTimeout(() => window.dispatchEvent(new CustomEvent("presenter:desktop-key-visual", {
        detail: { code: nextCode, pressed: false },
      })), 620);
    }, { code, response });
    await abortableDelay(1_050, signal);
  }

  async runNavigationMap(signal) {
    const scenarioId = "service-navigation-map";
    const startedAt = Date.now();
    const selector = '[data-presenter-id="navigation-mobile-frame"]';
    await this.setInteractionMode("mobile");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(selector, signal);
    await this.enforceMobilePointerShield(frame);
    const menu = frame.locator("[data-mobile-discovery-menu] > summary").first();
    await raceWithAbort(menu.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, menu, signal, selector);
    await menu.click();
    await abortableDelay(2_200, signal);
    const free = frame.locator('a[href*="/podborki/besplatnye-sobytiya/"]').first();
    await raceWithAbort(free.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await free.click();
    await abortableDelay(2_500, signal);
    for (const pathName of ["/izbrannoe/", "/dlya-menya/", "/populyarnoe/"]) {
      await raceWithAbort(
        frame.goto(`${FOCUS_PREVIEW_BASE_URL}${pathName}`, {
          waitUntil: "domcontentloaded",
          timeout: 30_000,
        }),
        signal,
      );
      await abortableDelay(2_000, signal);
    }
    await this.captureScenario(scenarioId);
    return {
      summary: "real mobile home menu opened, collection selected, then Favorites, For me and Popular were shown",
      durationMs: Date.now() - startedAt,
    };
  }

  async runFeedbackCollection(scenarioId, signal) {
    const startedAt = Date.now();
    const frameSelector = scenarioId === "service-social-proof"
      ? '[data-presenter-id="social-proof-frame"]'
      : '[data-presenter-id="fast-find-frame"]';
    await this.setInteractionMode("mobile");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    await this.enforceMobilePointerShield(frame);
    const likes = frame.locator('[data-feedback-action="like"]:visible');
    await raceWithAbort(likes.first().waitFor({ state: "visible", timeout: 30_000 }), signal);
    const likeCount = await likes.count();
    assertCondition(likeCount >= 3, `free collection exposes only ${likeCount} visible like buttons`);
    for (let index = 0; index < 3; index += 1) {
      const like = likes.nth(index);
      await this.naturalVerticalScroll(frame, like, signal, frameSelector);
      await abortableDelay(700, signal);
      await like.click();
      await abortableDelay(1_350, signal);
    }
    const dislike = frame.locator('[data-feedback-action="not_interested"]:visible').nth(
      Math.min(3, Math.max(0, (await frame.locator('[data-feedback-action="not_interested"]:visible').count()) - 1)),
    );
    await raceWithAbort(dislike.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.naturalVerticalScroll(frame, dislike, signal, frameSelector);
    await dislike.click();
    await abortableDelay(1_500, signal);
    await this.captureScenario(scenarioId);
    return {
      summary: "real free-events collection was naturally scrolled; three likes and one not-interested action were shown",
      durationMs: Date.now() - startedAt,
    };
  }

  async runArtifactDesktop(signal) {
    const scenarioId = "service-artifact-desktop";
    const startedAt = Date.now();
    const frameSelector = '[data-presenter-id="artifact-desktop-frame"]';
    await this.setInteractionMode("desktop");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    const artifact = frame.locator("[data-amber-artifact]:visible").first();
    await raceWithAbort(artifact.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, artifact, signal, frameSelector);
    await artifact.evaluate((node) => node.setAttribute("tabindex", "0"));
    await artifact.focus();
    await this.showDesktopKey("ArrowRight", "→", "Артефакт найден · открываем", signal, () => artifact.press("ArrowRight"));
    await artifact.click();
    await abortableDelay(2_000, signal);
    await this.captureScenario(scenarioId);
    return { summary: "real desktop weekend page scrolled to and opened the amber artifact", durationMs: Date.now() - startedAt };
  }

  async runKeyboardInterface(scenarioId, signal) {
    const startedAt = Date.now();
    const frameSelector = scenarioId === "service-keyboard-day"
      ? '[data-presenter-id="keyboard-day-frame"]'
      : '[data-presenter-id="keyboard-event-frame"]';
    await this.setInteractionMode("desktop");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    let target = scenarioId === "service-keyboard-event"
      ? frame.locator("[data-keyboard-event-surface]:visible").first()
      : frame.locator("[data-event-card]:visible").first();
    await raceWithAbort(target.waitFor({ state: "visible", timeout: 30_000 }), signal);
    if (scenarioId === "service-keyboard-day") {
      const cards = frame.locator("[data-event-card]:visible");
      const count = await cards.count();
      assertCondition(count >= 4, `day page exposes only ${count} desktop event cards`);
      for (const index of [0, 1, 2, 3]) {
        await cards.nth(index).evaluate((node) => node.setAttribute("tabindex", "0"));
      }
      await cards.first().focus();
      await this.showDesktopKey("ArrowRight", "→", "Выбрано следующее событие", signal, async () => {
        await cards.nth(1).focus();
      });
      await this.showDesktopKey("ArrowLeft", "←", "Вернулись к предыдущему событию", signal, async () => {
        await cards.first().focus();
      });
      await this.showDesktopKey("ArrowDown", "↓", "Перешли ниже по расписанию", signal, async () => {
        await this.naturalVerticalScroll(frame, cards.nth(3), signal, frameSelector);
        await cards.nth(3).focus();
      });
      await this.showDesktopKey("ArrowUp", "↑", "Вернулись выше", signal, async () => {
        await this.naturalVerticalScroll(frame, cards.first(), signal, frameSelector);
        await cards.first().focus();
      });
      await this.captureScenario(scenarioId);
      return {
        summary: "real desktop day cards were selected and scrolled with visible Arrow-key cues",
        durationMs: Date.now() - startedAt,
      };
    }
    await target.focus();
    const sequence = [["ArrowRight", "→", "Галерея сдвинулась"], ["ArrowDown", "↓", "Фокус ниже по странице"], ["KeyL", "L", "Событие отмечено лайком"]];
    for (const [code, label, response] of sequence) {
      await this.showDesktopKey(code, label, response, signal, () => target.press(code));
      target = frame.locator(":focus");
    }
    await this.captureScenario(scenarioId);
    return { summary: "real desktop interface visibly reacted to the demonstrated keys", durationMs: Date.now() - startedAt };
  }

  async runTransportBus(signal) {
    const scenarioId = "service-transport-bus";
    const startedAt = Date.now();
    const frameSelector = '[data-presenter-id="transport-bus-frame"]';
    await this.setInteractionMode("mobile");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    await this.enforceMobilePointerShield(frame);
    const schedule = frame.locator("[data-event-bus-schedule]:visible").first();
    await raceWithAbort(schedule.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, schedule, signal, frameSelector);
    await abortableDelay(2_800, signal);
    await this.captureScenario(scenarioId);
    return { summary: "real mobile event page naturally scrolled to its source-backed bus schedule", durationMs: Date.now() - startedAt };
  }

  async runReportProblem(signal) {
    const scenarioId = "service-report-problem";
    const startedAt = Date.now();
    const frameSelector = '[data-presenter-id="report-problem-frame"]';
    await this.setInteractionMode("mobile");
    await this.showPresenterScene(scenarioId, signal);
    await abortableDelay(2_800, signal);
    const scene = this.page.locator(`[data-presenter-scene-id="${scenarioId}"]`);
    await scene.evaluate((node) => node.setAttribute("data-report-phase", "interface"));
    const frame = await this.stageFrame(frameSelector, signal);
    await this.enforceMobilePointerShield(frame);
    const feedback = frame.locator("[data-focus-feedback]:visible").first();
    await raceWithAbort(feedback.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, feedback, signal, frameSelector);
    await abortableDelay(1_800, signal);
    const button = feedback.locator('[data-feedback-open="event_issue"], [data-feedback-open="surface"]').first();
    await button.click();
    await abortableDelay(2_000, signal);
    await this.captureScenario(scenarioId);
    return { summary: "problem statement transitioned to real mobile footer feedback controls and opened the issue form", durationMs: Date.now() - startedAt };
  }

  async runShareFriends(signal) {
    const scenarioId = "service-share-friends";
    const startedAt = Date.now();
    const frameSelector = '[data-presenter-id="share-friends-frame"]';
    await this.setInteractionMode("mobile");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    await this.enforceMobilePointerShield(frame);
    const share = frame.locator(".feedback-button--share:visible, [data-share-action]:visible").first();
    await raceWithAbort(share.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, share, signal, frameSelector);
    await abortableDelay(1_200, signal);
    await share.click();
    await abortableDelay(2_000, signal);
    await this.page.locator(`[data-presenter-scene-id="${scenarioId}"]`)
      .evaluate((node) => node.setAttribute("data-share-phase", "proof"));
    await abortableDelay(3_200, signal);
    await this.captureScenario(scenarioId);
    return { summary: "real event page share control was found, scrolled into view and pressed", durationMs: Date.now() - startedAt };
  }

  async runCalendarMemory(signal) {
    const scenarioId = "service-calendar-memory";
    const startedAt = Date.now();
    const frameSelector = '[data-presenter-id="calendar-memory-frame"]';
    await this.setInteractionMode("mobile");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    await this.enforceMobilePointerShield(frame);
    const calendar = frame.locator("[data-calendar-action]:visible").first();
    await raceWithAbort(calendar.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, calendar, signal, frameSelector);
    await calendar.click().catch(() => {});
    await abortableDelay(1_800, signal);
    await raceWithAbort(frame.goto(`${FOCUS_PREVIEW_BASE_URL}/izbrannoe/`, {
      waitUntil: "domcontentloaded",
      timeout: 30_000,
    }), signal);
    await abortableDelay(2_500, signal);
    await this.captureScenario(scenarioId);
    return { summary: "real calendar action was pressed, then the real Favorites page was opened", durationMs: Date.now() - startedAt };
  }

  async runCommunityCurator(signal) {
    const scenarioId = "service-community-curator";
    const startedAt = Date.now();
    const frameSelector = '[data-presenter-id="community-curator-frame"]';
    await this.setInteractionMode("desktop");
    await this.showPresenterScene(scenarioId, signal);
    const frame = await this.stageFrame(frameSelector, signal);
    const copies = frame.locator("button:visible").filter({ hasText: /копир|описан|картин|постер/i });
    await raceWithAbort(copies.first().waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, copies.first(), signal, frameSelector);
    const count = Math.min(2, await copies.count());
    for (let index = 0; index < count; index += 1) {
      const button = copies.nth(index);
      if (await button.isVisible().catch(() => false)) {
        await button.click();
        await abortableDelay(1_200, signal);
      }
    }
    await this.captureScenario(scenarioId);
    return { summary: "real desktop event interface showed and pressed author copy controls", durationMs: Date.now() - startedAt };
  }

  async runTransportRail(signal) {
    const scenarioId = "service-transport-rail";
    const startedAt = Date.now();
    await this.setInteractionMode("stage");
    await this.setAgentState("running", scenarioId);
    await this.showPresenterScene(scenarioId, signal);
    const scene = this.page.locator(`[data-presenter-scene-id="${scenarioId}"]`);
    await raceWithAbort(scene.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await scene.evaluate((node) => node.setAttribute("data-transport-phase", "mobile"));

    const mobileFrame = await this.focusFrame(
      '[data-presenter-id="transport-rail-mobile-frame"]',
      signal,
    );
    const mobileSchedule = mobileFrame.locator("[data-event-transport-schedule]:visible").first();
    await raceWithAbort(
      mobileSchedule.waitFor({ state: "visible", timeout: 30_000 }),
      signal,
    );
    await mobileSchedule.scrollIntoViewIfNeeded();
    await abortableDelay(2_400, signal);

    await scene.evaluate((node) => node.setAttribute("data-transport-phase", "desktop"));
    const desktopFrame = await this.focusFrame(
      '[data-presenter-id="transport-rail-desktop-frame"]',
      signal,
    );
    const desktopSchedule = desktopFrame.locator(
      "[data-desktop-transport] [data-event-transport-schedule]:visible",
    ).first();
    await raceWithAbort(
      desktopSchedule.waitFor({ state: "visible", timeout: 30_000 }),
      signal,
    );
    await desktopSchedule.scrollIntoViewIfNeeded();
    await abortableDelay(2_400, signal);
    await this.captureScenario(scenarioId);
    return {
      summary:
        "same source-backed rail schedule shown first in the mobile event page and then in desktop",
      durationMs: Date.now() - startedAt,
    };
  }

  async focusFrame(frameSelector, signal) {
    const iframe = this.page.locator(frameSelector);
    await raceWithAbort(iframe.waitFor({ state: "visible", timeout: 10_000 }), signal);
    const handle = await iframe.elementHandle();
    const frame = await handle?.contentFrame();
    assertCondition(frame, `focus frame ${frameSelector} did not attach`);
    await raceWithAbort(
      frame.waitForURL(
        (url) => url.href.startsWith(FOCUS_PREVIEW_BASE_URL),
        { waitUntil: "domcontentloaded", timeout: 30_000 },
      ),
      signal,
    );
    assertCondition(
      frame.url().startsWith(FOCUS_PREVIEW_BASE_URL),
      `focus frame opened unexpected URL ${frame.url()}`,
    );
    return frame;
  }

  async resetFocusFrame(frameSelector, signal) {
    const iframe = this.page.locator(frameSelector);
    await raceWithAbort(
      iframe.evaluate((node) => {
        if (!(node instanceof HTMLIFrameElement)) {
          throw new Error("focus frame is not an iframe");
        }
        const target = node.dataset.src || "";
        if (!target) throw new Error("focus frame has no pinned data-src");
        node.src = target;
      }),
      signal,
    );
  }

  async runFocusMedallionScene(scenarioId, signal, mode) {
    const startedAt = Date.now();
    await this.setInteractionMode(mode === "mobile" ? "mobile" : "desktop-passive");
    await this.showPresenterScene(scenarioId, signal);
    const frameSelector =
      mode === "mobile"
        ? '[data-presenter-id="medallion-mobile-frame"]'
        : '[data-presenter-id="medallion-desktop-frame"]';
    await this.resetFocusFrame(frameSelector, signal);
    const frame = await this.focusFrame(frameSelector, signal);
    const medallions = frame.locator("[data-medallion-layout]:visible").first();
    await raceWithAbort(medallions.waitFor({ state: "visible", timeout: 30_000 }), signal);
    const layout = await medallions.getAttribute("data-medallion-layout");
    const tokenCount = await medallions.locator("[data-medallion-role]").count();
    if (mode === "desktop") {
      assertCondition(layout === "desktop-slots", `desktop medallion layout is ${layout}`);
      assertCondition(
        (await medallions.getAttribute("data-top-slot-enabled")) === "true",
        "desktop example has no enabled top medallion slot",
      );
      const hero = frame.locator("[data-clean-hero-image]").first();
      await raceWithAbort(hero.waitFor({ state: "visible", timeout: 30_000 }), signal);
      const ratio = await hero.evaluate((image) =>
        image instanceof HTMLImageElement && image.naturalHeight
          ? image.naturalWidth / image.naturalHeight
          : 0,
      );
      assertCondition(ratio >= 1.45, `desktop example hero ratio ${ratio} is not horizontal`);
      await abortableDelay(4_500, signal);
      const inlineMedallions = medallions.locator('[data-medallion-slot="inline"]');
      await raceWithAbort(
        inlineMedallions.waitFor({ state: "visible", timeout: 30_000 }),
        signal,
      );
      await this.naturalVerticalScroll(frame, inlineMedallions, signal, frameSelector);
    } else {
      await medallions.scrollIntoViewIfNeeded();
    }
    assertCondition(
      tokenCount >= (mode === "desktop" ? 2 : 1),
      `${mode} example exposes only ${tokenCount} medallion`,
    );
    await abortableDelay(1_400, signal);
    await this.captureScenario(scenarioId);
    return {
      summary: mode === "desktop"
        ? `focus-preview desktop specimen shows top slot and ${tokenCount} real medallions`
        : `supplied focus-preview event 6865 shows ${tokenCount} real medallion in mobile composition`,
      durationMs: Date.now() - startedAt,
    };
  }

  async runFocusInvitation(signal) {
    const startedAt = Date.now();
    await this.setInteractionMode("stage");
    await this.showPresenterScene(FOCUS_INVITATION_SCENE_ID, signal);
    const scene = this.page.locator(
      `[data-presenter-scene-id="${FOCUS_INVITATION_SCENE_ID}"]`,
    );
    await raceWithAbort(scene.waitFor({ state: "visible", timeout: 10_000 }), signal);
    const link = scene.locator(".focus-qr");
    const actualUrl = new URL(await link.getAttribute("href"));
    const expectedUrl = new URL(FOCUS_INVITATION_URL);
    assertCondition(
      actualUrl.href === expectedUrl.href,
      `focus QR points to unexpected URL ${actualUrl.href}`,
    );
    assertCondition(
      (await link.locator("svg").count()) === 1,
      "focus invitation QR SVG is missing",
    );
    await abortableDelay(1_200, signal);
    await this.captureScenario(FOCUS_INVITATION_SCENE_ID);
    return {
      summary: "large focus-group QR points to the exact current onboarding invitation",
      durationMs: Date.now() - startedAt,
    };
  }

  async activateFocusParticipation(frame, signal) {
    await raceWithAbort(
      frame.goto(FOCUS_INVITATION_URL, { waitUntil: "domcontentloaded", timeout: 30_000 }),
      signal,
    );
    await this.setAgentState("running", "03.14 · приглашение открыто — показываем вход");
    await abortableDelay(2_800, signal);
    const installSkip = frame.locator("[data-focus-install-skip]");
    if (await installSkip.isVisible().catch(() => false)) {
      await installSkip.click();
      await this.setAgentState("running", "03.14 · продолжаем на сайте");
      await abortableDelay(2_400, signal);
    }
    const skip = frame.locator("[data-focus-skip]");
    await raceWithAbort(skip.waitFor({ state: "visible", timeout: 15_000 }), signal);
    await this.setAgentState("running", "03.14 · шаг фокус-группы виден");
    await abortableDelay(2_800, signal);
    await skip.click();
    await raceWithAbort(
      frame.waitForFunction(() => {
        try {
          return JSON.parse(
            localStorage.getItem("kenigevents:focus-participation:v1") || "null",
          )?.status === "active";
        } catch {
          return false;
        }
      }, undefined, { timeout: 10_000 }),
      signal,
    );
    await this.setAgentState("running", "03.14 · вход в фокус-группу завершён");
    await abortableDelay(2_200, signal);
  }

  async runFocusNps(signal) {
    const startedAt = Date.now();
    await this.setInteractionMode("mobile");
    await this.showPresenterScene("service-nps", signal);
    const selector = '[data-presenter-id="focus-nps-frame"]';
    const frame = await this.focusFrame(selector, signal);
    await this.activateFocusParticipation(frame, signal);
    await raceWithAbort(
      frame.goto(FOCUS_PAGE_RATING_URL, { waitUntil: "domcontentloaded", timeout: 30_000 }),
      signal,
    );
    const panel = frame.locator("[data-focus-lab-panel]:not([hidden])");
    await raceWithAbort(panel.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await this.naturalVerticalScroll(frame, panel, signal, selector);
    assertCondition(
      (await panel.locator("[data-focus-score]").count()) === 11,
      "real page rating block must show scores 0–10",
    );
    await this.setAgentState("running", "03.14 · оценка страницы 0–10 в кадре");
    await abortableDelay(4_500, signal);
    await this.captureScenario("service-nps");
    await this.setInteractionMode("stage");
    await this.showPresenterScene(FOCUS_INVITATION_SCENE_ID, signal);
    await this.setAgentState("running", "03.14 · возвращаем QR, чтобы успели отсканировать");
    await abortableDelay(2_000, signal);
    return {
      summary:
        "slow focus onboarding and real Today 0–10 rating were shown; the held focus-group QR was restored",
      durationMs: Date.now() - startedAt,
    };
  }

  async runPeopleScene(signal) {
    const startedAt = Date.now();
    await this.setInteractionMode("stage");
    await this.showPresenterScene("service-future-celebrity", signal);
    const selector = '[data-presenter-id="people-interface-frame"]';
    const iframe = this.page.locator(selector);
    await raceWithAbort(iframe.waitFor({ state: "visible", timeout: 20_000 }), signal);
    await raceWithAbort(
      this.page.waitForFunction(
        () =>
          document.querySelector('[data-presenter-scene-id="service-future-celebrity"]')
            ?.getAttribute("data-people-phase") === "interface",
        undefined,
        { timeout: 15_000 },
      ),
      signal,
    );
    const handle = await iframe.elementHandle();
    const frame = await handle?.contentFrame();
    assertCondition(frame, "people interface frame did not attach");
    const participants = frame.locator("[data-event-participants]:visible").first();
    await raceWithAbort(participants.waitFor({ state: "visible", timeout: 20_000 }), signal);
    await participants.scrollIntoViewIfNeeded();
    const like = participants.locator("[data-participant-like]").first();
    const before = await like.getAttribute("aria-pressed");
    if (before !== "true") await like.click();
    assertCondition(
      (await like.getAttribute("aria-pressed")) === "true",
      "participant like did not become pressed",
    );
    await abortableDelay(1_500, signal);
    await this.captureScenario("service-future-celebrity");
    return {
      summary: "three verified people transitioned into the real participant UI and one like was shown",
      durationMs: Date.now() - startedAt,
    };
  }

  async runSearchAuthSetup(signal) {
    const startedAt = Date.now();
    await this.setInteractionMode("desktop");
    await this.setAgentState(
      "running",
      "Подготовка поиска · один раз войдите в отдельный demo-аккаунт Яндекса",
    );
    const authPage = await this.context.newPage();
    this.auxiliaryPage = authPage;
    try {
      await authPage.bringToFront();
      await raceWithAbort(
        authPage.goto(`${FOCUS_PREVIEW_BASE_URL}/poisk/`, {
          waitUntil: "domcontentloaded",
          timeout: 30_000,
        }),
        signal,
      );
      const login = authPage.locator("[data-search-login]");
      const logout = authPage.locator("[data-search-logout]");
      await raceWithAbort(
        authPage.locator("[data-authorized-search]").waitFor({ state: "visible", timeout: 30_000 }),
        signal,
      );
      if (!(await logout.isVisible().catch(() => false))) {
        await abortableDelay(1_800, signal);
        await login.click();
        await this.setAgentState(
          "running",
          "Подготовка поиска · завершите вход через Яндекс; любая Run/Reset вернёт презентацию",
        );
        await raceWithAbort(
          authPage.waitForFunction(
            () => {
              const control = document.querySelector("[data-search-logout]");
              return control instanceof HTMLElement && !control.hidden;
            },
            undefined,
            { timeout: 9 * 60 * 1_000 },
          ),
          signal,
        );
      }
      assertCondition(
        await logout.isVisible().catch(() => false),
        "demo search account did not become authenticated",
      );
      if (config.storageStatePath) {
        await mkdir(path.dirname(config.storageStatePath), { recursive: true });
        await this.context.storageState({ path: config.storageStatePath });
      }
      await this.setAgentState("running", "Подготовка поиска · вход сохранён в локальном кеше Windows");
      await abortableDelay(1_500, signal);
    } finally {
      if (!authPage.isClosed()) await authPage.close().catch(() => {});
      if (this.auxiliaryPage === authPage) this.auxiliaryPage = null;
      if (this.page && !this.page.isClosed()) await this.page.bringToFront().catch(() => {});
    }
    return {
      summary:
        "dedicated Yandex demo-account session is authenticated and saved only in the Windows browser-state cache",
      durationMs: Date.now() - startedAt,
    };
  }

  async runFocusSearch(signal, rawQuery) {
    const startedAt = Date.now();
    const query = String(rawQuery || "").trim().slice(0, 180);
    assertCondition(query.length >= 2, "smart-search scene needs a query from the PWA");
    await this.setInteractionMode("mobile");
    await this.showPresenterScene("service-search-live", signal, { query });
    await this.resetFocusFrame('[data-presenter-id="focus-search-frame"]', signal);
    const frame = await this.focusFrame('[data-presenter-id="focus-search-frame"]', signal);
    const input = frame.locator("[data-search-input]");
    await raceWithAbort(input.waitFor({ state: "visible", timeout: 30_000 }), signal);
    await input.scrollIntoViewIfNeeded();
    await input.fill("");
    await input.pressSequentially(query, { delay: 86 });
    const submit = frame.locator("[data-search-submit]");
    const signedIn = await frame.locator("[data-search-logout]").isVisible().catch(() => false);
    assertCondition(
      signedIn,
      "Поиск требует однократного входа в отдельный demo-аккаунт; сессия затем сохраняется в общем browser-state cache",
    );
    assertCondition(
      await submit.isEnabled().catch(() => false),
      "search submit stayed disabled after the authenticated query was typed",
    );
    await submit.click();
    const results = frame.locator("[data-search-results]:visible");
    await raceWithAbort(results.waitFor({ state: "visible", timeout: 30_000 }), signal);
    const cards = results.locator("[data-event-card]");
    await raceWithAbort(cards.first().waitFor({ state: "visible", timeout: 30_000 }), signal);
    assertCondition((await cards.count()) > 0, "search completed without real event cards");
    if (config.storageStatePath) {
      await mkdir(path.dirname(config.storageStatePath), { recursive: true });
      await this.context.storageState({ path: config.storageStatePath });
    }
    await abortableDelay(2_000, signal);
    await this.captureScenario("service-search-live");
    return {
      summary:
        `query "${query}" was typed, submitted with the persistent demo session, ` +
        `and produced ${await cards.count()} real event cards`,
      durationMs: Date.now() - startedAt,
    };
  }

  async runWeekendDesktop(signal) {
    const startedAt = Date.now();
    await raceWithAbort(this.openStage(this.page), signal);
    await this.setInteractionMode("desktop-passive");
    await this.setAgentState("running", WEEKEND_DESKTOP_CONTRACT.id);
    await this.showPresenterScene(WEEKEND_DESKTOP_SCENE_ID, signal);
    await abortableDelay(4_500, signal);
    const iframe = this.page.locator(DESKTOP_FRAME_SELECTOR);
    await raceWithAbort(iframe.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await raceWithAbort(
      this.page.waitForFunction(
        (selector) =>
          document.querySelector(selector)?.getAttribute("data-presenter-frame-ready") ===
          "true",
        DESKTOP_FRAME_SELECTOR,
        { timeout: 30_000 },
      ),
      signal,
    );
    const frame = this.page.frameLocator(DESKTOP_FRAME_SELECTOR);
    const weekend = frame.locator(WEEKEND_DESKTOP_ROOT_SELECTOR);
    await raceWithAbort(weekend.waitFor({ state: "visible", timeout: 30_000 }), signal);
    assertCondition(
      (await weekend.getAttribute("data-weekend-start")) === "2026-08-01"
        && (await weekend.getAttribute("data-weekend-end")) === "2026-08-02",
      "weekend desktop page does not show the current 1–2 August 2026 range",
    );
    await this.waitForEmbeddedReady(frame, signal);
    const iframeHandle = await iframe.elementHandle();
    const desktopContentFrame = await iframeHandle?.contentFrame();
    assertCondition(desktopContentFrame, "desktop weekend frame did not attach");
    const desktopUrl = new URL(desktopContentFrame.url());
    const path = `${this.normalizedPreviewPath(desktopUrl.pathname)}${desktopUrl.hash}`;
    assertCondition(path === "/vyhodnye/", `desktop weekend route is ${path}`);
    await abortableDelay(1_500, signal);
    const footer = frame.locator(SITE_FOOTER_SELECTOR);
    await raceWithAbort(footer.waitFor({ state: "attached", timeout: 10_000 }), signal);
    await this.naturalVerticalScroll(
      frame,
      footer,
      signal,
      DESKTOP_FRAME_SELECTOR,
    );
    await abortableDelay(2_000, signal);
    await this.captureScenario(WEEKEND_DESKTOP_CONTRACT.id);
    return {
      summary:
        "live /vyhodnye/ desktop page filled the 1920x1080 stage and naturally scrolled down",
      durationMs: Date.now() - startedAt,
    };
  }

  async runOutroQr(signal) {
    const startedAt = Date.now();
    await raceWithAbort(
      this.page.locator(STAGE_READY_SELECTOR).waitFor({ state: "visible", timeout: 10_000 }),
      signal,
    );
    await this.setAgentState("running", OUTRO_QR_CONTRACT.id);
    await this.showPresenterScene(OUTRO_SCENE_ID, signal);

    const scene = this.page.locator(OUTRO_READY_SELECTOR);
    const qr = scene.locator(OUTRO_QR_SELECTOR);
    await raceWithAbort(qr.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await raceWithAbort(
      qr.evaluate(async (image, expectedUrl) => {
        if (!(image instanceof HTMLImageElement)) {
          throw new Error("outro QR node is not an image");
        }
        if (!image.complete) {
          await new Promise((resolve, reject) => {
            image.addEventListener("load", resolve, { once: true });
            image.addEventListener("error", reject, { once: true });
          });
        }
        if (image.naturalWidth <= 0 || image.currentSrc !== expectedUrl) {
          throw new Error(
            `outro QR failed to load from the pinned CDN URL: ${image.currentSrc}`,
          );
        }
      }, OUTRO_QR_ASSET.url),
      signal,
    );
    await abortableDelay(1_500, signal);
    await this.captureScenario(OUTRO_QR_CONTRACT.id);
    return {
      summary: "fullscreen survey QR loaded from the immutable CDN and remains visible",
      durationMs: Date.now() - startedAt,
    };
  }

  async openTomorrowFromHome(frame, signal) {
    const menuSummary = frame.locator(TOMORROW_MENU_SUMMARY_SELECTOR);
    await raceWithAbort(menuSummary.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.naturalVerticalScroll(frame, menuSummary, signal);
    await this.tapMobileLocator(frame, menuSummary, signal);
    const menu = frame.locator("[data-mobile-discovery-menu]");
    await this.waitForAttribute(menu, "open", null, signal);
    await abortableDelay(2_200, signal);
    const target = frame.locator(TOMORROW_MENU_LINK_SELECTOR);
    await raceWithAbort(target.waitFor({ state: "visible", timeout: 10_000 }), signal);
    const boundingBox = await target.boundingBox();
    if (!boundingBox) throw new Error(`${TOMORROW_MENU_LINK_SELECTOR} has no boundingBox`);
    log("target acquired", { selector: TOMORROW_MENU_LINK_SELECTOR, boundingBox });
    await abortableDelay(600, signal);
    await this.tapMobileLocator(frame, target, signal);
    await raceWithAbort(
      frame.locator(TOMORROW_READY_SELECTOR).waitFor({ state: "visible", timeout: 10_000 }),
      signal,
    );
    await this.waitForEmbeddedPath("/zavtra/", signal);
    await this.waitForEmbeddedReady(frame, signal);
    await abortableDelay(PACING.routeDwellMs, signal);
    await this.enforceMobilePointerShield(frame);
  }

  async runTomorrowMobile(signal) {
    const startedAt = Date.now();
    const frame = await this.prepareScenarioStage(TOMORROW_MOBILE_CONTRACT.id, signal);
    await this.openTomorrowFromHome(frame, signal);

    const event = await this.selectTomorrowEvent(frame, signal);
    const row = frame.locator(
      `${TOMORROW_ROWS_SELECTOR}[data-event-id="${event.eventId}"]`,
    );
    await this.naturalVerticalScroll(frame, row, signal);
    const rail = row.locator(MOBILE_EVENT_RAIL_SELECTOR);
    const digest = row.locator(MOBILE_EVENT_DESCRIPTION_SELECTOR);
    await raceWithAbort(rail.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await raceWithAbort(digest.waitFor({ state: "attached", timeout: 10_000 }), signal);
    log("deterministic tomorrow event selected", event);

    let digestRevealed = await this.isHorizontallyRevealed(digest);
    for (let attempt = 0; attempt < 4 && !digestRevealed; attempt += 1) {
      await this.swipeRailLeft(frame, rail, signal, "Листаем к описанию");
      await this.waitForScrollSettle(rail, signal, "horizontal");
      digestRevealed = await this.isHorizontallyRevealed(digest);
    }
    if (!digestRevealed) {
      const geometry = await this.railGeometry(rail);
      throw new Error(
        `${MOBILE_EVENT_DESCRIPTION_SELECTOR} did not become horizontally visible: ${JSON.stringify(geometry)}`,
      );
    }

    await this.dwellOnDescription(digest, signal, "rail", DESCRIPTION_DWELL_MS);
    await abortableDelay(PACING.routeDwellMs, signal);
    await this.tapMobileLocator(frame, digest, signal);
    await raceWithAbort(
      frame.locator(MOBILE_DETAIL_SELECTOR).waitFor({ state: "visible", timeout: 10_000 }),
      signal,
    );
    await this.waitForEmbeddedPath(/^\/sobytiya\/[^/]+\/$/u, signal);
    await this.waitForEmbeddedReady(frame, signal);

    await this.enforceMobilePointerShield(frame);
    const detailDescription = frame.locator(MOBILE_DETAIL_DESCRIPTION_SELECTOR);
    await raceWithAbort(detailDescription.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.naturalVerticalScroll(frame, detailDescription, signal);
    await this.dwellOnDescription(detailDescription, signal, "event-detail", DETAIL_DWELL_MS);
    await this.finishTypicalPacing(startedAt, signal);
    await this.captureScenario(TOMORROW_MOBILE_CONTRACT.id);
    return {
      summary:
        `event ${event.eventId} "${this.cleanTitle(event.title)}"; ` +
        "digest revealed after horizontal swipe; detail description visible",
    };
  }

  async runTomorrowRailLike(signal) {
    const startedAt = Date.now();
    const contract = TOMORROW_RAIL_LIKE_CONTRACT;
    const frame = await this.prepareScenarioStage(contract.id, signal);
    await this.openTomorrowFromHome(frame, signal);

    const row = frame.locator(
      `${TOMORROW_ROWS_SELECTOR}[data-event-id="${contract.eventId}"]`,
    );
    await raceWithAbort(row.waitFor({ state: "attached", timeout: 10_000 }), signal);
    const title = await row.getAttribute("data-event-title");
    assertCondition(
      title === contract.eventTitle,
      `event ${contract.eventId} title mismatch: ${JSON.stringify(title)}`,
    );
    await this.assertEventNotPreLiked(frame, row, contract.eventId);
    await this.naturalVerticalScroll(frame, row, signal);
    log("like scenario target framed", { eventId: contract.eventId });
    await this.setAgentState("running", "04.2 · карточка события в кадре");

    const rail = row.locator(MOBILE_EVENT_RAIL_SELECTOR);
    const like = row.locator('[data-feedback-action="like"]');
    await raceWithAbort(rail.waitFor({ state: "visible", timeout: 10_000 }), signal);
    const beforeCount = await this.readLikeCount(like);

    await this.dragRailToEndInOneRelease(frame, rail, signal);
    const atEnd = await this.railGeometry(rail);
    assertCondition(
      atEnd.maxScroll > 0 && atEnd.maxScroll - atEnd.scrollLeft <= 1,
      `event ${contract.eventId} rail did not settle at maxScroll: ${JSON.stringify(atEnd)}`,
    );
    log("like scenario rail reached edge", { eventId: contract.eventId, ...atEnd });
    await this.setAgentState("running", "04.2 · rail у правого края, показываем состояние");
    await this.dwellOnAudienceTarget(
      rail,
      signal,
      "RAIL СДВИНУТ ВПРАВО",
      2_400,
    );

    await this.pullLikeEdgeAndAssertArmed(frame, rail, signal);
    log("like scenario edge pull released", { eventId: contract.eventId });
    await this.setAgentState("running", "04.2 · дополнительная протяжка поставила лайк");
    await this.dwellOnAudienceTarget(
      like,
      signal,
      "ДОПОЛНИТЕЛЬНАЯ ПРОТЯЖКА → ЛАЙК",
      2_200,
    );
    const consent = frame.locator("[data-personalization-consent].is-visible");
    const consentAccept = consent.locator("[data-personalization-consent-accept]");
    const consentRequired = await this.waitForEitherPressedOrConsent(
      like,
      consent,
      signal,
    );
    log("like scenario post-gesture branch", {
      eventId: contract.eventId,
      consentRequired,
    });
    if (consentRequired) {
      await raceWithAbort(consentAccept.waitFor({ state: "visible", timeout: 2_000 }), signal);
      // The consent sheet can be covered by transient mobile chrome in the
      // preview. Keep the visible tap cue, but dispatch to the exact button
      // rather than forcing a coordinate click that may hit the chrome.
      await this.tapMobileLocator(frame, consentAccept, signal, { dispatch: true });
      log("like scenario consent accepted", { eventId: contract.eventId });
      await raceWithAbort(consent.waitFor({ state: "hidden", timeout: 5_000 }), signal);
      await this.waitForConsentProfile(frame, signal);
      await abortableDelay(1_200, signal);
      const pendingActionCommitted = await this.isLikePersistenceStored(
        frame,
        contract.eventId,
      );
      if (!pendingActionCommitted) {
        // Consent preparation replaces the listing controls in this build.
        // Reload and repeat the visible gesture against fresh authorized DOM
        // rather than dragging a detached rail locator.
        await raceWithAbort(
          this.embeddedFrame().goto(`${FOCUS_PREVIEW_BASE_URL}/zavtra/`, {
            waitUntil: "domcontentloaded",
            timeout: 30_000,
          }),
          signal,
        );
        await this.waitForEmbeddedPath("/zavtra/", signal);
        await this.waitForEmbeddedReady(frame, signal);
        const authorizedRow = frame.locator(
          `${TOMORROW_ROWS_SELECTOR}[data-event-id="${contract.eventId}"]`,
        );
        await raceWithAbort(authorizedRow.waitFor({ state: "attached", timeout: 10_000 }), signal);
        await this.naturalVerticalScroll(frame, authorizedRow, signal);
        const authorizedRail = authorizedRow.locator(MOBILE_EVENT_RAIL_SELECTOR);
        await this.dragRailToEndInOneRelease(frame, authorizedRail, signal);
        await this.pullLikeEdgeAndAssertArmed(frame, authorizedRail, signal);
        log("like scenario authorized edge pull released", { eventId: contract.eventId });
      }
    }

    await this.waitForLikePersistenceStorage(frame, contract.eventId, signal);
    log("like scenario storage committed", { eventId: contract.eventId });
    await abortableDelay(1_800, signal);

    await this.reloadEmbeddedFrame(signal);
    await this.waitForEmbeddedPath("/zavtra/", signal);
    await this.waitForEmbeddedReady(frame, signal);
    const reloadedRow = frame.locator(
      `${TOMORROW_ROWS_SELECTOR}[data-event-id="${contract.eventId}"]`,
    );
    await raceWithAbort(reloadedRow.waitFor({ state: "attached", timeout: 10_000 }), signal);
    await this.naturalVerticalScroll(frame, reloadedRow, signal);
    const reloadedLike = reloadedRow.locator('[data-feedback-action="like"]');
    await this.waitForLikeState(reloadedRow, reloadedLike, true, signal);
    log("like scenario UI state pressed after reload", { eventId: contract.eventId });
    await this.setAgentState("running", "04.2 · лайк сохранён и подтверждён после reload");
    const afterCount = await this.waitForStableLikeCount(
      reloadedLike,
      beforeCount + 1,
      signal,
    );
    assertCondition(
      afterCount === beforeCount + 1,
      `like count did not increment exactly once: before=${beforeCount} after=${afterCount}`,
    );
    await this.assertLikePersistenceStorage(frame, contract.eventId);
    await this.dwellOnAudienceTarget(
      reloadedLike,
      signal,
      `ЛАЙК СОХРАНЁН · ${afterCount}`,
      3_400,
    );

    await this.finishTypicalPacing(startedAt, signal);
    await this.captureScenario(contract.id);
    return {
      summary:
        `event ${contract.eventId} "${contract.eventTitle}" liked only by armed edge gesture; ` +
        `count ${beforeCount}→${afterCount}; storage and reload persistence verified`,
    };
  }

  async runWeekendAmberArtifact(signal) {
    const startedAt = Date.now();
    const contract = WEEKEND_AMBER_ARTIFACT_CONTRACT;
    const frame = await this.prepareScenarioStage(contract.id, signal);
    const embedded = this.embeddedFrame();
    await raceWithAbort(
      embedded.goto(`${ARTIFACT_PREVIEW_BASE_URL}/`, {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      }),
      signal,
    );
    await this.waitForEmbeddedReady(frame, signal);
    await this.setAgentState("running", "03.3 · открываем меню и раздел «Выходные»");

    const menuSummary = frame.locator(WEEKEND_MENU_SUMMARY_SELECTOR);
    await raceWithAbort(menuSummary.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.tapMobileLocator(frame, menuSummary, signal);
    const menu = frame.locator("[data-mobile-discovery-menu]");
    await this.waitForAttribute(menu, "open", null, signal);
    await abortableDelay(2_200, signal);
    const weekendLink = frame.locator(WEEKEND_MENU_LINK_SELECTOR);
    await raceWithAbort(weekendLink.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await abortableDelay(600, signal);
    await this.tapMobileLocator(frame, weekendLink, signal);
    await this.waitForEmbeddedPath("/vyhodnye/", signal);
    await this.waitForEmbeddedReady(frame, signal);
    await abortableDelay(PACING.routeDwellMs, signal);

    const weekendRoot = frame.locator(WEEKEND_ROOT_SELECTOR);
    await raceWithAbort(weekendRoot.waitFor({ state: "attached", timeout: 10_000 }), signal);
    const marker = Number(await weekendRoot.getAttribute("data-amber-artifact-event-id"));
    assertCondition(Number.isSafeInteger(marker) && marker > 0, "weekend artifact event marker is invalid");
    assertCondition(
      marker === contract.snapshotEventId,
      `weekend artifact snapshot drift: expected ${contract.snapshotEventId}, DOM marker=${marker}`,
    );
    const row = frame.locator(
      `[data-mobile-v23-page="weekend"] [data-mobile-listing-row][data-event-id="${marker}"]`,
    );
    const artifact = row.locator(ARTIFACT_SELECTOR);
    await raceWithAbort(row.waitFor({ state: "attached", timeout: 10_000 }), signal);
    await this.setAgentState("running", "03.3 · событие найдено, подводим карточку естественным скроллом");
    await this.naturalVerticalScroll(frame, row, signal);
    await this.setAgentState("running", "03.3 · сдвигаем rail карточки к артефакту");
    await this.revealRailLocatorWithRealDrags(frame, row.locator(".rail-window"), artifact, signal);
    await this.setAgentState("running", "03.3 · артефакт найден и виден в rail");
    await this.dwellOnAudienceTarget(artifact, signal, "АРТЕФАКТ НАЙДЕН", 3_200);
    await this.setAgentState("running", "03.3 · собираем найденный артефакт");
    await this.armArtifactEventProbe(frame);
    const beforeUrl = await this.embeddedUrl();
    await abortableDelay(PACING.routeDwellMs, signal);
    await this.tapMobileLocator(frame, artifact, signal);
    await this.waitForAttribute(artifact, "aria-pressed", "true", signal);
    const firstUrl = await this.embeddedUrl();
    assertCondition(firstUrl === beforeUrl, `first artifact tap changed URL: ${beforeUrl} → ${firstUrl}`);
    await this.assertArtifactCollected(frame, artifact, marker);
    log("artifact collected and held for the audience", { eventId: marker });
    await this.setAgentState("running", "03.3 · артефакт собран, показываем результат");
    await this.dwellOnAudienceTarget(artifact, signal, "АРТЕФАКТ СОБРАН", 2_800);

    await this.setAgentState("running", "03.3 · проверяем сохранение после перезагрузки");
    await this.reloadEmbeddedFrame(signal);
    await this.waitForEmbeddedPath("/vyhodnye/", signal);
    await this.waitForEmbeddedReady(frame, signal);
    const reloadedRoot = frame.locator(WEEKEND_ROOT_SELECTOR);
    const reloadedMarker = Number(
      await reloadedRoot.getAttribute("data-amber-artifact-event-id"),
    );
    assertCondition(reloadedMarker === marker, `artifact event marker changed after reload: ${marker} → ${reloadedMarker}`);
    const reloadedRow = frame.locator(
      `[data-mobile-v23-page="weekend"] [data-mobile-listing-row][data-event-id="${marker}"]`,
    );
    const reloadedArtifact = reloadedRow.locator(ARTIFACT_SELECTOR);
    await this.naturalVerticalScroll(frame, reloadedRow, signal);
    await this.revealRailLocatorWithRealDrags(
      frame,
      reloadedRow.locator(".rail-window"),
      reloadedArtifact,
      signal,
    );
    await abortableDelay(PACING.routeDwellMs, signal);
    await this.tapMobileLocator(frame, reloadedArtifact, signal);
    await this.waitForEmbeddedPath("/artefakty/#amber_cosmonaut", signal);
    await this.waitForEmbeddedReady(frame, signal);
    const dialog = frame.locator("[data-artifact-dialog]");
    await raceWithAbort(dialog.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.setAgentState("running", "03.3 · коллекция открыта, найден 1 артефакт");
    const foundCount = (await frame.locator("[data-artifact-found-count]").textContent())?.trim();
    assertCondition(foundCount === "1", `artifact collection found count is ${JSON.stringify(foundCount)}`);
    assertCondition(
      (await frame.locator('[data-artifact-state="found"]').count()) === 1,
      "artifact collection must contain exactly one found slot",
    );
    await this.dwellOnAudienceTarget(dialog, signal, "КОЛЛЕКЦИЯ · НАЙДЕНО 1", 4_500);

    await this.finishTypicalPacing(startedAt, signal);
    await this.captureScenario(contract.id);
    return {
      summary:
        `artifact amber_cosmonaut on DOM-selected event ${marker} collected once; ` +
        "storage/event/aria verified; reload and detail dialog count=1 verified",
    };
  }

  async selectTomorrowEvent(frame, signal) {
    const rows = frame.locator(TOMORROW_ROWS_SELECTOR);
    await raceWithAbort(rows.first().waitFor({ state: "visible", timeout: 10_000 }), signal);
    const candidates = await rows.evaluateAll((nodes) =>
      nodes
        .filter((node) => {
          const style = getComputedStyle(node);
          return !node.hidden && style.display !== "none" && style.visibility !== "hidden";
        })
        .map((node) => ({
          eventId: node.dataset.eventId,
          title: node.dataset.eventTitle,
          galleryCount: node.dataset.mobileRailGalleryCount,
        })),
    );
    const selected = selectDeterministicMobileEvent(candidates);
    if (!selected) {
      throw new Error(
        `${TOMORROW_ROWS_SELECTOR} has no deterministic event candidate with id/title/gallery count`,
      );
    }
    return selected;
  }

  async waitForEmbeddedReady(frame, signal) {
    const html = frame.locator("html");
    await raceWithAbort(html.waitFor({ state: "attached", timeout: 10_000 }), signal);
    await raceWithAbort(
      html.evaluate(async () => {
        if (document.readyState !== "complete") {
          await new Promise((resolve) => addEventListener("load", resolve, { once: true }));
        }
        await document.fonts?.ready;
      }),
      signal,
    );
    const mobileSurface = frame.locator("[data-mobile-listing-rails]");
    if ((await mobileSurface.count()) > 0) {
      await this.waitForAttribute(mobileSurface.first(), "data-mobile-v23-ready", "true", signal);
    }
    await this.waitForVisibleMediaSettled(frame, signal);
    try {
      await this.waitForScrollSettle(frame.locator("html"), signal, "vertical");
    } catch (error) {
      if (signal.aborted) throw error;
      log("initial vertical scroll did not settle; continuing with the scenario target gate", {
        error: errorText(error),
      });
    }
  }

  async waitForVisibleMediaSettled(frame, signal) {
    const deadline = Date.now() + 12_000;
    let lastState = null;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      const state = await frame.locator("body").evaluate(() => {
        const visible = (node) => {
          const rect = node.getBoundingClientRect();
          const style = getComputedStyle(node);
          return (
            rect.width > 0 &&
            rect.height > 0 &&
            rect.bottom > 0 &&
            rect.top < innerHeight &&
            rect.right > 0 &&
            rect.left < innerWidth &&
            style.display !== "none" &&
            style.visibility !== "hidden"
          );
        };
        const images = [...document.images].filter(visible);
        const videos = [...document.querySelectorAll("video")].filter(visible);
        return {
          pendingImages: images.filter((image) => !image.complete).length,
          pendingVideos: videos.filter((video) => video.readyState < 2).length,
          pendingMediaStates: [...document.querySelectorAll('[data-media-state="loading"]')]
            .filter(visible).length,
        };
      });
      lastState = state;
      if (!state.pendingImages && !state.pendingVideos && !state.pendingMediaStates) return;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    log("visible media readiness deadline reached; continuing with scenario-specific gates", {
      deadlineMs: 12_000,
      ...lastState,
    });
  }

  async waitForEmbeddedPath(expected, signal) {
    const deadline = Date.now() + 10_000;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      const url = new URL(await this.embeddedUrl());
      const routePath = this.normalizedPreviewPath(url.pathname);
      const value = `${routePath}${url.hash}`;
      if (
        (expected instanceof RegExp && expected.test(routePath)) ||
        (typeof expected === "string" && value === expected)
      ) {
        await abortableDelay(PACING.routeDwellMs, signal);
        return;
      }
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error(`embedded route did not reach ${String(expected)}; current=${await this.embeddedUrl()}`);
  }

  async embeddedUrl() {
    return this.embeddedFrame().url();
  }

  normalizedPreviewPath(pathname) {
    const focusPrefix = new URL(FOCUS_PREVIEW_BASE_URL).pathname.replace(/\/$/u, "");
    const artifactPrefix = new URL(ARTIFACT_PREVIEW_BASE_URL).pathname.replace(/\/$/u, "");
    for (const prefix of [focusPrefix, artifactPrefix]) {
      if (pathname === prefix) return "/";
      if (pathname.startsWith(`${prefix}/`)) return pathname.slice(prefix.length);
    }
    return pathname;
  }

  embeddedFrame() {
    const main = this.page.mainFrame();
    const frame = this.page.frames().find((candidate) => candidate.parentFrame() === main);
    if (!frame) throw new Error("embedded presenter frame is unavailable");
    return frame;
  }

  async reloadEmbeddedFrame(signal) {
    const embedded = this.embeddedFrame();
    await raceWithAbort(
      embedded.goto(embedded.url(), { waitUntil: "domcontentloaded", timeout: 10_000 }),
      signal,
    );
  }

  async naturalVerticalScroll(
    frame,
    locator,
    signal,
    iframeSelector = FRAME_SELECTOR,
  ) {
    assertNotAborted(signal);
    await raceWithAbort(locator.waitFor({ state: "attached", timeout: 10_000 }), signal);
    const geometry = await locator.evaluate((node) => {
      const rect = node.getBoundingClientRect();
      const desiredTop = Math.max(
        92,
        Math.min(innerHeight * 0.28, innerHeight - Math.min(rect.height, innerHeight * 0.6) - 132),
      );
      return {
        top: rect.top,
        bottom: rect.bottom,
        height: rect.height,
        viewportHeight: innerHeight,
        desiredTop,
        deltaY: rect.top - desiredTop,
        visibleRatio: rect.height
          ? Math.max(0, Math.min(rect.bottom, innerHeight) - Math.max(rect.top, 0))
            / Math.min(rect.height, innerHeight)
          : 0,
      };
    });

    if (Math.abs(geometry.deltaY) > 4 && geometry.visibleRatio < .9) {
      const iframeBox = await this.page.locator(iframeSelector).boundingBox();
      if (!iframeBox) throw new Error("presenter iframe has no boundingBox for wheel gesture");
      await this.page.mouse.move(
        Math.round(iframeBox.x + iframeBox.width / 2),
        Math.round(iframeBox.y + iframeBox.height / 2),
      );
      const trajectory = buildVerticalWheelTrajectory(geometry.deltaY);
      let observedIntermediate = false;
      const initialY = await frame.locator("html").evaluate(() => scrollY);
      for (let index = 0; index < trajectory.length; index += 1) {
        const step = trajectory[index];
        await this.page.mouse.wheel(0, step.deltaY);
        await abortableDelay(step.delayMs, signal);
        if (index < trajectory.length - 1) {
          const currentY = await frame.locator("html").evaluate(() => scrollY);
          if (Math.abs(currentY - initialY) > 1) observedIntermediate = true;
        }
      }
      assertCondition(observedIntermediate, "vertical wheel trajectory had no observable intermediate movement");
      await this.waitForScrollSettle(frame.locator("html"), signal, "vertical");
    }

    const correction = await locator.evaluate((node) => {
      const rect = node.getBoundingClientRect();
      const desiredTop = Math.max(
        92,
        Math.min(innerHeight * 0.28, innerHeight - Math.min(rect.height, innerHeight * 0.6) - 132),
      );
      const visible = Math.max(0, Math.min(rect.bottom, innerHeight) - Math.max(rect.top, 0));
      return {
        deltaY: rect.top - desiredTop,
        visibleRatio: rect.height ? visible / Math.min(rect.height, innerHeight) : 0,
      };
    });
    if (correction.visibleRatio < 0.72) {
      assertCondition(
        Math.abs(correction.deltaY) <= PACING.verticalFinalCorrectionPx,
        `natural scroll requires an oversized final correction: ${JSON.stringify(correction)}`,
      );
      await raceWithAbort(locator.scrollIntoViewIfNeeded({ timeout: 3_000 }), signal);
      await this.waitForScrollSettle(frame.locator("html"), signal, "vertical");
    }
    const visible = await locator.evaluate((node) => {
      const rect = node.getBoundingClientRect();
      const intersection = Math.max(0, Math.min(rect.bottom, innerHeight) - Math.max(rect.top, 0));
      return intersection >= Math.min(rect.height, innerHeight) * 0.72;
    });
    assertCondition(visible, "natural vertical scroll did not leave target visibly framed");
  }

  async waitForScrollSettle(locator, signal, axis) {
    const startedAt = Date.now();
    let stable = 0;
    let previous = null;
    while (Date.now() - startedAt <= PACING.settleMaxMs) {
      assertNotAborted(signal);
      const current = await locator.evaluate((node, requestedAxis) => {
        if (requestedAxis === "horizontal") return Number(node.scrollLeft || 0);
        return Number(document.scrollingElement?.scrollTop || scrollY || 0);
      }, axis);
      if (previous !== null && Math.abs(current - previous) <= 0.5) stable += 1;
      else stable = 0;
      if (stable >= PACING.settleStableSamples) return current;
      previous = current;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error(`${axis} scroll did not settle within ${PACING.settleMaxMs}ms`);
  }

  async waitForAttribute(locator, name, expected, signal) {
    const deadline = Date.now() + 5_000;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      const value = await locator.getAttribute(name);
      if ((expected === null && value !== null) || value === expected) return value;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error(`${name} did not become ${expected === null ? "present" : JSON.stringify(expected)}`);
  }

  async tapMobileLocator(frame, locator, signal, { force = false, dispatch = false } = {}) {
    assertNotAborted(signal);
    const box = await locator.boundingBox();
    if (!box) throw new Error("mobile tap target has no boundingBox; reveal it before tapping");
    const point = await locator.evaluate((node) => {
      const rect = node.getBoundingClientRect();
      return {
        x: Math.round(rect.x + rect.width / 2),
        y: Math.round(rect.y + rect.height / 2),
      };
    });
    await frame.locator("body").evaluate((body, destination) => {
      document.querySelectorAll("[data-autopresenter-tap]").forEach((node) => node.remove());
      const tap = document.createElement("div");
      tap.dataset.autopresenterTap = "true";
      tap.setAttribute("aria-hidden", "true");
      Object.assign(tap.style, {
        position: "fixed",
        zIndex: "2147483647",
        width: "28px",
        height: "28px",
        left: `${destination.x - 14}px`,
        top: `${destination.y - 14}px`,
        border: "4px solid rgba(38,211,154,.98)",
        borderRadius: "999px",
        background: "rgba(38,211,154,.24)",
        boxShadow: "0 0 0 8px rgba(255,255,255,.72)",
        pointerEvents: "none",
      });
      body.append(tap);
      const animation = tap.animate(
        [
          { transform: "scale(.45)", opacity: 0 },
          { transform: "scale(1)", opacity: 1, offset: 0.22 },
          { transform: "scale(2.8)", opacity: 0 },
        ],
        { duration: 760, easing: "cubic-bezier(.22,.8,.24,1)", fill: "forwards" },
      );
      animation.finished.finally(() => tap.remove());
    }, point);
    await abortableDelay(PACING.tapLeadMs, signal);
    if (dispatch) await raceWithAbort(locator.dispatchEvent("click"), signal);
    else await raceWithAbort(locator.click({ timeout: 5_000, force }), signal);
  }

  async showSwipeCue(rail, label, direction = "left") {
    await rail.evaluate((node, cue) => {
      document.querySelectorAll("[data-autopresenter-swipe-trail]").forEach((item) => item.remove());
      const rect = node.getBoundingClientRect();
      const trail = document.createElement("div");
      trail.dataset.autopresenterSwipeTrail = "true";
      trail.dataset.autopresenterSwipeFingerDirection = cue.direction;
      trail.dataset.autopresenterSwipeContentDirection = cue.direction === "left" ? "right" : "left";
      trail.setAttribute("aria-hidden", "true");
      Object.assign(trail.style, {
        position: "fixed",
        zIndex: "2147483647",
        left: `${Math.max(10, rect.left + rect.width * .12)}px`,
        top: `${Math.max(70, rect.top + rect.height * .35)}px`,
        width: `${Math.max(210, rect.width * .76)}px`,
        minHeight: "48px",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        borderRadius: "999px",
        background: "rgba(17,24,39,.82)",
        boxShadow: "0 10px 32px rgba(0,0,0,.28)",
        color: "#fff",
        font: "850 13px/1 Inter,system-ui,sans-serif",
        pointerEvents: "none",
      });
      trail.innerHTML =
        `<span style="color:#7de6c2;font-size:23px;margin-right:9px">${cue.direction === "left" ? "←━━━━━━━━" : "━━━━━━━━→"}</span>` +
        `<span>${cue.label}</span>`;
      document.body.append(trail);
      const animation = trail.animate(
        [
          { transform: "translateX(-16px)", opacity: 0 },
          { transform: "translateX(0)", opacity: 1, offset: .18 },
          { transform: "translateX(16px)", opacity: 1 },
        ],
        { duration: 1_650, easing: "cubic-bezier(.83,0,.17,1)", fill: "forwards" },
      );
      animation.finished.finally(() => trail.remove());
    }, { label, direction });
  }

  async swipeRailLeft(frame, rail, signal, label) {
    assertNotAborted(signal);
    const box = await rail.boundingBox();
    if (!box) throw new Error(`${MOBILE_EVENT_RAIL_SELECTOR} has no boundingBox`);
    const start = {
      x: Math.round(box.x + box.width * 0.86),
      y: Math.round(box.y + box.height * 0.52),
    };
    const end = {
      x: Math.round(box.x + box.width * 0.10),
      y: start.y,
    };
    await this.showSwipeCue(rail, label, "left");
    await this.performMouseDrag(start, end, signal);
  }

  async performMouseDrag(start, end, signal, { beforeMouseUp } = {}) {
    let pointerDown = false;
    try {
      await this.page.mouse.move(start.x, start.y);
      await this.page.mouse.down();
      pointerDown = true;
      for (let step = 1; step <= PACING.railSteps; step += 1) {
        assertNotAborted(signal);
        const progress = step / PACING.railSteps;
        const eased = progress * progress * progress * (progress * (progress * 6 - 15) + 10);
        await this.page.mouse.move(
          Math.round(start.x + (end.x - start.x) * eased),
          Math.round(start.y + (end.y - start.y) * eased),
        );
        await abortableDelay(PACING.railStepMs, signal);
      }
      if (beforeMouseUp) await beforeMouseUp();
      await this.page.mouse.up();
      pointerDown = false;
    } finally {
      if (pointerDown) await this.page.mouse.up().catch(() => {});
    }
  }

  async dragRailToEndInOneRelease(frame, rail, signal) {
    const geometry = await this.railGeometry(rail);
    assertCondition(geometry.maxScroll > 0, "like rail has no horizontal overflow");
    const box = await rail.boundingBox();
    if (!box) throw new Error("like rail has no boundingBox");
    const start = {
      x: Math.round(box.x + box.width * .88),
      y: Math.round(box.y + box.height * .52),
    };
    const required = Math.ceil(geometry.maxScroll - geometry.scrollLeft + 36);
    const minimumX = 20;
    assertCondition(
      required <= start.x - minimumX,
      `rail maxScroll cannot be reached by one visible bounded drag: required=${required} available=${start.x - minimumX}`,
    );
    const end = { x: Math.max(minimumX, start.x - required), y: start.y };
    await this.showSwipeCue(rail, "Тянем ряд до самого края", "left");
    await this.performMouseDrag(start, end, signal);
    await this.waitForScrollSettle(rail, signal, "horizontal");
  }

  async pullLikeEdgeAndAssertArmed(frame, rail, signal) {
    const geometry = await this.railGeometry(rail);
    assertCondition(geometry.maxScroll - geometry.scrollLeft <= 1, "like edge pull started before maxScroll");
    const box = await rail.boundingBox();
    if (!box) throw new Error("like rail has no boundingBox at edge");
    const start = {
      x: Math.round(box.x + box.width * .82),
      y: Math.round(box.y + box.height * .52),
    };
    const pull = Math.max(132, Math.ceil(box.width * .38));
    const end = { x: start.x - pull, y: start.y };
    await this.showSwipeCue(rail, "Ещё движение — поставить лайк", "left");
    await this.performMouseDrag(start, end, signal, {
      beforeMouseUp: async () => {
        const armed = await rail.evaluate((node) => node.classList.contains("is-like-armed"));
        assertCondition(armed, `rail did not arm like before mouseup after ${pull}px edge pull`);
      },
    });
    await this.waitForScrollSettle(rail, signal, "horizontal");
  }

  async revealRailLocatorWithRealDrags(frame, rail, target, signal) {
    await raceWithAbort(rail.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await raceWithAbort(target.waitFor({ state: "attached", timeout: 10_000 }), signal);
    let previous = -1;
    for (let attempt = 0; attempt < 7; attempt += 1) {
      if (attempt > 0 && await this.isHorizontallyRevealed(target)) return;
      const geometry = await this.railGeometry(rail);
      assertCondition(geometry.maxScroll > 0, `artifact rail has no horizontal movement: ${JSON.stringify(geometry)}`);
      assertCondition(
        geometry.scrollLeft > previous + .5 || previous < 0,
        `rail drag made no progress: ${JSON.stringify(geometry)}`,
      );
      previous = geometry.scrollLeft;
      await this.swipeRailLeft(frame, rail, signal, attempt === 0 ? "Сдвигаем карточку — ищем артефакт" : "Ищем находку у края");
      await this.waitForScrollSettle(rail, signal, "horizontal");
      await abortableDelay(1_100, signal);
    }
    throw new Error(`artifact did not become visible through real rail drags: ${JSON.stringify(await this.railGeometry(rail))}`);
  }

  async railGeometry(rail) {
    return rail.evaluate((node) => ({
      scrollLeft: node.scrollLeft,
      maxScroll: Math.max(0, node.scrollWidth - node.clientWidth),
      clientWidth: node.clientWidth,
      scrollWidth: node.scrollWidth,
    }));
  }

  async isHorizontallyRevealed(locator) {
    return locator.evaluate((node) => {
      const rail = node.closest(".rail-window");
      if (!rail) return false;
      const nodeRect = node.getBoundingClientRect();
      const railRect = rail.getBoundingClientRect();
      const intersection = Math.max(
        0,
        Math.min(nodeRect.right, railRect.right) - Math.max(nodeRect.left, railRect.left),
      );
      return intersection >= Math.min(nodeRect.width, railRect.width) * 0.72;
    });
  }

  async dwellOnDescription(locator, signal, surface, dwellMs) {
    await locator.evaluate((node, dwellSurface) => {
      node.dataset.autopresenterDescriptionDwell = dwellSurface;
      Object.assign(node.style, {
        outline: "4px solid rgba(38,211,154,.94)",
        outlineOffset: "5px",
        borderRadius: "10px",
        boxShadow: "0 0 0 10px rgba(38,211,154,.16)",
      });
    }, surface);
    await abortableDelay(dwellMs, signal);
  }

  async dwellOnAudienceTarget(locator, signal, label, dwellMs) {
    await locator.evaluate((node, audienceLabel) => {
      document.querySelectorAll("[data-autopresenter-audience-label]")
        .forEach((item) => item.remove());
      node.dataset.autopresenterAudienceTarget = audienceLabel;
      Object.assign(node.style, {
        outline: "4px solid rgba(38,211,154,.98)",
        outlineOffset: "5px",
        borderRadius: "12px",
        boxShadow: "0 0 0 10px rgba(38,211,154,.2)",
      });
      const rect = node.getBoundingClientRect();
      const tag = document.createElement("div");
      tag.dataset.autopresenterAudienceLabel = "true";
      tag.textContent = audienceLabel;
      Object.assign(tag.style, {
        position: "fixed",
        zIndex: "2147483647",
        left: `${Math.max(12, Math.min(innerWidth - 280, rect.left + 12))}px`,
        top: `${Math.max(12, rect.top - 54)}px`,
        padding: "12px 18px",
        borderRadius: "999px",
        color: "#08130f",
        background: "#7de6c2",
        boxShadow: "0 14px 36px rgba(0,0,0,.28)",
        font: "900 14px/1 Inter,system-ui,sans-serif",
        letterSpacing: ".04em",
        pointerEvents: "none",
      });
      document.body.append(tag);
    }, label);
    await abortableDelay(dwellMs, signal);
    await locator.evaluate((node) => {
      delete node.dataset.autopresenterAudienceTarget;
      node.style.outline = "";
      node.style.outlineOffset = "";
      node.style.borderRadius = "";
      node.style.boxShadow = "";
      document.querySelectorAll("[data-autopresenter-audience-label]")
        .forEach((item) => item.remove());
    }).catch(() => {});
  }

  async assertEventNotPreLiked(frame, row, eventId) {
    const state = await frame.locator("body").evaluate(
      (body, { profileKey, id }) => {
        let profile = null;
        try {
          profile = JSON.parse(localStorage.getItem(profileKey) || "null");
        } catch {}
        return {
          storageLiked: Array.isArray(profile?.liked_event_ids)
            && profile.liked_event_ids.some((value) => Number(value) === id),
          rowLiked: body.querySelector(`[data-mobile-listing-row][data-event-id="${id}"]`)
            ?.classList.contains("is-liked") || false,
        };
      },
      { profileKey: PROFILE_STORAGE_KEY, id: eventId },
    );
    const pressed = await row.locator('[data-feedback-action="like"]').getAttribute("aria-pressed");
    assertCondition(
      !state.storageLiked && !state.rowLiked && pressed !== "true",
      `event ${eventId} is already liked in the fresh scenario profile; refusing to silently unlike`,
    );
  }

  async waitForEitherPressedOrConsent(like, consent, signal) {
    const deadline = Date.now() + 2_500;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      if ((await like.getAttribute("aria-pressed")) === "true") return false;
      if ((await consent.count()) > 0 && await consent.isVisible()) return true;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error("like gesture produced neither a pressed control nor visible consent");
  }

  async waitForLikeState(row, like, expected, signal) {
    const deadline = Date.now() + 5_000;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      const pressed = (await like.getAttribute("aria-pressed")) === "true";
      const rowLiked = await row.evaluate((node) => node.classList.contains("is-liked"));
      if (pressed === expected && rowLiked === expected) return;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error(`like aria/row state did not become ${expected}`);
  }

  async readLikeCount(like) {
    const text = (await like.locator("[data-like-count]").textContent())?.trim() || "";
    const base = Number(await like.getAttribute("data-base-count"));
    const parsed = Number(text);
    return Number.isFinite(parsed) ? parsed : (Number.isFinite(base) ? base : 0);
  }

  async waitForStableLikeCount(like, minimum, signal) {
    const startedAt = Date.now();
    let previous = null;
    let stable = 0;
    while (Date.now() - startedAt <= 5_000) {
      assertNotAborted(signal);
      const current = await this.readLikeCount(like);
      if (current >= minimum && current === previous) stable += 1;
      else stable = 0;
      if (stable >= PACING.settleStableSamples) return current;
      previous = current;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error(`like count did not stabilize at or above ${minimum}`);
  }

  async assertLikePersistenceStorage(frame, eventId) {
    const state = await frame.locator("body").evaluate(
      (body, { profileKey, logKey, id }) => {
        const parse = (key) => {
          try { return JSON.parse(localStorage.getItem(key) || "null"); }
          catch { return null; }
        };
        const profile = parse(profileKey);
        const log = parse(logKey);
        return {
          liked: Array.isArray(profile?.liked_event_ids)
            && profile.liked_event_ids.some((value) => Number(value) === id),
          normalizedLog: Array.isArray(log)
            && log.some((entry) => Number(entry?.event_id) === id && entry?.action === "like_event"),
          legacyEventIdKey: Array.isArray(log)
            && log.some((entry) => entry && Object.hasOwn(entry, "eventId")),
          bodyPresent: Boolean(body),
        };
      },
      { profileKey: PROFILE_STORAGE_KEY, logKey: FEEDBACK_LOG_STORAGE_KEY, id: eventId },
    );
    assertCondition(state.bodyPresent && state.liked, `profile storage does not contain liked event ${eventId}`);
    assertCondition(state.normalizedLog, `feedback log has no normalized event_id=${eventId} like_event entry`);
    assertCondition(!state.legacyEventIdKey, "feedback log contains non-normalized eventId keys");
  }

  async isLikePersistenceStored(frame, eventId) {
    try {
      await this.assertLikePersistenceStorage(frame, eventId);
      return true;
    } catch {
      return false;
    }
  }

  async waitForConsentProfile(frame, signal) {
    const deadline = Date.now() + 5_000;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      const consented = await frame.locator("body").evaluate((body, profileKey) => {
        try {
          const profile = JSON.parse(localStorage.getItem(profileKey) || "null");
          return Boolean(body && profile?.consent_ok === true);
        } catch {
          return false;
        }
      }, PROFILE_STORAGE_KEY);
      if (consented) return;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error("consent action did not create the local presentation profile");
  }

  async waitForLikePersistenceStorage(frame, eventId, signal) {
    const deadline = Date.now() + 10_000;
    let lastError = null;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      try {
        await this.assertLikePersistenceStorage(frame, eventId);
        return;
      } catch (error) {
        lastError = error;
      }
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw lastError || new Error(`like persistence did not settle for event ${eventId}`);
  }

  async armArtifactEventProbe(frame) {
    await frame.locator("body").evaluate(() => {
      window.__autopresenterAmberEvent = null;
      addEventListener(
        "kenigevents:artifact-collected",
        (event) => {
          window.__autopresenterAmberEvent = event.detail;
        },
        { once: true },
      );
    });
  }

  async assertArtifactCollected(frame, artifact, eventId) {
    const state = await frame.locator("body").evaluate(
      (body, { storageKey, expectedEventId }) => {
        let collection = null;
        try {
          collection = JSON.parse(localStorage.getItem(storageKey) || "null");
        } catch {}
        const found = collection?.artifacts?.amber_cosmonaut;
        return {
          bodyPresent: Boolean(body),
          collectionId: collection?.collectionId,
          status: found?.status,
          eventId: Number(found?.eventId),
          placement: found?.placement,
          event: window.__autopresenterAmberEvent,
          expectedEventId,
        };
      },
      { storageKey: ARTIFACT_STORAGE_KEY, expectedEventId: eventId },
    );
    const aria = await artifact.getAttribute("aria-label");
    assertCondition((await artifact.getAttribute("aria-pressed")) === "true", "artifact aria-pressed is not true");
    assertCondition(/найден/iu.test(aria || ""), `artifact found aria label is invalid: ${JSON.stringify(aria)}`);
    assertCondition(state.bodyPresent && state.status === "found", "artifact storage status is not found");
    assertCondition(state.eventId === eventId, `artifact storage eventId ${state.eventId} != ${eventId}`);
    assertCondition(
      state.event?.artifactId === "amber_cosmonaut"
        && Number(state.event?.eventId) === eventId
        && state.event?.placement === state.placement,
      `artifact custom event mismatch: ${JSON.stringify(state.event)}`,
    );
  }

  async finishTypicalPacing(startedAt, signal) {
    const remaining = PACING.scenarioTypicalMinMs - (Date.now() - startedAt);
    if (remaining > 0) await abortableDelay(remaining, signal);
    assertNotAborted(signal);
  }

  cleanTitle(title) {
    return title.replace(/\s+/gu, " ").replaceAll('"', "'").trim().slice(0, 100);
  }

  async captureScenario(scenarioId) {
    if (!config.artifactDir) return;
    await this.page.screenshot({
      path: path.join(config.artifactDir, `${scenarioId}-1920x1080.png`),
      fullPage: false,
    });
  }

  async setAgentState(status, detail) {
    this.agentStatus = status;
    this.statusDetail = detail;
    if (this.page && !this.page.isClosed()) {
      await this.page
        .evaluate(
          ({ nextStatus, nextDetail }) => {
            window.dispatchEvent(
              new CustomEvent("presenter:status", {
                detail: { status: nextStatus, detail: nextDetail },
              }),
            );
          },
          { nextStatus: status, nextDetail: detail },
        )
        .catch(() => {});
    }
    await this.publishState().catch((error) => log("state publish failed", errorText(error)));
  }

  async publishState() {
    return this.request("/api/state/agent", {
      method: "POST",
      body: {
        agent_id: config.agentId,
        status: this.agentStatus,
        detail: this.statusDetail,
      },
    });
  }

  async ack(command, status, detail) {
    this.ackCache.set(command.id, { status, detail });
    return this.request(`/api/commands/${encodeURIComponent(command.id)}/ack`, {
      method: "POST",
      body: {
        agent_id: config.agentId,
        sequence: command.sequence,
        status,
        detail,
      },
    });
  }

  async request(route, { method = "GET", body } = {}) {
    const response = await fetch(`${config.relayUrl}${route}`, {
      method,
      headers: {
        ...this.authHeaders(),
        ...(body ? { "content-type": "application/json" } : {}),
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`${method} ${route} HTTP ${response.status}: ${text.slice(0, 300)}`);
    }
    return response.json();
  }

  authHeaders() {
    return config.agentToken
      ? { authorization: `Bearer ${config.agentToken}` }
      : {};
  }

  shutdown(signalName = "shutdown") {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shuttingDown = true;
    this.shutdownPromise = (async () => {
      log("shutting down", { signal: signalName });
      clearInterval(this.heartbeatTimer);
      this.pollAbort?.abort();
      this.runController?.abort();
      if (this.activeRun) {
        await Promise.race([
          this.activeRun.catch(() => {}),
          abortableDelay(config.hardStopMs).catch(() => {}),
        ]);
      }
      if (config.storageStatePath && this.context) {
        await mkdir(path.dirname(config.storageStatePath), { recursive: true }).catch(() => {});
        await this.context
          .storageState({ path: config.storageStatePath })
          .catch((error) => log("browser state persistence failed", errorText(error)));
      }
      await this.context?.close().catch(() => {});
      await this.browser?.close().catch(() => {});
    })();
    return this.shutdownPromise;
  }
}

async function main() {
  const { chromium } = loadPlaywright();
  const agent = new PrototypeAgent(chromium);
  let signalShutdown = null;
  for (const signalName of ["SIGINT", "SIGTERM"]) {
    process.once(signalName, () => {
      signalShutdown = agent.shutdown(signalName);
    });
  }

  try {
    await agent.start();
  } finally {
    await (signalShutdown || agent.shutdown("main-finally"));
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`[autopresenter-agent] fatal: ${error?.stack || errorText(error)}\n`);
    process.exitCode = 1;
  });
}

export { PrototypeAgent };
