#!/usr/bin/env node

import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { mkdir } from "node:fs/promises";
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
  isStaticPresentationScenario,
  resolveScenarioId,
  resolveScenarioTimeoutMs,
  selectDeterministicMobileEvent,
} from "./scenario-contract.mjs";
import {
  INTRO_LOOP_RUNTIME_MS,
  INTRO_MUSIC_ASSET,
  INTRO_SCENE_ID,
  FOCUS_PREVIEW_BASE_URL,
  FOCUS_INVITATION_SCENE_ID,
  FOCUS_INVITATION_URL,
  LECTURE_SCENES,
  WEEKEND_DESKTOP_SCENE_ID,
  ZNANIE_LOGO_ASSET,
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
const WEEKEND_DESKTOP_ROOT_SELECTOR = '[data-date-listing="weekend"]';
const SITE_FOOTER_SELECTOR = '[data-site-footer]';
const TOMORROW_NAV_SELECTOR = '[data-presenter-id="nav-tomorrow"]';
const TOMORROW_READY_SELECTOR = '[data-presenter-id="tomorrow-page-ready"]';
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
    if (!command || !["run", "stop", "reset", "shutdown"].includes(command.action)) return;

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
    if (command.action === "shutdown") {
      await this.handleShutdown(command, remote);
      return;
    }
    await this.handleReset(command, remote);
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
    if (scenarioId === WEEKEND_DESKTOP_CONTRACT.id) {
      return this.runWeekendDesktop(signal);
    }
    if (scenarioId === OUTRO_QR_CONTRACT.id) {
      return this.runOutroQr(signal);
    }
    throw new Error(`unreachable scenario dispatch: ${scenarioId}`);
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
      await this.assertStageAssetLoaded(
        `${selector} .brand-plate--lecture img`,
        ZNANIE_LOGO_ASSET.url,
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
    await abortableDelay(900, signal);
    await this.captureScenario(scenarioId);
    return {
      summary: lecture
        ? `source-backed lecture frame ${scenarioId} is visible and held until the next command`
        : `presentation frame ${scenarioId} is visible and held until the next command`,
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
    assertCondition(tokenCount >= 2, `${mode} example exposes only ${tokenCount} medallion`);
    await abortableDelay(1_400, signal);
    await this.captureScenario(scenarioId);
    return {
      summary: `real focus-preview event page shows medallions in ${mode} composition`,
      durationMs: Date.now() - startedAt,
    };
  }

  async runFocusInvitation(signal) {
    const startedAt = Date.now();
    await this.setInteractionMode("stage");
    await this.showPresenterScene(FOCUS_INVITATION_SCENE_ID, signal);
    const selector = '[data-presenter-id="focus-invitation-frame"]';
    await this.resetFocusFrame(selector, signal);
    const frame = await this.focusFrame(selector, signal);
    const expectedUrl = new URL(FOCUS_INVITATION_URL);
    const actualUrl = new URL(frame.url());
    assertCondition(
      actualUrl.origin === expectedUrl.origin && actualUrl.pathname === expectedUrl.pathname,
      `focus invitation opened unexpected URL ${frame.url()}`,
    );
    const body = frame.locator("body");
    await raceWithAbort(body.waitFor({ state: "visible", timeout: 30_000 }), signal);
    assertCondition(
      (await body.innerText()).includes("ПРИГЛАШЕНИЕ ПРИНЯТО"),
      "focus invitation acceptance is not visible",
    );
    const participation = await frame.evaluate(() => {
      try {
        return JSON.parse(localStorage.getItem("kenigevents:focus-participation:v1") || "null");
      } catch {
        return null;
      }
    });
    assertCondition(
      participation?.source === "invite_fragment" && participation?.status === "joining",
      "focus invitation fragment was not accepted into the participation state",
    );
    await abortableDelay(1_200, signal);
    await this.captureScenario(FOCUS_INVITATION_SCENE_ID);
    return {
      summary: "focus-group invitation fragment was accepted and the invitation remains visible",
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
    const enabled = signedIn && (await submit.isEnabled().catch(() => false));
    if (enabled) await submit.click();
    await abortableDelay(2_000, signal);
    await this.captureScenario("service-search-live");
    return {
      summary: enabled
        ? `query "${query}" was typed and submitted on the focus-preview search page`
        : `query "${query}" was typed visibly; site sign-in gate left unchanged`,
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
    await this.waitForEmbeddedReady(frame, signal);
    const path = await iframe.evaluate(
      (node) => `${node.contentWindow.location.pathname}${node.contentWindow.location.hash}`,
    );
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
    const target = frame.locator(TOMORROW_NAV_SELECTOR);
    await raceWithAbort(target.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.naturalVerticalScroll(frame, target, signal);
    const boundingBox = await target.boundingBox();
    if (!boundingBox) throw new Error(`${TOMORROW_NAV_SELECTOR} has no boundingBox`);
    log("target acquired", { selector: TOMORROW_NAV_SELECTOR, boundingBox });
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

    await this.pullLikeEdgeAndAssertArmed(frame, rail, signal);
    log("like scenario edge pull released", { eventId: contract.eventId });
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
      await this.tapMobileLocator(frame, consentAccept, signal);
      log("like scenario consent accepted", { eventId: contract.eventId });
    }

    await this.waitForLikeState(row, like, true, signal);
    log("like scenario UI state pressed", { eventId: contract.eventId });
    const afterCount = await this.waitForStableLikeCount(like, beforeCount + 1, signal);
    assertCondition(
      afterCount === beforeCount + 1,
      `like count did not increment exactly once: before=${beforeCount} after=${afterCount}`,
    );
    await this.assertLikePersistenceStorage(frame, contract.eventId);

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
    assertCondition(
      (await this.readLikeCount(reloadedLike)) === afterCount,
      `persisted like count changed after reload for event ${contract.eventId}`,
    );
    await this.assertLikePersistenceStorage(frame, contract.eventId);

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

    const menuSummary = frame.locator(WEEKEND_MENU_SUMMARY_SELECTOR);
    await raceWithAbort(menuSummary.waitFor({ state: "visible", timeout: 10_000 }), signal);
    await this.tapMobileLocator(frame, menuSummary, signal);
    const menu = frame.locator("[data-mobile-discovery-menu]");
    await this.waitForAttribute(menu, "open", null, signal);
    const weekendLink = frame.locator(WEEKEND_MENU_LINK_SELECTOR);
    await raceWithAbort(weekendLink.waitFor({ state: "visible", timeout: 10_000 }), signal);
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
    await this.naturalVerticalScroll(frame, row, signal);
    await this.revealRailLocatorWithRealDrags(frame, row.locator(".rail-window"), artifact, signal);
    await this.armArtifactEventProbe(frame);
    const beforeUrl = await this.embeddedUrl();
    await abortableDelay(PACING.routeDwellMs, signal);
    await this.tapMobileLocator(frame, artifact, signal);
    await this.waitForAttribute(artifact, "aria-pressed", "true", signal);
    const firstUrl = await this.embeddedUrl();
    assertCondition(firstUrl === beforeUrl, `first artifact tap changed URL: ${beforeUrl} → ${firstUrl}`);
    await this.assertArtifactCollected(frame, artifact, marker);

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
    const foundCount = (await frame.locator("[data-artifact-found-count]").textContent())?.trim();
    assertCondition(foundCount === "1", `artifact collection found count is ${JSON.stringify(foundCount)}`);
    assertCondition(
      (await frame.locator('[data-artifact-state="found"]').count()) === 1,
      "artifact collection must contain exactly one found slot",
    );

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
    await this.waitForScrollSettle(frame.locator("html"), signal, "vertical");
  }

  async waitForVisibleMediaSettled(frame, signal) {
    const deadline = Date.now() + 3_500;
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
      if (!state.pendingImages && !state.pendingVideos && !state.pendingMediaStates) return;
      await abortableDelay(PACING.settleSampleMs, signal);
    }
    throw new Error("visible embedded media did not settle within 3500ms");
  }

  async waitForEmbeddedPath(expected, signal) {
    const deadline = Date.now() + 10_000;
    while (Date.now() <= deadline) {
      assertNotAborted(signal);
      const url = new URL(await this.embeddedUrl());
      const value = `${url.pathname}${url.hash}`;
      if (
        (expected instanceof RegExp && expected.test(url.pathname)) ||
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
    return this.page.locator(FRAME_SELECTOR).evaluate((embedded) => embedded.contentWindow.location.href);
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
      };
    });

    if (Math.abs(geometry.deltaY) > 4) {
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

  async tapMobileLocator(frame, locator, signal) {
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
    await raceWithAbort(locator.click({ timeout: 5_000 }), signal);
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
        { duration: 1_050, easing: "cubic-bezier(.22,.8,.24,1)", fill: "forwards" },
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
        await this.page.mouse.move(
          Math.round(start.x + (end.x - start.x) * progress),
          Math.round(start.y + (end.y - start.y) * progress),
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
      if (await this.isHorizontallyRevealed(target)) return;
      const geometry = await this.railGeometry(rail);
      assertCondition(
        geometry.scrollLeft > previous + .5 || previous < 0,
        `rail drag made no progress: ${JSON.stringify(geometry)}`,
      );
      previous = geometry.scrollLeft;
      await this.swipeRailLeft(frame, rail, signal, "Ищем находку у края");
      await this.waitForScrollSettle(rail, signal, "horizontal");
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
