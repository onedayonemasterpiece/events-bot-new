#!/usr/bin/env node

import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import {
  TOMORROW_MOBILE_CONTRACT,
  selectDeterministicMobileEvent,
} from "./scenario-contract.mjs";

const SCENARIO = TOMORROW_MOBILE_CONTRACT.id;
const VIEWPORT = Object.freeze({ width: 1920, height: 1080 });
const FRAME_SELECTOR = '#presenter-mobile-frame[data-presenter-id="mobile-site-frame"]';
const STAGE_READY_SELECTOR =
  '[data-presenter-id="presenter-stage"][data-presenter-stage-ready="true"]';
const TARGET_SELECTOR = '[data-presenter-id="nav-tomorrow"]';
const DESTINATION_SELECTOR = '[data-presenter-id="tomorrow-page-ready"]';
const MOBILE_EVENT_SELECTOR =
  '[data-mobile-v23-page="tomorrow"] [data-mobile-listing-row][data-event-id]';
const MOBILE_EVENT_RAIL_SELECTOR = ".rail-window";
const MOBILE_EVENT_DESCRIPTION_SELECTOR = '.event-digest[aria-label="О событии"]';
const MOBILE_DETAIL_SELECTOR = "[data-mobile-event-production]";
const MOBILE_DETAIL_DESCRIPTION_SELECTOR =
  "[data-mobile-event-production] .mobile-event-production__prose";
const DESCRIPTION_DWELL_MS = 2_200;

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

function abortError() {
  const error = new Error("scenario stopped");
  error.name = "AbortError";
  return error;
}

function assertNotAborted(signal) {
  if (signal?.aborted) throw abortError();
}

function abortableDelay(milliseconds, signal) {
  assertNotAborted(signal);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, milliseconds);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(abortError());
      },
      { once: true },
    );
  });
}

function loadPlaywright() {
  const localRequire = createRequire(import.meta.url);
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
    this.ackCache = new Map();
    this.shuttingDown = false;
    this.shutdownPromise = null;
    this.heartbeatTimer = null;
    this.pollAbort = null;
    this.contextGeneration = 0;
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
      scenario: SCENARIO,
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
    await this.setInteractionMode(TOMORROW_MOBILE_CONTRACT.surface);
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
        // Intentionally do not await: polling must remain alive while the scenario runs.
        void this.dispatch(command, { remote: true }).catch((error) =>
          log("command dispatch failed", { id: command.id, error: errorText(error) }),
        );
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
    if (!command || !["run", "stop", "reset"].includes(command.action)) return;

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
      await this.handleRun(command, remote);
      return;
    }
    if (command.action === "stop") {
      await this.handleStop(command, remote);
      return;
    }
    await this.handleReset(command, remote);
  }

  isExpired(command) {
    if (!command.expires_at) return false;
    return Date.now() > Date.parse(command.expires_at) + config.commandTtlGraceMs;
  }

  async handleRun(command, remote) {
    if (this.activeRun) {
      if (remote) await this.ack(command, "error", `${SCENARIO} is already running`);
      return;
    }

    this.runController = new AbortController();
    await this.setAgentState("running", SCENARIO);
    if (remote) await this.ack(command, "running", SCENARIO);

    const runPromise = this.runTomorrowMobile(this.runController.signal)
      .then(async (evidence) => {
        const detail =
          `${SCENARIO}: event ${evidence.eventId} "${evidence.title}"; ` +
          "digest revealed after horizontal swipe; detail description visible";
        await this.setAgentState("completed", detail);
        if (remote) await this.ack(command, "completed", detail);
      })
      .catch(async (error) => {
        if (error?.name === "AbortError" || this.runController?.signal.aborted) {
          log("scenario cooperatively stopped", { commandId: command.id });
          return;
        }
        const detail = `${SCENARIO}: ${errorText(error)}`;
        await this.setAgentState("error", detail);
        if (remote) await this.ack(command, "error", detail);
      })
      .finally(() => {
        if (this.activeRun === runPromise) this.activeRun = null;
        this.runController = null;
      });

    this.activeRun = runPromise;
  }

  async handleStop(command, remote) {
    await this.setAgentState("stopping", "stop requested");
    if (remote) await this.ack(command, "stopping", "stop requested");
    await this.confirmStopped();
    // Idle is only published after the run settled or hard browser recovery completed.
    await this.setAgentState("idle", "agent confirmed stopped");
    if (remote) await this.ack(command, "idle", "agent confirmed stopped");
  }

  async handleReset(command, remote) {
    if (this.activeRun) {
      await this.setAgentState("stopping", "reset requested");
      if (remote) await this.ack(command, "stopping", "reset requested");
      await this.confirmStopped();
    }

    try {
      for (const extraPage of this.context.pages()) {
        if (extraPage !== this.page) await extraPage.close().catch(() => {});
      }
      await this.openStage();
      await this.setAgentState("idle", "stage reset");
      if (remote) await this.ack(command, "idle", "stage reset");
    } catch (error) {
      await this.hardRecoverContext(`reset recovery: ${errorText(error)}`);
      await this.setAgentState("idle", "stage reset after browser recovery");
      if (remote) await this.ack(command, "idle", "stage reset after browser recovery");
    }
  }

  async confirmStopped() {
    if (!this.activeRun) return;
    this.runController?.abort();
    const active = this.activeRun;
    const settled = Symbol("settled");
    const outcome = await Promise.race([
      active.then(() => settled, () => settled),
      abortableDelay(config.hardStopMs).then(() => "timeout"),
    ]);
    if (outcome === "timeout") {
      await this.hardRecoverContext("cooperative stop deadline exceeded");
      await active.catch(() => {});
    }
  }

  async hardRecoverContext(reason) {
    log("hard browser recovery", { reason });
    const oldContext = this.context;
    this.page = null;
    this.context = null;
    await oldContext?.close().catch(() => {});
    if (!this.shuttingDown) await this.createContextAndStage();
  }

  async runTomorrowMobile(signal) {
    assertNotAborted(signal);
    await this.openStage();
    // openStage navigates the shell, so mirror the already-accepted run state
    // again on the newly loaded visual status surface.
    await this.setAgentState("running", SCENARIO);
    assertNotAborted(signal);

    const frame = this.page.frameLocator(FRAME_SELECTOR);
    await this.enforceMobilePointerShield(frame);
    const target = frame.locator(TARGET_SELECTOR);
    await target.waitFor({ state: "visible", timeout: 20_000 });
    await target.scrollIntoViewIfNeeded();
    const boundingBox = await target.boundingBox();
    if (!boundingBox) throw new Error(`${TARGET_SELECTOR} has no boundingBox`);
    log("target acquired", { selector: TARGET_SELECTOR, boundingBox });
    await this.tapMobileLocator(frame, target, signal);
    await frame.locator(DESTINATION_SELECTOR).waitFor({ state: "visible", timeout: 20_000 });
    await this.page.waitForFunction(
      (selector) => {
        const embedded = document.querySelector(selector);
        return embedded?.contentWindow?.location?.pathname === "/zavtra/";
      },
      FRAME_SELECTOR,
      { timeout: 20_000 },
    );
    assertNotAborted(signal);

    await this.enforceMobilePointerShield(frame);
    const event = await this.selectTomorrowEvent(frame);
    const row = frame.locator(
      `${MOBILE_EVENT_SELECTOR}[data-event-id="${event.eventId}"]`,
    );
    await row.scrollIntoViewIfNeeded();
    await abortableDelay(350, signal);
    const rail = row.locator(MOBILE_EVENT_RAIL_SELECTOR);
    const digest = row.locator(MOBILE_EVENT_DESCRIPTION_SELECTOR);
    await rail.waitFor({ state: "visible", timeout: 10_000 });
    await digest.waitFor({ state: "attached", timeout: 10_000 });
    log("deterministic tomorrow event selected", event);

    let digestRevealed = await this.isHorizontallyRevealed(digest);
    for (let attempt = 0; attempt < 3 && !digestRevealed; attempt += 1) {
      await this.swipeRailTowardDescription(frame, rail, signal);
      await abortableDelay(420, signal);
      digestRevealed = await this.isHorizontallyRevealed(digest);
    }
    if (!digestRevealed) {
      const geometry = await rail.evaluate((node) => ({
        scrollLeft: node.scrollLeft,
        maxScroll: node.scrollWidth - node.clientWidth,
      }));
      throw new Error(
        `${MOBILE_EVENT_DESCRIPTION_SELECTOR} did not become horizontally visible: ${JSON.stringify(geometry)}`,
      );
    }

    await this.dwellOnDescription(digest, signal, "rail");
    await this.tapMobileLocator(frame, digest, signal);
    await frame.locator(MOBILE_DETAIL_SELECTOR).waitFor({ state: "visible", timeout: 20_000 });
    await this.page.waitForFunction(
      (selector) => {
        const embedded = document.querySelector(selector);
        return /^\/sobytiya\/[^/]+\/$/u.test(embedded?.contentWindow?.location?.pathname || "");
      },
      FRAME_SELECTOR,
      { timeout: 20_000 },
    );

    await this.enforceMobilePointerShield(frame);
    const detailDescription = frame.locator(MOBILE_DETAIL_DESCRIPTION_SELECTOR);
    await detailDescription.waitFor({ state: "visible", timeout: 20_000 });
    await detailDescription.scrollIntoViewIfNeeded();
    await this.dwellOnDescription(detailDescription, signal, "event-detail");

    if (config.artifactDir) {
      await this.page.screenshot({
        path: path.join(config.artifactDir, "tomorrow-mobile-1920x1080.png"),
        fullPage: false,
      });
    }
    assertNotAborted(signal);
    return {
      eventId: event.eventId,
      title: event.title.replace(/\s+/gu, " ").replaceAll('"', "'").trim().slice(0, 100),
    };
  }

  async selectTomorrowEvent(frame) {
    const rows = frame.locator(MOBILE_EVENT_SELECTOR);
    await rows.first().waitFor({ state: "visible", timeout: 20_000 });
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
        `${MOBILE_EVENT_SELECTOR} has no deterministic event candidate with id/title/gallery count`,
      );
    }
    return selected;
  }

  async tapMobileLocator(frame, locator, signal) {
    assertNotAborted(signal);
    await locator.scrollIntoViewIfNeeded();
    const box = await locator.boundingBox();
    if (!box) throw new Error("mobile tap target has no boundingBox");
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
    await abortableDelay(180, signal);
    // Required real Playwright action. CSS hides the pointer, so this presents
    // as the tap circle rather than a desktop cursor.
    await locator.click({ timeout: 10_000 });
  }

  async swipeRailTowardDescription(frame, rail, signal) {
    assertNotAborted(signal);
    const box = await rail.boundingBox();
    if (!box) throw new Error(`${MOBILE_EVENT_RAIL_SELECTOR} has no boundingBox`);
    const start = {
      x: Math.round(box.x + box.width * 0.84),
      y: Math.round(box.y + box.height * 0.52),
    };
    const end = {
      x: Math.round(box.x + box.width * 0.12),
      y: start.y,
    };

    await frame.locator("body").evaluate((body) => {
      document.querySelectorAll("[data-autopresenter-swipe-trail]").forEach((node) => node.remove());
      const trail = document.createElement("div");
      trail.dataset.autopresenterSwipeTrail = "true";
      trail.dataset.autopresenterSwipeFingerDirection = "left";
      trail.dataset.autopresenterSwipeContentDirection = "right";
      trail.setAttribute("aria-hidden", "true");
      Object.assign(trail.style, {
        position: "fixed",
        zIndex: "2147483647",
        left: "12%",
        top: "50%",
        width: "76%",
        height: "52px",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        borderRadius: "999px",
        background: "rgba(17,24,39,.78)",
        boxShadow: "0 10px 32px rgba(0,0,0,.28)",
        color: "#fff",
        font: "850 15px/1 Inter,system-ui,sans-serif",
        letterSpacing: ".02em",
        pointerEvents: "none",
      });
      trail.innerHTML =
        '<span style="color:#7de6c2;font-size:24px;margin-right:10px">←━━━━━━━━</span><span>Листаем событие вправо →</span>';
      body.append(trail);
      const animation = trail.animate(
        [
          { transform: "translateX(-20px)", opacity: 0 },
          { transform: "translateX(0)", opacity: 1, offset: 0.18 },
          { transform: "translateX(20px)", opacity: 1 },
        ],
        { duration: 900, easing: "cubic-bezier(.22,.8,.24,1)", fill: "forwards" },
      );
      animation.finished.finally(() => trail.remove());
    });

    let pointerDown = false;
    try {
      await this.page.mouse.move(start.x, start.y);
      await this.page.mouse.down();
      pointerDown = true;
      for (let step = 1; step <= 12; step += 1) {
        assertNotAborted(signal);
        const progress = step / 12;
        await this.page.mouse.move(
          Math.round(start.x + (end.x - start.x) * progress),
          start.y,
        );
        await abortableDelay(24, signal);
      }
      await this.page.mouse.up();
      pointerDown = false;
    } finally {
      if (pointerDown) await this.page.mouse.up().catch(() => {});
    }
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

  async dwellOnDescription(locator, signal, surface) {
    await locator.evaluate((node, dwellSurface) => {
      node.dataset.autopresenterDescriptionDwell = dwellSurface;
      Object.assign(node.style, {
        outline: "4px solid rgba(38,211,154,.94)",
        outlineOffset: "5px",
        borderRadius: "10px",
        boxShadow: "0 0 0 10px rgba(38,211,154,.16)",
      });
    }, surface);
    await abortableDelay(DESCRIPTION_DWELL_MS, signal);
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
      // Playwright guarantees the video only after BrowserContext.close()
      // resolves. All shutdown callers share this promise so main() cannot
      // race process exit against the signal handler's recorder flush.
      await this.context?.close().catch(() => {});
      await this.browser?.close().catch(() => {});
    })();
    return this.shutdownPromise;
  }
}

async function main() {
  const { chromium } = loadPlaywright();
  const agent = new PrototypeAgent(chromium);

  let shutdownStarted = false;
  for (const signalName of ["SIGINT", "SIGTERM"]) {
    process.on(signalName, () => {
      if (shutdownStarted) return;
      shutdownStarted = true;
      void agent.shutdown(signalName).finally(() => {
        process.exitCode = 0;
      });
    });
  }

  try {
    await agent.start();
  } finally {
    await agent.shutdown("main-exit");
  }
}

const invokedDirectly = process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);
if (invokedDirectly) {
  main().catch((error) => {
    process.stderr.write(`[autopresenter-agent] fatal: ${error?.stack || errorText(error)}\n`);
    process.exitCode = 1;
  });
}

export { PrototypeAgent, SCENARIO, VIEWPORT, loadPlaywright };
