#!/usr/bin/env node

import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const SCENARIO = "tomorrow-mobile";
const VIEWPORT = Object.freeze({ width: 1920, height: 1080 });
const FRAME_SELECTOR = '#presenter-mobile-frame[data-presenter-id="mobile-site-frame"]';
const STAGE_READY_SELECTOR =
  '[data-presenter-id="presenter-stage"][data-presenter-stage-ready="true"]';
const TARGET_SELECTOR = '[data-presenter-id="nav-tomorrow"]';
const DESTINATION_SELECTOR = '[data-presenter-id="tomorrow-page-ready"]';

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
        "--window-position=0,0",
        "--window-size=1920,1080",
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
    });

    await this.openStage(this.page);
    log("browser context ready", { generation });
  }

  async openStage(page = this.page) {
    await page.goto(config.stageUrl, { waitUntil: "domcontentloaded", timeout: 30_000 });
    await page.locator(STAGE_READY_SELECTOR).waitFor({ state: "visible", timeout: 30_000 });
    await page.locator(FRAME_SELECTOR).waitFor({ state: "visible", timeout: 30_000 });
    if (!config.headless && config.fullscreen) {
      const isFullscreen = await page.evaluate(() => Boolean(document.fullscreenElement));
      if (!isFullscreen) {
        await page.keyboard.press("f");
        await page
          .waitForFunction(() => Boolean(document.fullscreenElement), undefined, { timeout: 2_000 })
          .catch(() => log("browser denied automatic fullscreen; use local F"));
      }
    }
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
      .then(async () => {
        await this.setAgentState("completed", `${SCENARIO}: /zavtra/ ready`);
        if (remote) await this.ack(command, "completed", `${SCENARIO}: /zavtra/ ready`);
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
    const target = frame.locator(TARGET_SELECTOR);
    await target.waitFor({ state: "visible", timeout: 20_000 });
    await target.scrollIntoViewIfNeeded();
    const boundingBox = await target.boundingBox();
    if (!boundingBox) throw new Error(`${TARGET_SELECTOR} has no boundingBox`);

    const localBox = await target.evaluate((node) => {
      const rect = node.getBoundingClientRect();
      return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
    });
    const point = {
      x: Math.round(localBox.x + localBox.width / 2),
      y: Math.round(localBox.y + localBox.height / 2),
    };
    log("target acquired", { selector: TARGET_SELECTOR, boundingBox, framePoint: point });

    await frame.locator("body").evaluate(async (body, destination) => {
      document.querySelector("[data-autopresenter-cursor]")?.remove();
      const cursor = document.createElement("div");
      cursor.dataset.autopresenterCursor = "true";
      cursor.setAttribute("aria-hidden", "true");
      Object.assign(cursor.style, {
        position: "fixed",
        zIndex: "2147483647",
        width: "24px",
        height: "24px",
        left: "0",
        top: "0",
        pointerEvents: "none",
        filter: "drop-shadow(0 5px 9px rgba(0,0,0,.38))",
        transform: "translate3d(330px, 700px, 0)",
      });
      cursor.innerHTML =
        '<svg width="24" height="30" viewBox="0 0 24 30" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"><path d="M2 2v21l5.8-5.2 4.1 9.4 4.1-1.8-4-9.1H20L2 2Z" fill="#fff" stroke="#111827" stroke-width="2" stroke-linejoin="round"/></svg>';
      body.append(cursor);
      const animation = cursor.animate(
        [
          { transform: "translate3d(330px, 700px, 0) scale(.9)", opacity: 0 },
          { transform: "translate3d(330px, 700px, 0) scale(1)", opacity: 1, offset: 0.18 },
          {
            transform: `translate3d(${destination.x}px, ${destination.y}px, 0) scale(1)`,
            opacity: 1,
          },
        ],
        { duration: 900, easing: "cubic-bezier(.22,.8,.24,1)", fill: "forwards" },
      );
      await animation.finished;
    }, point);
    assertNotAborted(signal);

    // This is a genuine Playwright pointer action, not a DOM-dispatched hover.
    await target.hover({ timeout: 10_000 });
    await abortableDelay(180, signal);
    await frame.locator("body").evaluate((body, destination) => {
      const ripple = document.createElement("div");
      ripple.dataset.autopresenterRipple = "true";
      ripple.setAttribute("aria-hidden", "true");
      Object.assign(ripple.style, {
        position: "fixed",
        zIndex: "2147483646",
        width: "12px",
        height: "12px",
        left: `${destination.x - 6}px`,
        top: `${destination.y - 6}px`,
        border: "3px solid rgba(38,211,154,.95)",
        borderRadius: "999px",
        pointerEvents: "none",
      });
      body.append(ripple);
      const animation = ripple.animate(
        [
          { transform: "scale(.5)", opacity: 1 },
          { transform: "scale(5)", opacity: 0 },
        ],
        { duration: 520, easing: "ease-out", fill: "forwards" },
      );
      animation.finished.finally(() => ripple.remove());
    }, point);
    await abortableDelay(120, signal);
    assertNotAborted(signal);

    // Capture the visible pointer/ripple state. The WebM continues through completion.
    if (config.artifactDir) {
      await this.page.screenshot({
        path: path.join(config.artifactDir, "tomorrow-mobile-1920x1080.png"),
        fullPage: false,
      });
    }

    // Required real browser interaction. Never replace with a DOM-dispatched activation.
    await target.click({ timeout: 10_000 });
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
