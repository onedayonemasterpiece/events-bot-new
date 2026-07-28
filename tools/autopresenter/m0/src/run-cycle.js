"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { errorRecord, ContractError } = require("./errors");
const { ensureDirectory } = require("./json-file");
const {
  difference,
  listProcessesUnderRoot,
  sanitizeProcesses,
  terminateOwnedProcesses,
  waitForOwnedProcessesGone,
} = require("./process-inspector");

const PROFILE_PROBE_KEY = "autopresenter-m0-profile-probe";
const SCOPE = Object.freeze({
  m0Only: true,
  overlays: false,
  phoneUi: false,
  recording: false,
  relay: false,
  stage: false,
});

function initialRunRecord(config) {
  const record = {
    schemaVersion: 1,
    kind: "autopresenter-m0-cycle",
    candidateId: config.candidateId,
    run: config.run,
    cycleId: config.cycleId,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    coldStart: true,
    profileMode: config.profileMode,
    nodeProcessFresh: Boolean(config.nodeProcessFresh),
    browserProcessFresh: false,
    nodeExitCode: null,
    browserStarted: false,
    browserExecutable: config.browserExecutableRelative,
    usedSystemBrowser: false,
    browserDownloadAttempted: false,
    adminRequired: false,
    installRequired: false,
    target: config.target,
    locator: config.clickSelector,
    action: "locator.click",
    successMarkerVisible: false,
    orphanProcesses: [],
    screenshot: config.screenshotRelative,
    trace: config.traceRelative,
    passed: false,
    scope: SCOPE,
    assertions: {
      markerCount: null,
      markerVisible: false,
      clickCount: null,
      strictClickPerformed: false,
      successCount: null,
      aboutBlankObserved: false,
      profileStateExpected: config.profileStateExpected,
      profileStateObserved: null,
      profileStateMatches: false,
    },
    browserCleanup: {
      closeReturned: false,
      forcedTerminationPids: [],
      probeErrors: [],
    },
  };
  if (config.target === "live-site") {
    record.routeSuccess = false;
    record.contentSuccess = false;
  }
  return record;
}

function withTimeout(promise, timeoutMs, label) {
  let timeout;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timeout = setTimeout(() => {
        reject(
          new ContractError(
            "ACTION_TIMEOUT",
            `${label} did not finish within ${timeoutMs} ms`,
          ),
        );
      }, timeoutMs);
    }),
  ]).finally(() => clearTimeout(timeout));
}

async function exactVisible(locator, label) {
  const count = await locator.count();
  if (count !== 1) {
    throw new ContractError(
      "STRICT_LOCATOR_COUNT",
      `${label} must match exactly once; observed ${count}`,
    );
  }
  await locator.waitFor({ state: "visible", timeout: 10000 });
  return count;
}

async function saveFailureScreenshot(page, screenshotPath) {
  if (!page || page.isClosed()) {
    return false;
  }
  try {
    await page.screenshot({ path: screenshotPath, fullPage: false });
    return true;
  } catch {
    return false;
  }
}

async function runCycle(config, playwright) {
  const record = initialRunRecord(config);
  const browserRoot = config.portableBrowsersRoot;
  let context;
  let page;
  let traceStarted = false;
  let screenshotSaved = false;
  let primaryError;
  let before = { processes: [], probeErrors: [] };
  let during = { processes: [], probeErrors: [] };
  let spawned = [];

  ensureDirectory(config.runDirectory);
  ensureDirectory(config.profileDirectory);
  try {
    before = listProcessesUnderRoot(browserRoot);
    if (before.probeErrors.length) {
      throw new ContractError(
        "PROCESS_PROBE_FAILED",
        before.probeErrors.join("; "),
      );
    }
    if (before.processes.length) {
      throw new ContractError(
        "MANAGED_BROWSER_ALREADY_RUNNING",
        "Cold cycle requires no pre-existing process beneath this portable browsers/ root",
      );
    }

    context = await playwright.chromium.launchPersistentContext(
      config.profileDirectory,
      {
        executablePath: config.browserExecutablePath,
        headless: !config.headed,
        viewport: { height: 932, width: 430 },
      },
    );
    record.browserStarted = true;
    during = listProcessesUnderRoot(browserRoot);
    if (during.probeErrors.length) {
      throw new ContractError(
        "PROCESS_PROBE_FAILED",
        during.probeErrors.join("; "),
      );
    }
    spawned = difference(during.processes, before.processes);
    record.browserProcessFresh = spawned.length > 0;
    if (!record.browserProcessFresh) {
      throw new ContractError(
        "BROWSER_PROCESS_NOT_OBSERVED",
        "No fresh process was observed beneath the portable browsers/ root",
      );
    }

    context.setDefaultTimeout(10000);
    context.setDefaultNavigationTimeout(20000);
    await context.tracing.start({
      screenshots: true,
      snapshots: true,
      sources: false,
    });
    traceStarted = true;
    page = context.pages()[0] || (await context.newPage());
    record.assertions.aboutBlankObserved = page.url() === "about:blank";
    if (!record.assertions.aboutBlankObserved) {
      throw new ContractError(
        "ABOUT_BLANK_PRECONDITION_FAILED",
        "Fresh managed browser did not begin on about:blank",
      );
    }

    const response = await page.goto(config.targetUrl, {
      waitUntil: "domcontentloaded",
      timeout: 20000,
    });
    if (config.target === "live-site") {
      const requested = new URL(config.targetUrl);
      const observed = new URL(page.url());
      record.routeSuccess =
        Boolean(response) &&
        response.status() < 400 &&
        observed.origin === requested.origin &&
        observed.pathname === requested.pathname;
    }

    const marker = page.locator(config.markerSelector);
    record.assertions.markerCount = await exactVisible(marker, "marker selector");
    record.assertions.markerVisible = true;
    if (config.target === "live-site") {
      record.contentSuccess = true;
    } else {
      const previous = await page.evaluate(
        (key) => window.localStorage.getItem(key),
        PROFILE_PROBE_KEY,
      );
      record.assertions.profileStateObserved =
        previous === null ? "absent" : "present";
      record.assertions.profileStateMatches =
        record.assertions.profileStateObserved === config.profileStateExpected;
      if (!record.assertions.profileStateMatches) {
        throw new ContractError(
          "PROFILE_MODE_MISMATCH",
          `Expected profile state ${config.profileStateExpected}, observed ${record.assertions.profileStateObserved}`,
        );
      }
      await page.evaluate(
        ([key, value]) => window.localStorage.setItem(key, value),
        [PROFILE_PROBE_KEY, `${config.candidateId}:persistent-evidence`],
      );
    }

    const clickTarget = page.locator(config.clickSelector);
    record.assertions.clickCount = await exactVisible(
      clickTarget,
      "click selector",
    );
    await clickTarget.click({ timeout: 10000 });
    record.assertions.strictClickPerformed = true;

    const success = page.locator(config.successSelector);
    record.assertions.successCount = await exactVisible(
      success,
      "success selector",
    );
    record.successMarkerVisible = true;
    await page.screenshot({ path: config.screenshotPath, fullPage: false });
    screenshotSaved = true;
    await context.tracing.stop({ path: config.tracePath });
    traceStarted = false;
  } catch (error) {
    primaryError = error;
    screenshotSaved ||= await saveFailureScreenshot(page, config.screenshotPath);
  } finally {
    if (traceStarted && context) {
      try {
        await context.tracing.stop({ path: config.tracePath });
      } catch (error) {
        primaryError ||= error;
      }
    }
    if (context) {
      try {
        await withTimeout(context.close(), 5000, "browser context close");
        record.browserCleanup.closeReturned = true;
      } catch (error) {
        primaryError ||= error;
      }
    }

    let gone = await waitForOwnedProcessesGone(spawned, browserRoot, 5000);
    record.browserCleanup.probeErrors.push(...gone.probeErrors);
    if (gone.remaining.length) {
      const forced = await terminateOwnedProcesses(spawned, browserRoot);
      record.browserCleanup.forcedTerminationPids.push(
        ...forced.forcedTerminationPids,
      );
      record.browserCleanup.probeErrors.push(...forced.errors);
      gone = {
        probeErrors: forced.errors,
        remaining: forced.remaining,
      };
    }
    record.orphanProcesses = sanitizeProcesses(gone.remaining, browserRoot);
    record.browserCleanup.processes = {
      before: sanitizeProcesses(before.processes, browserRoot),
      during: sanitizeProcesses(during.processes, browserRoot),
      spawned: sanitizeProcesses(spawned, browserRoot),
      remaining: record.orphanProcesses,
    };
  }

  if (!screenshotSaved) {
    record.screenshot = undefined;
    primaryError ||= new ContractError(
      "SCREENSHOT_MISSING",
      "Cycle did not produce the required screenshot",
    );
  }
  if (!fs.existsSync(config.tracePath)) {
    record.trace = undefined;
    primaryError ||= new ContractError(
      "TRACE_MISSING",
      "Cycle did not produce the required Playwright trace",
    );
  }
  if (record.browserCleanup.probeErrors.length) {
    primaryError ||= new ContractError(
      "PROCESS_PROBE_FAILED",
      record.browserCleanup.probeErrors.join("; "),
    );
  }
  if (record.browserCleanup.forcedTerminationPids.length) {
    primaryError ||= new ContractError(
      "BROWSER_DID_NOT_CLOSE_CLEANLY",
      "Managed browser required forced termination",
    );
  }
  if (record.orphanProcesses.length) {
    primaryError ||= new ContractError(
      "BROWSER_ORPHAN",
      "Managed browser processes remained after bounded cleanup",
    );
  }

  const requiredAssertions =
    record.browserStarted &&
    record.browserProcessFresh &&
    record.assertions.aboutBlankObserved &&
    record.assertions.markerVisible &&
    record.assertions.strictClickPerformed &&
    record.successMarkerVisible &&
    (config.target === "live-site"
      ? record.routeSuccess && record.contentSuccess
      : record.assertions.profileStateMatches);
  record.passed = !primaryError && requiredAssertions;
  if (primaryError) {
    record.error = errorRecord(primaryError);
  }
  record.finishedAt = new Date().toISOString();
  return record;
}

module.exports = {
  PROFILE_PROBE_KEY,
  SCOPE,
  initialRunRecord,
  runCycle,
};
