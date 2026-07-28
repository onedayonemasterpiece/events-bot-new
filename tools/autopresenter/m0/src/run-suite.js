#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { parseSuiteOptions, resolveOptionalModulePath } = require("./cli-options");
const { errorRecord, ContractError } = require("./errors");
const { startFixtureServer } = require("./fixture-server");
const { ensureDirectory, readJson, writeJsonAtomic } = require("./json-file");
const { validateSystemInfo } = require("../reporting/validate");
const {
  assertTargetMachineProvenance,
  queryMachineProvenance,
} = require("./machine-provenance");
const {
  difference,
  isProcessAlive,
  listProcessesUnderRoot,
  sanitizeProcesses,
  terminateOwnedProcesses,
} = require("./process-inspector");
const {
  ensurePortableDirectory,
  isPathInside,
  loadPlaywrightCore,
  resolveManagedBrowser,
  resolvePortablePath,
  resolvePortableRoot,
  sha256File,
  toPortableRelative,
  validatePortableLayout,
  verifyExactCandidate,
} = require("./portable-contract");
const {
  LIVE_RUNS,
  LOCAL_FRESH_RUNS,
  LOCAL_PERSISTENT_RUNS,
  buildRunPlan,
} = require("./run-plan");

const HELP = `Autopresenter M0 cold-cycle runner

Required:
  --portable-root <absolute release root>
  --candidate-id <exact candidate id>
  --browser-executable <path beneath release browsers/>
  --portable-browsers-root <release browsers/>
  --output-dir <path beneath release logs/>
  --profile-root <path beneath release data/>
  --system-info <bundle-local target SYSTEM-INFO.json>

Modes:
  --mode all|local|live          default: all
  local: 10 fresh + 10 persistent cold cycles
  live:  5 fresh cold cycles

Live/all additionally require:
  --live-url <https://.../_review/<build>/zavtra/>
  --live-click-selector <exact unique selector>
  --live-success-selector <exact unique post-click selector>

Optional:
  --live-marker-selector <exact unique pre-click marker; defaults to click selector>
  --playwright-module <path beneath release app/>
  --headed | --headless          default: headed
`;

function candidateIdIsSafe(value) {
  return /^[a-z0-9][a-z0-9._-]{0,63}$/.test(value);
}

function assertInsideNamedRoot(candidate, root, label) {
  if (!isPathInside(root, candidate)) {
    throw new ContractError(
      "OUTPUT_PATH_INVALID",
      `${label} must remain within ${toPortableRelative(
        path.dirname(root),
        root,
      )}/`,
    );
  }
}

function compactOrphans(processes, browserRoot) {
  return sanitizeProcesses(processes, browserRoot).map(
    (item) => `${item.pid}:${item.executable}:${item.started}`,
  );
}

function projectEvidenceRecord(internal, nodeExitCode) {
  const orphanProcesses = compactOrphans(
    internal._outerOrphans || [],
    internal._browserRoot,
  ).concat(
    (internal.orphanProcesses || []).map(
      (item) => `${item.pid}:${item.executable}:${item.started}`,
    ),
  );
  const projectedError = internal.error
    ? {
        ...internal.error,
        message: String(internal.error.message || "Unknown cycle error").slice(
          0,
          2000,
        ),
      }
    : null;
  return {
    schemaVersion: 1,
    candidateId: internal.candidateId,
    machineAccountFingerprint: internal.machineAccountFingerprint,
    run: internal.run,
    startedAt: internal.startedAt,
    finishedAt: internal.finishedAt,
    coldStart: true,
    headed: Boolean(internal.headed),
    profileMode: internal.profileMode,
    nodeProcessFresh: Boolean(internal.nodeProcessFresh),
    browserProcessFresh: Boolean(internal.browserProcessFresh),
    nodeExitCode,
    browserStarted: Boolean(internal.browserStarted),
    browserExecutable: internal.browserExecutable,
    usedSystemBrowser: false,
    browserDownloadAttempted: false,
    adminRequired: false,
    installRequired: false,
    target: internal.target,
    locator: internal.locator,
    action: "locator.click",
    successMarkerVisible: Boolean(internal.successMarkerVisible),
    liveRouteSuccess:
      internal.target === "live-site" ? Boolean(internal.routeSuccess) : null,
    liveContentSuccess:
      internal.target === "live-site" ? Boolean(internal.contentSuccess) : null,
    orphanProcesses: [...new Set(orphanProcesses)],
    screenshot: internal.screenshot || null,
    trace: internal.trace || null,
    log: internal.log,
    error: projectedError,
  };
}

function failedInternalRecord(config, error) {
  return {
    candidateId: config.candidateId,
    machineAccountFingerprint: config.machineAccountFingerprint,
    run: config.run,
    startedAt: new Date().toISOString(),
    finishedAt: new Date().toISOString(),
    profileMode: config.profileMode,
    headed: Boolean(config.headed),
    nodeProcessFresh: config.nodeProcessFresh,
    browserProcessFresh: false,
    browserStarted: false,
    browserExecutable: config.browserExecutableRelative,
    target: config.target,
    locator: config.clickSelector,
    successMarkerVisible: false,
    routeSuccess: false,
    contentSuccess: false,
    orphanProcesses: [],
    error: errorRecord(error),
  };
}

function spawnWorker({ configPath, resultPath, portableRoot, timeoutMs }) {
  const workerPath = path.join(__dirname, "cycle-worker.js");
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [workerPath, "--config", configPath, "--result", resultPath],
      {
        cwd: portableRoot,
        env: {
          ...process.env,
          PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD: "1",
        },
        stdio: ["ignore", "pipe", "pipe"],
        windowsHide: false,
      },
    );
    const pid = child.pid;
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    child.stdout.on("data", (chunk) => {
      stdout = `${stdout}${chunk}`.slice(-8000);
    });
    child.stderr.on("data", (chunk) => {
      stderr = `${stderr}${chunk}`.slice(-8000);
    });
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);
    child.once("error", (error) => {
      clearTimeout(timer);
      resolve({ error, exitCode: 1, pid, signal: null, stderr, stdout, timedOut });
    });
    child.once("close", (exitCode, signal) => {
      clearTimeout(timer);
      resolve({
        exitCode: Number.isInteger(exitCode) ? exitCode : 1,
        pid,
        signal,
        stderr,
        stdout,
        timedOut,
      });
    });
  });
}

async function executeCycle({
  browser,
  common,
  planItem,
  profileDirectory,
  target,
}) {
  const runDirectory = path.join(common.outputDirectory, "runs", planItem.cycleId);
  ensureDirectory(runDirectory);
  const configPath = path.join(runDirectory, "worker-config.json");
  const internalResultPath = path.join(runDirectory, "runtime-result.json");
  const evidenceResultPath = path.join(runDirectory, "run.json");
  const screenshotPath = path.join(runDirectory, "screenshot.png");
  const tracePath = path.join(runDirectory, "trace.zip");
  const logPath = path.join(runDirectory, "worker.log");
  const config = {
    ...target,
    browserExecutableRelative: browser.executableRelative,
    browserExecutableValue: common.browserExecutableValue,
    candidateId: common.candidateId,
    machineAccountFingerprint: common.machineAccountFingerprint,
    cycleId: planItem.cycleId,
    headed: common.headed,
    nodeProcessFresh: true,
    playwrightModulePath: common.playwrightModulePath,
    portableBrowsersRootValue: common.portableBrowsersRootValue,
    portableRoot: common.portableRoot,
    profileDirectory,
    profileMode: planItem.profileMode,
    run: planItem.run,
    runDirectory,
    screenshotPath,
    screenshotRelative: toPortableRelative(common.outputDirectory, screenshotPath),
    target: planItem.target,
    tracePath,
    traceRelative: toPortableRelative(common.outputDirectory, tracePath),
  };
  writeJsonAtomic(configPath, config);

  const before = listProcessesUnderRoot(browser.portableBrowsersRoot);
  let worker;
  if (before.probeErrors.length || before.processes.length) {
    worker = {
      error: new ContractError(
        before.probeErrors.length
          ? "PROCESS_PROBE_FAILED"
          : "MANAGED_BROWSER_ALREADY_RUNNING",
        before.probeErrors.join("; ") ||
          "A process already exists beneath this candidate's browsers/ root",
      ),
      exitCode: 1,
      pid: null,
      signal: null,
      timedOut: false,
    };
  } else {
    worker = await spawnWorker({
      configPath,
      portableRoot: common.portableRoot,
      resultPath: internalResultPath,
      timeoutMs: planItem.target === "live-site" ? 90000 : 60000,
    });
  }
  fs.writeFileSync(
    logPath,
    [
      `exitCode=${worker.exitCode}`,
      `signal=${worker.signal || ""}`,
      `timedOut=${worker.timedOut}`,
      "--- stdout ---",
      worker.stdout || "",
      "--- stderr ---",
      worker.stderr || "",
      "",
    ].join(os.EOL),
    "utf8",
  );

  let internal;
  try {
    internal = readJson(internalResultPath);
  } catch {
    internal = failedInternalRecord(
      config,
      worker.error ||
        new ContractError(
          worker.timedOut ? "WORKER_TIMEOUT" : "WORKER_RESULT_MISSING",
          worker.timedOut
            ? "Fresh Node worker exceeded its bounded cycle timeout"
            : `Fresh Node worker did not write a result (signal: ${
                worker.signal || "none"
              })`,
        ),
    );
  }

  const after = listProcessesUnderRoot(browser.portableBrowsersRoot);
  let outerOrphans =
    before.probeErrors.length || after.probeErrors.length
      ? []
      : difference(after.processes, before.processes);
  let forced = { errors: [], forcedTerminationPids: [], remaining: outerOrphans };
  if (outerOrphans.length) {
    forced = await terminateOwnedProcesses(
      outerOrphans,
      browser.portableBrowsersRoot,
    );
    outerOrphans = forced.remaining;
  }
  if (worker.pid && isProcessAlive(worker.pid)) {
    internal.error ||= errorRecord(
      new ContractError(
        "NODE_WORKER_ORPHAN",
        `Cycle Node worker ${worker.pid} remained alive after close`,
      ),
    );
  }
  internal.nodeProcessFresh = Boolean(worker.pid);
  if (worker.timedOut || forced.forcedTerminationPids.length || forced.errors.length) {
    internal.error ||= errorRecord(
      new ContractError(
        "PROCESS_CLEANUP_NOT_GRACEFUL",
        "Cycle required timeout or forced process cleanup",
      ),
    );
  }
  internal._outerOrphans = outerOrphans;
  internal._browserRoot = browser.portableBrowsersRoot;
  internal.log = toPortableRelative(common.outputDirectory, logPath);
  const evidence = projectEvidenceRecord(internal, worker.exitCode);
  writeJsonAtomic(evidenceResultPath, evidence);
  return {
    evidence,
    internal,
    paths: {
      evidence: toPortableRelative(common.outputDirectory, evidenceResultPath),
      runtime: toPortableRelative(common.outputDirectory, internalResultPath),
    },
    worker: {
      exitCode: worker.exitCode,
      pid: worker.pid,
      signal: worker.signal,
      terminated: worker.pid ? !isProcessAlive(worker.pid) : true,
      timedOut: worker.timedOut,
    },
  };
}

function suiteMetrics(results, target, expected) {
  const selected = results.filter((item) => item.evidence.target === target);
  const successful = selected.filter(
    (item) =>
      item.evidence.nodeExitCode === 0 &&
      item.evidence.headed === true &&
      item.evidence.browserStarted &&
      item.evidence.browserProcessFresh &&
      item.evidence.successMarkerVisible &&
      item.evidence.orphanProcesses.length === 0 &&
      item.evidence.error === null &&
      (target === "local-fixture" ||
        (item.evidence.liveRouteSuccess && item.evidence.liveContentSuccess)),
  ).length;
  return {
    expected,
    observed: selected.length,
    successful,
    metTarget: selected.length === expected && successful === expected,
    runFiles: selected.map((item) => item.paths.evidence),
  };
}

function mayStartLiveSmoke(results) {
  return suiteMetrics(results, "local-fixture", 20).metTarget;
}

async function main() {
  const options = parseSuiteOptions(process.argv.slice(2));
  if (options.help) {
    process.stdout.write(HELP);
    return;
  }
  if (!candidateIdIsSafe(options.candidateId)) {
    throw new ContractError(
      "CANDIDATE_ID_INVALID",
      "--candidate-id must match ^[a-z0-9][a-z0-9._-]{0,63}$",
    );
  }

  const portableRoot = resolvePortableRoot(options.portableRootValue);
  const appRoot = path.resolve(__dirname, "..");
  validatePortableLayout({
    appRoot,
    nodeExecutable: process.execPath,
    portableRoot,
  });
  const browser = resolveManagedBrowser({
    browserExecutableValue: options.browserExecutableValue,
    portableBrowsersRootValue: options.portableBrowsersRootValue,
    portableRoot,
  });
  verifyExactCandidate({
    appRoot,
    browserExecutable: browser.executablePath,
    candidateId: options.candidateId,
    portableRoot,
  });
  const systemInfoPath = resolvePortablePath(
    portableRoot,
    options.systemInfoValue,
    "target system info",
  );
  const systemInfo = readJson(systemInfoPath);
  validateSystemInfo(systemInfo);
  if (systemInfo.provenance.sourceCandidateId !== options.candidateId) {
    throw new ContractError(
      "SYSTEM_INFO_CANDIDATE_MISMATCH",
      "SYSTEM-INFO.json was not collected from this exact candidate",
    );
  }
  assertTargetMachineProvenance(
    systemInfo.provenance.machineAccountFingerprint,
    systemInfo.os.build,
    queryMachineProvenance(),
  );
  const logsRoot = ensurePortableDirectory(portableRoot, "logs", "logs root");
  const dataRoot = ensurePortableDirectory(portableRoot, "data", "data root");
  const outputDirectory = ensurePortableDirectory(
    portableRoot,
    options.outputDirectoryValue,
    "output directory",
  );
  const profileRoot = ensurePortableDirectory(
    portableRoot,
    options.profileRootValue,
    "profile root",
  );
  assertInsideNamedRoot(outputDirectory, logsRoot, "Output directory");
  assertInsideNamedRoot(profileRoot, dataRoot, "Profile root");

  const playwrightModulePath = resolveOptionalModulePath(
    portableRoot,
    options.playwrightModuleValue,
  );
  // Preflight only. Every credited cycle loads the same local module again in
  // its own fresh Node process.
  loadPlaywrightCore({ appRoot, modulePath: playwrightModulePath });
  const beforeSuite = listProcessesUnderRoot(browser.portableBrowsersRoot);
  if (beforeSuite.probeErrors.length || beforeSuite.processes.length) {
    throw new ContractError(
      beforeSuite.probeErrors.length
        ? "PROCESS_PROBE_FAILED"
        : "MANAGED_BROWSER_ALREADY_RUNNING",
      beforeSuite.probeErrors.join("; ") ||
        "Candidate browsers/ root is not process-clean before the suite",
    );
  }

  const startedAt = new Date().toISOString();
  const sessionProfileRoot = path.join(
    profileRoot,
    `suite-${options.candidateId}-${Date.now()}`,
  );
  ensureDirectory(sessionProfileRoot);
  const plan = buildRunPlan(options.mode);
  const common = {
    browserExecutableValue: options.browserExecutableValue,
    candidateId: options.candidateId,
    headed: options.headed,
    machineAccountFingerprint:
      systemInfo.provenance.machineAccountFingerprint,
    outputDirectory,
    playwrightModulePath,
    portableBrowsersRootValue: options.portableBrowsersRootValue,
    portableRoot,
  };
  const fixtureRoot = path.resolve(__dirname, "..", "fixture");
  let fixture;
  const results = [];
  const seenNodePids = new Set();
  try {
    if (options.mode !== "live") {
      fixture = await startFixtureServer(fixtureRoot);
    }
    for (const item of plan) {
      if (
        options.mode === "all" &&
        item.target === "live-site" &&
        !mayStartLiveSmoke(results)
      ) {
        break;
      }
      const isPersistent = item.profileMode === "persistent";
      const profileDirectory = isPersistent
        ? path.join(sessionProfileRoot, "local-persistent")
        : path.join(sessionProfileRoot, item.cycleId);
      const persistentOrdinal =
        isPersistent ? item.run - LOCAL_FRESH_RUNS : null;
      const target =
        item.target === "local-fixture"
          ? {
              clickSelector: '[data-presenter-id="nav-tomorrow"]',
              markerSelector: '[data-presenter-id="nav-tomorrow"]',
              profileStateExpected:
                persistentOrdinal && persistentOrdinal > 1 ? "present" : "absent",
              successSelector: '[data-presenter-id="tomorrow-ready"]',
              targetUrl: fixture.rootUrl,
            }
          : {
              clickSelector: options.liveClickSelector,
              markerSelector: options.liveMarkerSelector,
              profileStateExpected: null,
              successSelector: options.liveSuccessSelector,
              targetUrl: options.liveUrl,
            };
      const result = await executeCycle({
        browser,
        common,
        planItem: item,
        profileDirectory,
        target,
      });
      if (result.worker.pid && seenNodePids.has(result.worker.pid)) {
        result.evidence.nodeProcessFresh = false;
        result.evidence.error ||= {
          code: "NODE_PID_REUSED",
          message: "A credited cycle did not receive a distinct Node process id",
          name: "ContractError",
        };
        writeJsonAtomic(
          path.join(outputDirectory, "runs", item.cycleId, "run.json"),
          result.evidence,
        );
      }
      if (result.worker.pid) {
        seenNodePids.add(result.worker.pid);
      }
      results.push(result);
      if (!isPersistent) {
        fs.rmSync(profileDirectory, { force: true, recursive: true });
      }
    }
  } finally {
    await fixture?.close();
    fs.rmSync(sessionProfileRoot, { force: true, recursive: true });
  }

  const afterSuite = listProcessesUnderRoot(browser.portableBrowsersRoot);
  const suiteOrphans =
    beforeSuite.probeErrors.length || afterSuite.probeErrors.length
      ? []
      : difference(afterSuite.processes, beforeSuite.processes);
  const localCompatibility =
    options.mode === "live"
      ? { status: "not-run", expected: 20, metTarget: null }
      : suiteMetrics(results, "local-fixture", 20);
  const liveSmoke =
    options.mode === "local"
      ? { status: "not-run", expected: LIVE_RUNS, metTarget: null }
      : options.mode === "all" && !mayStartLiveSmoke(results)
        ? {
            ...suiteMetrics(results, "live-site", LIVE_RUNS),
            status: "blocked-by-local-compatibility",
          }
        : suiteMetrics(results, "live-site", LIVE_RUNS);
  const runtimeChecksPassed =
    suiteOrphans.length === 0 &&
    !afterSuite.probeErrors.length &&
    localCompatibility.metTarget !== false &&
    liveSmoke.metTarget !== false;
  const report = {
    schemaVersion: 1,
    kind: "autopresenter-m0-runtime-suite",
    candidateId: options.candidateId,
    startedAt,
    finishedAt: new Date().toISOString(),
    runtimeChecksPassed,
    m0CompatibilityVerdict: "REQUIRES_TARGET_WINDOWS_10_EVIDENCE_AGGREGATION",
    scope: {
      m0Only: true,
      overlays: false,
      phoneUi: false,
      recording: false,
      relay: false,
      stage: false,
    },
    platform: {
      arch: process.arch,
      node: process.version,
      platform: process.platform,
      release: os.release(),
    },
    browser: {
      executable: browser.executableRelative,
      executableSha256: sha256File(browser.executablePath),
      portableBrowsersRoot: toPortableRelative(
        portableRoot,
        browser.portableBrowsersRoot,
      ),
      usedSystemBrowser: false,
      browserDownloadAttempted: false,
    },
    suites: {
      localCompatibility: {
        ...localCompatibility,
        freshExpected: LOCAL_FRESH_RUNS,
        persistentExpected: LOCAL_PERSISTENT_RUNS,
      },
      liveSmoke,
    },
    processCleanup: {
      passed: suiteOrphans.length === 0 && afterSuite.probeErrors.length === 0,
      browserOrphans: compactOrphans(
        suiteOrphans,
        browser.portableBrowsersRoot,
      ),
      nodeWorkers: results.map((item) => item.worker),
      probeErrors: afterSuite.probeErrors,
    },
  };
  writeJsonAtomic(path.join(outputDirectory, "m0-runtime-suite.json"), report);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  process.exitCode = runtimeChecksPassed ? 0 : 1;
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(
      `${JSON.stringify({
        kind: "autopresenter-m0-runtime-error",
        error: errorRecord(error),
      })}\n`,
    );
    process.exitCode = 1;
  });
}

module.exports = {
  compactOrphans,
  mayStartLiveSmoke,
  projectEvidenceRecord,
  suiteMetrics,
};
