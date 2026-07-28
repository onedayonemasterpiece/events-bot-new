#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const {
  parseSelfTestOptions,
  resolveOptionalModulePath,
} = require("./cli-options");
const { errorRecord, ContractError } = require("./errors");
const { startFixtureServer } = require("./fixture-server");
const { ensureDirectory, readJson, writeJsonAtomic } = require("./json-file");
const {
  ensurePortableDirectory,
  isPathInside,
  loadPlaywrightCore,
  resolveManagedBrowser,
  resolvePortablePath,
  resolvePortableRoot,
  toPortableRelative,
  validatePortableLayout,
  verifyVersionsManifest,
  verifyExactCandidate,
} = require("./portable-contract");
const { runCycle } = require("./run-cycle");

const HELP = `Autopresenter M0 offline self-test

Required:
  --portable-root <absolute release root>
  --browser-executable <path beneath release browsers/>
  --portable-browsers-root <release browsers/>
  --versions-file <VERSIONS.json>
  --data-dir <release data/>
  --logs-dir <release logs/>
  --output <JSON path beneath logs/>

Optional:
  --playwright-module <path beneath release app/>
  --headed | --headless          target acceptance requires headed
`;

function windows10X64Evidence(platform = process.platform, arch = process.arch, release = os.release()) {
  const match = /^10\.0\.(\d+)/.exec(release);
  const build = match ? Number(match[1]) : null;
  return {
    arch,
    build,
    isWindows10X64:
      platform === "win32" &&
      arch === "x64" &&
      Number.isInteger(build) &&
      build >= 10240 &&
      build < 22000,
    platform,
    release,
  };
}

function probeDirectory(directory, prefix) {
  const filePath = path.join(
    directory,
    `${prefix}-${process.pid}-${Date.now()}.probe`,
  );
  const payload = `autopresenter-m0:${prefix}\n`;
  fs.writeFileSync(filePath, payload, { encoding: "utf8", flag: "wx" });
  const contentMatches = fs.readFileSync(filePath, "utf8") === payload;
  fs.unlinkSync(filePath);
  return contentMatches && !fs.existsSync(filePath);
}

function orphanStrings(cycle) {
  return (cycle?.orphanProcesses || []).map(
    (item) => `${item.pid}:${item.executable}:${item.started}`,
  );
}

async function main() {
  const options = parseSelfTestOptions(process.argv.slice(2));
  if (options.help) {
    process.stdout.write(HELP);
    return;
  }
  const startedAt = new Date().toISOString();
  const portableRoot = resolvePortableRoot(options.portableRootValue);
  const logsRoot = ensurePortableDirectory(
    portableRoot,
    options.logsDirectoryValue,
    "logs directory",
  );
  const outputPath = resolvePortablePath(
    portableRoot,
    options.outputValue,
    "self-test output",
  );
  if (!isPathInside(logsRoot, outputPath)) {
    throw new ContractError(
      "SELF_TEST_OUTPUT_INVALID",
      "--output must remain beneath --logs-dir",
    );
  }
  const checks = [];
  let cycle;
  let primaryError;
  let fixture;
  let manifest;
  let browser;
  let devicePixelRatio;
  const platform = windows10X64Evidence();
  checks.push({
    id: "target-windows-10-x64",
    passed: platform.isWindows10X64,
    evidence: platform,
  });
  checks.push({
    id: "headed-mode",
    passed: options.headed,
    evidence: { headed: options.headed },
  });

  try {
    const appRoot = path.resolve(__dirname, "..");
    const layout = validatePortableLayout({
      appRoot,
      nodeExecutable: process.execPath,
      portableRoot,
    });
    checks.push({
      id: "portable-node",
      passed: true,
      evidence: {
        executable: toPortableRelative(portableRoot, layout.nodeExecutable),
        version: process.version,
      },
    });
    browser = resolveManagedBrowser({
      browserExecutableValue: options.browserExecutableValue,
      portableBrowsersRootValue: options.portableBrowsersRootValue,
      portableRoot,
    });
    const versionsPath = resolvePortablePath(
      portableRoot,
      options.versionsFileValue,
      "VERSIONS.json",
    );
    manifest = readJson(versionsPath);
    verifyExactCandidate({
      appRoot,
      browserExecutable: browser.executablePath,
      candidateId: manifest.candidateId,
      portableRoot,
    });
    const hashes = verifyVersionsManifest({
      browserExecutable: browser.executablePath,
      manifest,
      nodeExecutable: layout.nodeExecutable,
      portableRoot,
    });
    checks.push({
      id: "exact-executable-manifest",
      passed: true,
      evidence: hashes,
    });

    const dataDirectory = ensurePortableDirectory(
      portableRoot,
      options.dataDirectoryValue,
      "data directory",
    );
    const dataRoot = ensurePortableDirectory(portableRoot, "data", "data root");
    const releaseLogsRoot = ensurePortableDirectory(
      portableRoot,
      "logs",
      "logs root",
    );
    if (
      !isPathInside(dataRoot, dataDirectory) ||
      !isPathInside(releaseLogsRoot, logsRoot)
    ) {
      throw new ContractError(
        "SELF_TEST_DIRECTORY_INVALID",
        "Probe directories must remain beneath release data/ and logs/",
      );
    }
    const dataProbe = probeDirectory(dataDirectory, "data");
    const logsProbe = probeDirectory(logsRoot, "logs");
    checks.push({
      id: "probe-files-created-and-deleted",
      passed: dataProbe && logsProbe,
      evidence: { dataProbe, logsProbe },
    });

    const playwrightModulePath = resolveOptionalModulePath(
      portableRoot,
      options.playwrightModuleValue,
    );
    const loaded = loadPlaywrightCore({
      appRoot,
      modulePath: playwrightModulePath,
    });
    fixture = await startFixtureServer(path.resolve(__dirname, "..", "fixture"));
    const artifactDirectory = path.join(logsRoot, "self-test-artifacts");
    const profileDirectory = path.join(dataDirectory, "self-test-profile");
    fs.rmSync(profileDirectory, { force: true, recursive: true });
    ensureDirectory(artifactDirectory);
    cycle = await runCycle(
      {
        browserExecutablePath: browser.executablePath,
        browserExecutableRelative: browser.executableRelative,
        candidateId: manifest.candidateId || "self-test",
        clickSelector: '[data-presenter-id="nav-tomorrow"]',
        cycleId: "offline-self-test",
        headed: options.headed,
        markerSelector: '[data-presenter-id="nav-tomorrow"]',
        nodeProcessFresh: true,
        portableBrowsersRoot: browser.portableBrowsersRoot,
        profileDirectory,
        profileMode: "fresh",
        profileStateExpected: "absent",
        run: 1,
        runDirectory: artifactDirectory,
        screenshotPath: path.join(artifactDirectory, "screenshot.png"),
        screenshotRelative: toPortableRelative(
          logsRoot,
          path.join(artifactDirectory, "screenshot.png"),
        ),
        successSelector: '[data-presenter-id="tomorrow-ready"]',
        target: "local-fixture",
        targetUrl: fixture.rootUrl,
        tracePath: path.join(artifactDirectory, "trace.zip"),
        traceRelative: toPortableRelative(
          logsRoot,
          path.join(artifactDirectory, "trace.zip"),
        ),
      },
      loaded.playwright,
    );
    devicePixelRatio = cycle.assertions.devicePixelRatio;
    fs.rmSync(profileDirectory, { force: true, recursive: true });
    checks.push({
      id: "managed-headed-browser-about-blank",
      passed:
        options.headed &&
        cycle.browserStarted &&
        cycle.assertions.aboutBlankObserved,
      evidence: {
        aboutBlankObserved: cycle.assertions.aboutBlankObserved,
        browserStarted: cycle.browserStarted,
        headed: options.headed,
      },
    });
    checks.push({
      id: "loopback-fixture-strict-click",
      passed:
        cycle.assertions.strictClickPerformed &&
        cycle.successMarkerVisible &&
        cycle.assertions.profileStateMatches,
      evidence: {
        action: cycle.action,
        loopback: true,
        successMarkerVisible: cycle.successMarkerVisible,
      },
    });
    checks.push({
      id: "clean-browser-shutdown",
      passed:
        cycle.browserCleanup.closeReturned &&
        cycle.browserCleanup.forcedTerminationPids.length === 0 &&
        cycle.orphanProcesses.length === 0,
      evidence: cycle.browserCleanup,
    });
    if (cycle.error) {
      primaryError = new ContractError(
        cycle.error.code || "SELF_TEST_CYCLE_FAILED",
        cycle.error.message,
      );
    }
  } catch (error) {
    primaryError = error;
  } finally {
    await fixture?.close();
  }

  const passed = !primaryError && checks.every((check) => check.passed);
  const selfTest = {
    passed,
    offline: true,
    targetSiteAccessed: false,
    localFixtureClick: Boolean(cycle?.assertions?.strictClickPerformed),
    probeFilesCreatedAndDeleted: Boolean(
      checks.find((check) => check.id === "probe-files-created-and-deleted")
        ?.passed,
    ),
    nodeExitCode: passed ? 0 : 1,
    browserStarted: Boolean(cycle?.browserStarted),
    browserClosed: Boolean(
      cycle?.browserCleanup?.closeReturned &&
        cycle?.browserCleanup?.forcedTerminationPids?.length === 0 &&
        cycle?.orphanProcesses?.length === 0,
    ),
    usedSystemBrowser: false,
    browserDownloadAttempted: false,
    adminRequired: false,
    installRequired: false,
    orphanProcesses: orphanStrings(cycle),
    error: primaryError
      ? String(primaryError.message || primaryError).slice(0, 2000)
      : passed
        ? null
        : "One or more target self-test checks failed",
  };
  const report = {
    schemaVersion: 1,
    kind: "autopresenter-m0-self-test",
    startedAt,
    finishedAt: new Date().toISOString(),
    scope: {
      m0Only: true,
      targetNetworkAccessed: false,
      relayAccessed: false,
      npmInvoked: false,
      adminRequired: false,
    },
    platform,
    devicePixelRatio,
    portablePath: portableRoot,
    browser: browser
      ? {
          executable: browser.executableRelative,
          usedSystemBrowser: false,
          browserDownloadAttempted: false,
        }
      : null,
    checks,
    selfTest,
  };
  writeJsonAtomic(outputPath, report);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  process.exitCode = passed ? 0 : 1;
}

if (require.main === module) {
  main().catch((error) => {
    process.stderr.write(
      `${JSON.stringify({
        kind: "autopresenter-m0-self-test-error",
        error: errorRecord(error),
      })}\n`,
    );
    process.exitCode = 1;
  });
}

module.exports = {
  probeDirectory,
  windows10X64Evidence,
};
