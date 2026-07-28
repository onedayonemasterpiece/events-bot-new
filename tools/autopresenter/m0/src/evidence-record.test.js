"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const {
  mayStartLiveSmoke,
  projectEvidenceRecord,
  suiteMetrics,
} = require("./run-suite");

function internal(target = "local-fixture") {
  return {
    candidateId: "pre-cft-compat",
    machineAccountFingerprint: "d".repeat(64),
    run: 1,
    startedAt: "2026-07-28T00:00:00.000Z",
    finishedAt: "2026-07-28T00:00:01.000Z",
    profileMode: "fresh",
    headed: true,
    nodeProcessFresh: true,
    browserProcessFresh: true,
    browserStarted: true,
    browserExecutable: "browsers/managed/browser.exe",
    target,
    locator: '[data-presenter-id="nav-tomorrow"]',
    successMarkerVisible: true,
    routeSuccess: target === "live-site",
    contentSuccess: target === "live-site",
    orphanProcesses: [],
    screenshot: "runs/one/screenshot.png",
    trace: "runs/one/trace.zip",
    _outerOrphans: [],
    _browserRoot: "/release/browsers",
  };
}

test("projected run record has the strict evidence field set", () => {
  const record = projectEvidenceRecord(internal(), 0);
  assert.deepEqual(Object.keys(record).sort(), [
    "action",
    "adminRequired",
    "browserDownloadAttempted",
    "browserExecutable",
    "browserProcessFresh",
    "browserStarted",
    "candidateId",
    "coldStart",
    "error",
    "finishedAt",
    "headed",
    "installRequired",
    "liveContentSuccess",
    "liveRouteSuccess",
    "locator",
    "log",
    "machineAccountFingerprint",
    "nodeExitCode",
    "nodeProcessFresh",
    "orphanProcesses",
    "profileMode",
    "run",
    "schemaVersion",
    "screenshot",
    "startedAt",
    "successMarkerVisible",
    "target",
    "trace",
    "usedSystemBrowser",
  ]);
  assert.equal(record.action, "locator.click");
  assert.equal(record.liveRouteSuccess, null);
  assert.equal(record.error, null);
});

test("local and live metrics are independent and exact", () => {
  const local = Array.from({ length: 20 }, (_, index) => ({
    evidence: {
      ...projectEvidenceRecord({ ...internal(), run: index + 1 }, 0),
      target: "local-fixture",
    },
    paths: { evidence: `runs/local-${index + 1}/run.json` },
  }));
  const live = Array.from({ length: 5 }, (_, index) => ({
    evidence: projectEvidenceRecord(
      { ...internal("live-site"), run: index + 1 },
      0,
    ),
    paths: { evidence: `runs/live-${index + 1}/run.json` },
  }));
  assert.equal(suiteMetrics([...local, ...live], "local-fixture", 20).metTarget, true);
  assert.equal(suiteMetrics([...local, ...live], "live-site", 5).metTarget, true);
  live[0].evidence.liveContentSuccess = false;
  assert.equal(suiteMetrics([...local, ...live], "local-fixture", 20).metTarget, true);
  assert.equal(suiteMetrics([...local, ...live], "live-site", 5).metTarget, false);
});

test("live smoke is gated on a complete 20/20 local result", () => {
  const passing = Array.from({ length: 20 }, (_, index) => ({
    evidence: projectEvidenceRecord(
      { ...internal(), run: index + 1 },
      0,
    ),
    paths: { evidence: `run-${index + 1}.json` },
  }));
  assert.equal(mayStartLiveSmoke(passing), true);
  passing[19].evidence.error = { message: "one failed cycle" };
  assert.equal(mayStartLiveSmoke(passing), false);
});
