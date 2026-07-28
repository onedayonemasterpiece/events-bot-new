"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");
const { validateLiveUrl } = require("./cli-options");
const { startFixtureServer } = require("./fixture-server");
const {
  resolveManagedBrowser,
  resolvePortableRoot,
} = require("./portable-contract");
const { buildRunPlan } = require("./run-plan");
const { windows10X64Evidence } = require("./self-test");

function temporaryRelease() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "autopresenter-m0-"));
  fs.mkdirSync(path.join(root, "browsers", "managed"), { recursive: true });
  fs.writeFileSync(path.join(root, "browsers", "managed", "browser.exe"), "x");
  return root;
}

function get(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        body += chunk;
      });
      response.on("end", () =>
        resolve({ body, status: response.statusCode, headers: response.headers }),
      );
    }).on("error", reject);
  });
}

test("run plan is exactly 10 fresh, 10 persistent, and 5 live", () => {
  const plan = buildRunPlan("all");
  assert.equal(plan.length, 25);
  assert.equal(
    plan.filter(
      (item) =>
        item.target === "local-fixture" && item.profileMode === "fresh",
    ).length,
    10,
  );
  assert.equal(
    plan.filter(
      (item) =>
        item.target === "local-fixture" && item.profileMode === "persistent",
    ).length,
    10,
  );
  assert.equal(
    plan.filter((item) => item.target === "live-site").length,
    5,
  );
  assert.equal(new Set(plan.map((item) => item.cycleId)).size, 25);
});

test("live URL contract permits only immutable HTTPS zavtra route", () => {
  assert.equal(
    validateLiveUrl(
      "https://kenigevents.ru/_review/build-123/zavtra/",
    ),
    "https://kenigevents.ru/_review/build-123/zavtra/",
  );
  assert.throws(() => validateLiveUrl("http://example.test/_review/a/zavtra/"));
  assert.throws(() => validateLiveUrl("https://example.test/zavtra/"));
  assert.throws(() =>
    validateLiveUrl("https://example.test/_review/a/zavtra/?token=secret"),
  );
});

test("managed browser must resolve inside release browsers directory", () => {
  const root = temporaryRelease();
  try {
    const portableRoot = resolvePortableRoot(root);
    const browser = resolveManagedBrowser({
      portableRoot,
      portableBrowsersRootValue: "browsers",
      browserExecutableValue: "browsers/managed/browser.exe",
    });
    assert.equal(browser.executableRelative, "browsers/managed/browser.exe");
    assert.throws(() =>
      resolveManagedBrowser({
        portableRoot,
        portableBrowsersRootValue: "browsers",
        browserExecutableValue: process.execPath,
      }),
    );
  } finally {
    fs.rmSync(root, { force: true, recursive: true });
  }
});

test("fixture is loopback-only and exposes deterministic presenter hooks", async () => {
  const fixture = await startFixtureServer(
    path.resolve(__dirname, "..", "fixture"),
  );
  try {
    assert.match(fixture.rootUrl, /^http:\/\/127\.0\.0\.1:\d+\/$/);
    const home = await get(fixture.rootUrl);
    assert.equal(home.status, 200);
    assert.match(home.body, /data-presenter-id="nav-tomorrow"/);
    assert.equal(home.headers["cache-control"], "no-store");
    const tomorrow = await get(`${fixture.rootUrl}zavtra/`);
    assert.equal(tomorrow.status, 200);
    assert.match(tomorrow.body, /data-presenter-id="tomorrow-ready"/);
  } finally {
    await fixture.close();
  }
});

test("Windows 10 evidence never treats Linux or Windows 11 as target pass", () => {
  assert.equal(
    windows10X64Evidence("win32", "x64", "10.0.19045").isWindows10X64,
    true,
  );
  assert.equal(
    windows10X64Evidence("win32", "x64", "10.0.22631").isWindows10X64,
    false,
  );
  assert.equal(
    windows10X64Evidence("linux", "x64", "6.8.0").isWindows10X64,
    false,
  );
});
