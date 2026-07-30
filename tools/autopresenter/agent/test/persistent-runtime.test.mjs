import assert from "node:assert/strict";
import test from "node:test";

import { PrototypeAgent } from "../agent.mjs";

function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("sequential Run commands switch scenarios in the same context generation and page", async () => {
  const agent = new PrototypeAgent(null);
  const originalContext = { id: "one-context" };
  const originalPage = { id: "one-page" };
  const secondRun = deferred();
  const starts = [];

  agent.context = originalContext;
  agent.page = originalPage;
  agent.contextGeneration = 1;
  agent.setAgentState = async () => {};
  agent.ensureStageReady = async () => {};
  agent.runScenario = async (scenarioId, signal) => {
    starts.push({
      scenarioId,
      context: agent.context,
      page: agent.page,
      generation: agent.contextGeneration,
    });
    if (scenarioId === "tomorrow-mobile") {
      await new Promise((_, reject) => {
        signal.addEventListener(
          "abort",
          () => reject(signal.reason),
          { once: true },
        );
      });
    }
    await secondRun.promise;
    return { summary: "done" };
  };

  await agent.handleRun(
    { id: "run-one", scenario: "tomorrow-mobile" },
    false,
    "tomorrow-mobile",
  );
  const firstRun = agent.activeRun;

  await agent.handleRun(
    { id: "run-two", scenario: "tomorrow-rail-like" },
    false,
    "tomorrow-rail-like",
  );
  const switchedRun = agent.activeRun;

  assert.notEqual(switchedRun, firstRun);
  assert.equal(agent.activeScenario, "tomorrow-rail-like");
  assert.deepEqual(
    starts.map(({ scenarioId }) => scenarioId),
    ["tomorrow-mobile", "tomorrow-rail-like"],
  );
  for (const start of starts) {
    assert.equal(start.context, originalContext);
    assert.equal(start.page, originalPage);
    assert.equal(start.generation, 1);
  }

  secondRun.resolve();
  await switchedRun;
  assert.equal(agent.activeRun, null);
  assert.equal(agent.runController, null);
  assert.equal(agent.activeScenario, null);
});

test("a new Run restores the persistent stage after an external auth failure", async () => {
  const agent = new PrototypeAgent(null);
  const calls = [];
  agent.page = {
    isClosed: () => false,
    url: () => "chrome-error://chromewebdata/",
    locator(selector) {
      assert.equal(selector, '[data-presenter-id="presenter-stage"][data-presenter-stage-ready="true"]');
      return { async isVisible() { return false; } };
    },
    async bringToFront() { calls.push("front"); },
  };
  agent.auxiliaryPage = {
    isClosed: () => false,
    async close() { calls.push("close-auth"); },
  };
  agent.openStage = async (page) => {
    assert.equal(page, agent.page);
    calls.push("open-stage");
  };

  await agent.ensureStageReady("run lecture-01");

  assert.deepEqual(calls, ["close-auth", "open-stage"]);
  assert.equal(agent.auxiliaryPage, null);
});

test("manual scroll nudges the visible surface without stopping the active scenario", async () => {
  const agent = new PrototypeAgent(null);
  const wheel = [];
  const acknowledgments = [];
  const activeRun = Promise.resolve();
  agent.activeRun = activeRun;
  agent.activeScenario = "weekend-desktop";
  agent.page = {
    locator(selector) {
      assert.equal(selector, "iframe:visible");
      return {
        last() {
          return {
            async count() { return 1; },
            async boundingBox() { return { x: 100, y: 40, width: 900, height: 800 }; },
          };
        },
      };
    },
    mouse: {
      async move() {},
      async wheel(x, y) { wheel.push([x, y]); },
    },
  };
  agent.ack = async (_command, status, detail) => acknowledgments.push({ status, detail });

  await agent.handleManualScroll(
    { id: "scroll-1", options: { direction: "up", amount: 420 } },
    true,
  );

  assert.equal(agent.activeRun, activeRun);
  assert.equal(agent.activeScenario, "weekend-desktop");
  assert.equal(wheel.length, 3);
  assert.ok(wheel.every(([x, y]) => x === 0 && y < 0));
  assert.deepEqual(acknowledgments, [
    { status: "completed", detail: "manual scroll up 420px" },
  ]);
});
