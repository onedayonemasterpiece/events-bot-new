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
