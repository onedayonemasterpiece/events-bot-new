"use strict";

const LOCAL_FRESH_RUNS = 10;
const LOCAL_PERSISTENT_RUNS = 10;
const LIVE_RUNS = 5;

function buildRunPlan(mode) {
  const plan = [];
  if (mode !== "live") {
    for (let run = 1; run <= LOCAL_FRESH_RUNS; run += 1) {
      plan.push({
        cycleId: `local-fresh-${String(run).padStart(2, "0")}`,
        profileMode: "fresh",
        run,
        suite: "localCompatibility",
        target: "local-fixture",
      });
    }
    for (let offset = 1; offset <= LOCAL_PERSISTENT_RUNS; offset += 1) {
      plan.push({
        cycleId: `local-persistent-${String(offset).padStart(2, "0")}`,
        profileMode: "persistent",
        run: LOCAL_FRESH_RUNS + offset,
        suite: "localCompatibility",
        target: "local-fixture",
      });
    }
  }
  if (mode !== "local") {
    for (let run = 1; run <= LIVE_RUNS; run += 1) {
      plan.push({
        cycleId: `live-fresh-${String(run).padStart(2, "0")}`,
        profileMode: "fresh",
        run,
        suite: "liveSmoke",
        target: "live-site",
      });
    }
  }
  return plan;
}

module.exports = {
  LIVE_RUNS,
  LOCAL_FRESH_RUNS,
  LOCAL_PERSISTENT_RUNS,
  buildRunPlan,
};
