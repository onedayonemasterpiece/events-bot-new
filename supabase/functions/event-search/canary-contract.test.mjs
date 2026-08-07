import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const source = await readFile(
  new URL("./index.ts", import.meta.url),
  "utf8",
);

test("execution variants are a closed server-authorized contract", () => {
  for (const mode of [
    "cached_vector",
    "cold_vector",
    "cold_vector_llm",
    "degraded_vector_fallback",
  ]) {
    assert.match(source, new RegExp(`"${mode}"`));
  }
  assert.match(source, /body\.execution_mode/);
  assert.match(source, /isSearchCanaryPrincipal\(service, userId\)/);
  assert.match(source, /search_canary_persona_required/);
  assert.doesNotMatch(source, /user_metadata|raw_user_meta_data/);
});

test("vector-only and deterministic degradation cannot send an LLM request", () => {
  assert.match(
    source,
    /requestedLlm = requestedExecutionMode === "cold_vector_llm" \|\|\s+requestedExecutionMode === "degraded_vector_fallback"/,
  );
  assert.match(
    source,
    /deterministicLlmFailure = isCanary &&\s+requestedExecutionMode === "degraded_vector_fallback"/,
  );
  assert.match(source, /status: "degraded:deterministic_canary_failure"/);
  assert.match(source, /attempts: \[\]/);
  assert.match(source, /counters\.llm_provider_attempts \+= 1/);
});

test("cache and terminal paths write fresh owner receipts with counters", () => {
  assert.match(source, /recordSearchCanaryReceipt\(service/);
  assert.match(source, /actualMode: "cached_vector"/);
  assert.match(source, /terminalStatus: "quota_exceeded"/);
  assert.match(source, /terminalStatus: "provider_error"/);
  assert.match(source, /p_response_event_ids: ids/);
  assert.match(source, /get_event_search_revision_snapshot_internal_v1/);
  assert.match(source, /catalog_revision: revisions\.catalog_revision/);
  assert.match(source, /corpus_revision: revisions\.corpus_revision/);
});

test("cold canary LLM attempts reserve database budget before provider send", () => {
  const reserve = source.indexOf("options.reserve_canary_attempt()");
  const provider = source.indexOf("options.counters.llm_provider_attempts += 1");
  assert.ok(reserve > 0 && provider > reserve);
  assert.match(source, /reserve_event_search_canary_llm_budget_internal_v1/);
  assert.match(source, /degraded:canary_daily_budget_exhausted/);
});
