import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  GoogleProviderAttemptError,
  googleModelActionUrl,
  resolveStrictGoogleQuotaPool,
  SharedGoogleQuotaError,
  withSharedGoogleQuotaAttempt,
} from "./google-quota.ts";

const KEY_ID = "11111111-1111-4111-8111-111111111111";
const OTHER_KEY_ID = "22222222-2222-4222-8222-222222222222";
const SECRET = "test-secret-that-must-not-enter-accounting";

function reservation(overrides = {}) {
  return {
    ok: true,
    api_key_id: KEY_ID,
    env_var_name: "GOOGLE_API_KEY5",
    minute_bucket: "2026-07-31T17:00:00Z",
    day_bucket: "2026-07-31",
    quota_scope: "google:test-project",
    limiter_contract: "google_ai_project_model_atomic_v1",
    ...overrides,
  };
}

function fakeBackend({ responses = {}, rows = [] } = {}) {
  const calls = [];
  return {
    calls,
    async listActiveKeys(names) {
      calls.push({ name: "listActiveKeys", names });
      return rows;
    },
    async rpc(name, payload) {
      calls.push({ name, payload });
      const configured = responses[name];
      if (configured instanceof Error) throw configured;
      if (typeof configured === "function") return await configured(payload);
      return configured;
    },
  };
}

function key() {
  return {
    api_key_id: KEY_ID,
    configured_env_name: "GOOGLE_API_KEY5",
    limiter_env_name: "GOOGLE_API_KEY5",
    quota_scope: "google:test-project",
  };
}

function runOptions(backend, execute) {
  return {
    backend,
    key: key(),
    model: "gemini-embedding-2",
    reservedTpm: 512,
    consumer: "event-search-edge-test",
    accountName: "test",
    readEnv: (name) => name === "GOOGLE_API_KEY5" ? SECRET : "",
    execute,
  };
}

test("strict pool fails closed without the shared backend", async () => {
  await assert.rejects(
    resolveStrictGoogleQuotaPool(null, [{ env_name: "GOOGLE_API_KEY5" }]),
    (error) =>
      error instanceof SharedGoogleQuotaError && error.stage === "backend",
  );
});

test("provider URL construction is owned by the shared quota module", () => {
  const providerHost = ["generativelanguage", "googleapis", "com"].join(".");
  assert.equal(
    googleModelActionUrl("models/gemini-embedding-2", "embedContent"),
    `https://${providerHost}/v1beta/models/gemini-embedding-2:embedContent`,
  );
});

test("strict pool resolves compact/underscored aliases in declared order", async () => {
  const backend = fakeBackend({
    rows: [
      {
        id: OTHER_KEY_ID,
        env_var_name: "GOOGLE_API_KEY_4",
        quota_scope: "google:project-4",
        priority: 2,
      },
      {
        id: KEY_ID,
        env_var_name: "GOOGLE_API_KEY5",
        quota_scope: "google:project-5",
        priority: 1,
      },
    ],
  });
  const pool = await resolveStrictGoogleQuotaPool(backend, [
    { env_name: "GOOGLE_API_KEY5" },
    { env_name: "GOOGLE_API_KEY4" },
  ]);
  assert.deepEqual(pool, [
    {
      api_key_id: KEY_ID,
      configured_env_name: "GOOGLE_API_KEY5",
      limiter_env_name: "GOOGLE_API_KEY5",
      quota_scope: "google:project-5",
    },
    {
      api_key_id: OTHER_KEY_ID,
      configured_env_name: "GOOGLE_API_KEY4",
      limiter_env_name: "GOOGLE_API_KEY_4",
      quota_scope: "google:project-4",
    },
  ]);
});

test("strict pool rejects incomplete key metadata", async () => {
  const backend = fakeBackend({
    rows: [{
      id: KEY_ID,
      env_var_name: "GOOGLE_API_KEY5",
      quota_scope: "google:project-5",
      priority: 1,
    }],
  });
  await assert.rejects(
    resolveStrictGoogleQuotaPool(backend, [
      { env_name: "GOOGLE_API_KEY5" },
      { env_name: "GOOGLE_API_KEY4" },
    ]),
    (error) =>
      error instanceof SharedGoogleQuotaError && error.stage === "metadata",
  );
});

test("provider execution is ordered reserve, mark, send, finalize", async () => {
  const sequence = [];
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: () => {
        sequence.push("reserve");
        return reservation();
      },
      google_ai_mark_sent: () => sequence.push("mark"),
      google_ai_finalize: () => sequence.push("finalize"),
    },
  });
  const result = await withSharedGoogleQuotaAttempt(
    runOptions(backend, async (apiKey, lease) => {
      sequence.push("send");
      assert.equal(apiKey, SECRET);
      assert.equal(lease.api_key_id, KEY_ID);
      return {
        value: "ok",
        provider_status: "succeeded",
        usage: { input_tokens: 7, output_tokens: 3, total_tokens: 10 },
      };
    }),
  );
  assert.equal(result, "ok");
  assert.deepEqual(sequence, ["reserve", "mark", "send", "finalize"]);
  const reserveCall = backend.calls.find((call) => call.name === "google_ai_reserve");
  assert.deepEqual(reserveCall.payload.p_candidate_key_ids, [KEY_ID]);
  const finalizeCall = backend.calls.find((call) => call.name === "google_ai_finalize");
  assert.equal(finalizeCall.payload.p_usage_total_tokens, 10);
  assert.equal(JSON.stringify(backend.calls).includes(SECRET), false);
});

test("mark failure prevents provider execution and finalizes unsent reservation", async () => {
  let sent = false;
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: reservation(),
      google_ai_mark_sent: new Error("mark unavailable"),
      google_ai_finalize: null,
    },
  });
  await assert.rejects(
    withSharedGoogleQuotaAttempt(
      runOptions(backend, async () => {
        sent = true;
        return { value: "unexpected" };
      }),
    ),
    (error) =>
      error instanceof SharedGoogleQuotaError && error.stage === "mark_sent",
  );
  assert.equal(sent, false);
  const finalizeCall = backend.calls.find((call) => call.name === "google_ai_finalize");
  assert.equal(finalizeCall.payload.p_provider_status, "not_sent");
  assert.equal(finalizeCall.payload.p_usage_total_tokens, 0);
});

test("quota denial fails closed before mark or provider execution", async () => {
  let sent = false;
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: {
        ok: false,
        blocked_reason: "rpm",
        retry_after_ms: 1200,
      },
    },
  });
  await assert.rejects(
    withSharedGoogleQuotaAttempt(
      runOptions(backend, async () => {
        sent = true;
        return { value: "unexpected" };
      }),
    ),
    (error) =>
      error instanceof SharedGoogleQuotaError &&
      error.stage === "reserve" &&
      error.blocked_reason === "rpm" &&
      error.retry_after_ms === 1200,
  );
  assert.equal(sent, false);
  assert.deepEqual(backend.calls.map((call) => call.name), ["google_ai_reserve"]);
});

test("provider failure is finalized before it is rethrown", async () => {
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: reservation(),
      google_ai_mark_sent: null,
      google_ai_finalize: null,
      google_ai_report_provider_429: null,
    },
  });
  await assert.rejects(
    withSharedGoogleQuotaAttempt(
      runOptions(backend, async () => {
        throw new GoogleProviderAttemptError("provider_429", {
          provider_status: "http_429",
          error_type: "provider_http",
          error_code: "429",
          usage: { input_tokens: null, output_tokens: null, total_tokens: null },
        });
      }),
    ),
    /provider_429/u,
  );
  const finalizeCall = backend.calls.find((call) => call.name === "google_ai_finalize");
  assert.equal(finalizeCall.payload.p_provider_status, "http_429");
  assert.equal(finalizeCall.payload.p_error_type, "provider_http");
  assert.equal(finalizeCall.payload.p_error_code, "429");
  assert.equal(
    backend.calls.some((call) => call.name === "google_ai_report_provider_429"),
    true,
  );
});

test("provider 429 report failure prevents cross-key rotation", async () => {
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: reservation(),
      google_ai_mark_sent: null,
      google_ai_finalize: null,
      google_ai_report_provider_429: new Error("cooldown ledger unavailable"),
    },
  });
  await assert.rejects(
    withSharedGoogleQuotaAttempt(
      runOptions(backend, async () => {
        throw new GoogleProviderAttemptError("provider_429", {
          provider_status: "http_429",
          error_type: "provider_http",
          error_code: "429",
        });
      }),
    ),
    (error) =>
      error instanceof SharedGoogleQuotaError && error.stage === "finalize",
  );
});

test("malformed successful reservation is cleaned up and never sent", async () => {
  let sent = false;
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: reservation({ env_var_name: "" }),
      google_ai_finalize: null,
    },
  });
  await assert.rejects(
    withSharedGoogleQuotaAttempt(
      runOptions(backend, async () => {
        sent = true;
        return { value: "unexpected" };
      }),
    ),
    (error) =>
      error instanceof SharedGoogleQuotaError && error.stage === "metadata",
  );
  assert.equal(sent, false);
  assert.equal(
    backend.calls.some((call) => call.name === "google_ai_mark_sent"),
    false,
  );
  assert.equal(
    backend.calls.some((call) => call.name === "google_ai_finalize"),
    true,
  );
});

test("missing limiter contract is cleaned up before reading the key or sending", async () => {
  let sent = false;
  let keyRead = false;
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: reservation({ limiter_contract: undefined }),
      google_ai_finalize: null,
    },
  });
  const options = runOptions(backend, async () => {
    sent = true;
    return { value: "unexpected" };
  });
  options.readEnv = () => {
    keyRead = true;
    return SECRET;
  };
  await assert.rejects(
    withSharedGoogleQuotaAttempt(options),
    (error) =>
      error instanceof SharedGoogleQuotaError &&
      error.stage === "metadata" &&
      error.message === "shared_google_limiter_contract_missing",
  );
  assert.equal(keyRead, false);
  assert.equal(sent, false);
  assert.deepEqual(
    backend.calls.map((call) => call.name),
    ["google_ai_reserve", "google_ai_finalize"],
  );
});

test("incompatible limiter contract and missing quota scope both fail closed", async () => {
  for (const invalid of [
    { limiter_contract: "legacy_key_scoped_v0" },
    { quota_scope: "" },
  ]) {
    let sent = false;
    const backend = fakeBackend({
      responses: {
        google_ai_reserve: reservation(invalid),
        google_ai_finalize: null,
      },
    });
    await assert.rejects(
      withSharedGoogleQuotaAttempt(
        runOptions(backend, async () => {
          sent = true;
          return { value: "unexpected" };
        }),
      ),
      (error) => error instanceof SharedGoogleQuotaError && error.stage === "metadata",
    );
    assert.equal(sent, false);
    assert.equal(
      backend.calls.some((call) => call.name === "google_ai_mark_sent"),
      false,
    );
  }
});

test("finalize failure fails closed after one provider send", async () => {
  let sends = 0;
  const backend = fakeBackend({
    responses: {
      google_ai_reserve: reservation(),
      google_ai_mark_sent: null,
      google_ai_finalize: new Error("finalize unavailable"),
    },
  });
  await assert.rejects(
    withSharedGoogleQuotaAttempt(
      runOptions(backend, async () => {
        sends += 1;
        return { value: "not-returned" };
      }),
    ),
    (error) =>
      error instanceof SharedGoogleQuotaError && error.stage === "finalize",
  );
  assert.equal(sends, 1);
});

test("Edge provider call sites use the shared-attempt wrapper only", async () => {
  const source = await readFile(new URL("./index.ts", import.meta.url), "utf8");
  assert.equal(source.includes("providerKeyAttempts"), false);
  assert.equal(
    source.match(/withSharedGoogleQuotaAttempt\(\{/gu)?.length,
    2,
  );
  assert.equal(
    source.match(/generativelanguage\.googleapis\.com/gu)?.length || 0,
    0,
  );
});
