"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");
const {
  assertTargetMachineProvenance,
  machineAccountFingerprint,
} = require("./machine-provenance");

test("machine/account fingerprint is stable and mismatch fails closed", () => {
  const fingerprint = machineAccountFingerprint("machine-guid", "user-sid");
  assert.match(fingerprint, /^[a-f0-9]{64}$/);
  assert.doesNotThrow(() =>
    assertTargetMachineProvenance(fingerprint, "19045", {
      machineAccountFingerprint: fingerprint,
      build: 19045,
    }),
  );
  assert.throws(
    () =>
      assertTargetMachineProvenance(fingerprint, "19045", {
        machineAccountFingerprint: "a".repeat(64),
        build: 19045,
      }),
    /does not match SYSTEM-INFO/,
  );
  assert.throws(
    () =>
      assertTargetMachineProvenance(fingerprint, "19045", {
        machineAccountFingerprint: fingerprint,
        build: 22631,
      }),
    /not the recorded target Windows 10 build/,
  );
});
