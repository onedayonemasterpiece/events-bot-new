"use strict";

const crypto = require("node:crypto");
const { spawnSync } = require("node:child_process");
const { ContractError } = require("./errors");

function machineAccountFingerprint(machineGuid, userSid) {
  if (!machineGuid || !userSid) {
    throw new ContractError(
      "MACHINE_PROVENANCE_INCOMPLETE",
      "MachineGuid and current user SID are required",
    );
  }
  return crypto
    .createHash("sha256")
    .update(`${machineGuid}|${userSid}`)
    .digest("hex");
}

function queryMachineProvenance() {
  if (process.platform !== "win32" || process.arch !== "x64") {
    throw new ContractError(
      "TARGET_WINDOWS_X64_REQUIRED",
      "Machine provenance must be measured on target Windows x64",
    );
  }
  const script = String.raw`
$machineGuid = [string](Get-ItemPropertyValue 'HKLM:\SOFTWARE\Microsoft\Cryptography' MachineGuid)
$userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$os = Get-CimInstance Win32_OperatingSystem
@{
  machineGuid = $machineGuid
  userSid = $userSid
  build = [int]$os.BuildNumber
} | ConvertTo-Json -Compress
`;
  const encoded = Buffer.from(script, "utf16le").toString("base64");
  const result = spawnSync(
    "powershell.exe",
    ["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
    { encoding: "utf8", timeout: 15000, windowsHide: true },
  );
  if (result.error || result.status !== 0) {
    throw new ContractError(
      "MACHINE_PROVENANCE_PROBE_FAILED",
      String(result.error?.message || result.stderr || "no diagnostic").slice(
        0,
        1000,
      ),
    );
  }
  const value = JSON.parse(result.stdout.trim());
  return {
    machineAccountFingerprint: machineAccountFingerprint(
      value.machineGuid,
      value.userSid,
    ),
    build: Number(value.build),
  };
}

function assertTargetMachineProvenance(expectedFingerprint, expectedBuild, actual) {
  const numericExpectedBuild = Number(String(expectedBuild).split(".")[0]);
  if (
    !/^[a-f0-9]{64}$/.test(expectedFingerprint || "") ||
    expectedFingerprint !== actual.machineAccountFingerprint
  ) {
    throw new ContractError(
      "MACHINE_ACCOUNT_FINGERPRINT_MISMATCH",
      "Current Windows machine/account does not match SYSTEM-INFO.json",
    );
  }
  if (
    !Number.isInteger(actual.build) ||
    actual.build < 10240 ||
    actual.build >= 22000 ||
    actual.build !== numericExpectedBuild
  ) {
    throw new ContractError(
      "TARGET_WINDOWS_BUILD_MISMATCH",
      "Current OS build is not the recorded target Windows 10 build",
    );
  }
}

module.exports = {
  assertTargetMachineProvenance,
  machineAccountFingerprint,
  queryMachineProvenance,
};
