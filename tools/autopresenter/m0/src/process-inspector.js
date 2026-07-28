"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { isPathInside } = require("./portable-contract");

function identityKey(processRecord) {
  return `${processRecord.pid}:${processRecord.started || "unknown"}:${normalizeExecutable(
    processRecord.executablePath,
  )}`;
}

function normalizeExecutable(value) {
  const resolved = path.resolve(value);
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function linuxProcesses() {
  const processes = [];
  for (const entry of fs.readdirSync("/proc", { withFileTypes: true })) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) {
      continue;
    }
    const pid = Number(entry.name);
    try {
      const executablePath = fs.realpathSync.native(`/proc/${pid}/exe`);
      const statText = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
      const closeParen = statText.lastIndexOf(")");
      const fields = statText.slice(closeParen + 2).split(" ");
      processes.push({
        pid,
        ppid: Number(fields[1]),
        executablePath,
        started: fields[19] || "unknown",
      });
    } catch {
      // A process may exit between directory enumeration and inspection.
    }
  }
  return processes;
}

function windowsProcesses() {
  const command =
    "Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,ExecutablePath,CreationDate | ConvertTo-Json -Compress";
  const result = spawnSync(
    "powershell.exe",
    ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
    {
      encoding: "utf8",
      timeout: 15000,
      windowsHide: true,
    },
  );
  if (result.error || result.status !== 0) {
    throw new Error(
      `Windows process probe failed (${result.status ?? "spawn"}): ${String(
        result.error?.message || result.stderr || "no diagnostic",
      ).slice(0, 500)}`,
    );
  }
  const text = result.stdout.trim();
  if (!text) {
    return [];
  }
  const decoded = JSON.parse(text);
  return (Array.isArray(decoded) ? decoded : [decoded])
    .filter((item) => item.ExecutablePath)
    .map((item) => ({
      pid: Number(item.ProcessId),
      ppid: Number(item.ParentProcessId),
      executablePath: path.resolve(item.ExecutablePath),
      started: String(item.CreationDate || "unknown"),
    }));
}

function listProcessesUnderRoot(root) {
  try {
    const all =
      process.platform === "win32" ? windowsProcesses() : linuxProcesses();
    return {
      probeErrors: [],
      processes: all
        .filter((item) => isPathInside(root, item.executablePath))
        .sort((left, right) => left.pid - right.pid),
    };
  } catch (error) {
    return {
      probeErrors: [String(error.message || error).slice(0, 1000)],
      processes: [],
    };
  }
}

function difference(after, before) {
  const beforeKeys = new Set(before.map(identityKey));
  return after.filter((item) => !beforeKeys.has(identityKey(item)));
}

function sanitizeProcesses(processes, root) {
  return processes.map((item) => ({
    pid: item.pid,
    ppid: item.ppid,
    executable: path.relative(root, item.executablePath).split(path.sep).join("/"),
    started: item.started,
  }));
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitForOwnedProcessesGone(ownedProcesses, root, timeoutMs = 5000) {
  const keys = new Set(ownedProcesses.map(identityKey));
  const deadline = Date.now() + timeoutMs;
  let latest = listProcessesUnderRoot(root);
  while (
    latest.probeErrors.length === 0 &&
    latest.processes.some((item) => keys.has(identityKey(item))) &&
    Date.now() < deadline
  ) {
    await sleep(100);
    latest = listProcessesUnderRoot(root);
  }
  return {
    probeErrors: latest.probeErrors,
    remaining: latest.processes.filter((item) => keys.has(identityKey(item))),
  };
}

async function terminateOwnedProcesses(ownedProcesses, root) {
  const forcedTerminationPids = [];
  const errors = [];
  let current = listProcessesUnderRoot(root);
  if (current.probeErrors.length) {
    return {
      errors: current.probeErrors,
      forcedTerminationPids,
      remaining: ownedProcesses,
    };
  }
  const ownedKeys = new Set(ownedProcesses.map(identityKey));
  const verified = current.processes.filter((item) =>
    ownedKeys.has(identityKey(item)),
  );

  for (const item of verified) {
    try {
      process.kill(item.pid, "SIGTERM");
      forcedTerminationPids.push(item.pid);
    } catch (error) {
      if (error.code !== "ESRCH") {
        errors.push(`SIGTERM ${item.pid}: ${error.message}`);
      }
    }
  }
  let waited = await waitForOwnedProcessesGone(verified, root, 1500);
  for (const item of waited.remaining) {
    try {
      process.kill(item.pid, "SIGKILL");
    } catch (error) {
      if (error.code !== "ESRCH") {
        errors.push(`SIGKILL ${item.pid}: ${error.message}`);
      }
    }
  }
  waited = await waitForOwnedProcessesGone(verified, root, 1500);
  return {
    errors: [...errors, ...waited.probeErrors],
    forcedTerminationPids,
    remaining: waited.remaining,
  };
}

function isProcessAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code !== "ESRCH";
  }
}

module.exports = {
  difference,
  identityKey,
  isProcessAlive,
  listProcessesUnderRoot,
  sanitizeProcesses,
  terminateOwnedProcesses,
  waitForOwnedProcessesGone,
};
