#!/usr/bin/env node
"use strict";

const path = require("node:path");
const { errorRecord } = require("./errors");
const { readJson, writeJsonAtomic } = require("./json-file");
const {
  loadPlaywrightCore,
  resolveManagedBrowser,
  resolvePortableRoot,
  validatePortableLayout,
} = require("./portable-contract");
const { initialRunRecord, runCycle } = require("./run-cycle");

function workerArguments(argv) {
  if (
    argv.length !== 4 ||
    argv[0] !== "--config" ||
    argv[2] !== "--result"
  ) {
    throw new Error("Usage: cycle-worker.js --config <json> --result <json>");
  }
  return { configPath: argv[1], resultPath: argv[3] };
}

async function main() {
  let config;
  let resultPath;
  try {
    const args = workerArguments(process.argv.slice(2));
    resultPath = args.resultPath;
    config = readJson(args.configPath);
    const portableRoot = resolvePortableRoot(config.portableRoot);
    const appRoot = path.resolve(__dirname, "..");
    validatePortableLayout({
      appRoot,
      nodeExecutable: process.execPath,
      portableRoot,
    });
    const browser = resolveManagedBrowser({
      browserExecutableValue: config.browserExecutableValue,
      portableBrowsersRootValue: config.portableBrowsersRootValue,
      portableRoot,
    });
    config.browserExecutablePath = browser.executablePath;
    config.portableBrowsersRoot = browser.portableBrowsersRoot;
    const loaded = loadPlaywrightCore({
      appRoot,
      modulePath: config.playwrightModulePath,
    });
    const record = await runCycle(config, loaded.playwright);
    writeJsonAtomic(resultPath, record);
    process.exitCode = record.passed ? 0 : 1;
  } catch (error) {
    const record = initialRunRecord(config || {});
    record.finishedAt = new Date().toISOString();
    record.error = errorRecord(error);
    if (resultPath) {
      writeJsonAtomic(resultPath, record);
    }
    process.exitCode = 1;
  }
}

main();
