"use strict";

const path = require("node:path");
const { ContractError } = require("./errors");

function parseArguments(argv, specification) {
  const result = {};
  const known = new Set(Object.keys(specification));
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      throw new ContractError(
        "UNEXPECTED_ARGUMENT",
        `Unexpected positional argument: ${token}`,
      );
    }
    const name = token.slice(2);
    if (!known.has(name)) {
      throw new ContractError("UNKNOWN_OPTION", `Unknown option: --${name}`);
    }
    const definition = specification[name];
    if (definition.type === "boolean") {
      result[definition.key || name] = true;
      continue;
    }
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) {
      throw new ContractError("OPTION_VALUE_REQUIRED", `--${name} needs a value`);
    }
    result[definition.key || name] = value;
    index += 1;
  }
  return result;
}

function requireText(options, key, flag) {
  if (typeof options[key] !== "string" || options[key].trim() === "") {
    throw new ContractError("OPTION_REQUIRED", `${flag} is required`);
  }
  return options[key].trim();
}

function validateHeadMode(options) {
  if (options.headed && options.headless) {
    throw new ContractError(
      "HEAD_MODE_CONFLICT",
      "--headed and --headless are mutually exclusive",
    );
  }
  return !options.headless;
}

function validateLiveUrl(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new ContractError("LIVE_URL_INVALID", "--live-url must be an absolute URL");
  }
  if (url.protocol !== "https:") {
    throw new ContractError("LIVE_URL_HTTPS_REQUIRED", "Live smoke requires HTTPS");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new ContractError(
      "LIVE_URL_CREDENTIALS_FORBIDDEN",
      "Live smoke URL must not contain credentials, query parameters, or a fragment",
    );
  }
  if (!/\/_review\/[^/]+\/zavtra\/$/.test(url.pathname)) {
    throw new ContractError(
      "LIVE_URL_IMMUTABLE_ZAVTRA_REQUIRED",
      "Live smoke URL must be an immutable /_review/<build>/zavtra/ route",
    );
  }
  return url.toString();
}

const commonSpecification = {
  help: { type: "boolean" },
  "portable-root": { type: "string", key: "portableRootValue" },
  "browser-executable": { type: "string", key: "browserExecutableValue" },
  "portable-browsers-root": {
    type: "string",
    key: "portableBrowsersRootValue",
  },
  "playwright-module": { type: "string", key: "playwrightModuleValue" },
  headed: { type: "boolean" },
  headless: { type: "boolean" },
};

function parseSuiteOptions(argv) {
  const options = parseArguments(argv, {
    ...commonSpecification,
    "candidate-id": { type: "string", key: "candidateId" },
    "output-dir": { type: "string", key: "outputDirectoryValue" },
    "profile-root": { type: "string", key: "profileRootValue" },
    mode: { type: "string" },
    "live-url": { type: "string", key: "liveUrl" },
    "live-marker-selector": { type: "string", key: "liveMarkerSelector" },
    "live-click-selector": { type: "string", key: "liveClickSelector" },
    "live-success-selector": { type: "string", key: "liveSuccessSelector" },
  });
  if (options.help) {
    return { help: true };
  }

  options.portableRootValue = requireText(
    options,
    "portableRootValue",
    "--portable-root",
  );
  options.browserExecutableValue = requireText(
    options,
    "browserExecutableValue",
    "--browser-executable",
  );
  options.portableBrowsersRootValue = requireText(
    options,
    "portableBrowsersRootValue",
    "--portable-browsers-root",
  );
  options.candidateId = requireText(options, "candidateId", "--candidate-id");
  options.outputDirectoryValue = requireText(
    options,
    "outputDirectoryValue",
    "--output-dir",
  );
  options.profileRootValue = requireText(
    options,
    "profileRootValue",
    "--profile-root",
  );
  options.mode ||= "all";
  if (!["all", "local", "live"].includes(options.mode)) {
    throw new ContractError(
      "MODE_INVALID",
      "--mode must be one of all, local, or live",
    );
  }
  options.headed = validateHeadMode(options);

  if (options.mode !== "local") {
    options.liveUrl = validateLiveUrl(
      requireText(options, "liveUrl", "--live-url"),
    );
    options.liveClickSelector = requireText(
      options,
      "liveClickSelector",
      "--live-click-selector",
    );
    options.liveMarkerSelector =
      typeof options.liveMarkerSelector === "string" &&
      options.liveMarkerSelector.trim() !== ""
        ? options.liveMarkerSelector.trim()
        : options.liveClickSelector;
    options.liveSuccessSelector = requireText(
      options,
      "liveSuccessSelector",
      "--live-success-selector",
    );
  }
  return options;
}

function parseSelfTestOptions(argv) {
  const options = parseArguments(argv, {
    ...commonSpecification,
    "versions-file": { type: "string", key: "versionsFileValue" },
    "data-dir": { type: "string", key: "dataDirectoryValue" },
    "logs-dir": { type: "string", key: "logsDirectoryValue" },
    output: { type: "string", key: "outputValue" },
  });
  if (options.help) {
    return { help: true };
  }
  for (const [key, flag] of [
    ["portableRootValue", "--portable-root"],
    ["browserExecutableValue", "--browser-executable"],
    ["portableBrowsersRootValue", "--portable-browsers-root"],
    ["versionsFileValue", "--versions-file"],
    ["dataDirectoryValue", "--data-dir"],
    ["logsDirectoryValue", "--logs-dir"],
    ["outputValue", "--output"],
  ]) {
    options[key] = requireText(options, key, flag);
  }
  options.headed = validateHeadMode(options);
  return options;
}

function resolveOptionalModulePath(portableRoot, value) {
  if (!value) {
    return undefined;
  }
  return path.isAbsolute(value)
    ? path.resolve(value)
    : path.resolve(portableRoot, value);
}

module.exports = {
  parseSelfTestOptions,
  parseSuiteOptions,
  resolveOptionalModulePath,
  validateLiveUrl,
};
