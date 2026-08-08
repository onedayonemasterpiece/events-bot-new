#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

export function isRetryableMobileStartupResult(result) {
  const receipt = result?.failure_receipt;
  const counters = result?.counters || {};
  const zeroTraffic = ['requests', 'responses', 'routes', 'validation_posts']
    .every((key) => Number(counters[key] || 0) === 0);
  return result?.status === 'FAIL'
    && result?.platform === 'ios'
    && result?.error_code === 'WebDriverError'
    && Array.isArray(result?.query_cases) && result.query_cases.length === 0
    && zeroTraffic
    && receipt?.failure_stage === 'webdriver_session_create'
    && receipt?.auth_callback_started === false
    && receipt?.webdriver_client_session_created === false
    && receipt?.appium_server_ready === true
    && receipt?.error_class === 'webdriver_client_request_timeout'
    && receipt?.startup_attempt === 1
    && receipt?.log_inspected === true
    && receipt?.log_truncated === false
    && receipt?.wda_session_started === false
    && receipt?.wda_started === false
    && Number(receipt?.elapsed_ms) >= 295_000
    && Number(receipt?.elapsed_ms) <= 330_000;
}

const closedReceiptKeys = Object.freeze([
  'failure_stage', 'auth_callback_started', 'webdriver_client_session_created',
  'appium_server_ready', 'elapsed_ms', 'startup_attempt', 'error_class',
  'log_inspected', 'log_truncated', 'simulator_started', 'wda_install_attempted',
  'wda_start_attempted', 'wda_start_failed', 'wda_session_attempted',
  'wda_session_started', 'wda_session_failed', 'wda_started',
]);

export function closedMobileStartupRetryReceipt(result) {
  if (!isRetryableMobileStartupResult(result)) return null;
  const source = result.failure_receipt;
  const receipt = { schema_version: 'appium-startup-retry-v1' };
  for (const key of closedReceiptKeys) receipt[key] = source[key];
  return receipt;
}

export async function readPriorMobileStartupReceipt(path) {
  if (!path) return null;
  const value = JSON.parse(await readFile(path, 'utf8').catch(() => 'null'));
  if (value?.schema_version !== 'appium-startup-retry-v1') return null;
  const allowed = new Set(['schema_version', ...closedReceiptKeys]);
  if (Object.keys(value).some((key) => !allowed.has(key))) return null;
  const booleanKeys = closedReceiptKeys.filter((key) => ![
    'failure_stage', 'elapsed_ms', 'startup_attempt', 'error_class',
  ].includes(key));
  if (booleanKeys.some((key) => typeof value[key] !== 'boolean')) return null;
  if (!isRetryableMobileStartupResult({
    status: 'FAIL', platform: 'ios', error_code: 'WebDriverError',
    counters: {}, query_cases: [], failure_receipt: value,
  })) return null;
  return value;
}

async function main(path, receiptPath) {
  const result = JSON.parse(await readFile(path, 'utf8'));
  const receipt = closedMobileStartupRetryReceipt(result);
  if (!receipt) {
    process.exitCode = 1;
    return;
  }
  if (receiptPath) await writeFile(receiptPath, `${JSON.stringify(receipt)}\n`, { mode: 0o600 });
  process.exitCode = 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main(process.argv[2], process.argv[3]).catch(() => { process.exitCode = 1; });
}
