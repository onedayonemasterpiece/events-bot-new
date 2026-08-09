#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

import { isSafeMobilePreflightRetryReceipt } from '../mobile-web/appium-preflight.mjs';

const count = (value) => Number.isSafeInteger(Number(value)) && Number(value) >= 0
  ? Number(value) : Number.POSITIVE_INFINITY;

/**
 * Reuse the OTP mobile rule: one retry is allowed only after a closed,
 * first-attempt transport failure proves that no broker/Auth/Search side
 * effect happened and any created WebDriver session was deleted.
 */
export function productionHealthMobileRetryReceipt(result) {
  const preflight = result?.preflight;
  const auth = result?.auth || {};
  const search = result?.search || {};
  if (result?.schema_version !== 'search_production_health_evidence_v1'
    || result?.platform !== 'ios'
    || result?.product_health !== 'UNCONFIRMED'
    || result?.execution_status !== 'FAILED'
    || !isSafeMobilePreflightRetryReceipt(preflight)
    || count(auth.product_otp_issue_count) !== 0
    || count(auth.external_mail_send_count) !== 0
    || count(auth.external_mail_receipt_count) !== 0
    || count(auth.protected_probe_request_count) !== 0
    || count(search.ui_submission_count) !== 0
    || count(search.physical_post_count) !== 0) return null;

  return Object.freeze({
    schema_version: 'search-production-health-mobile-retry-v1',
    platform: 'ios',
    startup_attempt: 1,
    failure_stage: preflight.failure_stage || 'mobile_preflight',
    failure_class: preflight.failure_class || preflight.error_class || null,
    cleanup_confirmed: true,
    side_effects: Object.freeze({ ...preflight.side_effects }),
  });
}

export function validateProductionHealthMobileRetryReceipt(value) {
  const sideEffects = value?.side_effects;
  const topKeys = new Set([
    'schema_version', 'platform', 'startup_attempt', 'failure_stage',
    'failure_class', 'cleanup_confirmed', 'side_effects',
  ]);
  const sideEffectKeys = new Set([
    'schema_version', 'startup_attempt', 'broker_session_issued',
    'auth_callback_started', 'navigation_count', 'fetch_count',
    'search_post_count', 'webdriver_client_session_created',
    'webdriver_client_session_deleted',
  ]);
  if (value?.schema_version !== 'search-production-health-mobile-retry-v1'
    || value.platform !== 'ios'
    || value.startup_attempt !== 1
    || value.cleanup_confirmed !== true
    || !/^[a-z][a-z0-9_]{2,63}$/u.test(String(value.failure_stage || ''))
    || !/^[a-z][a-z0-9_]{2,63}$/u.test(String(value.failure_class || ''))
    || Object.keys(value).some((key) => !topKeys.has(key))
    || sideEffects?.schema_version !== 'mobile-preflight-side-effects-v1'
    || Object.keys(sideEffects).some((key) => !sideEffectKeys.has(key))
    || sideEffects.startup_attempt !== 1
    || sideEffects.broker_session_issued !== false
    || sideEffects.auth_callback_started !== false
    || sideEffects.navigation_count !== 0
    || sideEffects.fetch_count !== 0
    || sideEffects.search_post_count !== 0
    || (sideEffects.webdriver_client_session_created !== false
      && sideEffects.webdriver_client_session_deleted !== true)) return null;
  return value;
}

export async function readProductionHealthMobileRetryReceipt(path) {
  if (!path) return null;
  const value = JSON.parse(await readFile(path, 'utf8').catch(() => 'null'));
  return validateProductionHealthMobileRetryReceipt(value);
}

async function main(resultPath, receiptPath) {
  const result = JSON.parse(await readFile(resultPath, 'utf8'));
  const receipt = productionHealthMobileRetryReceipt(result);
  if (!receipt) {
    process.exitCode = 1;
    return;
  }
  if (receiptPath) await writeFile(receiptPath, `${JSON.stringify(receipt)}\n`, { mode: 0o600 });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main(process.argv[2], process.argv[3]).catch(() => { process.exitCode = 1; });
}
