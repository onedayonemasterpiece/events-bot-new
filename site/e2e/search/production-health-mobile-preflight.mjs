#!/usr/bin/env node
import { pathToFileURL } from 'node:url';

/**
 * Diagnostic-only preflight. Production health must instead create an adapter,
 * call adapter.preflight(), issue the broker session, and run the journey in
 * that same process. No WebDriver or auth handle is serialized here.
 */
export async function runMobilePreflightDiagnostic(platform, options = {}) {
  if (!['android', 'ios'].includes(platform)) throw new Error('mobile_preflight_platform_invalid');
  const module = platform === 'android'
    ? await import('./adapters/appium-android.mjs')
    : await import('./adapters/appium-ios.mjs');
  const create = platform === 'android'
    ? module.createAndroidSearchAdapter : module.createIosSearchAdapter;
  const adapter = await create(options);
  let receipt;
  try {
    receipt = await adapter.preflight();
  } finally {
    await adapter.close();
  }
  return Object.freeze({
    ...receipt,
    diagnostic_only: true,
    production_health_accepted: false,
    same_session_continuation: false,
    session_closed: true,
    continuation_strategy: 'unified_runner_in_process_only',
  });
}

async function main() {
  const receipt = await runMobilePreflightDiagnostic(String(process.argv[2] || '').toLowerCase());
  process.stdout.write(`${JSON.stringify(receipt)}\n`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main().catch((error) => {
    const receipt = error?.searchReceipt;
    process.stdout.write(`${JSON.stringify(receipt || {
      schema_version: 'mobile-preflight-failure-v1',
      platform: ['android', 'ios'].includes(process.argv[2]) ? process.argv[2] : 'unknown',
      failure_class: 'mobile_preflight_failed',
      retry_safe: false,
    })}\n`);
    process.exitCode = 1;
  });
}
