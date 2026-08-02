import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import process from 'node:process';

function xmlEscape(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

async function main() {
  const platform = String(process.env.E2E_PLATFORM || process.argv[2] || '').trim().toLowerCase();
  if (!['android', 'ios'].includes(platform)) throw new Error('mobile_platform_invalid');
  const selectedPath = resolve(String(process.env.E2E_SELECTED_EVENT_PATH || process.argv[3] || 'artifacts/event-reminders/selected-event.json'));
  const root = resolve(String(process.env.E2E_EVIDENCE_DIR || process.argv[4] || `artifacts/event-reminders/${platform}`));
  const smokeOk = String(process.env.E2E_MOBILE_SMOKE_OK || '0') === '1';
  const failureDomain = smokeOk ? null : String(process.env.E2E_MOBILE_FAILURE_DOMAIN || 'BLOCKED_MOBILE_ENVIRONMENT');
  const selected = JSON.parse(await readFile(selectedPath, 'utf8'));
  await mkdir(join(root, 'screenshots'), { recursive: true });
  await mkdir(join(root, 'native-ui'), { recursive: true });
  const result = {
    schema_version: 1,
    scenario_id: 'event.current_event.mobile_environment',
    platform,
    status: smokeOk ? 'PASS' : 'BLOCKED',
    failure_domain: failureDomain,
    expected_repo_sha: selected.expected_repo_sha || null,
    observed_repo_sha: selected.observed_repo_sha || null,
    selected_event: {
      event_url: selected.event_url,
      starts_at: selected.starts_at,
      ends_at: selected.ends_at,
      selection_reason: selected.selection_reason,
    },
    device: {
      device_name: process.env.E2E_DEVICE_NAME || null,
      platform_version: process.env.E2E_PLATFORM_VERSION || null,
      browser_name: process.env.E2E_BROWSER_NAME || (platform === 'android' ? 'Chrome' : 'Mobile Safari'),
      host_os_version: process.env.E2E_HOST_OS_VERSION || null,
      xcode_version: process.env.E2E_XCODE_VERSION || null,
      locale: 'ru-RU',
    },
    coverage: {
      emulator_booted: smokeOk,
      selected_event_url_open_requested: smokeOk,
      push_subscription: 'NOT_IMPLEMENTED',
      push_delivery: 'NOT_IMPLEMENTED',
      calendar_email_client_action: 'NOT_IMPLEMENTED',
      android_connector: platform === 'android' ? 'NOT_IMPLEMENTED' : 'SKIPPED_NOT_APPLICABLE',
    },
  };
  const planned = [
    { scenario_id: 'event.reminder.push_subscription', platform, status: 'NOT_IMPLEMENTED' },
    { scenario_id: 'event.reminder.push_delivery', platform, status: 'NOT_IMPLEMENTED' },
    { scenario_id: 'event.calendar_email.client_action', platform, status: 'NOT_IMPLEMENTED' },
    ...(platform === 'android' ? [{ scenario_id: 'event.calendar_connector.android', platform, status: 'NOT_IMPLEMENTED' }] : []),
  ];
  const summary = {
    schema_version: 1,
    scenario_id: result.scenario_id,
    platform,
    status: result.status,
    failure_domain: result.failure_domain,
    expected_repo_sha: result.expected_repo_sha,
    observed_repo_sha: result.observed_repo_sha,
    event_url: result.selected_event.event_url,
    note: 'Environment smoke only; product reminder/email/connector assertions remain NOT_IMPLEMENTED.',
    evidence: {
      result: 'result.json',
      selected_event: 'selected-event.json',
      planned_scenarios: 'planned-scenarios.jsonl',
      screenshots: 'screenshots/',
      native_ui: 'native-ui/',
      junit: 'junit.xml',
    },
  };
  await Promise.all([
    writeFile(join(root, 'selected-event.json'), `${JSON.stringify(selected, null, 2)}\n`),
    writeFile(join(root, 'result.json'), `${JSON.stringify(result, null, 2)}\n`),
    writeFile(join(root, 'qa-summary.json'), `${JSON.stringify(summary, null, 2)}\n`),
    writeFile(join(root, 'device.json'), `${JSON.stringify(result.device, null, 2)}\n`),
    writeFile(join(root, 'planned-scenarios.jsonl'), `${planned.map((item) => JSON.stringify(item)).join('\n')}\n`),
    writeFile(join(root, 'scenarios.jsonl'), `${JSON.stringify({ scenario_id: result.scenario_id, platform, status: result.status, failure_domain: result.failure_domain })}\n`),
    writeFile(
      join(root, 'junit.xml'),
      `<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="event-reminders-calendar-mobile-scaffold" tests="1" failures="${smokeOk ? 0 : 1}"><testcase classname="${platform}" name="event.current_event.mobile_environment">${smokeOk ? '' : `<failure type="${xmlEscape(failureDomain)}" message="${xmlEscape(failureDomain)}"/>`}</testcase></testsuite>\n`,
    ),
  ]);
  process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  if (!smokeOk) process.exitCode = 1;
}

main().catch((error) => {
  process.stderr.write(`mobile_environment_evidence_failed:${String(error?.message || error)}\n`);
  process.exitCode = 1;
});
