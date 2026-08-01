import process from 'node:process';

import { chromium } from 'playwright';

import { issueFocusAgentTestCredentials } from './issue-focus-agent-test-credentials.mjs';

const entryUrl = String(process.env.FOCUS_E2E_ENTRY_URL || '').trim();
if (!entryUrl) throw new Error('Set FOCUS_E2E_ENTRY_URL to the deployed focus onboarding page.');

const credentials = await issueFocusAgentTestCredentials({ entryUrl });
if (process.env.GITHUB_ACTIONS === 'true') {
  console.log(`::add-mask::${credentials.otp}`);
  console.log(`::add-mask::${credentials.magicLink}`);
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
const page = await context.newPage();
const responses = [];
page.on('response', (response) => {
  const url = new URL(response.url());
  if (
    (url.pathname === '/auth/v1/verify' && response.request().method() === 'POST')
    || url.pathname.endsWith('/rpc/register_focus_group_participant_v1')
  ) {
    responses.push({ path: url.pathname, status: response.status() });
  }
});

try {
  await page.goto(credentials.entryUrl, { waitUntil: 'domcontentloaded', timeout: 30_000 });
  const otp = page.locator('#focus-email-otp');
  await otp.waitFor({ state: 'visible', timeout: 15_000 });
  await otp.focus();
  // Deliberately enter the six digits one by one and never press Enter. This
  // is the phone journey whose sixth-digit autosubmit previously regressed.
  await otp.pressSequentially(credentials.otp, { delay: 45 });
  await page.locator('[data-focus-email-otp-status]').filter({ hasText: 'Проверяем код' })
    .waitFor({ state: 'visible', timeout: 5_000 });
  await page.locator('[data-focus-done-title]').filter({ hasText: 'Участие подтверждено' })
    .waitFor({ state: 'visible', timeout: 30_000 });

  const verify = responses.find((item) => item.path === '/auth/v1/verify');
  const membership = responses.find((item) => item.path.endsWith('/rpc/register_focus_group_participant_v1'));
  if (verify?.status !== 200) throw new Error(`OTP verify response was ${verify?.status ?? 'not observed'}.`);
  if (!membership || ![200, 204, 409].includes(membership.status)) {
    throw new Error(`Focus membership response was ${membership?.status ?? 'not observed'}.`);
  }
  if (new URL(page.url()).searchParams.has('agent_test_email')) {
    throw new Error('The dedicated test email leaked into the visible browser URL.');
  }
  console.log(`Focus email OTP E2E passed: autosubmit=ok verify=${verify.status} membership=${membership.status}`);
} finally {
  await context.close();
  await browser.close();
}
