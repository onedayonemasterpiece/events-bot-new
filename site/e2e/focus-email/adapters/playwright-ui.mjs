import { join } from 'node:path';
import { chromium } from 'playwright';

import { observedRepoSha } from '../helpers/release-identity.mjs';
import { redactText } from '../helpers/redaction.mjs';
import { createSanitizedNetworkRecorder } from '../helpers/sanitized-network-recorder.mjs';

async function maskPage(page) {
  await page.evaluate(() => {
    document.querySelectorAll('input[type="email"], input[autocomplete="one-time-code"]').forEach((node) => {
      node.value = '';
      node.setAttribute('data-e2e-masked', 'true');
    });
    document.querySelectorAll('[data-focus-email-destination], [data-static-auth-name]').forEach((node) => {
      node.textContent = 'f***@k***';
    });
    document.querySelectorAll('[data-focus-otp-digit]').forEach((node) => {
      if (node.textContent) node.textContent = '•';
    });
  });
}

export async function createPlaywrightUi({ target, expectedRepoSha, expectedFaultProfile = 'normal', evidenceRoot, secrets, directHost, relayHost }) {
  const browser = await chromium.launch({ headless: true });
  const viewport = { width: 390, height: 844 };
  const context = await browser.newContext({ viewport });
  const page = await context.newPage();
  const consoles = [];
  let otp = '';
  let verifiedRepoSha = null;
  const faultEvents = [];
  const transportEvents = [];
  let faultReceipt = null;
  let faultPageEventsSeen = 0;
  let transportPageEventsSeen = 0;
  async function syncTransportFaultEvidence() {
    const current = await page.evaluate(() => ({
      receipt: globalThis['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:receipt'] || null,
      faults: globalThis['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:events'] || [],
      transport: globalThis['KENIGEVENTS_E2E_TRANSPORT_FAULT_INJECTOR_V1:transport-events'] || [],
    })).catch(() => ({ receipt: null, faults: [], transport: [] }));
    faultReceipt ||= current.receipt;
    faultEvents.push(...current.faults.slice(faultPageEventsSeen));
    transportEvents.push(...current.transport.slice(transportPageEventsSeen));
    faultPageEventsSeen = current.faults.length;
    transportPageEventsSeen = current.transport.length;
    return { receipt: faultReceipt, fault_events: faultEvents, transport_events: transportEvents, transport_outcomes: [] };
  }
  page.on('console', (message) => {
    if (['error', 'warning'].includes(message.type())) {
      consoles.push({ type: message.type(), text: redactText(message.text(), [...secrets, otp]) });
    }
  });
  const recorder = createSanitizedNetworkRecorder(page, { directHost, relayHost });
  return {
    kind: 'browser', consoles, recorder,
    get observedRepoSha() { return verifiedRepoSha; },
    device: { platform: 'browser', browser_name: 'Chromium', browser_version: browser.version(), viewport },
    setOtpSecret(value) { otp = value; },
    async openInvite() {
      await page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 30_000 });
      if (new URL(page.url()).origin !== target.origin) throw new Error('navigation_left_allowed_origin');
    },
    async verifyReleaseIdentity() {
      verifiedRepoSha = await observedRepoSha(target, expectedRepoSha, fetch, expectedFaultProfile);
      return verifiedRepoSha;
    },
    async waitForInstallStage() { await page.locator('[data-intake-stage="install"]:not([hidden])').waitFor({ timeout: 20_000 }); },
    async skipInstall() { await page.locator('[data-focus-install-skip]').click(); },
    async openEmailStep() { await page.locator('[data-focus-email-open]').click(); },
    async focusEmailInput() { await page.locator('#focus-email').focus(); return null; },
    async enterEmail(value) { await page.locator('#focus-email').fill(value); },
    async requestOtpWithCompetingGestures() {
      await Promise.allSettled([
        page.locator('[data-focus-email-send]').click({ noWaitAfter: true }),
        page.locator('#focus-email').press('Enter'),
      ]);
    },
    async waitForCodeStep() { await page.locator('[data-focus-email-code-step]:not([hidden])').waitFor({ timeout: 25_000 }); },
    async focusOtpInput() { await page.locator('#focus-email-otp').focus(); return null; },
    async enterOtpDigitByDigit(value) { otp = value; await page.locator('#focus-email-otp').pressSequentially(value, { delay: 45 }); },
    async waitForMembershipConfirmed() {
      await page.locator('[data-focus-done-title]').filter({ hasText: 'Участие подтверждено' }).waitFor({ timeout: 30_000 });
    },
    async requestCounts() {
      return {
        issue: recorder.count('POST', '/auth/v1/otp'), verify: recorder.count('POST', '/auth/v1/verify'),
        registration: recorder.count('POST', '/rpc/register_focus_group_participant_v1'),
        registrationStatus: recorder.statuses('/rpc/register_focus_group_participant_v1').at(-1) ?? null,
      };
    },
    async reloadOrReopen() {
      await syncTransportFaultEvidence(); await page.reload({ waitUntil: 'domcontentloaded', timeout: 30_000 });
      faultPageEventsSeen = 0; transportPageEventsSeen = 0;
    },
    async transportFaultEvidence() { return syncTransportFaultEvidence(); },
    async waitForReturningMember() {
      await page.locator('[data-focus-done-title]').filter({ hasText: /Вы уже в фокус-группе|Участие подтверждено/u }).waitFor({ timeout: 20_000 });
    },
    async captureMaskedEvidence(name) { await maskPage(page); await page.screenshot({ path: join(evidenceRoot, 'screenshots', `${name}.png`), fullPage: true }); },
    async close() { await context.close(); await browser.close(); },
  };
}
