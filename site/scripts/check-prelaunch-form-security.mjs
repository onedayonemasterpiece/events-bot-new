import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

const url = option('--url');
const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-form-security'));
if (!url || !/^https?:\/\//u.test(url)) {
  throw new Error('Usage: check-prelaunch-form-security.mjs --url <url> [--artifact-dir <path>]');
}
mkdirSync(artifactDir, { recursive: true });

const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
const browser = await chromium.launch({ headless: true, executablePath });
const evidence = [];
const failures = [];

async function openScenario(name, behavior) {
  const context = await browser.newContext({
    viewport: { width: 390, height: 844 },
    reducedMotion: 'reduce',
    colorScheme: 'dark',
  });
  if (behavior.registrationHint) {
    await context.addInitScript(() => {
      window.localStorage.setItem('ke_prelaunch_notification_v1', 'registered');
    });
  }

  const calls = [];
  const stored = new Map();
  let directRegistrationAttempts = 0;
  let relayRegistrationAttempts = 0;

  const handle = (routeName) => async (route) => {
    const request = route.request();
    const parsed = new URL(request.url());
    const path = parsed.pathname;
    const body = request.postDataJSON?.() || {};
    calls.push({ route: routeName, path, method: request.method(), body });

    if (path.endsWith('/rpc/transport_probe_v1')) {
      if (behavior[`${routeName}Probe`] === 'down') {
        await route.abort('failed');
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ nonce: body.p_nonce, schema: 1 }),
      });
      return;
    }

    if (path.endsWith('/rpc/register_prelaunch_notification_v1')) {
      if (routeName === 'direct') directRegistrationAttempts += 1;
      else relayRegistrationAttempts += 1;

      const routeBehavior = behavior[`${routeName}Registration`];
      if (routeBehavior === 'down') {
        await route.abort('failed');
        return;
      }
      if (routeBehavior === 'capacity') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            accepted: false,
            status: 'daily_capacity_reached',
            launch_date: '2026-09-01',
            consent_version: 'prelaunch-updates-2026-v1',
          }),
        });
        return;
      }
      if (routeBehavior === 'rejected') {
        await route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            accepted: false,
            status: 'rejected',
            launch_date: '2026-09-01',
            consent_version: 'prelaunch-updates-2026-v1',
          }),
        });
        return;
      }

      const normalized = String(body.p_email || '');
      const previous = stored.get(normalized) || 0;
      stored.set(normalized, previous + 1);

      if (routeBehavior === 'delayed-success') {
        await new Promise((resolveDelay) => setTimeout(resolveDelay, 350));
      }
      if (routeBehavior === 'commit-then-500') {
        await route.fulfill({
          status: 500,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'simulated_lost_response' }),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          accepted: true,
          status: 'registered',
          launch_date: '2026-09-01',
          consent_version: 'prelaunch-updates-2026-v1',
        }),
      });
      return;
    }

    await route.fulfill({ status: 404, contentType: 'application/json', body: '{}' });
  };

  try {
    const page = await context.newPage();
    await page.route('https://direct.test/**', handle('direct'));
    await page.route('https://relay.test/**', handle('relay'));
    const response = await page.goto(url, { waitUntil: 'networkidle' });
    assert(response?.ok(), `${name}: local page HTTP ${response?.status()}`);
    await page.waitForFunction(() => {
      const form = document.querySelector('[data-prelaunch-form]');
      return form?.getAttribute('data-email-guard-ready') === 'true'
        && form?.getAttribute('data-experience-bound') === 'true';
    });

    const form = page.locator('[data-prelaunch-form]');
    const email = page.locator('input[name="email"]');
    const consent = page.locator('input[name="consent"]');
    const submit = page.locator('[data-prelaunch-submit]');
    const submitLabel = page.locator('[data-prelaunch-submit-label]');
    const status = page.locator('[data-prelaunch-status]');
    const complete = page.locator('[data-prelaunch-complete]');
    const reset = page.locator('[data-prelaunch-reset]');

    if (behavior.registrationHint) {
      assert(await form.getAttribute('data-experience-state') === 'registered', `${name}: stored hint did not restore registered state`);
      assert(await complete.isVisible(), `${name}: registered completion surface is hidden`);
      assert(!(await page.locator('.prelaunch-form__row').isVisible()), `${name}: registered form row remained visible`);
      await reset.click();
      assert(await form.getAttribute('data-experience-state') === 'idle', `${name}: reset did not restore idle state`);
      assert(await email.evaluate((node) => node === document.activeElement), `${name}: reset did not focus email`);
      assert(await page.evaluate(() => window.localStorage.getItem('ke_prelaunch_notification_v1')) === null, `${name}: reset left registration hint behind`);
      assert(calls.length === 0, `${name}: local registered/reset state reached transport`);
    } else if (behavior.invalidInputs) {
      for (const value of behavior.invalidInputs) {
        await email.fill(value);
        await consent.check();
        const before = calls.length;
        await submit.click();
        await page.waitForTimeout(80);
        assert(calls.length === before, `${name}: invalid input reached transport: ${value}`);
        assert(await email.evaluate((node) => node === document.activeElement), `${name}: invalid input did not retain focus`);
        assert((await status.textContent())?.includes('Проверьте адрес'), `${name}: invalid input has no safe error message`);
        assert(await form.getAttribute('data-experience-state') === 'error', `${name}: invalid input did not enter error state`);
      }
    } else {
      const submittedEmail = behavior.email || ' User.Name+Launch@Example.COM ';
      await email.fill(submittedEmail);
      if (behavior.consent === false) {
        const before = calls.length;
        await submit.click();
        await page.waitForTimeout(80);
        assert(calls.length === before, `${name}: missing consent reached transport`);
        assert((await status.textContent())?.includes('Подтвердите согласие'), `${name}: missing consent has no explicit message`);
        assert(await consent.evaluate((node) => node === document.activeElement), `${name}: missing consent did not focus checkbox`);
      } else {
        await consent.check();
        await submit.click();

        if (behavior.assertSubmitting) {
          await page.waitForFunction(() => (
            document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'submitting'
          ));
          assert(await submit.isDisabled(), `${name}: submit button stayed enabled while saving`);
          assert(await submit.getAttribute('aria-busy') === 'true', `${name}: aria-busy was not announced`);
          assert((await submitLabel.textContent()) === 'Сохраняем…', `${name}: submitting label is incorrect`);
        }

        if (behavior.expectSuccess) {
          await page.waitForFunction(() => (
            document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'success'
          ));
          assert(await complete.isVisible(), `${name}: success completion surface is hidden`);
          assert(!(await page.locator('.prelaunch-form__row').isVisible()), `${name}: form row remained visible after success`);
          assert(await page.evaluate(() => window.localStorage.getItem('ke_prelaunch_notification_v1')) === 'registered', `${name}: success hint was not persisted`);
        } else {
          await page.waitForFunction(() => (
            document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'error'
          ));
          assert(await email.inputValue() === submittedEmail.trim().toLowerCase(), `${name}: failed transport erased or denormalized email`);
          if (behavior.expectedStatusText) {
            assert((await status.textContent())?.includes(behavior.expectedStatusText), `${name}: unexpected error copy`);
          }
        }
      }
    }

    if (behavior.assertions) {
      await behavior.assertions({
        calls,
        stored,
        directRegistrationAttempts,
        relayRegistrationAttempts,
        page,
        form,
        email,
        consent,
        submit,
        status,
        complete,
      });
    }

    const screenshotPath = resolve(artifactDir, `${name}.png`);
    await page.screenshot({ path: screenshotPath, fullPage: false, animations: 'disabled' });
    const result = {
      name,
      ok: true,
      calls,
      stored: [...stored.entries()],
      directRegistrationAttempts,
      relayRegistrationAttempts,
      experienceState: await form.getAttribute('data-experience-state'),
      screenshotPath,
    };
    evidence.push(result);
    writeFileSync(resolve(artifactDir, `${name}.json`), `${JSON.stringify(result, null, 2)}\n`);
  } catch (error) {
    const message = `${name}: ${String(error?.stack || error)}`;
    failures.push(message);
    evidence.push({ name, ok: false, error: message, calls, stored: [...stored.entries()] });
  } finally {
    await context.close();
  }
}

try {
  await openScenario('invalid-inputs', {
    invalidInputs: [
      '<script>@example.com',
      'person\\payload@example.com',
      "x');drop-table@example.com",
      'person@@example.com',
      'person@example..com',
    ],
  });

  await openScenario('missing-consent', {
    consent: false,
    assertions: async ({ calls }) => assert(calls.length === 0, 'missing-consent: transport was called'),
  });

  await openScenario('registered-hint-reset', {
    registrationHint: true,
  });

  await openScenario('direct-success', {
    expectSuccess: true,
    assertions: async ({ calls, stored, directRegistrationAttempts, relayRegistrationAttempts, page, form }) => {
      const registration = calls.find((call) => call.path.endsWith('/rpc/register_prelaunch_notification_v1'));
      assert(registration?.route === 'direct', 'direct-success: direct route was not used');
      assert(registration.body.p_email === 'user.name+launch@example.com', 'direct-success: email was not normalized');
      assert(registration.body.p_consent_version === 'prelaunch-updates-2026-v1', 'direct-success: consent version drift');
      assert(stored.size === 1, 'direct-success: expected one stored identity');
      assert(directRegistrationAttempts === 1 && relayRegistrationAttempts === 0, 'direct-success: unexpected replay');
      await page.reload({ waitUntil: 'networkidle' });
      await page.waitForFunction(() => (
        document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'registered'
      ));
      assert(await form.getAttribute('data-experience-state') === 'registered', 'direct-success: reload did not restore registered state');
    },
  });

  await openScenario('submitting-single-request', {
    directRegistration: 'delayed-success',
    assertSubmitting: true,
    expectSuccess: true,
    assertions: async ({ directRegistrationAttempts, relayRegistrationAttempts }) => {
      assert(directRegistrationAttempts === 1, 'submitting-single-request: duplicate direct write');
      assert(relayRegistrationAttempts === 0, 'submitting-single-request: unexpected relay write');
    },
  });

  await openScenario('direct-down-relay-success', {
    directProbe: 'down',
    expectSuccess: true,
    assertions: async ({ calls, stored, relayRegistrationAttempts }) => {
      const registration = calls.find((call) => call.path.endsWith('/rpc/register_prelaunch_notification_v1'));
      assert(registration?.route === 'relay', 'direct-down-relay-success: relay route was not selected');
      assert(stored.size === 1 && relayRegistrationAttempts === 1, 'direct-down-relay-success: registration did not converge');
    },
  });

  await openScenario('lost-direct-response-idempotent-replay', {
    directRegistration: 'commit-then-500',
    expectSuccess: true,
    assertions: async ({ stored, directRegistrationAttempts, relayRegistrationAttempts }) => {
      assert(stored.size === 1, 'lost-direct-response: replay created more than one identity');
      assert(directRegistrationAttempts === 1 && relayRegistrationAttempts === 1, 'lost-direct-response: alternate replay did not occur');
      assert([...stored.values()][0] === 2, 'lost-direct-response: both attempts did not hit the same normalized key');
    },
  });

  await openScenario('capacity-reached', {
    directRegistration: 'capacity',
    expectSuccess: false,
    email: 'capacity@example.com',
    expectedStatusText: 'слишком много запросов',
    assertions: async ({ stored }) => assert(stored.size === 0, 'capacity-reached: rejected address was stored'),
  });

  await openScenario('backend-rejected', {
    directRegistration: 'rejected',
    expectSuccess: false,
    email: 'rejected@example.com',
    expectedStatusText: 'Не удалось сохранить email',
    assertions: async ({ stored }) => assert(stored.size === 0, 'backend-rejected: rejected address was stored'),
  });

  await openScenario('both-routes-down', {
    directProbe: 'down',
    relayProbe: 'down',
    expectSuccess: false,
    email: 'keep-me@example.com',
    expectedStatusText: 'Не удалось сохранить email',
    assertions: async ({ stored }) => assert(stored.size === 0, 'both-routes-down: data was stored unexpectedly'),
  });
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_form_security_v2',
  ok: failures.length === 0,
  url,
  coveredStates: [
    'idle',
    'invalid-email',
    'missing-consent',
    'submitting',
    'success',
    'registered-after-reload',
    'reset-to-idle',
    'capacity-error',
    'backend-rejection-error',
    'both-routes-down-error',
    'direct-to-relay-fallback',
    'idempotent-replay-after-lost-response',
  ],
  evidence,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-form-security-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length) throw new Error(failures.join('\n'));
