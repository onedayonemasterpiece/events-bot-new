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
      const normalized = String(body.p_email || '');
      const previous = stored.get(normalized) || 0;
      stored.set(normalized, previous + 1);

      if (behavior[`${routeName}Registration`] === 'down') {
        await route.abort('failed');
        return;
      }
      if (behavior[`${routeName}Registration`] === 'commit-then-500') {
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

    const email = page.locator('input[name="email"]');
    const consent = page.locator('input[name="consent"]');
    const submit = page.locator('[data-prelaunch-submit]');

    if (behavior.invalidInputs) {
      for (const value of behavior.invalidInputs) {
        await email.fill(value);
        await consent.check();
        const before = calls.length;
        await submit.click();
        await page.waitForTimeout(80);
        assert(calls.length === before, `${name}: invalid input reached transport: ${value}`);
        assert(await email.evaluate((node) => node === document.activeElement), `${name}: invalid input did not retain focus`);
        const status = await page.locator('[data-prelaunch-status]').textContent();
        assert(status?.includes('Проверьте адрес'), `${name}: invalid input has no safe error message`);
      }
    } else {
      await email.fill(behavior.email || ' User.Name+Launch@Example.COM ');
      if (behavior.consent === false) {
        const before = calls.length;
        await submit.click();
        await page.waitForTimeout(80);
        assert(calls.length === before, `${name}: missing consent reached transport`);
      } else {
        await consent.check();
        await submit.click();
        if (behavior.expectSuccess) {
          await page.waitForFunction(() => (
            document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'success'
          ));
        } else {
          await page.waitForFunction(() => (
            document.querySelector('[data-prelaunch-form]')?.getAttribute('data-experience-state') === 'error'
          ));
          assert(await email.inputValue() === (behavior.email || ' User.Name+Launch@Example.COM '), `${name}: failed transport erased email`);
        }
      }
    }

    if (behavior.assertions) {
      await behavior.assertions({ calls, stored, directRegistrationAttempts, relayRegistrationAttempts, page });
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

  await openScenario('direct-success', {
    expectSuccess: true,
    assertions: async ({ calls, stored, directRegistrationAttempts, relayRegistrationAttempts }) => {
      const registration = calls.find((call) => call.path.endsWith('/rpc/register_prelaunch_notification_v1'));
      assert(registration?.route === 'direct', 'direct-success: direct route was not used');
      assert(registration.body.p_email === 'user.name+launch@example.com', 'direct-success: email was not normalized');
      assert(registration.body.p_consent_version === 'prelaunch-updates-2026-v1', 'direct-success: consent version drift');
      assert(stored.size === 1, 'direct-success: expected one stored identity');
      assert(directRegistrationAttempts === 1 && relayRegistrationAttempts === 0, 'direct-success: unexpected replay');
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

  await openScenario('both-routes-down', {
    directProbe: 'down',
    relayProbe: 'down',
    expectSuccess: false,
    email: 'keep-me@example.com',
    assertions: async ({ stored }) => assert(stored.size === 0, 'both-routes-down: data was stored unexpectedly'),
  });
} finally {
  await browser.close();
}

const summary = {
  schema_version: 'prelaunch_form_security_v1',
  ok: failures.length === 0,
  url,
  evidence,
  failures,
};
writeFileSync(resolve(artifactDir, 'prelaunch-form-security-summary.json'), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
if (failures.length) throw new Error(failures.join('\n'));
