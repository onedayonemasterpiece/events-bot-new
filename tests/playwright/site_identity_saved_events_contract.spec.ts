import { expect, test } from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';

const controllerJs = fs.readFileSync(path.resolve(process.cwd(), 'site/src/lib/site-identity.js'), 'utf8');
const families = ['/segodnya/', '/vystavki/', '/sobytiya/test-occurrence/', '/poisk/'];

async function openHarness(page: any, pagePath: string) {
  await page.route('https://kenigevents.test/assets/site-identity.js', (route: any) => route.fulfill({ status: 200, contentType: 'application/javascript', body: controllerJs }));
  await page.route(`https://kenigevents.test${pagePath}`, (route: any) => route.fulfill({
    status: 200,
    contentType: 'text/html; charset=utf-8',
    body: `<!doctype html><meta charset="utf-8"><output id="state"></output><script type="module">
      import { createSiteIdentityController } from '/assets/site-identity.js';
      let count = 2;
      const user = { id: 'account-a', email: 'person@example.test' };
      const session = { access_token: 'fixture-token', refresh_token: 'fixture-refresh', user };
      const supabase = {
        auth: {
          getSession: async () => ({ data: { session } }),
          onAuthStateChange: () => ({ data: { subscription: { unsubscribe() {} } } }),
          signOut: async () => ({}),
        },
        rpc: async (name, args) => {
          if (name === 'personalization_saved_count_v1') return { data: count, error: null };
          if (name === 'personalization_save_occurrence_v1') {
            count = args.p_saved ? 2 : 1;
            return { data: [{ saved: args.p_saved, unique_saved_event_count: count, lifecycle_status: 'upcoming' }], error: null };
          }
          return { data: true, error: null };
        },
      };
      const controller = createSiteIdentityController({ supabase, window });
      window.identityHarness = controller;
      controller.subscribe((state) => document.querySelector('#state').textContent = JSON.stringify({ status: state.status, user: state.user?.id, count: state.savedCount }));
      await controller.init();
      await controller.refreshSavedCount();
    </script>`,
  }));
  await page.goto(`https://kenigevents.test${pagePath}`);
}

for (const family of families) {
  test(`restores the same account/count contract on ${family}`, async ({ page }) => {
    await openHarness(page, family);
    await expect.poll(async () => JSON.parse(await page.locator('#state').textContent() || '{}')).toEqual({ status: 'authenticated', user: 'account-a', count: 2 });
  });
}

test('repeat save and undo update the unique count without layout coupling', async ({ page }) => {
  await openHarness(page, families[0]);
  const result = await page.evaluate(async () => {
    const api = (window as any).identityHarness;
    const first = await api.saveOccurrence({ eventId: 42, occurrenceKey: '42@2026-08-01' });
    const repeat = await api.saveOccurrence({ eventId: 42, occurrenceKey: '42@2026-08-01' });
    const undo = await api.saveOccurrence({ eventId: 42, occurrenceKey: '42@2026-08-01', saved: false });
    return [first.unique_saved_event_count, repeat.unique_saved_event_count, undo.unique_saved_event_count];
  });
  expect(result).toEqual([2, 2, 1]);
});
