import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const artifactRoot = path.resolve(siteRoot, '..', 'artifacts', 'codex', 'l4-question-cta-visual');
mkdirSync(artifactRoot, { recursive:true });
const port = 43184;
const origin = `http://127.0.0.1:${port}`;
const server = spawn('python3', ['-m', 'http.server', String(port), '--bind', '127.0.0.1', '--directory', 'dist'], { cwd:siteRoot, stdio:'ignore' });
await new Promise((resolve) => setTimeout(resolve, 450));
const executablePath = [
  process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE,
  '/home/dev/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome',
  '/opt/ms-playwright/chromium-1223/chrome-linux64/chrome',
].find((candidate) => candidate && existsSync(candidate));
const browser = await chromium.launch({ headless:true, executablePath });

async function verify(viewport, fileName) {
  const page = await browser.newPage({ viewport });
  await page.goto(`${origin}/lab/question-cta/`, { waitUntil:'networkidle' });
  const ctas = page.locator('[data-event-question-cta]');
  assert.equal(await ctas.count(), 2);
  assert.deepEqual(await ctas.evaluateAll((nodes) => nodes.map((node) => node.dataset.questionCtaSource)), ['partner_post', 'managed_afisha_post']);
  for (const cta of await ctas.all()) {
    const link = cta.locator('[data-question-cta-link]');
    assert.equal(await link.getAttribute('data-question-cta-provider'), 'vk');
    assert.equal(await link.getAttribute('target'), '_blank');
    assert.match(await link.getAttribute('rel') || '', /nofollow/u);
    assert.equal(await cta.locator('img').count(), 2);
    assert.deepEqual(await cta.locator('img').evaluateAll((images) => images.map((image) => ({ complete:image.complete, width:image.naturalWidth }))), [
      { complete:true, width:800 },
      { complete:true, width:800 },
    ]);
  }
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth), `${fileName} has no horizontal overflow`);
  // The fixed production dock is outside this component's visual-review scope.
  await page.addStyleTag({ content:'.mobile-bottom-nav{display:none!important}' });
  await page.screenshot({ path:path.join(artifactRoot, fileName), fullPage:true });
  await ctas.nth(viewport.width < 640 ? 1 : 0).screenshot({ path:path.join(artifactRoot, fileName.replace('.png', '-cta.png')) });
  await page.close();
}

try {
  await verify({ width:1280, height:900 }, 'desktop.png');
  await verify({ width:390, height:844 }, 'mobile.png');
  process.stdout.write(`event question CTA browser acceptance: PASS\nartifacts=${path.relative(path.resolve(siteRoot, '..'), artifactRoot)}\n`);
} finally {
  await browser.close();
  server.kill('SIGTERM');
  await Promise.race([once(server, 'exit'), new Promise((resolve) => setTimeout(resolve, 1000))]);
}
