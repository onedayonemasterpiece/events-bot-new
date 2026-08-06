import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

const root = resolve('artifacts/prelaunch-ci-evidence');
rmSync(root, { recursive: true, force: true });
mkdirSync(root, { recursive: true });

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: 'inherit',
    env: process.env,
    ...options,
  });
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} exited ${result.status}`);
}

function emitFile(name, path) {
  const bytes = readFileSync(path);
  const digest = createHash('sha256').update(bytes).digest('hex');
  const encoded = bytes.toString('base64');
  const width = 72;
  const chunks = Math.ceil(encoded.length / width);
  console.log(`PRELAUNCH_EVIDENCE_BEGIN ${name} ${digest} ${bytes.length} ${chunks}`);
  for (let offset = 0; offset < encoded.length; offset += width) {
    console.log(`PRELAUNCH_EVIDENCE_DATA ${name} ${encoded.slice(offset, offset + width)}`);
  }
  console.log(`PRELAUNCH_EVIDENCE_END ${name}`);
}

const buildEnv = {
  ...process.env,
  PUBLIC_PRELAUNCH_MODE: 'on',
  PUBLIC_PERSONALIZATION_SUPABASE_URL: 'https://direct.test',
  PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL: 'https://relay.test',
  PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY: 'publishable-test-key',
};
run(process.execPath, ['node_modules/astro/astro.js', 'build'], { env: buildEnv });

const serverLog = resolve(root, 'http.log');
const server = spawn('python3', ['-m', 'http.server', '4173', '--directory', 'dist'], {
  stdio: ['ignore', 'pipe', 'pipe'],
});
let log = '';
server.stdout.on('data', (chunk) => { log += chunk.toString(); });
server.stderr.on('data', (chunk) => { log += chunk.toString(); });

async function waitForServer() {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      const response = await fetch('http://127.0.0.1:4173/');
      if (response.ok) return;
    } catch {
      // retry
    }
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
  }
  throw new Error(`prelaunch server did not start\n${log}`);
}

const failures = [];
try {
  await waitForServer();

  for (const [script, args] of [
    ['scripts/check-prelaunch-v27-structure.mjs', ['--url', 'http://127.0.0.1:4173/', '--artifact-dir', root]],
    ['scripts/check-prelaunch-form-security.mjs', ['--url', 'http://127.0.0.1:4173/', '--artifact-dir', resolve(root, 'form-security')]],
    ['scripts/check-prelaunch-viewport-fit.mjs', ['--url', 'http://127.0.0.1:4173/', '--artifact-dir', root]],
  ]) {
    const result = spawnSync(process.execPath, [script, ...args], { stdio: 'inherit', env: buildEnv });
    if (result.status !== 0) failures.push(`${script}:${result.status}`);
  }

  const executablePath = String(process.env.PRELAUNCH_CHROMIUM_EXECUTABLE_PATH || '').trim() || undefined;
  const browser = await chromium.launch({ headless: true, executablePath });
  try {
    const viewports = [
      ['square-1200x1200', 1200, 1200],
      ['wide-1728x900', 1728, 900],
      ['mobile-390x844', 390, 844],
      ['mobile-small-320x568', 320, 568],
    ];
    for (const [name, width, height] of viewports) {
      const context = await browser.newContext({
        viewport: { width, height },
        reducedMotion: 'reduce',
        colorScheme: 'dark',
      });
      try {
        const page = await context.newPage();
        const response = await page.goto('http://127.0.0.1:4173/', { waitUntil: 'networkidle' });
        if (!response?.ok()) throw new Error(`${name}: HTTP ${response?.status()}`);
        await page.waitForFunction(() => {
          const rootNode = document.querySelector('[data-prelaunch-page]');
          return rootNode?.getAttribute('data-artwork-ready') === 'true'
            && rootNode?.getAttribute('data-tile-pool-count') === '98';
        }, undefined, { timeout: 10_000 });
        await page.waitForTimeout(350);
        const output = resolve(root, `${name}.jpg`);
        await page.screenshot({
          path: output,
          type: 'jpeg',
          quality: 72,
          fullPage: false,
          animations: 'disabled',
        });
        emitFile(name, output);
      } finally {
        await context.close();
      }
    }
  } finally {
    await browser.close();
  }

  for (const path of [
    resolve(root, 'prelaunch-v27-structure-summary.json'),
    resolve(root, 'form-security/prelaunch-form-security-summary.json'),
    resolve(root, 'prelaunch-viewport-fit-summary.json'),
  ]) {
    if (!readFileSync(path, 'utf8').trim()) failures.push(`empty:${path}`);
    emitFile(path.split('/').at(-1), path);
  }
} finally {
  server.kill('SIGTERM');
  writeFileSync(serverLog, log);
}

if (failures.length) {
  throw new Error(`prelaunch CI evidence failures: ${failures.join(', ')}`);
}
