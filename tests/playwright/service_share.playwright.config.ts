import path from 'node:path';
import { defineConfig } from '@playwright/test';

const here = __dirname;
const siteDir = path.resolve(here, '../../site');
const externalBaseUrl = process.env.SERVICE_SHARE_BASE_URL?.replace(/\/+$/u, '');

export default defineConfig({
  testDir: here,
  testMatch: 'service_share_contract.spec.ts',
  timeout: 30_000,
  expect: { timeout: 8_000 },
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: externalBaseUrl || 'http://127.0.0.1:4321',
    browserName: 'chromium',
    trace: 'retain-on-failure',
  },
  webServer: externalBaseUrl ? undefined : {
    command: 'npm run dev -- --port 4321',
    cwd: siteDir,
    url: 'http://127.0.0.1:4321/__preview/',
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
