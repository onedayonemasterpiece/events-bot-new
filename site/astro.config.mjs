// @ts-check
import { defineConfig } from 'astro/config';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { selectedTransportFaultProfile } from './scripts/transport-fault-build-contract.mjs';

const siteOrigin = (process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
const basePath = process.env.SITE_BASE_PATH || '/';
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '').replace(/\/+$/u, '') || undefined;
const siteDir = dirname(fileURLToPath(import.meta.url));
const faultProfile = selectedTransportFaultProfile(process.env);
const faultAdapter = resolve(
  siteDir,
  faultProfile.enabled ? 'src/lib/transportFaultInjector.e2e.ts' : 'src/lib/transportFaultInjector.ts',
);

export default defineConfig({
  site: siteOrigin,
  base: basePath,
  output: 'static',
  trailingSlash: 'always',
  build: {
    assets: '_astro',
    assetsPrefix: astroAssetBaseUrl,
  },
  vite: {
    resolve: {
      alias: [
        { find: './transportFaultInjector.ts', replacement: faultAdapter },
      ],
    },
    define: faultProfile.enabled ? {
      __KENIGEVENTS_TRANSPORT_FAULT_PROFILE__: JSON.stringify(faultProfile),
    } : {},
    server: {
      allowedHosts: true,
    },
  },
});
