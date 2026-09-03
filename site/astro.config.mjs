// @ts-check
import { defineConfig } from 'astro/config';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { selectedTransportFaultProfile } from './scripts/transport-fault-build-contract.mjs';
import {
  normalizeStaticSitePageClasses,
  staticSitePageClassFilterIntegration,
} from './scripts/page-class-build-filter.mjs';

const siteOrigin = (process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
const basePath = process.env.SITE_BASE_PATH || '/';
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '').replace(/\/+$/u, '') || undefined;
const siteDir = dirname(fileURLToPath(import.meta.url));
const faultProfile = selectedTransportFaultProfile(process.env);
const faultAdapter = resolve(
  siteDir,
  faultProfile.enabled ? 'src/lib/transportFaultInjector.e2e.ts' : 'src/lib/transportFaultInjector.ts',
);
const pageClasses = normalizeStaticSitePageClasses(process.env.STATIC_SITE_PAGE_CLASSES || 'all');
const pageClassFilter = staticSitePageClassFilterIntegration(pageClasses);

export default defineConfig({
  site: siteOrigin,
  base: basePath,
  output: 'static',
  trailingSlash: 'always',
  integrations: pageClassFilter ? [pageClassFilter] : [],
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
