// @ts-check
import { defineConfig } from 'astro/config';

const siteOrigin = (process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
const basePath = process.env.SITE_BASE_PATH || '/';
const astroAssetBaseUrl = (process.env.PUBLIC_ASTRO_ASSET_BASE_URL || '').replace(/\/+$/u, '') || undefined;

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
    server: {
      allowedHosts: true,
    },
  },
});
