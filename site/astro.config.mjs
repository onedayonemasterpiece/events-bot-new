// @ts-check
import { defineConfig } from 'astro/config';

const siteOrigin = (process.env.PUBLIC_SITE_ORIGIN || 'https://kenigevents.ru').replace(/\/+$/u, '');
const basePath = process.env.SITE_BASE_PATH || '/';

export default defineConfig({
  site: siteOrigin,
  base: basePath,
  output: 'static',
  trailingSlash: 'always',
  vite: {
    server: {
      allowedHosts: true,
    },
  },
});
