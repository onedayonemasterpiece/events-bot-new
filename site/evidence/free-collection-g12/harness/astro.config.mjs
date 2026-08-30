import { defineConfig } from 'astro/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const harnessRoot = path.dirname(fileURLToPath(import.meta.url));
const siteRoot = path.resolve(harnessRoot, '../../..');

export default defineConfig({
  root: harnessRoot,
  srcDir: path.join(harnessRoot, 'src'),
  publicDir: path.join(harnessRoot, 'public'),
  outDir: path.join(harnessRoot, '.dist'),
  vite: {
    resolve: {
      alias: {
        '@current-a': path.join(siteRoot, 'src'),
      },
    },
  },
});
