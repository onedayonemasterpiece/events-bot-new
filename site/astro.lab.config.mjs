import path from 'node:path';

const buildId = process.env.LAB_BUILD_ID || 'briefing-lab-local';
const siteDir = path.dirname(new URL(import.meta.url).pathname);

export default {
  site: process.env.PUBLIC_SITE_ORIGIN || 'http://127.0.0.1:4177',
  base: `/${buildId}`,
  srcDir: path.join(siteDir, 'src/lab-briefing'),
  publicDir: path.join(siteDir, 'src/lab-briefing/public'),
  outDir: path.join(siteDir, 'dist-lab', buildId),
  output: 'static',
  build: { format: 'directory' },
};
