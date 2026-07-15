import { createServer } from 'node:http';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const distRoot = path.join(siteDir, 'dist-lab');
const candidates = existsSync(distRoot) ? readdirSync(distRoot).filter((name) => existsSync(path.join(distRoot, name, 'lab-manifest.json'))) : [];
const buildId = process.env.PREVIEW_BUILD_ID || candidates.sort((a, b) => statSync(path.join(distRoot, b)).mtimeMs - statSync(path.join(distRoot, a)).mtimeMs)[0];
if (!buildId || !/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(buildId) || buildId.includes('..')) throw new Error('Build lab first or provide safe PREVIEW_BUILD_ID');
const host = process.env.LAB_PREVIEW_HOST || '127.0.0.1';
const port = Number(process.env.LAB_PREVIEW_PORT || 4177);
const mime = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8', '.svg': 'image/svg+xml' };
const server = createServer((request, response) => {
  const pathname = decodeURIComponent(new URL(request.url || '/', `http://${host}:${port}`).pathname);
  let relative = pathname.replace(/^\/+/, '');
  if (pathname === `/${buildId}` || pathname === `/${buildId}/`) relative = `${buildId}/lab/briefing/index.html`;
  else if (pathname.startsWith('/_astro/')) relative = `${buildId}${pathname}`;
  else if (pathname.endsWith('/')) relative += 'index.html';
  const target = path.resolve(distRoot, relative);
  if (!target.startsWith(`${path.resolve(distRoot)}${path.sep}`) || !existsSync(target) || !statSync(target).isFile()) {
    response.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); response.end('Not found'); return;
  }
  response.writeHead(200, { 'content-type': mime[path.extname(target)] || 'application/octet-stream', 'cache-control': 'no-store' });
  response.end(readFileSync(target));
});
server.listen(port, host, () => console.log(`Briefing lab: http://${host}:${port}/${buildId}/lab/briefing/`));
