import type { APIRoute } from 'astro';
import { IS_PRODUCTION, SITE_ORIGIN } from '../lib/events';

const body = IS_PRODUCTION
  ? `User-agent: *\nAllow: /\nSitemap: ${SITE_ORIGIN}/sitemap.xml\n`
  : 'User-agent: *\nDisallow: /\n';

export const GET: APIRoute = () => new Response(body, {
  headers: { 'content-type': 'text/plain; charset=utf-8' },
});
