import type { APIRoute } from 'astro';
import { absoluteUrl, eventPath, getEvents } from '../lib/events';

function normalizeLastmod(value: string | null | undefined, fallback: string): string {
  if (!value) return fallback;
  const normalized = value.includes('T') ? value : `${value.replace(' ', 'T')}Z`;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? fallback : date.toISOString();
}

export const GET: APIRoute = () => {
  const now = new Date().toISOString();
  const entries = [
    { loc: absoluteUrl('/__preview/'), lastmod: now },
    { loc: absoluteUrl('/segodnya/'), lastmod: now },
    { loc: absoluteUrl('/vyhodnye/'), lastmod: now },
    ...getEvents().map((event) => ({ loc: absoluteUrl(eventPath(event)), lastmod: normalizeLastmod(event.updated_at, now) })),
  ];
  const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${entries.map((entry) => `  <url><loc>${entry.loc}</loc><lastmod>${entry.lastmod}</lastmod></url>`).join('\n')}\n</urlset>\n`;
  return new Response(xml, { headers: { 'content-type': 'application/xml; charset=utf-8' } });
};
