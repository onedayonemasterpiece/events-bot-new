import type { APIRoute } from 'astro';
import { HERO_REVIEW_CASES } from '../lib/heroReview';
import { absoluteUrl, eventPath, getEvents } from '../lib/events';
import { getInterestClubs, interestClubPath, INTEREST_CLUBS_PUBLIC_ENABLED } from '../lib/clubs';

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
    { loc: absoluteUrl('/zavtra/'), lastmod: now },
    { loc: absoluteUrl('/vyhodnye/'), lastmod: now },
    { loc: absoluteUrl('/vystavki/'), lastmod: now },
    { loc: absoluteUrl('/populyarnoe/'), lastmod: now },
    { loc: absoluteUrl('/poisk/'), lastmod: now },
    { loc: absoluteUrl('/partnerstvo/'), lastmod: now },
    { loc: absoluteUrl('/partners/'), lastmod: now },
    ...(INTEREST_CLUBS_PUBLIC_ENABLED ? [{ loc: absoluteUrl('/kluby-po-interesam/'), lastmod: now }] : []),
    { loc: absoluteUrl('/lab/hero/'), lastmod: now },
    { loc: absoluteUrl('/lab/hero/review/'), lastmod: now },
    { loc: absoluteUrl('/lab/design-system/'), lastmod: now },
    ...HERO_REVIEW_CASES.map((item) => ({ loc: absoluteUrl(`/lab/hero/review/${item.caseId}/`), lastmod: now })),
    ...getInterestClubs().map((club) => ({ loc: absoluteUrl(interestClubPath(club)), lastmod: normalizeLastmod(club.updated_at, now) })),
    ...getEvents().map((event) => ({ loc: absoluteUrl(eventPath(event)), lastmod: normalizeLastmod(event.updated_at, now) })),
  ];
  const xml = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${entries.map((entry) => `  <url><loc>${entry.loc}</loc><lastmod>${entry.lastmod}</lastmod></url>`).join('\n')}\n</urlset>\n`;
  return new Response(xml, { headers: { 'content-type': 'application/xml; charset=utf-8' } });
};
