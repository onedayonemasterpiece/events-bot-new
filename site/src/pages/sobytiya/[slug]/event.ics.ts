import type { APIRoute } from 'astro';
import { buildIcs } from '../../../lib/ics';
import { getEventBySlug, getEvents } from '../../../lib/events';

export function getStaticPaths() {
  return getEvents().map((event) => ({ params: { slug: event.slug } }));
}

export const GET: APIRoute = ({ params }) => {
  const event = getEventBySlug(params.slug || '');
  if (!event) {
    return new Response('Not found', { status: 404 });
  }
  return new Response(buildIcs(event), {
    headers: {
      'content-type': 'text/calendar; charset=utf-8',
      'content-disposition': `attachment; filename="kenigevents-${event.slug}.ics"`,
      'cache-control': 'public, max-age=300',
    },
  });
};
