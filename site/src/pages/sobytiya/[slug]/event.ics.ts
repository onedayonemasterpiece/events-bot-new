import type { APIRoute } from 'astro';
import { buildIcs, eventIcsDownloadFilename } from '../../../lib/ics';
import { getEventBySlug, getEventDetailEvents } from '../../../lib/events';

export function getStaticPaths() {
  return getEventDetailEvents().map((event) => ({ params: { slug: event.slug } }));
}

export const GET: APIRoute = ({ params }) => {
  const event = getEventBySlug(params.slug || '');
  if (!event) {
    return new Response('Not found', { status: 404 });
  }
  return new Response(buildIcs(event), {
    headers: {
      'content-type': 'text/calendar; charset=utf-8',
      'content-disposition': `attachment; filename="${eventIcsDownloadFilename(event)}"`,
      'cache-control': 'public, max-age=300',
    },
  });
};
