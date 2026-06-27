import { buildEventDetailRelatedManifest, getEventById, getEvents } from '../../../lib/events';

export function getStaticPaths() {
  return getEvents().map((event) => ({ params: { eventId: String(event.id) } }));
}

export function GET({ params }: { params: { eventId?: string } }) {
  const eventId = Number(params.eventId || 0);
  const event = getEventById(eventId);
  if (!event) {
    return new Response(JSON.stringify({ error: 'not_found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json; charset=utf-8' },
    });
  }
  const manifest = buildEventDetailRelatedManifest(event, 30);
  return new Response(
    JSON.stringify(manifest, null, 0),
    {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'public, max-age=300, stale-while-revalidate=3600',
      },
    },
  );
}
