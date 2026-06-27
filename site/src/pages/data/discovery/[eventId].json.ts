import { getEventById, getEvents, getPreloadedDiscoveryEvents, toDiscoveryEventPayload } from '../../../lib/events';

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
  const events = getPreloadedDiscoveryEvents(event, 30).map(toDiscoveryEventPayload);
  return new Response(
    JSON.stringify(
      {
        version: 1,
        event_id: event.id,
        strategy: 'static_seed_then_personalized_client_filter_v1',
        preload_target: 10,
        page_size: 10,
        events,
      },
      null,
      0,
    ),
    {
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'public, max-age=300, stale-while-revalidate=3600',
      },
    },
  );
}
