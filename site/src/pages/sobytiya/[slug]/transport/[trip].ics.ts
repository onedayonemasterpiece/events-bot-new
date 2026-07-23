import type { APIRoute } from 'astro';
import { buildTransportIcs, transportIcsDownloadFilename } from '../../../../lib/ics';
import { desktopEventWithExplicitEnd } from '../../../../lib/desktopEventTransport';
import {
  eventTransportCalendarEntries,
  eventTransportTripKey,
  getEventTransportSuggestion,
  type EventTrainOption,
  type EventTransportDirection,
  type EventTransportSuggestion,
} from '../../../../lib/eventTransport';
import { getEvents } from '../../../../lib/events';
import type { PreviewEvent } from '../../../../lib/types';

interface TransportIcsProps {
  event: PreviewEvent;
  suggestion: EventTransportSuggestion;
  direction: EventTransportDirection;
  train: EventTrainOption;
}

export function getStaticPaths() {
  return getEvents().flatMap((event) => {
    // Responsive event pages share one persisted Smart Update duration
    // projection, so generate exactly the trips users can actually select.
    // Keeping the raw fallback union would leave an unlinked stale ICS file.
    const transportEvent = desktopEventWithExplicitEnd(event);
    const suggestion = getEventTransportSuggestion(transportEvent);
    if (!suggestion) return [];
    return eventTransportCalendarEntries(suggestion).map(({ direction, train }) =>
      pathForTrip(transportEvent, suggestion, direction, train));
  });
}

function pathForTrip(
  event: PreviewEvent,
  suggestion: EventTransportSuggestion,
  direction: EventTransportDirection,
  train: EventTrainOption,
) {
  return {
    params: { slug: event.slug, trip: eventTransportTripKey(suggestion, direction, train) },
    props: { event, suggestion, direction, train } satisfies TransportIcsProps,
  };
}

export const GET: APIRoute = ({ props }) => {
  const { event, suggestion, direction, train } = props as TransportIcsProps;
  const trip = eventTransportTripKey(suggestion, direction, train);
  return new Response(buildTransportIcs(event, suggestion, direction, train), {
    headers: {
      'content-type': 'text/calendar; charset=utf-8',
      'content-disposition': `inline; filename="${transportIcsDownloadFilename(event, trip)}"`,
      'cache-control': 'public, max-age=300',
    },
  });
};
