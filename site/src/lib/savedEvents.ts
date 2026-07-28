import type { SupabaseClient } from '@supabase/supabase-js';

export interface DurableSavedEventRow {
  event_id: number;
  calendar_saved: boolean;
  favorite_saved: boolean;
  calendar_added_at: string | null;
  favorite_added_at: string | null;
  source_priority: number;
  sort_at: string | null;
}

export async function listDurableSavedEvents(client: SupabaseClient): Promise<DurableSavedEventRow[]> {
  const { data, error } = await client
    .from('my_saved_events_v1')
    .select('event_id,calendar_saved,favorite_saved,calendar_added_at,favorite_added_at,source_priority,sort_at')
    .order('source_priority', { ascending: true })
    .order('sort_at', { ascending: false, nullsFirst: false })
    .order('event_id', { ascending: true });
  if (error) throw error;
  return Array.isArray(data) ? data as DurableSavedEventRow[] : [];
}

export async function setDurableSavedEvent(
  client: SupabaseClient,
  eventId: number,
  source: 'calendar' | 'favorite',
  saved = true,
) {
  if (!Number.isSafeInteger(eventId) || eventId <= 0) throw new Error('invalid_saved_event_id');
  const { data, error } = await client.rpc('set_saved_event_state_v1', {
    p_event_id: eventId,
    p_source: source,
    p_saved: Boolean(saved),
  });
  if (error) throw error;
  return data;
}
