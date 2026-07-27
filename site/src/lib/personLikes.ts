import type { SupabaseClient } from '@supabase/supabase-js';

export interface PersonLikeSnapshot {
  personId: string;
  likesCount: number;
  liked: boolean;
}

const PERSON_ID_PATTERN = /^[a-z0-9][a-z0-9:_-]{2,127}$/u;
const MAX_PERSON_IDS = 64;

export function publicPersonIds(values: Iterable<unknown>): string[] {
  return [...new Set(
    [...values]
      .map((value) => String(value || '').trim())
      .filter((value) => PERSON_ID_PATTERN.test(value)),
  )].slice(0, MAX_PERSON_IDS);
}

function snapshotRows(value: unknown): PersonLikeSnapshot[] {
  if (!Array.isArray(value)) return [];
  return value.flatMap((raw) => {
    if (!raw || typeof raw !== 'object') return [];
    const row = raw as Record<string, unknown>;
    const personId = String(row.person_id || '').trim();
    if (!PERSON_ID_PATTERN.test(personId)) return [];
    return [{
      personId,
      likesCount: Math.max(0, Math.floor(Number(row.likes_count) || 0)),
      liked: row.liked === true,
    }];
  });
}

export async function getPersonLikeSnapshot(
  client: SupabaseClient,
  personIds: Iterable<unknown>,
): Promise<PersonLikeSnapshot[]> {
  const ids = publicPersonIds(personIds);
  if (ids.length === 0) return [];
  const { data, error } = await client.rpc('get_person_like_snapshot_v1', {
    p_person_ids: ids,
  });
  if (error) throw error;
  return snapshotRows(data);
}

export async function setPersonLike(
  client: SupabaseClient,
  personId: string,
  liked: boolean,
): Promise<PersonLikeSnapshot> {
  const [id] = publicPersonIds([personId]);
  if (!id) throw new Error('invalid_person_id');
  const { data, error } = await client.rpc('set_person_like_v1', {
    p_person_id: id,
    p_liked: liked,
  });
  if (error) throw error;
  const [snapshot] = snapshotRows(data);
  if (!snapshot || snapshot.personId !== id) {
    throw new Error('invalid_person_like_response');
  }
  return snapshot;
}
