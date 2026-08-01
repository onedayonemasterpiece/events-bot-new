import { supabaseAuthStorageKey } from './resilientSupabaseTransport.ts';

export const AUTH_INTENT_KEY = 'ke_yandex_auth_intent_v1';

type ResettableStorage = Pick<Storage, 'length' | 'key' | 'getItem' | 'removeItem'>;

/** Remove every persisted Supabase Auth fragment for this exact project. */
export function purgeStaticAuthStorage(supabaseUrl: string, storage: ResettableStorage): boolean {
  const authKey = supabaseAuthStorageKey(supabaseUrl);
  const keys: string[] = [];
  try {
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (key && (key === AUTH_INTENT_KEY || key.startsWith(authKey))) keys.push(key);
    }
    for (const key of keys) storage.removeItem(key);
    for (let index = 0; index < storage.length; index += 1) {
      const key = storage.key(index);
      if (key && (key === AUTH_INTENT_KEY || key.startsWith(authKey))) return false;
    }
    return storage.getItem(authKey) == null;
  } catch {
    return false;
  }
}
