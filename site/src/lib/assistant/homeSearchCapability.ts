/** Same capability gate for the inline entry and its page-end affordance. */
export function homeSearchCapability(env:Record<string,unknown>):boolean {
 return Boolean(env.PUBLIC_PERSONALIZATION_SUPABASE_URL&&env.PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY&&env.PUBLIC_EVENT_SEARCH_ASSISTANT_ENABLED==='1'&&env.PUBLIC_SITE_MODE!=='production'&&env.PUBLIC_EVENT_SEARCH_ASSISTANT_CAPTURE_ONLY!=='1');
}
