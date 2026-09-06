/** Portable, immutable dialogue kernel. The host MUST persist transitions with CAS.
 * No authentication, provider calls, UI geometry, permanent profile, or analytics writes.
 */
export type Mode = 'new_search' | 'refine_selection' | 'continue_draft' | 'explain_selection' | 'expand_selection';
export type Intent = {
  goal: string;
  localityIds: string[];
  excludedFormats: string[];
  freeOnly: boolean;
  maxPrice: number | null;
};
export type Patch = { goal?: string; localityIds?: string[]; excludedFormats?: string[];
  freeOnly?: boolean; maxPrice?: number | null };
export type Input = { id: string; sequence: number; epoch: number; previousId: string | null;
  mode: Mode; parentId: string | null; text: string };
export type Utterance = Input & { status: 'accepted' | 'interpreted'; };
export type Section = { id: string; parentId: string | null; mode: Mode; title: string;
  question: string; answer: string; eventIds: string[]; catalogRevision: string;
  intent: Intent; through: number; epoch: number };
export type Ticket = { epoch: number; revision: number; draftId: string; through: number };
export type Draft = { id: string; parentId: string | null; mode: Mode;
  from: number; intent: Intent; status: 'pending' | 'ready' | 'failed' | 'unknown'; error: string | null };
export type State = { epoch: number; revision: number; acceptedThrough: number;
  processedThrough: number; receipts: Utterance[]; sections: Section[];
  draft: Draft | null; activeIntent: Intent };
export class DialogueError extends Error {
  readonly code: string;
  constructor(code: string) { super(code); this.name = 'DialogueError'; this.code = code; }
}
const blank = (): Intent => ({ goal: '', localityIds: [], excludedFormats: [], freeOnly: false, maxPrice: null });
const copy = <T>(value: T): T => structuredClone(value);
function fail(code: string): never { throw new DialogueError(code); }
const id = (value: unknown): value is string => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/.test(value);
const fields = (obj: unknown, keys: string[]) => {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) fail('invalid_object');
  if (Object.keys(obj as object).some(key => !keys.includes(key))) fail('unknown_field');
};
function ids(value: unknown, max = 256): string[] {
  if (!Array.isArray(value) || value.length > max || value.some(v => !id(v))) fail('invalid_ids');
  return [...new Set(value as string[])];
}
function shortText(value: unknown, limit: number, allowEmpty = false): string {
  if (typeof value !== 'string' || value.length > limit || (!allowEmpty && !value.trim())) fail('invalid_text');
  return value as string;
}
export function initialState(): State {
  return { epoch: 1, revision: 0, acceptedThrough: 0, processedThrough: 0,
    receipts: [], sections: [], draft: null, activeIntent: blank() };
}
/** A deterministic host-applied patch; natural-language interpretation belongs to the provider adapter. */
export function applyIntentPatch(base: Intent, patch: Patch): Intent {
  fields(patch, ['goal', 'localityIds', 'excludedFormats', 'freeOnly', 'maxPrice']);
  const next = copy(base);
  if (Object.hasOwn(patch, 'goal')) next.goal = shortText(patch.goal, 2048, true);
  if (Object.hasOwn(patch, 'localityIds')) next.localityIds = ids(patch.localityIds);
  if (Object.hasOwn(patch, 'excludedFormats')) next.excludedFormats = ids(patch.excludedFormats);
  if (Object.hasOwn(patch, 'freeOnly')) {
    if (typeof patch.freeOnly !== 'boolean') fail('invalid_free_flag');
    next.freeOnly = patch.freeOnly as boolean;
  }
  if (Object.hasOwn(patch, 'maxPrice')) {
    const amount = patch.maxPrice;
    if (amount !== null && (typeof amount !== 'number' || !Number.isFinite(amount) || amount < 0 || amount > 1e8)) fail('invalid_price');
    next.maxPrice = amount as number | null;
  }
  // Contradictions are rejected; the interpreter must explicitly clear a replaced constraint.
  if (next.freeOnly && next.maxPrice !== null && next.maxPrice !== 0) fail('conflicting_price');
  return next;
}
function sameInput(a: Input, b: Input): boolean {
  return ['id', 'sequence', 'epoch', 'previousId', 'mode', 'parentId', 'text']
    .every(k => a[k as keyof Input] === b[k as keyof Input]);
}
/** Receipt sequence is server-ordered. A gap is rejected as NOT accepted; callers retain/reconcile input.
 * The durable intake may queue uploads out of order, but invokes this kernel only for a contiguous prefix.
 */
export function acceptInput(state: State, input: Input): State {
  fields(input, ['id', 'sequence', 'epoch', 'previousId', 'mode', 'parentId', 'text']);
  if (!id(input.id) || !Number.isSafeInteger(input.sequence) || input.sequence < 1) fail('invalid_identity');
  shortText(input.text, 8192);
  if (input.epoch !== state.epoch) fail('stale_epoch');
  if (!['new_search', 'refine_selection', 'continue_draft', 'explain_selection', 'expand_selection'].includes(input.mode)) fail('invalid_mode');
  if (input.parentId !== null && !id(input.parentId)) fail('invalid_parent');
  if (input.previousId !== null && !id(input.previousId)) fail('invalid_predecessor');
  const existing = state.receipts.find(r => r.id === input.id);
  if (existing) return sameInput(existing, input) ? state : fail('payload_conflict');
  if (state.receipts.length >= 256) fail('receipt_capacity');
  if (input.sequence !== state.acceptedThrough + 1) fail('sequence_conflict');
  const previous = state.receipts.at(-1)?.id ?? null;
  if (input.previousId !== previous) fail('predecessor_conflict');
  const next = copy(state);
  const parent = input.parentId === null ? null : next.sections.find(s => s.id === input.parentId);
  if (input.parentId !== null && !parent) fail('parent_not_found');
  if (next.draft) {
    if (next.draft.status === 'failed' || next.draft.status === 'unknown') fail('draft_needs_resolution');
    if (input.mode !== 'continue_draft' || input.parentId !== next.draft.parentId) fail('pending_draft_conflict');
    next.draft.status = 'pending';
  } else {
    if (next.sections.length >= 64) fail('history_capacity');
    if (input.mode === 'continue_draft') fail('draft_not_found');
    if (input.mode !== 'new_search' && !parent) fail('parent_required');
    if (input.mode === 'new_search' && input.parentId !== null) fail('new_search_has_parent');
    next.draft = { id: `draft:${input.id}`, parentId: input.parentId, mode: input.mode,
      from: input.sequence, intent: parent ? copy(parent.intent) : blank(), status: 'pending', error: null };
  }
  next.receipts.push({ ...copy(input), status: 'accepted' });
  next.acceptedThrough = input.sequence;
  next.revision++;
  return next;
}
/** Expected revision makes concurrent/superseded interpreter results explicit conflicts, never silent overwrites. */
export function interpretInput(state: State, uid: string, patch: Patch, expectedRevision: number): State {
  if (expectedRevision !== state.revision) fail('revision_conflict');
  const row = state.receipts.find(r => r.id === uid);
  if (!row || !state.draft) fail('utterance_not_found');
  if (row.status !== 'accepted' || row.sequence !== state.processedThrough + 1) fail('interpretation_order');
  if (!['pending', 'ready'].includes(state.draft.status)) fail('draft_needs_resolution');
  const next = copy(state);
  next.draft!.intent = applyIntentPatch(state.draft.intent, patch);
  next.activeIntent = copy(next.draft!.intent);
  next.receipts.find(r => r.id === uid)!.status = 'interpreted';
  next.processedThrough = row.sequence;
  next.draft!.status = next.processedThrough === next.acceptedThrough ? 'ready' : 'pending';
  next.revision++;
  return next;
}
export function retrievalTicket(state: State): Ticket {
  if (!state.draft || state.draft.status !== 'ready' || state.processedThrough !== state.acceptedThrough) fail('not_ready');
  return { epoch: state.epoch, revision: state.revision, draftId: state.draft.id, through: state.processedThrough };
}
export function ticketIsCurrent(state: State, ticket: Ticket): boolean {
  return state.epoch === ticket.epoch && state.revision === ticket.revision &&
    state.draft?.id === ticket.draftId && state.draft.status === 'ready' &&
    state.processedThrough === ticket.through && state.acceptedThrough === ticket.through;
}
export type Answer = { id: string; title: string; answer: string; eventIds: string[]; catalogRevision: string };
/** Host provides independently validated eligible IDs. Model IDs alone can never authorize results. */
export function commitAnswer(state: State, ticket: Ticket, answer: Answer, eligibleIds: readonly string[]): State {
  if (!ticketIsCurrent(state, ticket)) fail('stale_result');
  fields(answer, ['id', 'title', 'answer', 'eventIds', 'catalogRevision']);
  if (!id(answer.id) || !id(answer.catalogRevision)) fail('invalid_identity');
  if (state.sections.some(s => s.id === answer.id)) fail('section_conflict');
  shortText(answer.title, 160); shortText(answer.answer, 8192, true);
  const members = ids(answer.eventIds, 4096);
  const allowed = new Set(eligibleIds);
  if (members.some(member => !allowed.has(member))) fail('untrusted_result_id');
  const draft = state.draft!;
  if (draft.mode === 'refine_selection') {
    const parent = state.sections.find(s => s.id === draft.parentId)!;
    if (parent.catalogRevision !== answer.catalogRevision) fail('parent_revision_changed');
    if (members.some(member => !parent.eventIds.includes(member))) fail('subset_expanded');
  }
  const next = copy(state);
  next.sections.push({ ...copy(answer), eventIds: members, parentId: draft.parentId, mode: draft.mode,
    question: state.receipts.filter(r => r.sequence >= draft.from).map(r => r.text).join('\n'),
    intent: copy(draft.intent), through: ticket.through, epoch: state.epoch });
  next.draft = null;
  next.revision++;
  return next;
}
/** Stale failure must not replace newer success; error strings are safe enums supplied by the host. */
export function failDraft(state: State, ticket: Ticket, code: string, unknown: boolean): State {
  if (!ticketIsCurrent(state, ticket)) return state;
  if (!id(code)) fail('invalid_error');
  const next = copy(state); next.draft!.status = unknown ? 'unknown' : 'failed';
  next.draft!.error = code; next.revision++; return next;
}
export function resetTask(state: State, removeHistory = false): State {
  return { ...initialState(), epoch: state.epoch + 1, revision: state.revision + 1,
    sections: removeHistory ? [] : copy(state.sections) };
}
/** Current explicit hide/lifecycle overlays NEVER mutate a committed historical membership. */
export function visibleMembers(section: Section, hidden: ReadonlySet<string>, unavailable: ReadonlySet<string> = new Set()): string[] {
  return section.eventIds.filter(member => !hidden.has(member) && !unavailable.has(member));
}
/** Resolve an ordinal only against the exact visible list recorded when the user referred to it. */
export function resolveOrdinal(visibleIds: readonly string[], index: number): string {
  if (!Number.isSafeInteger(index) || index < 1 || index > visibleIds.length) fail('invalid_ordinal');
  return visibleIds[index - 1]!;
}
