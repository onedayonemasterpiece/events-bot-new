\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout='10s';
SET LOCAL search_path=pg_catalog,extensions;
SET LOCAL TIME ZONE 'UTC';
CREATE TEMP TABLE backup(j jsonb NOT NULL) ON COMMIT DROP;
\copy backup FROM 'rollback.csv' WITH (FORMAT csv)
CREATE TEMP TABLE transition ON COMMIT DROP AS SELECT (j#>>'{before,id}')::bigint AS id,
  j->'before' AS before, j->'after' AS after FROM backup;
ALTER TABLE transition ADD PRIMARY KEY(id);
LOCK TABLE public.event_search_documents, public.event_embeddings IN SHARE ROW EXCLUSIVE MODE;
CREATE TEMP VIEW current_release AS
SELECT d.event_id AS id, d.text_hash AS sh, d.related_text_hash AS h, d.related_doc_version AS v,
       d.metadata AS dm, e.text_hash AS eh, e.embedding::text AS vec, e.metadata AS rm, e.embedded_at AS ts,
       s.metadata AS sm, s.text_hash AS search_embedding_hash
FROM public.event_search_documents d
JOIN public.event_embeddings e ON e.event_id=d.event_id AND e.embedding_model='gemini-embedding-2' AND e.embedding_dim=768 AND e.embedding_doc_kind='related_v1'
JOIN public.event_embeddings s ON s.event_id=d.event_id AND s.embedding_model=e.embedding_model AND s.embedding_dim=e.embedding_dim AND s.embedding_doc_kind='search_v3';
DO $guard$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM transition)
    OR EXISTS (SELECT event_id FROM public.event_search_documents EXCEPT SELECT id FROM transition)
    OR EXISTS (SELECT id FROM transition EXCEPT SELECT id FROM current_release)
    OR EXISTS (SELECT FROM transition t JOIN current_release c USING(id)
        WHERE to_jsonb(c) IS DISTINCT FROM t.after)
    THEN RAISE EXCEPTION 'Live state advanced, or this release was already reverted. Rebuild from current inputs.'; END IF;
END $guard$;
UPDATE public.event_search_documents d SET related_text_hash=t.before->>'h', related_doc_version=t.before->>'v', metadata=t.before->'dm'
FROM transition t WHERE t.id=d.event_id;
UPDATE public.event_embeddings e SET embedding=(t.before->>'vec')::extensions.vector(768), text_hash=t.before->>'eh',
  embedded_at=(t.before->>'ts')::timestamptz, metadata=t.before->'rm'
FROM transition t WHERE t.id=e.event_id AND e.embedding_model='gemini-embedding-2' AND e.embedding_dim=768 AND e.embedding_doc_kind='related_v1';
UPDATE public.event_embeddings e SET metadata=t.before->'sm'
FROM transition t WHERE t.id=e.event_id AND e.embedding_model='gemini-embedding-2' AND e.embedding_dim=768 AND e.embedding_doc_kind='search_v3';
DO $verify$
BEGIN
  IF EXISTS (SELECT FROM transition t JOIN current_release c USING(id)
             WHERE to_jsonb(c) IS DISTINCT FROM t.before)
    THEN RAISE EXCEPTION 'Rollback readback differs'; END IF;
END $verify$;
COMMIT;
